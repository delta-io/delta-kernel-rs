use std::collections::HashSet;
use std::mem::size_of;

use rstest::rstest;

use super::*;
use crate::snapshot::memory::{hash_table_bytes, index_map_bytes, HeapSize, Memory, SizeContext};

// Independent recursive oracle; production measures roots alongside logical-default validation.
fn walk_schema(schema: &StructType) -> Memory {
    schema.type_name.heap_size(&mut SizeContext::default())
        + index_map_bytes::<String, StructField>(schema.fields.capacity())
        + hash_table_bytes::<(MetadataColumnSpec, usize)>(schema.metadata_columns.capacity())
        + schema
            .fields
            .iter()
            .map(|(key, field)| {
                key.heap_size(&mut SizeContext::default())
                    + field.name.heap_size(&mut SizeContext::default())
                    + field.metadata.heap_size(&mut SizeContext::default())
                    + walk_type(&field.data_type)
            })
            .sum::<Memory>()
}

fn walk_type(data_type: &DataType) -> Memory {
    match data_type {
        DataType::Struct(schema) | DataType::Variant(schema) | DataType::File(schema) => {
            Memory::precise(size_of::<StructType>()) + walk_schema(schema)
        }
        DataType::Array(array) => {
            Memory::precise(size_of::<ArrayType>())
                + array.type_name.heap_size(&mut SizeContext::default())
                + walk_type(&array.element_type)
        }
        DataType::Map(map) => {
            Memory::precise(size_of::<MapType>())
                + map.type_name.heap_size(&mut SizeContext::default())
                + walk_type(&map.key_type)
                + walk_type(&map.value_type)
        }
        DataType::Primitive(primitive) => primitive.heap_size(&mut SizeContext::default()),
        #[cfg(feature = "udt-in-dev")]
        DataType::UserDefined(_) => {
            Memory::unavailable(crate::snapshot::memory::Unavailable::UnsupportedType)
        }
    }
}

fn check_representations(logical: &StructType, partitioned: bool, mode: ColumnMappingMode) {
    let physical = logical.make_physical(mode).unwrap();
    let partitions: HashSet<&str> = if partitioned {
        ["partition"].into()
    } else {
        HashSet::new()
    };
    let logical_subset = StructType::new_unchecked(
        logical
            .fields()
            .filter(|field| !partitions.contains(field.name().as_str()))
            .cloned(),
    );
    let physical_subset = StructType::new_unchecked(
        logical
            .fields()
            .zip(physical.fields())
            .filter(|(logical, _)| !partitions.contains(logical.name().as_str()))
            .map(|(_, physical)| physical.clone()),
    );
    let schemas = [logical, &physical, &logical_subset, &physical_subset];
    let (has_default, memory) =
        validate_defaults_and_measure_schemas(schemas, &partitions).unwrap();
    assert_eq!(
        has_default,
        validate_column_defaults_metadata(logical).unwrap()
    );
    for (schema, measured) in schemas.into_iter().zip(memory) {
        assert_eq!(measured, walk_schema(schema));
    }
}

#[rstest]
fn construction_clone_and_mutation_match_oracle(
    #[values(0, 1, 17, 128)] width: usize,
    #[values(false, true)] nested: bool,
    #[values(false, true)] partitioned: bool,
) {
    let fields = (0..width)
        .map(|i| {
            let ty = if nested {
                MapType::new(
                    DataType::STRING,
                    ArrayType::new(
                        StructType::new_unchecked([StructField::nullable(
                            "nested",
                            DataType::STRING,
                        )]),
                        true,
                    ),
                    true,
                )
                .into()
            } else {
                DataType::STRING
            };
            let mut field = StructField::nullable(format!("field_{i}"), ty);
            field.name.reserve(99);
            field.metadata.insert(
                "json".into(),
                MetadataValue::Other(serde_json::json!({
                    "object": {"nested": ["hello", {"key": "value"}]}
                })),
            );
            field
        })
        .chain([StructField::nullable("partition", DataType::LONG)]);
    let mut schema = StructType::try_new(fields).unwrap();
    check_representations(&schema, partitioned, ColumnMappingMode::None);
    let cloned = schema.clone();
    assert_eq!(cloned, schema);
    check_representations(&cloned, partitioned, ColumnMappingMode::None);
    let parsed: StructType =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(parsed, schema);
    check_representations(&parsed, partitioned, ColumnMappingMode::None);
    schema
        .field_map_mut()
        .get_mut("partition")
        .unwrap()
        .metadata
        .insert("comment".into(), MetadataValue::String("x".repeat(8192)));
    check_representations(&schema, partitioned, ColumnMappingMode::None);
}

#[rstest]
#[case::primitive(DataType::LONG)]
#[case::nested_array(ArrayType::new(StructType::new_unchecked([
    column_default::field_with_default("nested", DataType::LONG, "7")
]), true).into())]
#[case::variant(DataType::unshredded_variant())]
#[case::file(DataType::file_type())]
fn default_validation_matches_existing_validator(#[case] data_type: DataType) {
    let schema = StructType::new_unchecked([column_default::field_with_default(
        "field", data_type, "NULL",
    )]);
    check_representations(&schema, false, ColumnMappingMode::None);
}

#[rstest]
fn mapped_and_partition_excluded_roots_match_oracle(
    #[values(ColumnMappingMode::Name, ColumnMappingMode::Id)] mode: ColumnMappingMode,
) {
    let mapped = |field: StructField, id: i64| {
        field.add_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref().to_owned(),
                MetadataValue::Number(id),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_owned(),
                MetadataValue::String(format!("physical_{id}")),
            ),
        ])
    };
    let schema = StructType::new_unchecked([
        mapped(
            StructField::nullable(
                "outer",
                StructType::new_unchecked([mapped(
                    StructField::nullable("child", DataType::STRING),
                    2,
                )]),
            ),
            1,
        ),
        mapped(StructField::nullable("partition", DataType::LONG), 3),
    ]);
    check_representations(&schema, true, mode);
}

#[test]
fn malformed_nested_default_is_still_rejected() {
    let schema = StructType::new_unchecked([StructField::nullable(
        "outer",
        StructType::new_unchecked([column_default::field_with_invalid_default("bad")]),
    )]);
    let before = validate_column_defaults_metadata(&schema).unwrap_err();
    let after = validate_defaults_and_measure_schemas([&schema; 4], &HashSet::new()).unwrap_err();
    assert_eq!(before.to_string(), after.to_string());
}

#[test]
fn incompatible_companion_shape_marks_only_that_estimate_unavailable() {
    let logical = StructType::new_unchecked([StructField::nullable("x", DataType::LONG)]);
    let incompatible = StructType::new_unchecked([StructField::nullable(
        "x",
        ArrayType::new(DataType::LONG, true),
    )]);
    let (_, memory) = validate_defaults_and_measure_schemas(
        [&logical, &incompatible, &logical, &logical],
        &HashSet::new(),
    )
    .unwrap();
    assert_eq!(memory[0], walk_schema(&logical));
    assert_eq!(
        memory[1].total_bytes(),
        Err(crate::snapshot::memory::Unavailable::SchemaShape)
    );
}
