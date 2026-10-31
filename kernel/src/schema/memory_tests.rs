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
        DataType::UserDefined(udt) => {
            Memory::precise(size_of::<DataType>())
                + udt.annotation().heap_size(&mut SizeContext::default())
                + walk_type(udt.sql_type())
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
    let (logical_subset, logical_subset_memory) = build_and_measure_schema(
        logical
            .fields()
            .filter(|field| !partitions.contains(field.name().as_str()))
            .cloned(),
    )
    .unwrap();
    let (physical_subset, physical_subset_memory) = build_and_measure_schema(
        logical
            .fields()
            .zip(physical.fields())
            .filter(|(logical, _)| !partitions.contains(logical.name().as_str()))
            .map(|(_, physical)| physical.clone()),
    )
    .unwrap();
    assert_eq!(logical_subset_memory, walk_schema(&logical_subset));
    assert_eq!(physical_subset_memory, walk_schema(&physical_subset));
    let schemas = [logical, &physical, &logical_subset, &physical_subset];
    for (index, schema) in schemas.into_iter().enumerate() {
        let (has_default, measured) = measure_schema(schema, index == 0).unwrap();
        assert_eq!(
            has_default,
            index == 0 && validate_column_defaults_metadata(logical).unwrap()
        );
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
    let after = measure_schema(&schema, true).unwrap_err();
    assert_eq!(before.to_string(), after.to_string());
}

#[test]
fn each_root_is_measured_without_assuming_the_shape_of_another_root() {
    let logical = StructType::new_unchecked([StructField::nullable("x", DataType::LONG)]);
    let physical = StructType::new_unchecked([StructField::nullable(
        "x",
        ArrayType::new(DataType::LONG, true),
    )]);
    for schema in [&logical, &physical] {
        assert_eq!(
            measure_schema(schema, false).unwrap().1,
            walk_schema(schema)
        );
    }
}

#[test]
fn construction_measurement_does_not_validate_companion_defaults() {
    let (schema, memory) =
        build_and_measure_schema([column_default::field_with_invalid_default("bad")].into_iter())
            .unwrap();
    assert_eq!(memory, walk_schema(&schema));
    assert!(measure_schema(&schema, true).is_err());
}

#[test]
fn discarded_duplicate_field_makes_construction_measurement_unavailable() {
    let (schema, memory) = build_and_measure_schema(
        [
            StructField::nullable("duplicate", DataType::STRING),
            StructField::nullable("duplicate", DataType::LONG),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(
        memory.total_bytes(),
        Err(crate::snapshot::memory::Unavailable::SchemaShape)
    );
}

#[cfg(feature = "geo-type-in-dev")]
#[test]
fn geospatial_schema_counts_box_and_crs_storage() {
    let schema = StructType::new_unchecked([
        StructField::nullable(
            "geometry",
            PrimitiveType::Geometry(Box::new(GeometryType::try_new("EPSG:4326").unwrap())),
        ),
        StructField::nullable(
            "geography",
            PrimitiveType::Geography(Box::new(
                GeographyType::try_new("EPSG:4326", EdgeInterpolationAlgorithm::Spherical).unwrap(),
            )),
        ),
    ]);
    assert!(measure_schema(&schema, false)
        .unwrap()
        .1
        .total_bytes()
        .is_ok());
    assert_eq!(
        measure_schema(&schema, false).unwrap().1,
        walk_schema(&schema)
    );
}

#[cfg(feature = "udt-in-dev")]
#[test]
fn user_defined_schema_counts_annotation_and_physical_tree() {
    let physical = StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]);
    let udt = UserDefinedType::try_new(
        physical,
        [
            ("class".to_owned(), Some("example.Id".to_owned())),
            ("pyClass".to_owned(), None),
        ]
        .into(),
    )
    .unwrap();
    let schema = StructType::new_unchecked([StructField::nullable("id", udt)]);
    assert!(measure_schema(&schema, false)
        .unwrap()
        .1
        .total_bytes()
        .is_ok());
    assert_eq!(
        measure_schema(&schema, false).unwrap().1,
        walk_schema(&schema)
    );
}
