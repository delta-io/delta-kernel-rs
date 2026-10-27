use rstest::rstest;

use super::*;

// Independent recursive traversal is an oracle for cache maintenance, never a production path.
fn walk_schema(schema: &StructType) -> Memory {
    schema.type_name.heap_size(&mut SizeContext::default())
        + index_map_bytes::<String, StructField>(schema.fields.capacity())
        + schema
            .metadata_columns
            .heap_size(&mut SizeContext::default())
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
        _ => data_type.heap_size(&mut SizeContext::default()),
    }
}

#[rstest]
fn construction_and_clone_summaries_match_oracle(
    #[values(0, 1, 17, 128)] fields: usize,
    #[values(false, true)] nested: bool,
    #[values(false, true)] checked: bool,
) {
    let fields = (0..fields).map(|i| {
        let ty = if nested {
            MapType::new(
                DataType::STRING,
                ArrayType::new(
                    StructType::new_unchecked([StructField::nullable("nested", DataType::STRING)]),
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
    });
    let schema = if checked {
        StructType::try_new(fields).unwrap()
    } else {
        StructType::new_unchecked(fields)
    };
    assert_eq!(schema.retained_memory, walk_schema(&schema));
    let cloned = schema.clone();
    assert_eq!(cloned, schema);
    assert_eq!(cloned.retained_memory, walk_schema(&cloned));
    let parsed: StructType =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(parsed, schema);
    assert_eq!(parsed.retained_memory, walk_schema(&parsed));
}

#[test]
fn mutating_nested_schema_invalidates_parent_and_clone_rebuilds_summary() {
    let schema = StructType::new_unchecked([StructField::nullable(
        "outer",
        StructType::new_unchecked([StructField::nullable("leaf", DataType::LONG)]),
    )]);
    let mut ty = DataType::from(schema);
    let inner = ty.struct_at_path(&["outer".into()]).unwrap();
    let field = inner.field_map_mut().get_mut("leaf").unwrap();
    field
        .metadata
        .insert("comment".into(), MetadataValue::String("x".repeat(8192)));
    let DataType::Struct(schema) = &ty else {
        panic!("struct expected")
    };
    assert_eq!(
        schema.retained_memory.total_bytes(),
        Err(Unavailable::MutatedSchema)
    );
    let cloned = schema.clone();
    assert_eq!(schema.as_ref(), cloned.as_ref());
    assert_eq!(cloned.retained_memory, walk_schema(&cloned));
    assert!(cloned.retained_memory.total_bytes().unwrap() >= 8192);
}

#[test]
fn unchecked_duplicate_field_does_not_report_the_replaced_allocation() {
    let schema = StructType::new_unchecked([
        StructField::nullable("same", DataType::LONG),
        StructField::nullable("same", DataType::STRING),
    ]);
    assert_eq!(
        schema.retained_memory.total_bytes(),
        Err(Unavailable::MutatedSchema)
    );
    let cloned = schema.clone();
    assert_eq!(cloned.retained_memory, walk_schema(&cloned));
}
