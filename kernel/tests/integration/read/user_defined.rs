use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use delta_kernel::arrow::array::{new_null_array, Array, RecordBatch, StructArray};
use delta_kernel::arrow::compute::{cast, concat_batches};
use delta_kernel::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::schema::{schema, schema_ref, ArrayType, DataType, MapType, UserDefinedType};
use delta_kernel::table_features::{assign_column_mapping_metadata, ColumnMappingMode};
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::json;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{add_commit, read_scan, record_batch_to_bytes};

#[rstest]
#[case::long(
    udt(DataType::LONG),
    ArrowType::Int64,
    ArrowType::Int64,
    r#"{"id":1,"value":42}
{"id":2,"value":null}"#
)]
#[case::vector(
    udt(schema! {
        nullable "kind": BYTE,
        nullable "values": (ArrayType::new(DataType::DOUBLE, false)),
    }),
    vector_arrow_type(), vector_arrow_type(),
    r#"{"id":1,"value":{"kind":1,"values":[1.5,2.5]}}
{"id":2,"value":null}"#
)]
#[case::array(
    udt(ArrayType::new(DataType::LONG, true)),
    list_arrow_type(),
    list_arrow_type(),
    r#"{"id":1,"value":[1,null,3]}
{"id":2,"value":null}"#
)]
#[case::map(
    udt(MapType::new(DataType::STRING, DataType::LONG, true)),
    map_arrow_type(),
    map_arrow_type(),
    r#"{"id":1,"value":{"a":1,"b":null}}
{"id":2,"value":null}"#
)]
#[case::struct_containing_udt(
    DataType::from(schema! { nullable "child": (udt(DataType::LONG)) }),
    ArrowType::Struct(vec![Field::new("child", ArrowType::Int64, true)].into()),
    ArrowType::Struct(vec![Field::new("child", ArrowType::Int64, true)].into()),
    r#"{"id":1,"value":{"child":42}}
{"id":2,"value":null}"#
)]
#[case::array_of_udt(
    DataType::from(ArrayType::new(udt(DataType::LONG), true)),
    list_arrow_type(),
    list_arrow_type(),
    r#"{"id":1,"value":[1,null,3]}
{"id":2,"value":null}"#
)]
#[case::map_of_udt(
    DataType::from(MapType::new(DataType::STRING, udt(DataType::LONG), true)),
    map_arrow_type(),
    map_arrow_type(),
    r#"{"id":1,"value":{"a":1,"b":null}}
{"id":2,"value":null}"#
)]
#[case::int32_to_long(
    udt(DataType::LONG),
    ArrowType::Int32,
    ArrowType::Int64,
    r#"{"id":1,"value":42}
{"id":2,"value":null}"#
)]
#[tokio::test]
async fn read_udt_as_sql_type_preserves_logical_schema(
    #[case] logical_type: DataType,
    #[case] file_type: ArrowType,
    #[case] expected_type: ArrowType,
    #[case] rows: &str,
    #[values(false, true)] project: bool,
    #[values(false, true)] missing: bool,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    mapping_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema_ref! {
        nullable "id": LONG,
        nullable "value": (logical_type),
    };
    let make_batch = |value_type| -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int64, true),
            Field::new("value", value_type, true),
        ]);
        Ok(ReaderBuilder::new(Arc::new(arrow_schema))
            .build(Cursor::new(rows))?
            .next()
            .unwrap()?)
    };
    let batch = make_batch(file_type.clone())?;
    let expected = make_batch(expected_type.clone())?;
    let (batch, expected) = if missing {
        let expected = RecordBatch::try_new(
            expected.schema(),
            vec![
                expected.column(0).clone(),
                new_null_array(&expected_type, expected.num_rows()),
            ],
        )?;
        (batch.project(&[0])?, expected)
    } else {
        (batch, expected)
    };
    let mut max_column_id = 0;
    let schema = if mapping_mode == ColumnMappingMode::None {
        schema
    } else {
        Arc::new(assign_column_mapping_metadata(
            &schema,
            &mut max_column_id,
            false,
        )?)
    };
    // Mapping adds field names and IDs after parsing the independently typed fixtures.
    let expected = if mapping_mode == ColumnMappingMode::None {
        expected
    } else {
        let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
        let columns = expected
            .columns()
            .iter()
            .zip(arrow_schema.fields())
            .map(|(column, field)| cast(column, field.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(Arc::new(arrow_schema), columns)?
    };
    let physical_schema: ArrowSchema = (&schema.make_physical(mapping_mode)?).try_into_arrow()?;
    let mut physical_fields = physical_schema.fields()[..batch.num_columns()].to_vec();
    if !missing && file_type == ArrowType::Int32 {
        physical_fields[1] = Arc::new(
            physical_fields[1]
                .as_ref()
                .clone()
                .with_data_type(file_type),
        );
    }
    let mut physical_columns = batch
        .columns()
        .iter()
        .zip(&physical_fields)
        .map(|(column, field)| cast(column, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(vector) = physical_columns
        .get(1)
        .and_then(|column| column.as_any().downcast_ref::<StructArray>())
    {
        // Reordering makes the read distinguish sqlType-name matching from positional matching.
        let fields = vector.fields().iter().rev().cloned().collect::<Vec<_>>();
        let columns = vector.columns().iter().rev().cloned().collect();
        let reordered =
            StructArray::try_new(fields.clone().into(), columns, vector.nulls().cloned())?;
        physical_fields[1] = Arc::new(
            physical_fields[1]
                .as_ref()
                .clone()
                .with_data_type(ArrowType::Struct(fields.into())),
        );
        physical_columns[1] = Arc::new(reordered);
    }
    if mapping_mode == ColumnMappingMode::Id {
        // Only field IDs link these Parquet names to the enclosing logical fields.
        physical_fields = physical_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Arc::new(Field::clone(field).with_name(format!("stored_{index}")))
            })
            .collect();
    }
    let physical_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(physical_fields)),
        physical_columns,
    )?;
    let bytes = record_batch_to_bytes(&physical_batch);
    let store = Arc::new(InMemory::new());
    let table_root = "memory:///";
    let (protocol, configuration) = if mapping_mode == ColumnMappingMode::None {
        (
            json!({"minReaderVersion":1,"minWriterVersion":2}),
            json!({}),
        )
    } else {
        (
            json!({"minReaderVersion":3,"minWriterVersion":7,
                "readerFeatures":["columnMapping"], "writerFeatures":["columnMapping"]}),
            json!({"delta.columnMapping.mode":mapping_mode,
                "delta.columnMapping.maxColumnId":max_column_id.to_string()}),
        )
    };
    let actions = [
        json!({"protocol":protocol}),
        json!({"metaData":{
            "id":"udt-read", "format":{"provider":"parquet","options":{}},
            "schemaString":serde_json::to_string(&schema)?,
            "partitionColumns":[], "configuration":configuration, "createdTime":0,
        }}),
        json!({"add":{
            "path":"data.parquet", "size":bytes.len(), "partitionValues":{},
            "modificationTime":0, "dataChange":true,
        }}),
    ];
    add_commit(
        table_root,
        store.as_ref(),
        0,
        actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    store.put(&Path::from("data.parquet"), bytes.into()).await?;
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    assert_eq!(
        serde_json::to_value(snapshot.schema())?,
        serde_json::to_value(&schema)?
    );
    let requested = if project {
        schema.project(&["value"])?
    } else {
        schema
    };
    let scan = snapshot
        .scan_builder()
        .with_schema(requested.clone())
        .build()?;
    assert_eq!(
        serde_json::to_value(scan.logical_schema())?,
        serde_json::to_value(requested)?
    );
    let batches = read_scan(&scan, engine)?;
    let expected = if project {
        expected.project(&[1])?
    } else {
        expected
    };
    assert_eq!(concat_batches(&expected.schema(), &batches)?, expected);
    Ok(())
}

fn udt(sql_type: impl Into<DataType>) -> DataType {
    UserDefinedType::try_new(
        sql_type,
        HashMap::from([
            ("class".into(), Some("example.Value".into())),
            ("pyClass".into(), None),
        ]),
    )
    .unwrap()
    .into()
}

fn vector_arrow_type() -> ArrowType {
    ArrowType::Struct(
        vec![
            Field::new("kind", ArrowType::Int8, true),
            Field::new(
                "values",
                ArrowType::List(Arc::new(Field::new("element", ArrowType::Float64, false))),
                true,
            ),
        ]
        .into(),
    )
}

fn list_arrow_type() -> ArrowType {
    ArrowType::List(Arc::new(Field::new("element", ArrowType::Int64, true)))
}

fn map_arrow_type() -> ArrowType {
    ArrowType::Map(
        Arc::new(Field::new(
            "key_value",
            ArrowType::Struct(
                vec![
                    Field::new("key", ArrowType::Utf8, false),
                    Field::new("value", ArrowType::Int64, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}
