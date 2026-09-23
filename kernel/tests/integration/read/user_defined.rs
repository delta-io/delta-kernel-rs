use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, RecordBatch, StructArray};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::schema::{schema_ref, ArrayType, DataType, MapType, UserDefinedType};
use delta_kernel::table_features::{assign_column_mapping_metadata, ColumnMappingMode};
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::json;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{add_commit, read_scan, record_batch_to_bytes};

#[rstest]
#[case::long(
    DataType::LONG,
    r#"{"id":1,"value":42}
{"id":2,"value":null}"#
)]
#[case::vector(
    DataType::from(delta_kernel::schema::schema! {
        nullable "kind": BYTE,
        nullable "values": (ArrayType::new(DataType::DOUBLE, false)),
    }),
    r#"{"id":1,"value":{"kind":1,"values":[1.5,2.5]}}
{"id":2,"value":null}"#
)]
#[case::array(
    DataType::from(ArrayType::new(DataType::LONG, true)),
    r#"{"id":1,"value":[1,null,3]}
{"id":2,"value":null}"#
)]
#[case::map(
    DataType::from(MapType::new(DataType::STRING, DataType::LONG, true)),
    r#"{"id":1,"value":{"a":1,"b":null}}
{"id":2,"value":null}"#
)]
#[tokio::test]
async fn read_udt_as_sql_type_preserves_logical_schema(
    #[case] sql_type: DataType,
    #[case] rows: &str,
    #[values(false, true)] project: bool,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    mapping_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let udt = UserDefinedType {
        sql_type: Box::new(sql_type),
        annotation: BTreeMap::from([
            ("class".into(), Some("example.Value".into())),
            ("pyClass".into(), None),
        ]),
    };
    let schema = schema_ref! {
        nullable "id": LONG,
        nullable "value": (udt),
    };
    let schema = if mapping_mode == ColumnMappingMode::None {
        schema
    } else {
        Arc::new(assign_column_mapping_metadata(&schema, &mut 0, false)?)
    };
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
    let batch = ReaderBuilder::new(Arc::new(arrow_schema))
        .build(Cursor::new(rows))?
        .next()
        .unwrap()?;
    let physical_schema: ArrowSchema = (&schema.make_physical(mapping_mode)?).try_into_arrow()?;
    let mut physical_fields = physical_schema.fields().to_vec();
    let mut physical_columns = batch.columns().to_vec();
    if let Some(vector) = physical_columns[1].as_any().downcast_ref::<StructArray>() {
        // Reordering makes the read distinguish sqlType-name matching from positional matching.
        let fields = vector.fields().iter().rev().cloned().collect::<Vec<_>>();
        let columns = vector.columns().iter().rev().cloned().collect();
        let reordered =
            StructArray::try_new(fields.clone().into(), columns, vector.nulls().cloned())?;
        physical_fields[1] = Arc::new(
            physical_fields[1]
                .as_ref()
                .clone()
                .with_data_type(ArrowDataType::Struct(fields.into())),
        );
        physical_columns[1] = Arc::new(reordered);
    }
    if mapping_mode == ColumnMappingMode::Id {
        // Only field IDs link these Parquet names to the enclosing logical fields.
        physical_fields = physical_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Arc::new(ArrowField::clone(field).with_name(format!("stored_{index}")))
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
                "delta.columnMapping.maxColumnId":"2"}),
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
    let expected = if project { batch.project(&[1])? } else { batch };
    assert_eq!(concat_batches(&expected.schema(), &batches)?, expected);
    Ok(())
}
