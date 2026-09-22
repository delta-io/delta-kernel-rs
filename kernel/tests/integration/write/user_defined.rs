//! UDT writes through physical data, transported write state, and metadata commits.

use std::io::Cursor;
use std::sync::Arc;

use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{
    schema, schema_ref, ArrayType, DataType, MapType, StructField, UserDefinedType,
};
use delta_kernel::table_features::{assign_column_mapping_metadata, ColumnMappingMode};
use delta_kernel::transaction::WriteState;
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::json;
use test_utils::{add_commit, begin_transaction, test_read, test_table_setup_mt};
use url::Url;

#[rstest]
#[case::long(
    DataType::LONG,
    r#"{"id":1,"value":42}
{"id":2,"value":null}"#
)]
#[case::vector(
    DataType::from(schema! {
        nullable "kind": BYTE,
        nullable "values": (ArrayType::new(DataType::DOUBLE, false)),
    }),
    r#"{"id":1,"value":{"kind":1,"values":[1.5,2.5]}}
{"id":2,"value":null}"#,
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_udt_preserves_annotation_across_metadata_and_checkpoints(
    #[case] sql_type: DataType,
    #[case] rows: &str,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    mapping_mode: ColumnMappingMode,
    #[values(false, true)] transport_write_state: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, table_path, engine) = test_table_setup_mt()?;
    let table_root = Url::from_directory_path(&table_path).unwrap();
    let udt = UserDefinedType {
        sql_type: Box::new(sql_type),
        annotation: [
            ("class".into(), Some("example.Value".into())),
            ("pyClass".into(), None),
            ("extension".into(), Some("opaque\0member".into())),
        ]
        .into(),
    };
    let expected_type = serde_json::to_value(&udt)?;
    let schema = schema_ref! { nullable "id": LONG, nullable "value": (udt) };
    let schema = if mapping_mode == ColumnMappingMode::None {
        schema
    } else {
        Arc::new(assign_column_mapping_metadata(&schema, &mut 0, false)?)
    };
    let (protocol, configuration) = if mapping_mode == ColumnMappingMode::None {
        (
            json!({"minReaderVersion":1,"minWriterVersion":2}),
            json!({}),
        )
    } else {
        (
            json!({"minReaderVersion":3,"minWriterVersion":7,
                "readerFeatures":["columnMapping"],"writerFeatures":["columnMapping"]}),
            json!({"delta.columnMapping.mode":mapping_mode,"delta.columnMapping.maxColumnId":"2"}),
        )
    };
    let actions = [
        json!({"protocol":protocol}),
        json!({"metaData":{
            "id":"udt-write","format":{"provider":"parquet","options":{}},
            "schemaString":serde_json::to_string(&schema)?,"partitionColumns":[],
            "configuration":configuration,"createdTime":0,
        }}),
    ];
    add_commit(
        table_root.as_str(),
        &LocalFileSystem::new(),
        0,
        actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    let snapshot = Snapshot::builder_for(table_root.as_str()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    let write_state = txn.write_state()?;
    let write_state = if transport_write_state {
        WriteState::decode(&write_state.encode()?)?
    } else {
        write_state
    };
    let context = write_state.write_context_builder().build()?;
    assert_eq!(
        serde_json::to_value(
            context
                .logical_data_schema()
                .field("value")
                .unwrap()
                .data_type()
        )?,
        expected_type,
    );
    let physical_name = schema.field("value").unwrap().physical_name(mapping_mode);
    assert_eq!(
        serde_json::to_value(
            context
                .physical_data_schema()
                .field(physical_name)
                .unwrap()
                .data_type()
        )?,
        expected_type,
    );
    let batch = ReaderBuilder::new(Arc::new(
        context.logical_data_schema().as_ref().try_into_arrow()?,
    ))
    .build(Cursor::new(rows))?
    .next()
    .unwrap()?;
    txn.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(batch.clone()), &context)
            .await?,
    );
    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    assert_eq!(
        serde_json::to_value(snapshot.schema().field("value").unwrap().data_type())?,
        expected_type
    );
    test_read(&ArrowEngineData::new(batch), &table_root, engine.clone())?;

    let snapshot = snapshot
        .alter_table()
        .add_column(StructField::nullable("added", DataType::LONG))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    snapshot.checkpoint(engine.as_ref(), None)?;
    let reloaded = Snapshot::builder_for(table_root.as_str()).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), 2);
    assert_eq!(
        serde_json::to_value(reloaded.schema().field("value").unwrap().data_type())?,
        expected_type
    );
    let expected = ReaderBuilder::new(Arc::new(reloaded.schema().as_ref().try_into_arrow()?))
        .build(Cursor::new(rows))?
        .next()
        .unwrap()?;
    test_read(&ArrowEngineData::new(expected), &table_root, engine)?;
    Ok(())
}
