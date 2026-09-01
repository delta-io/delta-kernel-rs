//! Integration tests for basic append write paths (single writes, repeated writes,
//! partitioned appends, and schema-mismatch rejection).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{new_null_array, Int32Array, StringArray};
use delta_kernel::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{schema, schema_ref};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::WriteState;
use delta_kernel::{DeltaResult, KernelError, Snapshot};
use itertools::Itertools;
use rstest::rstest;
use serde_json::{json, Deserializer};
use test_utils::{
    assert_result_error_with_message, into_record_batch, load_and_begin_transaction,
    modify_add_file_partition_keys, set_json_value, setup_test_tables, test_read,
    AddFilePartitionKeyModify,
};

use crate::common::write_utils::{
    check_action_timestamps, get_and_check_all_parquet_sizes, get_simple_int_schema,
    validate_txn_id, write_data_and_check_result_and_stats, ZERO_UUID,
};

#[tokio::test]
async fn test_append() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        let size =
            get_and_check_all_parquet_sizes(store.clone(), format!("/{table_name}/").as_str())
                .await;
        // check that the timestamps in commit_info and add actions are within 10s of
        // SystemTime::now() before we clear them for comparison
        check_action_timestamps(parsed_commits.iter())?;
        // check that the txn_id is valid before we clear it for comparison
        validate_txn_id(&parsed_commits[0]["commitInfo"]);

        // set timestamps to 0, paths and txn_id to known string values for comparison
        // (otherwise timestamps are non-deterministic, paths and txn_id are random UUIDs)
        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        set_json_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
        set_json_value(&mut parsed_commits[1], "add.path", json!("first.parquet"))?;
        set_json_value(&mut parsed_commits[2], "add.modificationTime", json!(0))?;
        set_json_value(&mut parsed_commits[2], "add.path", json!("second.parquet"))?;

        let expected_commit = vec![
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "UNKNOWN",
                    "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                    "operationParameters": {},
                    "txnId": ZERO_UUID
                }
            }),
            json!({
                "add": {
                    "path": "first.parquet",
                    "partitionValues": {},
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true,
                    "stats": "{\"numRecords\":3,\"nullCount\":{\"number\":0},\"minValues\":{\"number\":1},\"maxValues\":{\"number\":3},\"tightBounds\":true}"
                }
            }),
            json!({
                "add": {
                    "path": "second.parquet",
                    "partitionValues": {},
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true,
                    "stats": "{\"numRecords\":3,\"nullCount\":{\"number\":0},\"minValues\":{\"number\":4},\"maxValues\":{\"number\":6},\"tightBounds\":true}"
                }
            }),
        ];

        assert_eq!(parsed_commits, expected_commit);

        test_read(
            &ArrowEngineData::new(RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            )?),
            &table_url,
            engine,
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn test_no_add_actions() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_engine_info("default engine");

        // Commit without adding any add files
        assert!(txn.commit(&engine)?.is_committed());

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let parsed_actions: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        // Verify that there only is a commit info action
        assert_eq!(parsed_actions.len(), 1, "Expected only one action");
        assert!(parsed_actions[0].get("commitInfo").is_some());
    }
    Ok(())
}

#[tokio::test]
async fn test_append_twice() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, _, _) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let engine = Arc::new(engine);
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 2)
            .await?;

        test_read(
            &ArrowEngineData::new(RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(vec![
                    1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                ]))],
            )?),
            &table_url,
            engine,
        )?;
    }
    Ok(())
}

#[rstest]
#[case::local_write_state(false)]
#[case::transported_write_state(true)]
#[tokio::test]
async fn test_append_partitioned(
    #[case] transport_write_state: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    let partition_col = "partition";

    // create a simple partitioned table: one int column named 'number', partitioned by string
    // column named 'partition'
    let table_schema = schema_ref! {
        nullable "number": INTEGER,
        nullable "partition": STRING,
    };
    let data_schema = schema_ref! { nullable "number": INTEGER };

    for (table_url, engine, store, table_name) in
        setup_test_tables(table_schema.clone(), &[partition_col], None, "test_table").await?
    {
        let mut txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_engine_info("default engine")
            .with_data_change(false);

        // create two new arrow record batches to append
        let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(
                    data_schema
                        .as_ref()
                        .try_into_arrow()
                        .map_err(KernelError::from)?,
                ),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )
            .map_err(KernelError::from)?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });
        let partition_vals = vec!["a", "b"];

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let write_state = txn.write_state()?;
        let encoded_write_state = transport_write_state
            .then(|| write_state.encode())
            .transpose()?;
        let tasks = append_data
            .into_iter()
            .zip(partition_vals)
            .map(|(data, partition_val)| {
                let partition_values = HashMap::from([(
                    partition_col.to_string(),
                    Scalar::String(partition_val.into()),
                )]);
                let write_context = match &encoded_write_state {
                    Some(encoded) => WriteState::decode(encoded)
                        .and_then(|state| state.partitioned_write_context(partition_values)),
                    None => write_state.partitioned_write_context(partition_values),
                }
                .unwrap();
                let engine = engine.clone();
                tokio::task::spawn(async move {
                    engine
                        .write_parquet(data.as_ref().unwrap(), &write_context)
                        .await
                })
            });

        let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
        for meta in add_files_metadata {
            txn.add_files(meta?);
        }

        // commit!
        assert!(txn.commit(engine.as_ref())?.is_committed());

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        let size =
            get_and_check_all_parquet_sizes(store.clone(), format!("/{table_name}/").as_str())
                .await;
        // check that the timestamps in commit_info and add actions are within 10s of
        // SystemTime::now() before we clear them for comparison
        check_action_timestamps(parsed_commits.iter())?;
        // check that the txn_id is valid before we clear it for comparison
        validate_txn_id(&parsed_commits[0]["commitInfo"]);

        // set timestamps to 0, paths and txn_id to known string values for comparison
        // (otherwise timestamps are non-deterministic, paths and txn_id are random UUIDs)
        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        set_json_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
        set_json_value(&mut parsed_commits[1], "add.path", json!("first.parquet"))?;
        set_json_value(&mut parsed_commits[2], "add.modificationTime", json!(0))?;
        set_json_value(&mut parsed_commits[2], "add.path", json!("second.parquet"))?;

        let expected_commit = vec![
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "UNKNOWN",
                    "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                    "operationParameters": {},
                    "engineInfo": "default engine",
                    "txnId": ZERO_UUID
                }
            }),
            json!({
                "add": {
                    "path": "first.parquet",
                    "partitionValues": {
                        "partition": "a"
                    },
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": false,
                    "stats": "{\"numRecords\":3,\"nullCount\":{\"number\":0},\"minValues\":{\"number\":1},\"maxValues\":{\"number\":3},\"tightBounds\":true}"
                }
            }),
            json!({
                "add": {
                    "path": "second.parquet",
                    "partitionValues": {
                        "partition": "b"
                    },
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": false,
                    "stats": "{\"numRecords\":3,\"nullCount\":{\"number\":0},\"minValues\":{\"number\":4},\"maxValues\":{\"number\":6},\"tightBounds\":true}"
                }
            }),
        ];

        assert_eq!(parsed_commits, expected_commit);

        test_read(
            &ArrowEngineData::new(RecordBatch::try_new(
                Arc::new(table_schema.as_ref().try_into_arrow()?),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"])),
                ],
            )?),
            &table_url,
            engine,
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn test_append_invalid_schema() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let table_schema = schema_ref! { nullable "number": INTEGER };
    // incompatible data schema: one string column named 'string'
    let data_schema = schema_ref! { nullable "string": STRING };

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(table_schema, &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_engine_info("default engine");

        // create two new arrow record batches to append
        let append_data = [["a", "b"], ["c", "d"]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(
                    data_schema
                        .as_ref()
                        .try_into_arrow()
                        .map_err(KernelError::from)?,
                ),
                vec![Arc::new(StringArray::from(data.to_vec()))],
            )
            .map_err(KernelError::from)?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let write_context = Arc::new(txn.write_state()?.unpartitioned_write_context()?);
        let tasks = append_data.into_iter().map(|data| {
            // arc clones
            let engine = engine.clone();
            let write_context = write_context.clone();
            tokio::task::spawn(async move {
                engine
                    .write_parquet(data.as_ref().unwrap(), write_context.as_ref())
                    .await
            })
        });

        let mut add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
        assert!(add_files_metadata.all(|res| match res {
            Err(delta_kernel::Error::Kernel(KernelError::Arrow(
                ArrowError::InvalidArgumentError(_),
            ))) => true,
            Err(delta_kernel::Error::Kernel(KernelError::Backtraced { source, .. }))
                if matches!(
                    &*source,
                    KernelError::Arrow(ArrowError::InvalidArgumentError(_))
                ) =>
                true,
            _ => false,
        }));
    }
    Ok(())
}

#[tokio::test]
async fn commit_rejects_add_missing_required_field() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for field in ["path", "partitionValues", "size", "modificationTime"] {
        let (table_url, engine, _store, _table_name) =
            setup_test_tables(schema.clone(), &[], None, "required_field_table")
                .await?
                .into_iter()
                .next()
                .expect("at least one test table");
        let engine = Arc::new(engine);
        let mut txn =
            load_and_begin_transaction(table_url, engine.as_ref())?.with_data_change(true);

        let data = ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?);
        let write_context = txn.write_state()?.unpartitioned_write_context()?;

        // Corrupt the addFile at the second batch.
        let valid_meta = engine.write_parquet(&data, &write_context).await?;
        txn.add_files(valid_meta);
        let to_be_corrupted_meta = engine.write_parquet(&data, &write_context).await?;

        let batch = into_record_batch(to_be_corrupted_meta);
        let index = batch.schema().index_of(field)?;

        // The add-metadata schema declares these fields non-nullable, so rebuild the schema with
        // the target field made nullable before inserting a null column.
        let mut fields: Vec<ArrowField> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields[index] = fields[index].clone().with_nullable(true);
        let nullable_schema = Arc::new(ArrowSchema::new(fields));
        let mut columns = batch.columns().to_vec();
        columns[index] = new_null_array(nullable_schema.field(index).data_type(), batch.num_rows());
        let corrupted = RecordBatch::try_new(nullable_schema, columns)?;
        txn.add_files(Box::new(ArrowEngineData::new(corrupted)));

        let err = txn
            .commit(engine.as_ref())
            .expect_err(&format!(
                "commit should reject an add missing required field '{field}'"
            ))
            .to_string();
        assert!(
            err.contains(&format!("missing required field '{field}'")),
            "field {field}: unexpected error {err:?}"
        );
    }
    Ok(())
}

#[rstest]
#[case::missing(None, &[AddFilePartitionKeyModify::Drop { key: "p2" }])]
#[case::extra(None, &[AddFilePartitionKeyModify::Insert {
    key: "p3",
    value: Some("extra"),
}])]
#[case::incorrect_name(None, &[
    AddFilePartitionKeyModify::Drop { key: "p2" },
    AddFilePartitionKeyModify::Insert {
        key: "partition_2",
        value: Some("6"),
    },
])]
#[case::logical_partition_name_when_cm_name_mode(
    Some("name"),
    &[
        AddFilePartitionKeyModify::Drop { key: "p2" },
        AddFilePartitionKeyModify::Insert { key: "p2", value: Some("6") },
    ],
)]
#[case::logical_partition_name_when_cm_id_mode(
    Some("id"),
    &[
        AddFilePartitionKeyModify::Drop { key: "p2" },
        AddFilePartitionKeyModify::Insert { key: "p2", value: Some("6") },
    ],
)]
#[tokio::test(flavor = "multi_thread")]
async fn commit_rejects_add_with_invalid_partition_keys(
    #[case] column_mapping_mode: Option<&str>,
    #[case] modifications: &[AddFilePartitionKeyModify<'_>],
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let table_schema = schema_ref! {
        nullable "d": INTEGER,
        nullable "p1": STRING,
        nullable "p2": INTEGER,
    };
    let (_tmp_dir, table_path, engine) = test_utils::test_table_setup_mt()?;
    let mut builder = create_table(&table_path, table_schema, "test/1.0")
        .with_data_layout(DataLayout::partitioned(["p1", "p2"]));
    if let Some(mode) = column_mapping_mode {
        builder = builder.with_table_properties([("delta.columnMapping.mode", mode)]);
    }
    builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let data_schema = schema! { nullable "d": INTEGER };
    let data_schema: Arc<ArrowSchema> =
        Arc::new((&data_schema).try_into_arrow().map_err(KernelError::from)?);
    let make_add = |write_state: &Arc<WriteState>, p1: &str, p2: i32| {
        let wc = write_state.partitioned_write_context(HashMap::from([
            ("p1".to_string(), Scalar::String(p1.into())),
            ("p2".to_string(), Scalar::Integer(p2)),
        ]))?;
        let data = RecordBatch::try_new(
            data_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .map_err(KernelError::from)?;
        futures::executor::block_on(engine.write_parquet(&ArrowEngineData::new(data), &wc))
    };

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let mode = snapshot
        .table_properties()
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    let logical_schema = snapshot.schema();
    // Translate the `modifications` to the physical partition column names.
    let modifications: Vec<_> = modifications
        .iter()
        .map(|modification| match *modification {
            AddFilePartitionKeyModify::Drop { key } => {
                let key = logical_schema
                    .field(key)
                    .map(|field| field.physical_name(mode))
                    .unwrap_or(key);
                AddFilePartitionKeyModify::Drop { key }
            }
            insertion => insertion,
        })
        .collect();
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);
    let write_state = txn.write_state()?;
    let add = make_add(&write_state, "b", 6)?;
    let corrupted = modify_add_file_partition_keys(into_record_batch(add), &modifications);
    txn.add_files(Box::new(ArrowEngineData::new(corrupted)));
    assert_result_error_with_message(txn.commit(engine.as_ref()), "partitionValues keys");
    Ok(())
}
