//! Integration tests for basic append write paths (single writes, repeated writes,
//! partitioned appends, and schema-mismatch rejection).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, StringArray};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{DeltaResult, Error as KernelError, Snapshot};
use itertools::Itertools;
use serde_json::{json, Deserializer};
use test_utils::{set_json_value, setup_test_tables, test_read};

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
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Arc::new(FileSystemCommitter::new()), &engine)?
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

#[tokio::test]
async fn test_append_partitioned() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    let partition_col = "partition";

    // create a simple partitioned table: one int column named 'number', partitioned by string
    // column named 'partition'
    let table_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("number", DataType::INTEGER),
        StructField::nullable("partition", DataType::STRING),
    ])?);
    let data_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, store, table_name) in
        setup_test_tables(table_schema.clone(), &[partition_col], None, "test_table").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let mut txn = snapshot
            .transaction(Arc::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine")
            .with_data_change(false);

        // create two new arrow record batches to append
        let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(data_schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });
        let partition_vals = vec!["a", "b"];

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let tasks = append_data
            .into_iter()
            .zip(partition_vals)
            .map(|(data, partition_val)| {
                let write_context = Arc::new(
                    txn.partitioned_write_context(HashMap::from([(
                        partition_col.to_string(),
                        Scalar::String(partition_val.into()),
                    )]))
                    .unwrap(),
                );
                // arc clones
                let engine = engine.clone();
                tokio::task::spawn(async move {
                    engine
                        .write_parquet(data.as_ref().unwrap(), write_context.as_ref())
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
    let table_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);
    // incompatible data schema: one string column named 'string'
    let data_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "string",
        DataType::STRING,
    )])?);

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(table_schema, &[], None, "test_table").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Arc::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("default engine");

        // create two new arrow record batches to append
        let append_data = [["a", "b"], ["c", "d"]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(data_schema.as_ref().try_into_arrow()?),
                vec![Arc::new(StringArray::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());
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
            Err(KernelError::Arrow(ArrowError::InvalidArgumentError(_))) => true,
            Err(KernelError::Backtraced { source, .. })
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
