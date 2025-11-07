use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::Error as KernelError;
use delta_kernel::{DeltaResult, Engine, Snapshot, Version};
use url::Url;
use uuid::Uuid;

use delta_kernel::arrow::array::{ArrayRef, BinaryArray, StructArray};
use delta_kernel::arrow::array::{Int32Array, StringArray, TimestampMicrosecondArray};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::record_batch::RecordBatch;

use delta_kernel::engine::arrow_conversion::{TryFromKernel, TryIntoArrow as _};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::engine::default::DefaultEngine;

use delta_kernel::transaction::CommitResult;
use tempfile::TempDir;

use test_utils::set_json_value;

use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::json;
use serde_json::Deserializer;
use tempfile::tempdir;

use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

use test_utils::{
    assert_result_error_with_message, create_table, engine_store_setup, setup_test_tables,
    test_read,
};

mod common;

fn validate_txn_id(commit_info: &serde_json::Value) {
    let txn_id = commit_info["txnId"]
        .as_str()
        .expect("txnId should be present in commitInfo");
    Uuid::parse_str(txn_id).expect("txnId should be valid UUID format");
}

const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

#[tokio::test]
async fn test_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // create a transaction
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let committer = Box::new(FileSystemCommitter::new());
        let txn = snapshot
            .transaction(committer)?
            .with_engine_info("default engine");

        // commit!
        let _ = txn.commit(&engine)?;

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commit: serde_json::Value = serde_json::from_slice(&commit1.bytes().await?)?;

        validate_txn_id(&parsed_commit["commitInfo"]);

        set_json_value(&mut parsed_commit, "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commit, "commitInfo.txnId", json!(ZERO_UUID))?;

        let expected_commit = json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineInfo": "default engine",
                "txnId": ZERO_UUID,
            }
        });

        assert_eq!(parsed_commit, expected_commit);
    }
    Ok(())
}

// check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
fn check_action_timestamps<'a>(
    parsed_commits: impl Iterator<Item = &'a serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()
        .unwrap();

    parsed_commits.for_each(|commit| {
        if let Some(commit_info_ts) = &commit.pointer("/commitInfo/timestamp") {
            assert!((now - commit_info_ts.as_i64().unwrap()).abs() < 10_000);
        }
        if let Some(add_ts) = &commit.pointer("/add/modificationTime") {
            assert!((now - add_ts.as_i64().unwrap()).abs() < 10_000);
        }
    });

    Ok(())
}

// list all the files at `path` and check that all parquet files have the same size, and return
// that size
async fn get_and_check_all_parquet_sizes(store: Arc<dyn ObjectStore>, path: &str) -> u64 {
    use futures::stream::StreamExt;
    let files: Vec<_> = store.list(Some(&Path::from(path))).collect().await;
    let parquet_files = files
        .into_iter()
        .filter(|f| match f {
            Ok(f) => f.location.extension() == Some("parquet"),
            Err(_) => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(parquet_files.len(), 2);
    let size = parquet_files.first().unwrap().as_ref().unwrap().size;
    assert!(parquet_files
        .iter()
        .all(|f| f.as_ref().unwrap().size == size));
    size
}

async fn write_data_and_check_result_and_stats(
    table_url: Url,
    schema: SchemaRef,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    expected_since_commit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot.transaction(committer)?.with_data_change(true);

    // create two new arrow record batches to append
    let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(data.to_vec()))],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    });

    // write data out by spawning async tasks to simulate executors
    let write_context = Arc::new(txn.get_write_context());
    let tasks = append_data.into_iter().map(|data| {
        // arc clones
        let engine = engine.clone();
        let write_context = write_context.clone();
        tokio::task::spawn(async move {
            engine
                .write_parquet(
                    data.as_ref().unwrap(),
                    write_context.as_ref(),
                    HashMap::new(),
                )
                .await
        })
    });

    let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
    for meta in add_files_metadata {
        txn.add_files(meta?);
    }

    // commit!
    match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), expected_since_commit as Version);
            assert_eq!(
                committed.post_commit_stats().commits_since_checkpoint,
                expected_since_commit
            );
            assert_eq!(
                committed.post_commit_stats().commits_since_log_compaction,
                expected_since_commit
            );
        }
        _ => panic!("Commit should have succeeded"),
    };

    Ok(())
}

#[tokio::test]
async fn test_commit_info_action() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_engine_info("default engine");

        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        validate_txn_id(&parsed_commits[0]["commitInfo"]);

        // set timestamps to 0, paths and txn_id to known string values for comparison
        // (otherwise timestamps are non-deterministic, paths and txn_id are random UUIDs)
        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;

        let expected_commit = vec![json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineInfo": "default engine",
                "txnId": ZERO_UUID
            }
        })];

        assert_eq!(parsed_commits, expected_commit);
    }
    Ok(())
}

#[tokio::test]
async fn test_append() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

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
        // check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
        // before we clear them for comparison
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
                    "stats": "{\"numRecords\":3}"
                }
            }),
            json!({
                "add": {
                    "path": "second.parquet",
                    "partitionValues": {},
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true,
                    "stats": "{\"numRecords\":3}"
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
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()))?
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
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

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
            .transaction(Box::new(FileSystemCommitter::new()))?
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
        let write_context = Arc::new(txn.get_write_context());
        let tasks = append_data
            .into_iter()
            .zip(partition_vals)
            .map(|(data, partition_val)| {
                // arc clones
                let engine = engine.clone();
                let write_context = write_context.clone();
                tokio::task::spawn(async move {
                    engine
                        .write_parquet(
                            data.as_ref().unwrap(),
                            write_context.as_ref(),
                            HashMap::from([(partition_col.to_string(), partition_val.to_string())]),
                        )
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
        // check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
        // before we clear them for comparison
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
                    "stats": "{\"numRecords\":3}"
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
                    "stats": "{\"numRecords\":3}"
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
            .transaction(Box::new(FileSystemCommitter::new()))?
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
        let write_context = Arc::new(txn.get_write_context());
        let tasks = append_data.into_iter().map(|data| {
            // arc clones
            let engine = engine.clone();
            let write_context = write_context.clone();
            tokio::task::spawn(async move {
                engine
                    .write_parquet(
                        data.as_ref().unwrap(),
                        write_context.as_ref(),
                        HashMap::new(),
                    )
                    .await
            })
        });

        let mut add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
        assert!(add_files_metadata.all(|res| match res {
            Err(KernelError::Arrow(ArrowError::SchemaError(_))) => true,
            Err(KernelError::Backtraced { source, .. })
                if matches!(&*source, KernelError::Arrow(ArrowError::SchemaError(_))) =>
                true,
            _ => false,
        }));
    }
    Ok(())
}

#[tokio::test]
async fn test_write_txn_actions() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // can't have duplicate app_id in same transaction
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert!(matches!(
            snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_transaction_id("app_id1".to_string(), 0)
                .with_transaction_id("app_id1".to_string(), 1)
                .commit(&engine),
            Err(KernelError::Generic(msg)) if msg == "app_id app_id1 already exists in transaction"
        ));

        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_engine_info("default engine")
            .with_transaction_id("app_id1".to_string(), 1)
            .with_transaction_id("app_id2".to_string(), 2);

        // commit!
        assert!(txn.commit(&engine)?.is_committed());

        let snapshot = Snapshot::builder_for(table_url.clone())
            .at_version(1)
            .build(&engine)?;
        assert_eq!(
            snapshot.clone().get_app_id_version("app_id1", &engine)?,
            Some(1)
        );
        assert_eq!(
            snapshot.clone().get_app_id_version("app_id2", &engine)?,
            Some(2)
        );
        assert_eq!(
            snapshot.clone().get_app_id_version("app_id3", &engine)?,
            None
        );

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0)).unwrap();

        let time_ms: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()
            .unwrap();

        // check that last_updated times are identical
        let last_updated1 = parsed_commits[1]
            .get("txn")
            .unwrap()
            .get("lastUpdated")
            .unwrap();
        let last_updated2 = parsed_commits[2]
            .get("txn")
            .unwrap()
            .get("lastUpdated")
            .unwrap();
        assert_eq!(last_updated1, last_updated2);

        let last_updated = parsed_commits[1]
            .get_mut("txn")
            .unwrap()
            .get_mut("lastUpdated")
            .unwrap();
        // sanity check that last_updated time is within 10s of now
        assert!((last_updated.as_i64().unwrap() - time_ms).abs() < 10_000);
        *last_updated = serde_json::Value::Number(1.into());

        let last_updated = parsed_commits[2]
            .get_mut("txn")
            .unwrap()
            .get_mut("lastUpdated")
            .unwrap();
        // sanity check that last_updated time is within 10s of now
        assert!((last_updated.as_i64().unwrap() - time_ms).abs() < 10_000);
        *last_updated = serde_json::Value::Number(2.into());

        validate_txn_id(&parsed_commits[0]["commitInfo"]);

        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;

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
                "txn": {
                    "appId": "app_id1",
                    "version": 1,
                    "lastUpdated": 1
                }
            }),
            json!({
                "txn": {
                    "appId": "app_id2",
                    "version": 2,
                    "lastUpdated": 2
                }
            }),
        ];

        assert_eq!(parsed_commits, expected_commit);
    }
    Ok(())
}

#[tokio::test]
async fn test_append_timestamp_ntz() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a table with TIMESTAMP_NTZ column
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "ts_ntz",
        DataType::TIMESTAMP_NTZ,
    )])?);

    let (store, engine, table_location) = engine_store_setup("test_table_timestamp_ntz", None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec!["timestampNtz"],
        vec!["timestampNtz"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("default engine");

    // Create Arrow data with TIMESTAMP_NTZ values including edge cases
    // These are microseconds since Unix epoch
    let timestamp_values = vec![
        0i64,                  // Unix epoch (1970-01-01T00:00:00.000000)
        1634567890123456i64,   // 2021-10-18T12:31:30.123456
        1634567950654321i64,   // 2021-10-18T12:32:30.654321
        1672531200000000i64,   // 2023-01-01T00:00:00.000000
        253402300799999999i64, // 9999-12-31T23:59:59.999999 (near max valid timestamp)
        -62135596800000000i64, // 0001-01-01T00:00:00.000000 (near min valid timestamp)
    ];

    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(TimestampMicrosecondArray::from(timestamp_values))],
    )?;

    // Write data
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());

    let add_files_metadata = engine
        .write_parquet(
            &ArrowEngineData::new(data.clone()),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Verify the commit was written correctly
    let commit1 = store
        .get(&Path::from(
            "/test_table_timestamp_ntz/_delta_log/00000000000000000001.json",
        ))
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());
    // Ensure default of data change is true.
    assert!(parsed_commits[1]
        .get("add")
        .unwrap()
        .get("dataChange")
        .unwrap()
        .as_bool()
        .unwrap());

    // Verify the data can be read back correctly
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}

#[tokio::test]
async fn test_append_variant() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    fn unshredded_variant_schema_flipped() -> DataType {
        DataType::variant_type([
            StructField::not_null("value", DataType::BINARY),
            StructField::not_null("metadata", DataType::BINARY),
        ])
        .unwrap()
    }
    fn variant_arrow_type_flipped() -> ArrowDataType {
        let metadata_field = Field::new("metadata", ArrowDataType::Binary, false);
        let value_field = Field::new("value", ArrowDataType::Binary, false);
        let fields = vec![value_field, metadata_field];
        ArrowDataType::Struct(fields.into())
    }

    // create a table with VARIANT column
    let table_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("v", DataType::unshredded_variant()),
        StructField::nullable("i", DataType::INTEGER),
        StructField::nullable(
            "nested",
            // We flip the value and metadata fields in the actual parquet file for the test
            StructType::try_new(vec![StructField::nullable(
                "nested_v",
                unshredded_variant_schema_flipped(),
            )])?,
        ),
    ])?);

    let write_schema = table_schema.clone();

    let tmp_test_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_table_variant", Some(&tmp_test_dir_url));

    // We can add shredding features as well as we are allowed to write unshredded variants
    // into shredded tables and shredded reads are explicitly blocked in the default
    // engine's parquet reader.
    let table_url = create_table(
        store.clone(),
        table_location,
        table_schema.clone(),
        &[],
        true,
        vec!["variantType", "variantShredding-preview"],
        vec!["variantType", "variantShredding-preview"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_data_change(true);

    // First value corresponds to the variant value "1". Third value corresponds to the variant
    // representing the JSON Object {"a":2}.
    let metadata_v = vec![
        Some(&[0x01, 0x00, 0x00][..]),
        None,
        Some(&[0x01, 0x01, 0x00, 0x01, 0x61][..]),
    ];
    let value_v = vec![
        Some(&[0x0C, 0x01][..]),
        None,
        Some(&[0x02, 0x01, 0x00, 0x00, 0x01, 0x02][..]),
    ];

    let metadata_v_array = Arc::new(BinaryArray::from(metadata_v)) as ArrayRef;
    let value_v_array = Arc::new(BinaryArray::from(value_v)) as ArrayRef;

    // First value corresponds to the variant value "2". Third value corresponds to the variant
    // representing the JSON Object {"b":3}.
    let metadata_nested_v = vec![
        Some(&[0x01, 0x00, 0x00][..]),
        None,
        Some(&[0x01, 0x01, 0x00, 0x01, 0x62][..]),
    ];
    let value_nested_v = vec![
        Some(&[0x0C, 0x02][..]),
        None,
        Some(&[0x02, 0x01, 0x00, 0x00, 0x01, 0x03][..]),
    ];

    let value_nested_v_array = Arc::new(BinaryArray::from(value_nested_v)) as ArrayRef;
    let metadata_nested_v_array = Arc::new(BinaryArray::from(metadata_nested_v)) as ArrayRef;

    let variant_arrow = ArrowDataType::try_from_kernel(&DataType::unshredded_variant()).unwrap();
    let variant_arrow_flipped = variant_arrow_type_flipped();

    let i_values = vec![31, 32, 33];

    let fields = match variant_arrow {
        ArrowDataType::Struct(fields) => Ok(fields),
        _ => Err(KernelError::Generic(
            "Variant arrow data type is not struct.".to_string(),
        )),
    }?;
    let fields_flipped = match variant_arrow_flipped {
        ArrowDataType::Struct(fields) => Ok(fields),
        _ => Err(KernelError::Generic(
            "Variant arrow data type is not struct.".to_string(),
        )),
    }?;

    let null_bitmap = NullBuffer::from_iter([true, false, true]);

    let variant_v_array = StructArray::try_new(
        fields.clone(),
        vec![metadata_v_array, value_v_array],
        Some(null_bitmap.clone()),
    )?;

    let variant_nested_v_array = Arc::new(StructArray::try_new(
        fields_flipped.clone(),
        vec![
            value_nested_v_array.clone(),
            metadata_nested_v_array.clone(),
        ],
        Some(null_bitmap.clone()),
    )?);

    let data = RecordBatch::try_new(
        Arc::new(write_schema.as_ref().try_into_arrow()?),
        vec![
            // v variant
            Arc::new(variant_v_array.clone()),
            // i int
            Arc::new(Int32Array::from(i_values.clone())),
            // nested struct<nested_v variant>
            Arc::new(StructArray::try_new(
                vec![Field::new("nested_v", variant_arrow_type_flipped(), true)].into(),
                vec![variant_nested_v_array.clone()],
                None,
            )?),
        ],
    )
    .unwrap();

    // Write data
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());

    let add_files_metadata = (*engine)
        .parquet_handler()
        .as_any()
        .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
        .unwrap()
        .write_parquet_file(
            write_context.target_dir(),
            Box::new(ArrowEngineData::new(data.clone())),
            HashMap::new(),
        )
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Verify the commit was written correctly
    let commit1_url = tmp_test_dir_url
        .join("test_table_variant/_delta_log/00000000000000000001.json")
        .unwrap();
    let commit1 = store
        .get(&Path::from_url_path(commit1_url.path()).unwrap())
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());

    // The scanned data will match the logical schema, not the physical one
    let expected_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("v", DataType::unshredded_variant()),
        StructField::nullable("i", DataType::INTEGER),
        StructField::nullable(
            "nested",
            StructType::try_new(vec![StructField::nullable(
                "nested_v",
                DataType::unshredded_variant(),
            )])
            .unwrap(),
        ),
    ])?);

    // During the read, the flipped fields should be reordered into metadata, value.
    let variant_nested_v_array_expected = Arc::new(StructArray::try_new(
        fields,
        vec![metadata_nested_v_array, value_nested_v_array],
        Some(null_bitmap),
    )?);
    let variant_arrow_type: ArrowDataType =
        ArrowDataType::try_from_kernel(&DataType::unshredded_variant()).unwrap();
    let expected_data = RecordBatch::try_new(
        Arc::new(expected_schema.as_ref().try_into_arrow()?),
        vec![
            // v variant
            Arc::new(variant_v_array),
            // i int
            Arc::new(Int32Array::from(i_values)),
            // nested struct<nested_v variant>
            Arc::new(StructArray::try_new(
                vec![Field::new("nested_v", variant_arrow_type, true)].into(),
                vec![variant_nested_v_array_expected],
                None,
            )?),
        ],
    )
    .unwrap();

    test_read(&ArrowEngineData::new(expected_data), &table_url, engine)?;

    Ok(())
}

#[tokio::test]
async fn test_shredded_variant_read_rejection() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure that shredded variants are rejected by the default engine's parquet reader

    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    let table_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "v",
        DataType::unshredded_variant(),
    )])?);

    // The table will be attempted to be written in this form but be read into
    // STRUCT<metadata: BINARY, value: BINARY>. The read should fail because the default engine
    // currently does not support shredded reads.
    let shredded_write_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "v",
        DataType::try_struct_type([
            StructField::new("metadata", DataType::BINARY, true),
            StructField::new("value", DataType::BINARY, true),
            StructField::new("typed_value", DataType::INTEGER, true),
        ])?,
    )])?);

    let tmp_test_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_table_variant_2", Some(&tmp_test_dir_url));
    let table_url = create_table(
        store.clone(),
        table_location,
        table_schema.clone(),
        &[],
        true,
        vec!["variantType", "variantShredding-preview"],
        vec!["variantType", "variantShredding-preview"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_data_change(true);

    // First value corresponds to the variant value "1". Third value corresponds to the variant
    // representing the JSON Object {"a":2}.
    let metadata_v = vec![
        Some(&[0x01, 0x00, 0x00][..]),
        Some(&[0x01, 0x01, 0x00, 0x01, 0x61][..]),
    ];
    let value_v = vec![
        Some(&[0x0C, 0x01][..]),
        Some(&[0x02, 0x01, 0x00, 0x00, 0x01, 0x02][..]),
    ];
    let typed_value_v = vec![Some(21), Some(3)];

    let metadata_v_array = Arc::new(BinaryArray::from(metadata_v)) as ArrayRef;
    let value_v_array = Arc::new(BinaryArray::from(value_v)) as ArrayRef;
    let typed_value_v_array = Arc::new(Int32Array::from(typed_value_v)) as ArrayRef;

    let variant_arrow = ArrowDataType::Struct(
        vec![
            Field::new("metadata", ArrowDataType::Binary, true),
            Field::new("value", ArrowDataType::Binary, true),
            Field::new("typed_value", ArrowDataType::Int32, true),
        ]
        .into(),
    );

    let fields = match variant_arrow {
        ArrowDataType::Struct(fields) => Ok(fields),
        _ => Err(KernelError::Generic(
            "Variant arrow data type is not struct.".to_string(),
        )),
    }?;

    let variant_v_array = StructArray::try_new(
        fields.clone(),
        vec![metadata_v_array, value_v_array, typed_value_v_array],
        None,
    )?;

    let data = RecordBatch::try_new(
        Arc::new(shredded_write_schema.as_ref().try_into_arrow()?),
        vec![
            // v variant
            Arc::new(variant_v_array.clone()),
        ],
    )
    .unwrap();

    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());

    let add_files_metadata = (*engine)
        .parquet_handler()
        .as_any()
        .downcast_ref::<DefaultParquetHandler<TokioBackgroundExecutor>>()
        .unwrap()
        .write_parquet_file(
            write_context.target_dir(),
            Box::new(ArrowEngineData::new(data.clone())),
            HashMap::new(),
        )
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Verify the commit was written correctly
    let commit1_url = tmp_test_dir_url
        .join("test_table_variant_2/_delta_log/00000000000000000001.json")
        .unwrap();
    let commit1 = store
        .get(&Path::from_url_path(commit1_url.path()).unwrap())
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());

    let res = test_read(&ArrowEngineData::new(data), &table_url, engine);
    assert!(matches!(res,
        Err(e) if e.to_string().contains("The default engine does not support shredded reads")));

    Ok(())
}

#[tokio::test]
async fn test_set_domain_metadata_basic() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_domain_metadata_basic";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()))?;

    // write context does not conflict with domain metadata
    let _write_context = txn.get_write_context();

    // set multiple domain metadata
    let domain1 = "app.config";
    let config1 = r#"{"version": 1}"#;
    let domain2 = "spark.settings";
    let config2 = r#"{"cores": 4}"#;

    assert!(txn
        .with_domain_metadata(domain1.to_string(), config1.to_string())
        .with_domain_metadata(domain2.to_string(), config2.to_string())
        .commit(&engine)?
        .is_committed());

    let commit_data = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000001.json"
        )))
        .await?
        .bytes()
        .await?;

    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_data)
        .into_iter()
        .try_collect()?;

    let domain_actions: Vec<_> = actions
        .iter()
        .filter(|v| v.get("domainMetadata").is_some())
        .collect();

    for action in &domain_actions {
        let domain = action["domainMetadata"]["domain"].as_str().unwrap();
        let config = action["domainMetadata"]["configuration"].as_str().unwrap();
        assert!(!action["domainMetadata"]["removed"].as_bool().unwrap());

        match domain {
            d if d == domain1 => assert_eq!(config, config1),
            d if d == domain2 => assert_eq!(config, config2),
            _ => panic!("Unexpected domain: {}", domain),
        }
    }

    let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let domain1_config = final_snapshot.get_domain_metadata(domain1, &engine)?;
    assert_eq!(domain1_config, Some(config1.to_string()));
    let domain2_config = final_snapshot.get_domain_metadata(domain2, &engine)?;
    assert_eq!(domain2_config, Some(config2.to_string()));
    Ok(())
}

#[tokio::test]
async fn test_set_domain_metadata_errors() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_domain_metadata_errors";
    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // System domain rejection
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?;
    let res = txn
        .with_domain_metadata("delta.system".to_string(), "config".to_string())
        .commit(&engine);
    assert_result_error_with_message(
        res,
        "Cannot modify domains that start with 'delta.' as those are system controlled",
    );

    // Duplicate domain rejection
    let txn2 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?;
    let res = txn2
        .with_domain_metadata("app.config".to_string(), "v1".to_string())
        .with_domain_metadata("app.config".to_string(), "v2".to_string())
        .commit(&engine);
    assert_result_error_with_message(
        res,
        "Metadata for domain app.config already specified in this transaction",
    );

    Ok(())
}

#[tokio::test]
async fn test_set_domain_metadata_unsupported_writer_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_domain_metadata_unsupported";

    // Create table WITHOUT domain metadata writer feature support
    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec![],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let res = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata("app.config".to_string(), "test_config".to_string())
        .commit(&engine);

    assert_result_error_with_message(res, "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature");

    Ok(())
}

#[tokio::test]
async fn test_remove_domain_metadata_unsupported_writer_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_remove_domain_metadata_unsupported";

    // Create table WITHOUT domain metadata writer feature support
    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec![],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let res = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata_removed("app.config".to_string())
        .commit(&engine);

    assert_result_error_with_message(res, "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature");

    Ok(())
}

#[tokio::test]
async fn test_remove_domain_metadata_non_existent_domain() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_domain_metadata_unsupported";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()))?;

    let domain = "app.deprecated";

    // removing domain metadata that doesn't exist should NOT write a tombstone
    let _ = txn
        .with_domain_metadata_removed(domain.to_string())
        .commit(&engine)?;

    let commit_data = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000001.json"
        )))
        .await?
        .bytes()
        .await?;
    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_data)
        .into_iter()
        .try_collect()?;

    let domain_action = actions.iter().find(|v| v.get("domainMetadata").is_some());
    assert!(
        domain_action.is_none(),
        "No tombstone should be written for non-existent domain"
    );

    let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let config = final_snapshot.get_domain_metadata(domain, &engine)?;
    assert_eq!(config, None);

    Ok(())
}

#[tokio::test]
async fn test_domain_metadata_set_remove_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_domain_metadata_unsupported";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // set then remove same domain
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?;
    let err = txn
        .with_domain_metadata("app.config".to_string(), "v1".to_string())
        .with_domain_metadata_removed("app.config".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("already specified in this transaction"));

    // remove then set same domain
    let txn2 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?;
    let err = txn2
        .with_domain_metadata_removed("test.domain".to_string())
        .with_domain_metadata("test.domain".to_string(), "v1".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("already specified in this transaction"));

    // remove same domain twice
    let txn3 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?;
    let err = txn3
        .with_domain_metadata_removed("another.domain".to_string())
        .with_domain_metadata_removed("another.domain".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("already specified in this transaction"));

    // remove system domain
    let txn4 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?;
    let err = txn4
        .with_domain_metadata_removed("delta.system".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Cannot modify domains that start with 'delta.' as those are system controlled"));

    Ok(())
}

#[tokio::test]
async fn test_domain_metadata_set_then_remove() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let table_name = "test_domain_metadata_unsupported";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let domain = "app.config";
    let configuration = r#"{"version": 1}"#;

    // txn 1: set domain metadata
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()))?;
    let _ = txn
        .with_domain_metadata(domain.to_string(), configuration.to_string())
        .commit(&engine)?;

    // txn 2: remove the same domain metadata
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()))?;
    let _ = txn
        .with_domain_metadata_removed(domain.to_string())
        .commit(&engine)?;

    // verify removal commit preserves the previous configuration
    let commit_data = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000002.json"
        )))
        .await?
        .bytes()
        .await?;
    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_data)
        .into_iter()
        .try_collect()?;

    let domain_action = actions
        .iter()
        .find(|v| v.get("domainMetadata").is_some())
        .unwrap();
    assert_eq!(domain_action["domainMetadata"]["domain"], domain);
    assert_eq!(
        domain_action["domainMetadata"]["configuration"],
        configuration
    );
    assert_eq!(domain_action["domainMetadata"]["removed"], true);

    // verify reads see the metadata removal
    let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let domain_config = final_snapshot.get_domain_metadata(domain, &engine)?;
    assert_eq!(domain_config, None);

    Ok(())
}

async fn get_ict_at_version(
    store: Arc<dyn ObjectStore>,
    table_url: &Url,
    version: u64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let commit_path = table_url.join(&format!("_delta_log/{:020}.json", version))?;
    let commit = store.get(&Path::from_url_path(commit_path.path())?).await?;
    let commit_content = String::from_utf8(commit.bytes().await?.to_vec())?;

    // Parse each line of the commit log (NDJSON format)
    // CommitInfo MUST be the first action when ICT is enabled
    let lines: Vec<_> = commit_content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert!(
        !lines.is_empty(),
        "Commit log at version {} should not be empty",
        version
    );

    // First line should contain commitInfo with inCommitTimestamp
    let first_action: serde_json::Value = serde_json::from_str(lines[0])?;
    let commit_info = first_action
        .get("commitInfo")
        .expect("First action must be commitInfo when ICT is enabled");
    let ict = commit_info
        .get("inCommitTimestamp")
        .expect("commitInfo must have inCommitTimestamp when ICT is enabled")
        .as_i64()
        .unwrap();
    Ok(ict)
}

/// Helper function to generate a simple data file and add it to the transaction
/// This simplifies repetitive data generation in tests
async fn generate_and_add_data_file(
    txn: &mut delta_kernel::transaction::Transaction,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(values))],
    )?;

    let write_context = Arc::new(txn.get_write_context());
    let file_meta = engine
        .write_parquet(
            &ArrowEngineData::new(data),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;
    txn.add_files(file_meta);
    Ok(())
}

#[tokio::test]
async fn test_ict_commit_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number' with ICT enabled
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let tmp_dir = TempDir::new()?;
    let tmp_test_dir_url = Url::from_file_path(&tmp_dir).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_ict_first_commit", Some(&tmp_test_dir_url));

    // Create table with ICT enabled (writer version 7)
    let table_url = test_utils::create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],                       // no partition columns
        true,                      // use protocol 3.7
        vec![],                    // no reader features
        vec!["inCommitTimestamp"], // Enable ICT! Note: table feature is also set.
    )
    .await?;

    // FIRST COMMIT: This exercises version() == 0 branch and generates ICT
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(
        snapshot.version(),
        0,
        "Initial snapshot should be version 0"
    );

    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("ict test");

    // Add some data
    generate_and_add_data_file(&mut txn, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // First commit
    let commit_result = txn.commit(&engine)?;
    match commit_result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(
                committed.commit_version(),
                1,
                "First commit should result in version 1"
            );
        }
        CommitResult::ConflictedTransaction(conflicted) => {
            panic!(
                "First commit should not conflict, got conflict at version {}",
                conflicted.conflict_version()
            );
        }
        CommitResult::RetryableTransaction(_) => {
            panic!("First commit should not be retryable error");
        }
    }

    // VERIFY: Check that the commit log contains inCommitTimestamp
    let first_ict = get_ict_at_version(store.clone(), &table_url, 1).await?;

    assert!(
        first_ict > 1612345678,
        "First commit ICT ({}) should be greater than enablement timestamp (1612345678)",
        first_ict
    );

    // Second commit
    let snapshot2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(
        snapshot2.version(),
        1,
        "Second snapshot should be version 1"
    );

    let mut txn2 = snapshot2
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("ict test 2");

    // Add more data
    generate_and_add_data_file(&mut txn2, &engine, schema, vec![4, 5, 6]).await?;

    // Second commit
    let commit_result2 = txn2.commit(&engine)?;
    match commit_result2 {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(
                committed.commit_version(),
                2,
                "Second commit should result in version 2"
            );
        }
        CommitResult::ConflictedTransaction(conflicted) => {
            panic!(
                "Second commit should not conflict, got conflict at version {}",
                conflicted.conflict_version()
            );
        }
        CommitResult::RetryableTransaction(_) => {
            panic!("Second commit should not be retryable error");
        }
    }

    // VERIFY: Check that second commit has proper monotonic ICT
    let second_ict = get_ict_at_version(store, &table_url, 2).await?;

    // Verify monotonic property: second_ict > first_ict
    assert!(
        second_ict > first_ict,
        "Second ICT ({}) should be greater than first ICT ({})",
        second_ict,
        first_ict
    );

    Ok(())
}
