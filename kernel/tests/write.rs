use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::Error as KernelError;
use delta_kernel::{DeltaResult, Engine, Snapshot, Version};
use url::Url;
use uuid::Uuid;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
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
use delta_kernel::engine_data::FilteredEngineData;
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
    assert_result_error_with_message, copy_directory, create_add_files_metadata,
    create_default_engine, create_table, engine_store_setup, setup_test_tables, test_read,
};

mod common;

fn validate_txn_id(commit_info: &serde_json::Value) {
    let txn_id = commit_info["txnId"]
        .as_str()
        .expect("txnId should be present in commitInfo");
    Uuid::parse_str(txn_id).expect("txnId should be valid UUID format");
}

const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

/// Creates a table with deletion vector support and writes the specified files
async fn create_dv_table_with_files(
    table_name: &str,
    schema: Arc<StructType>,
    file_paths: &[&str],
) -> Result<
    (
        Arc<dyn ObjectStore>,
        Arc<dyn delta_kernel::Engine>,
        Url,
        Vec<String>,
    ),
    Box<dyn std::error::Error>,
> {
    let (store, engine, table_url) = engine_store_setup(table_name, None);
    let engine = Arc::new(engine);

    // Create table with DV support (protocol 3/7 with deletionVectors feature)
    create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true, // use_37_protocol
        vec!["deletionVectors"],
        vec!["deletionVectors"],
    )
    .await?;

    // Write files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test engine")
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    let add_files_schema = txn.add_files_schema();

    // Build metadata for all files at once
    let files: Vec<(&str, i64, i64, i64)> = file_paths
        .iter()
        .enumerate()
        .map(|(i, &path)| {
            (
                path,
                1024 + i as i64 * 100, // size
                1000000 + i as i64,    // mod_time
                3,                     // num_records
            )
        })
        .collect();
    let metadata = create_add_files_metadata(add_files_schema, files)?;
    txn.add_files(metadata);

    let _ = txn.commit(engine.as_ref())?;

    let paths: Vec<String> = file_paths.iter().map(|&s| s.to_string()).collect();
    Ok((store, engine, table_url, paths))
}

/// Extracts scan files from a snapshot for use in deletion vector updates
fn get_scan_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Vec<FilteredEngineData>> {
    let scan = snapshot.scan_builder().build()?;
    let all_scan_metadata: Vec<_> = scan.scan_metadata(engine)?.collect::<Result<Vec<_>, _>>()?;

    Ok(all_scan_metadata
        .into_iter()
        .map(|sm| sm.scan_files)
        .collect())
}

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

#[tokio::test]
async fn test_remove_files_adds_expected_entries() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that Remove actions generated from scan metadata contain all expected fields
    // from the Remove struct (defined in kernel/src/actions/mod.rs).
    //
    // This test uses the table-with-dv-small dataset which contains files with tags and deletion vectors.
    //
    // Not populated in the dataset are (covered by row_tracking tests):
    // baseRowId (optional i64)
    // defaultRowCommitVersion (optional i64)
    use std::path::PathBuf;

    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let tmp_table_path = tmp_dir.path().join("table-with-dv-small");
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    copy_directory(&source_path, &tmp_table_path)?;

    let table_url = url::Url::from_directory_path(&tmp_table_path).unwrap();
    let engine = create_default_engine(&table_url)?;

    let snapshot = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test engine")
        .with_data_change(true);

    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;

    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    let remove_metadata = FilteredEngineData::try_new(data, selection_vector)?;

    txn.remove_files(remove_metadata);

    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the commit log directly to verify remove actions
            let commit_path =
                tmp_table_path.join(format!("_delta_log/{:020}.json", commit_version));
            let commit_content = std::fs::read_to_string(commit_path)?;

            let parsed_commits: Vec<_> = Deserializer::from_str(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Verify we have at least commitInfo and remove actions
            assert!(
                parsed_commits.len() >= 2,
                "Expected at least 2 actions (commitInfo + remove)"
            );

            // Extract the commitInfo timestamp to validate against deletionTimestamp
            let commit_info_action = parsed_commits
                .iter()
                .find(|action| action.get("commitInfo").is_some())
                .expect("Missing commitInfo action");
            let commit_info = &commit_info_action["commitInfo"];
            let commit_timestamp = commit_info["timestamp"]
                .as_i64()
                .expect("Missing timestamp in commitInfo");

            // Verify remove actions
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            assert!(
                !remove_actions.is_empty(),
                "Expected at least one remove action"
            );

            assert_eq!(remove_actions.len(), 1);
            let remove_action = remove_actions[0];
            let remove = &remove_action["remove"];

            // path (required)
            assert!(remove.get("path").is_some(), "Missing path field");
            let path = remove["path"].as_str().expect("path should be a string");
            assert_eq!(
                path,
                "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
            );

            // dataChange (required)
            assert_eq!(remove["dataChange"].as_bool(), Some(true));

            // deletionTimestamp (optional) - should match commit timestamp
            let deletion_timestamp = remove["deletionTimestamp"]
                .as_i64()
                .expect("Missing deletionTimestamp");
            assert_eq!(
                deletion_timestamp, commit_timestamp,
                "deletionTimestamp should match commit timestamp"
            );

            // extendedFileMetadata (optional)
            assert_eq!(remove["extendedFileMetadata"].as_bool(), Some(true));

            // partitionValues (optional)
            let partition_vals = remove["partitionValues"]
                .as_object()
                .expect("Missing partitionValues");
            assert_eq!(partition_vals.len(), 0);

            // size (optional)
            let size = remove["size"].as_i64().expect("Missing size");
            assert_eq!(size, 635);

            // stats (optional)
            let stats = remove["stats"].as_str().expect("Missing stats");
            let stats_json: serde_json::Value = serde_json::from_str(stats)?;
            assert_eq!(stats_json["numRecords"], 10);

            // tags (optional)
            let tags = remove["tags"].as_object().expect("Missing tags");
            assert_eq!(
                tags.get("INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("MIN_INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("MAX_INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("OPTIMIZE_TARGET_SIZE").and_then(|v| v.as_str()),
                Some("268435456")
            );

            // deletionVector (optional)
            let dv = remove["deletionVector"]
                .as_object()
                .expect("Missing deletionVector");
            assert_eq!(dv.get("storageType").and_then(|v| v.as_str()), Some("u"));
            assert_eq!(
                dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("vBn[lx{q8@P<9BNH/isA")
            );
            assert_eq!(dv.get("offset").and_then(|v| v.as_i64()), Some(1));
            assert_eq!(dv.get("sizeInBytes").and_then(|v| v.as_i64()), Some(36));
            assert_eq!(dv.get("cardinality").and_then(|v| v.as_i64()), Some(2));

            // Row tracking fields should be absent as the feature is was not enabled on writing
            // row_tracking tests cover having these populated.
            assert!(remove.get("baseRowId").is_none());
            assert!(remove.get("defaultRowCommitVersion").is_none());
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_update_deletion_vectors_adds_expected_entries(
) -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that deletion vector updates write proper Remove and Add actions
    // to the transaction log.
    //
    // NOTE: Additional unit tests for update_deletion_vectors exist in kernel/src/transaction/mod.rs
    //
    // The test validates:
    // 1. Transaction setup for DV updates
    // 2. Scanning and extracting scan files with DV data
    // 3. Creating new DV descriptors for the files
    // 4. Calling update_deletion_vectors to update the DVs
    // 5. Committing and verifying the generated actions
    //
    // Expected commit log structure:
    // - commitInfo: Contains metadata about the transaction
    // - remove: Contains OLD deletion vector data and original file metadata
    // - add: Contains NEW deletion vector data and updated file metadata
    //
    // The test ensures:
    // - Remove action has the OLD DV descriptor with all 5 fields
    // - Add action has the NEW DV descriptor with all 5 fields
    // - All file metadata is preserved (size, stats, tags, partitionValues)
    // - dataChange is properly set to true
    // - deletionTimestamp matches commit timestamp
    use std::path::PathBuf;

    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let tmp_table_path = tmp_dir.path().join("table-with-dv-small");
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    copy_directory(&source_path, &tmp_table_path)?;

    let table_url = url::Url::from_directory_path(&tmp_table_path).unwrap();
    let engine = create_default_engine(&table_url)?;

    let snapshot = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    // Create transaction with DV update mode enabled
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    // Build scan and collect all scan metadata
    let scan = snapshot.clone().scan_builder().build()?;
    let all_scan_metadata: Vec<_> = scan
        .scan_metadata(engine.as_ref())?
        .collect::<Result<Vec<_>, _>>()?;

    // Extract scan files for DV update
    let scan_files: Vec<_> = all_scan_metadata
        .into_iter()
        .map(|sm| sm.scan_files)
        .collect();

    // Create new DV descriptors for the files
    let file_path = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
    let mut dv_map = HashMap::new();

    // Create a NEW deletion vector descriptor (different from the original)
    let new_dv = DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: "cd^-aqEH.-t@S}K{vb[*k^".to_string(),
        offset: Some(10),
        size_in_bytes: 40,
        cardinality: 3,
    };
    dv_map.insert(file_path.to_string(), new_dv);

    // Call update_deletion_vectors to exercise the API
    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;

    // Commit the transaction
    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the original version 1 log to get original file metadata
            let original_log_path = tmp_table_path.join("_delta_log/00000000000000000001.json");
            let original_log_content = std::fs::read_to_string(original_log_path)?;
            let original_commits: Vec<_> = Deserializer::from_str(&original_log_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            let file_path = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";

            // Extract original file metadata from version 1
            let original_add = original_commits
                .iter()
                .find(|action| {
                    action
                        .get("add")
                        .and_then(|add| add.get("path").and_then(|p| p.as_str()))
                        == Some(file_path)
                })
                .expect("Missing original add action in version 1")
                .get("add")
                .expect("Should have add field");

            let original_size = original_add["size"]
                .as_i64()
                .expect("Original add action should have size");
            let original_partition_values = original_add["partitionValues"]
                .as_object()
                .expect("Original add action should have partitionValues");
            let original_tags = original_add.get("tags");
            let original_stats = original_add.get("stats");

            // Read the commit log directly
            let commit_path =
                tmp_table_path.join(format!("_delta_log/{:020}.json", commit_version));
            let commit_content = std::fs::read_to_string(commit_path)?;

            let parsed_commits: Vec<_> = Deserializer::from_str(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Should have commitInfo, remove, and add actions
            assert!(
                parsed_commits.len() >= 3,
                "Expected at least 3 actions (commitInfo + remove + add), got {}",
                parsed_commits.len()
            );

            // Extract commitInfo timestamp
            let commit_info_action = parsed_commits
                .iter()
                .find(|action| action.get("commitInfo").is_some())
                .expect("Missing commitInfo action");
            let commit_info = &commit_info_action["commitInfo"];
            let commit_timestamp = commit_info["timestamp"]
                .as_i64()
                .expect("Missing timestamp in commitInfo");

            // Verify remove action contains OLD DV information
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            assert_eq!(
                remove_actions.len(),
                1,
                "Expected exactly one remove action"
            );

            let remove_action = remove_actions[0];
            let remove = &remove_action["remove"];

            assert_eq!(
                remove["path"].as_str(),
                Some(file_path),
                "Remove path should match"
            );
            assert_eq!(remove["dataChange"].as_bool(), Some(true));
            assert_eq!(
                remove["deletionTimestamp"].as_i64(),
                Some(commit_timestamp),
                "deletionTimestamp should match commit timestamp"
            );

            // Verify OLD deletion vector in remove action
            let old_dv = remove["deletionVector"]
                .as_object()
                .expect("Remove action should have deletionVector");
            assert_eq!(
                old_dv.get("storageType").and_then(|v| v.as_str()),
                Some("u"),
                "Old DV storage type should be 'u'"
            );
            assert_eq!(
                old_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("vBn[lx{q8@P<9BNH/isA"),
                "Old DV path should match original"
            );
            assert_eq!(
                old_dv.get("offset").and_then(|v| v.as_i64()),
                Some(1),
                "Old DV offset should be 1"
            );
            assert_eq!(
                old_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                Some(36),
                "Old DV size should be 36"
            );
            assert_eq!(
                old_dv.get("cardinality").and_then(|v| v.as_i64()),
                Some(2),
                "Old DV cardinality should be 2"
            );

            // Verify file metadata is preserved in remove action
            let remove_size = remove["size"]
                .as_i64()
                .expect("Remove action should have size");
            let remove_partition_values = remove["partitionValues"]
                .as_object()
                .expect("Remove action should have partitionValues");
            let remove_tags = remove.get("tags");
            let remove_stats = remove.get("stats");

            // Verify add action contains NEW DV information
            let add_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("add").is_some())
                .collect();

            assert_eq!(add_actions.len(), 1, "Expected exactly one add action");

            let add_action = add_actions[0];
            let add = &add_action["add"];

            assert_eq!(
                add["path"].as_str(),
                Some(file_path),
                "Add path should match"
            );
            assert_eq!(add["dataChange"].as_bool(), Some(true));

            // Verify NEW deletion vector in add action
            let new_dv = add["deletionVector"]
                .as_object()
                .expect("Add action should have deletionVector");
            assert_eq!(
                new_dv.get("storageType").and_then(|v| v.as_str()),
                Some("u"),
                "New DV storage type should be 'u'"
            );
            assert_eq!(
                new_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("cd^-aqEH.-t@S}K{vb[*k^"),
                "New DV path should match updated value"
            );
            assert_eq!(
                new_dv.get("offset").and_then(|v| v.as_i64()),
                Some(10),
                "New DV offset should be 10"
            );
            assert_eq!(
                new_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                Some(40),
                "New DV size should be 40"
            );
            assert_eq!(
                new_dv.get("cardinality").and_then(|v| v.as_i64()),
                Some(3),
                "New DV cardinality should be 3"
            );

            // Verify file metadata is preserved in add action
            let add_size = add["size"].as_i64().expect("Add action should have size");
            let add_partition_values = add["partitionValues"]
                .as_object()
                .expect("Add action should have partitionValues");
            let add_tags = add.get("tags");
            let add_stats = add.get("stats");

            // Ensure metadata is consistent between remove and add actions
            assert_eq!(
                remove_size, add_size,
                "File size should be preserved between remove and add"
            );
            assert_eq!(
                remove_partition_values, add_partition_values,
                "Partition values should be preserved between remove and add"
            );
            assert_eq!(
                remove_tags, add_tags,
                "Tags should be preserved between remove and add"
            );
            assert_eq!(
                remove_stats, add_stats,
                "Stats should be preserved between remove and add"
            );

            // Ensure metadata matches the original file metadata from version 1
            assert_eq!(
                remove_size, original_size,
                "Remove action size should match original file size"
            );
            assert_eq!(
                add_size, original_size,
                "Add action size should match original file size"
            );
            assert_eq!(
                remove_partition_values, original_partition_values,
                "Remove action partition values should match original"
            );
            assert_eq!(
                add_partition_values, original_partition_values,
                "Add action partition values should match original"
            );
            assert_eq!(
                remove_tags, original_tags,
                "Remove action tags should match original"
            );
            assert_eq!(
                add_tags, original_tags,
                "Add action tags should match original"
            );
            assert_eq!(
                remove_stats, original_stats,
                "Remove action stats should match original"
            );
            assert_eq!(
                add_stats, original_stats,
                "Add action stats should match original"
            );
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_update_deletion_vectors_multiple_files() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that update_deletion_vectors can update multiple files
    // in a single call, creating proper Remove and Add actions for each file.
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ])?);

    // Setup: Create table with 3 files
    let file_names = &["file0.parquet", "file1.parquet", "file2.parquet"];
    let (store, engine, table_url, file_paths) =
        create_dv_table_with_files("test_table", schema, file_names).await?;

    // Create DV update transaction
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    let mut scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;

    // Update deletion vectors for all 3 files in a single call
    let mut dv_map = HashMap::new();
    for (idx, file_path) in file_paths.iter().enumerate() {
        let descriptor = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: format!("dv_file_{}.bin", idx),
            offset: Some(idx as i32 * 10),
            size_in_bytes: 40 + idx as i32,
            cardinality: idx as i64 + 1,
        };
        dv_map.insert(file_path.to_string(), descriptor);
    }

    txn.update_deletion_vectors(dv_map, scan_files.drain(..).map(Ok))?;

    // Commit the transaction
    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the commit log directly from object store
            let final_commit_path =
                table_url.join(&format!("_delta_log/{:020}.json", commit_version))?;
            let commit_content = store
                .get(&Path::from_url_path(final_commit_path.path())?)
                .await?
                .bytes()
                .await?;

            let parsed_commits: Vec<_> = Deserializer::from_slice(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Extract all remove and add actions
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            let add_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("add").is_some())
                .collect();

            // Should have 3 remove and 3 add actions
            assert_eq!(
                remove_actions.len(),
                3,
                "Expected 3 remove actions for 3 files"
            );
            assert_eq!(add_actions.len(), 3, "Expected 3 add actions for 3 files");

            // Verify each file has a DV in both remove and add
            for (idx, file_path) in file_paths.iter().enumerate() {
                // Find the remove action for this file
                let remove_action = remove_actions
                    .iter()
                    .find(|action| action["remove"]["path"].as_str() == Some(file_path.as_str()))
                    .unwrap_or_else(|| panic!("Should find remove action for {}", file_path));

                // Find the add action for this file
                let add_action = add_actions
                    .iter()
                    .find(|action| action["add"]["path"].as_str() == Some(file_path.as_str()))
                    .unwrap_or_else(|| panic!("Should find add action for {}", file_path));

                // Verify remove action does NOT have a DV (since these were newly written files)
                assert!(
                    remove_action["remove"]["deletionVector"].is_null(),
                    "Remove action for newly written file should not have a DV"
                );

                // Verify add action has the NEW DV
                let add_dv = add_action["add"]["deletionVector"]
                    .as_object()
                    .expect("Add action should have deletionVector");

                let expected_path = format!("dv_file_{}.bin", idx);
                assert_eq!(
                    add_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                    Some(expected_path.as_str()),
                    "DV path should match for file {}",
                    file_path
                );
                assert_eq!(
                    add_dv.get("offset").and_then(|v| v.as_i64()),
                    Some(idx as i64 * 10),
                    "DV offset should match for file {}",
                    file_path
                );
                assert_eq!(
                    add_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                    Some(40 + idx as i64),
                    "DV size should match for file {}",
                    file_path
                );
                assert_eq!(
                    add_dv.get("cardinality").and_then(|v| v.as_i64()),
                    Some(idx as i64 + 1),
                    "DV cardinality should match for file {}",
                    file_path
                );
            }
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_remove_files_verify_files_excluded_from_scan(
) -> Result<(), Box<dyn std::error::Error>> {
    // Adds and then removes files and then verifies they don't appear in the scan.

    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        // First, add some files to the table
        let engine = Arc::new(engine);
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;

        // Get initial file count
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot.clone().scan_builder().build()?;
        let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (_, selection_vector) = scan_metadata.scan_files.into_parts();
        let initial_file_count = selection_vector.iter().filter(|&x| *x).count();

        assert!(initial_file_count > 0);

        // Now create a transaction to remove files
        let mut txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()))?;

        // Create a new scan to get file metadata for removal
        let scan2 = snapshot.scan_builder().build()?;
        let scan_metadata2 = scan2.scan_metadata(engine.as_ref())?.next().unwrap()?;

        // Create FilteredEngineData for removal (select all rows for removal)
        let file_remove_count = (scan_metadata2.scan_files.data().len()
            - scan_metadata2.scan_files.selection_vector().len())
            + scan_metadata2
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&x| *x)
                .count();
        assert!(file_remove_count > 0);

        // Add remove files to transaction
        txn.remove_files(scan_metadata2.scan_files);

        // Commit the transaction
        let result = txn.commit(engine.as_ref());

        match result? {
            CommitResult::CommittedTransaction(committed) => {
                assert_eq!(committed.commit_version(), 2);

                let new_snapshot = Snapshot::builder_for(table_url.clone())
                    .at_version(2)
                    .build(engine.as_ref())?;

                let new_scan = new_snapshot.scan_builder().build()?;
                let mut new_file_count = 0;
                for new_metadata in new_scan.scan_metadata(engine.as_ref())? {
                    new_file_count += new_metadata?.scan_files.data().len();
                }

                // All files were removed, so new_file_count should be zero
                assert_eq!(new_file_count, 0);
            }
            _ => panic!("Transaction did not succeeed."),
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_remove_files_with_modified_selection_vector() -> Result<(), Box<dyn std::error::Error>>
{
    // This test verifies that we can selectively remove files by:
    // 1. Calling remove_files multiple times with different subsets
    // 2. Modifying the selection vector to choose which files to remove

    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let engine = Arc::new(engine);

        // Write data multiple times to create multiple files
        for i in 1..=5 {
            write_data_and_check_result_and_stats(
                table_url.clone(),
                schema.clone(),
                engine.clone(),
                i,
            )
            .await?;
        }

        // Get initial file count
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot.clone().scan_builder().build()?;

        let mut initial_file_count = 0;
        for metadata in scan.scan_metadata(engine.as_ref())? {
            let metadata = metadata?;
            initial_file_count += metadata
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&x| *x)
                .count();
        }

        assert!(
            initial_file_count >= 3,
            "Need at least 3 files for this test, got {}",
            initial_file_count
        );

        // Create a transaction to remove files in two batches
        let mut txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_engine_info("selective remove test")
            .with_operation("DELETE".to_string())
            .with_data_change(true);

        // First batch: Remove only the first file
        let scan2 = snapshot.clone().scan_builder().build()?;
        let scan_metadata2 = scan2.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (data, mut selection_vector) = scan_metadata2.scan_files.into_parts();

        // Select only the first file for removal
        let mut first_batch_removed = 0;
        for selected in selection_vector.iter_mut() {
            if *selected && first_batch_removed < 1 {
                // Keep selected for removal
                first_batch_removed += 1;
            } else {
                // Don't remove
                *selected = false;
            }
        }

        assert_eq!(
            first_batch_removed, 1,
            "Should remove exactly 1 file in first batch"
        );
        txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

        // Second batch: Remove only the last file
        let scan3 = snapshot.clone().scan_builder().build()?;
        let scan_metadata3 = scan3.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (data2, mut selection_vector2) = scan_metadata3.scan_files.into_parts();

        // Find the last selected file and keep only that one selected
        let mut last_selected_idx = None;
        for (i, &selected) in selection_vector2.iter().enumerate() {
            if selected {
                last_selected_idx = Some(i);
            }
        }

        // Deselect all except the last one
        for (i, selected) in selection_vector2.iter_mut().enumerate() {
            if Some(i) != last_selected_idx {
                *selected = false;
            }
        }

        let second_batch_removed = selection_vector2.iter().filter(|&x| *x).count();
        assert_eq!(
            second_batch_removed, 1,
            "Should remove exactly 1 file in second batch"
        );
        txn.remove_files(FilteredEngineData::try_new(data2, selection_vector2)?);

        // Commit the transaction
        let result = txn.commit(engine.as_ref())?;

        match result {
            CommitResult::CommittedTransaction(committed) => {
                assert_eq!(committed.commit_version(), 6);

                // Verify that exactly 2 files were removed (1 from each batch)
                let new_snapshot = Snapshot::builder_for(table_url.clone())
                    .at_version(6)
                    .build(engine.as_ref())?;

                let new_scan = new_snapshot.scan_builder().build()?;
                let mut new_file_count = 0;
                for new_metadata in new_scan.scan_metadata(engine.as_ref())? {
                    let metadata = new_metadata?;
                    new_file_count += metadata
                        .scan_files
                        .selection_vector()
                        .iter()
                        .filter(|&x| *x)
                        .count();
                }

                // Verify we removed exactly 2 files (1 + 1)
                let total_removed = first_batch_removed + second_batch_removed;
                assert_eq!(total_removed, 2);
                assert_eq!(new_file_count, initial_file_count - total_removed);
                assert!(new_file_count > 0, "At least one file should remain");
            }
            _ => panic!("Transaction did not succeed"),
        }
    }
    Ok(())
}

// Helper function to create a table with CDF enabled
async fn create_cdf_table(
    table_name: &str,
    schema: SchemaRef,
) -> Result<(Url, Arc<DefaultEngine<TokioBackgroundExecutor>>, TempDir), Box<dyn std::error::Error>>
{
    let tmp_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    let (store, engine, table_location) = engine_store_setup(table_name, Some(&tmp_test_dir_url));

    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true, // use protocol 3.7
        vec![],
        vec!["changeDataFeed"],
    )
    .await?;

    Ok((table_url, Arc::new(engine), tmp_dir))
}

// Helper function to write data to a table
async fn write_data_to_table(
    table_url: &Url,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<Version, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test");

    add_files_to_transaction(&mut txn, engine, schema, values).await?;

    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => Ok(committed.commit_version()),
        _ => panic!("Transaction should be committed"),
    }
}

// Helper function to add files to an existing transaction
async fn add_files_to_transaction(
    txn: &mut delta_kernel::transaction::Transaction,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(values))],
    )?;

    let write_context = Arc::new(txn.get_write_context());
    let add_files_metadata = engine
        .write_parquet(
            &ArrowEngineData::new(data),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;
    txn.add_files(add_files_metadata);
    Ok(())
}

#[tokio::test]
async fn test_cdf_write_all_adds_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that add-only transactions work with CDF enabled
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_all_adds", schema.clone()).await?;

    // Add files - this should succeed
    let version = write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;
    assert_eq!(version, 1);

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_all_removes_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that remove-only transactions work with CDF enabled
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_all_removes", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;

    // Now remove the files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("cdf remove test")
        .with_data_change(true);

    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - remove-only transactions are allowed with CDF
    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 2);
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_mixed_no_data_change_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that mixed add+remove transactions work when dataChange=false.
    // It's allowed because the transaction does not contain any logical data changes.
    // This can happen when a table is being optimized/compacted.
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_mixed_no_data_change", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // Now create a transaction with both add AND remove files, but dataChange=false
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("cdf mixed test")
        .with_data_change(false); // dataChange=false is key here

    // Add new files
    add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - mixed operations are allowed when dataChange=false
    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 2);
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_mixed_with_data_change_fails() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that mixed add+remove transactions fail with helpful error when dataChange=true
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_mixed_with_data_change", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // Now create a transaction with both add AND remove files with dataChange=true
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("cdf mixed fail test")
        .with_data_change(true); // dataChange=true - this should fail

    // Add new files
    add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should fail with our new error message
    assert_result_error_with_message(
        txn.commit(engine.as_ref()),
        "Cannot add and remove data in the same transaction when Change Data Feed is enabled (delta.enableChangeDataFeed = true). \
         This would require writing CDC files for DML operations, which is not yet supported. \
         Consider using separate transactions: one to add files, another to remove files."
    );

    Ok(())
}
