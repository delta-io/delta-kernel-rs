//! Integration tests that exercise CommitInfo generation for kernel-authored commits.

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use itertools::Itertools;
use serde_json::{json, Deserializer};
use test_utils::{set_json_value, setup_test_tables};

mod common;

use common::write_utils::{get_simple_int_schema, validate_timestamp, validate_txn_id, ZERO_UUID};

#[tokio::test]
async fn test_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // create a transaction
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let committer = Box::new(FileSystemCommitter::new());
        let txn = snapshot
            .transaction(committer, &engine)?
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

#[tokio::test]
async fn test_commit_info_action() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
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

/// Verifies that when `engine_commit_info` is provided (the `Some` branch of `build_commit_info`):
/// - The written JSON is correctly wrapped in a top-level `"commitInfo"` key.
/// - Engine-only fields (not in `CommitInfo::to_schema()`) pass through to the log unchanged.
/// - Fields that overlap with kernel-managed CommitInfo fields are overridden by kernel values,
/// - All kernel-managed fields (`timestamp`, `kernelVersion`, `txnId`, `operationParameters`) are
///   present with correct values.
#[tokio::test]
async fn test_commit_info_with_engine_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // Build engine_commit_info with:
        //   - "myApp"    : engine-only field, must pass through unchanged.
        //   - "myVersion": engine-only field, must pass through unchanged.
        //   - "operation": overlapping with CommitInfo; kernel must override with "WRITE".
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("myApp", ArrowDataType::Utf8, false),
            Field::new("myVersion", ArrowDataType::Utf8, false),
            Field::new("operation", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["spark"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["3.5.0"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["STALE_OP"])) as ArrayRef,
            ],
        )?;
        let engine_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("myApp", DataType::STRING),
            StructField::not_null("myVersion", DataType::STRING),
            StructField::nullable("operation", DataType::STRING),
        ]));

        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_operation("WRITE".to_string())
            .with_commit_info(Box::new(ArrowEngineData::new(batch)), engine_schema);

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
        validate_timestamp(&parsed_commits[0]["commitInfo"]);

        // Zero out non-deterministic fields for stable comparison.
        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;

        // Null-valued CommitInfo fields (inCommitTimestamp, isBlindAppend, engineInfo) are
        // omitted from the JSON -- consistent with how the Delta log serializes optional fields.
        let expected_commits = vec![json!({
            "commitInfo": {
                // Engine-only fields pass through unchanged.
                "myApp": "spark",
                "myVersion": "3.5.0",
                // Kernel overrides the engine's stale "STALE_OP" with the real operation.
                "operation": "WRITE",
                // Remaining kernel-managed non-null fields are appended.
                "operationParameters": {},
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "txnId": ZERO_UUID,
                "timestamp": 0,
            }
        })];

        assert_eq!(parsed_commits, expected_commits);
    }
    Ok(())
}
