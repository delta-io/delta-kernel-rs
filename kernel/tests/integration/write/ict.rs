//! Integration tests for the in-commit timestamp (ICT) write path.

use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
use delta_kernel::schema::SchemaRef;
use delta_kernel::test_utils::engine_store_setup;
use delta_kernel::transaction::CommitResult;
use delta_kernel::Snapshot;
use tempfile::TempDir;
use url::Url;

use crate::common::write_utils::get_simple_int_schema;

async fn get_ict_at_version(
    store: Arc<DynObjectStore>,
    table_url: &Url,
    version: u64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let commit_path = table_url.join(&format!("_delta_log/{version:020}.json"))?;
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
        "Commit log at version {version} should not be empty"
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

    let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());
    let file_meta = engine
        .write_parquet(&ArrowEngineData::new(data), write_context.as_ref())
        .await?;
    txn.add_files(file_meta);
    Ok(())
}

#[tokio::test]
async fn test_ict_commit_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number' with ICT enabled
    let schema = get_simple_int_schema();

    let tmp_dir = TempDir::new()?;
    let tmp_test_dir_url = Url::from_file_path(&tmp_dir).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_ict_first_commit", Some(&tmp_test_dir_url));

    // Create table with ICT enabled (writer version 7)
    let table_url = delta_kernel::test_utils::create_table(
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
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
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
        "First commit ICT ({first_ict}) should be greater than enablement timestamp (1612345678)"
    );

    // Second commit
    let snapshot2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(
        snapshot2.version(),
        1,
        "Second snapshot should be version 1"
    );

    let mut txn2 = snapshot2
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
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
        "Second ICT ({second_ict}) should be greater than first ICT ({first_ict})"
    );

    Ok(())
}
