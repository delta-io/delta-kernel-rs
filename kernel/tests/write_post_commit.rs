//! Integration tests for post-commit snapshot propagation and write-parquet partition-context
//! error paths.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table as create_table_txn;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};
use tempfile::tempdir;
use test_utils::{create_default_engine, setup_test_tables, write_batch_to_table};
use url::Url;

mod common;

use common::write_utils::get_simple_int_schema;

#[tokio::test]
async fn test_post_commit_snapshot_create_then_insert() -> DeltaResult<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = tempdir().unwrap();
    let table_url = Url::from_directory_path(temp_dir.path()).unwrap();
    let engine = create_default_engine(&table_url)?;
    let schema = get_simple_int_schema();

    // Create table and verify post_commit_snapshot
    let create_result = create_table_txn(table_url.as_str(), schema, env!("CARGO_PKG_VERSION"))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let mut current_snapshot = match create_result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 0);
            // CREATE TABLE is the first commit: 1 commit since last checkpoint/compaction
            assert_eq!(committed.post_commit_stats().commits_since_checkpoint, 1);
            assert_eq!(
                committed.post_commit_stats().commits_since_log_compaction,
                1
            );
            let post_snapshot = committed
                .post_commit_snapshot()
                .expect("should have post_commit_snapshot");
            assert_eq!(post_snapshot.version(), 0);
            post_snapshot.clone()
        }
        _ => panic!("Create should succeed"),
    };

    // Do 10 inserts and verify post_commit_snapshot for each
    for i in 1..11 {
        let base_version = current_snapshot.version();

        let txn = current_snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_engine_info("test");

        match txn.commit(engine.as_ref())? {
            CommitResult::CommittedTransaction(committed) => {
                let post_snapshot = committed
                    .post_commit_snapshot()
                    .expect("should have post_commit_snapshot");

                assert_eq!(post_snapshot.version(), base_version + 1);
                assert_eq!(post_snapshot.version(), committed.commit_version());
                assert_eq!(post_snapshot.schema(), current_snapshot.schema());
                assert_eq!(post_snapshot.table_root(), current_snapshot.table_root());

                current_snapshot = post_snapshot.clone();
            }
            _ => panic!("Commit {i} should succeed"),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_write_parquet_succeed_with_logical_partition_names(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("letter", DataType::STRING),
    ])?);

    for (table_url, engine, _store, _table_name) in setup_test_tables(
        schema.clone(),
        &["letter"],
        None,
        "test_partition_translate",
    )
    .await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

        // Create data with only the non-partition column
        let data_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("id", DataType::INTEGER)]).unwrap(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(data_schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )?;

        // Pass partition values with logical name -- should succeed
        let result = write_batch_to_table(
            &snapshot,
            &engine,
            batch,
            HashMap::from([("letter".to_string(), Scalar::String("a".into()))]),
        )
        .await;
        assert!(
            result.is_ok(),
            "write_parquet should succeed with valid logical partition name"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_write_parquet_rejects_partitioned_write_context_on_unpartitioned_table(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_int_schema();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_partition_reject").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), &engine)?
            .with_engine_info("test");

        let result = txn.partitioned_write_context(HashMap::from([(
            "nonexistent".to_string(),
            Scalar::String("val".into()),
        )]));
        let err =
            result.expect_err("should fail with partitioned_write_context on unpartitioned table");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("table is not partitioned"),
            "Error should indicate table is not partitioned, got: {err_msg}"
        );
    }
    Ok(())
}
