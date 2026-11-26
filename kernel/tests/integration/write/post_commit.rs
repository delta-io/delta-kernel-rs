//! Integration tests for post-commit snapshot propagation and write-parquet partition-context
//! error paths.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::schema_ref;
use delta_kernel::transaction::create_table::create_table as create_table_txn;
use delta_kernel::transaction::{CommitResult, UpdateTableOperation};
use delta_kernel::{KernelError, Snapshot};
use tempfile::tempdir;
use test_utils::{
    begin_transaction_with, create_default_engine, setup_test_tables, write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::get_simple_int_schema;

#[tokio::test]
async fn test_post_commit_snapshot_create_then_insert() -> delta_kernel::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = tempdir().unwrap();
    let table_url = Url::from_directory_path(temp_dir.path()).unwrap();
    let engine = create_default_engine(&table_url)?;
    let schema = get_simple_int_schema();

    // Create table and verify post_commit_snapshot
    let create_result =
        create_table_txn(table_url.as_str(), schema, env!("CARGO_PKG_VERSION"), true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;

    let mut current_snapshot = match create_result {
        CommitResult::Committed(committed) => {
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

        let txn = begin_transaction_with(
            current_snapshot.clone(),
            engine.as_ref(),
            delta_kernel::transaction::UpdateTableOperation::Write,
            "test",
            true,
            |builder| builder,
        )?;

        match txn.commit(engine.as_ref())? {
            CommitResult::Committed(committed) => {
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
async fn test_post_commit_snapshot_does_not_count_stats_only_readd_twice()
-> delta_kernel::Result<()> {
    let temp_dir = tempdir().unwrap();
    let table_url = Url::from_directory_path(temp_dir.path()).unwrap();
    let engine = create_default_engine(&table_url)?;
    let schema = get_simple_int_schema();
    let created = match create_table_txn(
        table_url.as_str(),
        schema.clone(),
        env!("CARGO_PKG_VERSION"),
        true,
    )
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    {
        CommitResult::Committed(committed) => committed.post_commit_snapshot().unwrap().clone(),
        _ => panic!("Create should succeed"),
    };

    let mut write = begin_transaction_with(
        created,
        engine.as_ref(),
        UpdateTableOperation::Write,
        "test",
        true,
        |builder| builder,
    )?;
    write.ack_column_defaults();
    let write_context = write.write_state()?.write_context_builder().build()?;
    let batch = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow().unwrap()),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let add_metadata: RecordBatch = ArrowEngineData::try_from_engine_data(
        engine
            .write_parquet(&ArrowEngineData::new(batch), &write_context)
            .await?,
    )?
    .into();
    write.add_files(Box::new(ArrowEngineData::new(add_metadata.clone())));
    let written = match write.commit(engine.as_ref())? {
        CommitResult::Committed(committed) => committed.post_commit_snapshot().unwrap().clone(),
        _ => panic!("Write should succeed"),
    };
    assert_eq!(written.get_file_stats_if_present().unwrap().num_files(), 1);

    let mut readd = begin_transaction_with(
        written,
        engine.as_ref(),
        UpdateTableOperation::Write,
        "test",
        false,
        |builder| builder,
    )?;
    readd.ack_column_defaults();
    readd.add_files(Box::new(ArrowEngineData::new(add_metadata)));
    let readded = match readd.commit(engine.as_ref())? {
        CommitResult::Committed(committed) => committed.post_commit_snapshot().unwrap().clone(),
        _ => panic!("Re-add should succeed"),
    };
    assert_eq!(readded.get_file_stats_if_present(), None);
    Ok(())
}

#[tokio::test]
async fn test_write_parquet_succeed_with_logical_partition_names() -> delta_kernel::Result<()> {
    let schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "letter": STRING,
    };

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
        let data_schema = schema_ref! { nullable "id": INTEGER };
        let batch = RecordBatch::try_new(
            Arc::new(
                data_schema
                    .as_ref()
                    .try_into_arrow()
                    .map_err(KernelError::from)?,
            ),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .map_err(KernelError::from)?;

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
async fn test_write_context_builder_rejects_partition_values_on_unpartitioned_table()
-> delta_kernel::Result<()> {
    let schema = get_simple_int_schema();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_partition_reject").await?
    {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = begin_transaction_with(
            snapshot.clone(),
            &engine,
            delta_kernel::transaction::UpdateTableOperation::Write,
            "test",
            true,
            |builder| builder,
        )?;
        let write_state = txn.write_state()?;

        let result = write_state
            .write_context_builder()
            .with_partition_values(HashMap::from([(
                "nonexistent".to_string(),
                Scalar::String("val".into()),
            )]))
            .build();
        let err = result.expect_err("partition values should fail on an unpartitioned table");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("table is not partitioned"),
            "Error should indicate table is not partitioned, got: {err_msg}"
        );
    }
    Ok(())
}
