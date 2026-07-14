//! Integration tests that verify kernel-written files are referenced by relative path.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::transaction::create_table::create_table as create_table_txn;
use delta_kernel::Snapshot;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    begin_transaction, create_table_and_load_snapshot, read_add_infos, test_table_setup,
    write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::{get_simple_schema, simple_id_batch};

/// Helper to write a batch and return the post-commit snapshot.
async fn write_batch_to_table_simple(
    snapshot: &Arc<Snapshot>,
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    data: RecordBatch,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let mut txn = begin_transaction(snapshot.clone(), engine)?.with_engine_info("test");
    let write_context = txn.unpartitioned_write_context()?;
    let add_meta = engine
        .write_parquet(&ArrowEngineData::new(data), &write_context)
        .await?;
    txn.add_files(add_meta);
    let committed = txn.commit(engine)?.unwrap_committed();
    Ok(committed.post_commit_snapshot().unwrap().clone())
}

#[tokio::test]
async fn test_write_uses_relative_paths_and_readback() -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_schema();
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;

    let snapshot = write_batch_to_table_simple(
        &snapshot,
        engine.as_ref(),
        simple_id_batch(&schema, vec![1, 2, 3]),
    )
    .await?;

    // Verify paths in the log are relative (no scheme like "s3://")
    let add_infos = read_add_infos(&snapshot, engine.as_ref())?;
    assert_eq!(add_infos.len(), 1);
    let path = &add_infos[0].path;
    assert!(
        !path.contains("://"),
        "should produce relative paths, got: {path}"
    );

    // Verify data is readable via scan
    let scan = snapshot.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine)?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    Ok(())
}

#[tokio::test]
async fn test_multiple_files_in_commit_all_use_relative_paths(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_schema();
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;

    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?.with_engine_info("test");
    let write_context = txn.unpartitioned_write_context().unwrap();
    for values in [vec![1, 2], vec![3, 4]] {
        let add_meta = engine
            .write_parquet(
                &ArrowEngineData::new(simple_id_batch(&schema, values)),
                &write_context,
            )
            .await?;
        txn.add_files(add_meta);
    }
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap().clone();

    let add_infos = read_add_infos(&snapshot, engine.as_ref())?;
    assert_eq!(add_infos.len(), 2);
    for info in &add_infos {
        assert!(
            !info.path.contains("://"),
            "Expected relative path, got: {}",
            info.path
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_multiple_commits_with_relative_paths_all_readable(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_schema();
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let mut snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;

    for values in [vec![1, 2], vec![3, 4], vec![5, 6]] {
        snapshot = write_batch_to_table(
            &snapshot,
            engine.as_ref(),
            simple_id_batch(&schema, values),
            HashMap::new(),
        )
        .await?;
    }

    let scan = snapshot.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine)?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);

    Ok(())
}

#[tokio::test]
async fn test_create_table_with_data_uses_relative_paths() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = get_simple_schema();
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();

    let mut txn = create_table_txn(table_url.as_str(), schema.clone(), "test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let write_context = txn.unpartitioned_write_context()?;
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(simple_id_batch(&schema, vec![10, 20])),
            &write_context,
        )
        .await?;
    txn.add_files(add_meta);
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap().clone();

    let add_infos = read_add_infos(&snapshot, engine.as_ref())?;
    assert_eq!(add_infos.len(), 1);
    let path = &add_infos[0].path;
    assert!(
        !path.contains("://"),
        "should produce relative paths, got: {path}"
    );

    // Verify data is readable
    let scan = snapshot.scan_builder().build()?;
    let batches = test_utils::read_scan(&scan, engine)?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    Ok(())
}
