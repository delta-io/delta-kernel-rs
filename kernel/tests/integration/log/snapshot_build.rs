//! Integration tests for [`Snapshot`] build semantics.

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{CheckpointWriteResult, ChecksumWriteResult, IncrementalReplay};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{insert_data, test_table_setup_mt};

async fn setup_multi_version_table_with_crc<E: TaskExecutor>(
    engine: &Arc<DefaultEngine<E>>,
    table_path: &str,
) -> DeltaResult<()> {
    let schema = schema_ref! { nullable "id": INTEGER };
    let created = create_table(table_path, schema, "test_engine")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Persist CRC to disk so that future snapshots are able to get an in-memory CRC.
    created
        .post_commit_snapshot()
        .expect("create-table should produce a post-commit snapshot")
        .write_checksum(engine.as_ref())?;

    for id in 1..=3 {
        let snapshot = Snapshot::builder_for(table_path).build(engine.as_ref())?;
        let column: ArrayRef = Arc::new(Int32Array::from(vec![id]));
        insert_data(snapshot, engine, vec![column])
            .await?
            .unwrap_committed();
    }
    Ok(())
}

/// A snapshot built as a latest preserves the intent through version-preserving
/// derivations (checkpoint, checksum, incremental update-to-latest), a post-commit snapshot
/// produced from it is not considered built as latest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn built_as_latest_propagates_through_snapshot_derivations() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    setup_multi_version_table_with_crc(&engine, &table_path).await?;

    // Base snapshot: no time travel, built as latest.
    let base = Snapshot::builder_for(&table_path)
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .build(engine.as_ref())?;
    assert_eq!(base.version(), 3);
    assert!(base.built_as_latest());

    // checkpoint() preserves the query intent.
    let (ckpt_result, after_checkpoint) = base.checkpoint(engine.as_ref(), None)?;
    assert_eq!(ckpt_result, CheckpointWriteResult::Written);
    assert!(after_checkpoint.built_as_latest());

    // write_checksum() preserves the query intent.
    let (crc_result, after_checksum) = after_checkpoint.write_checksum(engine.as_ref())?;
    assert_eq!(crc_result, ChecksumWriteResult::Written);
    assert!(after_checksum.built_as_latest());

    // Incrementally build a snapshot without specifying a version is considered built as latest.
    let updated = Snapshot::builder_from(base.clone()).build(engine.as_ref())?;
    assert!(updated.built_as_latest());

    // A post-commit snapshot is not considered built as latest.
    let column: ArrayRef = Arc::new(Int32Array::from(vec![4]));
    let post_commit = insert_data(base, &engine, vec![column])
        .await?
        .unwrap_committed()
        .post_commit_snapshot()
        .expect("append should produce a post-commit snapshot")
        .clone();
    assert_eq!(post_commit.version(), 4);
    assert!(!post_commit.built_as_latest());

    Ok(())
}

/// A time-travel snapshot is never considered built as latest and none of the version-preserving
/// derivations (checkpoint, checksum, incremental update to the same pinned version) change
/// that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_snapshot_is_never_built_as_latest() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    setup_multi_version_table_with_crc(&engine, &table_path).await?;

    // Base: time-travel to version 3. Even though 3 happens to be the newest version, the
    // intent based `built_as_latest` is false.
    let base = Snapshot::builder_for(&table_path)
        .at_version(3)
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .build(engine.as_ref())?;
    assert_eq!(base.version(), 3);
    assert!(!base.built_as_latest());

    // checkpoint() preserves the intent.
    let (ckpt_result, after_checkpoint) = base.checkpoint(engine.as_ref(), None)?;
    assert_eq!(ckpt_result, CheckpointWriteResult::Written);
    assert!(!after_checkpoint.built_as_latest());

    // write_checksum() preserves the intent.
    let (crc_result, after_checksum) = after_checkpoint.write_checksum(engine.as_ref())?;
    assert_eq!(crc_result, ChecksumWriteResult::Written);
    assert!(!after_checksum.built_as_latest());

    // Time-traveling to another version is not considered built as latest.
    let another_version = Snapshot::builder_from(base.clone())
        .at_version(2)
        .build(engine.as_ref())?;
    assert!(!another_version.built_as_latest());

    // A post-commit snapshot from a time-travel base is not considered built as latest.
    let column: ArrayRef = Arc::new(Int32Array::from(vec![4]));
    let post_commit = insert_data(base, &engine, vec![column])
        .await?
        .unwrap_committed()
        .post_commit_snapshot()
        .expect("append should produce a post-commit snapshot")
        .clone();
    assert!(!post_commit.built_as_latest());

    Ok(())
}
