//! Incremental snapshot update metrics tests.
//!
//! Verifies I/O expectations when advancing an existing snapshot to a newer version via
//! `Snapshot::builder_from`. Scenarios covered:
//!
//! - **No new commits:** returns the existing snapshot unchanged; no log replay I/O.
//! - **New commits only (no new checkpoint):** incrementally replays only the net-new
//!   commits; `LogSegmentLoaded` is emitted with the net-new commit count.
//! - **New checkpoint found:** discards the existing snapshot and builds fresh from
//!   the new checkpoint (plus any tail commits after it).
//! - **Compaction overlapping old version:** conservatively filters out compaction files
//!   whose start version is at or before the existing snapshot version.

use super::{insert_rows, measuring_engine, setup_in_memory_table, simple_schema};
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::to_json_bytes;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStore as _;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::{insert_data, test_table_setup_mt};

// ============================================================================
// Scenario 1: no new commits -- returns existing snapshot unchanged
// ============================================================================

/// When no new commits exist after the existing snapshot, `builder_from` returns the
/// same snapshot. `SnapshotCompleted` is still emitted (the builder always reports
/// completion), but no `LogSegmentLoaded` fires because no log replay occurred.
#[tokio::test]
async fn no_new_commits_returns_existing_snapshot_no_log_replay() -> DeltaResult<()> {
    let (table_url, setup_engine, store) = setup_in_memory_table(1).await?;
    let existing = Snapshot::builder_for(table_url).build(setup_engine.as_ref())?;

    let (engine, reporter) = measuring_engine(store);
    let updated = Snapshot::builder_from(existing.clone()).build(&engine)?;

    assert_eq!(updated.version(), existing.version());
    // SnapshotCompleted is emitted even for no-op updates
    assert_eq!(reporter.snapshot_completions.get(), 1);
    // No log replay occurred
    assert_eq!(reporter.log_segment_loads.get(), 0);
    assert_eq!(reporter.json_read_calls.get(), 0);
    assert_eq!(reporter.parquet_read_calls.get(), 0);

    Ok(())
}

// ============================================================================
// Scenario 2: new commits only -- incremental tail replay (single commit)
// ============================================================================

/// `Snapshot::builder_from(existing)` replays only the commits that arrived after the
/// existing snapshot. The JSON reads reflect only the new tail commit, not the full
/// history. `LogSegmentLoaded` is emitted with the net-new commit count.
///
/// Table setup: v0 (create) + v1 (insert) = existing snapshot at v1; v2 (insert) added
/// after. The incremental build from v1 sees only v2.
#[tokio::test]
async fn incremental_update_replays_single_new_commit() -> DeltaResult<()> {
    let (table_url, setup_engine, store) = setup_in_memory_table(1).await?;
    let existing = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;

    insert_rows(&table_url, &setup_engine, 2, 1).await?;

    let (engine, reporter) = measuring_engine(store);
    let _updated = Snapshot::builder_from(existing).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 1);
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 1);
    assert!(reporter.list_calls.get() >= 1);

    Ok(())
}

// ============================================================================
// Scenario 3: multiple new commits -- all read in a single JSON call
// ============================================================================

/// When multiple commits arrive after the existing snapshot, all are read in a single
/// `read_json_files` call.
///
/// Table setup: existing at v1; v2, v3, v4 added after. Incremental build sees 3 commits.
#[tokio::test]
async fn incremental_update_batches_multiple_new_commits() -> DeltaResult<()> {
    let (table_url, setup_engine, store) = setup_in_memory_table(1).await?;
    let existing = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;

    insert_rows(&table_url, &setup_engine, 2, 3).await?;

    let (engine, reporter) = measuring_engine(store);
    let _updated = Snapshot::builder_from(existing).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 3);
    // All 3 new commits read in a single JSON call
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 3);
    assert_eq!(reporter.parquet_read_calls.get(), 0);

    Ok(())
}

// ============================================================================
// Scenario 4: new checkpoint found -- full rebuild, discards existing snapshot
// ============================================================================

/// When a new checkpoint appears after the existing snapshot was built, `builder_from`
/// discards the existing snapshot and builds a fresh one from the new checkpoint.
/// The metrics reflect the fresh rebuild: checkpoint read via parquet, tail commits
/// via JSON.
///
/// Table setup: v0 (create) + v1 (insert). Existing snapshot captured at v1 (NO
/// checkpoint exists yet). Then checkpoint written at v1 + commits v2 and v3 added.
/// When the incremental build lists from v0+1=v1, it discovers the new checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_checkpoint_triggers_full_rebuild() -> DeltaResult<()> {
    let (temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    // v0: create table
    let _ = create_table(&table_path, simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    // v1: insert a row
    let snap0 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let committed = insert_data(
        snap0,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?
    .unwrap_committed();

    // Capture existing snapshot at v1 BEFORE the checkpoint is written
    let existing = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    assert_eq!(existing.version(), 1);

    // Now write a checkpoint at v1
    committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone()
        .checkpoint(setup_engine.as_ref())?;

    // Add commits v2 and v3 after the checkpoint
    let mut snap = committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone();
    for val in [2i32, 3] {
        let c = insert_data(
            snap,
            &setup_engine,
            vec![Arc::new(Int32Array::from(vec![val]))],
        )
        .await?
        .unwrap_committed();
        snap = c
            .post_commit_snapshot()
            .expect("post-commit snapshot")
            .clone();
    }

    let (measure_engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let updated = Snapshot::builder_from(existing).build(&measure_engine)?;

    assert_eq!(updated.version(), 3);
    assert_eq!(reporter.snapshot_completions.get(), 1);
    // Checkpoint read via Parquet; tail commits (v2, v3) via JSON
    assert_eq!(reporter.parquet_read_calls.get(), 1);
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 2);
    // Keep temp_dir alive for the duration of the test
    drop(temp_dir);

    Ok(())
}

// ============================================================================
// Scenario 5: compaction overlapping old version -- conservatively filtered out
// ============================================================================

/// When a log compaction file's range overlaps with the existing snapshot's version,
/// the incremental path conservatively excludes it (since it may contain actions the
/// existing snapshot already processed). Only the net-new individual commit files are
/// replayed.
///
/// Table setup: v0 (create) + v1 + v2 (inserts). Compaction covering v0-v2 written.
/// Existing snapshot captured at v2. v3 added after. Incremental build from v2 sees
/// only v3 -- the compaction file is filtered out because its start version (v0) is
/// at or before the existing snapshot version (v2).
#[tokio::test]
async fn compaction_overlapping_existing_version_is_filtered_out() -> DeltaResult<()> {
    let (table_url, setup_engine, store) = setup_in_memory_table(2).await?;

    // Write compaction covering v0-v2
    let snap2 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let mut writer = snap2.log_compaction_writer(0, 2)?;
    let compaction_url = writer.compaction_path().clone();
    let batches: Vec<_> = writer
        .compaction_data(setup_engine.as_ref())?
        .collect::<DeltaResult<Vec<_>>>()?;
    let json_bytes = to_json_bytes(batches.into_iter().map(Ok))?;
    let compaction_path = Path::from_url_path(compaction_url.path())
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    store
        .put(&compaction_path, json_bytes.into())
        .await
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    // Capture existing snapshot at v2
    let existing = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    assert_eq!(existing.version(), 2);

    // Add one more commit (v3) after the existing snapshot
    insert_rows(&table_url, &setup_engine, 3, 1).await?;

    let (engine, reporter) = measuring_engine(store);
    let updated = Snapshot::builder_from(existing).build(&engine)?;

    assert_eq!(updated.version(), 3);
    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    // Only the net-new commit (v3) is replayed; the compaction file is excluded
    assert_eq!(reporter.commit_files.get(), 1);
    assert_eq!(reporter.compaction_files.get(), 0);
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 1);

    Ok(())
}
