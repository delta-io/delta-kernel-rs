//! Incremental snapshot update metrics tests.
//!
//! `Snapshot::builder_from(existing)` applies a simple heuristic to minimize I/O when
//! advancing a snapshot to a newer version. The behavior depends on what the log listing
//! finds between the existing snapshot's checkpoint version and the target version:
//!
//! - **No new commits:** returns the existing snapshot unchanged; no metrics emitted.
//! - **New commits only (no new checkpoint):** incrementally replays only the net-new
//!   commits for Protocol/Metadata changes, then merges with the existing log segment.
//!   `LogSegmentLoaded` is emitted with the net-new commit count.
//! - **New checkpoint found:** ignores the existing snapshot entirely and constructs a
//!   fresh snapshot from the new checkpoint (plus any tail commits after it). This path
//!   is equivalent to a full `builder_for` build -- nothing is incremental. The metric
//!   counts reflect the new checkpoint and its tail, not the prior snapshot's history.
//!
//! The tests below cover the incremental (no new checkpoint) path. The full-rebuild path
//! (new checkpoint found) is exercised by the same code paths as scenario 2/3 in `snapshot_load.rs`
//! and does not require separate coverage here.

use super::{measuring_engine, simple_schema};
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::insert_data;
use url::Url;

// ---------------------------------------------------------------------------
// Scenario 11: incremental update replays only new tail commits
// ---------------------------------------------------------------------------

/// `Snapshot::builder_from(existing)` starts from an existing snapshot and replays only the
/// commits that arrived after it. The JSON reads reflect only the new tail commit, not the full
/// history -- demonstrating that incremental updates are cheaper than a full `builder_for` rebuild.
///
/// `LogSegmentLoaded` is emitted with the net-new commit count (commits after the existing
/// snapshot's version), not the full listing total.
///
/// Table setup: v0 (create) + v1 (insert) = existing snapshot at v1; v2 (insert) added
/// after. The incremental build from v1 sees only v2.
#[tokio::test]
async fn incremental_snapshot_update_replays_only_new_commits() -> DeltaResult<()> {
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///").unwrap();
    let setup_engine = Arc::new(DefaultEngineBuilder::new(store.clone() as Arc<_>).build());

    let _ = create_table("memory:///", simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;
    let snap0 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let _ = insert_data(
        snap0,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?;

    // Capture snapshot at v1 (no reporter needed for setup)
    let existing_snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;

    // Add one more commit (v2) after the captured snapshot
    let _ = insert_data(
        Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![2]))],
    )
    .await?;

    // Incremental update from existing_snap (at v1) with a reporter
    let (engine, reporter) = measuring_engine(store);
    let _updated = Snapshot::builder_from(existing_snap).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    // LogSegmentLoaded is emitted with the net-new commit count (1 new commit: v2)
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 1);

    // JSON handler reads only commit v2, not the full 3-commit history
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 1);
    assert!(reporter.list_calls.get() >= 1);

    Ok(())
}
