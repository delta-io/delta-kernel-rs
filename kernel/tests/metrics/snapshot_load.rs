//! Snapshot-loading and on-demand API metrics tests.
//!
//! Covers all major log-segment shapes (delta-only, V1 checkpoint, log compaction, CRC
//! fast-path, CRC at prior version, checkpoint with tail commits) plus on-demand API calls
//! (`get_domain_metadata`) that incur additional I/O after a snapshot is already built.

use super::{measuring_engine, setup_table_with_v1_checkpoint, simple_schema};
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::engine::to_json_bytes;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStore as _;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::{insert_data, test_table_setup_mt};
use url::Url;

// ---------------------------------------------------------------------------
// Scenario 1: delta-only (2 commits, no checkpoint, no compaction)
// ---------------------------------------------------------------------------

/// A snapshot built from two JSON commits -- no checkpoint, no CRC, no compaction --
/// reports exactly the commit file count and triggers one JSON read call covering all
/// commit files.
#[tokio::test]
async fn delta_only_snapshot_emits_expected_metrics() -> DeltaResult<()> {
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

    let (engine, reporter) = measuring_engine(store);
    let _snap = Snapshot::builder_for(table_url).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 2);
    assert_eq!(reporter.checkpoint_files.get(), 0);
    assert_eq!(reporter.compaction_files.get(), 0);

    // Both commit files read in a single JSON call
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 2);
    assert_eq!(reporter.parquet_read_calls.get(), 0);

    // Exactly one listing covering exactly the 2 commit files
    assert_eq!(reporter.list_calls.get(), 1);
    assert_eq!(reporter.list_files_seen.get(), 2);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 2: v1 parquet checkpoint + one tail commit
// ---------------------------------------------------------------------------

/// After a v1 parquet checkpoint is written at version 1 and a further commit is added,
/// a fresh snapshot sees one checkpoint file, one tail commit, and performs a single
/// parquet read (checkpoint) plus a single JSON read (tail commit).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_with_v1_checkpoint_and_tail_commit_emits_expected_metrics() -> DeltaResult<()> {
    let (table_url, setup_engine, _temp_dir) = setup_table_with_v1_checkpoint().await?;

    // commit 2: insert another row after the checkpoint
    let snap2 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let _ = insert_data(
        snap2,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![2]))],
    )
    .await?;

    let (measure_engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 1); // only tail commit (v2)
    assert_eq!(reporter.checkpoint_files.get(), 1);
    assert_eq!(reporter.compaction_files.get(), 0);

    // One JSON read for the tail commit; one Parquet read for the checkpoint
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 1);
    assert_eq!(reporter.parquet_read_calls.get(), 1);
    assert_eq!(reporter.parquet_files_read.get(), 1);
    assert!(reporter.json_bytes_read.get() > 0);
    assert!(reporter.parquet_bytes_read.get() > 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 3: v1 parquet checkpoint at latest version (no tail commits)
// ---------------------------------------------------------------------------

/// When the latest version has a checkpoint and no subsequent commits exist, the snapshot
/// has zero commit files and the JSON handler is called with an empty file list.
/// CommitReader always invokes read_json_files even for an empty commit cover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_at_checkpoint_tip_emits_expected_metrics() -> DeltaResult<()> {
    let (table_url, _setup_engine, _temp_dir) = setup_table_with_v1_checkpoint().await?;

    let (measure_engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 0);
    assert_eq!(reporter.checkpoint_files.get(), 1);
    assert_eq!(reporter.compaction_files.get(), 0);

    // JSON handler is called with zero files; Parquet reads the checkpoint
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 0);
    assert_eq!(reporter.parquet_read_calls.get(), 1);
    assert_eq!(reporter.parquet_files_read.get(), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 4: log compaction covering early commits + one tail commit
// ---------------------------------------------------------------------------

/// When early commits are covered by a compacted log file, the snapshot reports both
/// the individual commit count and the compaction count. The JSON handler reads the
/// compaction file and the tail commit in a single call (the minimal cover).
#[tokio::test]
async fn snapshot_with_log_compaction_emits_expected_metrics() -> DeltaResult<()> {
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///").unwrap();
    let setup_engine = Arc::new(DefaultEngineBuilder::new(store.clone() as Arc<_>).build());

    let _ = create_table("memory:///", simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    for val in [1i32, 2] {
        let snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
        let _ = insert_data(
            snap,
            &setup_engine,
            vec![Arc::new(Int32Array::from(vec![val]))],
        )
        .await?;
    }

    // Write a compacted log file covering versions 0-2 using the public API
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

    // commit 3: tail commit after the compaction
    let snap3 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let _ = insert_data(
        snap3,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![3]))],
    )
    .await?;

    let (engine, reporter) = measuring_engine(store);
    let _snap = Snapshot::builder_for(table_url).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    // ascending_commit_files contains all 4 individual .json files (0, 1, 2, 3)
    assert_eq!(reporter.commit_files.get(), 4);
    assert_eq!(reporter.checkpoint_files.get(), 0);
    assert_eq!(reporter.compaction_files.get(), 1);

    // find_commit_cover selects [3.json, 0.2.compacted.json] -- 2 JSON files, one read call
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 2);
    assert_eq!(reporter.parquet_read_calls.get(), 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 5: CRC fast-path bypasses JSON replay
// ---------------------------------------------------------------------------

/// When a CRC file exists at the target snapshot version, Protocol+Metadata are loaded
/// directly from it, skipping all JSON log replay. The JSON handler is never called.
#[tokio::test]
async fn snapshot_with_crc_at_target_version_skips_json_replay() -> DeltaResult<()> {
    // The crc-full golden table has commit 0 + a CRC file at version 0.
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/"))
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let table_root =
        Url::from_directory_path(path).map_err(|_| delta_kernel::Error::generic("invalid path"))?;

    let (engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let _snap = Snapshot::builder_for(table_root).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 1);
    assert_eq!(reporter.checkpoint_files.get(), 0);

    // CRC at target version -- P+M loaded from CRC, no JSON or Parquet reads needed
    assert_eq!(reporter.json_read_calls.get(), 0);
    assert_eq!(reporter.parquet_read_calls.get(), 0);
    // Two storage reads: (1) _last_checkpoint hint attempt (file absent for this table),
    // (2) the CRC file itself. Both go through StorageHandler::read_files.
    assert_eq!(reporter.storage_read_calls.get(), 2);
    // Listing sees both the commit file and the CRC file
    assert_eq!(reporter.list_calls.get(), 1);
    assert_eq!(reporter.list_files_seen.get(), 2);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 6: CRC at a prior version (CRC exists but is older than latest)
// ---------------------------------------------------------------------------

/// When a CRC file exists at an older version than the snapshot, the kernel takes the
/// partial-replay path: it replays only the tail commits (those after the CRC version)
/// looking for Protocol/Metadata changes, then falls back to the CRC when none are
/// found. This produces JSON reads for the tail commits AND a storage read for the CRC.
///
/// This is a normal, expected table state -- having a CRC from a previous version is
/// not an error or degraded condition.
///
/// `write_checksum` requires stats from a post-commit snapshot (not a freshly built
/// one), so this test uses `create_table` commit -> `post_commit_snapshot` ->
/// `write_checksum` to write the CRC at v0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crc_at_prior_version_triggers_tail_replay_then_falls_back_to_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    // commit 0: create table; write CRC from the post-commit snapshot.
    // `write_checksum` requires an in-memory CRC computed during the transaction.
    let create_committed = create_table(&table_path, simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?
        .unwrap_committed();
    create_committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone()
        .write_checksum(setup_engine.as_ref())?;

    // commits 1 and 2: pure Add actions, no Protocol/Metadata changes
    for val in [1i32, 2] {
        let snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
        let _ = insert_data(
            snap,
            &setup_engine,
            vec![Arc::new(Int32Array::from(vec![val]))],
        )
        .await?;
    }

    // Measurement: build snapshot at latest (v2); CRC is at v0 (two versions behind)
    let (measure_engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 3); // v0, v1, v2

    // Tail replay: commits v1 and v2 (after the CRC at v0) are replayed via JSON
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 2);
    // CRC is loaded from storage as a fallback (P+M not found in the tail commits)
    assert!(reporter.storage_read_calls.get() >= 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 7: checkpoint behind latest version (with multiple tail commits)
// ---------------------------------------------------------------------------

/// A checkpoint that is multiple versions behind the latest forces a longer tail replay.
/// Checkpoint at v1 plus 3 additional commits (v2, v3, v4) verifies that all tail commits
/// are read in a single JSON call and both byte counters are non-zero. The specific counts
/// here reflect this table's setup: checkpoint at v1, three tail commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_with_multiple_tail_commits_emits_expected_metrics() -> DeltaResult<()> {
    let (table_url, setup_engine, _temp_dir) = setup_table_with_v1_checkpoint().await?;

    // commits 2, 3, 4: insert more data after the checkpoint
    for val in [2i32, 3, 4] {
        let snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
        let _ = insert_data(
            snap,
            &setup_engine,
            vec![Arc::new(Int32Array::from(vec![val]))],
        )
        .await?;
    }

    let (measure_engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    // Checkpoint at v1 -- listing starts from v2; tail is v2, v3, v4
    assert_eq!(reporter.commit_files.get(), 3);
    assert_eq!(reporter.checkpoint_files.get(), 1);
    // All 3 tail commits read in a single JSON call; checkpoint in a single Parquet call
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 3);
    assert_eq!(reporter.parquet_read_calls.get(), 1);
    assert!(reporter.json_bytes_read.get() > 0);
    assert!(reporter.parquet_bytes_read.get() > 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 8: on-demand domain metadata query incurs additional log replay
// ---------------------------------------------------------------------------

/// `snapshot.get_domain_metadata()` always performs a full log replay when no CRC is
/// present at the target version. Calling it after a snapshot is already built generates
/// a second round of JSON reads, demonstrating that on-demand metadata queries carry
/// their own I/O cost.
///
/// The specific count (`json_read_calls = 1`) reflects this test's table: 2 commits, no
/// checkpoint, no CRC. Tables with different log structures will produce different counts.
#[tokio::test]
async fn get_domain_metadata_incurs_additional_log_replay() -> DeltaResult<()> {
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

    let (engine, reporter) = measuring_engine(store);
    let snap = Snapshot::builder_for(table_url).build(&engine)?;

    // Snapshot build reads both commit files in one JSON call
    assert_eq!(reporter.json_read_calls.get(), 1);

    // Reset so the domain metadata query cost is isolated
    reporter.reset();
    let _ = snap.get_domain_metadata("myapp.config", &engine)?;

    assert_eq!(
        reporter.json_read_calls.get(),
        1,
        "get_domain_metadata replays the log, incurring one additional JSON read call"
    );
    assert!(reporter.json_files_read.get() > 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 9: snapshot.transaction() on a clustered table
// ---------------------------------------------------------------------------
// NOTE: This scenario is verified via clustering_e2e.rs tests because the
// `clustered-table` Rust feature is not exposed to the integration test binary.
// The mechanism: `transaction()` unconditionally calls `get_clustering_columns_physical()`,
// which for ClusteredTable-enabled tables does a full domain-metadata log replay for
// `delta.clustering`. Reset the reporter after snapshot build and call `transaction()`
// to isolate that I/O cost.
