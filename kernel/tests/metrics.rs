//! Integration tests that verify [`CountingReporter`] metrics across different table
//! states and API call patterns.
//!
//! All tests use only public kernel APIs (`create_table`, `Transaction`, `write_parquet`,
//! `log_compaction_writer`, `scan.execute()`) to ensure that the metrics surface is
//! verified through the same code paths a real engine connector would exercise.
//!
//! Scenarios covered:
//! - Delta-only snapshot (baseline)
//! - V1 checkpoint with/without tail commits
//! - Log compaction
//! - CRC fast-path (P+M bypasses JSON replay)
//! - Stale CRC (CRC older than latest version -- tail replay + CRC fallback)
//! - Stale checkpoint (checkpoint + multiple tail commits)
//! - On-demand domain metadata query (`get_domain_metadata`)
//! - `snapshot.transaction()` on a clustered table (loads clustering metadata)
//! - Scan data-file reads (`scan.execute()` contributes parquet I/O)
//! - Incremental snapshot update (`Snapshot::builder_from` replays only new commits)

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::engine::to_json_bytes;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStore as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine, Snapshot};

use test_utils::{insert_data, test_table_setup_mt, CountingReporter};
use url::Url;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `DefaultEngine` + `CountingReporter` backed by `store`, for use in the
/// *measurement* phase (building a snapshot and asserting metric counters).
fn measuring_engine(
    store: Arc<dyn delta_kernel::object_store::ObjectStore>,
) -> (
    DefaultEngine<delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor>,
    Arc<CountingReporter>,
) {
    let reporter = Arc::new(CountingReporter::default());
    let engine = DefaultEngineBuilder::new(store)
        .with_metrics_reporter(reporter.clone())
        .build();
    (engine, reporter)
}

/// Build a minimal single-column INTEGER schema for test tables.
fn simple_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![StructField::nullable("id", DataType::INTEGER)])
            .expect("valid schema"),
    )
}

/// Create a table at v0, insert one row, and write a v1 parquet checkpoint.
///
/// Returns `(table_url, setup_engine, _temp_dir)` where `_temp_dir` must be kept
/// alive for the duration of the test to prevent early cleanup. Callers can add
/// further commits via `insert_data` or directly build the measuring snapshot.
///
/// Uses `TokioMultiThreadExecutor` because `snapshot.checkpoint()` issues nested
/// `block_on` calls; `TokioBackgroundExecutor` deadlocks in that pattern.
async fn setup_table_with_v1_checkpoint() -> DeltaResult<(
    Url,
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    tempfile::TempDir,
)> {
    let (temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let _ = create_table(&table_path, simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    let snap0 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let committed = insert_data(
        snap0,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?
    .unwrap_committed();
    committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone()
        .checkpoint(setup_engine.as_ref())?;

    Ok((table_url, setup_engine, temp_dir))
}

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
// Scenario 6: stale CRC (CRC behind latest version)
// ---------------------------------------------------------------------------

/// When a CRC file exists at an older version than the snapshot, the kernel takes the
/// partial-replay path: it replays only the tail commits (those after the CRC version)
/// looking for Protocol/Metadata changes, then falls back to the CRC when none are
/// found. This produces JSON reads for the tail commits AND a storage read for the CRC.
///
/// `write_checksum` requires stats from a post-commit snapshot (not a freshly built
/// one), so this test uses `create_table` commit -> `post_commit_snapshot` ->
/// `write_checksum` to write the CRC at v0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_crc_replays_tail_commits_and_loads_crc_as_fallback() -> DeltaResult<()> {
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

    // Measurement: build snapshot at latest (v2); CRC is stale (at v0, not v2)
    let (measure_engine, reporter) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 3); // v0, v1, v2

    // Tail replay: commits v1 and v2 (after the stale CRC at v0) are replayed via JSON
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 2);
    // CRC is loaded from storage as a fallback (P+M not found in the tail commits)
    assert!(reporter.storage_read_calls.get() >= 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 7: stale checkpoint (checkpoint with many tail commits)
// ---------------------------------------------------------------------------

/// A checkpoint that is multiple versions behind the latest forces a longer tail replay.
/// Checkpoint at v1 plus 3 additional commits verifies that all tail commits are read
/// in a single JSON call and both byte counters are non-zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_checkpoint_with_multiple_tail_commits_emits_expected_metrics() -> DeltaResult<()> {
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
    // Checkpoint at v1 filters out v0 and v1; tail is v2, v3, v4
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

/// `snapshot.get_domain_metadata()` always performs a full log replay (no CRC at
/// target version here). Calling it after a snapshot is already built generates a
/// second round of JSON reads, demonstrating that on-demand metadata queries carry
/// their own I/O cost. `get_clustering_columns` uses the same internal path, so
/// this test covers both APIs.
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
// Scenario 9: snapshot.transaction() on a clustered table loads clustering metadata
// ---------------------------------------------------------------------------
// NOTE: This scenario is verified via clustering_e2e.rs tests because the
// `clustered-table` Rust feature is not exposed to the integration test binary.
// The mechanism: `transaction()` unconditionally calls `get_clustering_columns_physical()`,
// which for ClusteredTable-enabled tables does a full domain-metadata log replay for
// `delta.clustering`. Reset the reporter after snapshot build and call `transaction()`
// to isolate that I/O cost.

// ---------------------------------------------------------------------------
// Scenario 10: scan.execute() contributes parquet data-file reads
// ---------------------------------------------------------------------------

/// `scan.execute()` reads the actual parquet data files written during inserts.
/// These go through `DefaultParquetHandler::read_parquet_files` and appear in
/// `parquet_read_calls`, separately from any checkpoint reads. Resetting the
/// reporter after snapshot construction isolates the scan I/O.
///
/// Note: `scan.execute()` also does its own log replay (to collect Add/Remove
/// actions for scan metadata), so `json_read_calls` is non-zero even after the
/// reporter reset.
#[tokio::test]
async fn scan_execute_contributes_parquet_data_file_reads() -> DeltaResult<()> {
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///").unwrap();
    let setup_engine = Arc::new(DefaultEngineBuilder::new(store.clone() as Arc<_>).build());

    let _ = create_table("memory:///", simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    // Two inserts, each writing one parquet data file
    for val in [1i32, 2] {
        let snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
        let _ = insert_data(
            snap,
            &setup_engine,
            vec![Arc::new(Int32Array::from(vec![val]))],
        )
        .await?;
    }

    let (engine, reporter) = measuring_engine(store);
    let snap = Snapshot::builder_for(table_url).build(&engine)?;

    // Reset after snapshot build to isolate scan I/O
    reporter.reset();

    let engine: Arc<dyn Engine> = Arc::new(engine);
    let mut batches_seen = 0usize;
    for result in snap.scan_builder().build()?.execute(engine)? {
        result?;
        batches_seen += 1;
    }
    assert!(batches_seen > 0, "scan should return rows");

    // scan calls read_parquet_files once per data file (not batched), so 2 calls for 2 files
    assert_eq!(reporter.parquet_read_calls.get(), 2);
    assert_eq!(reporter.parquet_files_read.get(), 2);
    assert!(reporter.parquet_bytes_read.get() > 0);
    // scan.execute() does its own log replay for Add/Remove scan metadata
    assert_eq!(reporter.json_read_calls.get(), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 11: incremental snapshot update replays only new tail commits
// ---------------------------------------------------------------------------

/// `Snapshot::builder_from(existing)` starts from an existing snapshot and replays
/// only the commits that arrived after it. The JSON reads reflect only the new tail
/// commit, not the full history -- demonstrating that incremental updates are cheaper.
///
/// Note: `LogSegmentLoaded` is emitted with the net-new commit count only (the new
/// commits after the existing snapshot version), not the full log history.
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
