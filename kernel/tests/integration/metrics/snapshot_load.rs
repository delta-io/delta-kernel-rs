//! Metrics tests for snapshot loading and on-demand snapshot APIs.
//!
//! Each test builds a table in a known state, attaches a `CountingReporter`, and asserts the
//! exact metric counters a real connector would observe.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::to_json_bytes;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::snapshot::IncrementalReplay;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Snapshot};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{insert_data, test_table_setup, test_table_setup_mt};
use url::Url;

use super::{
    insert_rows, measuring_engine, setup_table_with_v1_checkpoint, simple_schema, LogState,
    TestTableBuilder,
};

// ============================================================================
// Scenario 1: delta-only (2 commits, no checkpoint, no compaction)
// ============================================================================

/// A snapshot built from two JSON commits -- no checkpoint, no CRC, no compaction --
/// reports exactly the commit file count and triggers one JSON read call covering all
/// commit files.
#[test]
fn delta_only_snapshot_emits_expected_metrics() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_data(1, 1)
        .build()?;

    let (engine, reporter, _guard) = measuring_engine(table.store().clone());
    let _snap = Snapshot::builder_for(table.table_root()).build(&engine)?;

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

// ============================================================================
// Scenario 2: v1 parquet checkpoint + one tail commit
// ============================================================================
// TODO: migrate to TestTableBuilder when checkpoint LogState variants land (#2284)

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
    .await?
    .unwrap_committed();

    let (measure_engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
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
    // On Windows (NTFS), listing a file written moments earlier can return size=0 because
    // the OS has not yet flushed size metadata to the directory entry. bytes_read is sourced
    // from these FileMeta::size values, so it can be 0 even when real I/O occurred.
    #[cfg(not(windows))]
    assert!(reporter.json_bytes_read.get() > 0);
    #[cfg(not(windows))]
    assert!(reporter.parquet_bytes_read.get() > 0);

    Ok(())
}

// ============================================================================
// Scenario 3: v1 parquet checkpoint at latest version (no tail commits)
// ============================================================================
// TODO: migrate to TestTableBuilder when checkpoint LogState variants land (#2284)

/// When the latest version has a checkpoint and no subsequent commits exist, the snapshot
/// has zero commit files and the JSON handler is called with an empty file list.
/// CommitReader always invokes read_json_files even for an empty commit cover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_at_checkpoint_tip_emits_expected_metrics() -> DeltaResult<()> {
    let (table_url, _setup_engine, _temp_dir) = setup_table_with_v1_checkpoint().await?;

    let (measure_engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
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

// ============================================================================
// Scenario 4: log compaction covering early commits + one tail commit
// ============================================================================

// TODO: migrate to TestTableBuilder when compaction LogState variants land (#2284)

/// When early commits are covered by a compacted log file, the snapshot reports both
/// the individual commit count and the compaction count. The JSON handler reads the
/// compaction file and the tail commit in a single call (the minimal cover).
// TODO(#2337): re-enable when log compaction is re-enabled
#[ignore = "log compaction is temporarily disabled (#2337)"]
#[tokio::test]
async fn snapshot_with_log_compaction_emits_expected_metrics() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2))
        .with_schema(simple_schema())
        .with_data(1, 1)
        .build()?;
    let store = table.store().clone();
    let table_url = Url::parse(table.table_root()).unwrap();
    let setup_engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());

    // Write a compacted log file covering versions 0-2 using the public API
    let snap2 = Snapshot::builder_for(table.table_root()).build(setup_engine.as_ref())?;
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
    insert_rows(&table_url, &setup_engine, 3, 1).await?;

    let (engine, reporter, _guard) = measuring_engine(store);
    let _snap = Snapshot::builder_for(table.table_root()).build(&engine)?;

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

// ============================================================================
// Scenario 5: CRC fast-path bypasses JSON replay
// ============================================================================
// TODO: migrate to TestTableBuilder when CRC LogState variants land (#2284)

/// When a CRC file exists at the target snapshot version, Protocol+Metadata are loaded
/// directly from it, skipping all JSON log replay. The JSON handler is never called.
#[tokio::test]
async fn snapshot_with_crc_at_target_version_skips_json_replay() -> DeltaResult<()> {
    // The crc-full golden table has commit 0 + a CRC file at version 0.
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/"))
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let table_root =
        Url::from_directory_path(path).map_err(|_| delta_kernel::Error::generic("invalid path"))?;

    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
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

// ============================================================================
// Scenario 6: CRC at a prior version (CRC exists but is older than latest)
// ============================================================================
// TODO: migrate to TestTableBuilder when CRC LogState variants land (#2284)

/// When a CRC file exists at an older version than the snapshot, the kernel advances it to the
/// target version via reverse-replay: it reads the tail commits (those after the CRC version)
/// once and reads the base CRC from storage. P&M come from the advanced CRC, so there is no
/// separate P&M scan.
///
/// This is a normal, expected table state -- having a CRC from a previous version is
/// not an error or degraded condition.
///
/// `write_checksum` requires stats from a post-commit snapshot (not a freshly built
/// one), so this test uses `create_table` commit -> `post_commit_snapshot` ->
/// `write_checksum` to write the CRC at v0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crc_at_prior_version_advances_via_reverse_replay() -> DeltaResult<()> {
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
    let mut snap = create_committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone();
    for val in [1i32, 2] {
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

    // Measurement: build snapshot at latest (v2); CRC is at v0 (two versions behind)
    let (measure_engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let snap = Snapshot::builder_for(table_url)
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .build(&measure_engine)?;
    assert_eq!(snap.crc().expect("stale CRC advanced to v2").version, 2);

    assert_eq!(reporter.snapshot_completions.get(), 1);
    assert_eq!(reporter.log_segment_loads.get(), 1);
    assert_eq!(reporter.commit_files.get(), 3); // v0, v1, v2

    // Reverse-replay reads the tail commits v1 and v2 (after the CRC at v0) in one JSON call.
    assert_eq!(reporter.json_read_calls.get(), 1);
    assert_eq!(reporter.json_files_read.get(), 2);
    // The base CRC at v0 is read from storage.
    assert!(reporter.storage_read_calls.get() >= 1);

    Ok(())
}

// ============================================================================
// Scenario 7: checkpoint behind latest version (with multiple tail commits)
// ============================================================================
// TODO: migrate to TestTableBuilder when checkpoint LogState variants land (#2284)

/// A checkpoint that is multiple versions behind the latest forces a longer tail replay.
/// Checkpoint at v1 plus 3 additional commits (v2, v3, v4) verifies that all tail commits
/// are read in a single JSON call and both byte counters are non-zero. The specific counts
/// here reflect this table's setup: checkpoint at v1, three tail commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_with_multiple_tail_commits_emits_expected_metrics() -> DeltaResult<()> {
    let (table_url, setup_engine, _temp_dir) = setup_table_with_v1_checkpoint().await?;

    // commits 2, 3, 4: insert more data after the checkpoint
    let mut snap = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    for val in [2i32, 3, 4] {
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

    let (measure_engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
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
    // See Scenario 2 comment: Windows NTFS may report stale size=0 for recently written files.
    #[cfg(not(windows))]
    assert!(reporter.json_bytes_read.get() > 0);
    #[cfg(not(windows))]
    assert!(reporter.parquet_bytes_read.get() > 0);

    Ok(())
}

// ============================================================================
// Scenario 8: on-demand domain metadata query incurs additional log replay
// ============================================================================

/// `snapshot.get_domain_metadata()` always performs a full log replay when no CRC is
/// present at the target version. Calling it after a snapshot is already built generates
/// a second round of JSON reads, demonstrating that on-demand metadata queries carry
/// their own I/O cost.
///
/// The specific count (`json_read_calls = 1`) reflects this test's table: 2 commits, no
/// checkpoint, no CRC. Tables with different log structures will produce different counts.
#[test]
fn get_domain_metadata_when_no_latest_crc_incurs_additional_log_replay() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_data(1, 1)
        .build()?;

    let (engine, reporter, _guard) = measuring_engine(table.store().clone());
    let snap = Snapshot::builder_for(table.table_root()).build(&engine)?;

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

// ============================================================================
// Scenario 9: domain metadata and set-transaction loads
// ============================================================================

// Creates a table with clustering and rowTracking enabled (two system domain metadatas), plus a
// user domain metadata and a SetTransaction.
async fn setup_table_with_dms_and_set_txns(
    write_crc: bool,
) -> DeltaResult<(Url, tempfile::TempDir)> {
    let (temp_dir, table_path, engine) = test_table_setup()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let committer = || Box::new(FileSystemCommitter::new());

    let properties = HashMap::from([("delta.enableRowTracking".to_string(), "true".to_string())]);
    let snap_v0 = create_table(&table_path, simple_schema(), "Test/1.0")
        .with_table_properties(properties)
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let snap_v1 = snap_v0
        .transaction(committer(), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_domain_metadata("myapp.config".to_string(), "v1".to_string())
        .with_transaction_id("my-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    if write_crc {
        snap_v1.write_checksum(engine.as_ref())?;
    }
    Ok((table_url, temp_dir))
}

#[rstest]
#[case::log_replay(false)]
#[case::crc_cache(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_transaction_loads_domain_metadata_internally(
    #[case] with_crc: bool,
) -> DeltaResult<()> {
    let (table_url, _temp_dir) = setup_table_with_dms_and_set_txns(with_crc).await?;

    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let engine = Arc::new(engine);
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Isolate the transaction's internal loads from the snapshot build.
    reporter.reset();
    insert_data(snap, &engine, vec![Arc::new(Int32Array::from(vec![1]))])
        .await?
        .unwrap_post_commit_snapshot();

    // Clustering domain (creating the transaction) + row-tracking domain (committing the append).
    assert_eq!(reporter.domain_metadata_loads.get(), 2);
    assert_eq!(
        reporter.domain_metadata_cache_hits.get(),
        2 * with_crc as u64
    );
    assert_eq!(reporter.domain_metadata_domains_returned.get(), 2);

    Ok(())
}

#[rstest]
#[case::log_replay(false)]
#[case::crc_cache(true)]
#[tokio::test]
async fn get_app_id_version_load_emits_loaded_metric(#[case] with_crc: bool) -> DeltaResult<()> {
    let (table_url, _temp_dir) = setup_table_with_dms_and_set_txns(with_crc).await?;

    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let snap = Snapshot::builder_for(table_url).build(&engine)?;

    // Hit on an app id with a committed transaction.
    reporter.reset();
    assert_eq!(snap.get_app_id_version("my-app", &engine)?, Some(1));
    assert_eq!(reporter.set_transaction_loads.get(), 1);
    assert_eq!(reporter.set_transaction_cache_hits.get(), with_crc as u64);
    assert_eq!(reporter.set_transaction_found.get(), 1);

    // Miss on an unknown app id. A Complete CRC answers the miss from cache; without a CRC it
    // falls back to log replay.
    reporter.reset();
    assert_eq!(snap.get_app_id_version("no-such-app", &engine)?, None);
    assert_eq!(reporter.set_transaction_loads.get(), 1);
    assert_eq!(reporter.set_transaction_cache_hits.get(), with_crc as u64);
    assert_eq!(reporter.set_transaction_found.get(), 0);

    Ok(())
}

#[rstest]
#[case::log_replay(false)]
#[case::crc_cache(true)]
#[tokio::test]
async fn get_domain_metadata_load_emits_loaded_metric(#[case] with_crc: bool) -> DeltaResult<()> {
    let (table_url, _temp_dir) = setup_table_with_dms_and_set_txns(with_crc).await?;

    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let snap = Snapshot::builder_for(table_url).build(&engine)?;

    // Hit on the user domain.
    reporter.reset();
    assert!(snap.get_domain_metadata("myapp.config", &engine)?.is_some());
    assert_eq!(reporter.domain_metadata_loads.get(), 1);
    assert_eq!(reporter.domain_metadata_cache_hits.get(), with_crc as u64);
    assert_eq!(reporter.domain_metadata_domains_returned.get(), 1);

    // Miss on a nonexistent domain. A Complete CRC answers the miss from cache; without a CRC it
    // falls back to log replay.
    reporter.reset();
    assert!(snap
        .get_domain_metadata("does.not.exist", &engine)?
        .is_none());
    assert_eq!(reporter.domain_metadata_loads.get(), 1);
    assert_eq!(reporter.domain_metadata_cache_hits.get(), with_crc as u64);
    assert_eq!(reporter.domain_metadata_domains_returned.get(), 0);

    Ok(())
}

#[tokio::test]
async fn failed_loads_emit_no_metric() -> DeltaResult<()> {
    let (table_url, _temp_dir) = setup_table_with_dms_and_set_txns(false).await?;

    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let snap = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // Remove a commit so the log has a gap and the replay-based load fails.
    let log_dir = table_url.to_file_path().unwrap().join("_delta_log");
    std::fs::remove_file(log_dir.join("00000000000000000001.json")).unwrap();

    reporter.reset();
    assert!(snap.get_domain_metadata("myapp.config", &engine).is_err());
    assert!(snap.get_app_id_version("my-app", &engine).is_err());
    assert_eq!(reporter.domain_metadata_loads.get(), 0);
    assert_eq!(reporter.set_transaction_loads.get(), 0);

    Ok(())
}
