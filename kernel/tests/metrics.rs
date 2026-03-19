//! Integration tests that verify [`CountingReporter`] metrics across different table
//! states: delta-only commits, v1 parquet checkpoint (with and without a tail commit),
//! log compaction, and the CRC fast-path that bypasses JSON replay entirely.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::ObjectStore as _;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};

use test_utils::{
    actions_to_string, add_commit, compacted_log_path_for_versions, insert_data,
    test_table_setup_mt, CountingReporter, TestAction,
};
use url::Url;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `DefaultEngine` + `CountingReporter` backed by `store`, for use in the
/// *measurement* phase (building a snapshot and asserting metric counters).
macro_rules! measuring_engine {
    ($store:expr) => {{
        let reporter = Arc::new(CountingReporter::default());
        let engine = DefaultEngineBuilder::new($store as Arc<_>)
            .with_metrics_reporter(reporter.clone())
            .build();
        (engine, reporter)
    }};
}

// ---------------------------------------------------------------------------
// Scenario 1: delta-only (2 commits, no checkpoint, no compaction)
// ---------------------------------------------------------------------------

/// A snapshot built from raw JSON commits -- no checkpoint, no CRC, no compaction --
/// reports exactly the commit file count and triggers one JSON read call covering all
/// commit files.
#[tokio::test]
async fn delta_only_snapshot_emits_expected_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let table_root = "memory:///";

    // commit 0: protocol + metadata; commit 1: add one file
    add_commit(
        table_root,
        store.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        store.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("file1.parquet".into())]),
    )
    .await?;

    let (engine, reporter) = measuring_engine!(store);
    let _snap = Snapshot::builder_for(Url::parse(table_root)?).build(&engine)?;

    // Operation-level metrics
    assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 2);
    assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 0);
    assert_eq!(reporter.compaction_files.load(Ordering::Relaxed), 0);

    // Handler-level metrics: all 2 commit files are read in a single json call
    assert_eq!(reporter.json_read_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.json_files_read.load(Ordering::Relaxed), 2);
    assert_eq!(reporter.parquet_read_calls.load(Ordering::Relaxed), 0);

    // At least one listing call was made to scan the delta log
    assert!(reporter.list_calls.load(Ordering::Relaxed) >= 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 2: v1 parquet checkpoint + one tail commit
// ---------------------------------------------------------------------------

/// After a v1 parquet checkpoint is written at version 1 and a further commit is added,
/// a fresh snapshot sees one checkpoint file, one tail commit, and performs a single
/// parquet read (checkpoint) plus a single JSON read (tail commit).
///
/// Uses `TokioMultiThreadExecutor` for the setup engine because `snapshot.checkpoint()`
/// internally issues nested `block_on` calls (reading existing actions while writing the
/// checkpoint file). `TokioBackgroundExecutor` deadlocks in this pattern; the multi-thread
/// executor uses `block_in_place` which handles re-entrant blocking safely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_with_v1_checkpoint_and_tail_commit_emits_expected_metrics() -> DeltaResult<()> {
    use delta_kernel::arrow::array::Int32Array;
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::schema::{DataType, StructField, StructType};

    let (_temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    // commit 0: create table via the transaction API
    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    // commit 1: insert data
    let snap0 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let result = insert_data(
        snap0,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?;
    let CommitResult::CommittedTransaction(committed) = result else {
        panic!("expected CommittedTransaction");
    };
    let snap1 = committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone();

    // Write v1 parquet checkpoint at version 1
    snap1.checkpoint(setup_engine.as_ref())?;

    // commit 2: one more JSON commit after the checkpoint
    add_commit(
        table_url.as_str(),
        &LocalFileSystem::new(),
        2,
        actions_to_string(vec![TestAction::Add("extra.parquet".into())]),
    )
    .await
    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    // Measurement: fresh engine with reporter backed by the same local file store
    let reporter = Arc::new(CountingReporter::default());
    let measure_engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()) as Arc<_>)
        .with_metrics_reporter(reporter.clone())
        .build();
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 1);
    // Only the tail commit (version 2) remains in ascending_commit_files after checkpoint filtering
    assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.compaction_files.load(Ordering::Relaxed), 0);

    // One JSON read for the single tail commit; one Parquet read for the checkpoint
    assert_eq!(reporter.json_read_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.json_files_read.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.parquet_read_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.parquet_files_read.load(Ordering::Relaxed), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 3: v1 parquet checkpoint at latest version (no tail commits)
// ---------------------------------------------------------------------------

/// When the latest version has a checkpoint and no subsequent commits exist, the snapshot
/// has zero commit files and the JSON handler is called with an empty file list.
///
/// Uses `TokioMultiThreadExecutor` for the setup engine for the same re-entrant
/// `block_on` reason as the with-tail variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_at_checkpoint_tip_emits_expected_metrics() -> DeltaResult<()> {
    use delta_kernel::arrow::array::Int32Array;
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::schema::{DataType, StructField, StructType};

    let (_temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    let snap0 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let result = insert_data(
        snap0,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?;
    let CommitResult::CommittedTransaction(committed) = result else {
        panic!("expected CommittedTransaction");
    };
    let snap1 = committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone();

    // Checkpoint at version 1; no further commits
    snap1.checkpoint(setup_engine.as_ref())?;

    let reporter = Arc::new(CountingReporter::default());
    let measure_engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()) as Arc<_>)
        .with_metrics_reporter(reporter.clone())
        .build();
    let _snap = Snapshot::builder_for(table_url).build(&measure_engine)?;

    assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 1);
    // No tail commits after the checkpoint
    assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 0);
    assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.compaction_files.load(Ordering::Relaxed), 0);

    // JSON handler is called with zero files (no tail commits); Parquet reads the checkpoint
    assert_eq!(reporter.json_read_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.json_files_read.load(Ordering::Relaxed), 0);
    assert_eq!(reporter.parquet_read_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.parquet_files_read.load(Ordering::Relaxed), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 4: log compaction covering early commits + one tail commit
// ---------------------------------------------------------------------------

/// When early commits are covered by a compacted log file, the snapshot reports both
/// the individual commit count and the compaction count. The JSON handler reads the
/// compaction file and the tail commit in a single call (the minimal cover).
#[tokio::test]
async fn snapshot_with_log_compaction_emits_expected_metrics(
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let table_root = "memory:///";

    // commits 0, 1, 2: individual JSON commits
    add_commit(
        table_root,
        store.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        store.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("file1.parquet".into())]),
    )
    .await?;
    add_commit(
        table_root,
        store.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("file2.parquet".into())]),
    )
    .await?;

    // Write a compacted log file covering versions 0-2.
    // The content mirrors what the kernel would produce: protocol + metadata + add actions.
    let compacted_content = format!(
        "{}\n{}\n{}",
        actions_to_string(vec![TestAction::Metadata]),
        r#"{"add":{"path":"file1.parquet","partitionValues":{},"size":262,"modificationTime":1587968586000,"dataChange":true}}"#,
        r#"{"add":{"path":"file2.parquet","partitionValues":{},"size":262,"modificationTime":1587968586000,"dataChange":true}}"#,
    );
    let compaction_path = compacted_log_path_for_versions(0, 2, "json");
    store
        .put(
            &delta_kernel::object_store::path::Path::from(format!("{}", compaction_path).as_str()),
            compacted_content.into(),
        )
        .await?;

    // commit 3: tail commit after the compaction
    add_commit(
        table_root,
        store.as_ref(),
        3,
        actions_to_string(vec![TestAction::Add("file3.parquet".into())]),
    )
    .await?;

    let (engine, reporter) = measuring_engine!(store);
    let _snap = Snapshot::builder_for(Url::parse(table_root)?).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 1);
    // ascending_commit_files contains all 4 individual .json files (0, 1, 2, 3)
    assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 4);
    assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 0);
    assert_eq!(reporter.compaction_files.load(Ordering::Relaxed), 1);

    // find_commit_cover selects [3.json, 0.2.compacted.json] -- 2 JSON files, one read call
    assert_eq!(reporter.json_read_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.json_files_read.load(Ordering::Relaxed), 2);
    assert_eq!(reporter.parquet_read_calls.load(Ordering::Relaxed), 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 5: CRC fast-path bypasses JSON replay
// ---------------------------------------------------------------------------

/// When a CRC file exists at the target snapshot version, Protocol+Metadata are loaded
/// directly from it, skipping all JSON log replay. The JSON handler is never called.
#[tokio::test]
async fn snapshot_with_crc_at_target_version_skips_json_replay(
) -> Result<(), Box<dyn std::error::Error>> {
    // The crc-full golden table has commit 0 + a CRC file at version 0.
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/"))?;
    let table_root = Url::from_directory_path(path).map_err(|_| "invalid path")?;

    let store = Arc::new(LocalFileSystem::new());
    let (engine, reporter) = measuring_engine!(store);
    let _snap = Snapshot::builder_for(table_root).build(&engine)?;

    assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 1);
    assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 0);

    // CRC at target version -- P+M are loaded from CRC, no JSON or Parquet reads needed
    assert_eq!(reporter.json_read_calls.load(Ordering::Relaxed), 0);
    assert_eq!(reporter.parquet_read_calls.load(Ordering::Relaxed), 0);

    Ok(())
}
