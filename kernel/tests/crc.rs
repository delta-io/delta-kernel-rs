//! Integration tests for CRC (version checksum) file-based APIs on Snapshot.

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::crc::{Crc, FileStatsValidity};
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::path::Path;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::{ChecksumWriteResult, Snapshot, SnapshotRef};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Engine, FileStats};
use rstest::rstest;
use test_utils::{add_commit, insert_data, test_table_setup};

// ============================================================================
// File stats from CRC on disk
// ============================================================================

#[tokio::test]
async fn test_get_file_stats_from_crc() -> DeltaResult<()> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/")).unwrap();
    let table_root = url::Url::from_directory_path(path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngineBuilder::new(store, Path::from("")).build();

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    let file_stats = snapshot.get_or_load_file_stats(&engine).unwrap();
    assert_eq!(file_stats.num_files(), 10);
    assert_eq!(file_stats.table_size_bytes(), 5259);
    assert!(file_stats.file_size_histogram().is_some());

    Ok(())
}

#[tokio::test]
async fn test_get_file_stats_no_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, true),
        StructField::new("value", DataType::STRING, true),
    ])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    let file_stats = snapshot.get_or_load_file_stats(engine.as_ref());
    assert_eq!(file_stats, None);

    Ok(())
}

#[tokio::test]
async fn test_get_file_stats_crc_not_at_snapshot_version() -> DeltaResult<()> {
    use test_utils::copy_directory;

    // ===== GIVEN =====
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Copy crc-full table (has CRC at version 0) into the temp dir
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/")).unwrap();
    copy_directory(&source_path, _temp_dir.path()).unwrap();

    // Verify the table starts at version 0 with valid CRC stats
    let snapshot = Snapshot::builder_for(table_path.clone()).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);
    assert!(snapshot.get_or_load_file_stats(engine.as_ref()).is_some());

    // ===== WHEN =====
    // Empty commit to advance to version 1 (no new CRC file written)
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let _ = txn.commit(engine.as_ref())?;

    // ===== THEN =====
    // Load a fresh snapshot at version 1
    let snapshot = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);

    // No CRC at version 1, so file stats should be None
    let file_stats = snapshot.get_or_load_file_stats(engine.as_ref());
    assert_eq!(file_stats, None);

    Ok(())
}

// ============================================================================
// CRC test visibility: get_current_crc_if_loaded_for_testing
// ============================================================================

#[tokio::test]
async fn test_get_current_crc_if_loaded_returns_loaded_crc() -> DeltaResult<()> {
    // ===== GIVEN =====
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/")).unwrap();
    let table_root = url::Url::from_directory_path(path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngineBuilder::new(store, Path::from("")).build();

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    // ===== WHEN =====
    let crc = snapshot.get_current_crc_if_loaded_for_testing().unwrap();

    // ===== THEN =====
    let file_stats = crc.file_stats().unwrap();
    assert_eq!(file_stats.table_size_bytes(), 5259);
    assert_eq!(file_stats.num_files(), 10);
    assert_eq!(crc.num_metadata, 1);
    assert_eq!(crc.num_protocol, 1);

    // Protocol and metadata should match the snapshot's table configuration
    assert_eq!(crc.protocol, *snapshot.table_configuration().protocol());
    assert_eq!(crc.metadata, *snapshot.table_configuration().metadata());

    // Domain metadata
    let dms = crc.domain_metadata.as_ref().unwrap();
    assert_eq!(dms.len(), 3);
    assert!(dms.contains_key("delta.clustering"));
    assert!(dms.contains_key("delta.rowTracking"));
    assert!(dms.contains_key("myApp.metadata"));

    Ok(())
}

#[tokio::test]
async fn test_get_current_crc_if_loaded_returns_none_when_no_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    // No CRC file exists, so get_current_crc_if_loaded_for_testing should return None
    assert!(snapshot.get_current_crc_if_loaded_for_testing().is_none());

    Ok(())
}

// ============================================================================
// Post-commit CRC existence: does a CRC exist on the post-commit snapshot?
// ============================================================================

fn create_table_and_commit(
    table_path: &str,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<delta_kernel::transaction::CommittedTransaction> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);
    let txn = create_table(table_path, schema, "test_engine")
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata("zip".to_string(), "zap0".to_string());

    Ok(txn.commit(engine)?.unwrap_committed())
}

#[tokio::test]
async fn test_create_table_produces_post_commit_crc() -> DeltaResult<()> {
    // ===== GIVEN / WHEN: Create the table =====
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;

    // ===== THEN: should have CRC at v0 =====
    assert_eq!(committed.commit_version(), 0);
    let snapshot = committed.post_commit_snapshot().unwrap();
    let crc = snapshot.get_current_crc_if_loaded_for_testing().unwrap();

    let file_stats = crc.file_stats().unwrap();
    assert_eq!(file_stats.num_files(), 0);
    assert_eq!(file_stats.table_size_bytes(), 0);
    assert_eq!(crc.num_metadata, 1);
    assert_eq!(crc.num_protocol, 1);
    assert_eq!(crc.protocol, *snapshot.table_configuration().protocol());
    assert_eq!(crc.metadata, *snapshot.table_configuration().metadata());
    let dms = crc.domain_metadata.as_ref().unwrap();
    assert_eq!(dms["zip"].configuration(), "zap0");

    Ok(())
}

#[rstest]
#[case::with_in_memory_crc(true)]
#[case::without_crc(false)]
#[tokio::test]
async fn test_post_commit_crc_chains_only_if_read_snapshot_has_crc(
    #[case] use_post_commit_snapshot: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let create_committed = create_table_and_commit(&table_path, engine.as_ref())?;

    let read_snapshot = if use_post_commit_snapshot {
        // Post-commit snapshot has in-memory CRC from the previous commit.
        create_committed.post_commit_snapshot().unwrap().clone()
    } else {
        // Fresh-from-disk snapshot has no CRC (no .crc file on disk).
        Snapshot::builder_for(table_path).build(engine.as_ref())?
    };
    assert_eq!(
        read_snapshot
            .get_current_crc_if_loaded_for_testing()
            .is_some(),
        use_post_commit_snapshot
    );

    let committed = read_snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_domain_metadata("zip".to_string(), "zap1".to_string())
        .commit(engine.as_ref())?
        .unwrap_committed();

    // The new post-commit snapshot should only have a CRC if the read snapshot had one.
    assert_eq!(committed.commit_version(), 1);
    assert_eq!(
        committed
            .post_commit_snapshot()
            .unwrap()
            .get_current_crc_if_loaded_for_testing()
            .is_some(),
        use_post_commit_snapshot
    );

    Ok(())
}

// ================================================================================
// Post-commit CRC correctness: are the CRC fields accurate after write and reload?
// ================================================================================

/// Writes the in-memory CRC to disk, reloads a fresh snapshot, and asserts that the
/// round-tripped CRC matches the in-memory one. Returns the loaded CRC for further assertions.
fn write_and_verify_crc(
    snapshot: &SnapshotRef,
    table_path: &str,
    engine: &dyn delta_kernel::Engine,
) -> Crc {
    let crc_in_memory = snapshot.get_current_crc_if_loaded_for_testing().unwrap();
    snapshot.write_checksum(engine).unwrap();

    let snapshot_fresh = Snapshot::builder_for(table_path).build(engine).unwrap();
    let crc_from_disk = snapshot_fresh
        .get_current_crc_if_loaded_for_testing()
        .unwrap();
    assert_eq!(crc_in_memory, crc_from_disk);
    crc_from_disk.clone()
}

#[tokio::test]
async fn test_post_commit_crc_tracks_file_stats_across_inserts() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // ===== GIVEN: Create the table =====
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap().clone();

    // ===== WHEN: Insert values 1..=10 =====
    let col1: ArrayRef = Arc::new(Int32Array::from((1..=10).collect::<Vec<_>>()));
    let committed = insert_data(snapshot_v0, &engine, vec![col1])
        .await?
        .unwrap_committed();

    // ===== THEN: should have CRC at v1 with right file stats =====
    assert_eq!(committed.commit_version(), 1);
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();
    let crc_v1 = write_and_verify_crc(snapshot_v1, &table_path, engine.as_ref());
    let stats_v1 = crc_v1.file_stats().unwrap();
    assert_eq!(stats_v1.num_files(), 1); // <--- 1 file added
    assert!(stats_v1.table_size_bytes() > 0); // <--- size is non-zero

    // ===== WHEN: Insert values 11..=20 =====
    let col2: ArrayRef = Arc::new(Int32Array::from((11..=20).collect::<Vec<_>>()));
    let committed = insert_data(snapshot_v1.clone(), &engine, vec![col2])
        .await?
        .unwrap_committed();

    // ===== THEN: should have CRC at v2 with right file stats =====
    assert_eq!(committed.commit_version(), 2);
    let snapshot_v2 = committed.post_commit_snapshot().unwrap();
    let crc_v2 = write_and_verify_crc(snapshot_v2, &table_path, engine.as_ref());
    let stats_v2 = crc_v2.file_stats().unwrap();
    assert_eq!(stats_v2.num_files(), 2); // <--- 2 files added
    assert!(stats_v2.table_size_bytes() > stats_v1.table_size_bytes()); // <--- size is greater than after first insert

    // ===== WHEN: Remove all files =====
    let scan = snapshot_v2.clone().scan_builder().build()?;
    let mut txn = snapshot_v2
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("DELETE".to_string())
        .with_data_change(true);
    for sm in scan.scan_metadata(engine.as_ref())? {
        txn.remove_files(sm?.scan_files);
    }
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();

    // ===== THEN: should have CRC at v3 with right file stats =====
    assert_eq!(committed.commit_version(), 3);
    let snapshot_v3 = committed.post_commit_snapshot().unwrap();
    let crc_v3 = write_and_verify_crc(snapshot_v3, &table_path, engine.as_ref());
    let stats_v3 = crc_v3.file_stats().unwrap();
    assert_eq!(stats_v3.num_files(), 0); // <--- 0 net file in the table
    assert_eq!(stats_v3.table_size_bytes(), 0); // <--- size is 0

    Ok(())
}

#[tokio::test]
async fn test_post_commit_crc_tracks_domain_metadata_changes() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // ===== WHEN: CREATE TABLE with zip -> zap0 =====
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();

    // ===== THEN: should have CRC at v0 with zip -> zap0 =====
    let crc_v0 = write_and_verify_crc(snapshot_v0, &table_path, engine.as_ref());
    let dms = crc_v0.domain_metadata.as_ref().unwrap();
    assert_eq!(dms["zip"].configuration(), "zap0");

    // ===== WHEN: update zip -> zap1, add foo -> bar =====
    let txn = snapshot_v0
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_domain_metadata("zip".to_string(), "zap1".to_string()) // <-- set to zap1
        .with_domain_metadata("foo".to_string(), "bar".to_string()); // <-- add foo
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();

    // ===== THEN: should have CRC at v1 with zip -> zap1, foo -> bar =====
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();
    let crc_v1 = write_and_verify_crc(snapshot_v1, &table_path, engine.as_ref());
    let dms = crc_v1.domain_metadata.as_ref().unwrap();
    assert_eq!(dms["zip"].configuration(), "zap1"); // <-- must be zap1
    assert_eq!(dms["foo"].configuration(), "bar"); // <-- must be bar

    // ===== WHEN: remove zip, keep foo =====
    let txn = snapshot_v1
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_domain_metadata_removed("zip".to_string()); // <-- remove zip
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();

    // ===== THEN: should have CRC at v2 with zip gone, foo still there =====
    let snapshot_v2 = committed.post_commit_snapshot().unwrap();
    let crc_v2 = write_and_verify_crc(snapshot_v2, &table_path, engine.as_ref());
    let dms = crc_v2.domain_metadata.as_ref().unwrap();
    assert!(!dms.contains_key("zip")); // <-- must be gone
    assert_eq!(dms["foo"].configuration(), "bar"); // <-- must still be bar

    Ok(())
}

#[tokio::test]
async fn test_post_commit_crc_non_incremental_op_makes_file_stats_indeterminate() -> DeltaResult<()>
{
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // ===== GIVEN: Create table (v0) and insert data (v1) =====
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap().clone();

    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let committed = insert_data(snapshot_v0, &engine, vec![col])
        .await?
        .unwrap_committed();
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();

    // ===== WHEN: Commit a non-incremental operation (ANALYZE STATS) =====
    let committed = snapshot_v1
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("ANALYZE STATS".to_string())
        .commit(engine.as_ref())?
        .unwrap_committed();

    // ===== THEN: CRC at v2 has indeterminate file stats =====
    assert_eq!(committed.commit_version(), 2);
    let snapshot_v2 = committed.post_commit_snapshot().unwrap();
    let crc_v2 = snapshot_v2.get_current_crc_if_loaded_for_testing().unwrap();
    assert_eq!(crc_v2.file_stats_validity, FileStatsValidity::Indeterminate);

    Ok(())
}

// ============================================================================
// Write checksum to disk
// ============================================================================

#[tokio::test]
async fn test_write_checksum_success_simple() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot = committed.post_commit_snapshot().unwrap();

    let (result, _updated) = snapshot.write_checksum(engine.as_ref())?;
    assert_eq!(result, ChecksumWriteResult::Written);

    // Verify the CRC file is readable by loading a fresh snapshot from disk
    let fresh_snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_snapshot
        .get_current_crc_if_loaded_for_testing()
        .is_some());

    Ok(())
}

#[rstest]
#[case::same_snapshot(false)]
#[case::fresh_snapshot(true)]
#[tokio::test]
async fn test_write_checksum_double_write_returns_already_exists(
    #[case] reload_snapshot: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot = committed.post_commit_snapshot().unwrap();

    let (first, updated) = snapshot.write_checksum(engine.as_ref())?;
    assert_eq!(first, ChecksumWriteResult::Written);

    let second = if reload_snapshot {
        let fresh = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        let (result, _) = fresh.write_checksum(engine.as_ref())?;
        result
    } else {
        let (result, _) = updated.write_checksum(engine.as_ref())?;
        result
    };
    assert_eq!(second, ChecksumWriteResult::AlreadyExists);

    Ok(())
}

#[tokio::test]
async fn test_write_checksum_with_no_in_memory_crc_returns_error() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let _ = create_table_and_commit(&table_path, engine.as_ref())?;

    // Load from disk -- no CRC file on disk, so no in-memory CRC
    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;

    let result = snapshot.write_checksum(engine.as_ref());
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_in_memory_crc_chains_across_multiple_commits_then_writes() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let mut snapshot = committed.post_commit_snapshot().unwrap().clone();
    assert!(snapshot.get_current_crc_if_loaded_for_testing().is_some());

    // Chain several commits without writing CRC to disk
    for i in 0..5 {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![i]));
        let committed = insert_data(snapshot, &engine, vec![col])
            .await?
            .unwrap_committed();
        snapshot = committed.post_commit_snapshot().unwrap().clone();
        assert!(
            snapshot.get_current_crc_if_loaded_for_testing().is_some(),
            "in-memory CRC lost at commit {}",
            committed.commit_version()
        );
    }

    // Only now write the CRC -- should have accumulated all 5 inserts
    assert_eq!(snapshot.version(), 5);
    let crc = write_and_verify_crc(&snapshot, &table_path, engine.as_ref());
    let crc_stats = crc.file_stats().unwrap();
    assert_eq!(crc_stats.num_files(), 5);
    assert!(crc_stats.table_size_bytes() > 0);

    // Verify histogram totals match disk ground truth
    let disk_sizes = parquet_file_sizes_on_disk(&table_path);
    assert_eq!(disk_sizes.len(), 5);
    assert_histogram_totals(&crc_stats, 5, disk_sizes.iter().sum());

    Ok(())
}

// When an incremental snapshot update picks up a CRC file from the new log segment, the loaded
// CRC data should be preserved in the resulting snapshot (not discarded by creating a second
// LazyCrc). This verifies that compute_post_commit_crc can find the CRC without additional I/O.
#[tokio::test]
async fn test_incremental_snapshot_preserves_loaded_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create table at v0 and write its CRC to disk
    let committed_v0 = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed_v0.post_commit_snapshot().unwrap();
    snapshot_v0.write_checksum(engine.as_ref())?;

    // Insert data at v1 and write its CRC to disk
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let committed_v1 = insert_data(snapshot_v0.clone(), &engine, vec![col])
        .await?
        .unwrap_committed();
    committed_v1
        .post_commit_snapshot()
        .unwrap()
        .write_checksum(engine.as_ref())?;

    // Load a fresh snapshot at v0 (from disk, not post-commit)
    let fresh_v0 = Snapshot::builder_for(&table_path)
        .at_version(0)
        .build(engine.as_ref())?;
    assert_eq!(fresh_v0.version(), 0);

    // Incrementally update from v0 -> v1
    let incremental_v1 = Snapshot::builder_from(fresh_v0).build(engine.as_ref())?;
    assert_eq!(incremental_v1.version(), 1);

    // The CRC at v1 should be loaded from the incremental update (not discarded)
    assert_eq!(incremental_v1.crc_version_for_testing(), Some(1));
    assert!(
        incremental_v1
            .get_current_crc_if_loaded_for_testing()
            .is_some(),
        "CRC should be loaded at v1 after incremental snapshot update"
    );

    // Committing from this snapshot should produce a post-commit CRC (proves
    // compute_post_commit_crc found the loaded CRC and applied the delta)
    let col: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
    let committed_v2 = insert_data(incremental_v1, &engine, vec![col])
        .await?
        .unwrap_committed();
    assert_eq!(committed_v2.commit_version(), 2);
    let snapshot_v2 = committed_v2.post_commit_snapshot().unwrap();
    assert!(
        snapshot_v2
            .get_current_crc_if_loaded_for_testing()
            .is_some(),
        "Post-commit CRC should chain from incremental snapshot's CRC"
    );

    Ok(())
}

// Incremental update where only the old segment has a CRC file (no new CRC written).
// The old CRC is preserved and the LazyCrc is reused from the old snapshot, but since
// it's at v0 while the snapshot is at v1, it won't be reported as loaded at the
// snapshot's version.
#[tokio::test]
async fn test_incremental_snapshot_old_crc_no_new_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create table at v0 and write CRC to disk
    let committed_v0 = create_table_and_commit(&table_path, engine.as_ref())?;
    committed_v0
        .post_commit_snapshot()
        .unwrap()
        .write_checksum(engine.as_ref())?;

    // Insert data at v1 -- do NOT write CRC for v1
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let committed_v1 = insert_data(
        committed_v0.post_commit_snapshot().unwrap().clone(),
        &engine,
        vec![col],
    )
    .await?
    .unwrap_committed();
    assert_eq!(committed_v1.commit_version(), 1);

    // Load a fresh snapshot at v0 (this loads 0.crc during P&M reading)
    let fresh_v0 = Snapshot::builder_for(&table_path)
        .at_version(0)
        .build(engine.as_ref())?;
    assert!(
        fresh_v0.get_current_crc_if_loaded_for_testing().is_some(),
        "Fresh v0 snapshot should have CRC loaded from 0.crc"
    );

    // Incrementally update from v0 -> v1. The new listing (starting at v1) doesn't find
    // any CRC file, so it falls back to the old segment's 0.crc. Since the old snapshot's
    // LazyCrc is at the same version, it is reused (may already be loaded in memory).
    let incremental_v1 = Snapshot::builder_from(fresh_v0).build(engine.as_ref())?;
    assert_eq!(incremental_v1.version(), 1);

    // The CRC is at v0, not v1, so it won't be reported as loaded at v1
    assert!(
        incremental_v1
            .get_current_crc_if_loaded_for_testing()
            .is_none(),
        "CRC at v0 should not be reported as loaded at v1 (version mismatch)"
    );

    Ok(())
}

// CRC should always write domainMetadata as an empty list (not omit the field) when there are
// no domain metadata actions, regardless of whether the feature is supported.
#[rstest]
#[case::dm_feature_supported(true)]
#[case::dm_feature_not_supported(false)]
#[tokio::test]
async fn test_write_checksum_with_no_dms_writes_empty_list(
    #[case] dm_supported: bool,
) -> DeltaResult<()> {
    use std::collections::HashMap;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    let mut builder = create_table(&table_path, schema, "test_engine");
    if dm_supported {
        let properties = HashMap::from([(
            "delta.feature.domainMetadata".to_string(),
            "supported".to_string(),
        )]);
        builder = builder.with_table_properties(properties);
    }
    let committed = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let snapshot = committed.post_commit_snapshot().unwrap();
    assert!(snapshot
        .get_all_domain_metadata(engine.as_ref())?
        .is_empty());
    let crc = write_and_verify_crc(snapshot, &table_path, engine.as_ref());
    assert_eq!(crc.domain_metadata, Some(Default::default()));

    Ok(())
}

// ============================================================================
// Domain metadata CRC fast path
// ============================================================================

/// Engine that panics if any handler is accessed.
struct FailingEngine;

impl Engine for FailingEngine {
    fn evaluation_handler(&self) -> Arc<dyn delta_kernel::EvaluationHandler> {
        unimplemented!()
    }
    fn storage_handler(&self) -> Arc<dyn delta_kernel::StorageHandler> {
        unimplemented!()
    }
    fn json_handler(&self) -> Arc<dyn delta_kernel::JsonHandler> {
        unimplemented!()
    }
    fn parquet_handler(&self) -> Arc<dyn delta_kernel::ParquetHandler> {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_get_domain_metadata_with_crc_skips_log_replay() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: CREATE TABLE with zip -> zap0 (and clustering DM from create_table_and_commit)
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();

    // v1: update zip -> zap1, add foo -> bar
    let committed = snapshot_v0
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_domain_metadata("zip".to_string(), "zap1".to_string())
        .with_domain_metadata("foo".to_string(), "bar".to_string())
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Asserts domain metadata on any snapshot, regardless of how it was loaded.
    let assert_domain_metadata = |snapshot: &Snapshot, engine: &dyn delta_kernel::Engine| {
        assert_eq!(
            snapshot.get_domain_metadata("zip", engine).unwrap(),
            Some("zap1".to_string())
        );
        assert_eq!(
            snapshot.get_domain_metadata("foo", engine).unwrap(),
            Some("bar".to_string())
        );
        assert!(snapshot
            .get_domain_metadata_internal("delta.clustering", engine)
            .unwrap()
            .is_some());
        assert_eq!(
            snapshot
                .get_domain_metadatas_internal(engine, None)
                .unwrap()
                .len(),
            3
        );
    };

    // Case 1: Post-commit snapshot with in-memory CRC => DM loaded from CRC (fast path).
    //         Use NoJsonReadsEngine to prove no log replay occurs.
    let post_commit_snapshot = committed.post_commit_snapshot().unwrap();
    assert!(post_commit_snapshot
        .get_current_crc_if_loaded_for_testing()
        .is_some());
    assert_domain_metadata(post_commit_snapshot, &FailingEngine);

    // Case 2: Fresh snapshot loaded from disk, no CRC file => DM loaded via log replay (slow path)
    let fresh_snapshot_no_crc = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_snapshot_no_crc
        .get_current_crc_if_loaded_for_testing()
        .is_none());
    assert_domain_metadata(&fresh_snapshot_no_crc, engine.as_ref());

    // Case 3: Write CRC to disk, then reload fresh snapshot => DM loaded from CRC (fast path)
    //         Use NoJsonReadsEngine to prove no log replay occurs.
    let _ = post_commit_snapshot.write_checksum(engine.as_ref())?;

    let fresh_snapshot_with_crc = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_snapshot_with_crc
        .get_current_crc_if_loaded_for_testing()
        .is_some());
    assert_domain_metadata(&fresh_snapshot_with_crc, &FailingEngine);

    Ok(())
}

// ============================================================================
// Set transaction CRC tracking
// ============================================================================

/// Comprehensive test for set transaction CRC tracking: verifies that set transactions are
/// correctly tracked in the CRC across commits, round-trip through write/reload, and that
/// the CRC fast path (no log replay) works for set transaction queries.
#[tokio::test]
async fn test_set_transaction_crc_tracking_and_fast_path() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // -- v0: CREATE TABLE (no set transactions) --
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();

    // Post-commit CRC has empty set_transactions (not null)
    let crc_v0 = write_and_verify_crc(snapshot_v0, &table_path, engine.as_ref());
    assert_eq!(crc_v0.set_transactions, Some(Default::default()));

    // Fresh snapshot with CRC on disk serves queries via fast path (no log replay)
    let fresh_v0 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_v0.get_current_crc_if_loaded_for_testing().is_some());
    assert_eq!(
        fresh_v0
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        None
    );

    // -- v1: commit with my-app=1 --
    let committed = snapshot_v0
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("my-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();

    // Post-commit CRC tracks my-app=1, queryable via fast path
    assert_eq!(
        snapshot_v1
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        snapshot_v1
            .get_app_id_version("nonexistent", &FailingEngine)
            .unwrap(),
        None
    );

    // Write CRC to disk, reload, verify round-trip and fast path
    let crc_v1 = write_and_verify_crc(snapshot_v1, &table_path, engine.as_ref());
    let txns_v1 = crc_v1.set_transactions.as_ref().unwrap();
    assert_eq!(txns_v1.len(), 1);
    assert!(txns_v1.contains_key("my-app"));

    let fresh_v1 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_v1.get_current_crc_if_loaded_for_testing().is_some());
    assert_eq!(
        fresh_v1
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        fresh_v1
            .get_app_id_version("nonexistent", &FailingEngine)
            .unwrap(),
        None
    );

    // -- v2: commit with my-app=2 (upsert) + other-app=1 (new) --
    let committed = snapshot_v1
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("my-app".to_string(), 2)
        .with_transaction_id("other-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot_v2 = committed.post_commit_snapshot().unwrap();

    // Post-commit CRC tracks updated versions, queryable via fast path
    assert_eq!(
        snapshot_v2
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        snapshot_v2
            .get_app_id_version("other-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );

    // Write CRC to disk, reload, verify round-trip and fast path
    let crc_v2 = write_and_verify_crc(snapshot_v2, &table_path, engine.as_ref());
    let txns_v2 = crc_v2.set_transactions.as_ref().unwrap();
    assert_eq!(txns_v2.len(), 2);

    let fresh_v2 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_v2.get_current_crc_if_loaded_for_testing().is_some());
    assert_eq!(
        fresh_v2
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        fresh_v2
            .get_app_id_version("other-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );

    Ok(())
}

// ============================================================================
// Set transaction CRC expiration
// ============================================================================

/// Tests the CRC fast path for set transaction expiration filtering. Since `lastUpdated` is set
/// to now, "interval 0 seconds" yields `expiration_timestamp = now`, so `last_updated <= now`
/// holds and the txn expires. A large retention or no retention should keep the txn visible.
#[rstest]
#[case::zero_retention_expires(Some("interval 0 seconds"), None)]
#[case::large_retention_not_expired(Some("interval 365 days"), Some(1))]
#[case::no_retention_no_filtering(None, Some(1))]
#[tokio::test]
async fn test_set_txn_expiration_via_crc_fast_path(
    #[case] retention: Option<&str>,
    #[case] expected: Option<i64>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    // v0: create the table with optional retention property
    let mut builder = create_table(&table_path, schema, "test_engine");
    if let Some(r) = retention {
        builder = builder.with_table_properties([("delta.setTransactionRetentionDuration", r)]);
    }
    let committed = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // v1: commit a set transaction for "my-app" (lastUpdated = now)
    let snapshot_v0 = committed.post_commit_snapshot().unwrap().clone();
    let committed = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("my-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Write CRC at v1 so the fast path is used on reload
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();
    snapshot_v1.write_checksum(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);

    // Verify CRC was loaded from disk
    assert!(snapshot.get_current_crc_if_loaded_for_testing().is_some());

    // FailingEngine proves the CRC fast path is used (no log replay)
    assert_eq!(
        snapshot
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        expected
    );

    Ok(())
}

/// Verifies that a set transaction with null `last_updated` never expires, even with the most
/// aggressive retention ("interval 0 seconds").
#[tokio::test]
async fn test_set_txn_null_last_updated_never_expires_via_log_replay() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: create table with aggressive retention
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);
    create_table(&table_path, schema, "test_engine")
        .with_table_properties([(
            "delta.setTransactionRetentionDuration",
            "interval 0 seconds",
        )])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // v1: raw commit with txn action that omits lastUpdated
    let store = Arc::new(LocalFileSystem::new());
    add_commit(
        &table_path,
        store.as_ref(),
        1,
        r#"{"txn":{"appId":"null-app","version":42}}"#.to_string(),
    )
    .await
    .unwrap();

    // Reload fresh snapshot at v1 -- no CRC covers v1, so log replay is used
    let fresh = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(fresh.version(), 1);

    // Despite aggressive retention, null last_updated means the txn never expires
    assert_eq!(
        fresh.get_app_id_version("null-app", engine.as_ref())?,
        Some(42)
    );

    Ok(())
}

// ============================================================================
// File Histogram Tracking Across Commits
// ============================================================================

/// Returns paths of all `.parquet` files in the table root directory.
///
/// NOTE: Uses non-recursive `read_dir`, so this only finds parquet files
/// directly in the table root. Partitioned tables store parquet files in
/// subdirectories and would require a recursive walk.
fn parquet_paths_on_disk(table_path: &str) -> Vec<PathBuf> {
    let url = delta_kernel::try_parse_uri(table_path).unwrap();
    let dir = url.to_file_path().unwrap();
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                Some(path)
            } else {
                None
            }
        })
        .collect()
}

/// Returns sorted sizes of all `.parquet` files in the table directory as
/// independent ground truth (not derived from the CRC computation).
///
/// NOTE: After a Delta remove, call `delete_parquet_files_on_disk` first
/// so that the disk state reflects only logically-active files (Delta
/// removes are logical, not physical).
fn parquet_file_sizes_on_disk(table_path: &str) -> Vec<i64> {
    let mut sizes: Vec<i64> = parquet_paths_on_disk(table_path)
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len() as i64)
        .collect();
    sizes.sort();
    sizes
}

/// Deletes all `.parquet` files in the table directory. Called after Delta
/// remove actions to keep `parquet_file_sizes_on_disk` accurate.
fn delete_parquet_files_on_disk(table_path: &str) {
    for path in parquet_paths_on_disk(table_path) {
        std::fs::remove_file(path).unwrap();
    }
}

/// Asserts that the histogram totals (summed across all bins) match the expected values,
/// and that the histogram file count sum equals [`FileStats::num_files`].
fn assert_histogram_totals(
    file_stats: &FileStats,
    expected_file_count: i64,
    expected_total_bytes: i64,
) {
    let hist = file_stats
        .file_size_histogram()
        .expect("histogram should be present");
    let count_sum: i64 = hist.file_counts().iter().sum();
    assert_eq!(count_sum, expected_file_count);
    assert_eq!(count_sum, file_stats.num_files());
    assert_eq!(hist.total_bytes().iter().sum::<i64>(), expected_total_bytes);
}

/// The first non-zero default histogram bin boundary (8KB).
const FIRST_BIN_BOUNDARY: i64 = 8192;

/// Approximate bytes per row for `(int32, 100-char padded string)` parquet data.
const APPROX_BYTES_PER_ROW: i64 = 104;

/// Row count guaranteed to produce a parquet file exceeding [`FIRST_BIN_BOUNDARY`].
/// Uses 2x the boundary divided by per-row size as a generous margin.
const LARGE_FILE_ROW_COUNT: i32 = (FIRST_BIN_BOUNDARY * 2 / APPROX_BYTES_PER_ROW) as i32;

/// Verifies that the in-memory CRC histogram correctly tracks file adds and removes across
/// multiple bins, cross-checked against actual file sizes on disk at each step. The CRC is
/// maintained in memory via `post_commit_snapshot` and written to disk at each step for
/// verification.
///
/// - v0: empty table -> histogram all zeros
/// - v1: insert small file (< 8KB, bin 0) -> 1 file in bin 0
/// - v2: insert large file (>= 8KB, bin 1+) -> files span two bins
/// - v3: remove all files, delete parquet from disk -> histogram returns to all zeros
#[tokio::test]
async fn test_file_histogram_tracks_adds_and_removes_across_bins() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("data", DataType::STRING),
    ])?);

    // ===== v0: empty table =====
    let committed = create_table(&table_path, schema, "test_engine")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();
    let crc_v0 = write_and_verify_crc(snapshot, &table_path, engine.as_ref());
    assert_histogram_totals(&crc_v0.file_stats().unwrap(), 0, 0);

    // ===== v1: insert small file (< 8KB -> bin 0) =====
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let data: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let committed = insert_data(snapshot.clone(), &engine, vec![ids, data])
        .await?
        .unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();
    let disk_sizes = parquet_file_sizes_on_disk(&table_path);
    assert_eq!(disk_sizes.len(), 1);
    let crc_v1 = write_and_verify_crc(snapshot, &table_path, engine.as_ref());
    let stats_v1 = crc_v1.file_stats().unwrap();
    assert_histogram_totals(&stats_v1, 1, disk_sizes.iter().sum());

    // Verify boundary metadata via public getters
    let hist = stats_v1.file_size_histogram().unwrap();
    assert_eq!(hist.sorted_bin_boundaries()[0], 0);
    assert_eq!(hist.sorted_bin_boundaries().len(), 95);

    // ===== v2: insert large file (>= 8KB -> bin 1+) =====
    let n = LARGE_FILE_ROW_COUNT;
    let ids: ArrayRef = Arc::new(Int32Array::from((0..n).collect::<Vec<_>>()));
    let strings: Vec<String> = (0..n).map(|i| format!("{i:0>100}")).collect();
    let data: ArrayRef = Arc::new(StringArray::from(strings));
    let committed = insert_data(snapshot.clone(), &engine, vec![ids, data])
        .await?
        .unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();

    // Cross-check: files land in different bins
    let disk_sizes = parquet_file_sizes_on_disk(&table_path);
    assert_eq!(disk_sizes.len(), 2);
    let small_size = disk_sizes[0];
    let large_size = disk_sizes[1];
    assert!(
        small_size < FIRST_BIN_BOUNDARY,
        "expected small file < 8KB, got {small_size}"
    );
    assert!(
        large_size >= FIRST_BIN_BOUNDARY,
        "expected large file >= 8KB, got {large_size}"
    );

    let crc_v2 = write_and_verify_crc(snapshot, &table_path, engine.as_ref());
    let stats_v2 = crc_v2.file_stats().unwrap();
    let hist = stats_v2.file_size_histogram().unwrap();
    assert_eq!(hist.file_counts()[0], 1, "bin 0 should have the small file");
    assert_eq!(hist.total_bytes()[0], small_size);

    // Find the exact bin for the large file based on its actual size
    let boundaries = hist.sorted_bin_boundaries();
    let large_bin = boundaries
        .windows(2)
        .enumerate()
        .find(|(_, w)| large_size >= w[0] && large_size < w[1])
        .map(|(i, _)| i)
        .unwrap_or(boundaries.len() - 1);
    assert_eq!(
        hist.file_counts()[large_bin],
        1,
        "large file ({large_size} bytes) should be in bin {large_bin}"
    );
    assert_eq!(hist.total_bytes()[large_bin], large_size);

    // ===== v3: remove all files =====
    let scan = snapshot.clone().scan_builder().build()?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("DELETE".to_string())
        .with_data_change(true);
    for sm in scan.scan_metadata(engine.as_ref())? {
        txn.remove_files(sm?.scan_files);
    }
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();

    // Delete physical parquet files so disk ground truth reflects the empty table
    delete_parquet_files_on_disk(&table_path);
    assert!(parquet_file_sizes_on_disk(&table_path).is_empty());

    let crc_v3 = write_and_verify_crc(snapshot, &table_path, engine.as_ref());
    assert_histogram_totals(&crc_v3.file_stats().unwrap(), 0, 0);

    Ok(())
}

/// Verifies the disk round-trip path: write the in-memory CRC to disk at v1, load a fresh
/// snapshot from disk (which deserializes the v1 CRC from JSON), then insert at v2. The v2
/// post-commit CRC is computed by applying the v2 delta to the deserialized v1 CRC, testing
/// that the in-memory chain works with a disk-loaded base.
#[tokio::test]
async fn test_file_histogram_survives_disk_round_trip_then_delta_merge() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();

    // v1: insert data and write CRC to disk
    let col1: ArrayRef = Arc::new(Int32Array::from((1..=10).collect::<Vec<_>>()));
    let committed = insert_data(snapshot_v0.clone(), &engine, vec![col1])
        .await?
        .unwrap_committed();
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();
    let v1_bytes = snapshot_v1
        .get_current_crc_if_loaded_for_testing()
        .unwrap()
        .file_stats()
        .unwrap()
        .table_size_bytes();
    snapshot_v1.write_checksum(engine.as_ref())?;

    // Load a FRESH snapshot from disk at v1 (CRC deserialized from JSON, not in-memory)
    let fresh_v1 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(fresh_v1.version(), 1);
    assert!(fresh_v1.get_current_crc_if_loaded_for_testing().is_some());

    // v2: insert using the fresh (disk-loaded) snapshot -- the post-commit CRC at v2
    // is computed by applying the v2 delta to the deserialized v1 CRC
    let col2: ArrayRef = Arc::new(Int32Array::from((11..=20).collect::<Vec<_>>()));
    let committed = insert_data(fresh_v1, &engine, vec![col2])
        .await?
        .unwrap_committed();
    assert_eq!(committed.commit_version(), 2);

    // Verify the merged histogram: 2 files, bytes match actual parquet files on disk
    let snapshot_v2 = committed.post_commit_snapshot().unwrap();
    let crc_v2 = write_and_verify_crc(snapshot_v2, &table_path, engine.as_ref());
    let disk_sizes = parquet_file_sizes_on_disk(&table_path);
    assert_eq!(disk_sizes.len(), 2);
    assert!(disk_sizes.iter().sum::<i64>() > v1_bytes);
    assert_histogram_totals(&crc_v2.file_stats().unwrap(), 2, disk_sizes.iter().sum());

    Ok(())
}

/// Rewrites the histogram in an on-disk CRC file to use custom 2-bin
/// boundaries `[0, 100]`. Since any parquet file is > 100 bytes (metadata
/// alone exceeds this), all files land
/// deterministically in bin 1 regardless of compression. The file_counts and total_bytes
/// arrays are rebuilt to match the new boundaries using the provided file sizes.
fn rewrite_crc_with_custom_bins(table_path: &str, version: u64, file_sizes: &[i64]) {
    let url = delta_kernel::try_parse_uri(table_path).unwrap();
    let crc_file = url
        .to_file_path()
        .unwrap()
        .join(format!("_delta_log/{version:020}.crc"));

    let json_str = std::fs::read_to_string(&crc_file).unwrap();
    let mut crc: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    // All files are > 100 bytes, so bin 0 is empty and bin 1 holds everything
    let total_bytes: i64 = file_sizes.iter().sum();
    let num_files = file_sizes.len() as i64;
    crc["fileSizeHistogram"] = serde_json::json!({
        "sortedBinBoundaries": [0, 100],
        "fileCounts": [0, num_files],
        "totalBytes": [0, total_bytes],
    });

    std::fs::write(&crc_file, serde_json::to_string(&crc).unwrap()).unwrap();
}

/// Cross-product test: (custom bins | default bins) x (incremental | non-incremental).
///
/// For incremental operations, the in-memory CRC histogram should survive with correct
/// values and boundary type. For non-incremental operations, the histogram is dropped to
/// None and file stats become Indeterminate, regardless of boundary type.
///
/// Custom bins are injected by rewriting the on-disk CRC to use 2-bin boundaries `[0, 100]`
/// before loading a fresh snapshot. The fresh snapshot deserializes the custom-bin CRC, and
/// the next commit's in-memory delta inherits those boundaries.
#[rstest]
#[tokio::test]
async fn test_file_histogram_with_bin_type_and_operation_type(
    #[values(true, false)] use_custom_bins: bool,
    #[values(true, false)] incremental: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // ===== GIVEN: table with 1 file at v1 and CRC on disk =====
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let committed = insert_data(snapshot_v0.clone(), &engine, vec![col])
        .await?
        .unwrap_committed();
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();
    snapshot_v1.write_checksum(engine.as_ref())?;

    let v1_disk_sizes = parquet_file_sizes_on_disk(&table_path);
    assert_eq!(v1_disk_sizes.len(), 1);

    if use_custom_bins {
        rewrite_crc_with_custom_bins(&table_path, 1, &v1_disk_sizes);
    }

    // Load fresh snapshot from disk (reads the possibly-modified CRC)
    let fresh_v1 = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(fresh_v1.version(), 1);
    assert!(fresh_v1.get_current_crc_if_loaded_for_testing().is_some());

    // ===== WHEN: perform the operation =====
    let snapshot_v2 = if incremental {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let committed = insert_data(fresh_v1, &engine, vec![col])
            .await?
            .unwrap_committed();
        assert_eq!(committed.commit_version(), 2);
        committed.post_commit_snapshot().unwrap().clone()
    } else {
        let committed = fresh_v1
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("ANALYZE STATS".to_string())
            .commit(engine.as_ref())?
            .unwrap_committed();
        assert_eq!(committed.commit_version(), 2);
        committed.post_commit_snapshot().unwrap().clone()
    };

    // ===== THEN: verify histogram state =====
    let crc_v2 = snapshot_v2.get_current_crc_if_loaded_for_testing().unwrap();

    if !incremental {
        // Non-incremental operations drop the histogram regardless of bin type
        assert_eq!(crc_v2.file_stats_validity, FileStatsValidity::Indeterminate);
        assert!(crc_v2.file_stats().is_none());
        return Ok(());
    }

    // Incremental: histogram should be present and correct
    let stats_v2 = crc_v2.file_stats().unwrap();
    let hist = stats_v2
        .file_size_histogram()
        .expect("incremental op should preserve histogram");
    let counts = hist.file_counts();
    let bytes = hist.total_bytes();
    let v2_disk_sizes = parquet_file_sizes_on_disk(&table_path);
    assert_eq!(v2_disk_sizes.len(), 2);
    let total_disk_bytes: i64 = v2_disk_sizes.iter().sum();

    if use_custom_bins {
        // Custom [0, 100] boundaries: 2 bins, all files in bin 1 (all > 100 bytes)
        assert_eq!(counts.len(), 2, "custom bins should have exactly 2 bins");
        assert_eq!(counts[0], 0, "no files should be in bin 0 ([0, 100))");
        assert_eq!(counts[1], 2, "both files should be in bin 1 ([100, inf))");
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[1], total_disk_bytes);
    } else {
        // Default 95-bin boundaries: all small test files land in bin 0 ([0, 8KB))
        assert_eq!(counts.len(), 95, "default bins should have 95 bins");
        assert_histogram_totals(&stats_v2, 2, total_disk_bytes);
    }

    Ok(())
}
