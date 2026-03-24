//! Integration tests for CRC (version checksum) file-based APIs on Snapshot.

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::crc::{Crc, FileStatsValidity};
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::{ChecksumWriteResult, FileStats, Snapshot};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Engine};
use rstest::rstest;
use test_utils::{insert_data, test_table_setup};

// ============================================================================
// File stats from CRC on disk
// ============================================================================

#[tokio::test]
async fn test_get_file_stats_from_crc() -> DeltaResult<()> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/")).unwrap();
    let table_root = url::Url::from_directory_path(path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngineBuilder::new(store).build();

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    let file_stats = snapshot.get_file_stats(&engine);
    let expected = FileStats {
        table_size_bytes: 5259,
        num_files: 10,
    };
    assert_eq!(file_stats, Some(expected));

    Ok(())
}

#[tokio::test]
async fn test_get_file_stats_no_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("value", DataType::STRING, true),
    ])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    let file_stats = snapshot.get_file_stats(engine.as_ref());
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
    assert!(snapshot.get_file_stats(engine.as_ref()).is_some());

    // ===== WHEN =====
    // Empty commit to advance to version 1 (no new CRC file written)
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let _ = txn.commit(engine.as_ref())?;

    // ===== THEN =====
    // Load a fresh snapshot at version 1
    let snapshot = Snapshot::builder_for(table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);

    // No CRC at version 1, so file stats should be None
    let file_stats = snapshot.get_file_stats(engine.as_ref());
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
    let engine = DefaultEngineBuilder::new(store).build();

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    // ===== WHEN =====
    let crc = snapshot.get_current_crc_if_loaded_for_testing().unwrap();

    // ===== THEN =====
    let file_stats = crc.file_stats().unwrap();
    assert_eq!(file_stats.table_size_bytes, 5259);
    assert_eq!(file_stats.num_files, 10);
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
    assert_eq!(file_stats.num_files, 0);
    assert_eq!(file_stats.table_size_bytes, 0);
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
    snapshot: &Snapshot,
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
    assert_eq!(stats_v1.num_files, 1); // <--- 1 file added
    assert!(stats_v1.table_size_bytes > 0); // <--- size is non-zero

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
    assert_eq!(stats_v2.num_files, 2); // <--- 2 files added
    assert!(stats_v2.table_size_bytes > stats_v1.table_size_bytes); // <--- size is greater than after first insert

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
    assert_eq!(stats_v3.num_files, 0); // <--- 0 net file in the table
    assert_eq!(stats_v3.table_size_bytes, 0); // <--- size is 0

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

    let result = snapshot.write_checksum(engine.as_ref())?;
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

    let first = snapshot.write_checksum(engine.as_ref())?;
    assert_eq!(first, ChecksumWriteResult::Written);

    let second = if reload_snapshot {
        let fresh = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        fresh.write_checksum(engine.as_ref())?
    } else {
        snapshot.write_checksum(engine.as_ref())?
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
    assert_eq!(crc_stats.num_files, 5);
    assert!(crc_stats.table_size_bytes > 0);

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

    // The CRC should be loaded from the incremental update (not discarded)
    assert!(
        incremental_v1
            .get_current_crc_if_loaded_for_testing()
            .is_some(),
        "CRC should be loaded after incremental snapshot update with CRC file at target version"
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

// When an incremental snapshot update has no CRC in the new log segment, the combined segment
// should have no CRC file either (the old segment's CRC is not carried forward because it could
// be stale). The resulting snapshot should report no loaded CRC.
#[tokio::test]
async fn test_incremental_snapshot_old_crc_no_new_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create table at v0 and write CRC to disk
    let committed_v0 = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed_v0.post_commit_snapshot().unwrap();
    snapshot_v0.write_checksum(engine.as_ref())?;

    // Insert data at v1 -- do NOT write CRC for v1
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let committed_v1 = insert_data(snapshot_v0.clone(), &engine, vec![col])
        .await?
        .unwrap_committed();
    assert_eq!(committed_v1.commit_version(), 1);

    // Load fresh snapshot at v0, then incrementally update to v1
    let fresh_v0 = Snapshot::builder_for(&table_path)
        .at_version(0)
        .build(engine.as_ref())?;
    let incremental_v1 = Snapshot::builder_from(fresh_v0).build(engine.as_ref())?;
    assert_eq!(incremental_v1.version(), 1);

    // No CRC at v1 exists, so the snapshot should not have a loaded CRC at this version
    assert!(
        incremental_v1
            .get_current_crc_if_loaded_for_testing()
            .is_none(),
        "No CRC should be loaded when new segment lacks a CRC file at target version"
    );

    Ok(())
}

// When an incremental snapshot update finds the same CRC file as the old snapshot, the old
// snapshot's LazyCrc should be reused (it may already be loaded in memory). The CRC won't
// match the new snapshot's version, so it won't be reported as loaded at that version, but
// it can still be used by P&M reading as an optimization.
#[tokio::test]
async fn test_incremental_snapshot_reuses_old_lazy_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create table at v0 and write CRC to disk
    let committed_v0 = create_table_and_commit(&table_path, engine.as_ref())?;
    committed_v0
        .post_commit_snapshot()
        .unwrap()
        .write_checksum(engine.as_ref())?;

    // Insert data at v1 -- do NOT write CRC for v1
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let _committed_v1 = insert_data(
        committed_v0.post_commit_snapshot().unwrap().clone(),
        &engine,
        vec![col],
    )
    .await?
    .unwrap_committed();

    // Load a fresh snapshot at v0 (this loads 0.crc during P&M reading)
    let fresh_v0 = Snapshot::builder_for(&table_path)
        .at_version(0)
        .build(engine.as_ref())?;
    assert!(
        fresh_v0.get_current_crc_if_loaded_for_testing().is_some(),
        "Fresh v0 snapshot should have CRC loaded from 0.crc"
    );

    // Incrementally update from v0 -> v1. The new listing still finds 0.crc as the
    // latest CRC file (no 1.crc exists), so the old snapshot's LazyCrc should be reused.
    let incremental_v1 = Snapshot::builder_from(fresh_v0).build(engine.as_ref())?;
    assert_eq!(incremental_v1.version(), 1);

    // The CRC is at v0, not v1, so get_current_crc_if_loaded_for_testing returns None
    // (version mismatch). But the LazyCrc itself was reused from the old snapshot.
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
    post_commit_snapshot.write_checksum(engine.as_ref())?;

    let fresh_snapshot_with_crc = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_snapshot_with_crc
        .get_current_crc_if_loaded_for_testing()
        .is_some());
    assert_domain_metadata(&fresh_snapshot_with_crc, &FailingEngine);

    Ok(())
}

// ============================================================================
// Set transaction CRC tracking
// TODO(#2141): Add tests for testing set txn expiration
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
