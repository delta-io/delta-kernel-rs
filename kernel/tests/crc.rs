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
// TODO: Add tests for testing set txn expiration
// ============================================================================

#[tokio::test]
async fn test_post_commit_crc_tracks_set_transactions() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: CREATE TABLE (no set transactions)
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();
    let crc_v0 = write_and_verify_crc(snapshot_v0, &table_path, engine.as_ref());
    assert_eq!(crc_v0.set_transactions.as_ref().unwrap().len(), 0);

    // v1: commit with my-app=1
    let committed = snapshot_v0
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("my-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot_v1 = committed.post_commit_snapshot().unwrap();
    let crc_v1 = write_and_verify_crc(snapshot_v1, &table_path, engine.as_ref());
    let txns_v1 = crc_v1.set_transactions.as_ref().unwrap();
    assert_eq!(txns_v1.len(), 1);
    assert!(txns_v1.contains_key("my-app"));
    // Verify through the snapshot API (which applies retention filtering)
    assert_eq!(
        snapshot_v1
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );

    // v2: commit with my-app=2 (upsert) + other-app=1 (new)
    let committed = snapshot_v1
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("my-app".to_string(), 2)
        .with_transaction_id("other-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot_v2 = committed.post_commit_snapshot().unwrap();
    let crc_v2 = write_and_verify_crc(snapshot_v2, &table_path, engine.as_ref());
    let txns_v2 = crc_v2.set_transactions.as_ref().unwrap();
    assert_eq!(txns_v2.len(), 2);
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

    Ok(())
}

#[tokio::test]
async fn test_get_app_id_version_with_crc_skips_log_replay() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: CREATE TABLE
    let committed = create_table_and_commit(&table_path, engine.as_ref())?;
    let snapshot_v0 = committed.post_commit_snapshot().unwrap();

    // v1: commit with my-app=1
    let committed = snapshot_v0
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("my-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Case 1: Post-commit snapshot with in-memory CRC => fast path (no I/O).
    let post_commit_snapshot = committed.post_commit_snapshot().unwrap();
    assert!(post_commit_snapshot
        .get_current_crc_if_loaded_for_testing()
        .is_some());
    assert_eq!(
        post_commit_snapshot
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        post_commit_snapshot
            .get_app_id_version("nonexistent", &FailingEngine)
            .unwrap(),
        None
    );

    // Case 2: Fresh snapshot without CRC file => slow path (real engine).
    let fresh_snapshot_no_crc = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_snapshot_no_crc
        .get_current_crc_if_loaded_for_testing()
        .is_none());
    assert_eq!(
        fresh_snapshot_no_crc
            .get_app_id_version("my-app", engine.as_ref())
            .unwrap(),
        Some(1)
    );

    // Case 3: Write CRC to disk, reload => fast path (no I/O).
    post_commit_snapshot.write_checksum(engine.as_ref())?;
    let fresh_snapshot_with_crc = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(fresh_snapshot_with_crc
        .get_current_crc_if_loaded_for_testing()
        .is_some());
    assert_eq!(
        fresh_snapshot_with_crc
            .get_app_id_version("my-app", &FailingEngine)
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        fresh_snapshot_with_crc
            .get_app_id_version("nonexistent", &FailingEngine)
            .unwrap(),
        None
    );

    Ok(())
}

// CRC should always write setTransactions as an empty list (not null) when no txn actions exist.
#[tokio::test]
async fn test_write_checksum_with_no_set_txns_writes_empty_list() -> DeltaResult<()> {
    use std::collections::HashMap;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    let committed = create_table(&table_path, schema, "test_engine")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let snapshot = committed.post_commit_snapshot().unwrap();
    let crc = write_and_verify_crc(snapshot, &table_path, engine.as_ref());
    assert_eq!(crc.set_transactions, Some(HashMap::new()));

    Ok(())
}
