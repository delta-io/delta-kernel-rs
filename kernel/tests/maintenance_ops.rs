//! Integration tests for table maintenance operations (checkpoint, checksum).

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::ChecksumWriteResult;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::test_table_setup_mt;

#[rstest]
#[case::v1_checkpoint(false)]
#[case::v2_checkpoint(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_and_checksum_return_updated_snapshots(
    #[case] v2_checkpoint: bool,
) -> DeltaResult<()> {
    // ===== GIVEN =====
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);
    let mut builder = create_table(&table_path, schema, "test_engine");
    if v2_checkpoint {
        builder = builder.with_table_properties([("delta.feature.v2Checkpoint", "supported")]);
    }
    let committed = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();

    // Precondition: no checkpoint, no CRC
    let seg = snapshot.log_segment();
    assert!(seg.listed.checkpoint_parts.is_empty());
    assert!(seg.checkpoint_version.is_none());
    assert!(seg.listed.latest_crc_file.is_none());

    // ===== WHEN: we checkpoint =====
    let snapshot_w_ckpt = snapshot.checkpoint(engine.as_ref())?;
    let seg = snapshot_w_ckpt.log_segment();

    // ===== THEN =====
    // Checkpoint version and parts are set
    assert_eq!(seg.checkpoint_version, Some(snapshot.version()));
    assert_eq!(seg.listed.checkpoint_parts.len(), 1);
    assert_eq!(seg.listed.checkpoint_parts[0].version, snapshot.version());

    // Commits and compactions subsumed by the checkpoint are cleared
    assert!(seg.listed.ascending_commit_files.is_empty());
    assert!(seg.listed.ascending_compaction_files.is_empty());

    // ===== WHEN: we write checksum =====
    let (crc_result, snapshot_w_both) = snapshot_w_ckpt.write_checksum(engine.as_ref())?;
    let seg = snapshot_w_both.log_segment();

    // ===== THEN =====
    // CRC file is recorded at the correct version
    assert_eq!(crc_result, ChecksumWriteResult::Written);
    let crc_file = seg
        .listed
        .latest_crc_file
        .as_ref()
        .expect("snapshot should have latest_crc_file set");
    assert_eq!(crc_file.version, snapshot.version());

    // The checkpoint is still present after the CRC write
    assert_eq!(seg.checkpoint_version, Some(snapshot.version()));

    Ok(())
}
