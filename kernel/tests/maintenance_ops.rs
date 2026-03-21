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
    // Setup
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

    assert!(snapshot.log_segment().listed.checkpoint_parts.is_empty());
    assert!(snapshot.log_segment().checkpoint_version.is_none());
    assert!(snapshot.log_segment().listed.latest_crc_file.is_none());

    // Checkpoint returns a snapshot with the checkpoint recorded
    let snapshot_w_ckpt = snapshot.checkpoint(engine.as_ref())?;

    assert_eq!(
        snapshot_w_ckpt.log_segment().checkpoint_version,
        Some(snapshot.version())
    );
    assert_eq!(
        snapshot_w_ckpt.log_segment().listed.checkpoint_parts.len(),
        1
    );
    assert_eq!(
        snapshot_w_ckpt.log_segment().listed.checkpoint_parts[0].version,
        snapshot.version()
    );
    assert!(snapshot_w_ckpt
        .log_segment()
        .listed
        .ascending_commit_files
        .is_empty());
    assert!(snapshot_w_ckpt
        .log_segment()
        .listed
        .ascending_compaction_files
        .is_empty());

    // Write checksum on the post-checkpoint snapshot returns a snapshot with CRC
    let (crc_result, snapshot_w_both) = snapshot_w_ckpt.write_checksum(engine.as_ref())?;

    assert_eq!(crc_result, ChecksumWriteResult::Written);
    let crc_file = snapshot_w_both
        .log_segment()
        .listed
        .latest_crc_file
        .as_ref()
        .expect("snapshot should have latest_crc_file set");
    assert_eq!(crc_file.version, snapshot.version());
    // The checkpoint is still present after the CRC write
    assert_eq!(
        snapshot_w_both.log_segment().checkpoint_version,
        Some(snapshot.version())
    );

    Ok(())
}
