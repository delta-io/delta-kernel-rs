//! Integration tests for table maintenance operations (checkpoint, checksum).

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{CheckpointWriteResult, ChecksumWriteResult};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use rstest::rstest;
use serde_json::json;
use test_utils::{add_commit, assert_result_error_with_message, test_table_setup_mt};
use url::Url;

#[rstest]
#[case::v1_checkpoint(false)]
#[case::v2_checkpoint(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_and_checksum_return_updated_snapshots(
    #[case] v2_checkpoint: bool,
) -> DeltaResult<()> {
    // ===== GIVEN =====
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "id": INTEGER };
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
    let (_, snapshot_w_ckpt) = snapshot.checkpoint(engine.as_ref(), None)?;
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

#[rstest]
#[case::v1_checkpoint(false)]
#[case::v2_checkpoint(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_already_exists(#[case] v2_checkpoint: bool) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let mut builder = create_table(&table_path, schema, "test_engine");
    if v2_checkpoint {
        builder = builder.with_table_properties([("delta.feature.v2Checkpoint", "supported")]);
    }
    let committed = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();

    // First checkpoint writes successfully
    let (result, snapshot_w_ckpt) = snapshot.checkpoint(engine.as_ref(), None)?;
    assert_eq!(result, CheckpointWriteResult::Written);

    // Calling checkpoint again on the returned snapshot detects the existing checkpoint
    let (result, unchanged) = snapshot_w_ckpt.checkpoint(engine.as_ref(), None)?;
    assert_eq!(result, CheckpointWriteResult::AlreadyExists);
    assert_eq!(unchanged.version(), snapshot_w_ckpt.version());

    // A fresh snapshot loaded from storage also returns AlreadyExists
    let fresh = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(
        fresh.log_segment().checkpoint_version,
        Some(snapshot.version())
    );
    let (result, _) = fresh.checkpoint(engine.as_ref(), None)?;
    assert_eq!(result, CheckpointWriteResult::AlreadyExists);

    Ok(())
}

#[rstest]
#[case::reader_writer(&["futureFeature"], &["futureFeature"])]
#[case::writer_only(&[], &["futureFeature"])]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_writes_reject_unsupported_table_features(
    #[case] reader_features: &[&str],
    #[case] writer_features: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let store = LocalFileSystem::new();
    let schema_string = json!({
        "type": "struct",
        "fields": [{
            "name": "id",
            "type": "integer",
            "nullable": true,
            "metadata": {},
        }],
    })
    .to_string();
    let commit = [
        json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": reader_features,
                "writerFeatures": writer_features,
            }
        }),
        json!({
            "metaData": {
                "id": "maintenance-feature-validation",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1_700_000_000_000i64,
            }
        }),
    ]
    .map(|action| action.to_string())
    .join("\n");
    add_commit(table_url.as_str(), &store, 0, commit).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let checkpoint_result = snapshot.checkpoint(engine.as_ref(), None).map(|_| ());
    assert_result_error_with_message(checkpoint_result, "futureFeature");

    let checksum_result = snapshot.write_checksum(engine.as_ref()).map(|_| ());
    assert_result_error_with_message(checksum_result, "futureFeature");

    Ok(())
}
