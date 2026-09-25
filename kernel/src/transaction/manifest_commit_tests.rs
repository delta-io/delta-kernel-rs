//! Tests for `adaptiveMetadata-preview` manifest (content-tree) commits and root manifest file
//! commits.

use super::root_manifest_file::RootManifestFile;
use super::tests::{add_dummy_file, create_existing_table_txn};
use crate::engine::arrow_data::ArrowEngineData;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::unit_test_utils::{
    assert_result_error_with_message, create_valid_add_file_batch, MockProtocolBuilder,
    MockTableConfigurationBuilder,
};
use crate::{DeltaResult, FileMeta};

fn dummy_root_manifest_file(read_snapshot: SnapshotRef) -> RootManifestFile {
    let file = FileMeta {
        location: read_snapshot.table_root().join("root-v1.parquet").unwrap(),
        last_modified: 0,
        size: 1024,
    };
    RootManifestFile::new(file, read_snapshot)
}

fn adaptive_table_config() -> TableConfiguration {
    MockTableConfigurationBuilder::new()
        .with_protocol(
            MockProtocolBuilder::new()
                .with_features([TableFeature::AdaptiveMetadataPreview])
                .build(),
        )
        .build()
}

#[test]
fn test_validate_root_manifest_file_succeeds_on_adaptive_table() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    let read_snapshot = txn.read_snapshot_opt.clone().unwrap();
    txn.effective_table_config = adaptive_table_config();
    txn.root_manifest_file = Some(dummy_root_manifest_file(read_snapshot));
    txn.validate_root_manifest_file_semantics()?;
    Ok(())
}

#[test]
fn test_validate_root_manifest_file_rejects_non_adaptive_table() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    let read_snapshot = txn.read_snapshot_opt.clone().unwrap();
    txn.root_manifest_file = Some(dummy_root_manifest_file(read_snapshot));
    let result = txn.validate_root_manifest_file_semantics();
    assert!(result.is_err());
    Ok(())
}

#[test]
fn test_validate_root_manifest_file_rejects_file_actions() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    let read_snapshot = txn.read_snapshot_opt.clone().unwrap();
    txn.effective_table_config = adaptive_table_config();
    txn.root_manifest_file = Some(dummy_root_manifest_file(read_snapshot));
    add_dummy_file(&mut txn);
    let result = txn.validate_root_manifest_file_semantics();
    assert!(result.is_err());
    Ok(())
}

#[test]
fn with_manifest_commit_succeeds_on_adaptive_table() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    assert!(txn.manifest_commit_state.is_some());
    Ok(())
}

#[test]
fn with_manifest_commit_rejects_non_adaptive_table() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    let result = txn.with_manifest_commit(engine.as_ref());
    assert_result_error_with_message(result, "adaptiveMetadata-preview");
    Ok(())
}

#[test]
fn commit_rejects_root_manifest_and_manifest_commit() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    let read_snapshot = txn.read_snapshot_opt.clone().unwrap();
    txn.effective_table_config = adaptive_table_config();
    txn.root_manifest_file = Some(dummy_root_manifest_file(read_snapshot));
    // Mutual exclusion is enforced at commit, so staging both succeeds here and fails on commit.
    txn.with_manifest_commit(engine.as_ref())?;
    assert_result_error_with_message(txn.commit(engine.as_ref()), "mutually exclusive");
    Ok(())
}

#[test]
fn commit_rejects_pending_manifest_commit() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    assert_result_error_with_message(txn.commit(engine.as_ref()), "not yet supported");
    Ok(())
}

#[test]
fn leaf_writer_ops_unsupported() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    let mut leaf_writer = txn
        .with_manifest_commit(engine.as_ref())?
        .new_leaf_node_writer(engine.as_ref())?;
    let add_batch = create_valid_add_file_batch(false /* all_nullable */);
    assert_result_error_with_message(
        leaf_writer.add_files(engine.as_ref(), Box::new(ArrowEngineData::new(add_batch))),
        "not yet supported",
    );
    assert_result_error_with_message(leaf_writer.finish(engine.as_ref()), "not yet supported");
    Ok(())
}
