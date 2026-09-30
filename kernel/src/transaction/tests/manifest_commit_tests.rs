//! Tests for `adaptiveMetadata-preview` manifest (content-tree) commits and root manifest file
//! commits.

use super::super::{ManifestCommitState, ManifestWrite, Transaction};
use super::{add_dummy_file, create_existing_table_txn};
use crate::actions::{DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA};
use crate::engine::arrow_data::ArrowEngineData;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    minimal_checkpoint_action, setup_table, write_commit,
};
use crate::unit_test_utils::{
    assert_result_error_with_message, create_valid_add_file_batch, MockProtocolBuilder,
    MockTableConfigurationBuilder,
};
use crate::{create_row, DeltaResult, FileMeta};

fn adaptive_table_config() -> TableConfiguration {
    MockTableConfigurationBuilder::new()
        .with_protocol(
            MockProtocolBuilder::new()
                .with_features([TableFeature::AdaptiveMetadataPreview])
                .build(),
        )
        .build()
}

/// A root manifest `FileMeta` located under the transaction's table root.
fn dummy_root_manifest_file_meta(txn: &Transaction) -> FileMeta {
    let table_root = txn.read_snapshot_opt.clone().unwrap().table_root().clone();
    FileMeta {
        location: table_root.join("metadata/root-v1.parquet").unwrap(),
        last_modified: 0,
        size: 1024,
    }
}

// === with_root_manifest_file staging ===

#[test]
fn with_root_manifest_file_rejects_non_adaptive_table() -> DeltaResult<()> {
    let (_engine, txn, _tempdir) = create_existing_table_txn()?;
    let file = dummy_root_manifest_file_meta(&txn);
    assert_result_error_with_message(
        txn.with_root_manifest_file(file),
        "adaptiveMetadata-preview",
    );
    Ok(())
}

#[test]
fn validate_manifest_write_allows_root_manifest_on_adaptive_table() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    let file = dummy_root_manifest_file_meta(&txn);
    txn = txn.with_root_manifest_file(file)?;
    txn.validate_manifest_write_semantics()?;
    Ok(())
}

#[test]
fn validate_manifest_write_rejects_root_manifest_with_file_actions() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    let file = dummy_root_manifest_file_meta(&txn);
    txn = txn.with_root_manifest_file(file)?;
    add_dummy_file(&mut txn);
    assert_result_error_with_message(
        txn.validate_manifest_write_semantics(),
        "cannot include file actions",
    );
    Ok(())
}

// === with_manifest_commit staging ===

#[test]
fn with_manifest_commit_succeeds_on_adaptive_table() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    assert!(matches!(txn.manifest_write, Some(ManifestWrite::Commit(_))));
    Ok(())
}

#[test]
fn with_manifest_commit_rejects_non_adaptive_table() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    let result = txn.with_manifest_commit(engine.as_ref());
    assert_result_error_with_message(result, "adaptiveMetadata-preview");
    Ok(())
}

// Repeated calls must reuse the state built by the first call rather than rebuild it.
#[test]
fn with_manifest_commit_reuses_state_on_repeated_calls() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    let first: *const ManifestCommitState = txn.with_manifest_commit(engine.as_ref())?;
    let second: *const ManifestCommitState = txn.with_manifest_commit(engine.as_ref())?;
    assert_eq!(
        first, second,
        "repeated with_manifest_commit must reuse the first state"
    );
    Ok(())
}

// === mutual exclusion (enforced when staging) ===

#[test]
fn with_manifest_commit_rejects_when_root_manifest_staged() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    let file = dummy_root_manifest_file_meta(&txn);
    txn = txn.with_root_manifest_file(file)?;
    assert_result_error_with_message(
        txn.with_manifest_commit(engine.as_ref()),
        "mutually exclusive",
    );
    Ok(())
}

#[test]
fn with_root_manifest_file_rejects_when_manifest_commit_staged() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    let file = dummy_root_manifest_file_meta(&txn);
    assert_result_error_with_message(txn.with_root_manifest_file(file), "mutually exclusive");
    Ok(())
}

// === checkpoint-version guard (in ManifestCommitState::try_new) ===

// A `checkpoint` action covering the snapshot's own version is fine to start a manifest commit on.
#[test]
fn manifest_commit_allows_checkpoint_covering_the_snapshot() -> DeltaResult<()> {
    let (engine, table_root) = setup_table()?;
    write_commit(
        &engine,
        &table_root,
        1,
        minimal_checkpoint_action("metadata/root-v1.parquet", 1)?.into_engine_data(&engine)?,
    )?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 1);
    // Checkpoint content-root version (1) >= snapshot version (1), so the guard passes.
    ManifestCommitState::try_new(&engine, snapshot.clone(), 2, &adaptive_table_config())?;
    Ok(())
}

// A delta commit landing after the last `checkpoint` action is not yet supported.
#[test]
fn manifest_commit_rejects_delta_commits_after_last_checkpoint() -> DeltaResult<()> {
    let (engine, table_root) = setup_table()?;
    write_commit(
        &engine,
        &table_root,
        1,
        minimal_checkpoint_action("metadata/root-v1.parquet", 1)?.into_engine_data(&engine)?,
    )?;
    // A later delta commit bumps the snapshot past the checkpoint's content-root version.
    let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
    write_commit(
        &engine,
        &table_root,
        2,
        create_row(&engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), domain_metadata)?,
    )?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 2);
    let result =
        ManifestCommitState::try_new(&engine, snapshot.clone(), 3, &adaptive_table_config());
    assert_result_error_with_message(result, "does not currently support delta log commits");
    Ok(())
}

// === commit ===

#[test]
fn commit_rejects_pending_manifest_commit() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    assert_result_error_with_message(txn.commit(engine.as_ref()), "not yet supported");
    Ok(())
}

// === leaf writer ===

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
