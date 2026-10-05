//! Tests for `adaptiveMetadata-preview` manifest (content-tree) commits and root manifest file
//! commits.

use super::super::{ManifestCommitState, ManifestWrite, SchemaOperation, Transaction};
use super::{add_dummy_file, create_existing_table_txn};
use crate::actions::{DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA};
use crate::engine::arrow_data::ArrowEngineData;
use crate::schema::{DataType, StructField};
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
use crate::{create_row, Error, FileMeta, Result};

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
fn with_root_manifest_file_rejects_non_adaptive_table() -> Result<()> {
    let (_engine, txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    let file = dummy_root_manifest_file_meta(&txn);
    assert_result_error_with_message(
        txn.with_root_manifest_file(file),
        "adaptiveMetadata-preview",
    );
    Ok(())
}

#[test]
fn validate_manifest_write_allows_root_manifest_on_adaptive_table() -> Result<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    let file = dummy_root_manifest_file_meta(&txn);
    txn = txn.with_root_manifest_file(file)?;
    txn.validate_manifest_write_semantics()
        .map_err(Error::Kernel)?;
    Ok(())
}

#[test]
fn validate_manifest_write_rejects_root_manifest_with_file_actions() -> Result<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
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
fn with_manifest_commit_succeeds_on_adaptive_table() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    assert!(matches!(txn.manifest_write, Some(ManifestWrite::Commit(_))));
    Ok(())
}

#[test]
fn with_manifest_commit_rejects_non_adaptive_table() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    let result = txn.with_manifest_commit(engine.as_ref());
    assert_result_error_with_message(result, "adaptiveMetadata-preview");
    Ok(())
}

// Repeated calls reuse the state built by the first call rather than rebuild it. Proven by swapping
// in a non-adaptive config after staging: the second call succeeds only if it skips try_new (which
// rejects non-adaptive configs).
#[test]
fn with_manifest_commit_reuses_state_on_repeated_calls() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    txn.effective_table_config = MockTableConfigurationBuilder::new().build();
    assert!(
        txn.with_manifest_commit(engine.as_ref()).is_ok(),
        "repeated call must reuse the staged state instead of re-running try_new"
    );
    Ok(())
}

// === mutual exclusion (enforced when staging) ===

#[test]
fn with_manifest_commit_rejects_when_root_manifest_staged() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
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
fn with_root_manifest_file_rejects_when_manifest_commit_staged() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    let file = dummy_root_manifest_file_meta(&txn);
    assert_result_error_with_message(txn.with_root_manifest_file(file), "mutually exclusive");
    Ok(())
}

// === checkpoint-version guard (in ManifestCommitState::try_new) ===

// A `checkpoint` action covering the snapshot's own version is fine to start a manifest commit on.
#[test]
fn manifest_commit_allows_checkpoint_covering_the_snapshot() -> Result<()> {
    let (engine, table_root) = setup_table().map_err(Error::Kernel)?;
    write_commit(
        &engine,
        &table_root,
        1,
        minimal_checkpoint_action("metadata/root-v1.parquet", 1)
            .map_err(Error::Kernel)?
            .into_engine_data(&engine)?,
    )
    .map_err(Error::Kernel)?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 1);
    // Checkpoint version (1) >= snapshot version (1), so the guard passes.
    ManifestCommitState::try_new(&engine, snapshot.clone(), 2, &adaptive_table_config())
        .map_err(Error::Kernel)?;
    Ok(())
}

// A delta commit landing after the last `checkpoint` action is not yet supported.
#[test]
fn manifest_commit_rejects_delta_commits_after_last_checkpoint() -> Result<()> {
    let (engine, table_root) = setup_table().map_err(Error::Kernel)?;
    write_commit(
        &engine,
        &table_root,
        1,
        minimal_checkpoint_action("metadata/root-v1.parquet", 1)
            .map_err(Error::Kernel)?
            .into_engine_data(&engine)?,
    )
    .map_err(Error::Kernel)?;
    // A later delta commit bumps the snapshot past the checkpoint version.
    let domain_metadata = DomainMetadata::new("test.domain".to_string(), "{}".to_string());
    write_commit(
        &engine,
        &table_root,
        2,
        create_row(&engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), domain_metadata)
            .map_err(Error::Kernel)?,
    )
    .map_err(Error::Kernel)?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 2);
    let result =
        ManifestCommitState::try_new(&engine, snapshot.clone(), 3, &adaptive_table_config());
    assert_result_error_with_message(result, "does not currently support delta log commits");
    Ok(())
}

// === physical schema source / schema evolution ===

// The leaf writer's physical schema must come from the effective table config passed to try_new,
// not from the read snapshot (which may predate schema evolution).
#[test]
fn new_leaf_node_writer_uses_effective_config_schema_not_snapshot() -> Result<()> {
    let (engine, table_root) = setup_table().map_err(Error::Kernel)?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let config = adaptive_table_config();
    let state = ManifestCommitState::try_new(&engine, snapshot.clone(), 1, &config)
        .map_err(Error::Kernel)?;
    let writer = state.new_leaf_node_writer(&engine);
    assert_eq!(writer.physical_schema(), &config.physical_schema());
    Ok(())
}

#[test]
fn with_schema_changes_rejects_after_staging_manifest_commit() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    let result = txn.with_schema_changes(vec![SchemaOperation::add_column(
        None,
        StructField::nullable("fresh_column", DataType::INTEGER),
    )]);
    assert_result_error_with_message(result, "after staging a manifest commit");
    Ok(())
}

#[test]
fn manifest_commit_after_schema_change_uses_evolved_schema() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    let mut txn = txn.with_schema_changes(vec![SchemaOperation::add_column(
        None,
        StructField::nullable("fresh_column", DataType::INTEGER),
    )])?;
    let expected = txn.effective_table_config.physical_schema();
    let writer = txn
        .with_manifest_commit(engine.as_ref())?
        .new_leaf_node_writer(engine.as_ref());
    assert_eq!(writer.physical_schema(), &expected);
    Ok(())
}

// === commit ===

#[test]
fn commit_rejects_pending_manifest_commit() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    txn.with_manifest_commit(engine.as_ref())?;
    assert_result_error_with_message(txn.commit(engine.as_ref()), "not yet supported");
    Ok(())
}

// === leaf writer ===

#[test]
fn leaf_writer_ops_unsupported() -> Result<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn().map_err(Error::Kernel)?;
    txn.effective_table_config = adaptive_table_config();
    let mut leaf_writer = txn
        .with_manifest_commit(engine.as_ref())?
        .new_leaf_node_writer(engine.as_ref());
    let add_batch = create_valid_add_file_batch(false /* all_nullable */);
    assert_result_error_with_message(
        leaf_writer.add_files(engine.as_ref(), Box::new(ArrowEngineData::new(add_batch))),
        "not yet supported",
    );
    assert_result_error_with_message(leaf_writer.finish(engine.as_ref()), "not yet supported");
    Ok(())
}
