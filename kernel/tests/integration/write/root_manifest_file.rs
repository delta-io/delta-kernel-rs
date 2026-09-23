//! Integration tests for `Transaction::with_root_manifest_file`.
#![cfg(feature = "adaptive-metadata-in-dev")]

use std::collections::HashMap;

use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::{Engine, FileMeta};
use serde_json::json;
use tempfile::TempDir;
use test_utils::{
    begin_transaction, create_table, create_table_with_column_mapping_mode, engine_store_setup,
    read_actions_from_commit,
};
use url::Url;

const READER_FEATURES: &[&str] = &[
    "columnMapping",
    "deletionVectors",
    "adaptiveMetadata-preview",
];
const WRITER_FEATURES: &[&str] = &[
    "columnMapping",
    "deletionVectors",
    "rowTracking",
    "domainMetadata",
    "inCommitTimestamp",
    "adaptiveMetadata-preview",
];

/// Creates a file-backed table supporting `adaptiveMetadata-preview` (and its full dependency
/// chain) at version 0, and loads a snapshot at that version. The returned [`TempDir`] must be kept
/// alive for the table's lifetime.
async fn setup_adaptive_metadata_table(
    table_name: &str,
) -> Result<(impl Engine, TempDir, Url, SnapshotRef), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let dir_url = Url::from_directory_path(temp_dir.path()).expect("valid directory url");
    let (store, engine, table_url) = engine_store_setup(table_name, Some(&dir_url));
    let schema = schema_ref! { nullable "id": INTEGER };

    create_table_with_column_mapping_mode(
        store,
        table_url.clone(),
        schema,
        &[],
        true,
        READER_FEATURES.to_vec(),
        WRITER_FEATURES.to_vec(),
        "id",
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    Ok((engine, temp_dir, table_url, snapshot))
}

/// Reads the single `commitInfo` action from the commit at `version`.
fn read_commit_info(
    table_url: &Url,
    version: u64,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut commit_infos = read_actions_from_commit(table_url, version, "commitInfo")?;
    assert_eq!(commit_infos.len(), 1, "expected exactly one commitInfo");
    Ok(commit_infos.remove(0))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_last_manifest_commit_is_written_and_carried_forward(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("root_manifest_file_last_manifest_commit").await?;

    // Manifest commit at version 1 records itself: version == contentRootVersion == 1.
    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let snapshot = begin_transaction(snapshot, &engine)?
        .with_root_manifest_file(file)?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    assert_eq!(
        read_commit_info(&table_url, 1)?["lastManifestCommit"],
        json!({"version": 1, "contentRootVersion": 1})
    );

    // A following regular commit (version 2) carries the value forward unchanged.
    begin_transaction(snapshot, &engine)?
        .with_domain_metadata("my.domain".to_string(), "v1".to_string())
        .commit(&engine)?
        .unwrap_committed();
    assert_eq!(
        read_commit_info(&table_url, 2)?["lastManifestCommit"],
        json!({"version": 1, "contentRootVersion": 1})
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_last_manifest_commit_carried_forward_from_freshly_loaded_snapshot(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("root_manifest_file_fresh_load_carry").await?;

    // Manifest commit at version 1.
    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    begin_transaction(snapshot, &engine)?
        .with_root_manifest_file(file)?
        .commit(&engine)?
        .unwrap_committed();

    // Reload a FRESH snapshot (not the post-commit one) so the value comes from log replay
    // capture, then a regular commit must still carry it forward.
    let reloaded = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    begin_transaction(reloaded, &engine)?
        .with_domain_metadata("my.domain".to_string(), "v1".to_string())
        .commit(&engine)?
        .unwrap_committed();

    assert_eq!(
        read_commit_info(&table_url, 2)?["lastManifestCommit"],
        json!({"version": 1, "contentRootVersion": 1})
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_regular_commit_without_prior_manifest_commit_omits_last_manifest_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("root_manifest_file_no_prior_manifest").await?;

    // A regular commit with no preceding manifest commit has nothing to carry forward.
    begin_transaction(snapshot, &engine)?
        .with_domain_metadata("my.domain".to_string(), "v1".to_string())
        .commit(&engine)?
        .unwrap_committed();

    let commit_info = read_commit_info(&table_url, 1)?;
    assert!(
        commit_info
            .get("lastManifestCommit")
            .is_none_or(serde_json::Value::is_null),
        "expected no lastManifestCommit, got: {commit_info}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_root_manifest_file_produces_a_self_contained_checkpoint_action(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("root_manifest_file_checkpoint").await?;

    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let txn = begin_transaction(snapshot, &engine)?.with_root_manifest_file(file.clone())?;
    txn.commit(&engine)?.unwrap_committed();

    let checkpoint_actions = read_actions_from_commit(&table_url, 1, "checkpoint")?;
    assert_eq!(checkpoint_actions.len(), 1);
    let entries = checkpoint_actions[0]
        .as_array()
        .expect("checkpoint is an array");

    let content_root = entries
        .iter()
        .find_map(|e| e.get("contentRoot"))
        .expect("contentRoot entry");
    assert_eq!(content_root["path"], json!(file.location.to_string()));
    assert_eq!(content_root["sizeInBytes"], json!(1024));

    let protocol = entries
        .iter()
        .find_map(|e| e.get("protocol"))
        .expect("protocol entry");
    assert_eq!(protocol["minReaderVersion"], json!(3));
    assert_eq!(protocol["minWriterVersion"], json!(7));
    assert_eq!(protocol["readerFeatures"], json!(READER_FEATURES));
    assert_eq!(protocol["writerFeatures"], json!(WRITER_FEATURES));

    let metadata = entries
        .iter()
        .find_map(|e| e.get("metaData"))
        .expect("metaData entry");
    let schema: serde_json::Value =
        serde_json::from_str(metadata["schemaString"].as_str().unwrap())?;
    let fields = schema["fields"].as_array().expect("schema fields");
    assert!(fields
        .iter()
        .any(|f| f["name"] == json!("id") && f["type"] == json!("integer")));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_root_manifest_file_merges_domain_metadata_and_transactions(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("root_manifest_file_merge").await?;

    let txn = begin_transaction(snapshot, &engine)?
        .with_domain_metadata("my.domain".to_string(), "v1".to_string())
        .with_transaction_id("app-1".to_string(), 5);
    let snapshot = txn.commit(&engine)?.unwrap_post_commit_snapshot();

    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let txn = begin_transaction(snapshot, &engine)?
        .with_root_manifest_file(file)?
        .with_domain_metadata("my.domain".to_string(), "v2".to_string())
        .with_transaction_id("app-2".to_string(), 7);
    txn.commit(&engine)?.unwrap_committed();

    let checkpoint_actions = read_actions_from_commit(&table_url, 2, "checkpoint")?;
    assert_eq!(checkpoint_actions.len(), 1);
    let entries = checkpoint_actions[0]
        .as_array()
        .expect("checkpoint is an array");

    let domain_metadata: HashMap<String, String> = entries
        .iter()
        .filter_map(|e| e.get("domainMetadata"))
        .map(|dm| {
            (
                dm["domain"].as_str().unwrap().to_string(),
                dm["configuration"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(domain_metadata.get("my.domain"), Some(&"v2".to_string()));

    let transactions: HashMap<String, i64> = entries
        .iter()
        .filter_map(|e| e.get("txn"))
        .map(|txn| {
            (
                txn["appId"].as_str().unwrap().to_string(),
                txn["version"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(transactions.get("app-1"), Some(&5));
    assert_eq!(transactions.get("app-2"), Some(&7));

    Ok(())
}

#[tokio::test]
async fn test_with_root_manifest_file_requires_the_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup("root_manifest_file_no_feature", None);
    let schema = schema_ref! { nullable "id": INTEGER };
    create_table(store, table_url.clone(), schema, &[], true, vec![], vec![]).await?;

    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let txn = test_utils::load_and_begin_transaction(table_url.as_str(), &engine)?
        .with_root_manifest_file(file)?;
    assert!(txn.commit(&engine).is_err());
    Ok(())
}
