//! Integration tests for `Transaction::with_external_root_manifest`.
#![cfg(feature = "adaptive-metadata-in-dev")]

use std::sync::Arc;

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::{Engine, FileMeta};
use serde_json::json;
use test_utils::{
    begin_transaction, create_table, create_table_with_column_mapping_mode, engine_store_setup,
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

/// Reads the raw actions of type `action_type` (e.g. `"checkpoint"`) from a commit file.
async fn read_commit_actions(
    store: &DynObjectStore,
    table_url: &Url,
    version: u64,
    action_type: &str,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let commit_path = table_url.join(&format!("_delta_log/{version:020}.json"))?;
    let path = Path::from_url_path(commit_path.path())?;
    let content = store.get(&path).await?.bytes().await?;
    let parsed: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&content)
        .into_iter::<serde_json::Value>()
        .collect::<Result<_, _>>()?;
    Ok(parsed
        .into_iter()
        .filter_map(|v| v.get(action_type).cloned())
        .collect())
}

/// Creates a table supporting `adaptiveMetadata-preview` (and its full dependency chain) at
/// version 0, and loads a snapshot at that version.
async fn setup_adaptive_metadata_table(
    table_name: &str,
) -> Result<(impl Engine, Arc<DynObjectStore>, Url, SnapshotRef), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup(table_name, None);
    let schema = schema_ref! { nullable "id": INTEGER };

    create_table_with_column_mapping_mode(
        store.clone(),
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
    Ok((engine, store, table_url, snapshot))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_external_root_manifest_produces_a_self_contained_checkpoint_action(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, store, table_url, snapshot) =
        setup_adaptive_metadata_table("external_root_manifest_checkpoint").await?;

    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let txn = begin_transaction(snapshot, &engine)?.with_external_root_manifest(file.clone())?;
    txn.commit(&engine)?.unwrap_committed();

    let checkpoint_actions =
        read_commit_actions(store.as_ref(), &table_url, 1, "checkpoint").await?;
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

    assert!(entries.iter().any(|e| e.get("protocol").is_some()));
    assert!(entries.iter().any(|e| e.get("metaData").is_some()));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_external_root_manifest_rejects_a_file_outside_the_table_root(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _store, _table_url, snapshot) =
        setup_adaptive_metadata_table("external_root_manifest_locality").await?;

    let file = FileMeta {
        location: Url::parse("memory:///elsewhere/root.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let result = begin_transaction(snapshot, &engine)?.with_external_root_manifest(file);
    assert!(result.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_external_root_manifest_merges_domain_metadata_and_transactions(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, store, table_url, snapshot) =
        setup_adaptive_metadata_table("external_root_manifest_merge").await?;

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
        .with_external_root_manifest(file)?
        .with_domain_metadata("my.domain".to_string(), "v2".to_string())
        .with_transaction_id("app-2".to_string(), 7);
    txn.commit(&engine)?.unwrap_committed();

    let checkpoint_actions =
        read_commit_actions(store.as_ref(), &table_url, 2, "checkpoint").await?;
    assert_eq!(checkpoint_actions.len(), 1);
    let entries = checkpoint_actions[0]
        .as_array()
        .expect("checkpoint is an array");

    let domain_metadata: std::collections::HashMap<String, String> = entries
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

    let transactions: std::collections::HashMap<String, i64> = entries
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
async fn test_with_external_root_manifest_requires_the_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup("external_root_manifest_no_feature", None);
    let schema = schema_ref! { nullable "id": INTEGER };
    create_table(store, table_url.clone(), schema, &[], true, vec![], vec![]).await?;

    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let result = test_utils::load_and_begin_transaction(table_url.as_str(), &engine)?
        .with_external_root_manifest(file);
    assert!(result.is_err());
    Ok(())
}
