//! Integration tests for `Transaction::with_root_manifest_file`.
#![cfg(feature = "adaptive-metadata-in-dev")]

use std::collections::HashMap;

use delta_kernel::schema::{schema_ref, DataType, StructField};
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::transaction::{Transaction, UpdateTableOperation};
use delta_kernel::{Engine, FileMeta, KernelError, KernelResult};
use rstest::rstest;
use serde_json::{json, Value};
use tempfile::TempDir;
use test_utils::{
    assert_result_error_with_message, begin_transaction_with, create_table,
    create_table_with_column_mapping_mode, engine_store_setup, read_actions_from_commit,
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

/// Creates a file-backed table supporting `adaptiveMetadata-preview` (and its dependencies) at
/// version 0, and loads a snapshot at that version. The returned [`TempDir`] must be kept alive
/// for the table's lifetime.
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
    let txn = begin_transaction_with(
        snapshot,
        &engine,
        UpdateTableOperation::Write,
        "test-engine",
        true,
        |builder| builder,
    )?
    .with_root_manifest_file(file.clone())?;
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
async fn alter_table_commit_rejects_root_manifest() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, before) =
        setup_adaptive_metadata_table("alter_table_root_manifest").await?;
    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let txn = before
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test-engine", false)
        .add_column(StructField::nullable("fresh_column", DataType::INTEGER))
        .build_with_filesystem_committer(&engine)?
        .with_root_manifest_file(file)?;

    let error = txn.commit(&engine).unwrap_err();
    assert!(
        matches!(&error, KernelError::InvalidTransactionState(message)
        if message == "ALTER TABLE cannot include manifest writes")
    );
    let fresh = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(fresh.version(), before.version());
    assert_eq!(fresh.schema(), before.schema());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_root_manifest_file_merges_domain_metadata_and_transactions(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("root_manifest_file_merge").await?;

    let txn = begin_transaction_with(
        snapshot,
        &engine,
        delta_kernel::transaction::UpdateTableOperation::Write,
        "test-engine",
        true,
        |builder| {
            builder
                .with_transaction_id("app-1", 5)
                .with_domain_metadata("my.domain", "v1")
        },
    )?;
    let snapshot = txn.commit(&engine)?.unwrap_post_commit_snapshot();

    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let txn = begin_transaction_with(
        snapshot,
        &engine,
        delta_kernel::transaction::UpdateTableOperation::Write,
        "test-engine",
        true,
        |builder| {
            builder
                .with_transaction_id("app-2", 7)
                .with_domain_metadata("my.domain", "v2")
        },
    )?
    .with_root_manifest_file(file)?;
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
    let result = test_utils::load_and_begin_transaction(table_url.as_str(), &engine)?
        .with_root_manifest_file(file);
    assert_result_error_with_message(
        result,
        "root manifest file commit requires the adaptiveMetadata-preview feature",
    );
    Ok(())
}

/// Returns the single `commitInfo` action written at `version`.
fn commit_info_at(table_url: &Url, version: u64) -> Result<Value, Box<dyn std::error::Error>> {
    let mut commit_infos = read_actions_from_commit(table_url, version, "commitInfo")?;
    assert_eq!(
        commit_infos.len(),
        1,
        "expected one commitInfo at version {version}"
    );
    Ok(commit_infos.remove(0))
}

fn begin_with_data_change(
    snapshot: SnapshotRef,
    engine: &dyn Engine,
    data_change: bool,
) -> KernelResult<Transaction> {
    begin_transaction_with(
        snapshot,
        engine,
        UpdateTableOperation::Write,
        "test-engine",
        data_change,
        |builder| builder,
    )
}

/// `commitInfo.dataChange` records the transaction's value with or without a root manifest.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_info_data_change(
    #[values(false, true)] data_change: bool,
    #[values(false, true)] root_manifest: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("commit_info_data_change").await?;

    let mut txn = begin_transaction_with(
        snapshot,
        &engine,
        UpdateTableOperation::Write,
        "test-engine",
        data_change,
        |builder| builder,
    )?;
    if root_manifest {
        txn = txn.with_root_manifest_file(FileMeta {
            location: table_url.join("metadata/root-v1.parquet")?,
            last_modified: 0,
            size: 1024,
        })?;
    }
    txn.commit(&engine)?.unwrap_committed();
    let commit_info = commit_info_at(&table_url, 1)?;
    assert_eq!(commit_info["dataChange"], json!(data_change));
    Ok(())
}

/// Every commit on an adaptiveMetadata table records `dataChange`, and `lastManifestCommit` is
/// absent before the first manifest commit, set by it, replaced by the next manifest commit, then
/// carried forward by later log commits. The carry-forward is read from the CRC when one exists at
/// the read version, otherwise from the read version's commit file; a checksum written after the
/// carry-forward (rebuilt by log replay) matches.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_info_records_data_change_and_last_manifest_commit(
    #[values(true, false)] data_change: bool,
    #[values(true, false)] crc_at_read_version: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("commit_info_last_manifest_commit").await?;
    let assert_commit_info = |version: u64,
                              expected_last_manifest_commit: &Value|
     -> Result<(), Box<dyn std::error::Error>> {
        let commit_info = commit_info_at(&table_url, version)?;
        assert_eq!(commit_info["dataChange"], json!(data_change));
        assert_eq!(
            commit_info
                .get("lastManifestCommit")
                .unwrap_or(&Value::Null),
            expected_last_manifest_commit
        );
        Ok(())
    };
    let commit_root_manifest =
        |snapshot: SnapshotRef, version: u64| -> Result<SnapshotRef, Box<dyn std::error::Error>> {
            let file = FileMeta {
                location: table_url.join(&format!("metadata/root-v{version}.parquet"))?,
                last_modified: 0,
                size: 1024,
            };
            Ok(begin_with_data_change(snapshot, &engine, data_change)?
                .with_root_manifest_file(file)?
                .commit(&engine)?
                .unwrap_post_commit_snapshot())
        };

    // v1: log commit before any manifest commit.
    let snapshot = begin_with_data_change(snapshot, &engine, data_change)?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    assert_commit_info(1, &Value::Null)?;

    // v2: root manifest commit records itself; v3: the next one replaces it.
    let snapshot = commit_root_manifest(snapshot, 2)?;
    assert_commit_info(2, &json!({ "version": 2, "contentRootVersion": 2 }))?;
    let snapshot = commit_root_manifest(snapshot, 3)?;
    let expected_last_manifest_commit = json!({ "version": 3, "contentRootVersion": 3 });
    assert_commit_info(3, &expected_last_manifest_commit)?;

    // v4: log commit carries lastManifestCommit forward from a freshly loaded snapshot.
    if crc_at_read_version {
        snapshot.write_checksum(&engine)?;
    }
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let snapshot = begin_with_data_change(snapshot, &engine, data_change)?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    assert_commit_info(4, &expected_last_manifest_commit)?;

    snapshot.write_checksum(&engine)?;
    let crc_path = table_url
        .to_file_path()
        .expect("file url")
        .join("_delta_log/00000000000000000004.crc");
    let crc: Value = serde_json::from_str(&std::fs::read_to_string(crc_path)?)?;
    assert_eq!(crc["lastManifestCommit"], expected_last_manifest_commit);
    Ok(())
}

#[tokio::test]
async fn test_commit_info_omits_adaptive_metadata_fields_without_the_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let dir_url = Url::from_directory_path(temp_dir.path()).expect("valid directory url");
    let (store, engine, table_url) =
        engine_store_setup("commit_info_no_adaptive_metadata", Some(&dir_url));
    let schema = schema_ref! { nullable "id": INTEGER };
    create_table(store, table_url.clone(), schema, &[], true, vec![], vec![]).await?;

    test_utils::load_and_begin_transaction(table_url.as_str(), &engine)?
        .commit(&engine)?
        .unwrap_committed();

    let commit_info = commit_info_at(&table_url, 1)?;
    assert!(commit_info.get("dataChange").is_none());
    assert!(commit_info.get("lastManifestCommit").is_none());
    Ok(())
}
