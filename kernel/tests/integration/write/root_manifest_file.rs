//! Integration tests for `Transaction::with_root_manifest_file`.
#![cfg(feature = "adaptive-metadata-in-dev")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{schema_ref, DataType, StructField};
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::transaction::{Transaction, UpdateTableOperation};
use delta_kernel::{Engine, FileMeta, KernelError, KernelResult};
use rstest::rstest;
use serde_json::{json, Value};
use tempfile::TempDir;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::{
    assert_result_error_with_message, begin_transaction_with, create_table,
    create_table_with_column_mapping_mode, engine_store_setup, read_actions_from_commit,
    write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::simple_id_batch;

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
/// for the table's lifetime. The engine runs on the caller's multi-thread runtime, so tests can
/// checkpoint with it.
async fn setup_adaptive_metadata_table(
    table_name: &str,
) -> Result<
    (
        DefaultEngine<TokioMultiThreadExecutor>,
        TempDir,
        Url,
        SnapshotRef,
    ),
    Box<dyn std::error::Error>,
> {
    let temp_dir = tempfile::tempdir()?;
    let dir_url = Url::from_directory_path(temp_dir.path()).expect("valid directory url");
    let (store, _, table_url) = engine_store_setup(table_name, Some(&dir_url));
    let engine = DefaultEngineBuilder::new(store.clone())
        .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        )))
        .build();
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
        .build(&engine, Box::new(FileSystemCommitter::new()))?
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
/// carried forward by later log commits, whether the next writer reuses the post-commit snapshot or
/// loads a fresh one.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_info_records_data_change_and_last_manifest_commit(
    #[values(true, false)] data_change: bool,
    #[values(true, false)] reload_snapshot: bool,
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

    // v4: log commit carries lastManifestCommit forward.
    let snapshot = if reload_snapshot {
        Snapshot::builder_for(table_url.clone()).build(&engine)?
    } else {
        snapshot
    };
    begin_with_data_change(snapshot, &engine, data_change)?
        .commit(&engine)?
        .unwrap_committed();
    assert_commit_info(4, &expected_last_manifest_commit)?;
    Ok(())
}

/// A manifest commit records its files in the manifest rather than as add/remove actions, so
/// kernel cannot count them: file stats are unknown from the manifest commit onward and no
/// checksum can be written, whether it is resolved from the post-commit snapshot, by replaying
/// the log, or from a checkpoint.
#[rstest]
#[case::post_commit("post_commit", "Indeterminate file stats")]
#[case::log_replay("log_replay", "Indeterminate file stats")]
#[case::checkpoint(
    "checkpoint",
    "checkpoint 1 may not list the files of adaptiveMetadata manifest commit 1"
)]
#[case::checkpoint_with_tail(
    "checkpoint_with_tail",
    "checkpoint 1 may not list the files of adaptiveMetadata manifest commit 1"
)]
#[tokio::test(flavor = "multi_thread")]
async fn test_no_checksum_after_manifest_commit(
    #[case] source: &str,
    #[case] expected_error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("no_checksum_after_manifest_commit").await?;
    let file = FileMeta {
        location: table_url.join("metadata/root-v1.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    let snapshot = begin_with_data_change(snapshot, &engine, true)?
        .with_root_manifest_file(file)?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    let snapshot = match source {
        "post_commit" => snapshot,
        "log_replay" => Snapshot::builder_for(table_url.clone()).build(&engine)?,
        "checkpoint" => {
            snapshot.checkpoint(&engine, None)?;
            Snapshot::builder_for(table_url.clone()).build(&engine)?
        }
        _ => {
            snapshot.checkpoint(&engine, None)?;
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            begin_with_data_change(snapshot, &engine, true)?
                .commit(&engine)?
                .unwrap_committed();
            Snapshot::builder_for(table_url.clone()).build(&engine)?
        }
    };
    assert!(snapshot.get_file_stats_if_present().is_none());
    assert_result_error_with_message(snapshot.write_checksum(&engine), expected_error);
    Ok(())
}

/// `lastManifestCommit` round-trips through a CRC: a fresh snapshot reads it from the CRC at the
/// manifest commit, the next log commit adds a file and carries the pointer forward, and that
/// commit's checksum counts the file and records the pointer.
#[tokio::test(flavor = "multi_thread")]
async fn test_last_manifest_commit_carried_through_crc() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir, table_url, snapshot) =
        setup_adaptive_metadata_table("last_manifest_commit_through_crc").await?;
    begin_with_data_change(snapshot, &engine, true)?
        .commit(&engine)?
        .unwrap_post_commit_snapshot()
        .write_checksum(&engine)?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let file = FileMeta {
        location: table_url.join("metadata/root-v2.parquet")?,
        last_modified: 0,
        size: 1024,
    };
    begin_with_data_change(snapshot, &engine, true)?
        .with_root_manifest_file(file)?
        .commit(&engine)?
        .unwrap_committed();

    write_updated_crc_for_manifest_commit(&table_url, 2)?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let batch = simple_id_batch(&snapshot.schema(), vec![1, 2]);
    write_batch_to_table(&snapshot, &engine, batch, HashMap::new())
        .await?
        .write_checksum(&engine)?;
    let expected_last_manifest_commit = json!({ "version": 2, "contentRootVersion": 2 });
    assert_eq!(
        commit_info_at(&table_url, 3)?["lastManifestCommit"],
        expected_last_manifest_commit
    );
    let crc = read_crc(&table_url, 3)?;
    assert_eq!(crc["lastManifestCommit"], expected_last_manifest_commit);
    let adds = read_actions_from_commit(&table_url, 3, "add")?;
    assert_eq!(adds.len(), 1);
    assert_eq!(crc["numFiles"], 1);
    assert_eq!(crc["tableSizeBytes"], adds[0]["size"]);
    Ok(())
}

/// Writes the CRC at manifest commit `version`: the CRC at `version - 1` with that commit's
/// `lastManifestCommit` and in-commit timestamp.
fn write_updated_crc_for_manifest_commit(
    table_url: &Url,
    version: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let commit_info = commit_info_at(table_url, version)?;
    let mut crc = read_crc(table_url, version - 1)?;
    crc["lastManifestCommit"] = commit_info["lastManifestCommit"].clone();
    crc["inCommitTimestampOpt"] = commit_info["inCommitTimestamp"].clone();
    std::fs::write(crc_path(table_url, version), serde_json::to_string(&crc)?)?;
    Ok(())
}

fn crc_path(table_url: &Url, version: u64) -> PathBuf {
    table_url
        .to_file_path()
        .expect("file url")
        .join(format!("_delta_log/{version:020}.crc"))
}

fn read_crc(table_url: &Url, version: u64) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_str(&std::fs::read_to_string(crc_path(
        table_url, version,
    ))?)?)
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
