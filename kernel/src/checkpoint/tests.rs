use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use serde_json::{Value, from_slice, json};
use tempfile::tempdir;
#[cfg(feature = "adaptive-metadata-in-dev")]
use test_utils::assert_result_error_with_message;
use test_utils::{TestAction, actions_to_string, add_commit, delta_path_for_version};
use url::Url;

use crate::action_reconciliation::{
    ActionReconciliationIterator, ActionReconciliationIteratorState, DEFAULT_RETENTION_SECS,
    deleted_file_retention_timestamp_with_time,
};
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::actions::{Add, Metadata, Protocol, Remove, TIGHT_BOUNDS};
use crate::arrow::array::{
    Array, AsArray, Int64Array, MapArray, RecordBatch, StructArray, create_array,
};
use crate::arrow::datatypes::{DataType, Field, Int64Type, Schema};
use crate::checkpoint::{
    CHECKPOINT_ACTIONS_SCHEMA_V2, CheckpointSpec, CheckpointWriter, LastCheckpointHintStats,
    V2CheckpointConfig, create_last_checkpoint_data,
};
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::sync::SyncEngine;
use crate::log_replay::HasSelectionVector;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::object_store::{ObjectStore as _, ObjectStoreExt as _};
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::schema::{DataType as KernelDataType, StructField, schema_ref};
use crate::snapshot::CheckpointWriteResult;
use crate::table_features::TableFeature;
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::unit_test_utils::Action;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, test_schema_flat_with_column_mapping,
};
use crate::{Engine as _, FileMeta, KernelError, KernelResult, LogPath, Result, Snapshot};

#[rstest::rstest]
#[case::default_retention(
    None,
    10_000_000 - (DEFAULT_RETENTION_SECS as i64 * 1_000)
)]
#[case::zero_retention(Some(Duration::from_secs(0)), 10_000_000)]
#[case::custom_retention(Some(Duration::from_secs(2_000)), 10_000_000 - 2_000_000)]
fn test_deleted_file_retention_timestamp(
    #[case] retention: Option<Duration>,
    #[case] expected_timestamp: i64,
) -> KernelResult<()> {
    let reference_time_secs = 10_000;
    let reference_time = Duration::from_secs(reference_time_secs);

    let result = deleted_file_retention_timestamp_with_time(retention, reference_time)?;
    assert_eq!(result, expected_timestamp);

    Ok(())
}

#[rstest::rstest]
#[case::equal_sizes(100, 100, true)]
#[case::writer_reported_less(100, 200, false)]
#[case::writer_reported_more(200, 100, false)]
fn test_verify_written_size(
    #[case] written_size: u64,
    #[case] observed_size: u64,
    #[case] expect_ok: bool,
) {
    let path = Url::parse("memory:///_delta_log/00000000000000000001.checkpoint.parquet").unwrap();
    let result = super::verify_written_size(&path, written_size, observed_size);
    if expect_ok {
        assert!(
            result.is_ok(),
            "expected Ok for equal sizes, got {result:?}"
        );
    } else {
        assert!(
            matches!(result, Err(crate::KernelError::Generic(_))),
            "expected KernelError::Generic for size mismatch, got {result:?}"
        );
    }
}

#[tokio::test]
async fn test_create_checkpoint_metadata_batch() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // 1st commit (version 0) - metadata and protocol actions
    // Protocol action includes the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
        ],
        0,
    )
    .await?;

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let snapshot_version = snapshot.version();
    let writer = snapshot.create_checkpoint_writer(&engine)?;

    // Use V2 schema for the checkpoint metadata batch
    let checkpoint_batch =
        writer.create_checkpoint_metadata_batch(&engine, &CHECKPOINT_ACTIONS_SCHEMA_V2)?;
    assert!(checkpoint_batch.filtered_data.has_selected_rows());

    // Verify the underlying EngineData contains the expected fields
    let (underlying_data, _) = checkpoint_batch.filtered_data.into_parts();
    let arrow_engine_data = ArrowEngineData::try_from_engine_data(underlying_data)?;
    let record_batch = arrow_engine_data.record_batch();

    // Verify the schema has the expected fields
    let schema = record_batch.schema();
    assert!(
        schema.field_with_name("checkpointMetadata").is_ok(),
        "Schema should have checkpointMetadata field"
    );
    assert!(
        schema.field_with_name("add").is_ok(),
        "Schema should have add field"
    );
    assert!(
        schema.field_with_name("remove").is_ok(),
        "Schema should have remove field"
    );

    // Verify we have one row
    assert_eq!(record_batch.num_rows(), 1);

    // Verify the checkpointMetadata action carries the expected tags and version
    let checkpoint_metadata = record_batch
        .column_by_name("checkpointMetadata")
        .expect("Schema should have checkpointMetadata field")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("checkpointMetadata must be a struct");
    let tags = checkpoint_metadata
        .column_by_name("tags")
        .expect("checkpointMetadata must carry a tags field");
    assert!(
        tags.as_any().downcast_ref::<MapArray>().is_some(),
        "tags must be a map"
    );
    assert!(tags.is_null(0), "tags should be written null");
    let version = checkpoint_metadata
        .column_by_name("version")
        .expect("checkpointMetadata must carry a version field")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("version must be an int64");
    assert_eq!(version.value(0), snapshot_version as i64);

    // Verify action counts
    assert_eq!(checkpoint_batch.actions_count, 1);
    assert_eq!(checkpoint_batch.add_actions_count, 0);

    Ok(())
}

#[test]
fn test_create_last_checkpoint_data() -> KernelResult<()> {
    let version = 10;
    let total_actions_counter = 100;
    let add_actions_counter = 75;
    let size_in_bytes: i64 = 1024 * 1024; // 1MB
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // Create last checkpoint metadata
    let last_checkpoint_batch = create_last_checkpoint_data(
        &engine,
        version,
        total_actions_counter,
        add_actions_counter,
        size_in_bytes,
    )?;

    // Verify the underlying EngineData contains the expected `LastCheckpointInfo` schema and data
    let arrow_engine_data = ArrowEngineData::try_from_engine_data(last_checkpoint_batch)?;
    let record_batch = arrow_engine_data.record_batch();

    // Build the expected RecordBatch
    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("version", DataType::Int64, false),
        Field::new("size", DataType::Int64, false),
        Field::new("parts", DataType::Int64, true),
        Field::new("sizeInBytes", DataType::Int64, true),
        Field::new("numOfAddFiles", DataType::Int64, true),
    ]));
    let expected = RecordBatch::try_new(
        expected_schema,
        vec![
            create_array!(Int64, [version]),
            create_array!(Int64, [total_actions_counter]),
            create_array!(Int64, [None]),
            create_array!(Int64, [size_in_bytes]),
            create_array!(Int64, [add_actions_counter]),
        ],
    )
    .unwrap();

    assert_eq!(*record_batch, expected);
    Ok(())
}

/// TODO(#855): Merge copies and move to `test_utils`
/// Create an in-memory store and return the store and the URL for the store's _delta_log directory.
pub(crate) fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (
        Arc::new(InMemory::new()),
        Url::parse("memory:///")
            .unwrap()
            .join("_delta_log/")
            .unwrap(),
    )
}

/// TODO(#855): Merge copies and move to `test_utils`
/// Writes all actions to a _delta_log json commit file in the store.
/// This function formats the provided filename into the _delta_log directory.
pub(super) async fn write_commit_to_store(
    store: &Arc<InMemory>,
    actions: Vec<Action>,
    version: u64,
) -> KernelResult<()> {
    let json_lines: Vec<String> = actions
        .into_iter()
        .map(|action| serde_json::to_string(&action).expect("action to string"))
        .collect();
    let content = json_lines.join("\n");
    let commit_path = delta_path_for_version(version, "json");
    store.put(&commit_path, content.into()).await?;
    Ok(())
}

/// Create a Protocol action without v2Checkpoint feature support
fn create_basic_protocol_action() -> Action {
    Action::Protocol(
        Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap(),
    )
}

/// Create a Protocol action with catalogManaged feature support. Per the Delta protocol,
/// catalogManaged depends on inCommitTimestamp.
fn create_catalog_managed_protocol_action() -> Action {
    Action::Protocol(
        Protocol::try_new_modern(["catalogManaged"], ["catalogManaged", "inCommitTimestamp"])
            .unwrap(),
    )
}

/// Create a Protocol action with v2Checkpoint feature support
pub(super) fn create_v2_checkpoint_protocol_action() -> Action {
    Action::Protocol(Protocol::try_new_modern(vec!["v2Checkpoint"], vec!["v2Checkpoint"]).unwrap())
}

/// Create a Metadata action with the given table configuration.
fn create_metadata_action_with_config(configuration: HashMap<String, String>) -> Action {
    Action::Metadata(
        Metadata::try_new(
            Some("test-table".into()),
            None,
            schema_ref! { nullable "value": INTEGER },
            vec![],
            0,
            configuration,
        )
        .unwrap(),
    )
}

/// Create a Metadata action with no configuration.
pub(super) fn create_metadata_action() -> Action {
    create_metadata_action_with_config(HashMap::new())
}

/// Create a simple Add action with the specified path (no stats)
pub(super) fn create_add_action(path: &str) -> Action {
    Action::Add(Add {
        path: path.into(),
        data_change: true,
        ..Default::default()
    })
}

/// Create a Remove action with the specified path
///
/// The remove action has deletion_timestamp set to i64::MAX to ensure the
/// remove action is not considered expired during testing.
pub(super) fn create_remove_action(path: &str) -> Action {
    Action::Remove(Remove {
        path: path.into(),
        data_change: true,
        deletion_timestamp: Some(i64::MAX), // Ensure the remove action is not expired
        ..Default::default()
    })
}

fn try_finalize_checkpoint(
    writer: CheckpointWriter,
    engine: &dyn crate::Engine,
    metadata: &FileMeta,
    data_iter: ActionReconciliationIterator,
) -> KernelResult<()> {
    let state = data_iter.state();
    drop(data_iter);
    let state = Arc::into_inner(state).expect("no other Arc references");
    let last_checkpoint_stats = LastCheckpointHintStats::from_reconciliation_state(
        state,
        metadata.size,
        0, /* num_sidecars */
    )?;
    writer.finalize(engine, &last_checkpoint_stats)
}

/// Helper to verify the contents of the `_last_checkpoint` file
async fn assert_last_checkpoint_contents(
    store: &Arc<InMemory>,
    expected_version: u64,
    expected_size: u64,
    expected_num_add_files: u64,
    expected_size_in_bytes: u64,
) -> KernelResult<()> {
    let last_checkpoint_data = read_last_checkpoint_file(store).await?;
    let expected_data = json!({
        "version": expected_version,
        "size": expected_size,
        "sizeInBytes": expected_size_in_bytes,
        "numOfAddFiles": expected_num_add_files,
    });
    assert_eq!(last_checkpoint_data, expected_data);
    Ok(())
}

/// Reads the `_last_checkpoint` file from storage
async fn read_last_checkpoint_file(store: &Arc<InMemory>) -> KernelResult<Value> {
    let path = Path::from("_delta_log/_last_checkpoint");
    let data = store.get(&path).await?;
    let byte_data = data.bytes().await?;
    Ok(from_slice(&byte_data)?)
}

/// Tests the `checkpoint()` API with:
/// - A table that does not support v2Checkpoint
/// - No version specified (latest version is used)
#[tokio::test]
async fn test_v1_checkpoint_latest_version_by_default() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // 1st commit: adds `fake_path_1`
    write_commit_to_store(
        &store,
        vec![create_add_action_with_stats("fake_path_1", 10)],
        0,
    )
    .await?;

    // 2nd commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            create_add_action_with_stats("fake_path_2", 20),
            create_remove_action("fake_path_1"),
        ],
        1,
    )
    .await?;

    // 3rd commit: metadata & protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![create_metadata_action(), create_basic_protocol_action()],
        2,
    )
    .await?;

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000002.checkpoint.parquet")
            .map_err(KernelError::from)?
    );

    let result = writer.checkpoint_data(&engine)?;
    let mut data_iter = result;
    // The first batch should be the metadata and protocol actions.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector(), &[true, true]);

    // The second batch should include both the add action and the remove action
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector(), &[true, true]);

    // The third batch should not be included as the selection vector does not
    // contain any true values, as the file added is removed in a following commit.
    assert!(data_iter.next().is_none());

    // Finalize and verify checkpoint metadata
    let size_in_bytes = 10;
    let metadata = FileMeta {
        location: Url::parse("memory:///fake_path_2").map_err(KernelError::from)?,
        last_modified: 0,
        size: size_in_bytes,
    };
    try_finalize_checkpoint(writer, &engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: latest version (2)
    // - size: 1 metadata + 1 protocol + 1 add action + 1 remove action
    // - numOfAddFiles: 1 add file from 2nd commit (fake_path_2)
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 2, 4, 1, size_in_bytes).await?;

    Ok(())
}

/// Tests the `checkpoint()` API with:
/// - A table that does not support v2Checkpoint
/// - A specific version specified (version 0)
#[tokio::test]
async fn test_v1_checkpoint_specific_version() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // 1st commit (version 0) - metadata and protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )
    .await?;

    // 2nd commit (version 1) - add actions
    write_commit_to_store(
        &store,
        vec![
            create_add_action_with_stats("file1.parquet", 100),
            create_add_action_with_stats("file2.parquet", 200),
        ],
        1,
    )
    .await?;

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    // Specify version 0 for checkpoint
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(0)
        .build(&engine)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;

    // Verify the checkpoint file path is the specified version.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")
            .map_err(KernelError::from)?
    );

    let result = writer.checkpoint_data(&engine)?;
    let mut data_iter = result;
    // The first batch should be the metadata and protocol actions.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector(), &[true, true]);

    // No more data should exist because we only requested version 0
    assert!(data_iter.next().is_none());

    // Finalize and verify checkpoint metadata
    let size_in_bytes = 10;
    let metadata = FileMeta {
        location: Url::parse("memory:///fake_path_2").map_err(KernelError::from)?,
        last_modified: 0,
        size: size_in_bytes,
    };
    try_finalize_checkpoint(writer, &engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: specified version (0)
    // - size: 1 metadata + 1 protocol
    // - numOfAddFiles: no add files in version 0
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 0, 2, 0, size_in_bytes).await?;

    Ok(())
}

#[tokio::test]
async fn test_finalize_errors_if_checkpoint_data_iterator_is_not_exhausted() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // 1st commit (version 0) - metadata and protocol actions
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )
    .await?;

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(0)
        .build(&engine)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let data_iter = writer.checkpoint_data(&engine)?;

    /* The returned data iterator has batches that we do not consume */

    // Attempting to build LastCheckpointHintStats from a non-exhausted state should fail
    let state = data_iter.state();
    drop(data_iter);
    let state = Arc::into_inner(state).expect("no other Arc references");
    let err = LastCheckpointHintStats::from_reconciliation_state(
        state, 0, /* size_in_bytes */
        0, /* num_sidecars */
    )
    .expect_err("from_reconciliation_state should fail on non-exhausted iterator");
    assert!(
        err.to_string()
            .contains("reconciliation iterator must be fully consumed")
    );

    Ok(())
}

#[test]
fn test_last_checkpoint_hint_stats_with_nonzero_num_sidecars() -> KernelResult<()> {
    let state = ActionReconciliationIteratorState::new_exhausted(5, 2);
    let stats = LastCheckpointHintStats::from_reconciliation_state(state, 100, 3)?;
    assert_eq!(stats.num_actions, 8); // 5 reconciled + 3 sidecar actions
    assert_eq!(stats.size_in_bytes, 100);
    assert_eq!(stats.num_of_add_files, 2); // sidecar actions do not bump this
    Ok(())
}

#[rstest::rstest]
#[case::num_sidecars_exceeds_i64(
    0,
    0,
    0,
    u64::MAX,
    "num_sidecars 18446744073709551615 exceeds i64"
)]
#[case::actions_count_overflow(
    i64::MAX,
    0,
    0,
    1,
    "checkpoint action count overflowed i64: 9223372036854775807 + 1"
)]
#[case::size_in_bytes_exceeds_i64(
    0,
    0,
    u64::MAX,
    0,
    "size_in_bytes 18446744073709551615 exceeds i64"
)]
fn test_last_checkpoint_hint_stats_rejects_invalid_input(
    #[case] actions_count: i64,
    #[case] add_actions_count: i64,
    #[case] size_in_bytes: u64,
    #[case] num_sidecars: u64,
    #[case] expected_err_substring: &str,
) {
    let state = ActionReconciliationIteratorState::new_exhausted(actions_count, add_actions_count);
    let err =
        LastCheckpointHintStats::from_reconciliation_state(state, size_in_bytes, num_sidecars)
            .expect_err("invalid input must error");
    assert!(
        err.to_string().contains(expected_err_substring),
        "error should mention {expected_err_substring}, got: {err}"
    );
}

/// Tests the `checkpoint()` API with:
/// - A table that does supports v2Checkpoint
/// - No version specified (latest version is used)
#[tokio::test]
async fn test_v2_checkpoint_supported_table() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // 1st commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            create_add_action_with_stats("fake_path_2", 50),
            create_remove_action("fake_path_1"),
        ],
        0,
    )
    .await?;

    // 2nd commit: metadata & protocol actions
    // Protocol action includes the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
        ],
        1,
    )
    .await?;

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000001.checkpoint.parquet")
            .map_err(KernelError::from)?
    );

    let result = writer.checkpoint_data(&engine)?;
    let mut data_iter = result;
    // The first batch should be the metadata and protocol actions.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector(), &[true, true]);

    // The second batch should include both the add action and the remove action
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector(), &[true, true]);

    // The third batch should be the CheckpointMetaData action.
    let batch = data_iter.next().unwrap()?;
    // According to the new contract, with_all_rows_selected creates an empty selection vector
    assert_eq!(batch.selection_vector(), &[] as &[bool]);
    assert!(batch.has_selected_rows());

    // No more data should exist
    assert!(data_iter.next().is_none());

    // Finalize and verify checkpoint metadata
    let size_in_bytes = 10;
    let metadata = FileMeta {
        location: Url::parse("memory:///fake_path_2").map_err(KernelError::from)?,
        last_modified: 0,
        size: size_in_bytes,
    };
    try_finalize_checkpoint(writer, &engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: latest version (1)
    // - size: 1 metadata + 1 protocol + 1 add action + 1 remove action + 1 checkpointMetadata
    // - numOfAddFiles: 1 add file from version 0
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 1, 5, 1, size_in_bytes).await?;

    Ok(())
}

#[tokio::test]
async fn test_no_checkpoint_on_unpublished_snapshot() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // normal commit with catalog-managed protocol
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action_with_config(HashMap::from([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            )])),
            create_catalog_managed_protocol_action(),
        ],
        0,
    )
    .await?;

    // staged commit
    let staged_commit_path = Path::from(
        "_delta_log/_staged_commits/00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
    );
    let add_action = Action::Add(Add::default());
    store
        .put(
            &staged_commit_path,
            serde_json::to_string(&add_action).unwrap().into(),
        )
        .await
        .unwrap();

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let staged_commit = FileMeta {
        location: Url::parse(
            "memory:///_delta_log/_staged_commits/00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        ).map_err(KernelError::from)?,
        last_modified: 0,
        size: 100,
    };
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(vec![LogPath::try_new(staged_commit).unwrap()])
        .with_max_catalog_version(1)
        .build(&engine)?;

    assert!(matches!(
        snapshot.create_checkpoint_writer(&engine).unwrap_err(),
        crate::KernelError::UnpublishedVersion(1)
    ));
    Ok(())
}

#[cfg(feature = "adaptive-metadata-in-dev")]
#[rstest::rstest]
#[tokio::test]
async fn test_no_standalone_checkpoint_on_adaptive_metadata_table(
    #[values(
        None,
        Some(CheckpointSpec::V1),
        Some(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar))
    )]
    spec: Option<CheckpointSpec>,
) -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let config = adaptive_metadata_table_configuration(test_schema_flat_with_column_mapping(), &[]);
    let commit = [
        json!({ "commitInfo": { "timestamp": 1, "inCommitTimestamp": 1 } }),
        json!({ "protocol": config.protocol() }),
        json!({ "metaData": config.metadata() }),
    ]
    .map(|action| action.to_string())
    .join("\n");
    add_commit("memory:///", store.as_ref(), 0, commit)
        .await
        .unwrap();

    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let message =
        "Standalone checkpoints are not yet supported for tables with adaptiveMetadata enabled";
    assert_result_error_with_message(snapshot.checkpoint(&engine, spec.as_ref()), message);
    assert_result_error_with_message(snapshot.create_checkpoint_writer(&engine), message);
    Ok(())
}

/// Create an Add action with JSON stats
fn create_add_action_with_stats(path: &str, num_records: i64) -> Action {
    let stats = format!(
        r#"{{"numRecords":{num_records},"minValues":{{"id":1,"name":"alice"}},"maxValues":{{"id":100,"name":"zoe"}},"nullCount":{{"id":0,"name":5}}}}"#
    );
    Action::Add(Add {
        path: path.into(),
        data_change: true,
        stats: Some(stats),
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_checkpoint() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    // Version 0: metadata & protocol
    write_commit_to_store(
        &store,
        vec![create_metadata_action(), create_basic_protocol_action()],
        0,
    )
    .await?;

    // Version 1: add 3 files
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file1.parquet"),
            create_add_action("file2.parquet"),
            create_add_action("file3.parquet"),
        ],
        1,
    )
    .await?;

    // Version 2: add 2 more files, remove 1
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file4.parquet"),
            create_add_action("file5.parquet"),
            create_remove_action("file1.parquet"),
        ],
        2,
    )
    .await?;

    // Version 3: add 1 file, remove 2
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file6.parquet"),
            create_remove_action("file2.parquet"),
            create_remove_action("file3.parquet"),
        ],
        3,
    )
    .await?;

    // Version 4: add 2 files
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file7.parquet"),
            create_add_action("file8.parquet"),
        ],
        4,
    )
    .await?;

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;

    snapshot.checkpoint(&engine, None)?;

    // First checkpoint: 1 metadata + 1 protocol + 5 add + 3 remove = 10, numOfAddFiles = 5
    let checkpoint_path = Path::from("_delta_log/00000000000000000004.checkpoint.parquet");
    let checkpoint_size = store
        .head(&checkpoint_path)
        .await
        .map_err(KernelError::from)?
        .size;
    assert_last_checkpoint_contents(&store, 4, 10, 5, checkpoint_size).await?;

    // Version 5: add 2 files, remove 1
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file9.parquet"),
            create_add_action("file10.parquet"),
            create_remove_action("file4.parquet"),
        ],
        5,
    )
    .await?;

    // Version 6: add 1 file
    write_commit_to_store(&store, vec![create_add_action("file11.parquet")], 6).await?;

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    snapshot.checkpoint(&engine, None)?;

    // Second checkpoint: 1 metadata + 1 protocol + 7 add + 4 remove = 13, numOfAddFiles = 7
    let checkpoint_path = Path::from("_delta_log/00000000000000000006.checkpoint.parquet");
    let checkpoint_size = store
        .head(&checkpoint_path)
        .await
        .map_err(KernelError::from)?
        .size;
    assert_last_checkpoint_contents(&store, 6, 13, 7, checkpoint_size).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_preserves_domain_metadata() -> Result<()> {
    // ===== Setup =====
    let tmp_dir = tempdir().unwrap();
    let table_path = tmp_dir.path();
    let table_url = Url::from_directory_path(table_path).unwrap();
    std::fs::create_dir_all(table_path.join("_delta_log")).unwrap();

    // ===== Create Table =====
    let commit0 = [
        json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": ["domainMetadata"]
            }
        }),
        json!({
            "metaData": {
                "id": "test-table-id",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        }),
    ]
    .map(|j| j.to_string())
    .join("\n");
    std::fs::write(
        table_path.join("_delta_log/00000000000000000000.json"),
        commit0,
    )
    .unwrap();

    // ===== Create Engine =====
    let engine = SyncEngine::new();

    let commit_domain_metadata = |domain: &str, value: &str| -> Result<()> {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .with_domain_metadata(domain, value)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        let result = txn.commit(&engine)?;
        assert!(result.is_committed());
        Ok(())
    };

    // ===== Commit Domain Metadata =====
    commit_domain_metadata("foo", "bar1")?;
    commit_domain_metadata("foo", "bar2")?;

    // ===== Case 1: Verify domain metadata is preserved *before* checkpoint =====
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(snapshot.version(), 2);
    let domain_value = snapshot.reader().get_domain_metadata("foo", &engine)?;
    assert_eq!(domain_value, Some("bar2".to_string()));

    // Trigger checkpoint
    snapshot.checkpoint(&engine, None)?;

    // ===== Case 2: Verify domain metadata is preserved *after* checkpoint =====
    let snapshot = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(&engine)?;
    let domain_value = snapshot.reader().get_domain_metadata("foo", &engine)?;
    assert_eq!(domain_value, Some("bar2".to_string()));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_excludes_tombstoned_domain_metadata() -> Result<()> {
    // ===== Setup =====
    let tmp_dir = tempdir().unwrap();
    let table_path = tmp_dir.path();
    let table_url = Url::from_directory_path(table_path).unwrap();
    std::fs::create_dir_all(table_path.join("_delta_log")).unwrap();

    // ===== Create Table with domainMetadata feature =====
    let commit0 = [
        json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": ["domainMetadata"]
            }
        }),
        json!({
            "metaData": {
                "id": "test-table-id",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        }),
    ]
    .map(|j| j.to_string())
    .join("\n");
    std::fs::write(
        table_path.join("_delta_log/00000000000000000000.json"),
        commit0,
    )
    .unwrap();

    // ===== Create Engine =====
    // Use SyncEngine::new() (not new_with_store(LocalFileSystem::new())) so the engine builds
    // per-URL LocalFileSystem instances rooted at the URL's drive. The bare
    // LocalFileSystem::new() rejects absolute Windows paths via file:// URLs.
    let engine = SyncEngine::new();

    // ===== Commit domain metadata for "foo" =====
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .with_domain_metadata("foo", "bar")
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let result = txn.commit(&engine)?;
    assert!(result.is_committed());

    // Verify domain exists before removal
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(
        snapshot.reader().get_domain_metadata("foo", &engine)?,
        Some("bar".to_string())
    );

    // ===== Remove domain metadata for "foo" (tombstone) =====
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let result = snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .with_domain_metadata_removed("foo")
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?;
    assert!(result.is_committed());

    // Verify domain is gone before checkpoint
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(snapshot.reader().get_domain_metadata("foo", &engine)?, None);

    // ===== Write checkpoint =====
    snapshot.checkpoint(&engine, None)?;

    // ===== Verify tombstoned domain is NOT present after loading from checkpoint =====
    let snapshot = Snapshot::builder_for(table_url)
        .at_version(snapshot.version())
        .build(&engine)?;
    assert_eq!(snapshot.reader().get_domain_metadata("foo", &engine)?, None);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_skips_last_checkpoint_write_when_hint_version_is_newer() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    let _ = create_table(
        table_root.as_str(),
        schema_ref! { nullable "value": INTEGER },
        "test",
        true,
    )
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;

    // Version 1
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("file1.parquet".to_string())]),
    )
    .await
    .map_err(|err| crate::KernelError::generic(err.to_string()))?;

    // Version 2
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("file2.parquet".to_string())]),
    )
    .await
    .map_err(|err| crate::KernelError::generic(err.to_string()))?;

    // Checkpoint at version 2
    let snapshot_v2 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    assert_eq!(snapshot_v2.version(), 2);
    snapshot_v2.checkpoint(&engine, None)?;
    let last_checkpoint = read_last_checkpoint_file(&store).await?;
    let size_in_bytes = last_checkpoint
        .get("sizeInBytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            crate::KernelError::generic("missing or invalid sizeInBytes in _last_checkpoint")
        })?;
    assert_last_checkpoint_contents(&store, 2, 4, 2, size_in_bytes).await?;

    // Time-travel to version 1 and writing a checkpoint should not override _last_checkpoint
    let snapshot_v1 = Snapshot::builder_for(table_root)
        .at_version(1)
        .build(&engine)?;
    snapshot_v1.checkpoint(&engine, None)?;
    assert_last_checkpoint_contents(&store, 2, 4, 2, size_in_bytes).await?;
    Ok(())
}

/// Helper to create metadata action with specific stats settings
fn create_metadata_with_stats_config(
    write_stats_as_json: bool,
    write_stats_as_struct: bool,
) -> Action {
    create_metadata_with_stats_config_and_partitions(
        write_stats_as_json,
        write_stats_as_struct,
        vec![],
    )
}

/// Helper to create metadata action with stats settings and partition columns
pub(super) fn create_metadata_with_stats_config_and_partitions(
    write_stats_as_json: bool,
    write_stats_as_struct: bool,
    partition_columns: Vec<String>,
) -> Action {
    let config = HashMap::from([
        (
            "delta.checkpoint.writeStatsAsJson".to_string(),
            write_stats_as_json.to_string(),
        ),
        (
            "delta.checkpoint.writeStatsAsStruct".to_string(),
            write_stats_as_struct.to_string(),
        ),
    ]);
    Action::Metadata(
        Metadata::try_new(
            Some("test-table".into()),
            None,
            schema_ref! {
                nullable "id": LONG,
                nullable "name": STRING,
                nullable "category": STRING,
            },
            partition_columns,
            0,
            config,
        )
        .unwrap(),
    )
}

/// Verifies checkpoint schema has expected fields based on stats configuration.
/// Non-partitioned tables should never have `partitionValues_parsed`.
fn verify_checkpoint_schema(
    schema: &Schema,
    expect_stats: bool,
    expect_stats_parsed: bool,
) -> KernelResult<()> {
    verify_checkpoint_schema_with_partitions(schema, expect_stats, expect_stats_parsed, false)
}

/// Verifies checkpoint schema has expected fields based on stats and partition configuration.
fn verify_checkpoint_schema_with_partitions(
    schema: &Schema,
    expect_stats: bool,
    expect_stats_parsed: bool,
    expect_partition_values_parsed: bool,
) -> KernelResult<()> {
    let add_field = schema
        .field_with_name("add")
        .expect("schema should have 'add' field");

    if let DataType::Struct(add_fields) = add_field.data_type() {
        let has_stats = add_fields.iter().any(|f| f.name() == "stats");
        let has_stats_parsed = add_fields.iter().any(|f| f.name() == "stats_parsed");
        let has_pv_parsed = add_fields
            .iter()
            .any(|f| f.name() == "partitionValues_parsed");

        assert_eq!(
            has_stats, expect_stats,
            "stats field: expected={expect_stats}, actual={has_stats}"
        );
        assert_eq!(
            has_stats_parsed, expect_stats_parsed,
            "stats_parsed field: expected={expect_stats_parsed}, actual={has_stats_parsed}"
        );
        assert_eq!(
            has_pv_parsed, expect_partition_values_parsed,
            "partitionValues_parsed field: expected={expect_partition_values_parsed}, actual={has_pv_parsed}"
        );
    } else {
        panic!("add field should be a struct");
    }
    Ok(())
}

#[rstest::rstest]
#[case::no_dv_support(false, None)]
#[case::no_dv_support_disabled(false, Some(false))]
#[case::dv_enabled(true, Some(true))]
#[case::dv_disabled(true, Some(false))]
#[case::dv_enablement_unset(true, None)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_tight_bounds_depends_on_dv_protocol_support(
    #[case] dv_supported: bool,
    #[case] dv_enabled: Option<bool>,
    #[values(
        CheckpointSpec::V1,
        CheckpointSpec::V2(V2CheckpointConfig::NoSidecar),
        CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        })
    )]
    spec: CheckpointSpec,
    #[values(false, true)] write_json: bool,
) -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let mut features = Vec::new();
    if dv_supported {
        features.push(TableFeature::DeletionVectors);
    }
    if matches!(spec, CheckpointSpec::V2(_)) {
        features.push(TableFeature::V2Checkpoint);
    }
    let protocol = Protocol::try_new_modern(features.clone(), features)?;
    let metadata = |enable_dvs: Option<bool>| {
        let mut config = HashMap::from([
            (
                "delta.checkpoint.writeStatsAsJson".into(),
                write_json.to_string(),
            ),
            ("delta.checkpoint.writeStatsAsStruct".into(), "true".into()),
        ]);
        if let Some(enabled) = enable_dvs {
            config.insert("delta.enableDeletionVectors".into(), enabled.to_string());
        }
        Metadata::try_new(
            None,
            None,
            schema_ref! { nullable "id": LONG, nullable "name": STRING },
            vec![],
            0,
            config,
        )
    };
    let mut actions = vec![
        Action::Protocol(protocol),
        Action::Metadata(metadata(dv_supported.then_some(true))?),
    ];
    for bounds in [Some(true), None, Some(false)] {
        let mut stats = json!({
            "numRecords": 100,
            "minValues": {"id": 1, "name": "alice"},
            "maxValues": {"id": 100, "name": "zoe"},
            "nullCount": {"id": 0, "name": 5},
        });
        if let Some(bounds) = bounds {
            stats[TIGHT_BOUNDS] = bounds.into();
        }
        let deletion_vector = if dv_supported && bounds == Some(false) {
            Some(DeletionVectorDescriptor::try_new(
                DeletionVectorStorageType::Inline,
                "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
                None,
                44,
                6,
            )?)
        } else {
            None
        };
        actions.push(Action::Add(Add {
            path: format!("bounds-{bounds:?}.parquet"),
            data_change: true,
            stats: Some(stats.to_string()),
            deletion_vector,
            ..Default::default()
        }));
    }
    write_commit_to_store(&store, actions, 0).await?;

    for version in 0..=1 {
        if version == 1 {
            // The second checkpoint reads file statistics from the first checkpoint after
            // DV creation is disabled or its enablement property is removed.
            write_commit_to_store(
                &store,
                vec![Action::Metadata(metadata(dv_enabled)?)],
                version,
            )
            .await?;
        }
        let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
        assert_eq!(snapshot.version(), version);
        let (result, _) = snapshot.checkpoint(&engine, Some(&spec))?;
        assert_eq!(result, CheckpointWriteResult::Written);

        let prefix = Path::from("_delta_log");
        let objects = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(KernelError::from)?;
        let mut observed = HashMap::new();
        for object in objects {
            let path = object.location.as_ref();
            if !path.ends_with(".parquet") || !path.contains(&format!("{version:020}.checkpoint")) {
                continue;
            }
            let bytes = store
                .get(&object.location)
                .await
                .map_err(KernelError::from)?
                .bytes()
                .await
                .map_err(KernelError::from)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(KernelError::from)?
                .build()
                .map_err(KernelError::from)?;
            for batch in reader {
                let batch = batch.map_err(KernelError::from)?;
                let add = batch.column_by_name("add").unwrap().as_struct();
                let stats = add.column_by_name("stats_parsed").unwrap().as_struct();
                let bounds = stats.column_by_name(TIGHT_BOUNDS);
                assert_eq!(bounds.is_some(), dv_supported);
                if let Some(bounds) = bounds {
                    assert_eq!(bounds.data_type(), &DataType::Boolean);
                    assert!(stats.fields().find(TIGHT_BOUNDS).unwrap().1.is_nullable());
                }
                let paths = add.column_by_name("path").unwrap().as_string::<i32>();
                let num_records = stats
                    .column_by_name("numRecords")
                    .unwrap()
                    .as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    if add.is_null(row) {
                        continue;
                    }
                    assert!(stats.is_valid(row));
                    assert!(num_records.is_valid(row));
                    assert_eq!(num_records.value(row), 100);
                    let bounds = bounds.and_then(|bounds| {
                        bounds.is_valid(row).then(|| bounds.as_boolean().value(row))
                    });
                    observed.insert(paths.value(row).to_string(), bounds);
                }
            }
        }
        let mut expected = HashMap::from([
            (
                "bounds-Some(true).parquet".into(),
                dv_supported.then_some(true),
            ),
            ("bounds-None.parquet".into(), None),
        ]);
        expected.insert(
            "bounds-Some(false).parquet".into(),
            dv_supported.then_some(false),
        );
        assert_eq!(observed, expected);
    }
    Ok(())
}

#[rstest::rstest]
#[case::wide_file_still_active(false, true)]
#[case::wide_file_removed(true, true)]
#[case::json_only(false, false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_without_dv_support_omits_tight_bounds_and_preserves_json(
    #[case] remove_wide_file: bool,
    #[case] write_struct: bool,
    #[values(false, true)] write_json: bool,
    #[values(false, true)] from_checkpoint: bool,
) -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    write_commit_to_store(
        &store,
        vec![
            create_basic_protocol_action(),
            create_metadata_with_stats_config(false, true),
            Action::Add(Add {
                path: "wide.parquet".into(),
                data_change: true,
                stats: Some(
                    json!({
                        "numRecords": 100,
                        "minValues": {"id": 0},
                        "maxValues": {"id": 200},
                        "nullCount": {"id": 50},
                        "tightBounds": false,
                    })
                    .to_string(),
                ),
                ..Default::default()
            }),
        ],
        0,
    )
    .await?;
    if from_checkpoint {
        // Construct a checkpoint with the full stats schema to cover structured-only input
        // written by a producer that includes tightBounds without DV protocol support.
        let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
        let writer = snapshot.create_checkpoint_writer(&engine)?;
        let context = &writer.schema_context;
        let (schema, expr) = super::build_checkpoint_transform(
            &context.stats_config,
            &writer.read_schema,
            &context.stats_schema,
            context.partition_schema.as_ref(),
        )?;
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            writer.read_schema.clone(),
            expr,
            schema.into(),
        )?;
        let batches = writer
            .snapshot
            .log_segment()
            .read_actions(&engine, writer.read_schema.clone())?
            .map(move |batch| evaluator.evaluate(batch?.actions.as_ref()));
        let size = engine
            .parquet_handler()
            .write_parquet_file(writer.checkpoint_path()?, Box::new(batches))?;
        writer.finalize(
            &engine,
            &LastCheckpointHintStats {
                num_actions: 3,
                size_in_bytes: i64::try_from(size).unwrap(),
                num_of_add_files: 1,
            },
        )?;
    }
    let mut actions = vec![create_metadata_with_stats_config(write_json, write_struct)];
    if remove_wide_file {
        actions.push(create_remove_action("wide.parquet"));
    }
    write_commit_to_store(&store, actions, 1).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    assert_eq!(
        snapshot.checkpoint(&engine, None)?.0,
        CheckpointWriteResult::Written
    );
    assert_eq!(read_last_checkpoint_file(&store).await?["version"], 1);
    if write_struct || write_json {
        let path = Path::from("_delta_log/00000000000000000001.checkpoint.parquet");
        let bytes = store
            .get(&path)
            .await
            .map_err(KernelError::from)?
            .bytes()
            .await
            .map_err(KernelError::from)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(KernelError::from)?
            .build()
            .map_err(KernelError::from)?;
        let mut found_add = false;
        for batch in reader {
            let batch = batch.map_err(KernelError::from)?;
            let add = batch.column_by_name("add").unwrap().as_struct();
            if write_struct {
                let stats = add.column_by_name("stats_parsed").unwrap().as_struct();
                assert!(stats.column_by_name(TIGHT_BOUNDS).is_none());
            }
            for row in 0..batch.num_rows() {
                if add.is_valid(row) {
                    found_add = true;
                    if write_json {
                        let stats = add.column_by_name("stats").unwrap().as_string::<i32>();
                        let stats: Value =
                            serde_json::from_str(stats.value(row)).map_err(KernelError::from)?;
                        assert_eq!(stats[TIGHT_BOUNDS], false);
                    }
                }
            }
        }
        assert_eq!(found_add, !remove_wide_file);
    }
    Ok(())
}

/// Tests all 16 combinations of writeStatsAsJson and writeStatsAsStruct settings with a
/// full round-trip through parquet.
///
/// For each combination (json1, struct1, json2, struct2):
/// 1. Writes checkpoint 1 to parquet with (json1, struct1) settings
/// 2. Changes config to (json2, struct2)
/// 3. Reads from checkpoint 1 to produce checkpoint 2 data, exercising COALESCE paths (e.g.,
///    recovering stats from stats_parsed via ToJson, or vice versa)
/// 4. Verifies checkpoint 2 schema matches (json2, struct2)
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stats_config_round_trip(
    #[values(true, false)] json1: bool,
    #[values(true, false)] struct1: bool,
    #[values(true, false)] json2: bool,
    #[values(true, false)] struct2: bool,
) -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;

    // Commit 0: protocol + metadata with initial settings
    write_commit_to_store(
        &store,
        vec![
            create_basic_protocol_action(),
            create_metadata_with_stats_config(json1, struct1),
        ],
        0,
    )
    .await?;

    // Commit 1: add action with JSON stats
    write_commit_to_store(
        &store,
        vec![create_add_action_with_stats("file1.parquet", 100)],
        1,
    )
    .await?;

    // Write checkpoint 1 to parquet with (json1, struct1) settings
    let snapshot1 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    snapshot1.checkpoint(&engine, None)?;

    // Commit 2: update metadata with new settings
    write_commit_to_store(
        &store,
        vec![create_metadata_with_stats_config(json2, struct2)],
        2,
    )
    .await?;

    // Build snapshot that reads from checkpoint 1 + commit 2.
    // The add action for file1.parquet comes from checkpoint 1, so the COALESCE
    // expressions must recover stats across format changes.
    let snapshot2 = Snapshot::builder_for(table_root).build(&engine)?;
    let writer2 = snapshot2.create_checkpoint_writer(&engine)?;
    let mut result2 = writer2.checkpoint_data(&engine)?;

    // Verify checkpoint 2 schema matches new settings
    let first_batch = result2.next().expect("should have at least one batch")?;
    let data = first_batch.apply_selection_vector()?;
    let record_batch = data.try_into_record_batch()?;
    verify_checkpoint_schema(&record_batch.schema(), json2, struct2)?;

    // Consume remaining batches (verifies COALESCE doesn't error)
    for batch in result2 {
        let _ = batch?;
    }

    Ok(())
}

/// Same as `test_stats_config_round_trip` but with a partitioned table.
/// Verifies that `partitionValues_parsed` is included in the checkpoint schema when
/// `writeStatsAsStruct` is true, and omitted when false.
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stats_config_round_trip_partitioned(
    #[values(true, false)] json1: bool,
    #[values(true, false)] struct1: bool,
    #[values(true, false)] json2: bool,
    #[values(true, false)] struct2: bool,
) -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;

    // Commit 0: protocol + partitioned metadata with initial settings
    write_commit_to_store(
        &store,
        vec![
            create_basic_protocol_action(),
            create_metadata_with_stats_config_and_partitions(
                json1,
                struct1,
                vec!["category".into()],
            ),
        ],
        0,
    )
    .await?;

    // Commit 1: add action with partition values and JSON stats
    let mut add = Add {
        path: "category=books/file1.parquet".into(),
        data_change: true,
        stats: Some(
            r#"{"numRecords":100,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":5}}"#.into(),
        ),
        ..Default::default()
    };
    add.partition_values
        .insert("category".into(), "books".into());
    write_commit_to_store(&store, vec![Action::Add(add)], 1).await?;

    // Write checkpoint 1 with (json1, struct1) settings
    let snapshot1 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    snapshot1.checkpoint(&engine, None)?;

    // Commit 2: update metadata with new settings
    write_commit_to_store(
        &store,
        vec![create_metadata_with_stats_config_and_partitions(
            json2,
            struct2,
            vec!["category".into()],
        )],
        2,
    )
    .await?;

    // Build snapshot that reads from checkpoint 1 + commit 2
    let snapshot2 = Snapshot::builder_for(table_root).build(&engine)?;
    let writer2 = snapshot2.create_checkpoint_writer(&engine)?;
    let result2 = writer2.checkpoint_data(&engine)?;

    // Collect all checkpoint batches
    let mut all_batches = Vec::new();
    for batch_result in result2 {
        let batch = batch_result?;
        let data = batch.apply_selection_vector()?;
        all_batches.push(data.try_into_record_batch()?);
    }

    // Verify checkpoint schema matches new settings
    verify_checkpoint_schema_with_partitions(
        &all_batches[0].schema(),
        json2,
        struct2,
        struct2, // partitionValues_parsed present iff writeStatsAsStruct=true
    )?;

    // When writeStatsAsStruct=true, verify partitionValues_parsed contains correct values
    if struct2 {
        let mut found_add = false;
        for record_batch in &all_batches {
            let add_col = record_batch
                .column_by_name("add")
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            for row in 0..record_batch.num_rows() {
                if !add_col.is_valid(row) {
                    continue;
                }
                found_add = true;
                let pv_parsed = add_col
                    .column_by_name("partitionValues_parsed")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let category_col = pv_parsed
                    .column_by_name("category")
                    .expect("partitionValues_parsed should have category field");
                assert_eq!(category_col.as_string::<i32>().value(row), "books");
            }
        }
        assert!(found_add, "should have found an add action");
    }

    Ok(())
}

// This tests that we can change the metadata of a schema field in between checkpoints and still
// manage to checkpoint, with parsed stats enabled.
// The checkpoint at version 0 is written with a schema without field metadata, so its
// stats_parsed nullCount fields are plain Int64. Then a new metadata action at version 1
// adds `__CHAR_VARCHAR_TYPE_STRING` to the "name" field. When checkpointing version 1,
// the kernel builds a stats schema with that metadata on nullCount fields (via
// NullCountStatsTransform), but the stats_parsed data from the old checkpoint lacks it,
// causing an Arrow schema mismatch in the COALESCE expression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_with_varchar_metadata_on_field() -> Result<()> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());

    let config = HashMap::from([
        ("delta.checkpoint.writeStatsAsJson".into(), "true".into()),
        ("delta.checkpoint.writeStatsAsStruct".into(), "true".into()),
    ]);

    // Version 0: schema WITHOUT __CHAR_VARCHAR_TYPE_STRING + add with stats
    let schema_v0 = schema_ref! {
        nullable "id": LONG,
        nullable "name": STRING,
    };
    write_commit_to_store(
        &store,
        vec![
            create_basic_protocol_action(),
            Action::Metadata(
                Metadata::try_new(
                    Some("test".into()),
                    None,
                    schema_v0,
                    vec![],
                    0,
                    config.clone(),
                )
                .unwrap(),
            ),
            Action::Add(Add {
                path: "file1.parquet".into(),
                data_change: true,
                stats: Some(
                    r#"{"numRecords":10,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":2}}"#.into(),
                ),
                ..Default::default()
            }),
        ],
        0,
    )
    .await?;

    // Checkpoint version 0: stats_parsed nullCount fields are plain Int64 (no metadata)
    let table_root = Url::parse("memory:///").map_err(KernelError::from)?;
    Snapshot::builder_for(table_root.clone())
        .build(&engine)?
        .checkpoint(&engine, None)?;

    // Version 1: new metadata WITH __CHAR_VARCHAR_TYPE_STRING on the "name" field
    let schema_v1 = schema_ref! {
        nullable "id": LONG,
        (StructField::nullable("name", KernelDataType::STRING).with_metadata([(
                "__CHAR_VARCHAR_TYPE_STRING",
                crate::schema::MetadataValue::String("varchar(255)".to_string()),
            )])),
    };
    write_commit_to_store(
        &store,
        vec![Action::Metadata(
            Metadata::try_new(Some("test".into()), None, schema_v1, vec![], 0, config).unwrap(),
        )],
        1,
    )
    .await?;

    // Checkpoint version 1: the add from checkpoint 0 has stats_parsed with nullCount fields
    // lacking metadata. Ensure our checkpointing drops the new metadata for the stats fields and
    // doesn't see a mismatch
    Snapshot::builder_for(table_root)
        .build(&engine)?
        .checkpoint(&engine, None)?;

    Ok(())
}
