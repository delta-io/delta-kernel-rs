use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{from_slice, json, Value};
use tempfile::tempdir;
use test_utils::{actions_to_string, add_commit, delta_path_for_version, TestAction};
use url::Url;

use crate::action_reconciliation::{
    deleted_file_retention_timestamp_with_time, ActionReconciliationIterator,
    ActionReconciliationIteratorState, DEFAULT_RETENTION_SECS,
};
use crate::actions::{Add, Metadata, Protocol, Remove};
use crate::arrow::array::{create_array, Array, AsArray, RecordBatch, StructArray};
use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::checkpoint::{
    create_last_checkpoint_data, CheckpointWriter, LastCheckpointHintStats,
    CHECKPOINT_ACTIONS_SCHEMA_V2,
};
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::default::executor::tokio::TokioMultiThreadExecutor;
use crate::engine::default::storage::PrefixedStore;
use crate::engine::default::DefaultEngineBuilder;
use crate::log_replay::HasSelectionVector;
use crate::object_store::local::LocalFileSystem;
use crate::object_store::path::Path;
use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
use crate::schema::{DataType as KernelDataType, StructField, StructType};
use crate::table_features::TableFeature;
use crate::transaction::create_table::create_table;
use crate::utils::test_utils::Action;
use crate::{DeltaResult, FileMeta, LogPath, Snapshot};

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
) -> DeltaResult<()> {
    let reference_time_secs = 10_000;
    let reference_time = Duration::from_secs(reference_time_secs);

    let result = deleted_file_retention_timestamp_with_time(retention, reference_time)?;
    assert_eq!(result, expected_timestamp);

    Ok(())
}

#[tokio::test]
async fn test_create_checkpoint_metadata_batch() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let engine =
        DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from(""))).build();

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

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

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

    // Verify action counts
    assert_eq!(checkpoint_batch.actions_count, 1);
    assert_eq!(checkpoint_batch.add_actions_count, 0);

    Ok(())
}

#[test]
fn test_create_last_checkpoint_data() -> DeltaResult<()> {
    let version = 10;
    let total_actions_counter = 100;
    let add_actions_counter = 75;
    let size_in_bytes: i64 = 1024 * 1024; // 1MB
    let (store, _) = test_utils::in_memory_store();
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store, Path::from(""))).build();

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
/// Writes all actions to a _delta_log json commit file in the store.
/// This function formats the provided filename into the _delta_log directory.
pub(super) async fn write_commit_to_store(
    store: &Arc<DynObjectStore>,
    actions: Vec<Action>,
    version: u64,
) -> DeltaResult<()> {
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
            Arc::new(StructType::new_unchecked([StructField::nullable(
                "value",
                KernelDataType::INTEGER,
            )])),
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
) -> DeltaResult<()> {
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
    store: &Arc<DynObjectStore>,
    expected_version: u64,
    expected_size: u64,
    expected_num_add_files: u64,
    expected_size_in_bytes: u64,
) -> DeltaResult<()> {
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
async fn read_last_checkpoint_file(store: &Arc<DynObjectStore>) -> DeltaResult<Value> {
    let path = Path::from("_delta_log/_last_checkpoint");
    let data = store.get(&path).await?;
    let byte_data = data.bytes().await?;
    Ok(from_slice(&byte_data)?)
}

/// Tests the `checkpoint()` API with:
/// - A table that does not support v2Checkpoint
/// - No version specified (latest version is used)
#[tokio::test]
async fn test_v1_checkpoint_latest_version_by_default() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let engine =
        DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from(""))).build();

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

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000002.checkpoint.parquet")?
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
        location: Url::parse("memory:///fake_path_2")?,
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
async fn test_v1_checkpoint_specific_version() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let engine =
        DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from(""))).build();

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

    let table_root = Url::parse("memory:///")?;
    // Specify version 0 for checkpoint
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(0)
        .build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    // Verify the checkpoint file path is the specified version.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?
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
        location: Url::parse("memory:///fake_path_2")?,
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
async fn test_finalize_errors_if_checkpoint_data_iterator_is_not_exhausted() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let engine =
        DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from(""))).build();

    // 1st commit (version 0) - metadata and protocol actions
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )
    .await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(0)
        .build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;
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
    assert!(err
        .to_string()
        .contains("reconciliation iterator must be fully consumed"));

    Ok(())
}

#[test]
fn test_last_checkpoint_hint_stats_with_nonzero_num_sidecars() -> DeltaResult<()> {
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
async fn test_v2_checkpoint_supported_table() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let engine =
        DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from(""))).build();

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

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000001.checkpoint.parquet")?
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
        location: Url::parse("memory:///fake_path_2")?,
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
async fn test_no_checkpoint_on_unpublished_snapshot() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let engine =
        DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from(""))).build();

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

    let table_root = Url::parse("memory:///")?;
    let staged_commit = FileMeta {
        location: Url::parse("memory:///_delta_log/_staged_commits/00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json")?,
        last_modified: 0,
        size: 100,
    };
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(vec![LogPath::try_new(staged_commit).unwrap()])
        .with_max_catalog_version(1)
        .build(&engine)?;

    assert!(matches!(
        snapshot.create_checkpoint_writer().unwrap_err(),
        crate::Error::Generic(e) if e == "Log segment is not published"
    ));
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
async fn test_snapshot_checkpoint() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from("")))
        .with_task_executor(executor)
        .build();

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

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;

    snapshot.checkpoint(&engine, None)?;

    // First checkpoint: 1 metadata + 1 protocol + 5 add + 3 remove = 10, numOfAddFiles = 5
    let checkpoint_path = Path::from("_delta_log/00000000000000000004.checkpoint.parquet");
    let checkpoint_size = store.head(&checkpoint_path).await?.size;
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
    let checkpoint_size = store.head(&checkpoint_path).await?.size;
    assert_last_checkpoint_contents(&store, 6, 13, 7, checkpoint_size).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_preserves_domain_metadata() -> DeltaResult<()> {
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
    let store = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from("")))
        .with_task_executor(executor)
        .build();

    let commit_domain_metadata = |domain: &str, value: &str| -> DeltaResult<()> {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        let result = txn
            .with_domain_metadata(domain.to_string(), value.to_string())
            .commit(&engine)?;
        assert!(result.is_committed());
        Ok(())
    };

    // ===== Commit Domain Metadata =====
    commit_domain_metadata("foo", "bar1")?;
    commit_domain_metadata("foo", "bar2")?;

    // ===== Case 1: Verify domain metadata is preserved *before* checkpoint =====
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(snapshot.version(), 2);
    let domain_value = snapshot.get_domain_metadata("foo", &engine)?;
    assert_eq!(domain_value, Some("bar2".to_string()));

    // Trigger checkpoint
    snapshot.checkpoint(&engine, None)?;

    // ===== Case 2: Verify domain metadata is preserved *after* checkpoint =====
    let snapshot = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(&engine)?;
    let domain_value = snapshot.get_domain_metadata("foo", &engine)?;
    assert_eq!(domain_value, Some("bar2".to_string()));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_skips_last_checkpoint_write_when_hint_version_is_newer() -> DeltaResult<()>
{
    let (store, _) = test_utils::in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from("")))
        .with_task_executor(executor)
        .build();

    let table_root = Url::parse("memory:///")?;
    let _ = create_table(
        table_root.as_str(),
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            KernelDataType::INTEGER,
        )])),
        "test",
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
    .map_err(|err| crate::Error::generic(err.to_string()))?;

    // Version 2
    add_commit(
        table_root.as_str(),
        store.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("file2.parquet".to_string())]),
    )
    .await
    .map_err(|err| crate::Error::generic(err.to_string()))?;

    // Checkpoint at version 2
    let snapshot_v2 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    assert_eq!(snapshot_v2.version(), 2);
    snapshot_v2.checkpoint(&engine, None)?;
    let last_checkpoint = read_last_checkpoint_file(&store).await?;
    let size_in_bytes = last_checkpoint
        .get("sizeInBytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            crate::Error::generic("missing or invalid sizeInBytes in _last_checkpoint")
        })?;
    assert_last_checkpoint_contents(&store, 2, 4, 2, size_in_bytes).await?;

    // Time-travel to version 1 and writing a checkpoint should not override _last_checkpoint
    let snapshot_v1 = Snapshot::builder_for(table_root)
        .at_version(1)
        .build(&engine)?;
    snapshot_v1.checkpoint(&engine, None)?;
    assert_last_checkpoint_contents(&store, 2, 4, 2, size_in_bytes).await
}

// TODO: Add test that checkpoint does not contain tombstoned domain metadata.

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
            StructType::new_unchecked([
                StructField::nullable("id", KernelDataType::LONG),
                StructField::nullable("name", KernelDataType::STRING),
                StructField::nullable("category", KernelDataType::STRING),
            ])
            .into(),
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
) -> DeltaResult<()> {
    verify_checkpoint_schema_with_partitions(schema, expect_stats, expect_stats_parsed, false)
}

/// Verifies checkpoint schema has expected fields based on stats and partition configuration.
fn verify_checkpoint_schema_with_partitions(
    schema: &Schema,
    expect_stats: bool,
    expect_stats_parsed: bool,
    expect_partition_values_parsed: bool,
) -> DeltaResult<()> {
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
) -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from("")))
        .with_task_executor(executor)
        .build();
    let table_root = Url::parse("memory:///")?;

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
    let writer2 = snapshot2.create_checkpoint_writer()?;
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
) -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from("")))
        .with_task_executor(executor)
        .build();
    let table_root = Url::parse("memory:///")?;

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
    let writer2 = snapshot2.create_checkpoint_writer()?;
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
async fn test_checkpoint_with_varchar_metadata_on_field() -> DeltaResult<()> {
    let (store, _) = test_utils::in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(PrefixedStore::new(store.clone(), Path::from("")))
        .with_task_executor(executor)
        .build();

    let config = HashMap::from([
        ("delta.checkpoint.writeStatsAsJson".into(), "true".into()),
        ("delta.checkpoint.writeStatsAsStruct".into(), "true".into()),
    ]);

    // Version 0: schema WITHOUT __CHAR_VARCHAR_TYPE_STRING + add with stats
    let schema_v0 = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", KernelDataType::LONG),
        StructField::nullable("name", KernelDataType::STRING),
    ]));
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
    let table_root = Url::parse("memory:///")?;
    Snapshot::builder_for(table_root.clone())
        .build(&engine)?
        .checkpoint(&engine, None)?;

    // Version 1: new metadata WITH __CHAR_VARCHAR_TYPE_STRING on the "name" field
    let schema_v1 = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", KernelDataType::LONG),
        StructField::nullable("name", KernelDataType::STRING).with_metadata([(
            "__CHAR_VARCHAR_TYPE_STRING",
            crate::schema::MetadataValue::String("varchar(255)".to_string()),
        )]),
    ]));
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
