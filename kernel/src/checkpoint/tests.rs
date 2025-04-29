use std::{sync::Arc, time::Duration};

use super::DEFAULT_RETENTION_SECS;
use crate::actions::{Add, Metadata, Protocol, Remove};
use crate::arrow::array::{ArrayRef, StructArray};
use crate::arrow::datatypes::{DataType, Schema};
use crate::checkpoint::deleted_file_retention_timestamp_with_time;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
use crate::object_store::{memory::InMemory, path::Path, ObjectStore};
use crate::utils::test_utils::Action;
use crate::DeltaResult;
use crate::Table;

use arrow_55::{
    array::{create_array, RecordBatch},
    datatypes::Field,
};

use test_utils::delta_path_for_version;
use url::Url;

#[test]
fn test_deleted_file_retention_timestamp() -> DeltaResult<()> {
    const MILLIS_PER_SECOND: i64 = 1_000;

    let reference_time_secs = 10_000;
    let reference_time = Duration::from_secs(reference_time_secs);
    let reference_time_millis = reference_time.as_millis() as i64;

    // Retention scenarios:
    // ( retention duration , expected_timestamp )
    let test_cases = [
        // None = Default retention (7 days)
        (
            None,
            reference_time_millis - (DEFAULT_RETENTION_SECS as i64 * MILLIS_PER_SECOND),
        ),
        // Zero retention
        (Some(Duration::from_secs(0)), reference_time_millis),
        // Custom retention (e.g., 2000 seconds)
        (
            Some(Duration::from_secs(2_000)),
            reference_time_millis - (2_000 * MILLIS_PER_SECOND),
        ),
    ];

    for (retention, expected_timestamp) in test_cases {
        let result = deleted_file_retention_timestamp_with_time(retention, reference_time)?;
        assert_eq!(result, expected_timestamp);
    }

    Ok(())
}

#[test]
fn test_create_checkpoint_metadata_batch() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit (version 0) - metadata and protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
        ],
        0,
    )?;

    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);
    let snapshot = table.snapshot(&engine, None)?;
    let writer = Arc::new(snapshot).checkpoint()?;

    let checkpoint_batch = writer.create_checkpoint_metadata_batch(&engine)?;

    // Check selection vector has one true value
    assert_eq!(checkpoint_batch.filtered_data.selection_vector, vec![true]);

    // Verify the underlying EngineData contains the expected CheckpointMetadata action
    let arrow_engine_data =
        ArrowEngineData::try_from_engine_data(checkpoint_batch.filtered_data.data)?;
    let record_batch = arrow_engine_data.record_batch();

    // Build the expected RecordBatch
    // Note: The schema is a struct with a single field "checkpointMetadata" of type struct
    // containing a single field "version" of type long
    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "checkpointMetadata",
        DataType::Struct(vec![Field::new("version", DataType::Int64, false)].into()),
        true,
    )]));
    let expected = RecordBatch::try_new(
        expected_schema,
        vec![Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("version", DataType::Int64, false)),
            create_array!(Int64, [0]) as ArrayRef,
        )]))],
    )
    .unwrap();

    assert_eq!(*record_batch, expected);
    assert_eq!(checkpoint_batch.actions_count, 1);
    assert_eq!(checkpoint_batch.add_actions_count, 0);

    Ok(())
}

/// TODO(#855): Merge copies and move to `test_utils`
/// Create an in-memory store and return the store and the URL for the store's _delta_log directory.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
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
fn write_commit_to_store(
    store: &Arc<InMemory>,
    actions: Vec<Action>,
    version: u64,
) -> DeltaResult<()> {
    let json_lines: Vec<String> = actions
        .into_iter()
        .map(|action| serde_json::to_string(&action).expect("action to string"))
        .collect();
    let content = json_lines.join("\n");

    let commit_path = format!("_delta_log/{}", delta_path_for_version(version, "json"));

    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async { store.put(&Path::from(commit_path), content.into()).await })?;

    Ok(())
}

/// Create a Protocol action without v2Checkpoint feature support
fn create_basic_protocol_action() -> Action {
    Action::Protocol(
        Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(Vec::<String>::new())).unwrap(),
    )
}

/// Create a Protocol action with v2Checkpoint feature support
fn create_v2_checkpoint_protocol_action() -> Action {
    Action::Protocol(
        Protocol::try_new(3, 7, Some(vec!["v2Checkpoint"]), Some(vec!["v2Checkpoint"])).unwrap(),
    )
}

/// Create a Metadata action
fn create_metadata_action() -> Action {
    Action::Metadata(Metadata {
        id: "test-table".into(),
        schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}".to_string(),
        ..Default::default()
    })
}

/// Create an Add action with the specified path
fn create_add_action(path: &str) -> Action {
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
fn create_remove_action(path: &str) -> Action {
    Action::Remove(Remove {
        path: path.into(),
        data_change: true,
        deletion_timestamp: Some(i64::MAX), // Ensure the remove action is not expired
        ..Default::default()
    })
}

/// Tests the `checkpoint()` API with:
/// - A table that does not support v2Checkpoint
/// - No version specified (latest version is used)
#[test]
fn test_v1_checkpoint_latest_version_by_default() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit: adds `fake_path_1`
    write_commit_to_store(&store, vec![create_add_action("fake_path_1")], 0)?;

    // 2nd commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            create_add_action("fake_path_2"),
            create_remove_action("fake_path_1"),
        ],
        1,
    )?;

    // 3rd commit: metadata & protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![create_metadata_action(), create_basic_protocol_action()],
        2,
    )?;

    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);
    let writer = table.checkpoint(&engine, None)?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000002.checkpoint.parquet")?
    );

    let mut data_iter = writer.checkpoint_data(&engine)?;
    // The first batch should be the metadata and protocol actions.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector, [true, true]);

    // The second batch should include both the add action and the remove action
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector, [true, true]);

    // The third batch should not be included as the selection vector does not
    // contain any true values, as the file added is removed in a following commit.
    assert!(data_iter.next().is_none());

    assert_eq!(data_iter.actions_count, 4);
    assert_eq!(data_iter.add_actions_count, 1);

    // TODO(#850): Finalize and verify _last_checkpoint
    Ok(())
}

/// Tests the `checkpoint()` API with:
/// - A table that does not support v2Checkpoint
/// - A specific version specified (version 0)
#[test]
fn test_v1_checkpoint_specific_version() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit (version 0) - metadata and protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )?;

    // 2nd commit (version 1) - add actions
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file1.parquet"),
            create_add_action("file2.parquet"),
        ],
        1,
    )?;

    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);
    // Specify version 0 for checkpoint
    let writer = table.checkpoint(&engine, Some(0))?;

    // Verify the checkpoint file path is the specified version.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?
    );

    let mut data_iter = writer.checkpoint_data(&engine)?;
    // The first batch should be the metadata and protocol actions.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector, [true, true]);

    // No more data should exist because we only requested version 0
    assert!(data_iter.next().is_none());

    assert_eq!(data_iter.actions_count, 2);
    assert_eq!(data_iter.add_actions_count, 0);

    // TODO(#850): Finalize and verify _last_checkpoint
    Ok(())
}

/// Tests the `checkpoint()` API with:
/// - A table that does supports v2Checkpoint
/// - No version specified (latest version is used)
#[test]
fn test_v2_checkpoint_supported_table() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            create_add_action("fake_path_2"),
            create_remove_action("fake_path_1"),
        ],
        0,
    )?;

    // 2nd commit: metadata & protocol actions
    // Protocol action includes the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
        ],
        1,
    )?;

    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);
    let writer = table.checkpoint(&engine, None)?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000001.checkpoint.parquet")?
    );

    let mut data_iter = writer.checkpoint_data(&engine)?;
    // The first batch should be the metadata and protocol actions.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector, [true, true]);

    // The second batch should include both the add action and the remove action
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector, [true, true]);

    // The third batch should be the CheckpointMetaData action.
    let batch = data_iter.next().unwrap()?;
    assert_eq!(batch.selection_vector, [true]);

    // No more data should exist
    assert!(data_iter.next().is_none());

    assert_eq!(data_iter.actions_count, 5);
    assert_eq!(data_iter.add_actions_count, 1);

    // TODO(#850): Finalize and verify _last_checkpoint
    Ok(())
}
