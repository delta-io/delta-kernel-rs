use crate::{
    actions::{Add, Metadata, Protocol, Remove},
    engine::{
        arrow_data::ArrowEngineData,
        default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    },
    utils::test_utils::Action,
    DeltaResult, EngineData, Table,
};
use arrow_53::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use object_store::{memory::InMemory, path::Path, ObjectStore};
use serde_json::{from_slice, json, Value};
use std::sync::Arc;
use test_utils::delta_path_for_version;
use url::Url;

/// TODO(seb): Merge copies and move to `test_utils`
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

/// TODO(seb): Merge copies and move to `test_utils`
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

/// Creates a metadata batch with size information for checkpoint
fn create_checkpoint_metadata_batch(size_in_bytes: i64) -> DeltaResult<impl EngineData> {
    let schema = Schema::new(vec![Field::new("sizeInBytes", DataType::Int64, false)]);
    let size_array = Int64Array::from(vec![size_in_bytes]);
    let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(size_array)])?;
    Ok(ArrowEngineData::new(record_batch))
}

/// Reads the `_last_checkpoint` file from storage
fn read_last_checkpoint_file(store: &Arc<InMemory>) -> DeltaResult<Value> {
    let path = Path::from("_delta_log/_last_checkpoint");
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let byte_data = rt.block_on(async {
        let data = store.get(&path).await?;
        data.bytes().await
    })?;
    Ok(from_slice(&byte_data)?)
}
/// Create a Protocol action without v2Checkpoint feature support
fn create_basic_protocol_action() -> Action {
    Action::Protocol(
        Protocol::try_new(
            3,
            7,
            Vec::<String>::new().into(),
            Vec::<String>::new().into(),
        )
        .unwrap(),
    )
}

/// Create a Protocol action with v2Checkpoint feature support
fn create_v2_checkpoint_protocol_action() -> Action {
    Action::Protocol(
        Protocol::try_new(
            3,
            7,
            vec!["v2Checkpoint"].into(),
            vec!["v2Checkpoint"].into(),
        )
        .unwrap(),
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

/// Helper to verify the contents of the `_last_checkpoint` file
fn assert_last_checkpoint_contents(
    store: &Arc<InMemory>,
    expected_version: u64,
    expected_size: u64,
    expected_num_add_files: u64,
    expected_size_in_bytes: i64,
) -> DeltaResult<()> {
    let last_checkpoint_data = read_last_checkpoint_file(store)?;
    let expected_data = json!({
        "version": expected_version,
        "size": expected_size,
        "parts": 1,
        "sizeInBytes": expected_size_in_bytes,
        "numOfAddFiles": expected_num_add_files,
    });
    assert_eq!(last_checkpoint_data, expected_data);
    Ok(())
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
    let mut writer = table.checkpoint(&engine, None)?;
    let checkpoint_data = writer.checkpoint_data(&engine)?;
    let mut data_iter = checkpoint_data.data;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        checkpoint_data.path,
        Url::parse("memory:///_delta_log/00000000000000000002.checkpoint.parquet")?
    );

    // The first batch should be the metadata and protocol actions.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The second batch should only include the add action as the remove action is expired.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The third batch should not be included as the selection vector does not
    // contain any true values, as the add action is removed in a following commit.
    assert!(data_iter.next().is_none());

    // Finalize and verify checkpoint metadata
    let size_in_bytes = 10;
    writer.finalize(&engine, &create_checkpoint_metadata_batch(size_in_bytes)?)?;
    assert_last_checkpoint_contents(
        &store,
        2,             // version: latest/last version in the log
        4,             // size: 1 metadata + 1 protocol + 1 add action + 1 remove action
        1,             // numOfAddFiles: from the 2nd commit (fake_path_2)
        size_in_bytes, // sizeInBytes: passed to finalize (10)
    )?;

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
    let mut writer = table.checkpoint(&engine, Some(0))?;
    let checkpoint_data = writer.checkpoint_data(&engine)?;
    let mut data_iter = checkpoint_data.data;

    // Verify the checkpoint file path is the specified version.
    assert_eq!(
        checkpoint_data.path,
        Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?
    );

    // The first batch should be the metadata and protocol actions.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // No more data should exist because we only requested version 0
    assert!(data_iter.next().is_none());

    // Finalize and verify
    let size_in_bytes = 10;
    writer.finalize(&engine, &create_checkpoint_metadata_batch(size_in_bytes)?)?;
    assert_last_checkpoint_contents(
        &store,
        0,             // version: specified version (0)
        2,             // size: 1 protocol + 1 metadata from version 0
        0,             // numOfAddFiles: no add files in version 0
        size_in_bytes, // sizeInBytes: passed to finalize (10)
    )?;

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
    let mut writer = table.checkpoint(&engine, None)?;
    let checkpoint_data = writer.checkpoint_data(&engine)?;
    let mut data_iter = checkpoint_data.data;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        checkpoint_data.path,
        Url::parse("memory:///_delta_log/00000000000000000001.checkpoint.parquet")?
    );

    // The first batch should be the metadata and protocol actions.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The second batch should be the add action as the remove action is expired.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The third batch should be the CheckpointMetaData action.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true]);

    // No more data should exist
    assert!(data_iter.next().is_none());

    // Finalize and verify
    let size_in_bytes = 10;
    writer.finalize(&engine, &create_checkpoint_metadata_batch(size_in_bytes)?)?;
    assert_last_checkpoint_contents(
        &store,
        1,             // version: latest version (1) with v2Checkpoint support
        5,             // size: 1 metadata + 1 protocol + 1 add + 1 remove + 1 checkpointMetadata
        1,             // numOfAddFiles: 1 add file from version 0
        size_in_bytes, // sizeInBytes: passed to finalize (10)
    )?;

    Ok(())
}

/// Tests the `checkpoint()` API with:
/// - a version that does not exist in the log
#[test]
fn test_checkpoint_error_handling_invalid_version() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit (version 0) - metadata and protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )?;
    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);
    let result = table.checkpoint(&engine, Some(999));

    // Should fail with an appropriate error
    // Returns error: "LogSegment end version 0 not the same as the specified end version 999"
    // TODO(seb): Returned error should be tailored to checkpoint creation
    assert!(result.is_err());

    Ok(())
}
