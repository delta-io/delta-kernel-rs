use object_store::{memory::InMemory, path::Path, ObjectStore};
use std::sync::Arc;
use test_utils::delta_path_for_version;
use url::Url;

use crate::{
    actions::{Add, Metadata, Protocol, Remove},
    engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    utils::test_utils::Action,
    DeltaResult, Table,
};

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

/// Tests the `checkpoint()` API with:
/// - A table that does not support v2Checkpoint
/// - No version specified (latest version is used)
#[test]
fn test_v1_checkpoint_latest_version_by_default() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit: adds `fake_path_1`
    write_commit_to_store(
        &store,
        vec![Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })],
        0,
    )?;

    // 2nd commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            Action::Add(Add {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                deletion_timestamp: Some(i64::MAX), // Ensure the remove action is not expired
                ..Default::default()
            }),
        ],
        1,
    )?;

    // 3rd commit: metadata & protocol actions
    // Protocol action does not include the v2Checkpoint reader/writer feature.
    write_commit_to_store(
        &store,
        vec![
            Action::Metadata(Metadata {
                id: "fake_path_1".into(),
                schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}".to_string(),
                ..Default::default()
            }),
            Action::Protocol(Protocol::try_new(3, 7, Vec::<String>::new().into(), Vec::<String>::new().into())?),
        ],
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

    // Recall that the batches of actions are returned in reverse order, with the
    // most recent actions first.

    // The first batch should be the metadata and protocol actions.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The second batch should only include the add action as the remove action is expired.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The third batch should not be included as the selection vector does not
    // contain any true values, as the add action is removed in a following commit.
    assert!(data_iter.next().is_none());

    // Verify the collected metadata
    // 2 actions (metadata, protocol) + 1 add action + 1 remove action (last action is reconciled)
    assert_eq!(*writer.total_actions_counter.borrow(), 4);
    // 1 add action
    assert_eq!(*writer.total_add_actions_counter.borrow(), 1);

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
        vec![
           Action::Protocol(Protocol::try_new(3, 7, Vec::<String>::new().into(), Vec::<String>::new().into())?),
            Action::Metadata(Metadata {
                id: "test-table-v0".into(),
                schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}".to_string(),
                ..Default::default()
            }),
        ],
        0,
    )?;

    // 2nd commit (version 1) - add and remove actions
    write_commit_to_store(
        &store,
        vec![
            Action::Add(Add {
                path: "file1.parquet".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "file2.parquet".into(),
                data_change: true,
                ..Default::default()
            }),
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

    // Verify the collected metadata
    // 2 actions (metadata and protocol) + 2 add actions
    assert_eq!(*writer.total_actions_counter.borrow(), 2);
    // 2 add actions
    assert_eq!(*writer.total_add_actions_counter.borrow(), 0);

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
            Action::Add(Add {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                deletion_timestamp: Some(i64::MAX), // Ensure the remove action is not expired
                ..Default::default()
            }),
        ],
        0,
    )?;

    // 2nd commit: metadata & protocol actions
    // Protocol action includes the v2Checkpoint reader/writer feature.
    write_commit_to_store(
            &store,
            vec![
                Action::Metadata(Metadata {
                    id: "fake_path_1".into(),
                    schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}".to_string(),
                    ..Default::default()
                }),
                Action::Protocol(Protocol::try_new(3, 7, vec!["v2Checkpoint"].into(), vec!["v2Checkpoint"].into())?),
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

    // Verify the collected metadata
    // 3 actions (metadata, protocol, and checkpointMetadata) + 1 add action + 1 remove action
    assert_eq!(*writer.total_actions_counter.borrow(), 5);
    // 2 add actions
    assert_eq!(*writer.total_add_actions_counter.borrow(), 1);

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
        vec![
           Action::Protocol(Protocol::try_new(3, 7, Vec::<String>::new().into(), Vec::<String>::new().into())?),
            Action::Metadata(Metadata {
                id: "test-table-v0".into(),
                schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}".to_string(),
                ..Default::default()
            }),
        ],
        0,
    )?;
    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);
    let result = table.checkpoint(&engine, Some(999));

    // Should fail with an appropriate error
    // Returns error: "LogSegment end version 0 not the same as the specified end version 999"
    // TODO(seb): Update the error message to be tailored to the checkpoint creation
    assert!(result.is_err());

    Ok(())
}
