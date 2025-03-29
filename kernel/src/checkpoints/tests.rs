use std::sync::Arc;

use object_store::{memory::InMemory, path::Path, ObjectStore};
use test_utils::delta_path_for_version;
use url::Url;

use crate::{
    actions::{Add, Metadata, Protocol, Remove},
    engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    utils::test_utils::Action,
    DeltaResult, Table,
};

// Create an in-memory store and return the store and the URL for the store's _delta_log directory.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (
        Arc::new(InMemory::new()),
        Url::parse("memory:///")
            .unwrap()
            .join("_delta_log/")
            .unwrap(),
    )
}

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

#[test]
fn test_checkpoint_latest_version_by_default() -> DeltaResult<()> {
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
                ..Default::default()
            }),
        ],
        1,
    )?;

    // 3rd commit: metadata & protocol actions
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
    let mut checkpointer = table.checkpoint(&engine, None)?.build(&engine)?;
    let checkpoint_data = checkpointer.get_checkpoint_info()?;
    let mut data_iter = checkpoint_data.data;
    assert_eq!(
        checkpoint_data.path,
        Url::parse("memory:///_delta_log/00000000000000000002.checkpoint.parquet")?
    );

    // The first batch should be the metadata and protocol actions.
    let checkpoint_data = data_iter.next().unwrap()?;

    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // The second batch should be the add action as the remove action is expired.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, false]);

    // The third batch should not be included as the selection vector does not
    // contain any true values, as the add action is removed in a following commit.
    assert!(data_iter.next().is_none());

    Ok(())
}

/// Test that `checkpoint` works with a specific version parameter
#[test]
fn test_checkpoint_specific_version() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create test actions
    // 1st commit (version 0) - metadata and protocol actions
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

    // Initialize the table
    let table_root = Url::parse("memory:///")?;
    let table = Table::new(table_root);

    // Create the V1CheckpointFileIterator for version 1 specifically
    let mut checkpointer = table.checkpoint(&engine, Some(0))?.build(&engine)?;

    // Get the file data iterator
    let checkpoint_data = checkpointer.get_checkpoint_info()?;

    // Verify checkpoint file path is for version 0
    let expected_path = Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?;
    assert_eq!(checkpoint_data.path, expected_path);

    let mut data_iter = checkpoint_data.data;

    // The first batch should be the metadata and protocol actions.
    let checkpoint_data = data_iter.next().unwrap()?;
    assert_eq!(checkpoint_data.selection_vector, [true, true]);

    // No more data should exist because we only requested version 0
    assert!(data_iter.next().is_none());

    Ok(())
}
