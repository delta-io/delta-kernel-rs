use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::action_reconciliation::{
    deleted_file_retention_timestamp_with_time, DEFAULT_RETENTION_SECS,
};
use crate::actions::{Add, Metadata, Protocol, Remove};
use crate::arrow::array::{ArrayRef, StructArray};
use crate::arrow::datatypes::{DataType, Schema};
use crate::checkpoint::create_last_checkpoint_data;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
use crate::log_replay::HasSelectionVector;
use crate::schema::{DataType as KernelDataType, StructField, StructType};
use crate::utils::test_utils::Action;
use crate::{DeltaResult, FileMeta, LogPath, Snapshot};

use arrow_56::{
    array::{create_array, RecordBatch},
    datatypes::Field,
};

use object_store::{memory::InMemory, path::Path, ObjectStore};
use serde_json::{from_slice, json, Value};
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
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;

    let checkpoint_batch = writer.create_checkpoint_metadata_batch(&engine)?;
    assert!(checkpoint_batch.filtered_data.has_selected_rows());

    // Verify the underlying EngineData contains the expected CheckpointMetadata action
    let (underlying_data, _) = checkpoint_batch.filtered_data.into_parts();
    let arrow_engine_data = ArrowEngineData::try_from_engine_data(underlying_data)?;
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

#[test]
fn test_create_last_checkpoint_data() -> DeltaResult<()> {
    let version = 10;
    let total_actions_counter = 100;
    let add_actions_counter = 75;
    let size_in_bytes: i64 = 1024 * 1024; // 1MB
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

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
    Action::Metadata(
        Metadata::try_new(
            Some("test-table".into()),
            None,
            StructType::new_unchecked([StructField::nullable("value", KernelDataType::INTEGER)]),
            vec![],
            0,
            HashMap::new(),
        )
        .unwrap(),
    )
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
    expected_size_in_bytes: u64,
) -> DeltaResult<()> {
    let last_checkpoint_data = read_last_checkpoint_file(store)?;
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
fn read_last_checkpoint_file(store: &Arc<InMemory>) -> DeltaResult<Value> {
    let path = Path::from("_delta_log/_last_checkpoint");
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let byte_data = rt.block_on(async {
        let data = store.get(&path).await?;
        data.bytes().await
    })?;
    Ok(from_slice(&byte_data)?)
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
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000002.checkpoint.parquet")?
    );

    let mut data_iter = writer.checkpoint_data(&engine)?;
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
    writer.finalize(&engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: latest version (2)
    // - size: 1 metadata + 1 protocol + 1 add action + 1 remove action
    // - numOfAddFiles: 1 add file from 2nd commit (fake_path_2)
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 2, 4, 1, size_in_bytes)?;

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
    // Specify version 0 for checkpoint
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(0)
        .build(&engine)?;
    let writer = snapshot.checkpoint()?;

    // Verify the checkpoint file path is the specified version.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?
    );

    let mut data_iter = writer.checkpoint_data(&engine)?;
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
    writer.finalize(&engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: specified version (0)
    // - size: 1 metadata + 1 protocol
    // - numOfAddFiles: no add files in version 0
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 0, 2, 0, size_in_bytes)?;

    Ok(())
}

#[test]
fn test_finalize_errors_if_checkpoint_data_iterator_is_not_exhausted() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // 1st commit (version 0) - metadata and protocol actions
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(0)
        .build(&engine)?;
    let writer = snapshot.checkpoint()?;
    let data_iter = writer.checkpoint_data(&engine)?;

    /* The returned data iterator has batches that we do not consume */

    let size_in_bytes = 10;
    let metadata = FileMeta {
        location: Url::parse("memory:///fake_path_2")?,
        last_modified: 0,
        size: size_in_bytes,
    };

    // Attempt to finalize the checkpoint with an iterator that has not been fully consumed
    let err = writer
        .finalize(&engine, &metadata, data_iter)
        .expect_err("finalize should fail");
    assert!(
        err.to_string().contains("Error writing checkpoint: The checkpoint data iterator must be fully consumed and written to storage before calling finalize")
    );

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
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;

    // Verify the checkpoint file path is the latest version by default.
    assert_eq!(
        writer.checkpoint_path()?,
        Url::parse("memory:///_delta_log/00000000000000000001.checkpoint.parquet")?
    );

    let mut data_iter = writer.checkpoint_data(&engine)?;
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
    writer.finalize(&engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: latest version (1)
    // - size: 1 metadata + 1 protocol + 1 add action + 1 remove action + 1 checkpointMetadata
    // - numOfAddFiles: 1 add file from version 0
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 1, 5, 1, size_in_bytes)?;

    Ok(())
}

#[test]
fn test_no_checkpoint_staged_commits() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // normal commit
    write_commit_to_store(
        &store,
        vec![create_metadata_action(), create_basic_protocol_action()],
        0,
    )?;

    // staged commit
    let staged_commit_path = Path::from(
        "_delta_log/_staged_commits/00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
    );
    futures::executor::block_on(async {
        let add_action = Action::Add(Add::default());
        store
            .put(
                &staged_commit_path,
                serde_json::to_string(&add_action).unwrap().into(),
            )
            .await
            .unwrap()
    });

    let table_root = Url::parse("memory:///")?;
    let staged_commit = FileMeta {
        location: Url::parse("memory:///_delta_log/_staged_commits/00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json")?,
        last_modified: 0,
        size: 100,
    };
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(vec![LogPath::try_new(staged_commit).unwrap()])
        .build(&engine)?;

    assert!(matches!(
        snapshot.checkpoint().unwrap_err(),
        crate::Error::Generic(e) if e == "Found staged commit file in log segment"
    ));
    Ok(())
}
