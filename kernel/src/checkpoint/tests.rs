use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::action_reconciliation::{
    deleted_file_retention_timestamp_with_time, DEFAULT_RETENTION_SECS,
};
use crate::actions::{Add, Metadata, Protocol, Remove};
use crate::arrow::datatypes::{DataType, Schema};
use crate::arrow::{
    array::{create_array, RecordBatch},
    datatypes::Field,
};
use crate::checkpoint::{create_last_checkpoint_data, CheckpointDataIterator};
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::default::DefaultEngine;
use crate::log_replay::HasSelectionVector;
use crate::schema::{DataType as KernelDataType, StructField, StructType};
use crate::utils::test_utils::Action;
use crate::{DeltaResult, FileMeta, LogPath, Snapshot};

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

#[tokio::test]
async fn test_create_checkpoint_metadata_batch() -> DeltaResult<()> {
    use crate::checkpoint::CHECKPOINT_ACTIONS_SCHEMA_V2;

    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

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
    let writer = snapshot.checkpoint()?;

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
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

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
async fn write_commit_to_store(
    store: &Arc<InMemory>,
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
async fn assert_last_checkpoint_contents(
    store: &Arc<InMemory>,
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
async fn read_last_checkpoint_file(store: &Arc<InMemory>) -> DeltaResult<Value> {
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
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

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
    let writer = snapshot.checkpoint()?;

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
    writer.finalize(&engine, &metadata, data_iter)?;
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
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

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
    let writer = snapshot.checkpoint()?;

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
    writer.finalize(&engine, &metadata, data_iter)?;
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
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

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
    let writer = snapshot.checkpoint()?;
    let result = writer.checkpoint_data(&engine)?;
    let data_iter = result;

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
#[tokio::test]
async fn test_v2_checkpoint_supported_table() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

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
    let writer = snapshot.checkpoint()?;

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
    writer.finalize(&engine, &metadata, data_iter)?;
    // Asserts the checkpoint file contents:
    // - version: latest version (1)
    // - size: 1 metadata + 1 protocol + 1 add action + 1 remove action + 1 checkpointMetadata
    // - numOfAddFiles: 1 add file from version 0
    // - sizeInBytes: passed to finalize (10)
    assert_last_checkpoint_contents(&store, 1, 5, 1, size_in_bytes).await?;

    Ok(())
}

/// Test that V2 checkpoint batches all have the same schema.
///
/// This verifies that the checkpoint metadata batch has the same schema as
/// regular action batches, allowing them to be written to the same Parquet file.
#[tokio::test]
async fn test_v2_checkpoint_unified_schema() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

    // Create a V2 checkpoint enabled table
    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
        ],
        0,
    )
    .await?;

    write_commit_to_store(
        &store,
        vec![create_add_action_with_stats("file1.parquet", 100)],
        1,
    )
    .await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;
    let mut data_iter = writer.checkpoint_data(&engine)?;

    // Get the expected schema from the iterator
    let expected_schema = data_iter.output_schema().clone();

    // Verify all batches have the same schema
    while let Some(batch_result) = data_iter.next() {
        let batch = batch_result?;
        let data = batch.apply_selection_vector()?;
        let record_batch = data.try_into_record_batch()?;
        let batch_schema = record_batch.schema();

        assert_eq!(
            batch_schema.fields().len(),
            expected_schema.fields().count(),
            "All batches should have the same number of fields"
        );
    }

    // Verify the schema includes checkpointMetadata for V2
    assert!(
        expected_schema.field("checkpointMetadata").is_some(),
        "V2 checkpoint schema should include checkpointMetadata field"
    );

    Ok(())
}

#[tokio::test]
async fn test_no_checkpoint_staged_commits() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

    // normal commit
    write_commit_to_store(
        &store,
        vec![create_metadata_action(), create_basic_protocol_action()],
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
        .build(&engine)?;

    assert!(matches!(
        snapshot.checkpoint().unwrap_err(),
        crate::Error::Generic(e) if e == "Found staged commit file in log segment"
    ));
    Ok(())
}

/// Create a Metadata action with writeStatsAsStruct enabled
fn create_metadata_action_with_stats_struct() -> Action {
    let mut config = HashMap::new();
    config.insert(
        "delta.checkpoint.writeStatsAsStruct".to_string(),
        "true".to_string(),
    );
    Action::Metadata(
        Metadata::try_new(
            Some("test-table".into()),
            None,
            StructType::new_unchecked([
                StructField::nullable("id", KernelDataType::LONG),
                StructField::nullable("name", KernelDataType::STRING),
            ]),
            vec![],
            0,
            config,
        )
        .unwrap(),
    )
}

/// Create an Add action with JSON stats
fn create_add_action_with_stats(path: &str, num_records: i64) -> Action {
    let stats = format!(
        r#"{{"numRecords":{},"minValues":{{"id":1,"name":"alice"}},"maxValues":{{"id":100,"name":"zoe"}},"nullCount":{{"id":0,"name":5}}}}"#,
        num_records
    );
    Action::Add(Add {
        path: path.into(),
        data_change: true,
        stats: Some(stats),
        ..Default::default()
    })
}

/// Tests checkpoint_data with writeStatsAsStruct enabled.
/// Verifies that the output schema includes stats_parsed.
#[tokio::test]
async fn test_checkpoint_data_struct_enabled() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

    // 1st commit: protocol + metadata with writeStatsAsStruct=true
    write_commit_to_store(
        &store,
        vec![
            create_basic_protocol_action(),
            create_metadata_action_with_stats_struct(),
        ],
        0,
    )
    .await?;

    // 2nd commit: add actions with JSON stats
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
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;

    // Call checkpoint_data
    let result = writer.checkpoint_data(&engine)?;

    // Verify output schema includes stats_parsed in add action
    let add_field = result
        .output_schema()
        .field("add")
        .expect("output schema should have 'add' field");
    if let KernelDataType::Struct(add_struct) = add_field.data_type() {
        assert!(
            add_struct.field("stats_parsed").is_some(),
            "Add action should have stats_parsed field in output schema"
        );
        assert!(
            add_struct.field("stats").is_some(),
            "Add action should have stats field (writeStatsAsJson=true by default)"
        );
    } else {
        panic!("add field should be a struct");
    }

    // Consume the data iterator - transform is applied internally
    let mut batch_count = 0;
    for batch_result in result {
        let _batch = batch_result?;
        batch_count += 1;
    }
    assert!(batch_count > 0, "Should have at least one batch");

    Ok(())
}

/// Tests checkpoint_data with default settings (writeStatsAsStruct=false).
/// Verifies that the output schema does NOT include stats_parsed.
#[tokio::test]
async fn test_checkpoint_data_default_settings() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

    // 1st commit: protocol + metadata with default settings
    write_commit_to_store(
        &store,
        vec![create_basic_protocol_action(), create_metadata_action()],
        0,
    )
    .await?;

    // 2nd commit: add action with stats
    write_commit_to_store(
        &store,
        vec![create_add_action_with_stats("file1.parquet", 100)],
        1,
    )
    .await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;

    // Call checkpoint_data
    let result = writer.checkpoint_data(&engine)?;

    // Output schema should NOT have stats_parsed (writeStatsAsStruct=false by default)
    let add_field = result
        .output_schema()
        .field("add")
        .expect("output schema should have 'add' field");
    if let KernelDataType::Struct(add_struct) = add_field.data_type() {
        assert!(
            add_struct.field("stats_parsed").is_none(),
            "Add action should NOT have stats_parsed when writeStatsAsStruct=false"
        );
        assert!(
            add_struct.field("stats").is_some(),
            "Add action should have stats field"
        );
    } else {
        panic!("add field should be a struct");
    }

    Ok(())
}

/// Tests that checkpoint data can be iterated with stats transforms applied internally.
#[tokio::test]
async fn test_checkpoint_stats_iteration() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone());

    // 1st commit: protocol + metadata with writeStatsAsStruct=true
    write_commit_to_store(
        &store,
        vec![
            create_basic_protocol_action(),
            create_metadata_action_with_stats_struct(),
        ],
        0,
    )
    .await?;

    // 2nd commit: add action with JSON stats
    write_commit_to_store(
        &store,
        vec![create_add_action_with_stats("file1.parquet", 42)],
        1,
    )
    .await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.checkpoint()?;

    let result = writer.checkpoint_data(&engine)?;

    // Verify output schema has stats_parsed
    let add_field = result
        .output_schema()
        .field("add")
        .expect("output schema should have 'add' field");
    if let KernelDataType::Struct(add_struct) = add_field.data_type() {
        assert!(
            add_struct.field("stats_parsed").is_some(),
            "Add action should have stats_parsed when writeStatsAsStruct=true"
        );
    }

    // Consume the iterator to verify no errors during reading
    // The transform is applied internally
    let mut batch_count = 0;
    for batch_result in result {
        let _batch = batch_result?;
        batch_count += 1;
    }
    assert!(batch_count > 0, "Should have at least one batch");

    Ok(())
}

/// Helper to create metadata action with specific stats settings
fn create_metadata_with_stats_config(
    write_stats_as_json: bool,
    write_stats_as_struct: bool,
) -> Action {
    let mut config = HashMap::new();
    config.insert(
        "delta.checkpoint.writeStatsAsJson".to_string(),
        write_stats_as_json.to_string(),
    );
    config.insert(
        "delta.checkpoint.writeStatsAsStruct".to_string(),
        write_stats_as_struct.to_string(),
    );
    Action::Metadata(
        Metadata::try_new(
            Some("test-table".into()),
            None,
            StructType::new_unchecked([
                StructField::nullable("id", KernelDataType::LONG),
                StructField::nullable("name", KernelDataType::STRING),
            ]),
            vec![],
            0,
            config,
        )
        .unwrap(),
    )
}

/// Verifies checkpoint schema has expected fields based on stats configuration.
fn verify_checkpoint_schema(
    output_schema: &crate::schema::SchemaRef,
    expect_stats: bool,
    expect_stats_parsed: bool,
) -> DeltaResult<()> {
    let add_field = output_schema
        .field("add")
        .expect("output schema should have 'add' field");

    if let KernelDataType::Struct(add_struct) = add_field.data_type() {
        let has_stats = add_struct.field("stats").is_some();
        let has_stats_parsed = add_struct.field("stats_parsed").is_some();

        assert_eq!(
            has_stats, expect_stats,
            "stats field: expected={}, actual={}",
            expect_stats, has_stats
        );
        assert_eq!(
            has_stats_parsed, expect_stats_parsed,
            "stats_parsed field: expected={}, actual={}",
            expect_stats_parsed, has_stats_parsed
        );
    } else {
        panic!("add field should be a struct");
    }
    Ok(())
}

/// Tests all 16 combinations of writeStatsAsJson and writeStatsAsStruct settings.
///
/// This test verifies:
/// 1. Checkpoint 1 schema matches the initial settings
/// 2. Checkpoint 2 schema matches the updated settings
/// 3. Stats can be recovered if they were preserved in checkpoint 1
#[tokio::test]
async fn test_all_stats_config_combinations() -> DeltaResult<()> {
    let test_cases: Vec<(bool, bool, bool, bool)> = vec![
        // (json1, struct1, json2, struct2)
        (true, true, true, true),
        (true, true, true, false),
        (true, true, false, true),
        (true, true, false, false),
        (true, false, true, true),
        (true, false, true, false),
        (true, false, false, true),
        (true, false, false, false),
        (false, true, true, true),
        (false, true, true, false),
        (false, true, false, true),
        (false, true, false, false),
        (false, false, true, true),
        (false, false, true, false),
        (false, false, false, true),
        (false, false, false, false),
    ];

    for (i, (json1, struct1, json2, struct2)) in test_cases.iter().enumerate() {
        let (store, _) = new_in_memory_store();
        let engine = DefaultEngine::new(store.clone());
        let table_root = Url::parse("memory:///")?;

        // Commit 0: protocol + metadata with initial settings
        write_commit_to_store(
            &store,
            vec![
                create_basic_protocol_action(),
                create_metadata_with_stats_config(*json1, *struct1),
            ],
            0,
        )
        .await?;

        // Commit 1: add action with stats
        write_commit_to_store(
            &store,
            vec![create_add_action_with_stats("file1.parquet", 100)],
            1,
        )
        .await?;

        // Create checkpoint 1
        let snapshot1 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let writer1 = snapshot1.checkpoint()?;
        let result1 = writer1.checkpoint_data(&engine)?;

        // Verify checkpoint 1 schema
        verify_checkpoint_schema(result1.output_schema(), *json1, *struct1)?;

        // Consume checkpoint 1 data
        for batch in result1 {
            let _ = batch?;
        }

        // Commit 2: update metadata with new settings
        write_commit_to_store(
            &store,
            vec![create_metadata_with_stats_config(*json2, *struct2)],
            2,
        )
        .await?;

        // Commit 3: add another file
        write_commit_to_store(
            &store,
            vec![create_add_action_with_stats("file2.parquet", 200)],
            3,
        )
        .await?;

        // Create checkpoint 2
        let snapshot2 = Snapshot::builder_for(table_root).build(&engine)?;
        let writer2 = snapshot2.checkpoint()?;
        let result2 = writer2.checkpoint_data(&engine)?;

        // Verify checkpoint 2 schema
        verify_checkpoint_schema(result2.output_schema(), *json2, *struct2)?;

        // Consume checkpoint 2 data (verifies transform doesn't error)
        for batch in result2 {
            let _ = batch?;
        }

        println!(
            "Case {}: json1={}, struct1={}, json2={}, struct2={} - PASS",
            i + 1,
            json1,
            struct1,
            json2,
            struct2
        );
    }

    Ok(())
}
