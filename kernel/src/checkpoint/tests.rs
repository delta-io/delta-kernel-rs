use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::action_reconciliation::{
    deleted_file_retention_timestamp_with_time, DEFAULT_RETENTION_SECS,
};
use crate::actions::{Add, Metadata, Protocol, Remove};
use crate::arrow::array::{ArrayRef, StructArray};
use crate::arrow::datatypes::{DataType, Schema};
use crate::arrow::{
    array::{create_array, RecordBatch},
    datatypes::Field,
};
use crate::checkpoint::create_last_checkpoint_data;
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::tokio::TokioMultiThreadExecutor;
use crate::engine::default::DefaultEngineBuilder;
use crate::log_replay::HasSelectionVector;
use crate::schema::{DataType as KernelDataType, StructField, StructType};
use crate::utils::test_utils::Action;
use crate::{DeltaResult, Engine, FileMeta, LogPath, Snapshot};

use object_store::local::LocalFileSystem;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use serde_json::{from_slice, json, Value};
use tempfile::tempdir;
use test_utils::delta_path_for_version;
use url::Url;

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
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

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

    let checkpoint_batch = writer.create_checkpoint_metadata_batch(&engine)?;
    assert!(checkpoint_batch.filtered_data.has_selected_rows());

    // Verify the underlying EngineData contains the expected CheckpointMetadata action
    let (underlying_data, _) = checkpoint_batch.filtered_data.into_parts();
    let arrow_engine_data = ArrowEngineData::try_from_engine_data(underlying_data)?;
    let record_batch = arrow_engine_data.record_batch();

    // Build the expected RecordBatch
    // Note: The schema is a struct with a single field "checkpointMetadata" of type struct
    // containing "version" (long) and "tags" (nullable Map<String, String>)
    use crate::arrow::array::{MapBuilder, MapFieldNames, StringBuilder};
    use crate::arrow::datatypes::Fields;

    let key_value_field = Field::new(
        "key_value",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    );
    let tags_field = Field::new(
        "tags",
        DataType::Map(Arc::new(key_value_field), false),
        true,
    );

    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "checkpointMetadata",
        DataType::Struct(
            vec![
                Field::new("version", DataType::Int64, false),
                tags_field.clone(),
            ]
            .into(),
        ),
        true,
    )]));

    // Create a null map array for tags with correct field names and non-nullable values
    let names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut map_builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
        .with_values_field(Field::new("value", DataType::Utf8, false));
    map_builder.append(false).unwrap(); // append null

    let expected = RecordBatch::try_new(
        expected_schema,
        vec![Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("version", DataType::Int64, false)),
                create_array!(Int64, [0]) as ArrayRef,
            ),
            (
                Arc::new(tags_field),
                Arc::new(map_builder.finish()) as ArrayRef,
            ),
        ]))],
    )
    .unwrap();

    assert_eq!(*record_batch, expected);
    assert_eq!(checkpoint_batch.actions_count, 1);
    assert_eq!(checkpoint_batch.add_actions_count, 0);

    use crate::parquet::arrow::ArrowWriter;
    use crate::parquet::file::reader::{FileReader, SerializedFileReader};

    // Create a new checkpoint metadata batch for writing (since the previous one was consumed)
    let checkpoint_batch_for_write = writer.create_checkpoint_metadata_batch(&engine)?;
    let (underlying_data_for_write, _) = checkpoint_batch_for_write.filtered_data.into_parts();
    let arrow_data_for_write = ArrowEngineData::try_from_engine_data(underlying_data_for_write)?;
    let record_batch_for_write = arrow_data_for_write.record_batch();

    // Write the checkpoint metadata batch to a parquet file
    let mut buffer = vec![];
    let mut parquet_writer =
        ArrowWriter::try_new(&mut buffer, record_batch_for_write.schema(), None)?;
    parquet_writer.write(record_batch_for_write)?;
    parquet_writer.close()?;

    let checkpoint_path = Path::from("_delta_log/checkpoint_metadata_test.parquet");
    store.put(&checkpoint_path, buffer.into()).await?;

    // Read the parquet file and verify the schema includes the tags field
    let checkpoint_path = Path::from("_delta_log/checkpoint_metadata_test.parquet");
    let checkpoint_data = store.get(&checkpoint_path).await?;
    let checkpoint_bytes = checkpoint_data.bytes().await?;
    let file_reader = SerializedFileReader::new(checkpoint_bytes)?;

    // Get the parquet schema to verify checkpointMetadata column exists with tags
    let parquet_schema = file_reader.metadata().file_metadata().schema();

    // Find the checkpointMetadata column in the schema
    let checkpoint_metadata_field = parquet_schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "checkpointMetadata");

    assert!(
        checkpoint_metadata_field.is_some(),
        "checkpointMetadata column should exist in the checkpoint file"
    );

    // Verify the checkpointMetadata struct has version and tags fields
    if let Some(field) = checkpoint_metadata_field {
        if let crate::parquet::schema::types::Type::GroupType { fields, .. } = field.as_ref() {
            let field_names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
            assert!(
                field_names.contains(&"version"),
                "checkpointMetadata should have version field, got: {:?}",
                field_names
            );
            assert!(
                field_names.contains(&"tags"),
                "checkpointMetadata should have tags field, got: {:?}",
                field_names
            );
        } else {
            panic!("checkpointMetadata should be a group type");
        }
    }

    // Read back the parquet file and verify the data values
    use crate::arrow::array::Array;
    use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let checkpoint_data_for_read = store.get(&checkpoint_path).await?;
    let checkpoint_bytes_for_read = checkpoint_data_for_read.bytes().await?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(checkpoint_bytes_for_read)?.build()?;

    let mut found_checkpoint_metadata = false;
    for batch_result in reader {
        let batch = batch_result?;

        // Check if this batch has a checkpointMetadata column
        if let Ok(col_idx) = batch.schema().index_of("checkpointMetadata") {
            let checkpoint_metadata_col = batch.column(col_idx);

            // The checkpointMetadata column should be a struct with version and tags
            if let Some(struct_array) = checkpoint_metadata_col
                .as_any()
                .downcast_ref::<StructArray>()
            {
                // Check if any row has a non-null checkpointMetadata
                for row_idx in 0..struct_array.len() {
                    if struct_array.is_valid(row_idx) {
                        found_checkpoint_metadata = true;

                        // Verify the version field
                        let version_col = struct_array.column_by_name("version").unwrap();
                        let version_array = version_col
                            .as_any()
                            .downcast_ref::<crate::arrow::array::Int64Array>()
                            .unwrap();
                        assert_eq!(version_array.value(row_idx), 0);

                        // Verify the tags field is null (as we're not setting any tags)
                        let tags_col = struct_array.column_by_name("tags").unwrap();
                        assert!(
                            tags_col.is_null(row_idx),
                            "tags should be null when no tags are set"
                        );
                    }
                }
            }
        }
    }

    assert!(
        found_checkpoint_metadata,
        "checkpointMetadata action should be present in the checkpoint file"
    );

    Ok(())
}

/// Tests that checkpoint metadata with non-null tags is correctly written to parquet.
/// This test creates a checkpoint metadata batch with actual tag values and verifies
/// that the tags are correctly serialized and can be read back from the parquet file.
#[tokio::test]
async fn test_create_checkpoint_metadata_batch_with_tags() -> DeltaResult<()> {
    use crate::actions::{CheckpointMetadata, CHECKPOINT_METADATA_NAME};
    use crate::arrow::array::Array;
    use crate::expressions::Scalar;
    use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::parquet::arrow::ArrowWriter;
    use crate::parquet::file::reader::{FileReader, SerializedFileReader};
    use crate::schema::ToSchema;
    use crate::EvaluationHandlerExtension;

    let (store, _) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    // Create checkpoint metadata schema
    let checkpoint_metadata_schema: Arc<StructType> =
        Arc::new(StructType::new_unchecked([StructField::nullable(
            CHECKPOINT_METADATA_NAME,
            CheckpointMetadata::to_schema(),
        )]));

    // Create tags map with actual values
    let mut tags: HashMap<String, String> = HashMap::new();
    tags.insert("delta.checkpoint.version".to_string(), "2".to_string());
    tags.insert("delta.checkpoint.format".to_string(), "parquet".to_string());

    // Convert tags to Scalar::Map
    let tags_scalar: Scalar = tags.try_into()?;

    // Create the checkpoint metadata batch with tags
    let version: i64 = 42;
    let checkpoint_metadata_batch = engine.evaluation_handler().create_one(
        checkpoint_metadata_schema.clone(),
        &[Scalar::from(version), tags_scalar],
    )?;

    // Convert to Arrow and write to parquet
    let arrow_data = ArrowEngineData::try_from_engine_data(checkpoint_metadata_batch)?;
    let record_batch = arrow_data.record_batch();

    let mut buffer = vec![];
    let mut parquet_writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
    parquet_writer.write(record_batch)?;
    parquet_writer.close()?;

    let checkpoint_path = Path::from("_delta_log/checkpoint_metadata_with_tags_test.parquet");
    store.put(&checkpoint_path, buffer.into()).await?;

    // Read the parquet file and verify the schema includes the tags field
    let checkpoint_data = store.get(&checkpoint_path).await?;
    let checkpoint_bytes = checkpoint_data.bytes().await?;
    let file_reader = SerializedFileReader::new(checkpoint_bytes.clone())?;

    // Get the parquet schema to verify checkpointMetadata column exists with tags
    let parquet_schema = file_reader.metadata().file_metadata().schema();

    // Find the checkpointMetadata column in the schema
    let checkpoint_metadata_field = parquet_schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "checkpointMetadata");

    assert!(
        checkpoint_metadata_field.is_some(),
        "checkpointMetadata column should exist in the checkpoint file"
    );

    // Verify the checkpointMetadata struct has version and tags fields
    if let Some(field) = checkpoint_metadata_field {
        if let crate::parquet::schema::types::Type::GroupType { fields, .. } = field.as_ref() {
            let field_names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
            assert!(
                field_names.contains(&"version"),
                "checkpointMetadata should have version field, got: {:?}",
                field_names
            );
            assert!(
                field_names.contains(&"tags"),
                "checkpointMetadata should have tags field, got: {:?}",
                field_names
            );
        } else {
            panic!("checkpointMetadata should be a group type");
        }
    }

    // Read back the parquet file and verify the data values including tags
    let reader = ParquetRecordBatchReaderBuilder::try_new(checkpoint_bytes)?.build()?;

    let mut found_checkpoint_metadata = false;
    for batch_result in reader {
        let batch = batch_result?;

        // Check if this batch has a checkpointMetadata column
        if let Ok(col_idx) = batch.schema().index_of("checkpointMetadata") {
            let checkpoint_metadata_col = batch.column(col_idx);

            // The checkpointMetadata column should be a struct with version and tags
            if let Some(struct_array) = checkpoint_metadata_col
                .as_any()
                .downcast_ref::<StructArray>()
            {
                // Check if any row has a non-null checkpointMetadata
                for row_idx in 0..struct_array.len() {
                    if struct_array.is_valid(row_idx) {
                        found_checkpoint_metadata = true;

                        // Verify the version field
                        let version_col = struct_array.column_by_name("version").unwrap();
                        let version_array = version_col
                            .as_any()
                            .downcast_ref::<crate::arrow::array::Int64Array>()
                            .unwrap();
                        assert_eq!(version_array.value(row_idx), 42);

                        // Verify the tags field is NOT null and contains the expected values
                        let tags_col = struct_array.column_by_name("tags").unwrap();
                        assert!(
                            !tags_col.is_null(row_idx),
                            "tags should NOT be null when tags are set"
                        );

                        // Verify the tags map contains the expected key-value pairs
                        use crate::arrow::array::MapArray;
                        let tags_map = tags_col.as_any().downcast_ref::<MapArray>().unwrap();

                        // Get the keys and values arrays
                        let keys = tags_map.keys();
                        let values = tags_map.values();

                        let keys_array = keys
                            .as_any()
                            .downcast_ref::<crate::arrow::array::StringArray>()
                            .unwrap();
                        let values_array = values
                            .as_any()
                            .downcast_ref::<crate::arrow::array::StringArray>()
                            .unwrap();

                        // Collect the key-value pairs
                        let mut read_tags: HashMap<String, String> = HashMap::new();
                        for i in 0..keys_array.len() {
                            read_tags.insert(
                                keys_array.value(i).to_string(),
                                values_array.value(i).to_string(),
                            );
                        }

                        // Verify the tags contain the expected values
                        assert_eq!(
                            read_tags.get("delta.checkpoint.version"),
                            Some(&"2".to_string()),
                            "tags should contain delta.checkpoint.version=2"
                        );
                        assert_eq!(
                            read_tags.get("delta.checkpoint.format"),
                            Some(&"parquet".to_string()),
                            "tags should contain delta.checkpoint.format=parquet"
                        );
                        assert_eq!(read_tags.len(), 2, "tags should contain exactly 2 entries");
                    }
                }
            }
        }
    }

    assert!(
        found_checkpoint_metadata,
        "checkpointMetadata action should be present in the checkpoint file"
    );

    Ok(())
}

#[test]
fn test_create_last_checkpoint_data() -> DeltaResult<()> {
    let version = 10;
    let total_actions_counter = 100;
    let add_actions_counter = 75;
    let size_in_bytes: i64 = 1024 * 1024; // 1MB
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

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
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    // 1st commit: adds `fake_path_1`
    write_commit_to_store(&store, vec![create_add_action("fake_path_1")], 0).await?;

    // 2nd commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            create_add_action("fake_path_2"),
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
    writer.finalize(&engine, &metadata, &data_iter.state())?;
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
    let engine = DefaultEngineBuilder::new(store.clone()).build();

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
            create_add_action("file1.parquet"),
            create_add_action("file2.parquet"),
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
    writer.finalize(&engine, &metadata, &data_iter.state())?;
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
    let engine = DefaultEngineBuilder::new(store.clone()).build();

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

    let size_in_bytes = 10;
    let metadata = FileMeta {
        location: Url::parse("memory:///fake_path_2")?,
        last_modified: 0,
        size: size_in_bytes,
    };

    // Attempt to finalize the checkpoint with an iterator that has not been fully consumed
    let err = writer
        .finalize(&engine, &metadata, &data_iter.state())
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
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    // 1st commit: adds `fake_path_2` & removes `fake_path_1`
    write_commit_to_store(
        &store,
        vec![
            create_add_action("fake_path_2"),
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
    writer.finalize(&engine, &metadata, &data_iter.state())?;
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
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

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
        snapshot.create_checkpoint_writer().unwrap_err(),
        crate::Error::Generic(e) if e == "Log segment is not published"
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_checkpoint() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(store.clone())
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

    snapshot.checkpoint(&engine)?;

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

    snapshot.checkpoint(&engine)?;

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
    let engine = DefaultEngineBuilder::new(store.clone())
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
    snapshot.checkpoint(&engine)?;

    // ===== Case 2: Verify domain metadata is preserved *after* checkpoint =====
    let snapshot = Snapshot::builder_for(table_url)
        .at_version(2)
        .build(&engine)?;
    let domain_value = snapshot.get_domain_metadata("foo", &engine)?;
    assert_eq!(domain_value, Some("bar2".to_string()));

    Ok(())
}

// TODO: Add test that checkpoint does not contain tombstoned domain metadata.
