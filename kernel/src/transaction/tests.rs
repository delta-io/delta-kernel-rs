use std::collections::HashMap;

use super::*;
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::CommitInfo;
use crate::arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
};
use crate::arrow::buffer::OffsetBuffer;
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
};
use crate::arrow::record_batch::RecordBatch;
use crate::committer::{FileSystemCommitter, PublishMetadata};
use crate::engine::arrow_conversion::TryIntoArrow;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_expression::ArrowEvaluationHandler;
use crate::engine::sync::SyncEngine;
use crate::object_store::local::LocalFileSystem;
use crate::schema::MapType;
use crate::table_features::ColumnMappingMode;
use crate::transaction::create_table::create_table;
use crate::utils::test_utils::{
    load_test_table, string_array_to_engine_data, test_schema_flat, test_schema_nested,
    test_schema_with_array, test_schema_with_map,
};
use crate::EvaluationHandler;
use crate::Snapshot;
use rstest::rstest;
use std::path::PathBuf;

impl Transaction {
    /// Set clustering columns for testing purposes without needing a table
    /// with the ClusteredTable feature enabled.
    fn with_clustering_columns_for_test(mut self, columns: Vec<ColumnName>) -> Self {
        self.clustering_columns_physical = Some(columns);
        self
    }
}

/// A mock committer that always returns an IOError, used to test the retryable error path.
struct IoErrorCommitter;

impl Committer for IoErrorCommitter {
    fn commit(
        &self,
        _engine: &dyn Engine,
        _actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        _commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        Err(Error::IOError(std::io::Error::other("simulated IO error")))
    }
    fn is_catalog_committer(&self) -> bool {
        false
    }
    fn publish(&self, _engine: &dyn Engine, _publish_metadata: PublishMetadata) -> DeltaResult<()> {
        Ok(())
    }
}

/// Sets up a snapshot for a table with deletion vector support at version 1
fn setup_dv_enabled_table() -> (SyncEngine, Arc<Snapshot>) {
    let engine = SyncEngine::new();
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url)
        .at_version(1)
        .build(&engine)
        .unwrap();
    (engine, snapshot)
}

fn setup_non_dv_table() -> (SyncEngine, Arc<Snapshot>) {
    let engine = SyncEngine::new();
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    (engine, snapshot)
}

/// Creates a test deletion vector descriptor with default values (the DV might not exist on disk)
fn create_test_dv_descriptor(path_suffix: &str) -> DeletionVectorDescriptor {
    use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: format!("dv_{path_suffix}"),
        offset: Some(0),
        size_in_bytes: 100,
        cardinality: 1,
    }
}

fn create_dv_transaction(snapshot: Arc<Snapshot>, engine: &dyn Engine) -> DeltaResult<Transaction> {
    Ok(snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_operation("DELETE".to_string())
        .with_engine_info("test_engine"))
}

// TODO: create a finer-grained unit tests for transactions (issue#1091)
#[test]
fn test_add_files_schema() -> Result<(), Box<dyn std::error::Error>> {
    let engine = SyncEngine::new();
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url)
        .at_version(1)
        .build(&engine)
        .unwrap();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_engine_info("default engine");

    let schema = txn.add_files_schema();
    let expected = StructType::new_unchecked(vec![
        StructField::not_null("path", DataType::STRING),
        StructField::not_null(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
        StructField::nullable(
            "stats",
            DataType::struct_type_unchecked(vec![
                StructField::nullable("numRecords", DataType::LONG),
                StructField::nullable("nullCount", DataType::struct_type_unchecked(vec![])),
                StructField::nullable("minValues", DataType::struct_type_unchecked(vec![])),
                StructField::nullable("maxValues", DataType::struct_type_unchecked(vec![])),
                StructField::nullable("tightBounds", DataType::BOOLEAN),
            ]),
        ),
    ]);
    assert_eq!(*schema, expected.into());
    Ok(())
}

#[test]
fn test_new_deletion_vector_path() -> Result<(), Box<dyn std::error::Error>> {
    let engine = SyncEngine::new();
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url.clone())
        .at_version(1)
        .build(&engine)
        .unwrap();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_engine_info("default engine");
    let write_context = txn.get_write_context();

    // Test with empty prefix
    let dv_path1 = write_context.new_deletion_vector_path(String::from(""));
    let abs_path1 = dv_path1.absolute_path()?;
    assert!(abs_path1.as_str().contains(url.as_str()));

    // Test with non-empty prefix
    let prefix = String::from("dv_test");
    let dv_path2 = write_context.new_deletion_vector_path(prefix.clone());
    let abs_path2 = dv_path2.absolute_path()?;
    assert!(abs_path2.as_str().contains(url.as_str()));
    assert!(abs_path2.as_str().contains(&prefix));

    // Test that two paths with same prefix are different (unique UUIDs)
    let dv_path3 = write_context.new_deletion_vector_path(prefix.clone());
    let abs_path3 = dv_path3.absolute_path()?;
    assert_ne!(abs_path2, abs_path3);

    Ok(())
}

#[test]
fn test_physical_schema_excludes_partition_columns() -> Result<(), Box<dyn std::error::Error>> {
    let engine = SyncEngine::new();
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_engine_info("default engine");

    let write_context = txn.get_write_context();
    let logical_schema = write_context.logical_schema();
    let physical_schema = write_context.physical_schema();

    // Logical schema should include the partition column
    assert!(
        logical_schema.contains("letter"),
        "Logical schema should contain partition column 'letter'"
    );

    // Physical schema should exclude the partition column
    assert!(
        !physical_schema.contains("letter"),
        "Physical schema should not contain partition column 'letter' (stored in path)"
    );

    // Both should contain the non-partition columns
    assert!(
        logical_schema.contains("number"),
        "Logical schema should contain data column 'number'"
    );

    assert!(
        physical_schema.contains("number"),
        "Physical schema should contain data column 'number'"
    );

    Ok(())
}

/// Helper: loads a test table snapshot and returns both the snapshot and its write context.
fn snapshot_and_write_context(
    table_path: &str,
) -> Result<(Arc<Snapshot>, WriteContext), Box<dyn std::error::Error>> {
    let engine = SyncEngine::new();
    let path = std::fs::canonicalize(PathBuf::from(table_path)).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url).build(&engine)?;
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    Ok((snapshot, txn.get_write_context()))
}

/// Helper: evaluates the logical-to-physical transform on the given batch and returns the
/// output RecordBatch.
fn eval_logical_to_physical(
    wc: &WriteContext,
    batch: RecordBatch,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let logical_schema = wc.logical_schema();
    let physical_schema = wc.physical_schema();
    let l2p = wc.logical_to_physical();

    let handler = ArrowEvaluationHandler;
    let evaluator = handler.new_expression_evaluator(
        logical_schema.clone(),
        l2p,
        physical_schema.clone().into(),
    )?;
    let result =
        ArrowEngineData::try_from_engine_data(evaluator.evaluate(&ArrowEngineData::new(batch))?)?;
    Ok(result.record_batch().clone())
}

#[test]
fn test_materialize_partition_columns_in_write_context() -> Result<(), Box<dyn std::error::Error>> {
    // Without materializePartitionColumns, partition column should be dropped
    let (snap_without, wc_without) = snapshot_and_write_context("./tests/data/basic_partitioned/")?;
    let partition_cols = snap_without.table_configuration().partition_columns();
    assert_eq!(partition_cols.len(), 1);
    assert_eq!(partition_cols[0], "letter");
    assert!(
        !snap_without
            .table_configuration()
            .protocol()
            .has_table_feature(&TableFeature::MaterializePartitionColumns),
        "basic_partitioned should not have materializePartitionColumns feature"
    );
    let expr_str = format!("{}", wc_without.logical_to_physical());
    assert!(
        expr_str.contains("drop letter"),
        "Partition column 'letter' should be dropped. Expression: {expr_str}"
    );

    // With materializePartitionColumns, no columns should be dropped (identity transform)
    let (snap_with, wc_with) =
        snapshot_and_write_context("./tests/data/partitioned_with_materialize_feature/")?;
    let partition_cols = snap_with.table_configuration().partition_columns();
    assert_eq!(partition_cols.len(), 1);
    assert_eq!(partition_cols[0], "letter");
    assert!(
        snap_with
            .table_configuration()
            .protocol()
            .has_table_feature(&TableFeature::MaterializePartitionColumns),
        "partitioned_with_materialize_feature should have materializePartitionColumns feature"
    );
    let expr_str = format!("{}", wc_with.logical_to_physical());
    assert!(
        !expr_str.contains("drop"),
        "No columns should be dropped with materializePartitionColumns. Expression: {expr_str}"
    );

    Ok(())
}

/// Physical schema should include partition columns when materializePartitionColumns is on.
#[test]
fn test_physical_schema_includes_partition_columns_when_materialized(
) -> Result<(), Box<dyn std::error::Error>> {
    let engine = SyncEngine::new();
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/partitioned_with_materialize_feature/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let snapshot = Snapshot::builder_for(url).at_version(1).build(&engine)?;

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let write_context = txn.get_write_context();
    let physical_schema = write_context.physical_schema();

    assert!(
        physical_schema.contains("letter"),
        "Partition column 'letter' should be in physical schema when materialized"
    );
    assert!(
        physical_schema.contains("number"),
        "Non-partition column 'number' should be in physical schema"
    );
    Ok(())
}

/// Tests that update_deletion_vectors validates table protocol requirements.
/// Validates that attempting DV updates on unsupported tables returns protocol error.
#[test]
fn test_update_deletion_vectors_unsupported_table() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, snapshot) = setup_non_dv_table();
    let mut txn = create_dv_transaction(snapshot, &engine)?;

    let dv_map = HashMap::new();
    let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

    let err = result.expect_err("Should fail on table without DV support");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("Deletion vector")
            && (err_msg.contains("require") || err_msg.contains("version")),
        "Expected protocol error about DV requirements, got: {err_msg}"
    );
    Ok(())
}

/// Tests that update_deletion_vectors validates DV descriptors match scan files.
/// Validates detection of mismatch between provided DV descriptors and actual files.
#[test]
fn test_update_deletion_vectors_mismatch_count() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, snapshot) = setup_dv_enabled_table();
    let mut txn = create_dv_transaction(snapshot, &engine)?;

    let mut dv_map = HashMap::new();
    let descriptor = create_test_dv_descriptor("non_existent");
    dv_map.insert("non_existent_file.parquet".to_string(), descriptor);

    let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

    assert!(
        result.is_err(),
        "Should fail when DV descriptors don't match scan files"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
            err_msg.contains("matched") && err_msg.contains("does not match"),
            "Expected error about mismatched count (expected 1 descriptor, 0 matched files), got: {err_msg}");
    Ok(())
}

/// Tests that update_deletion_vectors handles empty DV updates correctly as a no-op.
/// This edge case occurs when a DELETE operation matches no rows.
#[test]
fn test_update_deletion_vectors_empty_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, snapshot) = setup_dv_enabled_table();
    let mut txn = create_dv_transaction(snapshot, &engine)?;

    let dv_map = HashMap::new();
    let result = txn.update_deletion_vectors(dv_map, std::iter::empty());

    assert!(
        result.is_ok(),
        "Empty DV updates should succeed as no-op, got error: {result:?}"
    );

    Ok(())
}

// ============================================================================
// validate_blind_append tests
// ============================================================================
fn add_dummy_file<S>(txn: &mut Transaction<S>) {
    let data = string_array_to_engine_data(StringArray::from(vec!["dummy"]));
    txn.add_files(data);
}

fn create_existing_table_txn(
) -> DeltaResult<(Arc<dyn Engine>, Transaction, Option<tempfile::TempDir>)> {
    let (engine, snapshot, tempdir) = load_test_table("table-without-dv-small")?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    Ok((engine, txn, tempdir))
}

#[test]
fn test_validate_blind_append_success() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    add_dummy_file(&mut txn);
    txn.validate_blind_append_semantics()?;
    Ok(())
}

#[test]
fn test_validate_blind_append_requires_adds() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    let result = txn.validate_blind_append_semantics();
    assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
    Ok(())
}

#[test]
fn test_validate_blind_append_requires_data_change() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    txn.set_data_change(false);
    add_dummy_file(&mut txn);
    let result = txn.validate_blind_append_semantics();
    assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
    Ok(())
}

#[test]
fn test_validate_blind_append_rejects_removes() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    add_dummy_file(&mut txn);
    let remove_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
        StringArray::from(vec!["remove"]),
    ));
    txn.remove_files(remove_data);
    let result = txn.validate_blind_append_semantics();
    assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
    Ok(())
}

#[test]
fn test_validate_blind_append_rejects_dv_updates() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    add_dummy_file(&mut txn);
    let dv_data = FilteredEngineData::with_all_rows_selected(string_array_to_engine_data(
        StringArray::from(vec!["dv"]),
    ));
    txn.dv_matched_files.push(dv_data);
    let result = txn.validate_blind_append_semantics();
    assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
    Ok(())
}

#[test]
fn test_validate_blind_append_rejects_create_table() -> DeltaResult<()> {
    let tempdir = tempfile::tempdir()?;
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(crate::engine::default::DefaultEngineBuilder::new(store).build());
    let mut txn = create_table(
        tempdir.path().to_str().expect("valid temp path"),
        schema,
        "test_engine",
    )
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    // CreateTableTransaction does not expose with_blind_append() (compile-time
    // prevention per #1768). Directly set the field to test the runtime check.
    txn.is_blind_append = true;
    add_dummy_file(&mut txn);
    let result = txn.validate_blind_append_semantics();
    assert!(matches!(result, Err(Error::InvalidTransactionState(_))));
    Ok(())
}

#[test]
fn test_blind_append_sets_commit_info_flag() -> Result<(), Box<dyn std::error::Error>> {
    let commit_info = CommitInfo::new(1, None, None, None, true);
    assert_eq!(commit_info.is_blind_append, Some(true));

    let commit_info_false = CommitInfo::new(1, None, None, None, false);
    assert_eq!(commit_info_false.is_blind_append, None);
    Ok(())
}

#[test]
fn test_blind_append_commit_rejects_no_adds() -> DeltaResult<()> {
    let (_engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    // No files added — commit should fail with blind append validation
    let err = txn
        .commit(_engine.as_ref())
        .expect_err("Blind append with no adds should fail");
    assert!(
        err.to_string()
            .contains("Blind append requires at least one added data file"),
        "Unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn test_blind_append_commit_success() -> DeltaResult<()> {
    let (engine, mut txn, _tempdir) = create_existing_table_txn()?;
    txn = txn.with_blind_append();
    add_dummy_file(&mut txn);
    // Blind append with add files should pass validation and proceed to commit.
    // The commit itself may fail due to schema mismatch with the dummy data,
    // but we verify validation (line 415) passes on the Ok path.
    let result = txn.commit(engine.as_ref());
    // If it fails, it should NOT be an InvalidTransactionState error
    if let Err(e) = result {
        assert!(
            !matches!(e, Error::InvalidTransactionState(_)),
            "Blind append validation should have passed, got: {e}"
        );
    }
    Ok(())
}

// Note: Additional test coverage for partial file matching (where some files in a scan
// have DV updates but others don't) is provided by the end-to-end integration test
// kernel/tests/dv.rs and kernel/tests/write.rs, which exercises
// the full deletion vector write workflow including the DvMatchVisitor logic.

#[test]
fn test_commit_io_error_returns_retryable_transaction() -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
    let mut txn = snapshot.transaction(Box::new(IoErrorCommitter), engine.as_ref())?;
    add_dummy_file(&mut txn);
    let result = txn.commit(engine.as_ref())?;
    assert!(
        matches!(result, CommitResult::RetryableTransaction(_)),
        "Expected RetryableTransaction, got: {result:?}"
    );
    if let CommitResult::RetryableTransaction(retryable) = result {
        assert!(
            retryable.error.to_string().contains("simulated IO error"),
            "Unexpected error: {}",
            retryable.error
        );
    }
    Ok(())
}

#[test]
fn test_existing_table_txn_debug() -> DeltaResult<()> {
    let (_engine, txn, _tempdir) = create_existing_table_txn()?;
    let debug_str = format!("{txn:?}");
    // Existing-table transactions should include the snapshot version number
    assert!(
        debug_str.contains("Transaction") && debug_str.contains("read_snapshot version"),
        "Debug output should contain Transaction info: {debug_str}"
    );
    // Should NOT contain "create_table"
    assert!(
        !debug_str.contains("create_table"),
        "Existing table debug should not contain create_table: {debug_str}"
    );
    Ok(())
}

// Input schemas have no CM metadata; create_table automatically assigns IDs and
// physical names when mode is Name or Id.
#[rstest]
#[case::flat_none(test_schema_flat(), ColumnMappingMode::None)]
#[case::flat_name(test_schema_flat(), ColumnMappingMode::Name)]
#[case::flat_id(test_schema_flat(), ColumnMappingMode::Id)]
#[case::nested_none(test_schema_nested(), ColumnMappingMode::None)]
#[case::nested_name(test_schema_nested(), ColumnMappingMode::Name)]
#[case::nested_id(test_schema_nested(), ColumnMappingMode::Id)]
#[case::map_none(test_schema_with_map(), ColumnMappingMode::None)]
#[case::map_name(test_schema_with_map(), ColumnMappingMode::Name)]
#[case::map_id(test_schema_with_map(), ColumnMappingMode::Id)]
#[case::array_none(test_schema_with_array(), ColumnMappingMode::None)]
#[case::array_name(test_schema_with_array(), ColumnMappingMode::Name)]
#[case::array_id(test_schema_with_array(), ColumnMappingMode::Id)]
fn test_physical_schema_column_mapping(
    #[case] schema: SchemaRef,
    #[case] mode: ColumnMappingMode,
) -> DeltaResult<()> {
    let (_engine, txn) = crate::utils::test_utils::setup_column_mapping_txn(schema, mode)?;
    let write_context = txn.get_write_context();
    crate::utils::test_utils::validate_physical_schema_column_mapping(
        write_context.logical_schema(),
        write_context.physical_schema(),
        mode,
    );
    Ok(())
}

/// Builds a RecordBatch with logical field names matching [`test_schema_nested`].
fn build_test_record_batch() -> DeltaResult<RecordBatch> {
    let arrow_schema: ArrowSchema = test_schema_nested().as_ref().try_into_arrow()?;

    let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));

    // info struct fields
    let name_arr: ArrayRef = Arc::new(StringArray::from(vec!["alice", "bob"]));
    let age_arr: ArrayRef = Arc::new(Int32Array::from(vec![30, 25]));

    // info.tags: Map<String, String>
    let keys = StringArray::from(vec!["k1", "k2"]);
    let vals = StringArray::from(vec!["v1", "v2"]);
    let entries_field = ArrowField::new(
        "key_value",
        ArrowDataType::Struct(
            vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", ArrowDataType::Utf8, true),
            ]
            .into(),
        ),
        false,
    );
    let entries = StructArray::try_new(
        vec![
            ArrowField::new("key", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Utf8, true),
        ]
        .into(),
        vec![Arc::new(keys), Arc::new(vals)],
        None,
    )?;
    let map_offsets = crate::arrow::buffer::OffsetBuffer::new(vec![0i32, 1, 2].into());
    let tags_arr: ArrayRef = Arc::new(MapArray::new(
        Arc::new(entries_field),
        map_offsets,
        entries,
        None,
        false,
    ));

    // info.scores: Array<Int>
    let score_values = Int32Array::from(vec![10, 20, 30]);
    let offsets = crate::arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3].into());
    let scores_arr: ArrayRef = Arc::new(ListArray::try_new(
        Arc::new(ArrowField::new("element", ArrowDataType::Int32, true)),
        offsets,
        Arc::new(score_values),
        None,
    )?);

    // info struct
    let info_fields = vec![
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("age", ArrowDataType::Int32, true),
        ArrowField::new("tags", tags_arr.data_type().clone(), true),
        ArrowField::new("scores", scores_arr.data_type().clone(), true),
    ];
    let info_arr: ArrayRef = Arc::new(StructArray::try_new(
        info_fields.into(),
        vec![name_arr, age_arr, tags_arr, scores_arr],
        None,
    )?);

    Ok(RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![id_arr, info_arr],
    )?)
}

/// Validates that [`WriteContext::logical_to_physical`] correctly renames fields at all nesting levels.
/// Builds a RecordBatch with logical names, evaluates the transform, and checks that the
/// output uses physical names from the physical schema — including nested struct children.
fn validate_logical_to_physical_transform(mode: ColumnMappingMode) -> DeltaResult<()> {
    let schema = test_schema_nested();
    let (_engine, txn) = crate::utils::test_utils::setup_column_mapping_txn(schema, mode)?;
    let write_context = txn.get_write_context();
    let logical_schema = write_context.logical_schema();
    let physical_schema = write_context.physical_schema();
    let logical_to_physical_expression = write_context.logical_to_physical();

    if mode != ColumnMappingMode::None {
        assert_ne!(
            logical_schema, physical_schema,
            "Physical schema should differ from logical schema when column mapping is enabled"
        );
    }

    let batch = build_test_record_batch()?;

    // Evaluate the logical_to_physical expression
    let input_schema: SchemaRef = logical_schema.clone();
    let handler = ArrowEvaluationHandler;
    let evaluator = handler.new_expression_evaluator(
        input_schema,
        logical_to_physical_expression.clone(),
        physical_schema.clone().into(),
    )?;
    let result = evaluator.evaluate(&ArrowEngineData::new(batch))?;
    let result = ArrowEngineData::try_from_engine_data(result)?;
    let result_batch = result.record_batch();

    // Verify: all field names, types, and metadata match the physical schema
    let expected_arrow_schema: ArrowSchema = physical_schema.as_ref().try_into_arrow()?;
    assert_eq!(result_batch.schema().as_ref(), &expected_arrow_schema);

    // Verify: data is preserved (id values)
    let id_col = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column should be Int64");
    assert_eq!(id_col.values(), &[1i64, 2]);

    Ok(())
}

#[rstest]
#[case::name_mode(ColumnMappingMode::Name)]
#[case::id_mode(ColumnMappingMode::Id)]
#[case::none_mode(ColumnMappingMode::None)]
fn test_logical_to_physical_transform(#[case] mode: ColumnMappingMode) -> DeltaResult<()> {
    validate_logical_to_physical_transform(mode)
}

#[rstest]
#[case::dropped("./tests/data/basic_partitioned/", 2, &[])]
#[case::kept("./tests/data/partitioned_with_materialize_feature/", 3, &["letter"])]
fn test_partition_column_in_eval_output(
    #[case] table_path: &str,
    #[case] expected_cols: usize,
    #[case] expected_partition_cols: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::arrow::array::Float64Array;
    let (_snap, wc) = snapshot_and_write_context(table_path)?;
    let batch = RecordBatch::try_new(
        Arc::new(wc.logical_schema().as_ref().try_into_arrow()?),
        vec![
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Float64Array::from(vec![1.5])),
        ],
    )?;
    let rb = eval_logical_to_physical(&wc, batch)?;
    assert_eq!(rb.num_columns(), expected_cols);
    for col in expected_partition_cols {
        assert!(rb.schema().fields().iter().any(|f| f.name() == *col));
    }
    Ok(())
}

// =========================================================================
// Stats validation tests for clustering columns
// =========================================================================

/// Per-file stats configuration for test add file helpers.
enum TestFileStats {
    /// No stats (null stats struct)
    None,
    /// Normal stats with non-null min/max
    Present,
    /// All-null column: nullCount == numRecords, null min/max
    AllNull,
}

/// Creates test add file metadata with configurable stats for the "value" column.
fn create_test_add_files(paths: Vec<&str>, stats: Vec<TestFileStats>) -> Box<dyn EngineData> {
    let path_array = StringArray::from(paths.to_vec());
    let size_array = Int64Array::from(vec![1024i64; paths.len()]);
    let mod_time_array = Int64Array::from(vec![1000000i64; paths.len()]);

    // Create stats struct with full structure for "value" column (matches test table schema)
    let value_field = Arc::new(ArrowField::new("value", ArrowDataType::Int64, true));

    let num_records: Vec<Option<i64>> = stats
        .iter()
        .map(|s| match s {
            TestFileStats::None => Option::None,
            _ => Some(100),
        })
        .collect();
    let null_count_values: Vec<Option<i64>> = stats
        .iter()
        .map(|s| match s {
            TestFileStats::None => Option::None,
            TestFileStats::Present => Some(0),
            TestFileStats::AllNull => Some(100),
        })
        .collect();
    let min_values: Vec<Option<i64>> = stats
        .iter()
        .map(|s| match s {
            TestFileStats::Present => Some(1),
            _ => Option::None,
        })
        .collect();
    let max_values: Vec<Option<i64>> = stats
        .iter()
        .map(|s| match s {
            TestFileStats::Present => Some(100),
            _ => Option::None,
        })
        .collect();

    let num_records_array = Int64Array::from(num_records);
    let null_count_array = Int64Array::from(null_count_values);
    let null_count_struct = StructArray::new(
        Fields::from(vec![value_field.clone()]),
        vec![Arc::new(null_count_array) as ArrayRef],
        None,
    );
    let min_values_array = Int64Array::from(min_values);
    let min_values_struct = StructArray::new(
        Fields::from(vec![value_field.clone()]),
        vec![Arc::new(min_values_array) as ArrayRef],
        None,
    );
    let max_values_array = Int64Array::from(max_values);
    let max_values_struct = StructArray::new(
        Fields::from(vec![value_field]),
        vec![Arc::new(max_values_array) as ArrayRef],
        None,
    );

    // Build stats struct fields
    let value_struct_type = ArrowDataType::Struct(Fields::from(vec![ArrowField::new(
        "value",
        ArrowDataType::Int64,
        true,
    )]));
    let stats_fields = Fields::from(vec![
        ArrowField::new("numRecords", ArrowDataType::Int64, true),
        ArrowField::new("nullCount", value_struct_type.clone(), true),
        ArrowField::new("minValues", value_struct_type.clone(), true),
        ArrowField::new("maxValues", value_struct_type, true),
    ]);

    // Create validity bitmap - stats struct is null when stats are absent
    let stats_validity: Vec<bool> = stats
        .iter()
        .map(|s| !matches!(s, TestFileStats::None))
        .collect();
    let stats_struct = StructArray::new(
        stats_fields.clone(),
        vec![
            Arc::new(num_records_array) as ArrayRef,
            Arc::new(null_count_struct) as ArrayRef,
            Arc::new(min_values_struct) as ArrayRef,
            Arc::new(max_values_struct) as ArrayRef,
        ],
        Some(stats_validity.into()),
    );

    // Create empty partition values map
    let entries_field = Arc::new(ArrowField::new(
        "key_value",
        ArrowDataType::Struct(
            vec![
                Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                Arc::new(ArrowField::new("value", ArrowDataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    ));
    let empty_keys = StringArray::from(Vec::<&str>::new());
    let empty_values = StringArray::from(Vec::<Option<&str>>::new());
    let empty_entries = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
            Arc::new(empty_keys) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("value", ArrowDataType::Utf8, true)),
            Arc::new(empty_values) as ArrayRef,
        ),
    ]);
    let offsets = OffsetBuffer::from_lengths(vec![0; paths.len()]);
    let partition_values = MapArray::new(entries_field, offsets, empty_entries, None, false);

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("path", ArrowDataType::Utf8, false),
        ArrowField::new(
            "partitionValues",
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "key_value",
                    ArrowDataType::Struct(
                        vec![
                            Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                            Arc::new(ArrowField::new("value", ArrowDataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
        ArrowField::new("size", ArrowDataType::Int64, false),
        ArrowField::new("modificationTime", ArrowDataType::Int64, false),
        ArrowField::new("stats", ArrowDataType::Struct(stats_fields), true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(path_array),
            Arc::new(partition_values),
            Arc::new(size_array),
            Arc::new(mod_time_array),
            Arc::new(stats_struct),
        ],
    )
    .unwrap();

    Box::new(ArrowEngineData::new(batch))
}

#[test]
fn test_stats_validation_allows_all_null_clustering_column() {
    let (engine, snapshot) = setup_non_dv_table();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)
        .unwrap()
        .with_operation("WRITE".to_string())
        .with_clustering_columns_for_test(vec![ColumnName::new(["value"])]);

    let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::AllNull]);

    let result = txn.validate_add_files_stats(&[add_files]);

    assert!(
        result.is_ok(),
        "Stats validation should pass for all-null clustering columns, got: {result:?}",
    );
}

#[test]
fn test_stats_validation_when_clustering_cols_missing_stats() {
    let (engine, snapshot) = setup_non_dv_table();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)
        .unwrap()
        .with_operation("WRITE".to_string())
        // Enable clustering columns for this test
        .with_clustering_columns_for_test(vec![ColumnName::new(["value"])]);

    // Add files WITHOUT stats
    let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::None]);

    // Directly test the validation method instead of committing
    let result = txn.validate_add_files_stats(&[add_files]);

    assert!(
        result.is_err(),
        "Expected validation to fail when stats are missing for clustering columns"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Stats validation error") || err_msg.contains("no stats"),
        "Expected stats validation error, got: {err_msg}"
    );
}

#[test]
fn test_stats_validation_when_clustering_stats_present() {
    let (engine, snapshot) = setup_non_dv_table();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)
        .unwrap()
        .with_operation("WRITE".to_string())
        // Enable clustering columns for this test
        .with_clustering_columns_for_test(vec![ColumnName::new(["value"])]);

    // Add files WITH stats
    let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::Present]);

    // Directly test the validation method
    let result = txn.validate_add_files_stats(&[add_files]);

    assert!(
        result.is_ok(),
        "Stats validation should pass when stats are present, got: {result:?}"
    );
}

#[test]
fn test_stats_validation_skipped_without_clustering() {
    let (engine, snapshot) = setup_non_dv_table();
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)
        .unwrap()
        .with_operation("WRITE".to_string());
    // No clustering columns set (default)

    // Add files WITHOUT stats
    let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::None]);

    // Directly test the validation method - should pass because no clustering
    let result = txn.validate_add_files_stats(&[add_files]);

    assert!(
        result.is_ok(),
        "Stats validation should be skipped without clustering, got: {result:?}"
    );
}
