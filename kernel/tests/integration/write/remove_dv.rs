//! Integration tests for remove-file and deletion-vector-update write paths.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::{NUM_RECORDS, TIGHT_BOUNDS};
use delta_kernel::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use delta_kernel::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use delta_kernel::arrow::compute::{concat, concat_batches};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::expressions::{
    col, lit, null_lit, ExpressionStructPatchBuilder, MapData, Scalar,
};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::scan::{scan_row_schema, StatsOptions};
use delta_kernel::schema::{schema_ref, DataType, MapType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{
    DeltaResult, Engine, Expression as Expr, KernelError, Predicate as Pred, Snapshot,
};
use itertools::Itertools;
use rstest::rstest;
use serde_json::Deserializer;
use tempfile::tempdir;
use test_utils::{
    assert_result_error_with_message, begin_transaction, copy_directory, create_add_files_metadata,
    create_default_engine, create_default_engine_mt_executor, insert_data, into_record_batch,
    load_and_begin_transaction, read_actions_from_commit, replace_array_row, setup_test_table_p37,
    setup_test_tables, test_table_setup,
};
use url::Url;

use crate::common::write_utils::{
    create_dv_table_with_files, get_scan_files, get_simple_int_schema, sequential_dv_descriptors,
    set_table_properties, write_data_and_check_result_and_stats,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppendOnlyWrite {
    Remove,
    DeletionVectorUpdate,
}

#[rstest::rstest]
#[case::no_data_change_all_selected(
    false, /* data_change */
    &[true, true, true], /* selection_vector */
    None, /* expected_error */
)]
#[case::data_change_all_selected(
    true, /* data_change */
    &[true, true, true], /* selection_vector */
    Some("Append-only tables cannot remove files"), /* expected_error */
)]
#[case::no_data_change_partially_selected(
    false, /* data_change */
    &[true, false, true], /* selection_vector */
    None, /* expected_error */
)]
#[case::data_change_partially_selected(
    true, /* data_change */
    &[true, false, true], /* selection_vector */
    Some("Append-only tables cannot remove files"), /* expected_error */
)]
#[case::data_change_none_selected(
    true, /* data_change */
    &[false, false, false], /* selection_vector */
    None, /* expected_error */
)]
#[tokio::test]
async fn append_only_enforces_data_change_for_file_actions(
    #[values(AppendOnlyWrite::Remove, AppendOnlyWrite::DeletionVectorUpdate)]
    operation: AppendOnlyWrite,
    #[values(0, 1)] batch_index: usize,
    #[case] data_change: bool,
    #[case] selection_vector: &[bool],
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let schema = schema_ref! { nullable "number": INTEGER };

    let snapshot = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([
            ("delta.appendOnly", "true"),
            ("delta.enableDeletionVectors", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
    let write_context = txn.write_state()?.unpartitioned_write_context()?;
    let arrow_schema: Arc<ArrowSchema> =
        Arc::new(write_context.physical_schema().as_ref().try_into_arrow()?);
    for value in [1, 2, 3] {
        let data = ArrowEngineData::new(RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )?);
        txn.add_files(engine.write_parquet(&data, &write_context).await?);
    }
    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

    let staged_batches = (0..2)
        .map(|index| {
            let scan_files = selected_scan_file_batch(snapshot.clone(), engine.as_ref())?;
            let (data, _) = scan_files.into_parts();
            assert_eq!(data.len(), selection_vector.len());
            let selection_vector = if index == batch_index {
                selection_vector.to_vec()
            } else {
                vec![false; data.len()]
            };
            FilteredEngineData::try_new(data, selection_vector)
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?
        .with_operation("DELETE".to_string())
        .with_data_change(data_change);
    // NOTE: The data_change=false cases do not preserve removed records in replacement AddFiles.
    // This is not a valid rearrangement, we construct such commit only for testing.
    let commit_result = match operation {
        AppendOnlyWrite::Remove => {
            for scan_files in staged_batches {
                txn.remove_files(scan_files);
            }
            txn.commit(engine.as_ref())
        }
        AppendOnlyWrite::DeletionVectorUpdate => {
            let add_actions = read_actions_from_commit(&table_url, 1, "add")?;
            let dv_map = add_actions
                .iter()
                .zip(selection_vector)
                .enumerate()
                .filter(|(_, (_, selected))| **selected)
                .map(|(index, (add, _))| {
                    let path = add["path"]
                        .as_str()
                        .expect("add path should be present")
                        .to_string();
                    let dv = DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: format!("dv-{index}.bin"),
                        offset: Some(0),
                        size_in_bytes: 1,
                        cardinality: 1,
                    };
                    (path, dv)
                })
                .collect();
            txn.update_deletion_vectors(dv_map, staged_batches.into_iter().map(Ok))?;
            txn.commit(engine.as_ref())
        }
    };

    if let Some(expected_error) = expected_error {
        assert_result_error_with_message(commit_result, expected_error);
    } else {
        commit_result?.unwrap_committed();
    }

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(
        snapshot.version(),
        if expected_error.is_some() { 1 } else { 2 }
    );
    let scan = snapshot.scan_builder().build()?;
    let mut active_files = 0;
    for scan_metadata in scan.scan_metadata(engine.as_ref())? {
        let scan_files = scan_metadata?.scan_files;
        active_files += scan_files
            .selection_vector()
            .iter()
            .filter(|selected| **selected)
            .count()
            + scan_files.data().len()
            - scan_files.selection_vector().len();
    }
    let selected_files = selection_vector
        .iter()
        .filter(|selected| **selected)
        .count();
    let expected_active_files = match operation {
        AppendOnlyWrite::Remove if expected_error.is_none() => 3 - selected_files,
        AppendOnlyWrite::Remove | AppendOnlyWrite::DeletionVectorUpdate => 3,
    };
    assert_eq!(active_files, expected_active_files);

    Ok(())
}

fn selected_scan_file_batch(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
) -> DeltaResult<FilteredEngineData> {
    for scan_files in get_scan_files(snapshot, engine)? {
        let data = scan_files.apply_selection_vector()?;
        if !data.is_empty() {
            return Ok(FilteredEngineData::with_all_rows_selected(data));
        }
    }
    Err(KernelError::generic("expected at least one scan file"))
}

#[derive(Clone, Copy)]
struct StagedRemoveFileModification {
    field: &'static str,
    value: StagedRemoveFileFieldValue,
    modified_row_index: usize,
}

#[derive(Clone, Copy)]
enum StagedRemoveFileFieldValue {
    Null,
    String(&'static str),
    Int64(i64),
}

impl StagedRemoveFileModification {
    const fn modify_value(
        field: &'static str,
        string_value: Option<&'static str>,
        modified_row_index: usize,
    ) -> Self {
        Self {
            field,
            value: match string_value {
                Some(value) => StagedRemoveFileFieldValue::String(value),
                None => StagedRemoveFileFieldValue::Null,
            },
            modified_row_index,
        }
    }

    const fn modify_size(size: i64, modified_row_index: usize) -> Self {
        Self {
            field: "size",
            value: StagedRemoveFileFieldValue::Int64(size),
            modified_row_index,
        }
    }
}

#[rstest]
#[case::missing_path(
    StagedRemoveFileModification::modify_value("path", None, 0 /* modified_row_index */),
    &[true, true, true],
    Some("missing required field 'path'"),
)]
#[case::empty_path(
    StagedRemoveFileModification::modify_value("path", Some(""), 1 /* modified_row_index */),
    &[true, true, true],
    Some("path must not be empty"),
)]
#[case::missing_path_unselected(
    StagedRemoveFileModification::modify_value("path", None, 2 /* modified_row_index */),
    &[true, true, false],
    None,
)]
#[case::short_selection_vector_missing_path(
    StagedRemoveFileModification::modify_value("path", None, 2 /* modified_row_index */),
    &[false, false],
    Some("missing required field 'path'"),
)]
#[case::missing_size(
    StagedRemoveFileModification::modify_value("size", None, 1 /* modified_row_index */),
    &[true, true, true],
    Some("missing required field 'size'"),
)]
#[case::missing_size_unselected(
    StagedRemoveFileModification::modify_value("size", None, 2 /* modified_row_index */),
    &[true, true, false],
    None,
)]
#[case::negative_size(
    StagedRemoveFileModification::modify_size(-1, 1 /* modified_row_index */),
    &[true, true, true],
    Some("size must be non-negative"),
)]
#[case::missing_modification_time(
    StagedRemoveFileModification::modify_value(
        "modificationTime",
        None,
        1 /* modified_row_index */,
    ),
    &[true, true, true],
    None,
)]
#[case::missing_stats(
    StagedRemoveFileModification::modify_value("stats", None, 2 /* modified_row_index */),
    &[true, true, true],
    None,
)]
#[case::missing_deletion_vector(
    StagedRemoveFileModification::modify_value(
        "deletionVector",
        None,
        0 /* modified_row_index */,
    ),
    &[true, true, true],
    None,
)]
#[case::missing_file_constant_values(
    StagedRemoveFileModification::modify_value(
        "fileConstantValues",
        None,
        1 /* modified_row_index */,
    ),
    &[true, true, true],
    None,
)]
#[tokio::test]
async fn commit_validates_staged_remove_fields(
    #[case] modification: StagedRemoveFileModification,
    #[case] selection_vector: &[bool],
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // === Create table ===
    let schema = get_simple_int_schema();
    let (table_url, engine, _store, _table_name) =
        setup_test_table_p37(schema, &[], None, "remove_required_field_table").await?;
    let engine = Arc::new(engine);

    // === Insert files ===
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
    let adds = create_add_files_metadata(
        txn.add_files_schema(),
        vec![
            ("file-1.parquet", 1, 1, Some(1)),
            ("file-2.parquet", 2, 2, Some(1)),
            ("file-3.parquet", 3, 3, Some(1)),
        ],
    )?;
    txn.add_files(adds);
    txn.commit(engine.as_ref())?.unwrap_committed();

    // === Modify staged remove metadata ===
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut batches = Vec::new();
    for scan_files in get_scan_files(snapshot.clone(), engine.as_ref())? {
        batches.push(into_record_batch(scan_files.apply_selection_vector()?));
    }
    let schema = batches
        .first()
        .expect("at least one scan metadata batch")
        .schema();
    let batch = concat_batches(&schema, &batches)?;
    assert_eq!(batch.num_rows(), 3);
    let path_index = batch.schema().index_of("path")?;
    let paths = batch
        .column(path_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("path is a string column");
    let mut expected_surviving_paths = paths
        .iter()
        .enumerate()
        .filter(|(row, _)| !selection_vector.get(*row).copied().unwrap_or(true))
        .map(|(_, path)| path.expect("path is present").to_owned())
        .collect::<Vec<_>>();
    expected_surviving_paths.sort();
    let corrupted = modify_staged_remove_file(&batch, modification)?;

    // === Commit and assert ===
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    txn.remove_files(FilteredEngineData::try_new(
        Box::new(ArrowEngineData::new(corrupted)),
        selection_vector.to_vec(),
    )?);
    let result = txn.commit(engine.as_ref());
    if let Some(expected_error) = expected_error {
        assert_result_error_with_message(result, expected_error);
    } else {
        let snapshot = result?.unwrap_post_commit_snapshot();
        let mut surviving_paths = Vec::new();
        for scan_files in get_scan_files(snapshot, engine.as_ref())? {
            let (data, selection_vector) = scan_files.into_parts();
            let batch = into_record_batch(data);
            let path_index = batch.schema().index_of("path")?;
            let paths = batch
                .column(path_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("path is a string column");
            for row in 0..batch.num_rows() {
                if selection_vector.get(row).copied().unwrap_or(true) {
                    surviving_paths.push(paths.value(row).to_owned());
                }
            }
        }
        surviving_paths.sort();
        assert_eq!(surviving_paths, expected_surviving_paths);
    }
    Ok(())
}

#[tokio::test]
async fn test_remove_files_adds_expected_entries() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that Remove actions generated from scan metadata contain all expected
    // fields from the Remove struct (defined in kernel/src/actions/mod.rs).
    //
    // This test uses the table-with-dv-small dataset which contains files with tags and deletion
    // vectors.
    //
    // Not populated in the dataset are (covered by row_tracking tests):
    // baseRowId (optional i64)
    // defaultRowCommitVersion (optional i64)
    use std::path::PathBuf;

    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let tmp_table_path = tmp_dir.path().join("table-with-dv-small");
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    copy_directory(&source_path, &tmp_table_path)?;

    let table_url = url::Url::from_directory_path(&tmp_table_path).unwrap();
    let engine = create_default_engine(&table_url)?;

    let snapshot = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_data_change(true);

    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;

    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    let remove_metadata = FilteredEngineData::try_new(data, selection_vector)?;

    txn.remove_files(remove_metadata);

    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the commit log directly to verify remove actions
            let commit_path = tmp_table_path.join(format!("_delta_log/{commit_version:020}.json"));
            let commit_content = std::fs::read_to_string(commit_path)?;

            let parsed_commits: Vec<_> = Deserializer::from_str(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Verify we have at least commitInfo and remove actions
            assert!(
                parsed_commits.len() >= 2,
                "Expected at least 2 actions (commitInfo + remove)"
            );

            // Extract the commitInfo timestamp to validate against deletionTimestamp
            let commit_info_action = parsed_commits
                .iter()
                .find(|action| action.get("commitInfo").is_some())
                .expect("Missing commitInfo action");
            let commit_info = &commit_info_action["commitInfo"];
            let commit_timestamp = commit_info["timestamp"]
                .as_i64()
                .expect("Missing timestamp in commitInfo");

            // Verify remove actions
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            assert!(
                !remove_actions.is_empty(),
                "Expected at least one remove action"
            );

            assert_eq!(remove_actions.len(), 1);
            let remove_action = remove_actions[0];
            let remove = &remove_action["remove"];

            // path (required)
            assert!(remove.get("path").is_some(), "Missing path field");
            let path = remove["path"].as_str().expect("path should be a string");
            assert_eq!(
                path,
                "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
            );

            // dataChange (required)
            assert_eq!(remove["dataChange"].as_bool(), Some(true));

            // deletionTimestamp (optional) - should match commit timestamp
            let deletion_timestamp = remove["deletionTimestamp"]
                .as_i64()
                .expect("Missing deletionTimestamp");
            assert_eq!(
                deletion_timestamp, commit_timestamp,
                "deletionTimestamp should match commit timestamp"
            );

            // extendedFileMetadata (optional)
            assert_eq!(remove["extendedFileMetadata"].as_bool(), Some(true));

            // partitionValues (optional)
            let partition_vals = remove["partitionValues"]
                .as_object()
                .expect("Missing partitionValues");
            assert_eq!(partition_vals.len(), 0);

            // size (optional)
            let size = remove["size"].as_i64().expect("Missing size");
            assert_eq!(size, 635);

            // stats (optional)
            let stats = remove["stats"].as_str().expect("Missing stats");
            let stats_json: serde_json::Value = serde_json::from_str(stats)?;
            assert_eq!(stats_json[NUM_RECORDS], 10);

            // tags (optional)
            let tags = remove["tags"].as_object().expect("Missing tags");
            assert_eq!(
                tags.get("INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("MIN_INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("MAX_INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("OPTIMIZE_TARGET_SIZE").and_then(|v| v.as_str()),
                Some("268435456")
            );

            // deletionVector (optional)
            let dv = remove["deletionVector"]
                .as_object()
                .expect("Missing deletionVector");
            assert_eq!(dv.get("storageType").and_then(|v| v.as_str()), Some("u"));
            assert_eq!(
                dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("vBn[lx{q8@P<9BNH/isA")
            );
            assert_eq!(dv.get("offset").and_then(|v| v.as_i64()), Some(1));
            assert_eq!(dv.get("sizeInBytes").and_then(|v| v.as_i64()), Some(36));
            assert_eq!(dv.get("cardinality").and_then(|v| v.as_i64()), Some(2));

            // Row tracking fields should be absent as the feature is was not enabled on writing
            // row_tracking tests cover having these populated.
            assert!(remove.get("baseRowId").is_none());
            assert!(remove.get("defaultRowCommitVersion").is_none());
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

/// Verifies that `extendedFileMetadata` is true exactly when `size` and `partitionValues` are
/// present; `tags` does not affect it.
///
/// `Transaction::remove_files` requires `size`, so only `partitionValues` may be missing from that
/// pair.
#[rstest::rstest]
#[case::all_present(&[], true)]
#[case::missing_partition_values(&[ExtendedMetadataField::PartitionValues], false)]
#[case::missing_tags(&[ExtendedMetadataField::Tags], true)]
#[case::only_size(&[
    ExtendedMetadataField::PartitionValues,
    ExtendedMetadataField::Tags,
], false)]
#[tokio::test]
async fn test_remove_scanned_file_sets_extended_metadata(
    #[case] missing_fields: &[ExtendedMetadataField],
    #[case] expected_extended_file_metadata: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let schema = schema_ref! { nullable "number": INTEGER };

    let snapshot = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let snapshot = insert_data(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?
    .unwrap_post_commit_snapshot();

    let scan = snapshot.clone().scan_builder().build()?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
    for scan_metadata in scan.scan_metadata(engine.as_ref())? {
        txn.remove_files(with_missing_extended_metadata_fields(
            engine.as_ref(),
            scan_metadata?.scan_files,
            missing_fields,
        )?);
    }
    let commit_result = txn.commit(engine.as_ref());
    commit_result?.unwrap_committed();
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    let remove_actions = read_actions_from_commit(&table_url, 2, "remove")?;
    assert_eq!(remove_actions.len(), 1);
    let remove = &remove_actions[0];
    assert_eq!(
        remove["extendedFileMetadata"],
        expected_extended_file_metadata
    );
    for field in ExtendedMetadataField::ALL {
        let present = remove
            .get(field.name())
            .is_some_and(|value| !value.is_null());
        assert_eq!(present, !missing_fields.contains(&field));
    }

    let scan = snapshot.scan_builder().build()?;
    let mut surviving_files = 0;
    for scan_metadata in scan.scan_metadata(engine.as_ref())? {
        surviving_files += scan_metadata?
            .scan_files
            .selection_vector()
            .iter()
            .filter(|selected| **selected)
            .count();
    }
    assert_eq!(surviving_files, 0);

    Ok(())
}

#[tokio::test]
async fn test_update_deletion_vectors_adds_expected_entries(
) -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that deletion vector updates write proper Remove and Add actions
    // to the transaction log.
    //
    // NOTE: Additional unit tests for update_deletion_vectors exist in
    // kernel/src/transaction/mod.rs
    //
    // The test validates:
    // 1. Transaction setup for DV updates
    // 2. Scanning and extracting scan files with DV data
    // 3. Creating new DV descriptors for the files
    // 4. Calling update_deletion_vectors to update the DVs
    // 5. Committing and verifying the generated actions
    //
    // Expected commit log structure:
    // - commitInfo: Contains metadata about the transaction
    // - remove: Contains OLD deletion vector data and original file metadata
    // - add: Contains NEW deletion vector data and updated file metadata
    //
    // The test ensures:
    // - Remove action has the OLD DV descriptor with all 5 fields
    // - Add action has the NEW DV descriptor with all 5 fields
    // - All file metadata is preserved (size, stats, tags, partitionValues)
    // - dataChange is properly set to true
    // - deletionTimestamp matches commit timestamp
    use std::path::PathBuf;

    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let tmp_table_path = tmp_dir.path().join("table-with-dv-small");
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    copy_directory(&source_path, &tmp_table_path)?;

    let table_url = url::Url::from_directory_path(&tmp_table_path).unwrap();
    let engine = create_default_engine(&table_url)?;

    let snapshot = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    // Create transaction with DV update mode enabled
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    // Build scan and collect all scan metadata
    let scan = snapshot.clone().scan_builder().build()?;
    let all_scan_metadata: Vec<_> = scan
        .scan_metadata(engine.as_ref())?
        .collect::<Result<Vec<_>, _>>()?;

    // Extract scan files for DV update
    let scan_files: Vec<_> = all_scan_metadata
        .into_iter()
        .map(|sm| sm.scan_files)
        .collect();

    // Create new DV descriptors for the files
    let file_path = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
    let mut dv_map = HashMap::new();

    // Create a NEW deletion vector descriptor (different from the original)
    let new_dv = DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: "cd^-aqEH.-t@S}K{vb[*k^".to_string(),
        offset: Some(10),
        size_in_bytes: 40,
        cardinality: 3,
    };
    dv_map.insert(file_path.to_string(), new_dv);

    // Call update_deletion_vectors to exercise the API
    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;

    // Commit the transaction
    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the original version 1 log to get original file metadata
            let original_log_path = tmp_table_path.join("_delta_log/00000000000000000001.json");
            let original_log_content = std::fs::read_to_string(original_log_path)?;
            let original_commits: Vec<_> = Deserializer::from_str(&original_log_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            let file_path = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";

            // Extract original file metadata from version 1
            let original_add = original_commits
                .iter()
                .find(|action| {
                    action
                        .get("add")
                        .and_then(|add| add.get("path").and_then(|p| p.as_str()))
                        == Some(file_path)
                })
                .expect("Missing original add action in version 1")
                .get("add")
                .expect("Should have add field");

            let original_size = original_add["size"]
                .as_i64()
                .expect("Original add action should have size");
            let original_partition_values = original_add["partitionValues"]
                .as_object()
                .expect("Original add action should have partitionValues");
            let original_tags = original_add.get("tags");
            let original_stats = original_add.get("stats");

            // Read the commit log directly
            let commit_path = tmp_table_path.join(format!("_delta_log/{commit_version:020}.json"));
            let commit_content = std::fs::read_to_string(commit_path)?;

            let parsed_commits: Vec<_> = Deserializer::from_str(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Should have commitInfo, remove, and add actions
            assert!(
                parsed_commits.len() >= 3,
                "Expected at least 3 actions (commitInfo + remove + add), got {}",
                parsed_commits.len()
            );

            // Extract commitInfo timestamp
            let commit_info_action = parsed_commits
                .iter()
                .find(|action| action.get("commitInfo").is_some())
                .expect("Missing commitInfo action");
            let commit_info = &commit_info_action["commitInfo"];
            let commit_timestamp = commit_info["timestamp"]
                .as_i64()
                .expect("Missing timestamp in commitInfo");

            // Verify remove action contains OLD DV information
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            assert_eq!(
                remove_actions.len(),
                1,
                "Expected exactly one remove action"
            );

            let remove_action = remove_actions[0];
            let remove = &remove_action["remove"];

            assert_eq!(
                remove["path"].as_str(),
                Some(file_path),
                "Remove path should match"
            );
            assert_eq!(remove["dataChange"].as_bool(), Some(true));
            assert_eq!(
                remove["deletionTimestamp"].as_i64(),
                Some(commit_timestamp),
                "deletionTimestamp should match commit timestamp"
            );

            // Verify OLD deletion vector in remove action
            let old_dv = remove["deletionVector"]
                .as_object()
                .expect("Remove action should have deletionVector");
            assert_eq!(
                old_dv.get("storageType").and_then(|v| v.as_str()),
                Some("u"),
                "Old DV storage type should be 'u'"
            );
            assert_eq!(
                old_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("vBn[lx{q8@P<9BNH/isA"),
                "Old DV path should match original"
            );
            assert_eq!(
                old_dv.get("offset").and_then(|v| v.as_i64()),
                Some(1),
                "Old DV offset should be 1"
            );
            assert_eq!(
                old_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                Some(36),
                "Old DV size should be 36"
            );
            assert_eq!(
                old_dv.get("cardinality").and_then(|v| v.as_i64()),
                Some(2),
                "Old DV cardinality should be 2"
            );

            // Verify file metadata is preserved in remove action
            let remove_size = remove["size"]
                .as_i64()
                .expect("Remove action should have size");
            let remove_partition_values = remove["partitionValues"]
                .as_object()
                .expect("Remove action should have partitionValues");
            let remove_tags = remove.get("tags");
            let remove_stats = remove.get("stats");

            // Verify add action contains NEW DV information
            let add_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("add").is_some())
                .collect();

            assert_eq!(add_actions.len(), 1, "Expected exactly one add action");

            let add_action = add_actions[0];
            let add = &add_action["add"];

            assert_eq!(
                add["path"].as_str(),
                Some(file_path),
                "Add path should match"
            );
            assert_eq!(add["dataChange"].as_bool(), Some(true));

            // Verify NEW deletion vector in add action
            let new_dv = add["deletionVector"]
                .as_object()
                .expect("Add action should have deletionVector");
            assert_eq!(
                new_dv.get("storageType").and_then(|v| v.as_str()),
                Some("u"),
                "New DV storage type should be 'u'"
            );
            assert_eq!(
                new_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("cd^-aqEH.-t@S}K{vb[*k^"),
                "New DV path should match updated value"
            );
            assert_eq!(
                new_dv.get("offset").and_then(|v| v.as_i64()),
                Some(10),
                "New DV offset should be 10"
            );
            assert_eq!(
                new_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                Some(40),
                "New DV size should be 40"
            );
            assert_eq!(
                new_dv.get("cardinality").and_then(|v| v.as_i64()),
                Some(3),
                "New DV cardinality should be 3"
            );

            // Verify file metadata is preserved in add action
            let add_size = add["size"].as_i64().expect("Add action should have size");
            let add_partition_values = add["partitionValues"]
                .as_object()
                .expect("Add action should have partitionValues");
            let add_tags = add.get("tags");
            let add_stats = add.get("stats");

            // Ensure metadata is consistent between remove and add actions
            assert_eq!(
                remove_size, add_size,
                "File size should be preserved between remove and add"
            );
            assert_eq!(
                remove_partition_values, add_partition_values,
                "Partition values should be preserved between remove and add"
            );
            assert_eq!(
                remove_tags, add_tags,
                "Tags should be preserved between remove and add"
            );
            assert_eq!(
                remove_stats, add_stats,
                "Stats should be preserved between remove and add"
            );

            // Ensure metadata matches the original file metadata from version 1
            assert_eq!(
                remove_size, original_size,
                "Remove action size should match original file size"
            );
            assert_eq!(
                add_size, original_size,
                "Add action size should match original file size"
            );
            assert_eq!(
                remove_partition_values, original_partition_values,
                "Remove action partition values should match original"
            );
            assert_eq!(
                add_partition_values, original_partition_values,
                "Add action partition values should match original"
            );
            assert_eq!(
                remove_tags, original_tags,
                "Remove action tags should match original"
            );
            assert_eq!(
                add_tags, original_tags,
                "Add action tags should match original"
            );
            assert_eq!(
                remove_stats, original_stats,
                "Remove action stats should match original"
            );
            assert_eq!(
                add_stats, original_stats,
                "Add action stats should match original"
            );
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[derive(Clone)]
struct ScanFileModification {
    field_name: &'static str,
    value: ArrayRef,
    row_id: usize,
}

#[rstest::rstest]
#[case::missing_path(
    ScanFileModification {
        field_name: "path",
        value: new_null_array(&ArrowDataType::Utf8, 1),
        row_id: 0,
    },
    "Number of matched DV files does not match number of new DV descriptors"
)]
#[case::empty_path(
    ScanFileModification {
        field_name: "path",
        value: string_array(""),
        row_id: 0,
    },
    "path must not be empty"
)]
#[case::missing_partition_values(
    ScanFileModification {
        field_name: "partitionValues",
        value: null_partition_values(),
        row_id: 1,
    },
    "missing required field 'partitionValues'"
)]
#[case::extra_partition_key(
    ScanFileModification {
        field_name: "partitionValues",
        value: string_map_array(&[("stray", Some("value"))]),
        row_id: 2,
    },
    "partitionValues keys"
)]
#[case::duplicate_partition_key(
    ScanFileModification {
        field_name: "partitionValues",
        value: string_map_array(&[
            ("stray", Some("first")),
            ("stray", Some("second")),
        ]),
        row_id: 0,
    },
    "duplicate partition column names"
)]
#[case::missing_size(
    ScanFileModification {
        field_name: "size",
        value: new_null_array(&ArrowDataType::Int64, 1),
        row_id: 1,
    },
    "missing required field 'size'"
)]
#[case::negative_size(
    ScanFileModification {
        field_name: "size",
        value: int64_array(-1),
        row_id: 2,
    },
    "size must be non-negative"
)]
#[case::missing_modification_time(
    ScanFileModification {
        field_name: "modificationTime",
        value: new_null_array(&ArrowDataType::Int64, 1),
        row_id: 0,
    },
    "missing required field 'modificationTime'"
)]
#[tokio::test]
async fn test_update_deletion_vectors_rejects_corrupted_scan_files(
    #[case] modification: ScanFileModification,
    #[case] expected_error: &str,
    #[values(0, 1, 2)] invalid_batch_index: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    const BATCH_COUNT: usize = 3;

    let schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "part": STRING,
    };
    let (_store, engine, table_url, file_paths) = create_dv_table_with_files(
        "test_table",
        schema,
        &[("part", Some("value"))],
        &["file0.parquet", "file1.parquet", "file2.parquet"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let scan_file = get_scan_files(snapshot.clone(), engine.as_ref())?
        .into_iter()
        .next()
        .expect("table should contain one scan-file batch");
    let scan_files =
        make_scan_file_batches(scan_file, &modification, invalid_batch_index, BATCH_COUNT);
    let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
    let mut descriptors = sequential_dv_descriptors(&file_paths);
    if modification.field_name == "path" {
        if modification.value.is_null(0) {
            assert_result_error_with_message(
                txn.update_deletion_vectors(descriptors, scan_files.into_iter().map(Ok)),
                expected_error,
            );
            return Ok(());
        }
        let path = modification.value.as_string::<i32>().value(0);
        let descriptor = descriptors
            .remove(&file_paths[0])
            .expect("descriptor for the original path modified by this test");
        descriptors.insert(path.to_string(), descriptor);
    }
    txn.update_deletion_vectors(descriptors, scan_files.into_iter().map(Ok))?;

    assert_result_error_with_message(txn.commit(engine.as_ref()), expected_error);
    Ok(())
}

fn make_scan_file_batches(
    scan_file: FilteredEngineData,
    modification: &ScanFileModification,
    invalid_batch_index: usize,
    batch_count: usize,
) -> Vec<FilteredEngineData> {
    let (data, selection_vector) = scan_file.into_parts();
    let batch = into_record_batch(data);
    (0..batch_count)
        .map(|batch_index| {
            let selection_vector = if batch_index == invalid_batch_index {
                selection_vector.clone()
            } else {
                vec![false; batch.num_rows()]
            };
            let scan_file = FilteredEngineData::try_new(
                Box::new(ArrowEngineData::new(batch.clone())),
                selection_vector,
            )
            .expect("selection vector length should match scan-file row count");
            if batch_index == invalid_batch_index {
                modify_scan_file(scan_file, modification)
            } else {
                scan_file
            }
        })
        .collect()
}

fn modify_scan_file(
    scan_file: FilteredEngineData,
    modification: &ScanFileModification,
) -> FilteredEngineData {
    let (data, selection_vector) = scan_file.into_parts();
    let batch = into_record_batch(data);
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    let row_index = (0..batch.num_rows())
        .filter(|&row_index| selection_vector.get(row_index).copied().unwrap_or(true))
        .nth(modification.row_id)
        .expect("modified selected row must exist in scan-file batch");

    if modification.field_name == "partitionValues" {
        let constants_index = schema
            .index_of("fileConstantValues")
            .expect("fileConstantValues field in scan data");
        let constants = columns[constants_index].as_struct();
        let partition_values_index = constants
            .fields()
            .iter()
            .position(|field| field.name() == "partitionValues")
            .expect("partitionValues field in fileConstantValues");
        let partition_values = constants.column(partition_values_index);
        let mut constant_columns = constants.columns().to_vec();
        constant_columns[partition_values_index] =
            replace_array_row(partition_values, modification.value.clone(), row_index);
        columns[constants_index] = Arc::new(StructArray::new(
            constants.fields().clone(),
            constant_columns,
            constants.nulls().cloned(),
        ));
    } else {
        let field_index = schema
            .index_of(modification.field_name)
            .expect("modified field in scan data");
        columns[field_index] =
            replace_array_row(&columns[field_index], modification.value.clone(), row_index);
    }

    let batch = RecordBatch::try_new(schema, columns)
        .expect("modified scan-file schema and columns should form a valid batch");
    FilteredEngineData::try_new(Box::new(ArrowEngineData::new(batch)), selection_vector)
        .expect("selection vector length should match modified scan-file row count")
}

fn string_array(value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}

fn int64_array(value: i64) -> ArrayRef {
    Arc::new(Int64Array::from(vec![value]))
}

fn null_partition_values() -> ArrayRef {
    let partition_values = string_map_array(&[]);
    new_null_array(partition_values.data_type(), 1)
}

fn string_map_array(values: &[(&str, Option<&str>)]) -> ArrayRef {
    let mut builder = MapBuilder::new(
        Some(MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(ArrowField::new("key", ArrowDataType::Utf8, false))
    .with_values_field(ArrowField::new("value", ArrowDataType::Utf8, true));
    for (key, value) in values {
        builder.keys().append_value(key);
        match value {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder
        .append(true)
        .expect("partition-values row should be valid");
    Arc::new(builder.finish())
}

#[rstest::rstest]
#[case::unpartitioned(&[])]
#[case::partitioned(&[("value", Some("partition"))])]
#[tokio::test]
async fn test_update_deletion_vectors_multiple_files(
    #[case] partition_values: &[(&str, Option<&str>)],
) -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that update_deletion_vectors can update multiple files
    // in a single call, creating proper Remove and Add actions for each file.
    let _ = tracing_subscriber::fmt::try_init();

    let schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "value": STRING,
    };

    // Setup: Create table with 3 files
    let file_names = &["file0.parquet", "file1.parquet", "file2.parquet"];
    let (store, engine, table_url, file_paths) =
        create_dv_table_with_files("test_table", schema, partition_values, file_names).await?;

    // Create DV update transaction
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    let mut scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;

    // Update deletion vectors for all 3 files in a single call
    let dv_map = sequential_dv_descriptors(&file_paths);

    txn.update_deletion_vectors(dv_map, scan_files.drain(..).map(Ok))?;

    // Commit the transaction
    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the commit log directly from object store
            let final_commit_path =
                table_url.join(&format!("_delta_log/{commit_version:020}.json"))?;
            let commit_content = store
                .get(&Path::from_url_path(final_commit_path.path())?)
                .await?
                .bytes()
                .await?;

            let parsed_commits: Vec<_> = Deserializer::from_slice(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Extract all remove and add actions
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            let add_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("add").is_some())
                .collect();

            // Should have 3 remove and 3 add actions
            assert_eq!(
                remove_actions.len(),
                3,
                "Expected 3 remove actions for 3 files"
            );
            assert_eq!(add_actions.len(), 3, "Expected 3 add actions for 3 files");

            // Verify each file has a DV in both remove and add
            for (idx, file_path) in file_paths.iter().enumerate() {
                // Find the remove action for this file
                let remove_action = remove_actions
                    .iter()
                    .find(|action| action["remove"]["path"].as_str() == Some(file_path.as_str()))
                    .unwrap_or_else(|| panic!("Should find remove action for {file_path}"));

                // Find the add action for this file
                let add_action = add_actions
                    .iter()
                    .find(|action| action["add"]["path"].as_str() == Some(file_path.as_str()))
                    .unwrap_or_else(|| panic!("Should find add action for {file_path}"));

                // Verify remove action does NOT have a DV (since these were newly written files)
                assert!(
                    remove_action["remove"]["deletionVector"].is_null(),
                    "Remove action for newly written file should not have a DV"
                );

                // Verify add action has the NEW DV
                let add_dv = add_action["add"]["deletionVector"]
                    .as_object()
                    .expect("Add action should have deletionVector");

                let expected_path = format!("dv_file_{idx}.bin");
                assert_eq!(
                    add_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                    Some(expected_path.as_str()),
                    "DV path should match for file {file_path}"
                );
                assert_eq!(
                    add_dv.get("offset").and_then(|v| v.as_i64()),
                    Some(idx as i64 * 10),
                    "DV offset should match for file {file_path}"
                );
                assert_eq!(
                    add_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                    Some(40 + idx as i64),
                    "DV size should match for file {file_path}"
                );
                assert_eq!(
                    add_dv.get("cardinality").and_then(|v| v.as_i64()),
                    Some(idx as i64 + 1),
                    "DV cardinality should match for file {file_path}"
                );
            }
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[rstest::rstest]
#[case::target_in_explicit_prefix(&[true, true], &[0], false)]
#[case::target_in_implicit_tail(&[true, true], &[3], false)]
#[case::multiple_targets(&[true, true], &[0, 3], false)]
#[case::empty_selection_vector(&[], &[3], false)]
#[case::unselected_rows_are_ignored(&[true, false, true, false], &[2], false)]
#[case::unselected_target_is_rejected(&[true, false, true, false], &[2, 3], true)]
#[case::no_updates_short_selection_vector(&[true, true], &[], false)]
#[case::no_updates_empty_selection_vector(&[], &[], false)]
#[tokio::test]
async fn test_update_deletion_vectors_respects_selection_vector(
    #[case] selection_vector: &[bool],
    #[case] target_indexes: &[usize],
    #[case] expect_mismatch: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "value": STRING,
    };

    let file_names = &[
        "file0.parquet",
        "file1.parquet",
        "file2.parquet",
        "file3.parquet",
    ];
    let (store, engine, table_url, file_paths) =
        create_dv_table_with_files("test_table", schema, &[], file_names).await?;

    // Attach DV to all files first, if later DV updates incorrectly remove the existing DV,
    // the test will fail.
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut setup_txn = begin_transaction(snapshot.clone(), engine.as_ref())?;
    let initial_dvs = sequential_dv_descriptors(&file_paths);
    let initial_dv_ids: HashMap<_, _> = initial_dvs
        .iter()
        .map(|(path, dv)| (path.clone(), dv.unique_id()))
        .collect();
    setup_txn.update_deletion_vectors(
        initial_dvs,
        get_scan_files(snapshot, engine.as_ref())?
            .into_iter()
            .map(Ok),
    )?;
    setup_txn.commit(engine.as_ref())?.unwrap_committed();

    let targeted: Vec<String> = target_indexes
        .iter()
        .map(|&index| file_paths[index].clone())
        .collect();
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    let dv_map: HashMap<String, DeletionVectorDescriptor> = targeted
        .iter()
        .map(|path| {
            (
                path.clone(),
                DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: format!("dv_{path}.bin"),
                    offset: Some(1),
                    size_in_bytes: 40,
                    cardinality: 1,
                },
            )
        })
        .collect();
    let updated_dv_ids: HashMap<_, _> = dv_map
        .iter()
        .map(|(path, dv)| (path.clone(), dv.unique_id()))
        .collect();

    let batches = get_scan_files(snapshot, engine.as_ref())?
        .into_iter()
        .map(|scan_files| scan_files.apply_selection_vector().map(into_record_batch))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = concat_batches(&batches[0].schema(), &batches)?;
    let scan_files = FilteredEngineData::try_new(
        Box::new(ArrowEngineData::new(batch)),
        selection_vector.to_vec(),
    )?;

    let update_result = txn.update_deletion_vectors(dv_map, std::iter::once(Ok(scan_files)));
    if expect_mismatch {
        assert_result_error_with_message(
            update_result,
            "Number of matched DV files does not match number of new DV descriptors",
        );
    } else {
        update_result?;
    }
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();
    let version = committed.commit_version();

    // Read the commit directly from the (in-memory) store.
    let commit_path = table_url.join(&format!("_delta_log/{version:020}.json"))?;
    let commit_content = store
        .get(&Path::from_url_path(commit_path.path())?)
        .await?
        .bytes()
        .await?;
    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_content)
        .into_iter()
        .try_collect()?;
    let adds: Vec<&serde_json::Value> = actions.iter().filter_map(|a| a.get("add")).collect();
    let removes: Vec<&serde_json::Value> = actions.iter().filter_map(|a| a.get("remove")).collect();

    let expected_count = if expect_mismatch {
        0
    } else {
        target_indexes.len()
    };
    assert_eq!(adds.len(), expected_count, "unexpected re-added files");
    assert_eq!(removes.len(), expected_count, "unexpected removed files");

    let mut added_paths: Vec<&str> = adds.iter().map(|a| a["path"].as_str().unwrap()).collect();
    added_paths.sort();
    let mut removed_paths: Vec<&str> = removes
        .iter()
        .map(|a| a["path"].as_str().unwrap())
        .collect();
    removed_paths.sort();
    let mut expected: Vec<&str> = if expect_mismatch {
        Vec::new()
    } else {
        targeted.iter().map(String::as_str).collect()
    };
    expected.sort();
    assert_eq!(
        added_paths, expected,
        "re-added paths must be exactly the targeted files"
    );
    assert_eq!(
        removed_paths, expected,
        "removed paths must be exactly the targeted files"
    );

    let dv_id = |action: &serde_json::Value| {
        let dv = &action["deletionVector"];
        format!(
            "{}{}@{}",
            dv["storageType"].as_str().unwrap(),
            dv["pathOrInlineDv"].as_str().unwrap(),
            dv["offset"].as_i64().unwrap()
        )
    };
    for add in &adds {
        let path = add["path"].as_str().unwrap();
        assert_eq!(dv_id(add), updated_dv_ids[path]);
    }
    for remove in &removes {
        let path = remove["path"].as_str().unwrap();
        assert_eq!(dv_id(remove), initial_dv_ids[path]);
    }

    // Each new add carries its DV and widened tightBounds, with numRecords preserved.
    for add in &adds {
        assert!(
            add["deletionVector"].is_object(),
            "new add must carry a deletion vector"
        );
        let stats: serde_json::Value = serde_json::from_str(add["stats"].as_str().unwrap())?;
        assert_eq!(
            stats[TIGHT_BOUNDS].as_bool(),
            Some(false),
            "DV-updated add must widen tightBounds"
        );
        assert_eq!(
            stats[NUM_RECORDS].as_i64(),
            Some(3),
            "numRecords must be preserved"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_remove_files_verify_files_excluded_from_scan(
) -> Result<(), Box<dyn std::error::Error>> {
    // Adds and then removes files and then verifies they don't appear in the scan.

    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        // First, add some files to the table
        let engine = Arc::new(engine);
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;

        // Get initial file count
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot.clone().scan_builder().build()?;
        let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (_, selection_vector) = scan_metadata.scan_files.into_parts();
        let initial_file_count = selection_vector.iter().filter(|&x| *x).count();

        assert!(initial_file_count > 0);

        // Now create a transaction to remove files
        let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?;

        // Create a new scan to get file metadata for removal
        let scan2 = snapshot.scan_builder().build()?;
        let scan_metadata2 = scan2.scan_metadata(engine.as_ref())?.next().unwrap()?;

        // Create FilteredEngineData for removal (select all rows for removal)
        let file_remove_count = (scan_metadata2.scan_files.data().len()
            - scan_metadata2.scan_files.selection_vector().len())
            + scan_metadata2
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&x| *x)
                .count();
        assert!(file_remove_count > 0);

        // Add remove files to transaction
        txn.remove_files(scan_metadata2.scan_files);

        // Commit the transaction
        let result = txn.commit(engine.as_ref());

        match result? {
            CommitResult::CommittedTransaction(committed) => {
                assert_eq!(committed.commit_version(), 2);

                let new_snapshot = Snapshot::builder_for(table_url.clone())
                    .at_version(2)
                    .build(engine.as_ref())?;

                let new_scan = new_snapshot.scan_builder().build()?;
                let mut new_file_count = 0;
                for new_metadata in new_scan.scan_metadata(engine.as_ref())? {
                    new_file_count += new_metadata?.scan_files.data().len();
                }

                // All files were removed, so new_file_count should be zero
                assert_eq!(new_file_count, 0);
            }
            _ => panic!("Transaction did not succeeed."),
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExtendedMetadataField {
    Size,
    PartitionValues,
    Tags,
}

impl ExtendedMetadataField {
    const ALL: [Self; 3] = [Self::Size, Self::PartitionValues, Self::Tags];

    fn name(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::PartitionValues => "partitionValues",
            Self::Tags => "tags",
        }
    }
}

fn with_missing_extended_metadata_fields(
    engine: &dyn Engine,
    scan_files: FilteredEngineData,
    missing_fields: &[ExtendedMetadataField],
) -> Result<FilteredEngineData, Box<dyn std::error::Error>> {
    let (data, selection_vector) = scan_files.into_parts();
    let map_type = MapType::new(DataType::STRING, DataType::STRING, true);
    let tags = if missing_fields.contains(&ExtendedMetadataField::Tags) {
        Scalar::null(map_type.clone())
    } else {
        Scalar::Map(MapData::try_new(map_type.clone(), [("key", "value")])?)
    };
    let mut patch =
        ExpressionStructPatchBuilder::new().replace_at(["fileConstantValues"], "tags", lit(tags));
    for field in missing_fields {
        patch = match field {
            ExtendedMetadataField::Size => patch.replace("size", null_lit(DataType::LONG)),
            ExtendedMetadataField::PartitionValues => patch.replace_at(
                ["fileConstantValues"],
                field.name(),
                null_lit(map_type.clone()),
            ),
            ExtendedMetadataField::Tags => patch,
        };
    }
    let schema = scan_row_schema();
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        schema.clone(),
        Arc::new(Expr::struct_patch(patch)?),
        schema.into(),
    )?;
    let data = evaluator.evaluate(data.as_ref())?;
    Ok(FilteredEngineData::try_new(data, selection_vector)?)
}

#[tokio::test]
async fn test_remove_files_with_modified_selection_vector() -> Result<(), Box<dyn std::error::Error>>
{
    // This test verifies that we can selectively remove files by:
    // 1. Calling remove_files multiple times with different subsets
    // 2. Modifying the selection vector to choose which files to remove

    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let engine = Arc::new(engine);

        // Write data multiple times to create multiple files
        for i in 1..=5 {
            write_data_and_check_result_and_stats(
                table_url.clone(),
                schema.clone(),
                engine.clone(),
                i,
            )
            .await?;
        }

        // Get initial file count
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot.clone().scan_builder().build()?;

        let mut initial_file_count = 0;
        for metadata in scan.scan_metadata(engine.as_ref())? {
            let metadata = metadata?;
            initial_file_count += metadata
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&x| *x)
                .count();
        }

        assert!(
            initial_file_count >= 3,
            "Need at least 3 files for this test, got {initial_file_count}"
        );

        // Create a transaction to remove files in two batches
        let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
            .with_engine_info("selective remove test")
            .with_operation("DELETE".to_string())
            .with_data_change(true);

        // First batch: Remove only the first file
        let scan2 = snapshot.clone().scan_builder().build()?;
        let scan_metadata2 = scan2.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (data, mut selection_vector) = scan_metadata2.scan_files.into_parts();

        // Select only the first file for removal
        let mut first_batch_removed = 0;
        for selected in selection_vector.iter_mut() {
            if *selected && first_batch_removed < 1 {
                // Keep selected for removal
                first_batch_removed += 1;
            } else {
                // Don't remove
                *selected = false;
            }
        }

        assert_eq!(
            first_batch_removed, 1,
            "Should remove exactly 1 file in first batch"
        );
        txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

        // Second batch: Remove only the last file
        let scan3 = snapshot.clone().scan_builder().build()?;
        let scan_metadata3 = scan3.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (data2, mut selection_vector2) = scan_metadata3.scan_files.into_parts();

        // Find the last selected file and keep only that one selected
        let mut last_selected_idx = None;
        for (i, &selected) in selection_vector2.iter().enumerate() {
            if selected {
                last_selected_idx = Some(i);
            }
        }

        // Deselect all except the last one
        for (i, selected) in selection_vector2.iter_mut().enumerate() {
            if Some(i) != last_selected_idx {
                *selected = false;
            }
        }

        let second_batch_removed = selection_vector2.iter().filter(|&x| *x).count();
        assert_eq!(
            second_batch_removed, 1,
            "Should remove exactly 1 file in second batch"
        );
        txn.remove_files(FilteredEngineData::try_new(data2, selection_vector2)?);

        // Commit the transaction
        let result = txn.commit(engine.as_ref())?;

        match result {
            CommitResult::CommittedTransaction(committed) => {
                assert_eq!(committed.commit_version(), 6);

                // Verify that exactly 2 files were removed (1 from each batch)
                let new_snapshot = Snapshot::builder_for(table_url.clone())
                    .at_version(6)
                    .build(engine.as_ref())?;

                let new_scan = new_snapshot.scan_builder().build()?;
                let mut new_file_count = 0;
                for new_metadata in new_scan.scan_metadata(engine.as_ref())? {
                    let metadata = new_metadata?;
                    new_file_count += metadata
                        .scan_files
                        .selection_vector()
                        .iter()
                        .filter(|&x| *x)
                        .count();
                }

                // Verify we removed exactly 2 files (1 + 1)
                let total_removed = first_batch_removed + second_batch_removed;
                assert_eq!(total_removed, 2);
                assert_eq!(new_file_count, initial_file_count - total_removed);
                assert!(new_file_count > 0, "At least one file should remain");
            }
            _ => panic!("Transaction did not succeed"),
        }
    }
    Ok(())
}

/// Regression test for https://github.com/delta-io/delta-kernel-rs/issues/2040
///
/// When `scan_metadata()` is called with a predicate, the scan row schema includes a
/// `stats_parsed` column (7th column). Passing that scan metadata to `remove_files()` then
/// `commit()` previously failed with "Too few fields in output schema" because the transform
/// evaluator exhausted the output schema when it encountered the extra column.
///
/// Both predicate and non-predicate scans are tested because `remove_files` should behave
/// identically regardless. Cases also vary the checkpoint format:
/// - `use_struct_stats_checkpoint=false`: `stats` is non-null (raw JSON from the Add action). The
///   remove action's `stats` comes from passthrough.
/// - `use_struct_stats_checkpoint=true`: a checkpoint is written with `writeStatsAsJson=false,
///   writeStatsAsStruct=true`, so the checkpoint stores `stats_parsed` but omits the raw `stats`
///   JSON string. Scan rows from that checkpoint have `stats=null` but `stats_parsed` non-null. The
///   remove action's `stats` is produced via `coalesce(null, to_json(stats_parsed))`, which
///   exercises the coalesce path in the fix.
#[rstest::rstest]
#[case(false, false)]
#[case(false, true)]
#[case(true, false)]
#[case(true, true)]
// Multi-thread runtime required because the struct-stats checkpoint case calls
// `Snapshot::checkpoint`, which makes nested `block_on` calls that deadlock
// a single-threaded executor. `TokioMultiThreadExecutor` uses `block_in_place`
// which avoids the deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_remove_files_after_predicate_scan_includes_stats_parsed(
    #[case] use_struct_stats_checkpoint: bool,
    #[case] use_predicate: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    // Use a local directory so we can inspect the commit log for stats content.
    let tmp_dir = tempdir()?;
    let tmp_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], Some(&tmp_url), "test_table").await?
    {
        let engine = Arc::new(engine);

        // Write data (two parquet files with numbers [1,2,3] and [4,5,6]).
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;

        // When use_struct_stats_checkpoint=true, update table properties so the checkpoint
        // omits the stats JSON string (writeStatsAsJson=false) but stores stats as a struct
        // (writeStatsAsStruct=true). After checkpointing, scan rows from the checkpoint have
        // stats=null and stats_parsed=non-null, exercising the coalesce path in the fix.
        let snapshot = if use_struct_stats_checkpoint {
            let table_path = table_url.to_file_path().unwrap();
            let snapshot_v2 = set_table_properties(
                table_path.to_str().unwrap(),
                &table_url,
                engine.as_ref(),
                1,
                &[
                    ("delta.checkpoint.writeStatsAsJson", "false"),
                    ("delta.checkpoint.writeStatsAsStruct", "true"),
                ],
            )?;
            // `Snapshot::checkpoint` makes nested `block_on` calls internally (it reads the
            // log segment lazily while writing). This requires `TokioMultiThreadExecutor`,
            // which uses `block_in_place` to avoid deadlocking a single-thread runtime.
            let mt_engine = create_default_engine_mt_executor(&table_url)?;
            snapshot_v2.checkpoint(mt_engine.as_ref(), None)?;
            Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?
        } else {
            Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?
        };

        // commit_version = 2 (no checkpoint) or 3 (properties commit + checkpoint bump)
        let expected_commit_version = if use_struct_stats_checkpoint { 3 } else { 2 };

        // Always request all stats columns so stats_parsed is present in scan metadata
        // regardless of whether a predicate is used. This ensures remove_files can always
        // reconstruct stats (including the coalesce path when writeStatsAsJson=false).
        let mut scan_builder = snapshot
            .clone()
            .scan_builder()
            .with_stats(StatsOptions::all());
        if use_predicate {
            scan_builder =
                scan_builder.with_predicate(Arc::new(Pred::gt(col!("number"), lit(0_i32))));
        }
        let scan = scan_builder.build()?;

        let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);

        // Pass scan metadata (which contains stats_parsed) directly to remove_files.
        // This previously failed with "Too few fields in output schema".
        for scan_metadata in scan.scan_metadata(engine.as_ref())? {
            txn.remove_files(scan_metadata?.scan_files);
        }

        let committed = txn.commit(engine.as_ref())?.unwrap_committed();
        assert_eq!(committed.commit_version(), expected_commit_version);

        let remove_actions =
            read_actions_from_commit(&table_url, expected_commit_version, "remove")?;
        assert!(
            !remove_actions.is_empty(),
            "expected remove actions in commit"
        );

        // stats must be populated in every remove action: stats_parsed is always present
        // (via `StatsOptions::all()`), so the coalesce path handles even checkpoints
        // that omit the raw JSON stats string (writeStatsAsJson=false).
        for remove in &remove_actions {
            let stats_str = remove["stats"]
                .as_str()
                .expect("stats field should be a non-null JSON string");
            let stats: serde_json::Value = serde_json::from_str(stats_str)?;
            assert!(
                stats[NUM_RECORDS].as_i64().unwrap_or(0) > 0,
                "stats.numRecords should be populated, got: {stats}"
            );
        }
    }
    Ok(())
}

/// Remove files via scan metadata on a partitioned table. Covers three predicate
/// shapes against the same table so the remove-transform correctly handles the
/// parsed scan columns in every combination:
/// - no predicate: no `partitionValues_parsed`.
/// - data-column predicate: no `partitionValues_parsed` (negative case; the fix must not affect
///   scans whose predicate misses the partition columns).
/// - partition predicate: `partitionValues_parsed` present.
///
/// Every case sets `.with_stats(StatsOptions::all())`, which forces `stats_parsed`
/// into the scan output regardless of the predicate shape, so the partition-
/// predicate case exercises both parsed-column drop paths together while the
/// other two exercise only the `stats_parsed` drop path. The coalesce
/// *reconstruction* of `stats` from `stats_parsed` is not exercised here
/// because `stats` is non-null; the sibling
/// `test_remove_files_after_predicate_scan_includes_stats_parsed` covers that.
///
/// `expected_partitions` is the multiset of `country` values expected across
/// the generated Remove actions. Its length gives the expected Remove count,
/// and its contents pin the correct partition was chosen (catches regressions
/// where the wrong partition is removed).
#[rstest::rstest]
#[case::no_predicate(None, &["usa", "japan"])]
#[case::data_predicate(
    Some(Pred::gt(col!("id"), lit(0_i32))),
    &["usa", "japan"]
)]
#[case::partition_predicate(
    Some(Pred::eq(col!("country"), lit("usa".to_string()))),
    &["usa"]
)]
#[tokio::test]
async fn test_remove_files_partitioned_with_parsed_columns(
    #[case] predicate: Option<Pred>,
    #[case] expected_partitions: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let partition_col = "country";
    let table_schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "country": STRING,
    };
    let data_schema = schema_ref! { nullable "id": INTEGER };

    // Local directory backing: `read_actions_from_commit` reads commit JSON off disk
    // and does not support the default in-memory store's `memory://` URL.
    let tmp_dir = tempdir()?;
    let tmp_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    for (table_url, engine, _store, _table_name) in setup_test_tables(
        table_schema.clone(),
        &[partition_col],
        Some(&tmp_url),
        "test_table",
    )
    .await?
    {
        let engine = Arc::new(engine);

        // Write two partitions: country="usa" and country="japan".
        let mut txn =
            load_and_begin_transaction(table_url.clone(), engine.as_ref())?.with_data_change(true);
        let write_state = txn.write_state()?;
        let append_data = [[1, 2, 3], [10, 20, 30]].map(|data| -> delta_kernel::DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(data_schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });
        for (data, partition_val) in append_data.into_iter().zip(["usa", "japan"]) {
            let ctx = write_state.partitioned_write_context(HashMap::from([(
                partition_col.to_string(),
                Scalar::String(partition_val.into()),
            )]))?;
            let add_meta = engine.write_parquet(data?.as_ref(), &ctx).await?;
            txn.add_files(add_meta);
        }
        txn.commit(engine.as_ref())?.unwrap_committed();

        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let mut scan_builder = snapshot
            .clone()
            .scan_builder()
            .with_stats(StatsOptions::all());
        if let Some(pred) = predicate.clone() {
            scan_builder = scan_builder.with_predicate(Arc::new(pred));
        }
        let scan = scan_builder.build()?;

        let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
        for scan_metadata in scan.scan_metadata(engine.as_ref())? {
            txn.remove_files(scan_metadata?.scan_files);
        }
        let committed = txn.commit(engine.as_ref())?.unwrap_committed();
        assert_eq!(committed.commit_version(), 2);

        let remove_actions = read_actions_from_commit(&table_url, 2, "remove")?;
        assert_eq!(
            remove_actions.len(),
            expected_partitions.len(),
            "unexpected remove count; got {}: {remove_actions:?}",
            remove_actions.len()
        );

        let mut actual_partitions: Vec<String> = remove_actions
            .iter()
            .filter_map(|r| {
                r["partitionValues"][partition_col]
                    .as_str()
                    .map(String::from)
            })
            .collect();
        actual_partitions.sort();
        let mut expected_sorted: Vec<String> =
            expected_partitions.iter().map(|s| s.to_string()).collect();
        expected_sorted.sort();
        assert_eq!(
            actual_partitions, expected_sorted,
            "partitionValues mismatch across removes; got: {remove_actions:?}"
        );

        // stats_parsed is present on every scan row, so the stats-with-parsed
        // evaluator is selected for every case; it must still yield a populated
        // stats JSON on every remove action.
        for remove in &remove_actions {
            let stats_str = remove["stats"]
                .as_str()
                .expect("stats field should be a non-null JSON string");
            let stats: serde_json::Value = serde_json::from_str(stats_str)?;
            assert!(
                stats[NUM_RECORDS].as_i64().unwrap_or(0) > 0,
                "stats.numRecords should be populated, got: {stats}"
            );
        }
    }
    Ok(())
}

fn modify_staged_remove_file(
    batch: &RecordBatch,
    modification: StagedRemoveFileModification,
) -> Result<RecordBatch, ArrowError> {
    let field_index = batch.schema().index_of(modification.field)?;
    let mut columns = batch.columns().to_vec();
    let modified_value = match modification.value {
        StagedRemoveFileFieldValue::Null => {
            new_null_array(batch.schema().field(field_index).data_type(), 1)
        }
        StagedRemoveFileFieldValue::String(value) => {
            Arc::new(StringArray::from(vec![value])) as ArrayRef
        }
        StagedRemoveFileFieldValue::Int64(value) => {
            Arc::new(Int64Array::from(vec![value])) as ArrayRef
        }
    };
    let column = batch.column(field_index);
    let slices = [
        column.slice(0, modification.modified_row_index),
        modified_value,
        column.slice(
            modification.modified_row_index + 1,
            batch.num_rows() - modification.modified_row_index - 1,
        ),
    ];
    let arrays = slices
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<&dyn Array>>();
    columns[field_index] = concat(&arrays)?;
    RecordBatch::try_new(batch.schema(), columns)
}
