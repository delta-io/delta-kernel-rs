//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::KernelDeletionVector;
use delta_kernel::actions::{NUM_RECORDS, TIGHT_BOUNDS};
use delta_kernel::arrow::array::{BooleanArray, Int64Array, StructArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::scan::StatsOptions;
use delta_kernel::schema::{schema_ref, DataType, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, EngineData, Snapshot};
use itertools::Itertools;
use tempfile::tempdir;
use test_utils::{
    add_commit, create_add_files_metadata, create_default_engine_mt_executor, create_table,
    engine_store_setup, generate_batch, into_record_batch, load_and_begin_transaction,
    read_actions_from_commit, record_batch_to_bytes, IntoArray,
};

use crate::common::write_utils::{
    create_dv_update_transaction, get_scan_files, resolve_struct_field,
    write_deletion_vector_to_store,
};

/// Helper to write a parquet file with the given data to the table.
/// Returns the file path (relative to table root) that was written.
async fn write_parquet_file(
    store: &Arc<dyn delta_kernel::object_store::ObjectStore>,
    table_url: &url::Url,
    file_suffix: &str,
    data: &delta_kernel::arrow::record_batch::RecordBatch,
) -> Result<(String, usize), Box<dyn std::error::Error>> {
    use delta_kernel::object_store::path::Path as ObjectStorePath;

    let parquet_data = record_batch_to_bytes(data);
    let parquet_data_len = parquet_data.len();
    let data_file_path = format!("data_file_{file_suffix}.parquet");

    // Construct the full object store path for the parquet file
    let data_url = table_url.join(&data_file_path)?;
    let data_object_path = ObjectStorePath::from_url_path(data_url.path())?;
    store.put(&data_object_path, parquet_data.into()).await?;

    Ok((data_file_path, parquet_data_len))
}

fn count_total_scan_rows(
    scan_result_iter: impl Iterator<Item = DeltaResult<Box<dyn EngineData>>>,
) -> DeltaResult<usize> {
    scan_result_iter
        .map(|result| Ok(result?.len()))
        .fold_ok(0, Add::add)
}

#[test_log::test(rstest::rstest)]
#[case::with_dv("./tests/data/table-with-dv-small/", 8)]
#[case::without_dv("./tests/data/table-without-dv-small/", 10)]
fn test_table_scan(
    #[case] table_path: &str,
    #[case] expected_rows: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from(table_path))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let stream = scan.execute(engine)?;
    let total_rows = count_total_scan_rows(stream)?;
    assert_eq!(total_rows, expected_rows);
    Ok(())
}

/// Helper to verify that scan results match expected ids and values (after sorting).
/// Extracts int32 id column and string value column from batches and compares with expected.
fn verify_sorted_scan_results(
    batches: Vec<delta_kernel::arrow::record_batch::RecordBatch>,
    expected_ids: Vec<i32>,
    expected_values: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::array::{Array, Int32Array, StringArray};

    // Extract actual ids and values from batches
    let mut actual_ids = Vec::new();
    let mut actual_values = Vec::new();

    for batch in batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let val_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            actual_ids.push(id_col.value(i));
            actual_values.push(val_col.value(i).to_string());
        }
    }

    // Sort and compare ids
    actual_ids.sort();
    assert_eq!(
        actual_ids, expected_ids,
        "IDs should match expected non-deleted rows"
    );

    // Sort and compare values
    actual_values.sort();
    let mut expected_values_sorted = expected_values
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    expected_values_sorted.sort();
    assert_eq!(
        actual_values, expected_values_sorted,
        "Values should match expected non-deleted rows"
    );

    Ok(())
}

/// End-to-end test that:
/// 1. Creates a table with deletion vector support
/// 2. Writes a parquet file with actual data rows
/// 3. Creates deletion vectors marking specific rows as deleted
/// 4. Writes the deletion vectors to a file using StreamingDeletionVectorWriter
/// 5. Commits the deletion vectors in a transaction
/// 6. Verifies that scanning only returns non-deleted rows
#[tokio::test]
async fn test_write_deletion_vectors_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Create a table schema with id and value columns
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ])?);

    // Setup table with deletion vector support
    let temp_dir = tempdir()?;
    let base_url = url::Url::from_directory_path(temp_dir.path()).unwrap();
    let (store, engine, table_url) = engine_store_setup("test_table", Some(&base_url));
    let engine = Arc::new(engine);

    // Create table with DV support (protocol 3/7 with deletionVectors feature)
    create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true, // use_37_protocol
        vec!["deletionVectors"],
        vec!["deletionVectors"],
    )
    .await?;

    // Step 1: Create and write two parquet files
    let data_batch_1 = generate_batch(vec![
        ("id", vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_arrow_array()),
        (
            "value",
            vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].into_arrow_array(),
        ),
    ])?;

    let data_batch_2 = generate_batch(vec![
        (
            "id",
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19].into_arrow_array(),
        ),
        (
            "value",
            vec!["k", "l", "m", "n", "o", "p", "q", "r", "s", "t"].into_arrow_array(),
        ),
    ])?;

    let (data_file_path_1, parquet_data_len_1) =
        write_parquet_file(&store, &table_url, "1", &data_batch_1).await?;
    let (data_file_path_2, parquet_data_len_2) =
        write_parquet_file(&store, &table_url, "2", &data_batch_2).await?;

    // Step 2: Add both files to the table via a transaction
    let mut txn = load_and_begin_transaction(table_url.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("WRITE".to_string());

    // Create add file metadata for both files
    let add_files_schema = txn.add_files_schema();
    let add_metadata = create_add_files_metadata(
        add_files_schema,
        vec![
            (
                &data_file_path_1,
                parquet_data_len_1 as i64,
                1000000,
                Some(10),
            ),
            (
                &data_file_path_2,
                parquet_data_len_2 as i64,
                1000000,
                Some(10),
            ),
        ],
    )?;

    txn.add_files(add_metadata);
    let commit_result = txn.commit(engine.as_ref())?;
    assert!(matches!(
        commit_result,
        CommitResult::CommittedTransaction(_)
    ));

    // Step 3: Verify we can read all 20 rows before deletion
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    let scan = snapshot.scan_builder().build()?;
    let stream = scan.execute(engine.clone())?;
    let total_rows_before = count_total_scan_rows(stream)?;
    assert_eq!(total_rows_before, 20, "Should have 20 rows before deletion");

    // Step 4: First deletion - Apply DV only to the first file (delete rows 2, 5, and 7)
    // Define deletion indexes in one place to avoid duplication
    const FILE1_FIRST_DELETE_INDEXES: [u64; 3] = [2, 5, 7];
    const FILE1_SECOND_DELETE_INDEX: u64 = 1;
    const FILE2_DELETE_INDEXES: [u64; 2] = [2, 5];

    let mut dv_file1_first = KernelDeletionVector::new();
    dv_file1_first.add_deleted_row_indexes(FILE1_FIRST_DELETE_INDEXES);

    // Step 5: Update deletion vectors for first file only
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;
    let write_context = txn.unpartitioned_write_context()?;
    let dv_descriptor_1 =
        write_deletion_vector_to_store(&store, &write_context, dv_file1_first, "").await?;
    let scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;

    let mut dv_map = HashMap::new();
    dv_map.insert(data_file_path_1.clone(), dv_descriptor_1);

    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;
    let commit_result = txn.commit(engine.as_ref())?;
    assert!(matches!(
        commit_result,
        CommitResult::CommittedTransaction(_)
    ));

    // Step 6: Verify first deletion - should have 17 rows (7 from file 1 + 10 from file 2)
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let stream = scan.execute(engine.clone())?;

    let total_rows_after_first_delete = count_total_scan_rows(stream)?;
    assert_eq!(
        total_rows_after_first_delete, 17,
        "Should have 17 rows after deleting 3 rows from first file"
    );

    // Step 7: Second deletion - Delete row 1 from file 1 and rows 12, 15 from file 2
    let mut dv_file1_second = KernelDeletionVector::new();
    dv_file1_second.add_deleted_row_indexes(FILE1_FIRST_DELETE_INDEXES); // Previous deletions
    dv_file1_second.add_deleted_row_indexes([FILE1_SECOND_DELETE_INDEX]); // Additional deletion

    let mut dv_file2 = KernelDeletionVector::new();
    dv_file2.add_deleted_row_indexes(FILE2_DELETE_INDEXES); // Delete rows at indices 2 and 5 (ids 12, 15)

    // Step 8: Update deletion vectors for both files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;
    let write_context = txn.unpartitioned_write_context()?;

    // Write deletion vectors for both files
    let dv_descriptor_1_second =
        write_deletion_vector_to_store(&store, &write_context, dv_file1_second, "").await?;
    let dv_descriptor_2 =
        write_deletion_vector_to_store(&store, &write_context, dv_file2, "").await?;

    let mut dv_map1 = HashMap::new();
    dv_map1.insert(data_file_path_1.clone(), dv_descriptor_1_second);
    let mut dv_map2 = HashMap::new();
    dv_map2.insert(data_file_path_2.clone(), dv_descriptor_2);

    // Test multiple calls
    txn.update_deletion_vectors(
        dv_map1,
        get_scan_files(snapshot.clone(), engine.as_ref())?
            .into_iter()
            .map(Ok),
    )?;
    txn.update_deletion_vectors(
        dv_map2,
        get_scan_files(snapshot.clone(), engine.as_ref())?
            .into_iter()
            .map(Ok),
    )?;
    let commit_result = txn.commit(engine.as_ref())?;
    assert!(matches!(
        commit_result,
        CommitResult::CommittedTransaction(_)
    ));

    // Step 9: Verify final deletion - should have 14 rows (6 from file 1 + 8 from file 2)
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let stream = scan.execute(engine.clone())?;

    // Collect all rows to verify content
    let batches: Vec<_> = stream
        .map(|result| result.map(into_record_batch))
        .collect::<Result<Vec<_>, _>>()?;

    // Verify the correct rows remain
    // File 1: all except 1, 2, 5, 7 => 0, 3, 4, 6, 8, 9
    // File 2: all except 12, 15 (indices 2, 5) => 10, 11, 13, 14, 16, 17, 18, 19
    let expected_ids = vec![0, 3, 4, 6, 8, 9, 10, 11, 13, 14, 16, 17, 18, 19];
    let expected_values = [
        "a", "d", "e", "g", "i", "j", "k", "l", "n", "o", "q", "r", "s", "t",
    ];

    // Verify the correct rows remain using helper
    verify_sorted_scan_results(batches, expected_ids, &expected_values)?;

    Ok(())
}

#[rstest::rstest]
#[case::tight_bounds_true(Some(true))]
#[case::tight_bounds_false(Some(false))]
#[case::tight_bounds_absent(None)]
#[tokio::test(flavor = "multi_thread")]
async fn test_dv_update_stats_tight_bound(
    #[case] initial_tight_bounds: Option<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Nested schema: a `point` struct with two leaf columns.
    let schema = schema_ref! {
        nullable "point": { nullable "x": INTEGER, nullable "y": INTEGER },
    };

    let temp_dir = tempdir()?;
    let base_url = url::Url::from_directory_path(temp_dir.path()).unwrap();
    let (store, engine, table_url) = engine_store_setup("test_table", Some(&base_url));
    let engine = Arc::new(engine);

    // Hand-write v0 as create table doesn't support `delta.checkpoint.writeStatsAsStruct`.
    std::fs::create_dir_all(table_url.to_file_path().unwrap())?;
    let protocol = serde_json::json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"],
            "writerFeatures": ["deletionVectors"],
        }
    });
    let metadata = serde_json::json!({
        "metaData": {
            "id": "test_id",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": serde_json::to_string(&schema)?,
            "partitionColumns": [],
            "configuration": {
                "delta.enableDeletionVectors": "true",
                "delta.checkpoint.writeStatsAsStruct": "true",
            },
            "createdTime": 1677811175819u64,
        }
    });
    let v0 = format!(
        "{}\n{}\n",
        serde_json::to_string(&protocol)?,
        serde_json::to_string(&metadata)?
    );
    add_commit(table_url.as_str(), store.as_ref(), 0, v0).await?;

    // Commit an Add at v1 with hand-crafted nested stats and the parameterized `tightBounds`.
    // Kernel inserts always set tightBounds: true, so we handcraft the commit here.
    let data_file_path = "data_file_1.parquet";
    let mut stats = serde_json::json!({
        "numRecords": 10,
        "nullCount": {"point": {"x": 0, "y": 2}},
        "minValues": {"point": {"x": 1, "y": 5}},
        "maxValues": {"point": {"x": 100, "y": 50}},
    });
    if let Some(tb) = initial_tight_bounds {
        stats["tightBounds"] = serde_json::Value::Bool(tb);
    }
    let stats_str = serde_json::to_string(&stats)?;
    let add = serde_json::json!({
        "add": {
            "path": data_file_path,
            "partitionValues": {},
            "size": 1000,
            "modificationTime": 1,
            "dataChange": true,
            "stats": stats_str,
        }
    });
    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        serde_json::to_string(&add)?,
    )
    .await?;

    // Apply a DV to the file and commit.
    let mut dv = KernelDeletionVector::new();
    dv.add_deleted_row_indexes([3u64]);
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;
    let write_context = txn.unpartitioned_write_context()?;
    let dv_descriptor = write_deletion_vector_to_store(&store, &write_context, dv, "").await?;

    let scan_files = get_scan_files(snapshot, engine.as_ref())?;
    let mut dv_map = HashMap::new();
    dv_map.insert(data_file_path.to_string(), dv_descriptor);
    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;
    txn.commit(engine.as_ref())?.unwrap_committed();

    // The new AddFile must report tightBounds: false while preserving every other stats field.
    let v2_adds = read_actions_from_commit(&table_url, 2, "add")?;
    assert_eq!(v2_adds.len(), 1, "expected exactly one add at v2");
    let add_stats: serde_json::Value = serde_json::from_str(
        v2_adds[0]["stats"]
            .as_str()
            .expect("add stats should be a JSON string"),
    )?;
    assert_eq!(
        add_stats["tightBounds"].as_bool(),
        Some(false),
        "DV-update add must report tightBounds: false regardless of the original value"
    );
    assert_eq!(add_stats["numRecords"], stats["numRecords"]);
    assert_eq!(
        add_stats["nullCount"], stats["nullCount"],
        "nested nullCount must survive verbatim"
    );
    assert_eq!(
        add_stats["minValues"], stats["minValues"],
        "nested minValues must survive verbatim"
    );
    assert_eq!(
        add_stats["maxValues"], stats["maxValues"],
        "nested maxValues must survive verbatim"
    );

    // The paired RemoveFile must preserve the original stats unchanged.
    let v2_removes = read_actions_from_commit(&table_url, 2, "remove")?;
    assert_eq!(v2_removes.len(), 1, "expected exactly one remove at v2");
    let remove_stats: serde_json::Value = serde_json::from_str(
        v2_removes[0]["stats"]
            .as_str()
            .expect("remove stats should be a JSON string"),
    )?;
    assert_eq!(
        remove_stats["tightBounds"].as_bool(),
        initial_tight_bounds,
        "remove action must preserve the original tightBounds unchanged"
    );

    // Checkpoint and validate the tightBounds of stats_parsed.
    let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mt_engine = create_default_engine_mt_executor(&table_url)?;
    snapshot_v2.checkpoint(mt_engine.as_ref(), None)?;

    let ckpt_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let scan = ckpt_snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .build()?;
    let mut checked_stats_parsed = false;
    for scan_metadata in scan.scan_metadata(engine.as_ref())? {
        let (data, selection_vector) = scan_metadata?.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        let batch_struct = StructArray::from(batch.clone());
        let tight_bounds: &BooleanArray =
            resolve_struct_field(&batch_struct, &["stats_parsed".into(), TIGHT_BOUNDS.into()]);
        let num_records: &Int64Array =
            resolve_struct_field(&batch_struct, &["stats_parsed".into(), NUM_RECORDS.into()]);
        for (i, &selected) in selection_vector.iter().enumerate().take(batch.num_rows()) {
            if !selected {
                continue;
            }
            assert!(
                !tight_bounds.value(i),
                "checkpoint stats_parsed.tightBounds must be false after a DV update"
            );
            assert_eq!(
                num_records.value(i),
                10,
                "checkpoint stats_parsed.numRecords must be preserved"
            );
            checked_stats_parsed = true;
        }
    }
    assert!(
        checked_stats_parsed,
        "expected at least one selected file with stats_parsed in the checkpoint"
    );

    Ok(())
}
