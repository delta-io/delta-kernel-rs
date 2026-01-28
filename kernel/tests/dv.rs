//! Read a small table with/without deletion vectors.
//! Must run at the root of the crate
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, EngineData, Snapshot};
use tempfile::tempdir;
use test_utils::{
    create_add_files_metadata, create_table, engine_store_setup, generate_batch, into_record_batch,
    record_batch_to_bytes, IntoArray,
};

use itertools::Itertools;
use test_log::test;

/// Helper to write a parquet file with the given data to the table.
/// Returns the file path (relative to table root) that was written.
async fn write_parquet_file(
    store: &Arc<dyn object_store::ObjectStore>,
    table_url: &url::Url,
    file_suffix: &str,
    data: &delta_kernel::arrow::record_batch::RecordBatch,
) -> Result<(String, usize), Box<dyn std::error::Error>> {
    use object_store::path::Path as ObjectStorePath;

    let parquet_data = record_batch_to_bytes(data);
    let parquet_data_len = parquet_data.len();
    let data_file_path = format!("data_file_{}.parquet", file_suffix);

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

#[test]
fn dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let stream = scan.execute(engine)?;
    let total_rows = count_total_scan_rows(stream)?;
    assert_eq!(total_rows, 8);
    Ok(())
}

#[test]
fn non_dv_table() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/"))?;
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let stream = scan.execute(engine)?;
    let total_rows = count_total_scan_rows(stream)?;
    assert_eq!(total_rows, 10);
    Ok(())
}

/// Helper to extract scan files from a snapshot
fn get_scan_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Vec<FilteredEngineData>> {
    let scan = snapshot.scan_builder().build()?;
    let all_scan_metadata: Vec<_> = scan.scan_metadata(engine)?.collect::<Result<Vec<_>, _>>()?;

    Ok(all_scan_metadata
        .into_iter()
        .map(|sm| sm.scan_files)
        .collect())
}

/// Helper to get a write context for creating deletion vector paths.
fn get_write_context(
    table_url: &url::Url,
    engine: &dyn delta_kernel::Engine,
) -> Result<delta_kernel::transaction::WriteContext, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()))?;
    Ok(txn.get_write_context())
}

/// Helper to write a deletion vector to object store and return its descriptor.
async fn write_deletion_vector_to_store(
    store: &Arc<dyn object_store::ObjectStore>,
    write_context: &delta_kernel::transaction::WriteContext,
    dv: KernelDeletionVector,
    prefix: &str,
) -> Result<
    delta_kernel::actions::deletion_vector::DeletionVectorDescriptor,
    Box<dyn std::error::Error>,
> {
    use object_store::path::Path as ObjectStorePath;

    let dv_path = write_context.new_deletion_vector_path(String::from(prefix));
    let dv_absolute_path = dv_path.absolute_path()?;
    let dv_object_path = ObjectStorePath::parse(dv_absolute_path.path())?;

    let mut dv_buffer = Vec::new();
    let mut dv_writer = StreamingDeletionVectorWriter::new(&mut dv_buffer);
    let dv_write_result = dv_writer.write_deletion_vector(dv)?;
    dv_writer.finalize()?;

    store.put(&dv_object_path, dv_buffer.into()).await?;

    Ok(dv_write_result.to_descriptor(&dv_path))
}

/// Helper to create a transaction for deletion vector updates.
fn create_dv_update_transaction(
    table_url: &url::Url,
    engine: &dyn delta_kernel::Engine,
) -> Result<delta_kernel::transaction::Transaction, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    Ok(snapshot
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test engine")
        .with_operation("DELETE".to_string()))
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
        ("id", vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_array()),
        (
            "value",
            vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].into_array(),
        ),
    ])?;

    let data_batch_2 = generate_batch(vec![
        (
            "id",
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19].into_array(),
        ),
        (
            "value",
            vec!["k", "l", "m", "n", "o", "p", "q", "r", "s", "t"].into_array(),
        ),
    ])?;

    let (data_file_path_1, parquet_data_len_1) =
        write_parquet_file(&store, &table_url, "1", &data_batch_1).await?;
    let (data_file_path_2, parquet_data_len_2) =
        write_parquet_file(&store, &table_url, "2", &data_batch_2).await?;

    // Step 2: Add both files to the table via a transaction
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("test engine")
        .with_operation("WRITE".to_string());

    // Create add file metadata for both files
    let add_files_schema = txn.add_files_schema();
    let add_metadata = create_add_files_metadata(
        add_files_schema,
        vec![
            (&data_file_path_1, parquet_data_len_1 as i64, 1000000, 10),
            (&data_file_path_2, parquet_data_len_2 as i64, 1000000, 10),
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

    // Step 5: Get write context and write the first deletion vector to a file
    let write_context = get_write_context(&table_url, engine.as_ref())?;
    let dv_descriptor_1 =
        write_deletion_vector_to_store(&store, &write_context, dv_file1_first, "").await?;

    // Step 6: Update deletion vectors for first file only
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;
    let scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;

    let mut dv_map = HashMap::new();
    dv_map.insert(data_file_path_1.clone(), dv_descriptor_1);

    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;
    let commit_result = txn.commit(engine.as_ref())?;
    assert!(matches!(
        commit_result,
        CommitResult::CommittedTransaction(_)
    ));

    // Step 9: Verify first deletion - should have 17 rows (7 from file 1 + 10 from file 2)
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let stream = scan.execute(engine.clone())?;

    let total_rows_after_first_delete = count_total_scan_rows(stream)?;
    assert_eq!(
        total_rows_after_first_delete, 17,
        "Should have 17 rows after deleting 3 rows from first file"
    );

    // Step 10: Second deletion - Delete row 1 from file 1 and rows 12, 15 from file 2
    let mut dv_file1_second = KernelDeletionVector::new();
    dv_file1_second.add_deleted_row_indexes(FILE1_FIRST_DELETE_INDEXES); // Previous deletions
    dv_file1_second.add_deleted_row_indexes([FILE1_SECOND_DELETE_INDEX]); // Additional deletion

    let mut dv_file2 = KernelDeletionVector::new();
    dv_file2.add_deleted_row_indexes(FILE2_DELETE_INDEXES); // Delete rows at indices 2 and 5 (ids 12, 15)

    // Write deletion vectors for both files
    let dv_descriptor_1_second =
        write_deletion_vector_to_store(&store, &write_context, dv_file1_second, "").await?;
    let dv_descriptor_2 =
        write_deletion_vector_to_store(&store, &write_context, dv_file2, "").await?;

    // Step 11: Update deletion vectors for both files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;

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

    // Step 12: Verify final deletion - should have 14 rows (6 from file 1 + 8 from file 2)
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
