//! Integration tests for change-data-feed aware write paths.

use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::schema::SchemaRef;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{Snapshot, Version};
use tempfile::{tempdir, TempDir};
use test_utils::{assert_result_error_with_message, create_table, engine_store_setup};
use url::Url;

use crate::common::write_utils::get_simple_int_schema;

// Helper function to create a table with CDF enabled
async fn create_cdf_table(
    table_name: &str,
    schema: SchemaRef,
) -> Result<(Url, Arc<DefaultEngine<TokioBackgroundExecutor>>, TempDir), Box<dyn std::error::Error>>
{
    let tmp_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    let (store, engine, table_location) = engine_store_setup(table_name, Some(&tmp_test_dir_url));

    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true, // use protocol 3.7
        vec![],
        vec!["changeDataFeed"],
    )
    .await?;

    Ok((table_url, Arc::new(engine), tmp_dir))
}

// Helper function to write data to a table
async fn write_data_to_table(
    table_url: &Url,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<Version, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Arc::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("test");

    add_files_to_transaction(&mut txn, engine, schema, values).await?;

    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => Ok(committed.commit_version()),
        _ => panic!("Transaction should be committed"),
    }
}

// Helper function to add files to an existing transaction
async fn add_files_to_transaction(
    txn: &mut delta_kernel::transaction::Transaction,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(values))],
    )?;

    let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());
    let add_files_metadata = engine
        .write_parquet(&ArrowEngineData::new(data), write_context.as_ref())
        .await?;
    txn.add_files(add_files_metadata);
    Ok(())
}

#[tokio::test]
async fn test_cdf_write_all_adds_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that add-only transactions work with CDF enabled
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_all_adds", schema.clone()).await?;

    // Add files - this should succeed
    let version = write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;
    assert_eq!(version, 1);

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_all_removes_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that remove-only transactions work with CDF enabled
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_all_removes", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;

    // Now remove the files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Arc::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("cdf remove test")
        .with_data_change(true);

    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - remove-only transactions are allowed with CDF
    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 2);
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_mixed_no_data_change_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that mixed add+remove transactions work when dataChange=false.
    // It's allowed because the transaction does not contain any logical data changes.
    // This can happen when a table is being optimized/compacted.
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_mixed_no_data_change", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // Now create a transaction with both add AND remove files, but dataChange=false
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Arc::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("cdf mixed test")
        .with_data_change(false); // dataChange=false is key here

    // Add new files
    add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - mixed operations are allowed when dataChange=false
    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 2);
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_mixed_with_data_change_fails() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that mixed add+remove transactions fail with helpful error when
    // dataChange=true
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_mixed_with_data_change", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // Now create a transaction with both add AND remove files with dataChange=true
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Arc::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("cdf mixed fail test")
        .with_data_change(true); // dataChange=true - this should fail

    // Add new files
    add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should fail with our new error message
    assert_result_error_with_message(
        txn.commit(engine.as_ref()),
        "Cannot add and remove data in the same transaction when Change Data Feed is enabled (delta.enableChangeDataFeed = true). \
         This would require writing CDC files for DML operations, which is not yet supported. \
         Consider using separate transactions: one to add files, another to remove files."
    );

    Ok(())
}
