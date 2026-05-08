use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::Snapshot;
use itertools::Itertools;
use serde_json::Deserializer;
use tempfile::tempdir;
use test_utils::{create_table, engine_store_setup, test_table_setup_mt};
use url::Url;

/// Validates that kernel rejects remove actions on row-tracking tables.
#[tokio::test]
async fn test_row_tracking_blocks_remove_files() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    let tmp_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    let (store, engine, table_location) =
        engine_store_setup("test_row_tracking", Some(&tmp_test_dir_url));

    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["rowTracking", "domainMetadata"],
    )
    .await?;

    // ===== FIRST COMMIT: Add files with row tracking =====
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_engine_info("row tracking test")
        .with_data_change(true);

    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )?;

    let engine_arc = Arc::new(engine);
    let write_context = Arc::new(txn.unpartitioned_write_context()?);
    let add_files_metadata = engine_arc
        .write_parquet(&ArrowEngineData::new(data), write_context.as_ref())
        .await?;

    txn.add_files(add_files_metadata);

    let result = txn.commit(engine_arc.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 1);
        }
        _ => panic!("First commit should be committed"),
    }

    // ===== VERIFY: Check add action contains row tracking fields =====
    let commit1_url = tmp_test_dir_url
        .join("test_row_tracking/_delta_log/00000000000000000001.json")
        .unwrap();
    let commit1 = store
        .get(&Path::from_url_path(commit1_url.path()).unwrap())
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Find the add action
    let add_actions: Vec<_> = parsed_commits
        .iter()
        .filter(|action| action.get("add").is_some())
        .collect();

    assert_eq!(add_actions.len(), 1, "Expected exactly one add action");

    let add = &add_actions[0]["add"];

    // Verify baseRowId is present and has expected value
    assert!(
        add.get("baseRowId").is_some(),
        "baseRowId MUST be present when row tracking is enabled"
    );
    let base_row_id = add["baseRowId"]
        .as_i64()
        .expect("baseRowId should be an i64");
    // For the first file in a table with row tracking, baseRowId should start at 0
    // (high water mark starts at -1, so first baseRowId is -1 + 1 = 0)
    assert_eq!(base_row_id, 0, "First file should have baseRowId 0");

    let default_row_commit_version = add["defaultRowCommitVersion"]
        .as_i64()
        .expect("Missing defaultRowCommitVersion");
    assert_eq!(default_row_commit_version, 1);

    // ===== SECOND COMMIT: Remove the file =====
    let snapshot2 = Snapshot::builder_for(table_url.clone()).build(engine_arc.as_ref())?;
    let mut txn2 = snapshot2
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine_arc.as_ref())?
        .with_engine_info("row tracking remove test")
        .with_data_change(true);

    let scan = snapshot2.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine_arc.as_ref())?.next().unwrap()?;

    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    let remove_metadata = FilteredEngineData::try_new(data, selection_vector)?;

    txn2.remove_files(remove_metadata);

    // ===== VERIFY: kernel rejects the commit because row tracking blocks remove actions. =====
    let err = txn2
        .commit(engine_arc.as_ref())
        .expect_err("commit must fail when remove_files is staged on a row-tracking table");
    let msg = err.to_string();
    assert!(
        msg.contains("Remove actions are not yet supported") && msg.contains("rowTracking"),
        "expected remove-block error mentioning rowTracking, got: {msg}",
    );

    Ok(())
}

/// `rowTracking` listed as supported (but not enabled) does NOT trigger the remove block.
#[tokio::test(flavor = "multi_thread")]
async fn test_row_tracking_supported_but_not_enabled_allows_remove(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    // Row tracking is supported but not enabled.
    let _ = kernel_create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties([("delta.feature.rowTracking", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Append a file, then remove it -- commit must succeed.
    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    test_utils::insert_data(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?
    .unwrap_committed();

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let scan = snapshot.clone().scan_builder().build()?;
    let scan_files = scan
        .scan_metadata(engine.as_ref())?
        .next()
        .unwrap()?
        .scan_files;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);
    txn.remove_files(scan_files);
    txn.commit(engine.as_ref())?.unwrap_committed();
    Ok(())
}
