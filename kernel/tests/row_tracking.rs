use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStore;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};

use itertools::Itertools;
use serde_json::Deserializer;
use tempfile::{tempdir, TempDir};
use url::Url;

mod common;
use common::{latest_snapshot_test, load_test_data};
use test_utils::{create_table, engine_store_setup, test_read};

// mod golden_tables;
// use golden_tables::latest_snapshot_test;

/// Helper function to create a simple table with row tracking enabled
async fn create_row_tracking_table(
    tmp_dir: &TempDir,
    table_name: &str,
    schema: SchemaRef,
) -> Result<
    (
        Url,
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<dyn ObjectStore>,
    ),
    Box<dyn std::error::Error>,
> {
    let tmp_test_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    let (store, engine, table_location) = engine_store_setup(table_name, Some(&tmp_test_dir_url));

    // Create table with row tracking feature enabled
    let table_url = create_table(
        store.clone(),
        table_location,
        schema,
        &[],  // no partition columns
        true, // use 37 protocol
        vec![],
        vec!["rowTracking"],
    )
    .await?;

    Ok((table_url, Arc::new(engine), store))
}

/// Helper function to write data and return the number of records written
async fn write_data_to_table(
    table_url: &Url,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    data: Vec<DeltaResult<Box<ArrowEngineData>>>,
) -> DeltaResult<CommitResult> {
    let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), engine.as_ref(), None)?);
    let mut txn = snapshot.transaction()?;

    // Write data out by spawning async tasks to simulate executors
    let write_context = Arc::new(txn.get_write_context());
    let tasks = data.into_iter().map(|data| {
        let engine = engine.clone();
        let write_context = write_context.clone();
        tokio::task::spawn(async move {
            engine
                .write_parquet(
                    data.as_ref().unwrap(),
                    write_context.as_ref(),
                    HashMap::new(),
                    true,
                )
                .await
        })
    });

    let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();

    for meta in add_files_metadata {
        let metadata = meta?;
        txn.add_files(metadata);
    }

    // Commit the transaction
    Ok(txn.commit(engine.as_ref())?)
}

/// Helper function to verify row tracking fields in commit log
async fn verify_row_tracking_commit(
    store: &Arc<dyn ObjectStore>,
    table_url: &Url,
    commit_version: u64,
    expected_num_actions: usize,
    expected_base_row_ids: Vec<i64>,
    expected_row_id_high_water_mark: i64,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let commit_url = table_url.join(&format!("_delta_log/{:020}.json", commit_version))?;
    let commit = store
        .get(&Path::from_url_path(commit_url.path()).unwrap())
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Verify structure: commitInfo + add actions + domainMetadata
    assert_eq!(
        parsed_commits.len(),
        expected_num_actions,
        "Unexpected number of actions in commit"
    );

    // Check that the commitInfo exists
    assert!(parsed_commits[0].get("commitInfo").is_some());

    // Check that add actions have row tracking fields
    for i in 1..expected_num_actions - 1 {
        let add_action = parsed_commits[i]
            .get("add")
            .expect("Should have add action");

        // Check that baseRowId exists and is a number
        let base_row_id = add_action
            .get("baseRowId")
            .expect("Add action should have baseRowId field");
        assert_eq!(
            base_row_id.as_i64().unwrap(),
            expected_base_row_ids[i - 1],
            "baseRowId should match expected value"
        );

        // Check that defaultRowCommitVersion exists and equals the commit version
        let default_row_commit_version = add_action
            .get("defaultRowCommitVersion")
            .expect("Add action should have defaultRowCommitVersion field");
        assert_eq!(
            default_row_commit_version.as_i64().unwrap(),
            commit_version as i64,
            "defaultRowCommitVersion should match commit version"
        );
    }

    // Check that domain metadata action exists for row tracking
    let domain_metadata = parsed_commits[expected_num_actions - 1]
        .get("domainMetadata")
        .expect("Should have domainMetadata action");

    assert_eq!(
        domain_metadata.get("domain").unwrap().as_str().unwrap(),
        "delta.rowTracking"
    );

    // Check that the configuration contains rowIdHighWaterMark
    let configuration = domain_metadata
        .get("configuration")
        .unwrap()
        .as_str()
        .unwrap();
    let config_json: serde_json::Value = serde_json::from_str(configuration)?;
    assert!(config_json.get("rowIdHighWaterMark").is_some());
    let row_id_high_water_mark = config_json
        .get("rowIdHighWaterMark")
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(row_id_high_water_mark, expected_row_id_high_water_mark);

    Ok(parsed_commits)
}

#[tokio::test]
async fn test_append_row_tracking() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_append", schema.clone()).await?;

    // Create two new arrow record batches to append
    let data = [[1, 2, 3], [4, 5, 6]]
        .map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        })
        .into_iter()
        .collect::<Vec<_>>();

    // Write data: two batches with 3 records each
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_commit(
        &store,
        &table_url,
        1,          // commit to verify
        4,          // expected number of actions in commit
        vec![0, 3], // expected base row IDs
        5,          // expected high watermark
    )
    .await?;

    // Verify the data can still be read correctly
    test_read(
        &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
        )?),
        &table_url,
        engine,
    )?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_single_record_batches() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_single_records", schema.clone()).await?;

    // Write individual records in separate batches
    let data = [[1], [2], [3]]
        .map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        })
        .into_iter()
        .collect::<Vec<_>>();
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_commit(
        &store,
        &table_url,
        1,             // commit to verify
        5,             // expected number of actions in commit
        vec![0, 1, 2], // expected base row IDs
        2,             // expected high watermark
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_large_batch() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_large_batch", schema.clone()).await?;

    // Write a large batch with 1000 records
    let large_batch: Vec<i32> = (1..=1000).collect();
    let data = [large_batch.clone()]
        .map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        })
        .into_iter()
        .collect::<Vec<_>>();
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_commit(
        &store,
        &table_url,
        1,       // commit to verify
        3,       // expected number of actions in commit
        vec![0], // expected base row IDs
        999,     // expected high watermark
    )
    .await?;

    // Verify the data can still be read correctly
    test_read(
        &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(large_batch))],
        )?),
        &table_url,
        engine,
    )?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_consecutive_transactions() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_consecutive_commits", schema.clone())
            .await?;

    // First transaction: write two batches with 3 records each
    let data_1 = [[1, 2, 3], [4, 5, 6]]
        .map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        })
        .into_iter()
        .collect::<Vec<_>>();
    write_data_to_table(&table_url, engine.clone(), data_1).await?;

    // Verify first commit
    verify_row_tracking_commit(
        &store,
        &table_url,
        1,          // commit to verify
        4,          // expected number of actions in commit
        vec![0, 3], // expected base row IDs
        5,          // expected high watermark
    )
    .await?;

    // Second transaction: write one batch with 2 records
    // This should read the existing row tracking domain metadata and assign base row IDs starting from 6
    let data_2 = [[7, 8]]
        .map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        })
        .into_iter()
        .collect::<Vec<_>>();
    write_data_to_table(&table_url, engine.clone(), data_2).await?;

    // Verify second commit
    verify_row_tracking_commit(
        &store,
        &table_url,
        2,       // commit to verify
        3,       // expected number of actions in commit
        vec![6], // expected base row IDs
        7,       // expected high watermark
    )
    .await?;

    // Verify the data can still be read correctly
    test_read(
        &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(vec![7, 8, 1, 2, 3, 4, 5, 6]))],
        )?),
        &table_url,
        engine,
    )?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_three_consecutive_transactions() -> Result<(), Box<dyn std::error::Error>>
{
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_three_transactions", schema.clone()).await?;

    // First transaction
    let data_1 = [
        (vec![1_i64], vec!["a".to_string()]),
        (
            vec![2_i64, 3_i64, 4_i64],
            vec!["b".to_string(), "c".to_string(), "d".to_string()],
        ),
        (vec![5_i64, 6_i64], vec!["e".to_string(), "f".to_string()]),
    ]
    .into_iter()
    .map(|(ids, names)| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    })
    .collect::<Vec<_>>();

    write_data_to_table(&table_url, engine.clone(), data_1).await?;
    verify_row_tracking_commit(
        &store,
        &table_url,
        1,             // commit to verify
        5,             // expected number of actions in commit
        vec![0, 1, 4], // expected base row IDs
        5,             // expected high watermark
    )
    .await?;

    // Second transaction
    let data_2 = [(vec![7_i64, 8_i64], vec!["g".to_string(), "h".to_string()])]
        .into_iter()
        .map(|(ids, names)| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        })
        .collect::<Vec<_>>();

    write_data_to_table(&table_url, engine.clone(), data_2).await?;
    verify_row_tracking_commit(
        &store,
        &table_url,
        2,       // commit to verify
        3,       // expected number of actions in commit
        vec![6], // expected base row IDs
        7,       // expected high watermark
    )
    .await?;

    // Third transaction
    let data_3 = [
        (vec![9_i64, 10_i64], vec!["i".to_string(), "j".to_string()]),
        (vec![11_i64, 12_i64], vec!["k".to_string(), "l".to_string()]),
    ]
    .into_iter()
    .map(|(ids, names)| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    })
    .collect::<Vec<_>>();

    write_data_to_table(&table_url, engine.clone(), data_3).await?;
    verify_row_tracking_commit(
        &store,
        &table_url,
        3,           // commit to verify
        4,           // expected number of actions in commit
        vec![8, 10], // expected base row IDs
        11,          // expected high watermark
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_parallel_transactions_conflict() -> Result<(), Box<dyn std::error::Error>>
{
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_parallel_row_tracking", schema.clone())
            .await?;

    let engine1 = engine.clone();
    let engine2 = engine;

    // Create two snapshots from the same initial state
    let snapshot1 = Arc::new(Snapshot::try_new(
        table_url.clone(),
        engine1.as_ref(),
        None,
    )?);
    let snapshot2 = Arc::new(Snapshot::try_new(
        table_url.clone(),
        engine2.as_ref(),
        None,
    )?);

    // Create two transactions from the same snapshot (simulating parallel transactions)
    let mut txn1 = snapshot1.transaction()?.with_engine_info("transaction 1");
    let mut txn2 = snapshot2.transaction()?.with_engine_info("transaction 2");

    // Prepare data for both transactions
    let data1 = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;
    let data2 = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![4, 5]))],
    )?;

    // Write data for both transactions
    let write_context1 = Arc::new(txn1.get_write_context());
    let write_context2 = Arc::new(txn2.get_write_context());

    let metadata1 = engine1
        .write_parquet(
            &ArrowEngineData::new(data1),
            write_context1.as_ref(),
            HashMap::new(),
            true,
        )
        .await?;

    let metadata2 = engine2
        .write_parquet(
            &ArrowEngineData::new(data2),
            write_context2.as_ref(),
            HashMap::new(),
            true,
        )
        .await?;

    txn1.add_files(metadata1);
    txn2.add_files(metadata2);

    // Commit the first transaction - this should succeed
    let result1 = txn1.commit(engine1.as_ref())?;
    match result1 {
        CommitResult::Committed { version, .. } => {
            assert_eq!(version, 1, "First transaction should commit at version 1");
        }
        CommitResult::Conflict(_, version) => {
            panic!("First transaction should not conflict, got conflict at version {version}");
        }
    }

    // Commit the second transaction - this should result in a conflict
    let result2 = txn2.commit(engine2.as_ref())?;
    match result2 {
        CommitResult::Committed { version, .. } => {
            panic!("Second transaction should conflict, but got committed at version {version}");
        }
        CommitResult::Conflict(_conflicted_txn, version) => {
            assert_eq!(version, 1, "Conflict should be at version 1");

            // TODO: In the future, we need to resolve conflicts and retry the commit
            // For now, we just verify that we got the conflict as expected
        }
    }

    // Verify that the winning transaction is in the log and that it has the correct metadata
    verify_row_tracking_commit(
        &store,
        &table_url,
        1,       // commit to verify
        3,       // expected number of actions in commit
        vec![0], // expected base row IDs
        2,       // expected high watermark
    )
    .await?;

    // Verify the data matches the winning transaction
    test_read(
        &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))], // Only data from winning transaction
        )?),
        &table_url,
        engine1,
    )?;

    Ok(())
}

fn setup_golden_table(
    table_name: &str,
) -> (
    Url,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Arc<dyn ObjectStore>,
    Option<PathBuf>,
    TempDir,
) {
    let test_dir = load_test_data("tests/golden_data", table_name).unwrap();
    let test_path = test_dir.path().join(table_name);
    let expected_path = test_path.join("expected");
    let expected_path = expected_path.exists().then_some(expected_path);

    let (store, engine, table_location) =
        engine_store_setup("delta", Some(&Url::from_directory_path(test_path).unwrap()));

    (
        table_location,
        Arc::new(engine),
        store,
        expected_path,
        test_dir,
    )
}

#[tokio::test]
async fn test_row_tracking_golden_table_write() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let schema = Arc::new(StructType::new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]));

    // Load golden table created by delta-spark
    // The table has three rows from two transactions with fresh and overwritten values similar to
    // the approach in https://github.com/delta-io/delta/pull/4924
    let (table_url, engine, store, expected, _test_dir) = setup_golden_table("row-tracking-write");

    // Append data to golden table
    let data = [
        (vec![10], vec!["a".to_string()]),
        (
            vec![11, 12, 13],
            vec!["b".to_string(), "c".to_string(), "d".to_string()],
        ),
        (vec![14, 15], vec!["e".to_string(), "f".to_string()]),
    ]
    .into_iter()
    .map(|(ids, names)| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    })
    .collect::<Vec<_>>();

    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify state of the golden table
    verify_row_tracking_commit(
        &store,
        &table_url,
        3,             // commit to verify - the table has three prior commits
        5,             // expected number of actions in commit
        vec![5, 6, 9], // expected base row IDs
        10,            // expected high watermark
    )
    .await?;

    latest_snapshot_test(Arc::try_unwrap(engine).unwrap(), table_url, expected).await
}
