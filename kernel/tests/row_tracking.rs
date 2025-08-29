use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use object_store::{path::Path, ObjectStore};
use serde_json::{Deserializer, Value};
use tempfile::{tempdir, TempDir};
use url::Url;

use delta_kernel::arrow::array::{Int32Array, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Error, Snapshot};

use test_utils::{create_table, engine_store_setup, read_scan, test_read};

/// Helper function to create a simple table with row tracking enabled.
async fn create_row_tracking_table(
    tmp_dir: &TempDir,
    table_name: &str,
    schema: SchemaRef,
) -> DeltaResult<(
    Url,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Arc<dyn ObjectStore>,
)> {
    let tmp_test_dir_url = Url::from_directory_path(tmp_dir.path())
        .map_err(|_| Error::generic("Failed to convert directory path to URL"))?;
    let (store, engine, table_location) = engine_store_setup(table_name, Some(&tmp_test_dir_url));

    // Create table with row tracking feature enabled
    let table_url = create_table(
        store.clone(),
        table_location,
        schema,
        &[],    // no partition columns
        true,   // use 37 protocol
        vec![], // no reader features
        vec!["rowTracking"],
    )
    .await
    .map_err(|e| Error::generic(format!("Failed to create table: {e}")))?;

    Ok((table_url, Arc::new(engine), store))
}

/// Helper function to write data and return the number of records written.
async fn write_data_to_table(
    table_url: &Url,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    data: Vec<ArrowEngineData>,
) -> DeltaResult<CommitResult> {
    let snapshot = Arc::new(Snapshot::builder(table_url.clone()).build(engine.as_ref())?);
    let mut txn = snapshot.transaction()?;

    // Write data out by spawning async tasks to simulate executors
    let write_context = Arc::new(txn.get_write_context());
    let tasks = data.into_iter().map(|data| {
        let engine = engine.clone();
        let write_context = write_context.clone();
        tokio::task::spawn(async move {
            engine
                .write_parquet(&data, write_context.as_ref(), HashMap::new(), true)
                .await
        })
    });

    let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();

    for meta in add_files_metadata {
        let metadata = meta?;
        txn.add_files(metadata);
    }

    // Commit the transaction
    txn.commit(engine.as_ref())
}

/// Helper function to verify row tracking-related information in a commit.
async fn verify_row_tracking_in_commit(
    store: &Arc<dyn ObjectStore>,
    table_url: &Url,
    commit_version: u64,
    expected_base_row_ids: Vec<i64>,
    expected_row_id_high_water_mark: i64,
) -> DeltaResult<()> {
    let commit_url = table_url.join(&format!("_delta_log/{:020}.json", commit_version))?;
    let commit = store.get(&Path::from_url_path(commit_url.path())?).await?;

    let parsed_actions: Vec<_> = Deserializer::from_slice(&commit.bytes().await?)
        .into_iter::<Value>()
        .try_collect()?;

    // Extract base row IDs and default commit versions
    let (mut base_row_ids, default_commit_versions): (Vec<_>, Vec<_>) = parsed_actions
        .iter()
        .filter_map(|action| {
            action.get("add").map(|add| {
                let base_row_id = add
                    .get("baseRowId")
                    .cloned()
                    .expect("Add action should have baseRowId field")
                    .as_i64()
                    .expect("baseRowId should be an i64");
                let default_commit_version = add
                    .get("defaultRowCommitVersion")
                    .cloned()
                    .expect("Add action should have defaultRowCommitVersion field")
                    .as_i64()
                    .expect("defaultRowCommitVersion should be an i64");
                (base_row_id, default_commit_version)
            })
        })
        .unzip();
    base_row_ids.sort();

    assert_eq!(base_row_ids, expected_base_row_ids);
    assert_eq!(
        default_commit_versions,
        vec![commit_version as i64; default_commit_versions.len()]
    );

    // Extract the row ID high water mark
    let row_tracking_domain_config = parsed_actions
        .iter()
        .filter_map(|action| {
            action.get("domainMetadata").and_then(|meta| {
                let domain = meta
                    .get("domain")
                    .expect("Domain metadata must have a domain");
                match domain.as_str() {
                    Some("delta.rowTracking") => Some(
                        meta.get("configuration")
                            .expect("Domain metadata must have a configuration")
                            .as_str()
                            .expect("Configuration should be a string"),
                    ),
                    _ => None,
                }
            })
        })
        .collect::<Vec<_>>();

    assert_eq!(
        row_tracking_domain_config.len(),
        1,
        "There must be exactly one row tracking domain metadata action"
    );

    let row_id_high_water_mark = serde_json::from_str::<Value>(row_tracking_domain_config[0])?
        .get("rowIdHighWaterMark")
        .expect("rowIdHighWaterMark should be present")
        .as_i64()
        .expect("rowIdHighWaterMark should be an i64");
    assert_eq!(
        row_id_high_water_mark, expected_row_id_high_water_mark,
        "rowIdHighWaterMark should match expected value"
    );

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_append() -> DeltaResult<()> {
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
    let data: Vec<ArrowEngineData> = [[1, 2, 3], [4, 5, 6]]
        .into_iter()
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,          // commit to verify
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
async fn test_row_tracking_single_record_batches() -> DeltaResult<()> {
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
    let data: Vec<ArrowEngineData> = [[1], [2], [3]]
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,             // commit to verify
        vec![0, 1, 2], // expected base row IDs
        2,             // expected high watermark
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_large_batch() -> DeltaResult<()> {
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
    let data: Vec<ArrowEngineData> = [large_batch.clone()]
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,       // commit to verify
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
async fn test_row_tracking_consecutive_transactions() -> DeltaResult<()> {
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
    let data_1: Vec<ArrowEngineData> = [[1, 2, 3], [4, 5, 6]]
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data_1).await?;

    // Verify first commit
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,          // commit to verify
        vec![0, 3], // expected base row IDs
        5,          // expected high watermark
    )
    .await?;

    // Second transaction: write one batch with 2 records
    // This should read the existing row tracking domain metadata and assign base row IDs starting from 6
    let data_2: Vec<ArrowEngineData> = [[7, 8]]
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data_2).await?;

    // Verify second commit
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        2,       // commit to verify
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
async fn test_row_tracking_three_consecutive_transactions() -> DeltaResult<()> {
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
    .map(|(ids, names)| -> DeltaResult<ArrowEngineData> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        Ok(ArrowEngineData::new(batch))
    })
    .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data_1).await?;

    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,             // commit to verify
        vec![0, 1, 4], // expected base row IDs
        5,             // expected high watermark
    )
    .await?;

    // Second transaction
    let data_2: Vec<ArrowEngineData> =
        [(vec![7_i64, 8_i64], vec!["g".to_string(), "h".to_string()])]
            .into_iter()
            .map(|(ids, names)| -> DeltaResult<ArrowEngineData> {
                let batch = RecordBatch::try_new(
                    Arc::new(schema.as_ref().try_into_arrow()?),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(StringArray::from(names)),
                    ],
                )?;
                Ok(ArrowEngineData::new(batch))
            })
            .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data_2).await?;

    verify_row_tracking_in_commit(
        &store,
        &table_url,
        2,       // commit to verify
        vec![6], // expected base row IDs
        7,       // expected high watermark
    )
    .await?;

    // Third transaction
    let data_3: Vec<ArrowEngineData> = [
        (vec![9_i64, 10_i64], vec!["i".to_string(), "j".to_string()]),
        (vec![11_i64, 12_i64], vec!["k".to_string(), "l".to_string()]),
    ]
    .into_iter()
    .map(|(ids, names)| -> DeltaResult<ArrowEngineData> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        Ok(ArrowEngineData::new(batch))
    })
    .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data_3).await?;

    verify_row_tracking_in_commit(
        &store,
        &table_url,
        3,           // commit to verify
        vec![8, 10], // expected base row IDs
        11,          // expected high watermark
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_with_regular_and_empty_adds() -> DeltaResult<()> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_append", schema.clone()).await?;

    // Create two regular and one empty arrow record batches to append
    let data: Vec<ArrowEngineData> = [vec![1, 2, 3], Vec::<i32>::new(), vec![4, 5, 6]]
        .into_iter()
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,             // commit to verify
        vec![0, 3, 3], // expected base row IDs
        5,             // expected high watermark
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
async fn test_row_tracking_with_empty_adds() -> DeltaResult<()> {
    // Setup
    let _ = tracing_subscriber::fmt::try_init();
    let tmp_test_dir = tempdir()?;
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    let (table_url, engine, store) =
        create_row_tracking_table(&tmp_test_dir, "test_append", schema.clone()).await?;

    // Create two new _empty_ arrow record batches to append
    let data: Vec<ArrowEngineData> = [Vec::<i32>::new(), Vec::<i32>::new()]
        .into_iter()
        .map(|data| -> DeltaResult<ArrowEngineData> {
            let batch = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data))],
            )?;
            Ok(ArrowEngineData::new(batch))
        })
        .collect::<Result<Vec<_>, _>>()?;
    write_data_to_table(&table_url, engine.clone(), data).await?;

    // Verify the commit was written correctly
    // NB: The expected high water mark is a bit unintuitive here, as we are appending empty batches.
    // Appending empty batches means that we assign the same base row ID multiple times and that the
    // high water mark is lower than the last assigned base row ID (because that base row ID has no
    // actual row attached to it).
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,          // commit to verify
        vec![0, 0], // expected base row IDs
        -1,         // expected high watermark
    )
    .await?;

    // Verify that the table is empty
    let snapshot = Snapshot::builder(table_url).build(engine.as_ref())?;
    let scan = snapshot.into_scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;

    assert!(batches.is_empty(), "Table should be empty");

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_without_adds() -> DeltaResult<()> {
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
    let snapshot = Arc::new(Snapshot::builder(table_url.clone()).build(engine.as_ref())?);
    let txn = snapshot.transaction()?;

    // Commit without adding any add files
    txn.commit(engine.as_ref())?;

    // Fetch and parse the commit
    let commit_url = table_url.join(&format!("_delta_log/{:020}.json", 1))?;
    let commit = store.get(&Path::from_url_path(commit_url.path())?).await?;

    let parsed_actions: Vec<_> = Deserializer::from_slice(&commit.bytes().await?)
        .into_iter::<Value>()
        .try_collect()?;

    // Verify that there only is a commit info action
    // NOTE: We specifically test that we don't write domain metadata for commits without actual data
    assert_eq!(parsed_actions.len(), 1, "Expected only one action");
    assert!(parsed_actions[0].get("commitInfo").is_some());

    Ok(())
}

#[tokio::test]
async fn test_row_tracking_parallel_transactions_conflict() -> DeltaResult<()> {
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
    let snapshot1 = Arc::new(Snapshot::builder(table_url.clone()).build(engine1.as_ref())?);
    let snapshot2 = Arc::new(Snapshot::builder(table_url.clone()).build(engine2.as_ref())?);

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
    verify_row_tracking_in_commit(
        &store,
        &table_url,
        1,       // commit to verify
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
