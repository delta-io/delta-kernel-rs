//! End-to-end integration tests for clustered tables.
//!
//! These tests exercise the full lifecycle: create table with clustering columns,
//! write data, commit, checkpoint, and verify the data and clustering metadata
//! are preserved throughout.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::CommitResult;

use test_utils::{read_add_infos, read_scan, test_table_setup_mt, write_batch_to_table};

/// Creates a RecordBatch with (id: int, name: string, city: string) columns.
fn make_batch(
    ids: Vec<i32>,
    names: Vec<&str>,
    cities: Vec<&str>,
    arrow_schema: &Arc<ArrowSchema>,
) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn test_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("city", DataType::STRING, true),
        ])
        .unwrap(),
    )
}

fn test_arrow_schema(schema: &StructType) -> Arc<ArrowSchema> {
    Arc::new(TryFromKernel::try_from_kernel(schema).unwrap())
}

/// Full lifecycle with multiple clustering columns: create table, verify post-commit snapshot,
/// write two batches, verify stats include all clustering columns, checkpoint, and verify
/// clustering metadata and data survive the checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_clustered_table_write_and_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = test_schema();
    let arrow_schema = test_arrow_schema(&schema);
    let expected_clustering = vec![ColumnName::new(["id"]), ColumnName::new(["city"])];

    // Create table clustered on "id" and "city"
    let create_result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::Clustered {
            columns: expected_clustering.clone(),
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;
    let snapshot = match create_result {
        CommitResult::CommittedTransaction(committed) => committed
            .post_commit_snapshot()
            .expect("post-commit snapshot should exist")
            .clone(),
        other => panic!("Expected CommittedTransaction, got: {other:?}"),
    };

    // Verify clustering columns and features on post-commit snapshot
    assert_eq!(
        snapshot.get_clustering_columns(engine.as_ref())?,
        Some(expected_clustering.clone())
    );
    assert!(
        snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::ClusteredTable)
    );
    assert!(
        snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::DomainMetadata)
    );

    // First write: 3 rows
    let batch = make_batch(
        vec![1, 2, 3],
        vec!["alice", "bob", "charlie"],
        vec!["seattle", "portland", "seattle"],
        &arrow_schema,
    );
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    assert_eq!(snapshot.version(), 1);

    // Second write: 2 more rows
    let batch = make_batch(
        vec![4, 5],
        vec!["dave", "eve"],
        vec!["austin", "portland"],
        &arrow_schema,
    );
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    assert_eq!(snapshot.version(), 2);

    // Verify stats include all clustering columns
    let add_infos = read_add_infos(&snapshot, engine.as_ref())?;
    assert!(!add_infos.is_empty());
    for info in &add_infos {
        let stats = info.stats.as_ref().expect("Add action should have stats");
        for col in &expected_clustering {
            let col_name = col.to_string();
            assert!(
                stats["minValues"].get(&col_name).is_some(),
                "Stats should include minValues for clustering column '{col_name}'"
            );
            assert!(
                stats["maxValues"].get(&col_name).is_some(),
                "Stats should include maxValues for clustering column '{col_name}'"
            );
        }
    }

    // Verify data readable before checkpoint
    let scan = snapshot.clone().scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    // Checkpoint
    snapshot.checkpoint(engine.as_ref())?;

    // Load fresh snapshot from checkpoint and verify everything survived
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let fresh = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(fresh.version(), 2);
    assert_eq!(
        fresh.get_clustering_columns(engine.as_ref())?,
        Some(expected_clustering)
    );
    let scan = fresh.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    Ok(())
}

/// Writing data to a clustered table opened from a fresh snapshot (simulating a separate session
/// that did not create the table). Verifies that clustering column metadata is picked up from
/// the existing table and applied to new writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_clustered_table_write_from_fresh_snapshot(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = test_schema();
    let arrow_schema = test_arrow_schema(&schema);

    // Create table (simulates a prior session)
    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["city"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Open a fresh snapshot (as if a different process is writing)
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Write via manual transaction to exercise the snapshot -> transaction path
    let batch = make_batch(
        vec![10, 20],
        vec!["xavier", "yara"],
        vec!["denver", "miami"],
        &arrow_schema,
    );
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);
    let write_context = txn.get_write_context();
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(batch),
            &write_context,
            HashMap::new(),
        )
        .await?;
    txn.add_files(add_meta);
    let result = txn.commit(engine.as_ref())?;
    let snapshot = match result {
        CommitResult::CommittedTransaction(c) => {
            c.post_commit_snapshot().unwrap().clone()
        }
        other => panic!("Expected CommittedTransaction, got: {other:?}"),
    };

    // Verify stats include the clustering column from the existing table
    let add_infos = read_add_infos(&snapshot, engine.as_ref())?;
    for info in &add_infos {
        if let Some(stats) = &info.stats {
            if stats["numRecords"] == 2 {
                assert!(
                    stats["minValues"].get("city").is_some(),
                    "Stats should include clustering column 'city' for data written via fresh snapshot"
                );
            }
        }
    }

    Ok(())
}
