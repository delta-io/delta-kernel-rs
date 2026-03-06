//! End-to-end integration tests for clustered tables.
//!
//! These tests exercise the full lifecycle: create table with clustering columns,
//! write data, commit, checkpoint, and verify the data and clustering metadata
//! are preserved throughout.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::CommitResult;
use rstest::rstest;

use test_utils::{
    generate_batch, read_add_infos, read_scan, test_table_setup_mt, write_batch_to_table, IntoArray,
};

/// Full lifecycle: create a clustered table, write data, verify stats include clustering columns,
/// checkpoint, and verify clustering metadata and data survive. When `use_fresh_snapshot` is true,
/// the write happens via a fresh snapshot (simulating a separate session that did not create the
/// table).
#[rstest]
#[case::post_commit_snapshot(false)]
#[case::fresh_snapshot(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_clustered_table_write_and_checkpoint(
    #[case] use_fresh_snapshot: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(
        StructType::try_new(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("city", DataType::STRING, true),
        ])
        .unwrap(),
    );
    let expected_clustering = vec![ColumnName::new(["id"]), ColumnName::new(["city"])];

    // Create table clustered on "id" and "city"
    let create_result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::Clustered {
            columns: expected_clustering.clone(),
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = if use_fresh_snapshot {
        // Open a fresh snapshot (as if a different process is writing)
        let table_url = delta_kernel::try_parse_uri(&table_path)?;
        Snapshot::builder_for(table_url).build(engine.as_ref())?
    } else {
        match create_result {
            CommitResult::CommittedTransaction(committed) => committed
                .post_commit_snapshot()
                .expect("post-commit snapshot should exist")
                .clone(),
            other => panic!("Expected CommittedTransaction, got: {other:?}"),
        }
    };

    // First write: 3 rows
    let batch = generate_batch(vec![
        ("id", vec![1, 2, 3].into_array()),
        ("name", vec!["alice", "bob", "charlie"].into_array()),
        ("city", vec!["seattle", "portland", "seattle"].into_array()),
    ])?;
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    assert_eq!(snapshot.version(), 1);

    // Second write: 2 more rows
    let batch = generate_batch(vec![
        ("id", vec![4, 5].into_array()),
        ("name", vec!["dave", "eve"].into_array()),
        ("city", vec!["austin", "portland"].into_array()),
    ])?;
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
    let scan = fresh.clone().scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    // Verify stats still include clustering columns after checkpoint
    let add_infos = read_add_infos(&fresh, engine.as_ref())?;
    assert!(!add_infos.is_empty());
    for info in &add_infos {
        let stats = info.stats.as_ref().expect("Add action should have stats");
        for col in &expected_clustering {
            let col_name = col.to_string();
            assert!(
                stats["minValues"].get(&col_name).is_some(),
                "Stats should include minValues for clustering column '{col_name}' after checkpoint"
            );
            assert!(
                stats["maxValues"].get(&col_name).is_some(),
                "Stats should include maxValues for clustering column '{col_name}' after checkpoint"
            );
        }
    }

    Ok(())
}
