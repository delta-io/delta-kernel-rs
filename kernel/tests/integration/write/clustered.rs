//! Integration tests for checkpointing non-kernel tables and clustered-table write/stats flows.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int64Array, StringArray, StructArray};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::StructType;
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::transaction::create_table::create_table as create_table_txn;
use delta_kernel::Snapshot;
use itertools::Itertools;
use tempfile::TempDir;
use test_utils::{
    create_default_engine_mt_executor, nested_batches, nested_schema, read_actions_from_commit,
    test_table_setup, write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::{
    assert_column_mapping_mode, assert_min_max_stats, resolve_json_path, resolve_struct_field,
    set_table_properties,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_non_kernel_written_table() {
    // Table written by a non-kernel-integrated connector: 7 rows with columns (i, j, k), where
    // parquet field nullabilities differ from the Delta schema. DefaultEngine reads it, coerces
    // nullabilities to match the Delta schema, creates a checkpoint, and verifies the data is
    // unchanged.
    let source_path = std::path::Path::new("./tests/data/external-table-different-nullability");
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path().join("test-checkpoint-table");
    test_utils::copy_directory(source_path, &table_path).unwrap();

    let url = Url::from_directory_path(&table_path).unwrap();
    let store = store_from_url(&url).unwrap();
    let executor = Arc::new(
        delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ),
    );
    let engine: Arc<delta_kernel::engine::default::DefaultEngine<_>> = Arc::new(
        delta_kernel::engine::default::DefaultEngineBuilder::new(store)
            .with_task_executor(executor)
            .build(),
    );

    // Read data before checkpoint
    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap();
    let scan_before = Arc::clone(&snapshot).scan_builder().build().unwrap();
    let batches_before = test_utils::read_scan(&scan_before, engine.clone()).unwrap();

    // Create checkpoint via snapshot.checkpoint()
    snapshot.checkpoint(engine.as_ref(), None).unwrap();

    // Read data after checkpoint
    let snapshot_after = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap();
    let scan_after = snapshot_after.scan_builder().build().unwrap();
    let batches_after = test_utils::read_scan(&scan_after, engine.clone()).unwrap();

    // Verify data unchanged
    let formatted_before =
        delta_kernel::arrow::util::pretty::pretty_format_batches(&batches_before)
            .unwrap()
            .to_string();
    let formatted_after = delta_kernel::arrow::util::pretty::pretty_format_batches(&batches_after)
        .unwrap()
        .to_string();
    assert_eq!(
        formatted_before, formatted_after,
        "Row data changed after checkpoint creation!"
    );

    // Verify checkpoint file exists
    let delta_log_path = table_path.join("_delta_log");
    let has_checkpoint = std::fs::read_dir(&delta_log_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.contains(".checkpoint.parquet"))
        });
    assert!(has_checkpoint, "Expected at least one checkpoint file");
}

struct ClusteredTableSetup {
    _tmp_dir: TempDir,
    table_path: String,
    table_url: Url,
    engine: Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    snapshot: Arc<Snapshot>,
}

/// Creates a clustered table with column mapping and sets table properties.
fn setup_clustered_table(
    cm_mode: &str,
    schema: Arc<StructType>,
    clustering_cols: Vec<ColumnName>,
    table_properties: &[(&str, &str)],
) -> Result<ClusteredTableSetup, Box<dyn std::error::Error>> {
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_tmp_dir, table_path, _) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let engine = create_default_engine_mt_executor(&table_url)?;

    let _ = create_table_txn(table_url.as_str(), schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", cm_mode)])
        .with_data_layout(DataLayout::Clustered {
            columns: clustering_cols,
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = set_table_properties(
        &table_path,
        &table_url,
        engine.as_ref(),
        0,
        table_properties,
    )?;

    Ok(ClusteredTableSetup {
        _tmp_dir,
        table_path,
        table_url,
        engine,
        snapshot,
    })
}

/// E2E test: create a clustered table with column mapping, write data, and verify that
/// add.stats in the commit log contains min/max statistics for the clustering columns
/// (including a nested column).
#[rstest::rstest]
#[case::cm_none("none")]
#[case::cm_name("name")]
#[case::cm_id("id")]
#[tokio::test(flavor = "multi_thread")]
async fn test_clustered_table_write_has_stats(
    #[case] cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let clustering_cols = vec![
        ColumnName::new(["row_number"]),
        ColumnName::new(["address", "street"]),
    ];
    let setup = setup_clustered_table(
        cm_mode,
        nested_schema()?,
        clustering_cols.clone(),
        &[("delta.dataSkippingNumIndexedCols", "0")],
    )?;
    let engine = &setup.engine;
    let mut snapshot = setup.snapshot;
    for batch in nested_batches()? {
        snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    }

    let cm = assert_column_mapping_mode(&snapshot, cm_mode);
    let physical_paths: Vec<Vec<String>> = clustering_cols
        .iter()
        .map(|c| {
            get_any_level_column_physical_name(snapshot.schema().as_ref(), c, cm)
                .unwrap()
                .into_inner()
        })
        .collect();
    if cm != ColumnMappingMode::None {
        let logical_paths: Vec<Vec<&str>> = vec![vec!["row_number"], vec!["address", "street"]];
        for (phys, logical) in physical_paths.iter().zip(&logical_paths) {
            assert_ne!(
                phys.iter().map(String::as_str).collect_vec(),
                *logical,
                "physical path should differ from logical when cm={cm:?}"
            );
        }
    }

    // Resolve a non-clustering column to verify it's excluded from stats
    let non_clustering_physical = get_any_level_column_physical_name(
        snapshot.schema().as_ref(),
        &ColumnName::new(["name"]),
        cm,
    )?
    .into_inner();

    // Verify stats for each write commit (v2 and v3, since v1 is the property update).
    // Batch 1 (v2): row_number 1..3, address.street "st1".."st3"
    // Batch 2 (v3): row_number 4..6, address.street "st4".."st6"
    let expected: [(i64, i64, &str, &str); 2] = [(1, 3, "st1", "st3"), (4, 6, "st4", "st6")];
    for (version, (min_rn, max_rn, min_st, max_st)) in expected.iter().enumerate() {
        let version = (version + 2) as u64;
        let add_actions = read_actions_from_commit(&setup.table_url, version, "add")?;
        assert!(
            !add_actions.is_empty(),
            "v{version}: should have add actions"
        );

        for add in &add_actions {
            let stats: serde_json::Value = serde_json::from_str(
                add.get("stats")
                    .and_then(|s| s.as_str())
                    .expect("add action should have stats"),
            )?;
            // Clustering columns should have stats despite numIndexedCols=0
            assert_min_max_stats(&stats, &physical_paths[0], *min_rn, *max_rn);
            assert_min_max_stats(&stats, &physical_paths[1], *min_st, *max_st);

            // Non-clustering column "name" should NOT have stats
            let non_cluster_min = resolve_json_path(&stats["minValues"], &non_clustering_physical);
            assert!(
                non_cluster_min.is_null(),
                "v{version}: non-clustering column 'name' should not have stats"
            );
        }
    }

    Ok(())
}

/// E2E test: create a clustered table with column mapping, enable writeStatsAsStruct,
/// write data, checkpoint, and verify stats_parsed.
#[rstest::rstest]
#[case::cm_none("none")]
#[case::cm_name("name")]
#[case::cm_id("id")]
#[tokio::test(flavor = "multi_thread")]
async fn test_clustered_table_write_has_stats_parsed(
    #[case] cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let clustering_cols = vec![
        ColumnName::new(["row_number"]),
        ColumnName::new(["address", "street"]),
    ];
    let setup = setup_clustered_table(
        cm_mode,
        nested_schema()?,
        clustering_cols.clone(),
        &[
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.dataSkippingNumIndexedCols", "0"),
        ],
    )?;
    let engine = &setup.engine;
    let mut snapshot = setup.snapshot;
    for batch in nested_batches()? {
        snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    }

    let cm = assert_column_mapping_mode(&snapshot, cm_mode);
    let physical_paths: Vec<Vec<String>> = clustering_cols
        .iter()
        .map(|c| {
            get_any_level_column_physical_name(snapshot.schema().as_ref(), c, cm)
                .unwrap()
                .into_inner()
        })
        .collect();
    if cm != ColumnMappingMode::None {
        let logical_paths: Vec<Vec<&str>> = vec![vec!["row_number"], vec!["address", "street"]];
        for (phys, logical) in physical_paths.iter().zip(&logical_paths) {
            assert_ne!(
                phys.iter().map(String::as_str).collect_vec(),
                *logical,
                "physical path should differ from logical when cm={cm:?}"
            );
        }
    }
    let non_clustering_physical = get_any_level_column_physical_name(
        snapshot.schema().as_ref(),
        &ColumnName::new(["name"]),
        cm,
    )?
    .into_inner();

    snapshot.checkpoint(engine.as_ref(), None)?;

    // Read checkpoint parquet directly to verify stats_parsed contains only clustering columns.
    // ScanBuilder::include_all_stats_columns() doesn't support stats_parsed when
    // dataSkippingNumIndexedCols=0. Read directly from the checkpoint parquet file instead.
    use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let delta_log = std::path::Path::new(&setup.table_path).join("_delta_log");
    let ckpt_path = std::fs::read_dir(&delta_log)?
        .filter_map(|e| e.ok())
        .find(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.contains(".checkpoint.parquet"))
        })
        .expect("checkpoint parquet should exist")
        .path();
    let file = std::fs::File::open(&ckpt_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    let min_path = |field: &[String]| -> Vec<String> { [&["minValues".into()], field].concat() };
    let max_path = |field: &[String]| -> Vec<String> { [&["maxValues".into()], field].concat() };

    let mut stats_rows: Vec<(i64, i64, String, String)> = Vec::new();
    for batch in reader {
        let batch = batch?;
        let batch_struct = StructArray::from(batch);
        let add: &StructArray = resolve_struct_field(&batch_struct, &["add".into()]);
        let stats_parsed: &StructArray = resolve_struct_field(add, &["stats_parsed".into()]);

        // Non-clustering column should not appear in stats_parsed
        let min_values: &StructArray = resolve_struct_field(stats_parsed, &["minValues".into()]);
        assert!(
            min_values
                .column_by_name(&non_clustering_physical[0])
                .is_none(),
            "non-clustering column '{}' should not have stats_parsed",
            non_clustering_physical[0]
        );

        let min_row_num: &Int64Array =
            resolve_struct_field(stats_parsed, &min_path(&physical_paths[0]));
        let max_row_num: &Int64Array =
            resolve_struct_field(stats_parsed, &max_path(&physical_paths[0]));
        let min_st: &StringArray =
            resolve_struct_field(stats_parsed, &min_path(&physical_paths[1]));
        let max_st: &StringArray =
            resolve_struct_field(stats_parsed, &max_path(&physical_paths[1]));

        for i in 0..stats_parsed.len() {
            if !stats_parsed.is_null(i) {
                stats_rows.push((
                    min_row_num.value(i),
                    max_row_num.value(i),
                    min_st.value(i).to_string(),
                    max_st.value(i).to_string(),
                ));
            }
        }
    }

    stats_rows.sort_by_key(|r| r.0);
    assert_eq!(stats_rows.len(), 2, "should have stats_parsed for 2 files");
    assert_eq!(stats_rows[0], (1, 3, "st1".to_string(), "st3".to_string()));
    assert_eq!(stats_rows[1], (4, 6, "st4".to_string(), "st6".to_string()));

    Ok(())
}
