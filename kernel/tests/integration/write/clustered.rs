//! Integration tests for checkpointing non-kernel tables and clustered-table write/stats flows.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES};
use delta_kernel::arrow::array::{Array, Int64Array, StringArray, StructArray};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{column_name, ArrayData, ColumnName, MapData, Scalar};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::schema::{schema_ref, ArrayType, DataType, MapType, StructField, StructType};
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::transaction::create_table::create_table as create_table_txn;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::{
    Transaction, UpdateTableOperation, CLUSTERING_PROVIDER_NAME, TAGS_NAME,
};
use delta_kernel::{EngineData as _, Snapshot};
use itertools::Itertools;
use serde_json::{json, Value};
use tempfile::TempDir;
use test_utils::delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    assert_result_error_with_message, create_add_files_metadata, create_default_engine_mt_executor,
    into_record_batch, nested_batches, nested_schema, read_actions_from_commit, test_table_setup,
    write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::{
    assert_column_mapping_mode, assert_min_max_stats, get_simple_schema, resolve_json_path,
    resolve_struct_field, set_table_properties, simple_id_batch,
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
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(
        test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ),
    );
    let engine: Arc<test_utils::delta_kernel_default_engine::DefaultEngine<_>> = Arc::new(
        test_utils::delta_kernel_default_engine::DefaultEngineBuilder::new(store)
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
    let (_tmp_dir, table_path, _) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let engine = create_default_engine_mt_executor(&table_url)?;

    let _ = create_table_txn(table_url.as_str(), schema, "Test/1.0", true)
        .with_table_properties([("delta.columnMapping.mode", cm_mode)])
        .with_data_layout(DataLayout::Clustered {
            columns: clustering_cols,
        })
        .build_with_filesystem_committer(engine.as_ref())?
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

    let clustering_cols = vec![column_name!("row_number"), column_name!("address.street")];
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
    let non_clustering_physical =
        get_any_level_column_physical_name(snapshot.schema().as_ref(), &column_name!("name"), cm)?
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
            let non_cluster_min = resolve_json_path(&stats[MIN_VALUES], &non_clustering_physical);
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

    let clustering_cols = vec![column_name!("row_number"), column_name!("address.street")];
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
    let non_clustering_physical =
        get_any_level_column_physical_name(snapshot.schema().as_ref(), &column_name!("name"), cm)?
            .into_inner();

    snapshot.checkpoint(engine.as_ref(), None)?;

    // Read checkpoint parquet directly to verify stats_parsed contains only clustering columns.
    // `ScanBuilder::with_stats(StatsOptions::all())` doesn't support stats_parsed when
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

    let min_path = |field: &[String]| -> Vec<String> { [&[MIN_VALUES.into()], field].concat() };
    let max_path = |field: &[String]| -> Vec<String> { [&[MAX_VALUES.into()], field].concat() };

    let mut stats_rows: Vec<(i64, i64, String, String)> = Vec::new();
    for batch in reader {
        let batch = batch?;
        let batch_struct = StructArray::from(batch);
        let add: &StructArray = resolve_struct_field(&batch_struct, &["add".into()]);
        let stats_parsed: &StructArray = resolve_struct_field(add, &["stats_parsed".into()]);

        // Non-clustering column should not appear in stats_parsed
        let min_values: &StructArray = resolve_struct_field(stats_parsed, &[MIN_VALUES.into()]);
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

struct NewFilesSetup {
    _tmp_dir: TempDir,
    table_url: Url,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    txn: Transaction,
    add_files_metadata: RecordBatch,
}

/// Creates a table clustered on `id` and writes one data file per entry of `file_ids`.
/// Returns the transaction and its file metadata without staging the files.
async fn write_new_files_to_clustered_table(
    row_tracking: bool,
    file_ids: Vec<Vec<i32>>,
) -> Result<NewFilesSetup, Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let schema = get_simple_schema();
    let table_properties: &[(&str, &str)] = if row_tracking {
        &[("delta.enableRowTracking", "true")]
    } else {
        &[]
    };
    let snapshot = create_table_txn(table_url.as_str(), schema.clone(), "Test/1.0", true)
        .with_table_properties(table_properties.iter().copied())
        .with_data_layout(DataLayout::Clustered {
            columns: vec![column_name!("id")],
        })
        .build_with_filesystem_committer(engine.as_ref())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let txn = snapshot
        .update_table(UpdateTableOperation::Write, "Test/1.0", true)
        .build_with_filesystem_committer(engine.as_ref())?;
    let write_state = txn.write_state()?;
    let write_context = write_state.write_context_builder().build()?;
    let mut file_metadata = Vec::new();
    for ids in file_ids {
        let data = ArrowEngineData::new(simple_id_batch(&schema, ids));
        let metadata = engine.write_parquet(&data, &write_context).await?;
        file_metadata.push(into_record_batch(metadata));
    }
    let add_files_metadata = concat_batches(&file_metadata[0].schema(), &file_metadata)?;
    Ok(NewFilesSetup {
        _tmp_dir,
        table_url,
        engine,
        txn,
        add_files_metadata,
    })
}

#[rstest::rstest]
#[tokio::test]
async fn test_add_file_tags_and_clustering_provider_written_to_add_actions(
    #[values(false, true)] row_tracking: bool,
    #[values(false, true)] tags: bool,
    #[values(false, true)] clustering_provider: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let NewFilesSetup {
        _tmp_dir,
        table_url,
        engine,
        mut txn,
        add_files_metadata,
    } = write_new_files_to_clustered_table(row_tracking, vec![vec![1, 2], vec![3], vec![4]])
        .await?;

    let mut fields = Vec::new();
    let mut columns = Vec::new();
    if tags {
        let tags_type = MapType::new(DataType::STRING, DataType::STRING, true);
        fields.push(StructField::nullable(TAGS_NAME, tags_type.clone()));
        columns.push(ArrayData::try_new(
            ArrayType::new(tags_type.clone(), true),
            [
                Scalar::Map(MapData::try_new(
                    tags_type.clone(),
                    [("a", Scalar::from("1")), ("b", Scalar::from("2"))],
                )?),
                Scalar::Map(MapData::try_new(
                    tags_type.clone(),
                    [("a", Scalar::Null(DataType::STRING))],
                )?),
                Scalar::Null(tags_type.into()),
            ],
        )?);
    }
    if clustering_provider {
        fields.push(StructField::nullable(
            CLUSTERING_PROVIDER_NAME,
            DataType::STRING,
        ));
        columns.push(ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            [
                Scalar::from("liquid"),
                Scalar::Null(DataType::STRING),
                Scalar::from("liquid"),
            ],
        )?);
    }
    let add_files_metadata = ArrowEngineData::new(add_files_metadata);
    txn.add_files(if fields.is_empty() {
        Box::new(add_files_metadata)
    } else {
        add_files_metadata.append_columns(Arc::new(StructType::try_new(fields)?), columns)?
    });
    let version = txn
        .commit(engine.as_ref())?
        .unwrap_committed()
        .commit_version();

    let adds = read_actions_from_commit(&table_url, version, "add")?;
    let read_back = |name: &str| adds.iter().map(|add| add.get(name).cloned()).collect_vec();
    let expected = |enabled: bool, values: [Option<Value>; 3]| {
        if enabled {
            values.to_vec()
        } else {
            vec![None; 3]
        }
    };
    assert_eq!(
        read_back("tags"),
        expected(
            tags,
            [
                Some(json!({"a": "1", "b": "2"})),
                Some(json!({"a": null})),
                None
            ]
        )
    );
    assert_eq!(
        read_back("clusteringProvider"),
        expected(
            clustering_provider,
            [Some(json!("liquid")), None, Some(json!("liquid"))]
        )
    );
    for add in &adds {
        assert_eq!(add["baseRowId"].is_i64(), row_tracking);
    }
    Ok(())
}

#[rstest::rstest]
#[tokio::test]
async fn test_add_files_with_mixed_optional_columns(
    #[values(false, true)] row_tracking: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let NewFilesSetup {
        _tmp_dir,
        table_url,
        engine,
        mut txn,
        add_files_metadata,
    } = write_new_files_to_clustered_table(row_tracking, vec![vec![1], vec![2], vec![3], vec![4]])
        .await?;

    let shapes = [(false, false), (true, false), (false, true), (true, true)];
    for (row, (tags, provider)) in shapes.into_iter().enumerate() {
        let metadata = ArrowEngineData::new(add_files_metadata.slice(row, 1));
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        if tags {
            let tags_type = MapType::new(DataType::STRING, DataType::STRING, true);
            fields.push(StructField::nullable(TAGS_NAME, tags_type.clone()));
            columns.push(ArrayData::try_new(
                ArrayType::new(tags_type.clone(), true),
                [Scalar::Map(MapData::try_new(
                    tags_type,
                    [("key", "value")],
                )?)],
            )?);
        }
        if provider {
            fields.push(StructField::nullable(
                CLUSTERING_PROVIDER_NAME,
                DataType::STRING,
            ));
            columns.push(ArrayData::try_new(
                ArrayType::new(DataType::STRING, true),
                [Scalar::from("liquid")],
            )?);
        }
        txn.add_files(if fields.is_empty() {
            Box::new(metadata)
        } else {
            metadata.append_columns(Arc::new(StructType::try_new(fields)?), columns)?
        });
    }
    let version = txn
        .commit(engine.as_ref())?
        .unwrap_committed()
        .commit_version();
    let adds = read_actions_from_commit(&table_url, version, "add")?;
    assert_eq!(adds.len(), shapes.len());
    for (row, (add, (tags, provider))) in adds.iter().zip(shapes).enumerate() {
        assert_eq!(
            add.get("tags"),
            tags.then(|| json!({"key": "value"})).as_ref()
        );
        assert_eq!(
            add.get("clusteringProvider"),
            provider.then(|| json!("liquid")).as_ref()
        );
        assert_eq!(
            add.get("baseRowId"),
            row_tracking.then(|| json!(row)).as_ref()
        );
        assert_eq!(
            add.get("defaultRowCommitVersion"),
            row_tracking.then(|| json!(version)).as_ref()
        );
        assert_eq!(add["dataChange"], true);
    }
    Ok(())
}

#[rstest::rstest]
#[case::absent(None)]
#[case::null(Some([None, None]))]
#[case::populated(Some([None, Some("liquid")]))]
#[case::empty_string(Some([None, Some("")]))]
fn test_add_file_clustering_provider_on_unclustered_table(
    #[values(false, true)] create_transaction: bool,
    #[values(false, true)] row_tracking: bool,
    #[case] providers: Option<[Option<&str>; 2]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let properties = if row_tracking {
        vec![("delta.enableRowTracking", "true")]
    } else {
        vec![]
    };
    let mut create = create_table_txn(table_url.as_str(), get_simple_schema(), "Test/1.0", true)
        .with_table_properties(properties)
        .build_with_filesystem_committer(engine.as_ref())?;
    let mut metadata = create_add_files_metadata(
        create.add_files_schema(),
        vec![
            ("one.parquet", 10, 0, Some(1)),
            ("two.parquet", 10, 0, Some(1)),
        ],
    )?;
    let tags_type = MapType::new(DataType::STRING, DataType::STRING, true);
    metadata = metadata.append_columns(
        schema_ref! { nullable TAGS_NAME: { STRING => nullable STRING } },
        vec![ArrayData::try_new(
            ArrayType::new(tags_type.clone(), true),
            vec![Scalar::Map(MapData::try_new(tags_type, [("key", "value")])?); 2],
        )?],
    )?;
    if let Some(values) = providers {
        metadata = metadata.append_columns(
            schema_ref! { nullable CLUSTERING_PROVIDER_NAME: STRING },
            vec![ArrayData::try_new(
                ArrayType::new(DataType::STRING, true),
                values.map(|value| {
                    value
                        .map(Scalar::from)
                        .unwrap_or(Scalar::Null(DataType::STRING))
                }),
            )?],
        )?;
    }

    let result = if create_transaction {
        create.add_files(metadata);
        create
            .commit(engine.as_ref())
            .map(|result| result.unwrap_committed().commit_version())
    } else {
        let snapshot = create
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "Test/1.0", true)
            .build_with_filesystem_committer(engine.as_ref())?;
        txn.add_files(metadata);
        txn.commit(engine.as_ref())
            .map(|result| result.unwrap_committed().commit_version())
    };
    if providers.is_some_and(|values| values.iter().any(Option::is_some)) {
        assert_result_error_with_message(
            result,
            "clusteringProvider requires the 'clustering' writer feature",
        );
    } else {
        let adds = read_actions_from_commit(&table_url, result?, "add")?;
        assert_eq!(adds.len(), 2);
        for add in adds {
            assert_eq!(add["tags"], json!({"key": "value"}));
            assert!(add.get("clusteringProvider").is_none());
        }
    }
    Ok(())
}

#[rstest::rstest]
#[tokio::test]
async fn test_add_file_reversed_optional_columns_rejected_at_commit(
    #[values(false, true)] row_tracking: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let NewFilesSetup {
        _tmp_dir,
        engine,
        mut txn,
        add_files_metadata,
        ..
    } = write_new_files_to_clustered_table(row_tracking, vec![vec![1]]).await?;
    let schema = schema_ref! {
        nullable CLUSTERING_PROVIDER_NAME: STRING,
        nullable TAGS_NAME: { STRING => nullable STRING },
    };
    let columns = schema
        .fields()
        .map(|field| {
            ArrayData::try_new(
                ArrayType::new(field.data_type().clone(), true),
                [Scalar::Null(field.data_type().clone())],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = ArrowEngineData::new(add_files_metadata).append_columns(schema, columns)?;
    txn.add_files(metadata);
    assert_result_error_with_message(
        txn.commit(engine.as_ref()),
        "Expected schema field 'clusteringProvider' is out of order",
    );
    Ok(())
}

#[rstest::rstest]
#[case::tags_not_map(TAGS_NAME, DataType::STRING)]
#[case::provider_not_string(CLUSTERING_PROVIDER_NAME, DataType::LONG)]
#[case::tags_key_not_string(
    TAGS_NAME,
    DataType::from(MapType::new(DataType::LONG, DataType::STRING, true))
)]
#[case::tags_value_not_string(
    TAGS_NAME,
    DataType::from(MapType::new(DataType::STRING, DataType::LONG, true))
)]
#[tokio::test]
async fn test_add_file_optional_column_wrong_type_rejected(
    #[values(false, true)] row_tracking: bool,
    #[case] name: &str,
    #[case] data_type: DataType,
) -> Result<(), Box<dyn std::error::Error>> {
    let NewFilesSetup {
        _tmp_dir,
        engine,
        mut txn,
        add_files_metadata,
        ..
    } = write_new_files_to_clustered_table(row_tracking, vec![vec![1]]).await?;
    let metadata = ArrowEngineData::new(add_files_metadata).append_columns(
        Arc::new(StructType::try_new([StructField::nullable(
            name,
            data_type.clone(),
        )])?),
        vec![ArrayData::try_new(
            ArrayType::new(data_type.clone(), true),
            [Scalar::Null(data_type)],
        )?],
    )?;
    txn.add_files(metadata);
    assert!(
        txn.commit(engine.as_ref()).is_err(),
        "accepted wrong type for {name}"
    );
    Ok(())
}
