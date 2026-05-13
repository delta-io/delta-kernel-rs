//! Integration tests for column-mapping-aware write paths (stats, partition values, removes).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::{ColumnName, Scalar};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::{Engine, FileMeta, Snapshot};
use test_utils::table_builder::{FeatureSet, TestTableBuilder};
use test_utils::{
    assert_partition_values, assert_schema_has_field, copy_directory,
    create_table_and_load_snapshot, nested_batches, nested_schema, read_actions_from_commit,
    read_add_infos, remove_all_and_get_remove_actions, resolve_field, test_table_setup,
    write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::{
    assert_min_max_stats, get_parquet_field_id, resolve_struct_field, set_table_properties,
};

/// 1. Creates a table with the given column mapping mode
/// 2. Writes two batches of data
/// 3. Checkpoints and verifies add.stats uses physical column names in the checkpoint
/// 4. Reads a parquet footer to verify physical names/IDs
/// 5. Reads data back to verify correctness
/// 6. Removes files and verifies remove.stats matches the original add.stats
#[rstest::rstest]
#[case::cm_none(ColumnMappingMode::None)]
#[case::cm_id(ColumnMappingMode::Id)]
#[case::cm_name(ColumnMappingMode::Name)]
#[tokio::test(flavor = "multi_thread")]
async fn test_column_mapping_write(
    #[case] cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = nested_schema()?;

    let (_tmp_dir, table_path, _) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );

    // Step 1: Create table
    let mode_str = match cm_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    };
    let mut latest_snapshot = create_table_and_load_snapshot(
        &table_path,
        schema.clone(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", mode_str)],
    )?;

    // Get physical field paths for stats verification (top-level and nested)
    let cm = latest_snapshot
        .table_properties()
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    let row_number_physical = get_any_level_column_physical_name(
        latest_snapshot.schema().as_ref(),
        &ColumnName::new(["row_number"]),
        cm,
    )?
    .into_inner();
    let street_physical = get_any_level_column_physical_name(
        latest_snapshot.schema().as_ref(),
        &ColumnName::new(["address", "street"]),
        cm,
    )?
    .into_inner();

    // Step 2: Write two batches
    for data in nested_batches()? {
        latest_snapshot =
            write_batch_to_table(&latest_snapshot, engine.as_ref(), data, HashMap::new()).await?;
    }

    // Enable writeStatsAsStruct so the checkpoint contains native stats_parsed.
    // CREATE TABLE doesn't allow this property yet, so we write a metadata-update commit directly.
    latest_snapshot = set_table_properties(
        &table_path,
        &table_url,
        engine.as_ref(),
        latest_snapshot.version(),
        &[("delta.checkpoint.writeStatsAsStruct", "true")],
    )?;

    // Step 3: Checkpoint and verify add.stats uses correct column names
    let snapshot_for_checkpoint = latest_snapshot.clone();
    snapshot_for_checkpoint.checkpoint(engine.as_ref(), None)?;
    let ckpt_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let add_actions = read_add_infos(&ckpt_snapshot, engine.as_ref())?;
    let mut all_stats: Vec<_> = add_actions
        .iter()
        .filter_map(|a| a.stats.as_ref())
        .filter(|s| s.get("minValues").is_some())
        .collect();
    assert_eq!(all_stats.len(), 2, "should have stats for 2 files");
    all_stats.sort_by_key(|s| s["minValues"][&row_number_physical[0]].as_i64().unwrap());

    // Batch 1: row_number 1..3, address.street "st1".."st3"
    assert_min_max_stats(all_stats[0], &row_number_physical, 1, 3);
    assert_min_max_stats(all_stats[0], &street_physical, "st1", "st3");

    // Batch 2: row_number 4..6, address.street "st4".."st6"
    assert_min_max_stats(all_stats[1], &row_number_physical, 4, 6);
    assert_min_max_stats(all_stats[1], &street_physical, "st4", "st6");

    // Step 3b: Verify stats_parsed in scan metadata uses correct physical column names
    {
        let scan = ckpt_snapshot
            .scan_builder()
            .include_all_stats_columns()
            .build()?;
        let scan_metadata_results: Vec<_> = scan
            .scan_metadata(engine.as_ref())?
            .collect::<Result<Vec<_>, _>>()?;

        let mut stats_rows: Vec<(i64, i64, String, String)> = Vec::new();
        for sm in scan_metadata_results {
            let (data, sel) = sm.scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();

            let batch_struct = StructArray::from(batch.clone());
            let stats_parsed: &StructArray =
                resolve_struct_field(&batch_struct, &["stats_parsed".into()]);

            let min_path = |field: &[String]| -> Vec<String> {
                [&["stats_parsed".into(), "minValues".into()], field].concat()
            };
            let max_path = |field: &[String]| -> Vec<String> {
                [&["stats_parsed".into(), "maxValues".into()], field].concat()
            };
            let min_row_num: &Int64Array =
                resolve_struct_field(&batch_struct, &min_path(&row_number_physical));
            let max_row_num: &Int64Array =
                resolve_struct_field(&batch_struct, &max_path(&row_number_physical));
            let min_st: &StringArray =
                resolve_struct_field(&batch_struct, &min_path(&street_physical));
            let max_st: &StringArray =
                resolve_struct_field(&batch_struct, &max_path(&street_physical));

            for (i, &selected) in sel.iter().enumerate().take(batch.num_rows()) {
                if selected && !stats_parsed.is_null(i) {
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
    }

    // Step 4: Read parquet footer to verify physical names and native field_id
    {
        let parquet_path = &add_actions
            .first()
            .expect("should have at least one add file")
            .path;
        let parquet_url = table_url.join(parquet_path)?;
        let local_path = parquet_url.to_file_path().unwrap();

        let obj_meta = store
            .head(&Path::from_url_path(parquet_url.path())?)
            .await?;
        let file_meta = FileMeta::new(
            parquet_url,
            0, /* last_modified */
            obj_meta.size as u64,
        );
        let footer = engine.parquet_handler().read_parquet_footer(&file_meta)?;
        let footer_schema = footer.schema;

        let logical_schema = latest_snapshot.schema();
        for logical_path in [&["row_number"][..], &["address", "street"]] {
            let col = ColumnName::new(logical_path.iter().copied());
            let physical =
                get_any_level_column_physical_name(logical_schema.as_ref(), &col, cm)?.into_inner();
            assert_schema_has_field(&footer_schema, &physical);

            let field_id = get_parquet_field_id(&local_path, &physical);
            let logical_field = resolve_field(logical_schema.as_ref(), logical_path).unwrap();
            match cm_mode {
                ColumnMappingMode::Id | ColumnMappingMode::Name => {
                    let expected_id = logical_field
                        .column_mapping_id()
                        .expect("expected ColumnMappingId number")
                        as i32;
                    assert_eq!(
                        field_id,
                        Some(expected_id),
                        "parquet field_id mismatch for {logical_path:?}"
                    );
                }
                ColumnMappingMode::None => {
                    assert_eq!(
                        field_id, None,
                        "parquet field_id should not be set in None column mapping mode"
                    );
                }
            }
        }
    }

    // Step 5: Read data back to verify correctness
    {
        let post_ckpt_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = post_ckpt_snapshot.scan_builder().build()?;
        let batches: Vec<RecordBatch> = scan
            .execute(engine.clone())?
            .map(|r| {
                let data = r.unwrap();
                let arrow = ArrowEngineData::try_from_engine_data(data).unwrap();
                arrow.record_batch().clone()
            })
            .collect();

        let result_schema = batches[0].schema();
        let combined = delta_kernel::arrow::compute::concat_batches(&result_schema, &batches)?;
        assert_eq!(
            combined.num_rows(),
            6,
            "Should have 6 rows from two written batches"
        );

        // Verify logical column names and data values
        let combined_struct = StructArray::from(combined);

        // Top-level: row_number should contain [1..=6]
        let row_numbers: &Int64Array =
            resolve_struct_field(&combined_struct, &["row_number".into()]);
        let mut vals: Vec<i64> = (0..row_numbers.len())
            .map(|i| row_numbers.value(i))
            .collect();
        vals.sort();
        assert_eq!(vals, vec![1, 2, 3, 4, 5, 6]);

        // Nested: address.street should contain ["st1"..="st6"]
        let streets: &StringArray =
            resolve_struct_field(&combined_struct, &["address".into(), "street".into()]);
        let mut street_vals: Vec<&str> = (0..streets.len()).map(|i| streets.value(i)).collect();
        street_vals.sort();
        assert_eq!(street_vals, vec!["st1", "st2", "st3", "st4", "st5", "st6"]);
    }

    // Step 6: Remove files and verify remove.stats matches original add.stats
    {
        let original_add_stats: Vec<serde_json::Value> =
            add_actions.iter().filter_map(|a| a.stats.clone()).collect();
        assert!(
            !original_add_stats.is_empty(),
            "should have at least one add with stats"
        );

        let remove_actions =
            remove_all_and_get_remove_actions(&latest_snapshot, &table_url, engine.as_ref())?;
        assert!(
            !remove_actions.is_empty(),
            "Expected at least one remove action"
        );

        let remove_stats: Vec<serde_json::Value> = remove_actions
            .iter()
            .filter_map(|r| {
                r["stats"]
                    .as_str()
                    .map(|s| serde_json::from_str(s).unwrap())
            })
            .collect();
        assert_eq!(
            remove_stats, original_add_stats,
            "remove.stats should match original add.stats"
        );
    }

    Ok(())
}

/// Verifies that partitioned writes use physical column names in add.partitionValues.
#[rstest::rstest]
#[case::cm_none("./tests/data/partition_cm/none")]
#[case::cm_id("./tests/data/partition_cm/id")]
#[case::cm_name("./tests/data/partition_cm/name")]
#[tokio::test(flavor = "multi_thread")]
async fn test_column_mapping_partitioned_write(
    #[case] table_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Copy test data to a temp dir so we can write to it
    let tmp_dir = tempfile::tempdir()?;
    copy_directory(std::path::Path::new(table_dir), tmp_dir.path())?;
    let table_url = Url::from_directory_path(tmp_dir.path()).unwrap();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let cm = snapshot
        .table_properties()
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    let physical_name = get_any_level_column_physical_name(
        snapshot.schema().as_ref(),
        &ColumnName::new(["category"]),
        cm,
    )?
    .into_inner()
    .remove(0);

    // Verify physical name for column mapping mode
    if table_dir.ends_with("none") {
        assert_eq!(physical_name, "category");
    } else {
        assert_ne!(
            physical_name, "category",
            "physical name should differ from logical name under column mapping"
        );
    }

    // Write data with partition value
    let data_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);
    let batch = RecordBatch::try_new(
        Arc::new(data_schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;
    let partition_values = HashMap::from([("category".to_string(), Scalar::String("A".into()))]);
    write_batch_to_table(&snapshot, engine.as_ref(), batch, partition_values).await?;

    // Read commit log and verify add.partitionValues key uses physical name
    let add_actions = read_actions_from_commit(&table_url, 1, "add")?;
    assert!(!add_actions.is_empty(), "no add action found in commit log");
    for add in &add_actions {
        assert_partition_values(add, &physical_name, "A");
    }

    // Remove the written file and verify remove action preserves physical names
    let post_write_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let remove_actions =
        remove_all_and_get_remove_actions(&post_write_snapshot, &table_url, engine.as_ref())?;
    assert!(
        !remove_actions.is_empty(),
        "no remove action found in commit log"
    );
    for remove in &remove_actions {
        assert_partition_values(remove, &physical_name, "A");
    }

    Ok(())
}

// End-to-end: column-mapped + materializePartitionColumns + partition cols in the middle
// of the logical schema. Asserts parquet field order = data cols first then partition cols,
// and verifies checkpoint + scan round trip works as expected.
#[rstest::rstest]
#[case::cm_none(ColumnMappingMode::None)]
#[case::cm_id(ColumnMappingMode::Id)]
#[case::cm_name(ColumnMappingMode::Name)]
#[tokio::test(flavor = "multi_thread")]
async fn test_column_mapping_materialized_partition_in_middle_roundtrip(
    #[case] cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Logical schema: data cols interleaved with partition cols.
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("a", DataType::INTEGER),
        StructField::nullable("p1", DataType::STRING),
        StructField::nullable("b", DataType::INTEGER),
        StructField::nullable("p2", DataType::STRING),
        StructField::nullable("c", DataType::INTEGER),
    ])?);

    // Step 1: CREATE TABLE with column mapping + materializePartitionColumns via TestTableBuilder.
    let cm_str = match cm_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    };
    let table = TestTableBuilder::new()
        .with_schema(schema.clone())
        .with_partition_columns(["p2", "p1"])
        .with_features(
            FeatureSet::new()
                .column_mapping(cm_str)
                .with_property("delta.feature.materializePartitionColumns", "supported"),
        )
        .build()?;

    let engine = Arc::new(
        DefaultEngineBuilder::new(table.store().clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );
    let table_url = Url::parse(table.table_root())?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    // Step 2: Write one batch carrying data with the full logical schema. Materialization keeps the
    // partition columns in the parquet, sourced from the input batch via the L2P transform.
    let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "x", "x"])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["y", "y", "y"])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ],
    )?;
    let partition_values = HashMap::from([
        ("p1".to_string(), Scalar::String("x".into())),
        ("p2".to_string(), Scalar::String("y".into())),
    ]);
    let post_write_snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), batch, partition_values).await?;

    // Step 3: Validate the physical parquet layout: data cols first, partition cols last,
    // all under physical names. Field order in `footer_schema` mirrors the parquet column
    // order on disk.
    let logical_schema = snapshot.schema();
    let physical_name = |logical: &str| -> Result<String, Box<dyn std::error::Error>> {
        let mut names = get_any_level_column_physical_name(
            logical_schema.as_ref(),
            &ColumnName::new([logical]),
            cm_mode,
        )?
        .into_inner();
        Ok(names.remove(0))
    };
    let expected_physical_order = [
        physical_name("a")?,
        physical_name("b")?,
        physical_name("c")?,
        physical_name("p2")?,
        physical_name("p1")?,
    ];

    let add_actions = read_add_infos(&post_write_snapshot, engine.as_ref())?;
    assert_eq!(add_actions.len(), 1, "expected one add file");
    let parquet_url = table_url.join(&add_actions[0].path)?;
    let obj_meta = table
        .store()
        .head(&Path::from_url_path(parquet_url.path())?)
        .await?;
    let file_meta = FileMeta::new(
        parquet_url,
        0, /* last_modified */
        obj_meta.size as u64,
    );
    let footer = engine.parquet_handler().read_parquet_footer(&file_meta)?;
    let footer_field_names: Vec<&str> = footer.schema.fields().map(|f| f.name.as_str()).collect();
    assert_eq!(
        footer_field_names,
        expected_physical_order
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>(),
        "parquet field order must be data cols first, then partition cols (in \
         partition_columns order), under physical names"
    );

    // Step 4: Checkpoint, reload a fresh snapshot, scan, and assert the logical schema +
    // values survive the round-trip. Logical column names and order must come back intact.
    post_write_snapshot.checkpoint(engine.as_ref(), None)?;
    let reloaded = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let scan = reloaded.scan_builder().build()?;
    let batches: Vec<RecordBatch> = scan
        .execute(engine.clone())?
        .map(|r| {
            let arrow = ArrowEngineData::try_from_engine_data(r.unwrap()).unwrap();
            arrow.record_batch().clone()
        })
        .collect();
    let result_schema = batches[0].schema();
    let combined = delta_kernel::arrow::compute::concat_batches(&result_schema, &batches)?;
    let combined_schema = combined.schema();
    let logical_field_names: Vec<&str> = combined_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        logical_field_names,
        vec!["a", "p1", "b", "p2", "c"],
        "reloaded scan must return the logical schema field order"
    );

    let combined_struct = StructArray::from(combined);
    let a: &Int32Array = resolve_struct_field(&combined_struct, &["a".into()]);
    let p1: &StringArray = resolve_struct_field(&combined_struct, &["p1".into()]);
    let b: &Int32Array = resolve_struct_field(&combined_struct, &["b".into()]);
    let p2: &StringArray = resolve_struct_field(&combined_struct, &["p2".into()]);
    let c: &Int32Array = resolve_struct_field(&combined_struct, &["c".into()]);

    let mut rows: Vec<(i32, String, i32, String, i32)> = (0..a.len())
        .map(|i| {
            (
                a.value(i),
                p1.value(i).to_string(),
                b.value(i),
                p2.value(i).to_string(),
                c.value(i),
            )
        })
        .collect();
    rows.sort_by_key(|r| r.0);
    assert_eq!(
        rows,
        vec![
            (1, "x".to_string(), 10, "y".to_string(), 100),
            (2, "x".to_string(), 20, "y".to_string(), 200),
            (3, "x".to_string(), 30, "y".to_string(), 300),
        ],
    );

    Ok(())
}
