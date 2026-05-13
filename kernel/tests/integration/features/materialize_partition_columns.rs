//! Integration tests for the `materializePartitionColumns` writer feature.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch, StringArray, StructArray};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::expressions::{ColumnName, Scalar};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::{Engine, FileMeta, Snapshot};
use test_utils::table_builder::{FeatureSet, TestTable, TestTableBuilder};
use test_utils::{read_add_infos, write_batch_to_table};
use url::Url;

use crate::common::write_utils::resolve_struct_field;

// End-to-end: column-mapped + materializePartitionColumns table with partition cols in the middle
// of the logical schema. Asserts parquet field order: data cols first then partition cols,
// and verifies checkpoint + scan round trip works as expected.
#[rstest::rstest]
#[case::cm_none(ColumnMappingMode::None)]
#[case::cm_id(ColumnMappingMode::Id)]
#[case::cm_name(ColumnMappingMode::Name)]
#[tokio::test(flavor = "multi_thread")]
async fn test_column_mapping_materialized_partition_in_middle_roundtrip(
    #[case] cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let trigger =
        FeatureSet::new().with_property("delta.feature.materializePartitionColumns", "supported");
    assert_partition_in_middle_roundtrip(trigger, cm_mode).await
}

/// When materialization is on and the connector hands kernel a data batch that omits a partition
/// column, kernel should produce an error.
#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_write_missing_partition_columns_errors(
) -> Result<(), Box<dyn std::error::Error>> {
    let features =
        FeatureSet::new().with_property("delta.feature.materializePartitionColumns", "supported");
    assert_missing_partition_cols_errors(features).await
}

/// End-to-end roundtrip assertion against any feature set that makes the kernel materialize
/// partition columns. Logical schema is `[a, p1, b, p2, c]` with partition cols `[p2, p1]`
/// (reversed vs. schema order, to prove metadata order wins). Asserts the parquet field
/// order is `[a, b, c, p2, p1]` in parquet schema, then checkpoints, reloads, and verifies
/// the scan returns the logical schema + values.
pub(super) async fn assert_partition_in_middle_roundtrip(
    trigger: FeatureSet,
    cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let cm_str = match cm_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    };
    let (table, engine, table_url, snapshot) =
        setup_partitioned_table(trigger.column_mapping(cm_str))?;
    let schema = snapshot.schema();

    // Write one batch carrying data with the full logical schema. Materialization keeps the
    // partition columns in the parquet, sourced from the input batch via the
    // logical-to-physical transform.
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
    let post_write_snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), batch, make_partition_values()).await?;

    // Validate the physical parquet layout: data cols first, partition cols last, under
    // physical names.
    let physical_name = |logical: &str| -> Result<String, Box<dyn std::error::Error>> {
        let mut names = get_any_level_column_physical_name(
            schema.as_ref(),
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

    // Checkpoint, reload, scan, and assert the logical schema + values survive the roundtrip.
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

/// Negative assertion: when materialization is on and the connector hands kernel a data batch
/// that omits a partition column, the logical-to-physical evaluator must surface a `No such field:
/// <name>` error naming the missing partition column.
pub(super) async fn assert_missing_partition_cols_errors(
    features: FeatureSet,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, engine, _table_url, snapshot) = setup_partitioned_table(features)?;

    // Batch missing the partition columns, only data columns.
    let data_only_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("a", DataType::INTEGER),
        StructField::nullable("b", DataType::INTEGER),
        StructField::nullable("c", DataType::INTEGER),
    ])?);
    let arrow_schema = Arc::new(data_only_schema.as_ref().try_into_arrow()?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ],
    )?;

    let result =
        write_batch_to_table(&snapshot, engine.as_ref(), batch, make_partition_values()).await;
    let err = result
        .expect_err("materialized write should reject a batch that omits the partition columns");
    // The logical-to-physical transform's `Expression::column([partition_col])` resolves via
    // `extract_column`, which surfaces `Schema error: No such field: <name>` when the batch
    // lacks the column.
    let msg = format!("{err}");
    assert!(
        msg.contains("No such field") && (msg.contains("p1") || msg.contains("p2")),
        "expected a missing-field error naming a partition column; got: {msg}"
    );

    Ok(())
}

/// Logical schema used by every test in this module: `[a, p1, b, p2, c]`. `p1`/`p2` are the
/// partition columns; data columns are interleaved with them.
fn make_logical_schema() -> Result<SchemaRef, delta_kernel::Error> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::nullable("a", DataType::INTEGER),
        StructField::nullable("p1", DataType::STRING),
        StructField::nullable("b", DataType::INTEGER),
        StructField::nullable("p2", DataType::STRING),
        StructField::nullable("c", DataType::INTEGER),
    ])?))
}

/// Canonical partition-values map: `p1="x"`, `p2="y"`. Matches the literal values used in
/// `make_full_data_batch`.
fn make_partition_values() -> HashMap<String, Scalar> {
    HashMap::from([
        ("p1".to_string(), Scalar::String("x".into())),
        ("p2".to_string(), Scalar::String("y".into())),
    ])
}

/// `(table handle, multi-thread engine, table url, freshly-loaded snapshot)`. Returned by
/// [`setup_partitioned_table`].
type PartitionedTableFixture = (
    TestTable,
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    Url,
    Arc<Snapshot>,
);

/// Build a `[p2, p1]`-partitioned table with the given trigger feature(s), and return the
/// table handle, multi-threaded engine, table url, and freshly-loaded snapshot.
fn setup_partitioned_table(
    features: FeatureSet,
) -> Result<PartitionedTableFixture, Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let table = TestTableBuilder::new()
        .with_schema(make_logical_schema()?)
        .with_partition_columns(["p2", "p1"])
        .with_features(features)
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
    Ok((table, engine, table_url, snapshot))
}
