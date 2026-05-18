//! Integration tests for [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load)
//! and DV masking via [`LoadSink::dv_ref`].

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
use delta_kernel::arrow::array::AsArray;
use delta_kernel::expressions::{ColumnName, Scalar, StructData};
use delta_kernel::plans::ir::nodes::{DvRef, FileType, RelationHandle, ScanFileColumns, SinkType};
use delta_kernel::plans::ir::{PlanBuilder, RelationRegistry};
use delta_kernel::schema::{DataType, StructField, StructType, ToSchema};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use test_utils::parquet::write_i64_parquet;
use url::Url;
use uuid::Uuid;

fn col(name: &str) -> ColumnName {
    ColumnName::from_naive_str_split(name)
}

/// `ScanFileColumns` with only the upstream `path` column wired (no size/count). Matches the
/// shape used by every `LoadSink` test in this file.
fn scan_file_path_only() -> ScanFileColumns {
    ScanFileColumns {
        path: col("path"),
        size: None,
        record_count: None,
    }
}

/// Resolve `kernel/tests/data/table-with-dv-small` to a base directory URL. The DV fixture is
/// reused by multiple `LoadSink` tests.
fn dv_small_fixture_base_url() -> Url {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = std::fs::canonicalize(manifest_dir.join("../kernel/tests/data/table-with-dv-small"))
        .unwrap();
    Url::from_directory_path(&root).unwrap()
}

/// Sum `num_rows` across every batch registered under `handle` after the producing plan has run.
async fn relation_row_count(executor: &DataFusionExecutor, handle: &RelationHandle) -> usize {
    executor
        .collect_relation(handle)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

#[tokio::test]
async fn load_sink_broadcasts_passthrough_columns() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("data.parquet");
    write_i64_parquet(&parquet_path, "x", &[1_i64, 2_i64]);

    let rel_path = parquet_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let base_url = Url::from_directory_path(dir.path()).unwrap();

    let upstream_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("tag", DataType::STRING),
        ])
        .unwrap(),
    );

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());

    let lit = PlanBuilder::values(
        upstream_schema,
        vec![
            vec![
                Scalar::String(rel_path.clone()),
                Scalar::String("alpha".into()),
            ],
            vec![Scalar::String(rel_path), Scalar::String("beta".into())],
        ],
    )
    .unwrap();

    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let producer_plan = lit
        .load(
            "loaded",
            file_schema,
            FileType::Parquet,
            Some(base_url),
            vec![col("tag")],
            scan_file_path_only(),
            None,
            &mut registry,
        )
        .expect("load sink");
    let SinkType::Load(load_sink) = &producer_plan.sink else {
        unreachable!("PlanBuilder::load always produces SinkType::Load");
    };
    let handle = load_sink.output_relation.clone();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer_plan]).await.unwrap();
    assert_eq!(relation_row_count(&executor, &handle).await, 4);

    let batches = executor.collect_relation(&handle).await.unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4);

    let idx_x = batches[0].schema().column_with_name("x").unwrap().0;
    let idx_tag = batches[0].schema().column_with_name("tag").unwrap().0;

    let mut pairs = Vec::new();
    for b in &batches {
        let xa = b
            .column(idx_x)
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
        let ta = b.column(idx_tag).as_string::<i32>();
        for r in 0..b.num_rows() {
            pairs.push((xa.value(r), ta.value(r).to_string()));
        }
    }
    pairs.sort_by_key(|p| (p.0, p.1.clone()));
    assert_eq!(
        pairs,
        vec![
            (1, "alpha".into()),
            (1, "beta".into()),
            (2, "alpha".into()),
            (2, "beta".into()),
        ]
    );
}

#[tokio::test]
async fn load_sink_without_dv_reads_full_parquet_row_group() {
    let base_url = dv_small_fixture_base_url();
    let path_str =
        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet".to_string();

    let upstream_schema =
        Arc::new(StructType::try_new([StructField::not_null("path", DataType::STRING)]).unwrap());

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("value", DataType::INTEGER)]).unwrap());

    let lit = PlanBuilder::values(upstream_schema, vec![vec![Scalar::String(path_str)]]).unwrap();

    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let producer_plan = lit
        .load(
            "dv_loaded",
            Arc::clone(&file_schema),
            FileType::Parquet,
            Some(base_url),
            Vec::new(),
            scan_file_path_only(),
            None,
            &mut registry,
        )
        .expect("load sink");
    let SinkType::Load(load_sink) = &producer_plan.sink else {
        unreachable!("PlanBuilder::load always produces SinkType::Load");
    };
    let handle = load_sink.output_relation.clone();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer_plan]).await.unwrap();
    assert_eq!(relation_row_count(&executor, &handle).await, 10);

    let batches = executor.collect_relation(&handle).await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 10);
}

fn dv_example_scalar() -> Scalar {
    let fields: Vec<_> = DeletionVectorDescriptor::to_schema()
        .fields()
        .cloned()
        .collect();
    Scalar::Struct(
        StructData::try_new(
            fields,
            vec![
                Scalar::String("u".into()),
                Scalar::String("vBn[lx{q8@P<9BNH/isA".into()),
                Scalar::Integer(1),
                Scalar::Integer(36),
                Scalar::Long(2),
            ],
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn load_sink_applies_dv_ref_masking_from_descriptor_column() {
    let base_url = dv_small_fixture_base_url();
    let path_str =
        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet".to_string();

    let dv_cn = col("dv");

    let upstream_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                dv_cn.to_string(),
                DataType::Struct(Box::new(DeletionVectorDescriptor::to_schema())),
            ),
        ])
        .unwrap(),
    );

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("value", DataType::INTEGER)]).unwrap());

    let lit = PlanBuilder::values(
        upstream_schema,
        vec![vec![Scalar::String(path_str), dv_example_scalar()]],
    )
    .unwrap();

    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let producer_plan = lit
        .load(
            "dv_masked",
            Arc::clone(&file_schema),
            FileType::Parquet,
            Some(base_url),
            Vec::new(),
            scan_file_path_only(),
            Some(DvRef::skip(dv_cn)),
            &mut registry,
        )
        .expect("load sink");
    let SinkType::Load(load_sink) = &producer_plan.sink else {
        unreachable!("PlanBuilder::load always produces SinkType::Load");
    };
    let handle = load_sink.output_relation.clone();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer_plan]).await.unwrap();
    assert_eq!(relation_row_count(&executor, &handle).await, 8);

    let batches = executor.collect_relation(&handle).await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 8);
}

#[tokio::test]
async fn load_sink_reads_ndjson_with_matching_schema() {
    let dir = tempfile::tempdir().unwrap();
    let jpath = dir.path().join("rows.ndjson");
    std::fs::write(&jpath, "{\"y\":10}\n{\"y\":20}\n").unwrap();

    let rel = jpath.file_name().unwrap().to_str().unwrap().to_string();
    let base_url = Url::from_directory_path(dir.path()).unwrap();

    let upstream_schema =
        Arc::new(StructType::try_new([StructField::not_null("path", DataType::STRING)]).unwrap());

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("y", DataType::LONG)]).unwrap());

    let lit = PlanBuilder::values(upstream_schema, vec![vec![Scalar::String(rel)]]).unwrap();

    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let producer_plan = lit
        .load(
            "json_loaded",
            Arc::clone(&file_schema),
            FileType::Json,
            Some(base_url),
            Vec::new(),
            scan_file_path_only(),
            None,
            &mut registry,
        )
        .expect("load sink");
    let SinkType::Load(load_sink) = &producer_plan.sink else {
        unreachable!("PlanBuilder::load always produces SinkType::Load");
    };
    let handle = load_sink.output_relation.clone();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer_plan]).await.unwrap();
    assert_eq!(relation_row_count(&executor, &handle).await, 2);

    let batches = executor.collect_relation(&handle).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    let idx_y = batches[0].schema().column_with_name("y").unwrap().0;
    let mut ys = Vec::new();
    for b in &batches {
        ys.extend(
            b.column(idx_y)
                .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
                .values()
                .iter()
                .copied(),
        );
    }
    ys.sort_unstable();
    assert_eq!(ys, vec![10_i64, 20_i64]);
}

/// [`crate::exec::LoadExec`] must yield each kernel-handler parquet batch as soon as it is
/// produced -- no per-file batching, no full-file materialization. Drive a single-upstream-row
/// load over a parquet file that contains multiple row groups, then assert that the output
/// stream yielded strictly more batches than the upstream had rows. The pre-refactor `drain_load`
/// path would have concatenated everything into a single output `RecordBatch` per upstream row,
/// so this check directly proves the new streaming shape.
#[tokio::test]
async fn load_exec_streams_one_parquet_row_group_per_batch() {
    use std::fs::File;

    use delta_kernel::arrow::array::{Int64Array, RecordBatch as ArrowRecordBatch};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
    use delta_kernel::parquet::file::properties::WriterProperties;

    // Write 4 row groups of 16 rows each (64 rows total) into one parquet file. Default
    // `read_parquet_files` semantics surface one Arrow batch per row group.
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("multi_rg.parquet");
    {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "x",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = ArrowRecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..64_i64))],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(16))
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let rel = parquet_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let base_url = Url::from_directory_path(dir.path()).unwrap();

    let upstream_schema =
        Arc::new(StructType::try_new([StructField::not_null("path", DataType::STRING)]).unwrap());
    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());

    // Single upstream row: only one file, but it has 4 row groups -> we expect 4 streamed batches.
    let lit = PlanBuilder::values(upstream_schema, vec![vec![Scalar::String(rel)]]).unwrap();
    let mut registry = RelationRegistry::new(Uuid::new_v4());
    let producer_plan = lit
        .load(
            "multi_rg_loaded",
            Arc::clone(&file_schema),
            FileType::Parquet,
            Some(base_url),
            Vec::new(),
            scan_file_path_only(),
            None,
            &mut registry,
        )
        .expect("load sink");
    let SinkType::Load(load_sink) = &producer_plan.sink else {
        unreachable!("PlanBuilder::load always produces SinkType::Load");
    };
    let handle = load_sink.output_relation.clone();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer_plan]).await.unwrap();

    let batches = executor.collect_relation(&handle).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 64, "row count must match the file's row count");
    assert!(
        batches.len() > 1,
        "LoadExec must yield one batch per parquet row group (got {} batch(es) from a 4-RG file); \
         a single batch here means LoadExec eagerly concatenates per file",
        batches.len(),
    );
}

