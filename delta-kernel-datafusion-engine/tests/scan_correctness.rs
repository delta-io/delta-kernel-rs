//! Integration coverage for [`delta_kernel_datafusion_engine::compile::scan`] lowering:
//! per-file row indices, unordered scans with row-index columns, and scan predicates vs a direct
//! parquet read + kernel filter reference.

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::run_to_batches as scan_collect;
use delta_kernel::arrow::array::{Array, AsArray, BooleanArray};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::{column_expr, Expression};
use delta_kernel::plans::ir::nodes::{FileType, ScanNode};
use delta_kernel::plans::ir::PlanBuilder;
use delta_kernel::schema::{DataType as KernelDataType, MetadataColumnSpec, StructType};
use delta_kernel::{EvaluationHandler, FileMeta};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use test_utils::parquet::{file_meta, write_i64_parquet};
use test_utils::schemas::single_long_schema;

fn read_parquet_batches(path: &Path) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    reader.into_iter().map(|r| r.unwrap()).collect()
}

/// Matches [`delta_kernel_datafusion_engine::exec::KernelFilterExec`] batch semantics (NULL
/// predicates keep rows).
fn filter_batches_kernel_semantics(
    batches: &[RecordBatch],
    kernel_schema: Arc<StructType>,
    predicate: Arc<Expression>,
) -> Vec<RecordBatch> {
    let evaluator = ArrowEvaluationHandler
        .new_expression_evaluator(kernel_schema, predicate, KernelDataType::BOOLEAN)
        .unwrap();

    let mut out = Vec::new();
    for batch in batches {
        let batch_for_eval = ArrowEngineData::new(batch.clone());
        let pred_data = evaluator.evaluate(&batch_for_eval).unwrap();
        let pred_batch = pred_data.try_into_record_batch().unwrap();
        let predicate_arr = pred_batch.column(0).as_boolean();
        let mask = BooleanArray::from_iter((0..predicate_arr.len()).map(|i| {
            Some(if predicate_arr.is_null(i) {
                true
            } else {
                predicate_arr.value(i)
            })
        }));
        let filtered = filter_record_batch(batch, &mask).unwrap();
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    out
}

fn batch_column_i64(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch.schema().column_with_name(name).unwrap().0;
    batch
        .column(idx)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}

fn flatten_i64_named(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| batch_column_i64(b, col))
        .collect()
}

#[tokio::test]
async fn multi_file_parquet_row_index_resets_each_file_by_value_range() {
    let dir = tempfile::tempdir().unwrap();
    let p1 = dir.path().join("a.parquet");
    let p2 = dir.path().join("b.parquet");
    write_i64_parquet(&p1, "x", &[100, 101, 102]);
    write_i64_parquet(&p2, "x", &[200, 201]);

    let schema = single_long_schema();
    let schema_with_row_index = Arc::new(
        schema
            .as_ref()
            .add_metadata_column("rid", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let plan = PlanBuilder::from_scan(ScanNode::new(
        FileType::Parquet,
        vec![file_meta(&p1), file_meta(&p2)],
        schema_with_row_index,
    ));

    let batches = scan_collect(plan).await.unwrap();

    let mut by_x: HashMap<i64, i64> = HashMap::new();
    for b in &batches {
        let xs = batch_column_i64(b, "x");
        let rids = batch_column_i64(b, "rid");
        assert_eq!(xs.len(), rids.len());
        for (&x, &rid) in xs.iter().zip(rids.iter()) {
            assert!(by_x.insert(x, rid).is_none(), "duplicate row for x={x}");
        }
    }

    for x in [100_i64, 101, 102] {
        assert_eq!(by_x[&x], x - 100, "file A expects rid 0..=2");
    }
    for x in [200_i64, 201] {
        assert_eq!(by_x[&x], x - 200, "file B expects rid 0..=1");
    }
}

/// Repro for the latent parquet-rs bug where `with_supplied_schema` validation fails when
/// the supplied schema includes virtual columns AND the table contains Utf8/Utf8View types
/// (which trips `apply_file_schema_type_coercions` early-return guard).
///
/// The existing `multi_file_parquet_row_index_resets_each_file_by_value_range` doesn't hit
/// this because all columns are Int64 → no Utf8 → coercion path skipped → no validation.
/// Adding a Utf8 column should reproduce the "expected N struct fields got N+1" error if the
/// bug exists in the scan.rs flow too (independent of LoadExec changes).
#[tokio::test]
async fn scan_with_row_index_and_utf8_column() {
    use delta_kernel::arrow::array::{Int64Array, RecordBatch as ArrowRecordBatch, StringArray};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
    use delta_kernel::schema::{DataType, StructField, StructType};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.parquet");
    {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("x", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = ArrowRecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([10_i64, 20, 30])),
                Arc::new(StringArray::from_iter_values(["a", "b", "c"])),
            ],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let kernel_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("x", DataType::LONG),
            StructField::not_null("name", DataType::STRING),
        ])
        .unwrap(),
    );
    let with_row_index = Arc::new(
        kernel_schema
            .as_ref()
            .add_metadata_column("rid", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );

    let plan = PlanBuilder::from_scan(ScanNode::new(
        FileType::Parquet,
        vec![file_meta(&path)],
        with_row_index,
    ));

    let batches = scan_collect(plan).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "must read all 3 rows from the file");
}

#[tokio::test]
async fn scan_predicate_matches_arrow_parquet_reference_multi_file_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<PathBuf> = ["p1.parquet", "p2.parquet", "p3.parquet"]
        .iter()
        .map(|n| dir.path().join(n))
        .collect();
    write_i64_parquet(&paths[0], "x", &[5, 8, 12]);
    write_i64_parquet(&paths[1], "x", &[25, 30]);
    write_i64_parquet(&paths[2], "x", &[3, 100]);

    let kernel_schema = single_long_schema();
    let pred = Arc::new(Expression::from_pred(column_expr!("x").gt(
        Expression::literal(delta_kernel::expressions::Scalar::Long(10)),
    )));

    let metas: Vec<FileMeta> = paths.iter().map(|p| file_meta(p)).collect();

    let mut reference_batches = Vec::new();
    for p in &paths {
        let raw = read_parquet_batches(p);
        reference_batches.extend(filter_batches_kernel_semantics(
            &raw,
            Arc::clone(&kernel_schema),
            Arc::clone(&pred),
        ));
    }
    let reference_x = flatten_i64_named(&reference_batches, "x");

    let plan = PlanBuilder::from_scan(
        ScanNode::new(FileType::Parquet, metas, Arc::clone(&kernel_schema))
            .with_predicate(Arc::clone(&pred)),
    );

    let df_batches = scan_collect(plan).await.unwrap();

    let mut df_x = flatten_i64_named(&df_batches, "x");
    let mut reference_sorted = reference_x;
    df_x.sort();
    reference_sorted.sort();

    assert_eq!(
        df_x, reference_sorted,
        "native residual scan filter output must match parquet iterator + kernel evaluator reference"
    );
    assert_eq!(df_x, vec![12, 25, 30, 100]);
}
