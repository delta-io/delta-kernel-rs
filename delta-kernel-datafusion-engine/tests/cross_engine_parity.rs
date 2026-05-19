//! Cross-engine parity: kernel [`ArrowEvaluationHandler`] + expression evaluation vs
//! [`DataFusionExecutor`] on the same declarative [`Plan`] trees (Results sink).
//!
//! Shapes mirror the FSR-style declarative slices: literal, scan→Results, filter, project,
//! ordered union, window `row_number`, hash join.

mod common;

use std::fs::File;
use std::sync::Arc;

use common::{
    assert_batch_column_data_equal, concat_or_clone, kernel_literal_batch, run_to_batches,
};
use datafusion_common::assert_batches_sorted_eq;
use delta_kernel::arrow::array::{BooleanArray, Int64Array, RecordBatch};
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::expressions::{
    column_expr, BinaryExpressionOp, ColumnName, Expression, Predicate, Scalar,
};
use delta_kernel::plans::ir::nodes::{JoinType, OrderingSpec, WindowFunction};
use delta_kernel::plans::ir::PlanBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use test_utils::parquet::{file_meta, write_i64_parquet};
use test_utils::schemas::{long_rows, long_schema, single_long_schema};

fn scalar_long(s: &Scalar) -> i64 {
    match s {
        Scalar::Long(v) => *v,
        other => panic!("expected LONG scalar, got {other:?}"),
    }
}

fn kernel_filter(batch: &RecordBatch, predicate: &Expression) -> RecordBatch {
    let mask = evaluate_expression(predicate, batch, Some(&DataType::BOOLEAN)).expect("pred");
    let mask = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean mask");
    filter_record_batch(batch, mask).expect("filter_record_batch")
}

fn kernel_project(
    batch: &RecordBatch,
    columns: &[Arc<Expression>],
    output_schema: Arc<StructType>,
) -> RecordBatch {
    let arrow_schema: Arc<delta_kernel::arrow::datatypes::Schema> = Arc::new(
        output_schema
            .as_ref()
            .try_into_arrow()
            .expect("schema->arrow"),
    );
    let arrays: Vec<_> = columns
        .iter()
        .zip(output_schema.fields())
        .map(|(expr, field)| {
            evaluate_expression(expr.as_ref(), batch, Some(field.data_type())).expect("project col")
        })
        .collect();
    RecordBatch::try_new(arrow_schema, arrays).expect("project batch")
}

fn assert_batches_equal(expected: &RecordBatch, actual: &[RecordBatch]) {
    let actual = concat_or_clone(actual);
    assert_eq!(expected.num_rows(), actual.num_rows(), "row count mismatch");
    assert_eq!(
        expected.num_columns(),
        actual.num_columns(),
        "column count mismatch"
    );
    assert_eq!(expected.schema(), actual.schema(), "schema mismatch");
    assert_eq!(expected, &actual, "batch equality");
}

#[tokio::test]
async fn parity_literal_matches_kernel_create_many() {
    let schema = long_schema(&[("a", false), ("b", false)]);
    let rows = long_rows([[1, 10], [2, 20]]);
    let expected = kernel_literal_batch(Arc::clone(&schema), &rows);
    let got = run_to_batches(PlanBuilder::values(schema, rows).unwrap())
        .await
        .unwrap();
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_filter_matches_kernel_semantics() {
    let schema = single_long_schema();
    let rows = long_rows([[5], [15], [25]]);
    let base = kernel_literal_batch(Arc::clone(&schema), &rows);
    let pred = Arc::new(Expression::from_pred(Predicate::gt(
        column_expr!("x"),
        Expression::literal(Scalar::Long(10)),
    )));
    let expected = kernel_filter(&base, pred.as_ref());

    let got = run_to_batches(PlanBuilder::values(schema, rows).unwrap().filter(pred))
        .await
        .unwrap();
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_project_matches_kernel_evaluation() {
    let in_schema = single_long_schema();
    let rows = long_rows([[3], [4]]);
    let base = kernel_literal_batch(Arc::clone(&in_schema), &rows);

    let out_schema = long_schema(&[("x", false), ("doubled", false)]);
    let columns = vec![
        Arc::new(Expression::column(["x"])),
        Arc::new(Expression::binary(
            BinaryExpressionOp::Plus,
            column_expr!("x"),
            Expression::literal(Scalar::Long(3)),
        )),
    ];
    // doubled = x + 3
    let expected = kernel_project(&base, &columns, Arc::clone(&out_schema));

    let got = run_to_batches(
        PlanBuilder::values(in_schema, rows)
            .unwrap()
            .project(columns, out_schema),
    )
    .await
    .unwrap();
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_ordered_union_matches_kernel_concat() {
    let schema = long_schema(&[("k", false)]);
    let left_rows = long_rows([[1], [2]]);
    let right_rows = long_rows([[100]]);
    let b_left = kernel_literal_batch(Arc::clone(&schema), &left_rows);
    let b_right = kernel_literal_batch(Arc::clone(&schema), &right_rows);
    let expected =
        concat_batches(&b_left.schema(), &[b_left, b_right]).expect("concat union reference");

    let got = run_to_batches(
        PlanBuilder::union(
            vec![
                PlanBuilder::values(Arc::clone(&schema), left_rows).unwrap(),
                PlanBuilder::values(Arc::clone(&schema), right_rows).unwrap(),
            ],
            true,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_window_row_number_matches_ordered_partition_reference() {
    let schema = long_schema(&[("part", true), ("v", false)]);
    let rows = long_rows([[1, 10], [1, 20], [2, 30]]);

    let mut ref_rn: Vec<i64> = Vec::new();
    let mut last_part: Option<i64> = None;
    let mut n = 0i64;
    for r in &rows {
        let p = scalar_long(&r[0]);
        if last_part != Some(p) {
            n = 1;
            last_part = Some(p);
        } else {
            n += 1;
        }
        ref_rn.push(n);
    }

    let out_schema = long_schema(&[("part", true), ("v", false), ("_rn", false)]);

    let base = kernel_literal_batch(Arc::clone(&schema), &rows);
    let rn_arr = Int64Array::from_iter_values(ref_rn.iter().copied());
    let mut cols = base.columns().to_vec();
    cols.push(Arc::new(rn_arr));
    let arrow_out: Arc<delta_kernel::arrow::datatypes::Schema> =
        Arc::new(out_schema.as_ref().try_into_arrow().unwrap());
    let expected = RecordBatch::try_new(arrow_out, cols).unwrap();

    let got = run_to_batches(
        PlanBuilder::values(schema, rows)
            .unwrap()
            .window(
                vec![WindowFunction {
                    output_col: "_rn".into(),
                }],
                vec![Arc::new(Expression::column(["part"]))],
                vec![OrderingSpec::asc(ColumnName::new(["v"]))],
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_inner_join_matches_reference_including_duplicate_build_keys() {
    let build_schema = long_schema(&[("bk", false), ("bv", false)]);
    let probe_schema = long_schema(&[("pk", false), ("pv", false)]);
    let build_rows = long_rows([[10, 100], [10, 101], [20, 200]]);
    let probe_rows = long_rows([[10, 1000], [99, 9999]]);

    let root = PlanBuilder::values(build_schema, build_rows)
        .unwrap()
        .join_on(
            PlanBuilder::values(probe_schema, probe_rows).unwrap(),
            vec![Arc::new(Expression::column(["bk"]))],
            vec![Arc::new(Expression::column(["pk"]))],
            JoinType::Inner,
        )
        .unwrap();
    let got = run_to_batches(root).await.unwrap();
    // Two probe rows match build_key 10; the duplicate bv=100 and bv=101 build rows must both
    // surface (multiset preserved). The probe row with pk=99 has no match and must be dropped.
    assert_batches_sorted_eq!(
        &[
            "+----+-----+----+------+",
            "| bk | bv  | pk | pv   |",
            "+----+-----+----+------+",
            "| 10 | 100 | 10 | 1000 |",
            "| 10 | 101 | 10 | 1000 |",
            "+----+-----+----+------+",
        ],
        &got
    );
}

#[tokio::test]
async fn parity_left_anti_join_matches_reference_probe_order() {
    let build_schema = long_schema(&[("bk", false)]);
    let probe_schema = long_schema(&[("pk", false), ("pv", false)]);

    let root = PlanBuilder::values(build_schema, long_rows([[1]]))
        .unwrap()
        .join_on(
            PlanBuilder::values(probe_schema, long_rows([[2, 20], [1, 10]])).unwrap(),
            vec![Arc::new(Expression::column(["bk"]))],
            vec![Arc::new(Expression::column(["pk"]))],
            JoinType::LeftAnti,
        )
        .unwrap();
    let got = run_to_batches(root).await.unwrap();
    // build_key {1} drops the pk=1 probe row; pk=2 has no matching build_key -> kept.
    assert_batches_sorted_eq!(
        &[
            "+----+----+",
            "| pk | pv |",
            "+----+----+",
            "| 2  | 20 |",
            "+----+----+",
        ],
        &got
    );
}

#[tokio::test]
async fn parity_scan_single_parquet_matches_arrow_reader() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.parquet");
    write_i64_parquet(&path, "x", &[7, 8, 9]);

    let kernel_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());

    let file = File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut expected_batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        expected_batches.push(batch.expect("parquet batch"));
    }
    let expected = concat_or_clone(&expected_batches);

    let kernel_schema_clone = Arc::clone(&kernel_schema);
    let got = run_to_batches(PlanBuilder::scan_parquet(
        vec![file_meta(&path)],
        kernel_schema,
    ))
    .await
    .unwrap();
    assert_batch_column_data_equal(&kernel_schema_clone, &expected, &concat_or_clone(&got));
}
