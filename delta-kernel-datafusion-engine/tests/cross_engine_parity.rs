//! Cross-engine parity: kernel [`ArrowEvaluationHandler`] + expression evaluation vs
//! [`DataFusionExecutor`] on the same declarative [`Plan`] trees (Results sink).
//!
//! Shapes mirror the acceptance / FSR-style declarative slices supported in Phase 1.x (literal,
//! scan→Results, filter, project, ordered union, window `row_number`, hash join, assert).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::{AsArray, BooleanArray, Int64Array, RecordBatch};
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch};
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::{
    column_expr, BinaryExpressionOp, ColumnName, Expression, Predicate, Scalar,
};
use delta_kernel::plans::ir::nodes::{
    AssertCheck, JoinHint, JoinNode, JoinType, OrderingSpec, WindowFunction,
};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{EvaluationHandler, FileMeta};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

fn scalar_long(s: &Scalar) -> i64 {
    match s {
        Scalar::Long(v) => *v,
        other => panic!("expected LONG scalar, got {other:?}"),
    }
}

fn kernel_literal_batch(schema: Arc<StructType>, rows: &[Vec<Scalar>]) -> RecordBatch {
    let handler = ArrowEvaluationHandler;
    let row_refs: Vec<&[Scalar]> = rows.iter().map(|r| r.as_slice()).collect();
    handler
        .create_many(schema, &row_refs)
        .expect("create_many")
        .try_into_record_batch()
        .expect("record batch")
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
            .expect("schema→arrow"),
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

async fn df_collect(plan: delta_kernel::plans::ir::Plan) -> Vec<RecordBatch> {
    DataFusionExecutor::try_new()
        .expect("executor")
        .execute_plan_collect(plan)
        .await
        .expect("execute")
}

fn concat_or_clone(batches: &[RecordBatch]) -> RecordBatch {
    match batches.len() {
        0 => panic!("empty batches"),
        1 => batches[0].clone(),
        _ => concat_batches(&batches[0].schema(), batches).expect("concat_batches"),
    }
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

/// Parquet metadata can differ from DataFusion scan headers; compare column data only.
fn assert_batch_column_data_equal(
    kernel_schema: &Arc<StructType>,
    expected: &RecordBatch,
    actual: &RecordBatch,
) {
    assert_eq!(expected.num_rows(), actual.num_rows(), "row count");
    assert_eq!(expected.num_columns(), actual.num_columns(), "column count");
    let canonical: Arc<delta_kernel::arrow::datatypes::Schema> = Arc::new(
        kernel_schema
            .as_ref()
            .try_into_arrow()
            .expect("kernel→arrow schema"),
    );
    let exp = RecordBatch::try_new(canonical.clone(), expected.columns().to_vec())
        .expect("canonical expected");
    let act = RecordBatch::try_new(canonical, actual.columns().to_vec()).expect("canonical actual");
    assert_eq!(exp, act);
}

#[tokio::test]
async fn parity_literal_matches_kernel_create_many() {
    let schema = Arc::new(
        StructType::try_new([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
        ])
        .unwrap(),
    );
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(2), Scalar::Long(20)],
    ];
    let expected = kernel_literal_batch(Arc::clone(&schema), &rows);
    let plan = DeclarativePlanNode::values(schema, rows)
        .unwrap()
        .into_results();
    let got = df_collect(plan).await;
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_filter_matches_kernel_semantics() {
    let schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());
    let rows = vec![
        vec![Scalar::Long(5)],
        vec![Scalar::Long(15)],
        vec![Scalar::Long(25)],
    ];
    let base = kernel_literal_batch(Arc::clone(&schema), &rows);
    let pred = Arc::new(Expression::from_pred(Predicate::gt(
        column_expr!("x"),
        Expression::literal(Scalar::Long(10)),
    )));
    let expected = kernel_filter(&base, pred.as_ref());

    let plan = DeclarativePlanNode::values(schema, rows)
        .unwrap()
        .filter(pred)
        .into_results();
    let got = df_collect(plan).await;
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_project_matches_kernel_evaluation() {
    let in_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());
    let rows = vec![vec![Scalar::Long(3)], vec![Scalar::Long(4)]];
    let base = kernel_literal_batch(Arc::clone(&in_schema), &rows);

    let out_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("x", DataType::LONG),
            StructField::not_null("doubled", DataType::LONG),
        ])
        .unwrap(),
    );
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

    let plan = DeclarativePlanNode::values(in_schema, rows)
        .unwrap()
        .project(columns, out_schema)
        .into_results();
    let got = df_collect(plan).await;
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_ordered_union_matches_kernel_concat() {
    let schema =
        Arc::new(StructType::try_new([StructField::not_null("k", DataType::LONG)]).unwrap());
    let left_rows = vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]];
    let right_rows = vec![vec![Scalar::Long(100)]];
    let b_left = kernel_literal_batch(Arc::clone(&schema), &left_rows);
    let b_right = kernel_literal_batch(Arc::clone(&schema), &right_rows);
    let expected =
        concat_batches(&b_left.schema(), &[b_left, b_right]).expect("concat union reference");

    let plan = DeclarativePlanNode::union(vec![
        DeclarativePlanNode::values(Arc::clone(&schema), left_rows).unwrap(),
        DeclarativePlanNode::values(Arc::clone(&schema), right_rows).unwrap(),
    ])
    .unwrap()
    .into_results();
    let got = df_collect(plan).await;
    assert_batches_equal(&expected, &got);
}

#[tokio::test]
async fn parity_window_row_number_matches_ordered_partition_reference() {
    let schema = Arc::new(
        StructType::try_new([
            StructField::new("part", DataType::LONG, true),
            StructField::not_null("v", DataType::LONG),
        ])
        .unwrap(),
    );
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(1), Scalar::Long(20)],
        vec![Scalar::Long(2), Scalar::Long(30)],
    ];

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

    let out_schema = Arc::new(
        StructType::try_new([
            StructField::new("part", DataType::LONG, true),
            StructField::not_null("v", DataType::LONG),
            StructField::not_null("_rn", DataType::LONG),
        ])
        .unwrap(),
    );

    let base = kernel_literal_batch(Arc::clone(&schema), &rows);
    let rn_arr = Int64Array::from_iter_values(ref_rn.iter().copied());
    let mut cols = base.columns().to_vec();
    cols.push(Arc::new(rn_arr));
    let arrow_out: Arc<delta_kernel::arrow::datatypes::Schema> =
        Arc::new(out_schema.as_ref().try_into_arrow().unwrap());
    let expected = RecordBatch::try_new(arrow_out, cols).unwrap();

    let plan = DeclarativePlanNode::values(schema, rows)
        .unwrap()
        .window(
            vec![WindowFunction {
                output_col: "_rn".into(),
            }],
            vec![Arc::new(Expression::column(["part"]))],
            vec![OrderingSpec::asc(ColumnName::new(["v"]))],
        )
        .unwrap()
        .into_results();
    let got = df_collect(plan).await;
    assert_batches_equal(&expected, &got);
}

fn inner_join_sorted_tuples(
    build: &[(i64, i64)],
    probe: &[(i64, i64)],
) -> Vec<(i64, i64, i64, i64)> {
    let mut m: HashMap<i64, Vec<i64>> = HashMap::new();
    for &(bk, bv) in build {
        m.entry(bk).or_default().push(bv);
    }
    let mut out = Vec::new();
    for &(pk, pv) in probe {
        if let Some(bvs) = m.get(&pk) {
            for &bv in bvs {
                out.push((pk, bv, pk, pv));
            }
        }
    }
    out.sort();
    out
}

fn batch_to_inner_join_tuples(batch: &RecordBatch) -> Vec<(i64, i64, i64, i64)> {
    let bk = batch.column(0).as_primitive::<Int64Type>();
    let bv = batch.column(1).as_primitive::<Int64Type>();
    let pk = batch.column(2).as_primitive::<Int64Type>();
    let pv = batch.column(3).as_primitive::<Int64Type>();
    let mut v: Vec<_> = (0..batch.num_rows())
        .map(|i| (bk.value(i), bv.value(i), pk.value(i), pv.value(i)))
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn parity_inner_join_matches_reference_including_duplicate_build_keys() {
    let build_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("bk", DataType::LONG),
            StructField::not_null("bv", DataType::LONG),
        ])
        .unwrap(),
    );
    let probe_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("pk", DataType::LONG),
            StructField::not_null("pv", DataType::LONG),
        ])
        .unwrap(),
    );

    let build_rows = vec![
        vec![Scalar::Long(10), Scalar::Long(100)],
        vec![Scalar::Long(10), Scalar::Long(101)],
        vec![Scalar::Long(20), Scalar::Long(200)],
    ];
    let probe_rows = vec![
        vec![Scalar::Long(10), Scalar::Long(1000)],
        vec![Scalar::Long(99), Scalar::Long(9999)],
    ];

    let build_tuples: Vec<(i64, i64)> = build_rows
        .iter()
        .map(|r| (scalar_long(&r[0]), scalar_long(&r[1])))
        .collect();
    let probe_tuples: Vec<(i64, i64)> = probe_rows
        .iter()
        .map(|r| (scalar_long(&r[0]), scalar_long(&r[1])))
        .collect();
    let expected = inner_join_sorted_tuples(&build_tuples, &probe_tuples);

    let join_node = JoinNode {
        build_keys: vec![Arc::new(Expression::column(["bk"]))],
        probe_keys: vec![Arc::new(Expression::column(["pk"]))],
        join_type: JoinType::Inner,
        hint: JoinHint::Hash,
    };

    let root = DeclarativePlanNode::join(
        join_node,
        DeclarativePlanNode::values(build_schema, build_rows).unwrap(),
        DeclarativePlanNode::values(probe_schema, probe_rows).unwrap(),
    );
    let got = df_collect(root.into_results()).await;
    let merged = concat_or_clone(&got);
    assert_eq!(
        batch_to_inner_join_tuples(&merged),
        expected,
        "inner join multiset parity"
    );
}

#[tokio::test]
async fn parity_left_anti_join_matches_reference_probe_order() {
    let build_schema =
        Arc::new(StructType::try_new([StructField::not_null("bk", DataType::LONG)]).unwrap());
    let probe_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("pk", DataType::LONG),
            StructField::not_null("pv", DataType::LONG),
        ])
        .unwrap(),
    );

    let build =
        DeclarativePlanNode::values(build_schema.clone(), vec![vec![Scalar::Long(1)]]).unwrap();
    let probe_rows = vec![
        vec![Scalar::Long(2), Scalar::Long(20)],
        vec![Scalar::Long(1), Scalar::Long(10)],
    ];
    let probe = DeclarativePlanNode::values(probe_schema.clone(), probe_rows.clone()).unwrap();

    let join_node = JoinNode {
        build_keys: vec![Arc::new(Expression::column(["bk"]))],
        probe_keys: vec![Arc::new(Expression::column(["pk"]))],
        join_type: JoinType::LeftAnti,
        hint: JoinHint::Hash,
    };

    let root = DeclarativePlanNode::join(join_node, build, probe);
    let got = df_collect(root.into_results()).await;
    let batch = concat_or_clone(&got);

    let build_keys: HashSet<i64> = [1i64].into_iter().collect();
    let expected_probe_rows: Vec<_> = probe_rows
        .into_iter()
        .filter(|r| !build_keys.contains(&scalar_long(&r[0])))
        .collect();
    let expected_batch = kernel_literal_batch(probe_schema, &expected_probe_rows);
    assert_batches_equal(&expected_batch, std::slice::from_ref(&batch));
}

#[tokio::test]
async fn parity_assert_pass_through_matches_kernel_when_checks_pass() {
    let schema = Arc::new(
        StructType::try_new([
            StructField::not_null("a", DataType::BOOLEAN),
            StructField::not_null("b", DataType::BOOLEAN),
        ])
        .unwrap(),
    );
    let rows = vec![vec![Scalar::Boolean(true), Scalar::Boolean(true)]];
    let expected = kernel_literal_batch(Arc::clone(&schema), &rows);

    let plan = DeclarativePlanNode::values(schema, rows)
        .unwrap()
        .assert(vec![
            AssertCheck {
                predicate: Arc::new(Expression::column(["a"])),
                error_code: "A".into(),
                error_message: "fail a".into(),
            },
            AssertCheck {
                predicate: Arc::new(Expression::column(["b"])),
                error_code: "B".into(),
                error_message: "fail b".into(),
            },
        ])
        .into_results();

    let got = df_collect(plan).await;
    assert_batches_equal(&expected, &got);
}

fn write_i64_parquet(path: &std::path::Path, values: &[i64]) {
    use delta_kernel::arrow::datatypes::{DataType as ArrowDT, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("x", ArrowDT::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(
            values.iter().copied(),
        ))],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn file_meta(path: &std::path::Path) -> FileMeta {
    FileMeta::new(
        Url::from_file_path(path).unwrap(),
        0,
        std::fs::metadata(path).unwrap().len(),
    )
}

#[tokio::test]
async fn parity_scan_single_parquet_matches_arrow_reader() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.parquet");
    write_i64_parquet(&path, &[7, 8, 9]);

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
    let plan =
        DeclarativePlanNode::scan_parquet(vec![file_meta(&path)], kernel_schema).into_results();
    let got = df_collect(plan).await;
    assert_batch_column_data_equal(&kernel_schema_clone, &expected, &concat_or_clone(&got));
}
