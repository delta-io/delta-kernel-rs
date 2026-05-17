//! Integration tests for declarative `Window` (`row_number`) compilation and execution.

mod common;

use std::sync::Arc;

use common::run_to_batches_with_blocking as run_to_batches;
use delta_kernel::arrow::array::{AsArray, RecordBatch};
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::expressions::{ColumnName, Expression, Scalar};
use delta_kernel::plans::ir::nodes::{OrderingSpec, WindowFunction};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel_datafusion_engine::DataFusionExecutor;

fn rn_column(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let schema = batch.schema();
    let idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .unwrap_or_else(|| panic!("column {name} not in schema {:?}", schema.fields()));
    batch
        .column(idx)
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}

fn sample_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::new("part", DataType::LONG, true),
            StructField::new("v", DataType::LONG, false),
        ])
        .expect("schema"),
    )
}

#[test]
fn row_number_resets_on_partition_change_ordered_by_v() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(1), Scalar::Long(20)],
        vec![Scalar::Long(2), Scalar::Long(30)],
        vec![Scalar::Long(2), Scalar::Long(40)],
    ];
    let plan = DeclarativePlanNode::values(schema.clone(), rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                output_col: "_rn".into(),
            }],
            vec![Arc::new(Expression::column(["part"]))],
            vec![OrderingSpec::asc(ColumnName::new(["v"]))],
        )
        .expect("window");

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches = run_to_batches(&exec, plan).unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(rn_column(batch, "_rn"), vec![1, 2, 1, 2]);
}

#[test]
fn row_number_global_when_no_partition_keys() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(99), Scalar::Long(20)],
    ];
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                output_col: "rn".into(),
            }],
            vec![],
            vec![OrderingSpec::asc(ColumnName::new(["v"]))],
        )
        .expect("window");

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches = run_to_batches(&exec, plan).unwrap();

    assert_eq!(rn_column(&batches[0], "rn"), vec![1, 2]);
}

#[test]
fn multiple_row_number_functions_duplicate_rank_column() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(1), Scalar::Long(20)],
    ];
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .window(
            vec![
                WindowFunction {
                    output_col: "a".into(),
                },
                WindowFunction {
                    output_col: "b".into(),
                },
            ],
            vec![Arc::new(Expression::column(["part"]))],
            vec![OrderingSpec::asc(ColumnName::new(["v"]))],
        )
        .expect("window");

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches = run_to_batches(&exec, plan).unwrap();

    assert_eq!(rn_column(&batches[0], "a"), vec![1, 2]);
    assert_eq!(rn_column(&batches[0], "b"), vec![1, 2]);
}

/// Native DataFusion Window lowering: explicit ORDER BY (DESC on `v`) sorts within each
/// PARTITION BY `part` group and assigns `row_number()` per the resulting ordering. Verifies the
/// FSR commit-4 contract — non-empty `order_by` no longer errors and yields the expected sequence
/// regardless of upstream stream order.
#[test]
fn row_number_with_order_by_desc_assigns_rank_within_partition() {
    let schema = sample_schema();
    // Intentionally interleave partitions so stream order would give wrong answers without sort.
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(2), Scalar::Long(50)],
        vec![Scalar::Long(1), Scalar::Long(30)],
        vec![Scalar::Long(2), Scalar::Long(40)],
        vec![Scalar::Long(1), Scalar::Long(20)],
    ];
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                output_col: "_rn".into(),
            }],
            vec![Arc::new(Expression::column(["part"]))],
            vec![OrderingSpec::desc(ColumnName::new(["v"]))],
        )
        .expect("window");

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches = run_to_batches(&exec, plan).unwrap();

    // Aggregate (part, v, rn) tuples across however many batches the engine emits, then verify
    // the per-partition rank assignment on the canonicalized rows.
    let mut tuples: Vec<(i64, i64, i64)> = Vec::new();
    for batch in &batches {
        let part_idx = batch
            .schema()
            .index_of("part")
            .expect("part column present");
        let v_idx = batch.schema().index_of("v").expect("v column present");
        let rn_idx = batch.schema().index_of("_rn").expect("_rn column present");
        let part = batch.column(part_idx).as_primitive::<Int64Type>().values();
        let v = batch.column(v_idx).as_primitive::<Int64Type>().values();
        let rn = batch.column(rn_idx).as_primitive::<Int64Type>().values();
        for i in 0..batch.num_rows() {
            tuples.push((part[i], v[i], rn[i]));
        }
    }
    tuples.sort();

    // For partition=1, sorted DESC by v: (30, 1), (20, 2), (10, 3).
    // For partition=2, sorted DESC by v: (50, 1), (40, 2).
    let expected = vec![(1, 10, 3), (1, 20, 2), (1, 30, 1), (2, 40, 2), (2, 50, 1)];
    assert_eq!(tuples, expected);
}

/// Smoke test: ORDER BY ASC works and matches the DESC test inverted.
#[test]
fn row_number_with_order_by_asc_matches_inverted_desc() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(30)],
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(1), Scalar::Long(20)],
    ];
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                output_col: "_rn".into(),
            }],
            vec![Arc::new(Expression::column(["part"]))],
            vec![OrderingSpec::asc(ColumnName::new(["v"]))],
        )
        .expect("window");

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches = run_to_batches(&exec, plan).unwrap();

    let mut tuples: Vec<(i64, i64)> = Vec::new();
    for batch in &batches {
        let v_idx = batch.schema().index_of("v").expect("v column present");
        let rn_idx = batch.schema().index_of("_rn").expect("_rn column present");
        let v = batch.column(v_idx).as_primitive::<Int64Type>().values();
        let rn = batch.column(rn_idx).as_primitive::<Int64Type>().values();
        for i in 0..batch.num_rows() {
            tuples.push((v[i], rn[i]));
        }
    }
    tuples.sort();
    // ASC by v inside the single partition: (10, 1), (20, 2), (30, 3).
    assert_eq!(tuples, vec![(10, 1), (20, 2), (30, 3)]);
}
