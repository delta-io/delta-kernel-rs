//! Integration tests for declarative hash joins over literal sources.

mod common;

use std::sync::Arc;

use common::run_to_batches_blocking as run_to_batches;
use delta_kernel::arrow::array::AsArray;
use delta_kernel::expressions::{Expression, Scalar};
use delta_kernel::plans::ir::nodes::JoinType;
use delta_kernel::plans::ir::PlanBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};

#[test]
fn hash_inner_join_literals_matching_keys_single_row() {
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

    let build = PlanBuilder::values(
        build_schema.clone(),
        vec![
            vec![Scalar::Long(10), Scalar::Long(100)],
            vec![Scalar::Long(20), Scalar::Long(200)],
        ],
    )
    .unwrap();

    let probe = PlanBuilder::values(
        probe_schema.clone(),
        vec![
            vec![Scalar::Long(10), Scalar::Long(1000)],
            vec![Scalar::Long(99), Scalar::Long(9999)],
        ],
    )
    .unwrap();

    let root = build
        .join_on(
            probe,
            vec![Arc::new(Expression::column(["bk"]))],
            vec![Arc::new(Expression::column(["pk"]))],
            JoinType::Inner,
        )
        .unwrap();
    let batches = run_to_batches(root).unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "expected one matching inner-join row");

    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let bk = batch
        .column(0)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    let bv = batch
        .column(1)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    let pk = batch
        .column(2)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    let pv = batch
        .column(3)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    assert_eq!(bk.value(0), 10);
    assert_eq!(bv.value(0), 100);
    assert_eq!(pk.value(0), 10);
    assert_eq!(pv.value(0), 1000);
}

#[test]
fn hash_left_anti_join_literals_keeps_non_matching_probe_rows() {
    let build_schema =
        Arc::new(StructType::try_new([StructField::not_null("bk", DataType::LONG)]).unwrap());
    let probe_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("pk", DataType::LONG),
            StructField::not_null("pv", DataType::LONG),
        ])
        .unwrap(),
    );

    let build = PlanBuilder::values(build_schema.clone(), vec![vec![Scalar::Long(1)]]).unwrap();

    let probe = PlanBuilder::values(
        probe_schema.clone(),
        vec![
            vec![Scalar::Long(2), Scalar::Long(20)],
            vec![Scalar::Long(1), Scalar::Long(10)],
        ],
    )
    .unwrap();

    let root = build
        .join_on(
            probe,
            vec![Arc::new(Expression::column(["bk"]))],
            vec![Arc::new(Expression::column(["pk"]))],
            JoinType::LeftAnti,
        )
        .unwrap();
    let batches = run_to_batches(root).unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(batch.num_columns(), 2);
    let pk = batch
        .column(0)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    let pv = batch
        .column(1)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    assert_eq!(pk.value(0), 2);
    assert_eq!(pv.value(0), 20);
}
