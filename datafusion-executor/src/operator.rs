//! Conversion from a kernel [`Operator`](KernelOperator) to a DataFusion
//! [`LogicalPlan`](DFLogicalPlan) node.
//!
//! DataFusion has no single "operator" type: each relational operator is its own `LogicalPlan`
//! variant holding its inputs, so lowering an operator *is* building the plan node that wraps its
//! already-lowered inputs. Most nodes are built through `LogicalPlanBuilder`, which validates as it
//! goes (a filter predicate must be boolean, a projection's expression count must match its
//! schema).
//!
//! This module lowers one operator at a time; the walk that feeds it each node's inputs is
//! [`crate::plan`].

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    col as df_col, lit as df_lit, EmptyRelation, Expr as DFExpr, Filter as DFFilter,
    LogicalPlan as DFLogicalPlan, LogicalPlanBuilder,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::Scalar as KernelScalar;
use delta_kernel::plans::ir::nodes::{
    Filter as KernelFilter, Operator as KernelOperator, Values as KernelValues,
};
use delta_kernel::schema::StructType;

use crate::predicate::to_df_predicate_expr;
use crate::scalar::to_df_scalar;

/// Lowers one kernel [`Operator`](KernelOperator) over its already-lowered inputs.
///
/// # Errors
/// Returns an error if the operator has the wrong number of inputs, has no DataFusion lowering
/// yet, or if lowering its payload fails.
pub(crate) fn lower_operator(
    op: &KernelOperator,
    inputs: &[Arc<DFLogicalPlan>],
) -> Result<DFLogicalPlan, DataFusionError> {
    let input_count_error = |expected: usize| {
        DataFusionError::Plan(format!(
            "{op} expects {expected} input(s), but received {}",
            inputs.len()
        ))
    };
    match op {
        KernelOperator::Values(values) => {
            let [] = inputs else {
                return Err(input_count_error(0));
            };
            lower_values(values)
        }
        KernelOperator::Filter(filter) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_filter(filter, input)
        }
        // TODO: lower the remaining operators (scans, Project/Load/Aggregate, SemiJoin, UnionAll),
        // each in its own change.
        _ => Err(DataFusionError::NotImplemented(format!(
            "lowering operator {op} to a DataFusion LogicalPlan"
        ))),
    }
}

/// Lowers a [`Filter`](KernelFilter) node into a DataFusion [`Filter`](DFFilter) logical plan over
/// its input.
fn lower_filter(
    filter: &KernelFilter,
    input: &Arc<DFLogicalPlan>,
) -> Result<DFLogicalPlan, DataFusionError> {
    let input_schema: StructType = input.schema().as_arrow().try_into_kernel()?;
    let predicate = to_df_predicate_expr(&filter.predicate, &input_schema)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let filter = DFFilter::try_new(predicate, Arc::clone(input))?;
    Ok(DFLogicalPlan::Filter(filter))
}

/// Lowers a [`Values`](KernelValues) node into literal rows carrying `schema`'s field names.
///
/// An empty `rows` is the uninhabited relation over `schema`, which DataFusion spells as an
/// `EmptyRelation` rather than a `Values`.
///
/// DataFusion's [`LogicalPlanBuilder::values_with_schema`] automatically inserts a cast whenever
/// [`can_cast_types`](datafusion::arrow::compute::can_cast_types) accepts a type mismatch. Since
/// this still conforms to Kernel's schema, it is accepted functionality.
fn lower_values(values: &KernelValues) -> Result<DFLogicalPlan, DataFusionError> {
    let arrow_schema: ArrowSchema = values.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(arrow_schema.try_into()?);

    if values.rows.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        };
        return Ok(DFLogicalPlan::EmptyRelation(empty));
    }

    let rows: Result<Vec<Vec<DFExpr>>, DataFusionError> =
        values.rows.iter().map(|row| lower_row(row)).collect();
    // The builder assigns column1, column2, ...; restore the names declared by the kernel schema.
    let field_aliases = df_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| df_col(format!("column{}", index + 1)).alias(field.name()));
    LogicalPlanBuilder::values_with_schema(rows?, &df_schema)?
        .project(field_aliases)?
        .build()
}

/// Lowers one row of literals into DataFusion expressions, one per column.
///
/// # Errors
/// Returns an error for a literal with no DataFusion equivalent.
fn lower_row(row: &[KernelScalar]) -> Result<Vec<DFExpr>, DataFusionError> {
    let lower_literal = |scalar| {
        let lowered =
            to_df_scalar(scalar).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(df_lit(lowered))
    };
    row.iter().map(lower_literal).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::{
        col, lit, ArrayData as KernelArrayData, BinaryPredicateOp as KernelBinaryPredicateOp,
        JunctionPredicateOp as KernelJunctionPredicateOp, MapData as KernelMapData,
        Predicate as KernelPredicate, StructData as KernelStructData,
    };
    use delta_kernel::schema::{schema, ArrayType, DataType, MapType, StructField, StructType};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// A two-field schema: `a` long, `b` string.
    fn test_schema() -> StructType {
        schema! {
            nullable "a": LONG,
            nullable "b": STRING,
        }
    }

    fn lower_values_node(
        schema: StructType,
        rows: Vec<Vec<KernelScalar>>,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        let values = KernelValues::new(schema, rows);
        lower_operator(&KernelOperator::Values(values), &[])
    }

    async fn execute(plan: DFLogicalPlan) -> Result<Vec<RecordBatch>, DataFusionError> {
        SessionContext::new()
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await
    }

    /// An empty input relation with `schema`.
    fn input_with_schema(schema: StructType) -> Arc<DFLogicalPlan> {
        Arc::new(lower_values_node(schema, vec![]).unwrap())
    }

    // === Values ===

    async fn execute_rows(
        schema: StructType,
        rows: Vec<Vec<KernelScalar>>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        execute(lower_values_node(schema, rows)?).await
    }

    fn schema_for_row(names: &[&str], row: &[KernelScalar]) -> StructType {
        let fields = names
            .iter()
            .zip(row)
            .map(|(name, scalar)| StructField::nullable(*name, scalar.data_type()));
        schema! { ..(fields) }
    }

    fn nested_scalars() -> Vec<KernelScalar> {
        let struct_fields = schema! {
            not_null "a": INTEGER,
            nullable "b": STRING,
        }
        .into_fields()
        .collect();
        let struct_scalar = KernelScalar::Struct(
            KernelStructData::try_new(
                struct_fields,
                vec![KernelScalar::Integer(1), KernelScalar::String("x".into())],
            )
            .unwrap(),
        );
        let array_scalar = KernelScalar::Array(
            KernelArrayData::try_new(
                ArrayType::new(DataType::INTEGER, true),
                [
                    KernelScalar::Integer(1),
                    KernelScalar::null(DataType::INTEGER),
                ],
            )
            .unwrap(),
        );
        let map_scalar = KernelScalar::Map(
            KernelMapData::try_new(
                MapType::new(DataType::STRING, DataType::INTEGER, true),
                [
                    (KernelScalar::String("x".into()), KernelScalar::Integer(1)),
                    (
                        KernelScalar::String("y".into()),
                        KernelScalar::null(DataType::INTEGER),
                    ),
                ],
            )
            .unwrap(),
        );
        vec![struct_scalar, array_scalar, map_scalar]
    }

    /// Kernel's absent relation builds to an empty [`KernelValues`], so this shape is reachable
    /// from any empty file set and must not be mistaken for a malformed plan.
    #[test]
    fn empty_values_lowers_to_empty_relation() {
        let plan = lower_values_node(test_schema(), vec![]).unwrap();
        let DFLogicalPlan::EmptyRelation(empty) = &plan else {
            panic!("expected EmptyRelation, got {plan:?}");
        };
        assert!(!empty.produce_one_row);
        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["a", "b"],
            "schema survives an empty relation"
        );
    }

    #[rstest]
    #[case::multiple_rows(
        &["a", "b"],
        vec![vec![1i64.into(), "x".into()], vec![2i64.into(), "y".into()]],
        &[
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | x |",
            "| 2 | y |",
            "+---+---+",
        ]
    )]
    #[case::numeric(
        &["byte", "short", "integer", "long", "float", "double", "decimal"],
        vec![vec![
            KernelScalar::Byte(1),
            KernelScalar::Short(2),
            KernelScalar::Integer(3),
            KernelScalar::Long(4),
            KernelScalar::Float(1.25),
            KernelScalar::Double(2.5),
            KernelScalar::decimal(12345, 7, 2).unwrap(),
        ]],
        &[
            "+------+-------+---------+------+-------+--------+---------+",
            "| byte | short | integer | long | float | double | decimal |",
            "+------+-------+---------+------+-------+--------+---------+",
            "| 1    | 2     | 3       | 4    | 1.25  | 2.5    | 123.45  |",
            "+------+-------+---------+------+-------+--------+---------+",
        ]
    )]
    #[case::string_boolean_binary_and_null(
        &["string", "boolean", "binary", "null"],
        vec![vec![
            KernelScalar::String("hello".into()),
            KernelScalar::Boolean(true),
            KernelScalar::Binary(vec![0x01, 0x02]),
            KernelScalar::null(DataType::INTEGER),
        ]],
        &[
            "+--------+---------+--------+------+",
            "| string | boolean | binary | null |",
            "+--------+---------+--------+------+",
            "| hello  | true    | 0102   |      |",
            "+--------+---------+--------+------+",
        ]
    )]
    #[case::timestamp_timestamp_ntz_and_date(
        &["timestamp", "timestamp_ntz", "date"],
        vec![vec![
            KernelScalar::Timestamp(1_000_000),
            KernelScalar::TimestampNtz(1_000_000),
            KernelScalar::Date(1),
        ]],
        &[
            "+----------------------+---------------------+------------+",
            "| timestamp            | timestamp_ntz       | date       |",
            "+----------------------+---------------------+------------+",
            "| 1970-01-01T00:00:01Z | 1970-01-01T00:00:01 | 1970-01-02 |",
            "+----------------------+---------------------+------------+",
        ]
    )]
    #[case::array_map_and_struct(
        &["struct", "array", "map"],
        vec![nested_scalars()],
        &[
            "+--------------+-------+-------------+",
            "| struct       | array | map         |",
            "+--------------+-------+-------------+",
            "| {a: 1, b: x} | [1, ] | {x: 1, y: } |",
            "+--------------+-------+-------------+",
        ]
    )]
    #[tokio::test]
    async fn values_execute_rows(
        #[case] names: &[&str],
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] expected: &[&str],
    ) {
        let schema = schema_for_row(names, &rows[0]);
        let batches = execute_rows(schema, rows).await.unwrap();
        assert_batches_eq!(expected, &batches);
    }

    #[rstest]
    #[case::too_few(vec![vec![1i64.into()]], "got 1 values in row 0 but expected 2")]
    #[case::too_many(
        vec![vec![1i64.into(), "x".into(), true.into()]],
        "got 3 values in row 0 but expected 2"
    )]
    fn values_reject_rows_with_the_wrong_width(
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] expected: &str,
    ) {
        let err = lower_values_node(test_schema(), rows).unwrap_err();
        assert!(err.to_string().contains(expected), "{err}");
    }

    #[test]
    fn values_reject_an_input() {
        let input = input_with_schema(test_schema());
        let op = KernelOperator::Values(KernelValues::new(test_schema(), vec![]));
        let err = lower_operator(&op, std::slice::from_ref(&input)).unwrap_err();
        assert!(
            err.to_string()
                .contains("values expects 0 input(s), but received 1"),
            "{err}"
        );
    }

    // === Filter ===

    #[test]
    fn filter_wraps_its_input_and_inherits_schema() {
        let input = input_with_schema(test_schema());
        let filter = KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        };
        let lowered = lower_operator(
            &KernelOperator::Filter(filter),
            std::slice::from_ref(&input),
        )
        .unwrap();

        let DFLogicalPlan::Filter(filter) = &lowered else {
            panic!("expected Filter, got {lowered:?}");
        };
        assert!(Arc::ptr_eq(&filter.input, &input));
        assert_eq!(lowered.schema(), input.schema());
    }

    async fn execute_filter(
        rows: Vec<Vec<KernelScalar>>,
        predicate: KernelPredicate,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let input = Arc::new(lower_values_node(test_schema(), rows)?);
        let filter = KernelFilter {
            predicate: predicate.into(),
        };
        execute(lower_operator(
            &KernelOperator::Filter(filter),
            std::slice::from_ref(&input),
        )?)
        .await
    }

    fn comparison_rows() -> Vec<Vec<KernelScalar>> {
        vec![
            vec![1i64.into(), "x".into()],
            vec![5i64.into(), "y".into()],
            vec![9i64.into(), "z".into()],
        ]
    }

    const X_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | x |",
        "+---+---+",
    ];
    const Y_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 5 | y |",
        "+---+---+",
    ];
    const Z_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 9 | z |",
        "+---+---+",
    ];
    const X_AND_Z_ROWS: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | x |",
        "| 9 | z |",
        "+---+---+",
    ];

    #[tokio::test]
    async fn filter_executes_is_null_predicate() {
        let batches = execute_filter(
            vec![
                vec![KernelScalar::null(DataType::LONG), "kept".into()],
                vec![1i64.into(), "removed".into()],
            ],
            KernelPredicate::is_null(col!("a")),
        )
        .await
        .unwrap();

        assert_batches_eq!(
            &[
                "+---+------+",
                "| a | b    |",
                "+---+------+",
                "|   | kept |",
                "+---+------+",
            ],
            &batches
        );
    }

    #[rstest]
    #[case::equal(KernelBinaryPredicateOp::Equal, Y_ROW)]
    #[case::less_than(KernelBinaryPredicateOp::LessThan, X_ROW)]
    #[case::greater_than(KernelBinaryPredicateOp::GreaterThan, Z_ROW)]
    #[case::distinct(KernelBinaryPredicateOp::Distinct, X_AND_Z_ROWS)]
    #[tokio::test]
    async fn filter_executes_binary_predicate(
        #[case] op: KernelBinaryPredicateOp,
        #[case] expected: &[&str],
    ) {
        let predicate = KernelPredicate::binary(op, col!("a"), lit(5i64));
        let batches = execute_filter(comparison_rows(), predicate).await.unwrap();
        assert_batches_eq!(expected, &batches);
    }

    #[rstest]
    #[case::and(KernelJunctionPredicateOp::And, Y_ROW)]
    #[case::or(KernelJunctionPredicateOp::Or, X_AND_Z_ROWS)]
    #[tokio::test]
    async fn filter_executes_junction_predicate(
        #[case] op: KernelJunctionPredicateOp,
        #[case] expected: &[&str],
    ) {
        let predicates = match op {
            KernelJunctionPredicateOp::And => [
                KernelPredicate::gt(col!("a"), lit(1i64)),
                KernelPredicate::lt(col!("a"), lit(9i64)),
            ],
            KernelJunctionPredicateOp::Or => [
                KernelPredicate::lt(col!("a"), lit(5i64)),
                KernelPredicate::gt(col!("a"), lit(5i64)),
            ],
        };
        let predicate = KernelPredicate::junction(op, predicates);
        let batches = execute_filter(comparison_rows(), predicate).await.unwrap();
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn filter_where_null_removes_all_rows() {
        let batches = execute_filter(comparison_rows(), KernelPredicate::null_literal())
            .await
            .unwrap();
        assert!(batches.is_empty());
    }

    #[rstest]
    #[case::missing(0)]
    #[case::extra(2)]
    fn filter_rejects_wrong_input_count(#[case] actual: usize) {
        let input = input_with_schema(test_schema());
        let inputs = vec![input; actual];
        let op = KernelOperator::Filter(KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        });
        let err = lower_operator(&op, &inputs).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("filter expects 1 input(s), but received {actual}")),
            "{err}"
        );
    }
}
