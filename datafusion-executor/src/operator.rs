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
use datafusion::common::{Column, DataFusionError};
use datafusion::logical_expr::{
    lit, EmptyRelation, Expr as DFExpr, Filter as DFFilter, LogicalPlan as DFLogicalPlan,
    LogicalPlanBuilder,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Scalar as KernelScalar;
use delta_kernel::plans::ir::nodes::{
    Filter as KernelFilter, Operator as KernelOperator, Values as KernelValues,
};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;

use crate::predicate::to_df_predicate_expr;
use crate::scalar::to_df_scalar;

/// One lowered relation and the kernel schema used to resolve its downstream expressions.
///
/// The kernel schema remains authoritative for expression lowering because converting
/// DataFusion's inferred Arrow schema back would lose kernel-specific type semantics.
#[derive(Debug)]
pub(crate) struct LoweredNode {
    pub(crate) plan: Arc<DFLogicalPlan>,
    pub(crate) schema: KernelSchemaRef,
}

impl LoweredNode {
    fn new(plan: DFLogicalPlan, schema: KernelSchemaRef) -> Self {
        Self {
            plan: Arc::new(plan),
            schema,
        }
    }
}

/// Lowers one kernel [`Operator`](KernelOperator) over its already-lowered parents.
///
/// # Errors
/// Returns an error if the operator has the wrong number of parents, has no DataFusion lowering
/// yet, or if lowering its payload fails.
pub(crate) fn lower_operator(
    op: &KernelOperator,
    parents: &[&LoweredNode],
) -> Result<LoweredNode, DataFusionError> {
    let parent_count_error = |expected: usize| {
        DataFusionError::Plan(format!(
            "{op} expects {expected} parent(s), but received {}",
            parents.len()
        ))
    };
    match op {
        KernelOperator::Values(values) => {
            let [] = parents else {
                return Err(parent_count_error(0));
            };
            lower_values(values)
        }
        KernelOperator::Filter(filter) => {
            let [parent] = parents else {
                return Err(parent_count_error(1));
            };
            lower_filter(filter, parent)
        }
        // TODO: lower the remaining operators (scans, Project/Load/Aggregate, SemiJoin, UnionAll),
        // each in its own change.
        _ => Err(DataFusionError::NotImplemented(format!(
            "lowering operator {op} to a DataFusion LogicalPlan"
        ))),
    }
}

/// Lowers a [`Filter`](KernelFilter) node into a DataFusion [`Filter`](DFFilter) logical plan over
/// its parent.
fn lower_filter(
    filter: &KernelFilter,
    parent: &LoweredNode,
) -> Result<LoweredNode, DataFusionError> {
    let predicate = to_df_predicate_expr(&filter.predicate, parent.schema.as_ref())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let filter = DFFilter::try_new(predicate, Arc::clone(&parent.plan))?;
    let plan = DFLogicalPlan::Filter(filter);
    Ok(LoweredNode::new(plan, Arc::clone(&parent.schema)))
}

/// Lowers a [`Values`](KernelValues) node into literal rows carrying `schema`'s field names.
///
/// An empty `rows` is the uninhabited relation over `schema`, which DataFusion spells as an
/// `EmptyRelation` rather than a `Values`.
///
/// DataFusion's [`LogicalPlanBuilder::values_with_schema`] automatically inserts a cast whenever
/// [`can_cast_types`](datafusion::arrow::compute::can_cast_types) accepts a type mismatch. Since
/// this still conforms to Kernel's schema, it is accepted functionality.
fn lower_values(values: &KernelValues) -> Result<LoweredNode, DataFusionError> {
    let arrow_schema: ArrowSchema = values.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(arrow_schema.try_into()?);

    if values.rows.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        };
        let plan = DFLogicalPlan::EmptyRelation(empty);
        return Ok(LoweredNode::new(plan, Arc::clone(&values.schema)));
    }

    let rows: Result<Vec<Vec<DFExpr>>, DataFusionError> =
        values.rows.iter().map(|row| lower_row(row)).collect();
    // The builder assigns column1, column2, ...; restore the names declared by the kernel schema.
    let field_aliases = df_schema.fields().iter().enumerate().map(|(index, field)| {
        DFExpr::Column(Column::from_name(format!("column{}", index + 1))).alias(field.name())
    });
    let plan = LogicalPlanBuilder::values_with_schema(rows?, &df_schema)?
        .project(field_aliases)?
        .build()?;
    Ok(LoweredNode::new(plan, Arc::clone(&values.schema)))
}

/// Lowers one row of literals into DataFusion expressions, one per column.
///
/// # Errors
/// Returns an error for a literal with no DataFusion equivalent.
fn lower_row(row: &[KernelScalar]) -> Result<Vec<DFExpr>, DataFusionError> {
    let lower_literal = |scalar| {
        let lowered =
            to_df_scalar(scalar).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(lit(lowered))
    };
    row.iter().map(lower_literal).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::{
        col, ArrayData as KernelArrayData, BinaryPredicateOp as KernelBinaryPredicateOp,
        JunctionPredicateOp as KernelJunctionPredicateOp, MapData as KernelMapData,
        Predicate as KernelPredicate, StructData as KernelStructData,
    };
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// A two-field schema: `a` long, `b` string.
    fn test_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
        ])
        .unwrap()
    }

    fn lower_values_node(
        schema: StructType,
        rows: Vec<Vec<KernelScalar>>,
    ) -> Result<LoweredNode, DataFusionError> {
        let values = KernelValues::new(schema, rows);
        lower_operator(&KernelOperator::Values(values), &[])
    }

    async fn execute(lowered: LoweredNode) -> Result<Vec<RecordBatch>, DataFusionError> {
        let plan = Arc::unwrap_or_clone(lowered.plan);
        SessionContext::new()
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await
    }

    /// An empty parent relation with `schema`.
    fn parent_with_schema(schema: StructType) -> LoweredNode {
        lower_values_node(schema, vec![]).unwrap()
    }

    // === Values ===

    async fn execute_rows(
        schema: StructType,
        rows: Vec<Vec<KernelScalar>>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        execute(lower_values_node(schema, rows)?).await
    }

    fn schema_for_row(names: &[&str], row: &[KernelScalar]) -> StructType {
        StructType::try_new(
            names
                .iter()
                .zip(row)
                .map(|(name, scalar)| StructField::nullable(*name, scalar.data_type())),
        )
        .unwrap()
    }

    fn nested_scalars() -> Vec<KernelScalar> {
        let struct_fields = vec![
            StructField::not_null("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ];
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
        let plan = lower_values_node(test_schema(), vec![]).unwrap().plan;
        let DFLogicalPlan::EmptyRelation(empty) = plan.as_ref() else {
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
    fn values_reject_a_parent() {
        let parent = parent_with_schema(test_schema());
        let op = KernelOperator::Values(KernelValues::new(test_schema(), vec![]));
        let err = lower_operator(&op, &[&parent]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Values expects 0 parent(s), but received 1"),
            "{err}"
        );
    }

    // === Filter ===

    async fn execute_filter(
        rows: Vec<Vec<KernelScalar>>,
        predicate: KernelPredicate,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let parent = lower_values_node(test_schema(), rows)?;
        let filter = KernelFilter {
            predicate: predicate.into(),
        };
        execute(lower_operator(&KernelOperator::Filter(filter), &[&parent])?).await
    }

    fn comparison_rows() -> Vec<Vec<KernelScalar>> {
        vec![
            vec![1i64.into(), "x".into()],
            vec![5i64.into(), "y".into()],
            vec![9i64.into(), "z".into()],
        ]
    }

    fn assert_comparison_rows(expected_rows: &[(i64, &str)], batches: &[RecordBatch]) {
        const BORDER: &str = "+---+---+";
        const HEADER: &str = "| a | b |";
        let mut expected = vec![BORDER.to_owned(), HEADER.to_owned(), BORDER.to_owned()];
        expected.extend(expected_rows.iter().map(|(a, b)| format!("| {a} | {b} |")));
        expected.push(BORDER.to_owned());
        let expected = expected.iter().map(String::as_str).collect::<Vec<_>>();
        assert_batches_eq!(&expected, batches);
    }

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
    #[case::equal(KernelBinaryPredicateOp::Equal, &[(5, "y")])]
    #[case::less_than(KernelBinaryPredicateOp::LessThan, &[(1, "x")])]
    #[case::greater_than(KernelBinaryPredicateOp::GreaterThan, &[(9, "z")])]
    #[case::distinct(KernelBinaryPredicateOp::Distinct, &[(1, "x"), (9, "z")])]
    #[tokio::test]
    async fn filter_executes_binary_predicate(
        #[case] op: KernelBinaryPredicateOp,
        #[case] expected: &[(i64, &str)],
    ) {
        let predicate = KernelPredicate::binary(op, col!("a"), KernelScalar::Long(5));
        let batches = execute_filter(comparison_rows(), predicate).await.unwrap();
        assert_comparison_rows(expected, &batches);
    }

    #[rstest]
    #[case::and(KernelJunctionPredicateOp::And, &[(5, "y")])]
    #[case::or(KernelJunctionPredicateOp::Or, &[(1, "x"), (9, "z")])]
    #[tokio::test]
    async fn filter_executes_junction_predicate(
        #[case] op: KernelJunctionPredicateOp,
        #[case] expected: &[(i64, &str)],
    ) {
        let predicates = match op {
            KernelJunctionPredicateOp::And => [
                KernelPredicate::gt(col!("a"), KernelScalar::Long(1)),
                KernelPredicate::lt(col!("a"), KernelScalar::Long(9)),
            ],
            KernelJunctionPredicateOp::Or => [
                KernelPredicate::lt(col!("a"), KernelScalar::Long(5)),
                KernelPredicate::gt(col!("a"), KernelScalar::Long(5)),
            ],
        };
        let predicate = KernelPredicate::junction(op, predicates);
        let batches = execute_filter(comparison_rows(), predicate).await.unwrap();
        assert_comparison_rows(expected, &batches);
    }

    #[rstest]
    #[case::missing(0)]
    #[case::extra(2)]
    fn filter_rejects_wrong_parent_count(#[case] actual: usize) {
        let parent = parent_with_schema(test_schema());
        let parents = vec![&parent; actual];
        let op = KernelOperator::Filter(KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        });
        let err = lower_operator(&op, &parents).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "Filter expects 1 parent(s), but received {actual}"
            )),
            "{err}"
        );
    }
}
