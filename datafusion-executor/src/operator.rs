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
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::{
    col as df_col, lit as df_lit, EmptyRelation, Expr as DFExpr, ExprSchemable,
    Filter as DFFilter,
    LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, Projection as DFProjection,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::{Expression as KernelExpression, Scalar as KernelScalar};
use delta_kernel::plans::ir::nodes::{
    Filter as KernelFilter, Operator as KernelOperator, Project as KernelProject,
    Values as KernelValues,
};
use delta_kernel::schema::StructType;
use delta_kernel::DeltaResult;

use crate::expression::{struct_null_when_not, to_df_struct_columns};
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
        KernelOperator::Project(project) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_project(project, input)
        }
        KernelOperator::Filter(filter) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_filter(filter, input)
        }
        // TODO: lower the remaining operators (scans, Load/Aggregate, SemiJoin, UnionAll), each
        // in its own change.
        _ => Err(DataFusionError::NotImplemented(format!(
            "lowering operator {op} to a DataFusion LogicalPlan"
        ))),
    }
}

/// Lowers a [`Project`](KernelProject) to a single DataFusion projection.
///
/// A kernel Project holds a struct expression and its declared output schema, while DataFusion
/// expects a flat expression list. Each struct field is therefore lowered, cast to its declared
/// type, and aliased with its declared name before the declared output schema is attached to the
/// DataFusion projection.
///
/// # Errors
/// Returns an error if the Project expression is not a `Struct`/`StructPatch`, lowering a field or
/// its nullability guard fails, or DataFusion cannot cast a field to its declared type.
fn lower_project(
    project: &KernelProject,
    input: &Arc<DFLogicalPlan>,
) -> Result<DFLogicalPlan, DataFusionError> {
    let input_schema: StructType = input.schema().as_arrow().try_into_kernel()?;
    let arrow_schema: ArrowSchema = project.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(DFSchema::try_from(arrow_schema)?);
    let columns = project_output_columns(&project.expr, &input_schema, project.schema.as_ref())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let exprs: Result<Vec<DFExpr>, DataFusionError> = columns
        .into_iter()
        .zip(project.schema.fields())
        .map(|((name, expr), field)| {
            let target = field.data_type().try_into_arrow()?;
            let expr = expr.cast_to(&target, input.schema())?.alias(name);
            Ok(expr)
        })
        .collect();
    let projection = DFProjection::try_new_with_schema(exprs?, Arc::clone(input), df_schema)?;
    Ok(DFLogicalPlan::Projection(projection))
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

/// Flattens a [`KernelProject`]'s struct output and applies its row-level null guard to every
/// output column.
fn project_output_columns(
    expr: &KernelExpression,
    input_schema: &StructType,
    output_type: &StructType,
) -> DeltaResult<Vec<(String, DFExpr)>> {
    let (columns, null_guard) = to_df_struct_columns(expr, input_schema, output_type)?;
    let Some(guard) = null_guard else {
        return Ok(columns);
    };
    let guarded = columns
        .into_iter()
        .map(|(name, value)| (name, struct_null_when_not(guard.clone(), value)))
        .collect();
    Ok(guarded)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue as DFScalarValue;
    use datafusion::logical_expr::{col as df_col, Case};
    use datafusion::prelude::SessionContext;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use delta_kernel::expressions::{
        col, lit, ArrayData as KernelArrayData, BinaryPredicateOp as KernelBinaryPredicateOp,
        Expression as KernelExpr, ExpressionStructPatch, ExpressionStructPatchBuilder,
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
    #[case::too_few(
        vec![vec![1i64.into()]],
        0,
        "got 1 values in row 0 but expected 2"
    )]
    #[case::too_many(
        vec![vec![1i64.into(), "x".into(), true.into()]],
        0,
        "got 3 values in row 0 but expected 2"
    )]
    #[case::unexpected_input(
        vec![],
        1,
        "values expects 0 input(s), but received 1"
    )]
    fn values_reject_wrong_row_width_or_input_count(
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] input_count: usize,
        #[case] expected: &str,
    ) {
        let input = input_with_schema(test_schema());
        let inputs = vec![input; input_count];
        let op = KernelOperator::Values(KernelValues::new(test_schema(), rows));
        let err = lower_operator(&op, &inputs).unwrap_err();
        assert!(err.to_string().contains(expected), "{err}");
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
            vec![KernelScalar::null(DataType::LONG), "n".into()],
            vec![1i64.into(), "x".into()],
            vec![5i64.into(), "y".into()],
            vec![9i64.into(), "z".into()],
        ]
    }

    const NO_ROWS: &[&str] = &[];
    const NULL_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "|   | n |",
        "+---+---+",
    ];
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
    const NULL_X_AND_Z_ROWS: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "|   | n |",
        "| 1 | x |",
        "| 9 | z |",
        "+---+---+",
    ];

    #[rstest]
    #[case::is_null(KernelPredicate::is_null(col!("a")), NULL_ROW)]
    #[case::equal(
        KernelPredicate::binary(KernelBinaryPredicateOp::Equal, col!("a"), lit(5i64)),
        Y_ROW
    )]
    #[case::less_than(
        KernelPredicate::binary(KernelBinaryPredicateOp::LessThan, col!("a"), lit(5i64)),
        X_ROW
    )]
    #[case::greater_than(
        KernelPredicate::binary(KernelBinaryPredicateOp::GreaterThan, col!("a"), lit(5i64)),
        Z_ROW
    )]
    #[case::distinct(
        KernelPredicate::binary(KernelBinaryPredicateOp::Distinct, col!("a"), lit(5i64)),
        NULL_X_AND_Z_ROWS
    )]
    #[case::and(
        KernelPredicate::junction(
            KernelJunctionPredicateOp::And,
            [
                KernelPredicate::gt(col!("a"), lit(1i64)),
                KernelPredicate::lt(col!("a"), lit(9i64)),
            ],
        ),
        Y_ROW
    )]
    #[case::or(
        KernelPredicate::junction(
            KernelJunctionPredicateOp::Or,
            [
                KernelPredicate::lt(col!("a"), lit(5i64)),
                KernelPredicate::gt(col!("a"), lit(5i64)),
            ],
        ),
        X_AND_Z_ROWS
    )]
    #[case::where_null(KernelPredicate::NULL, NO_ROWS)]
    #[tokio::test]
    async fn filter_executes_predicate(
        #[case] predicate: KernelPredicate,
        #[case] expected: &[&str],
    ) {
        let batches = execute_filter(comparison_rows(), predicate).await.unwrap();
        if expected.is_empty() {
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        } else {
            assert_batches_sorted_eq!(expected, &batches);
        }
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

    // === Project ===

    fn empty_schema() -> StructType {
        let fields: Vec<StructField> = Vec::new();
        StructType::try_new(fields).unwrap()
    }

    /// The field names DataFusion reports for `plan`'s output columns.
    fn output_names(plan: &DFLogicalPlan) -> Vec<&String> {
        plan.schema().fields().iter().map(|f| f.name()).collect()
    }

    /// The Arrow types DataFusion reports for `plan`'s output columns.
    fn output_types(plan: &DFLogicalPlan) -> Vec<ArrowDataType> {
        let fields = plan.schema().fields();
        fields.iter().map(|f| f.data_type().clone()).collect()
    }

    /// Lowers a Project over `parent`.
    fn lower_project_expr(
        expr: KernelExpr,
        schema: StructType,
        parent: &Arc<DFLogicalPlan>,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        lower_operator(
            &KernelOperator::Project(KernelProject {
                expr: expr.into(),
                schema: Arc::new(schema),
            }),
            std::slice::from_ref(parent),
        )
    }

    #[rstest]
    #[case::missing(0)]
    #[case::extra(2)]
    fn project_rejects_wrong_parent_count(#[case] actual: usize) {
        let parent = input_with_schema(test_schema());
        let parents = vec![parent; actual];
        let op = KernelOperator::Project(KernelProject {
            expr: KernelExpr::struct_from([col!("a")]).into(),
            schema: Arc::new(
                StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap(),
            ),
        });
        let err = lower_operator(&op, &parents).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "project expects 1 input(s), but received {actual}"
            )),
            "{err}"
        );
    }

    #[rstest]
    #[case::flat(
        KernelExpr::struct_from([col!("b"), col!("a")]),
        StructType::try_new([
            StructField::nullable("renamed_b", DataType::STRING),
            StructField::nullable("renamed_a", DataType::LONG),
        ]).unwrap(),
        vec!["renamed_b", "renamed_a"]
    )]
    #[case::nested(
        KernelExpr::struct_from([KernelExpr::struct_from([col!("a")])]),
        StructType::try_new([StructField::nullable(
            "nested",
            StructType::try_new([StructField::nullable("leaf", DataType::LONG)]).unwrap(),
        )]).unwrap(),
        vec!["nested"]
    )]
    fn project_lowers_single_projection_with_declared_schema(
        #[case] expr: KernelExpr,
        #[case] output: StructType,
        #[case] expected_names: Vec<&str>,
    ) {
        let expected_arrow: ArrowSchema = (&output).try_into_arrow().unwrap();
        let expected_types: Vec<ArrowDataType> = expected_arrow
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let parent = input_with_schema(test_schema());
        let lowered = lower_project_expr(expr, output, &parent).unwrap();

        assert_eq!(output_names(&lowered), expected_names);
        assert_eq!(output_types(&lowered), expected_types);
        let DFLogicalPlan::Projection(projection) = &lowered else {
            panic!("expected Projection, got {lowered:?}");
        };
        assert!(Arc::ptr_eq(&projection.input, &parent));
        assert_eq!(projection.expr.len(), expected_names.len());
        assert!(matches!(
            projection.input.as_ref(),
            DFLogicalPlan::EmptyRelation(_)
        ));
    }

    #[rstest]
    #[case::replace(
        ExpressionStructPatchBuilder::new()
            .replace("a", KernelExpr::literal(7i64))
            .build()
            .unwrap(),
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
        ]).unwrap(),
        vec!["a", "b"]
    )]
    #[case::drop(
        ExpressionStructPatchBuilder::new().drop("a").build().unwrap(),
        StructType::try_new([
            StructField::nullable("b", DataType::STRING),
        ]).unwrap(),
        vec!["b"]
    )]
    #[case::inject(
        ExpressionStructPatchBuilder::new()
            .append(KernelExpr::literal(7i64))
            .build()
            .unwrap(),
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
            StructField::nullable("injected", DataType::LONG),
        ]).unwrap(),
        vec!["a", "b", "injected"]
    )]
    fn project_lowers_struct_patch(
        #[case] patch: ExpressionStructPatch,
        #[case] output: StructType,
        #[case] expected_names: Vec<&str>,
    ) {
        let parent = input_with_schema(test_schema());
        let expr = KernelExpr::struct_patch(patch).unwrap();
        let lowered = lower_project_expr(expr, output, &parent).unwrap();
        assert_eq!(output_names(&lowered), expected_names);
    }

    #[test]
    fn project_schema_and_predicate_are_available_to_a_downstream_filter() {
        let parent = input_with_schema(test_schema());
        let output =
            StructType::try_new([StructField::nullable("projected", DataType::LONG)]).unwrap();
        let projected = Arc::new(
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap(),
        );
        let filter = KernelFilter {
            predicate: KernelPredicate::is_null(col!("projected")).into(),
        };
        let filtered = lower_operator(
            &KernelOperator::Filter(filter),
            std::slice::from_ref(&projected),
        )
        .unwrap();
        let DFLogicalPlan::Filter(filter) = &filtered else {
            panic!("expected Filter, got {filtered:?}");
        };
        assert_eq!(filter.predicate, df_col("projected").is_null());
        assert!(Arc::ptr_eq(&filter.input, &projected));
        assert_eq!(filtered.schema(), projected.schema());
    }

    /// A boolean nullability guard masks each output column individually (`CASE WHEN guard THEN
    /// value ELSE NULL`), the per-field equivalent of nulling the whole struct.
    #[test]
    fn project_with_boolean_nullability_guard_masks_each_column() {
        let input = StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
        ])
        .unwrap();
        let parent = input_with_schema(input);
        let output = StructType::try_new([StructField::nullable("out", DataType::LONG)]).unwrap();
        let expr = KernelExpr::struct_with_nullability_from([col!("a")], col!("flag"));
        let lowered = lower_project_expr(expr, output, &parent).unwrap();

        assert_eq!(output_names(&lowered), ["out"]);
        assert_eq!(output_types(&lowered), [ArrowDataType::Int64]);
        let DFLogicalPlan::Projection(projection) = &lowered else {
            panic!("expected Projection, got {lowered:?}");
        };
        let expected = DFExpr::Case(Case::new(
            None,
            vec![(Box::new(df_col("flag")), Box::new(df_col("a")))],
            Some(Box::new(df_lit(DFScalarValue::Null))),
        ))
        .alias("out");
        assert_eq!(projection.expr, [expected]);
    }

    #[test]
    fn zero_field_project_lowers_to_empty_projection() {
        let parent = input_with_schema(test_schema());
        let lowered = lower_project_expr(
            KernelExpr::struct_from([] as [KernelExpr; 0]),
            empty_schema(),
            &parent,
        )
        .unwrap();
        assert!(lowered.schema().fields().is_empty());
        let DFLogicalPlan::Projection(project) = &lowered else {
            panic!("expected Projection");
        };
        assert!(project.expr.is_empty());
    }

    #[rstest]
    #[case::unknown(KernelExpr::unknown("engine_expr"), "must be a Struct or StructPatch")]
    #[case::non_struct(KernelExpr::literal(1i64), "must be a Struct or StructPatch")]
    #[case::unresolved_nullability(
        KernelExpr::struct_with_nullability_from(
            [] as [KernelExpr; 0],
            col!("missing")
        ),
        "missing"
    )]
    #[case::malformed_patch(
        KernelExpr::struct_patch(ExpressionStructPatchBuilder::new()).unwrap(),
        "produced more fields"
    )]
    fn zero_field_project_rejects_invalid_expression(
        #[case] expr: KernelExpr,
        #[case] expected_message: &str,
    ) {
        let parent = input_with_schema(test_schema());
        let err = lower_project_expr(expr, empty_schema(), &parent).unwrap_err();
        assert!(err.to_string().contains(expected_message), "{err}");
    }

    #[test]
    fn project_rejects_uncastable_output_type() {
        let parent = input_with_schema(test_schema());
        let output =
            StructType::try_new([StructField::nullable("a", DataType::unshredded_variant())])
                .unwrap();
        let err =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap_err();
        let message = err.to_string();
        assert!(matches!(&err, DataFusionError::Plan(_)), "{message}");
        assert!(
            message.contains("Cannot automatically convert"),
            "{message}"
        );
    }

    #[rstest]
    #[case::primitive(
        StructType::try_new([
            StructField::nullable("a", DataType::INTEGER),
        ]).unwrap(),
        KernelExpr::struct_from([col!("a")]),
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
        ]).unwrap()
    )]
    #[case::nested(
        StructType::try_new([
            StructField::nullable(
                "nested",
                StructType::try_new([
                    StructField::nullable("leaf", DataType::INTEGER),
                ]).unwrap(),
            ),
        ]).unwrap(),
        KernelExpr::struct_from([col!("nested")]),
        StructType::try_new([
            StructField::nullable(
                "nested",
                StructType::try_new([
                    StructField::nullable("leaf", DataType::LONG),
                ]).unwrap(),
            ),
        ]).unwrap()
    )]
    fn project_casts_output_to_declared_type(
        #[case] input: StructType,
        #[case] expr: KernelExpr,
        #[case] output: StructType,
    ) {
        let expected: ArrowSchema = (&output).try_into_arrow().unwrap();
        let expected_types: Vec<ArrowDataType> = expected
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let parent = input_with_schema(input);
        let lowered = lower_project_expr(expr, output, &parent).unwrap();
        assert_eq!(output_types(&lowered), expected_types);
    }

    #[tokio::test]
    async fn project_executes_integer_to_long_cast() {
        let input = StructType::try_new([StructField::nullable("a", DataType::INTEGER)]).unwrap();
        let parent =
            Arc::new(lower_values_node(input, vec![vec![KernelScalar::Integer(7)]]).unwrap());
        let output = StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap();
        let lowered =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap();

        let batches = execute(lowered).await.unwrap();
        assert_batches_eq!(&["+---+", "| a |", "+---+", "| 7 |", "+---+",], &batches);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ArrowDataType::Int64
        );
    }

    #[tokio::test]
    async fn project_rejects_invalid_cast_value_during_execution() {
        let input = StructType::try_new([StructField::nullable("a", DataType::STRING)]).unwrap();
        let parent = Arc::new(
            lower_values_node(input, vec![vec![KernelScalar::String("abc".into())]]).unwrap(),
        );
        let output = StructType::try_new([StructField::nullable("a", DataType::INTEGER)]).unwrap();
        let lowered =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap();

        let err = execute(lowered).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot cast string 'abc' to value of Int32 type"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn project_executes_nested_integer_to_long_cast() {
        let input_nested =
            StructType::try_new([StructField::nullable("leaf", DataType::INTEGER)]).unwrap();
        let input =
            StructType::try_new([StructField::nullable("nested", input_nested.clone())]).unwrap();
        let nested = KernelScalar::Struct(
            KernelStructData::try_new(
                input_nested.fields().cloned().collect(),
                vec![KernelScalar::Integer(7)],
            )
            .unwrap(),
        );
        let parent = Arc::new(lower_values_node(input, vec![vec![nested]]).unwrap());
        let output_nested =
            StructType::try_new([StructField::nullable("leaf", DataType::LONG)]).unwrap();
        let output = StructType::try_new([StructField::nullable("nested", output_nested)]).unwrap();
        let lowered =
            lower_project_expr(KernelExpr::struct_from([col!("nested")]), output, &parent).unwrap();

        let batches = execute(lowered).await.unwrap();
        assert_batches_eq!(
            &[
                "+-----------+",
                "| nested    |",
                "+-----------+",
                "| {leaf: 7} |",
                "+-----------+",
            ],
            &batches
        );
        let schema = batches[0].schema();
        let ArrowDataType::Struct(fields) = schema.field(0).data_type() else {
            panic!("expected nested struct");
        };
        assert_eq!(fields[0].data_type(), &ArrowDataType::Int64);
    }

    #[test]
    fn project_normalizes_kernel_compatible_arrow_representations() {
        let expected = StructType::try_new([
            StructField::nullable("string", DataType::STRING),
            StructField::nullable("array", ArrayType::new(DataType::LONG, true)),
            StructField::nullable(
                "map",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
        ])
        .unwrap();
        let map_entries = ArrowDataType::Struct(
            vec![
                Arc::new(ArrowField::new("key", ArrowDataType::Utf8View, false)),
                Arc::new(ArrowField::new("value", ArrowDataType::Utf8View, true)),
            ]
            .into(),
        );
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("string", ArrowDataType::Utf8View, true),
            ArrowField::new(
                "array",
                ArrowDataType::LargeList(Arc::new(ArrowField::new(
                    "element",
                    ArrowDataType::Int64,
                    true,
                ))),
                true,
            ),
            ArrowField::new(
                "map",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new("entries", map_entries, false)),
                    false,
                ),
                true,
            ),
        ]);
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(arrow_schema).unwrap()),
        };
        let parent = Arc::new(DFLogicalPlan::EmptyRelation(empty));
        let output: ArrowSchema = (&expected).try_into_arrow().unwrap();
        let expected_types: Vec<ArrowDataType> = output
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let expr = KernelExpr::struct_from([col!("string"), col!("array"), col!("map")]);

        let lowered = lower_project_expr(expr, expected, &parent).unwrap();
        assert_eq!(output_types(&lowered), expected_types);
    }

    #[rstest]
    #[case::interval(DataType::INTERVAL_YEAR_MONTH)]
    #[case::variant(DataType::unshredded_variant())]
    fn project_preserves_kernel_physical_type(#[case] data_type: DataType) {
        let schema = StructType::try_new([StructField::nullable("a", data_type)]).unwrap();
        let expected: ArrowSchema = (&schema).try_into_arrow().unwrap();
        let expected_type = expected.field(0).data_type().clone();
        let parent = input_with_schema(schema.clone());
        let lowered =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), schema, &parent).unwrap();
        assert_eq!(output_types(&lowered), [expected_type]);
    }

    /// Errors from converting a Project's child expression propagate to the caller.
    #[test]
    fn project_propagates_expression_conversion_error() {
        let parent = input_with_schema(test_schema());
        let output = StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap();
        let expr = KernelExpr::struct_from([KernelExpr::unknown("engine_expr")]);
        let err = lower_project_expr(expr, output, &parent).unwrap_err();
        let message = err.to_string();
        assert!(matches!(&err, DataFusionError::External(_)), "{message}");
        assert!(
            message.contains(r#"cannot convert Unknown expression "engine_expr""#),
            "{message}"
        );
    }
}
