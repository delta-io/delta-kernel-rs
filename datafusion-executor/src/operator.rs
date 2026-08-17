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
    col as df_col, lit as df_lit, EmptyRelation, Expr as DFExpr, ExprSchemable, Filter as DFFilter,
    LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, Projection as DFProjection,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::Scalar as KernelScalar;
use delta_kernel::plans::ir::nodes::{
    Filter as KernelFilter, Operator as KernelOperator, Project as KernelProject,
    Values as KernelValues,
};
use delta_kernel::schema::StructType;

use crate::expression::to_df_struct_columns;
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
    let columns = to_df_struct_columns(&project.expr, &input_schema, project.schema.as_ref())
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .into_guarded_columns();
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue as DFScalarValue;
    use datafusion::logical_expr::{col as df_col, Case};
    use datafusion::prelude::SessionContext;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use delta_kernel::expressions::{
        col, lit as kernel_lit, null_lit, ArrayData as KernelArrayData,
        BinaryPredicateOp as KernelBinaryPredicateOp, Expression as KernelExpr, ExpressionRef,
        ExpressionStructPatchBuilder, JunctionPredicateOp as KernelJunctionPredicateOp,
        MapData as KernelMapData, Predicate as KernelPredicate, StructData as KernelStructData,
    };
    use delta_kernel::schema::{
        schema, ArrayType, DataType, MapType, SchemaRef, StructField, StructType,
    };
    use delta_kernel::struct_patch::ProjectionStructPatchBuilder;
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
        KernelPredicate::binary(KernelBinaryPredicateOp::Equal, col!("a"), kernel_lit(5i64)),
        Y_ROW
    )]
    #[case::less_than(
        KernelPredicate::binary(
            KernelBinaryPredicateOp::LessThan,
            col!("a"),
            kernel_lit(5i64),
        ),
        X_ROW
    )]
    #[case::greater_than(
        KernelPredicate::binary(
            KernelBinaryPredicateOp::GreaterThan,
            col!("a"),
            kernel_lit(5i64),
        ),
        Z_ROW
    )]
    #[case::distinct(
        KernelPredicate::binary(
            KernelBinaryPredicateOp::Distinct,
            col!("a"),
            kernel_lit(5i64),
        ),
        NULL_X_AND_Z_ROWS
    )]
    #[case::and(
        KernelPredicate::junction(
            KernelJunctionPredicateOp::And,
            [
                KernelPredicate::gt(col!("a"), kernel_lit(1i64)),
                KernelPredicate::lt(col!("a"), kernel_lit(9i64)),
            ],
        ),
        Y_ROW
    )]
    #[case::or(
        KernelPredicate::junction(
            KernelJunctionPredicateOp::Or,
            [
                KernelPredicate::lt(col!("a"), kernel_lit(5i64)),
                KernelPredicate::gt(col!("a"), kernel_lit(5i64)),
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
        expr: impl Into<ExpressionRef>,
        schema: impl Into<SchemaRef>,
        parent: &Arc<DFLogicalPlan>,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        lower_operator(
            &KernelOperator::Project(KernelProject {
                expr: expr.into(),
                schema: schema.into(),
            }),
            std::slice::from_ref(parent),
        )
    }

    fn project_nested_schema() -> StructType {
        StructType::try_new([StructField::nullable("value", DataType::INTEGER)]).unwrap()
    }

    fn project_input_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
            StructField::nullable("small", DataType::INTEGER),
            StructField::nullable("nested", project_nested_schema()),
        ])
        .unwrap()
    }

    fn project_nested_value(value: i32) -> KernelScalar {
        let schema = project_nested_schema();
        KernelScalar::Struct(
            KernelStructData::try_new(
                schema.fields().cloned().collect(),
                vec![KernelScalar::Integer(value)],
            )
            .unwrap(),
        )
    }

    fn project_input() -> Arc<DFLogicalPlan> {
        let rows = vec![
            vec![
                10i64.into(),
                2i64.into(),
                true.into(),
                1i32.into(),
                project_nested_value(7),
            ],
            vec![
                20i64.into(),
                KernelScalar::null(DataType::LONG),
                false.into(),
                2i32.into(),
                project_nested_value(8),
            ],
            vec![
                KernelScalar::null(DataType::LONG),
                4i64.into(),
                KernelScalar::null(DataType::BOOLEAN),
                3i32.into(),
                KernelScalar::null(project_nested_schema()),
            ],
        ];
        Arc::new(lower_values_node(project_input_schema(), rows).unwrap())
    }

    fn struct_project(
        fields: impl IntoIterator<Item = (StructField, KernelExpr)>,
    ) -> (SchemaRef, ExpressionRef) {
        let (fields, exprs): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        (
            Arc::new(StructType::try_new(fields).unwrap()),
            KernelExpr::struct_from(exprs).into(),
        )
    }

    fn guarded_struct_project(
        fields: impl IntoIterator<Item = (StructField, KernelExpr)>,
        guard: KernelExpr,
    ) -> (SchemaRef, ExpressionRef) {
        let (fields, exprs): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        (
            Arc::new(StructType::try_new(fields).unwrap()),
            KernelExpr::struct_with_nullability_from(exprs, guard).into(),
        )
    }

    fn struct_patch_project() -> (SchemaRef, ExpressionRef) {
        let input = project_input_schema();
        ProjectionStructPatchBuilder::new(&input)
            .replace_expr("b", KernelExpr::coalesce([col!("b"), kernel_lit(99i64)]))
            .drop("flag")
            .drop("small")
            .drop("nested")
            .append(
                StructField::nullable("sum", DataType::LONG),
                col!("a") + KernelExpr::coalesce([col!("b"), kernel_lit(99i64)]),
            )
            .build()
            .unwrap()
    }

    fn nested_struct_patch_project() -> (SchemaRef, ExpressionRef) {
        let input = project_input_schema();
        ProjectionStructPatchBuilder::new_nested(&input, ["nested"])
            .replace_expr("value", col!("nested.value") + kernel_lit(1i32))
            .build()
            .unwrap()
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
        ProjectionStructPatchBuilder::new(&test_schema())
            .replace_expr("a", KernelExpr::literal(7i64))
            .build()
            .unwrap(),
        vec!["a", "b"]
    )]
    #[case::drop(
        ProjectionStructPatchBuilder::new(&test_schema()).drop("a").build().unwrap(),
        vec!["b"]
    )]
    #[case::inject(
        ProjectionStructPatchBuilder::new(&test_schema())
            .append(
                StructField::nullable("injected", DataType::LONG),
                KernelExpr::literal(7i64),
            )
            .build()
            .unwrap(),
        vec!["a", "b", "injected"]
    )]
    fn project_lowers_struct_patch(
        #[case] project: (SchemaRef, ExpressionRef),
        #[case] expected_names: Vec<&str>,
    ) {
        let input = input_with_schema(test_schema());
        let (output, expr) = project;
        let lowered = lower_project_expr(expr, output, &input).unwrap();
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

    #[rstest]
    #[case::literal_column_and_cast(
        struct_project([
            (StructField::nullable("selected", DataType::LONG), col!("a")),
            (StructField::nullable("literal", DataType::STRING), kernel_lit("constant")),
            (
                StructField::nullable("null_value", DataType::LONG),
                null_lit(DataType::LONG),
            ),
            (StructField::nullable("widened", DataType::LONG), col!("small")),
        ]),
        &[
            "+----------+----------+------------+---------+",
            "| selected | literal  | null_value | widened |",
            "+----------+----------+------------+---------+",
            "| 10       | constant |            | 1       |",
            "| 20       | constant |            | 2       |",
            "|          | constant |            | 3       |",
            "+----------+----------+------------+---------+",
        ]
    )]
    #[case::arithmetic(
        struct_project([
            (StructField::nullable("sum", DataType::LONG), col!("a") + col!("b")),
            (
                StructField::nullable("difference", DataType::LONG),
                col!("a") - col!("b"),
            ),
            (
                StructField::nullable("product", DataType::LONG),
                col!("a") * col!("b"),
            ),
            (
                StructField::nullable("quotient", DataType::LONG),
                col!("a") / col!("b"),
            ),
        ]),
        &[
            "+-----+------------+---------+----------+",
            "| sum | difference | product | quotient |",
            "+-----+------------+---------+----------+",
            "| 12  | 8          | 20      | 5        |",
            "|     |            |         |          |",
            "|     |            |         |          |",
            "+-----+------------+---------+----------+",
        ]
    )]
    #[case::variadic(
        struct_project([
            (
                StructField::nullable("coalesced", DataType::LONG),
                KernelExpr::coalesce([col!("b"), col!("a"), kernel_lit(99i64)]),
            ),
            (
                StructField::nullable(
                    "array",
                    ArrayType::new(DataType::LONG, true),
                ),
                KernelExpr::array([col!("a"), col!("b"), kernel_lit(5i64)]),
            ),
        ]),
        &[
            "+-----------+------------+",
            "| coalesced | array      |",
            "+-----------+------------+",
            "| 2         | [10, 2, 5] |",
            "| 20        | [20, , 5]  |",
            "| 4         | [, 4, 5]   |",
            "+-----------+------------+",
        ]
    )]
    #[case::predicate(
        struct_project([
            (
                StructField::nullable("is_null", DataType::BOOLEAN),
                KernelExpr::from_pred(col!("b").is_null()),
            ),
            (
                StructField::nullable("greater", DataType::BOOLEAN),
                KernelExpr::from_pred(col!("a").gt(kernel_lit(15i64))),
            ),
            (
                StructField::nullable("conjunction", DataType::BOOLEAN),
                KernelExpr::from_pred(KernelPredicate::and(
                    col!("a").is_not_null(),
                    col!("b").lt(kernel_lit(3i64)),
                )),
            ),
            (
                StructField::nullable("distinct", DataType::BOOLEAN),
                KernelExpr::from_pred(col!("a").distinct(col!("b"))),
            ),
        ]),
        &[
            "+---------+---------+-------------+----------+",
            "| is_null | greater | conjunction | distinct |",
            "+---------+---------+-------------+----------+",
            "| false   | false   | true        | true     |",
            "| true    | true    |             | true     |",
            "| false   |         | false       | true     |",
            "+---------+---------+-------------+----------+",
        ]
    )]
    #[case::nested_struct(
        struct_project([(
            StructField::nullable(
                "record",
                StructType::try_new([
                    StructField::nullable("value", DataType::LONG),
                    StructField::nullable("label", DataType::STRING),
                ]).unwrap(),
            ),
            KernelExpr::struct_with_nullability_from(
                [col!("nested.value"), kernel_lit("seen")],
                KernelExpr::from_pred(col!("nested").is_not_null()),
            ),
        )]),
        &[
            "+-------------------------+",
            "| record                  |",
            "+-------------------------+",
            "| {value: 7, label: seen} |",
            "| {value: 8, label: seen} |",
            "|                         |",
            "+-------------------------+",
        ]
    )]
    #[case::array_of_structs(
        struct_project([(
            StructField::nullable(
                "records",
                ArrayType::new(
                    StructType::try_new([StructField::nullable("value", DataType::LONG)]).unwrap(),
                    true,
                ),
            ),
            KernelExpr::array([
                KernelExpr::struct_from([col!("a")]),
                KernelExpr::struct_from([col!("b")]),
            ]),
        )]),
        &[
            "+---------------------------+",
            "| records                   |",
            "+---------------------------+",
            "| [{value: 10}, {value: 2}] |",
            "| [{value: 20}, {value: }]  |",
            "| [{value: }, {value: 4}]   |",
            "+---------------------------+",
        ]
    )]
    #[case::top_level_nullability(
        guarded_struct_project(
            [
                (StructField::nullable("a", DataType::LONG), col!("a")),
                (StructField::nullable("b", DataType::LONG), col!("b")),
            ],
            col!("flag"),
        ),
        &[
            "+----+---+",
            "| a  | b |",
            "+----+---+",
            "| 10 | 2 |",
            "|    |   |",
            "|    |   |",
            "+----+---+",
        ]
    )]
    #[case::struct_patch(
        struct_patch_project(),
        &[
            "+----+----+-----+",
            "| a  | b  | sum |",
            "+----+----+-----+",
            "| 10 | 2  | 12  |",
            "| 20 | 99 | 119 |",
            "|    | 4  |     |",
            "+----+----+-----+",
        ]
    )]
    #[case::nested_struct_patch(
        nested_struct_patch_project(),
        &[
            "+-------+",
            "| value |",
            "+-------+",
            "| 8     |",
            "| 9     |",
            "|       |",
            "+-------+",
        ]
    )]
    #[tokio::test]
    async fn project_executes_expression(
        #[case] project: (SchemaRef, ExpressionRef),
        #[case] expected: &[&str],
    ) {
        let (output, expr) = project;
        let lowered = lower_project_expr(expr, output, &project_input()).unwrap();
        let batches = execute(lowered).await.unwrap();
        assert_batches_eq!(expected, &batches);
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
