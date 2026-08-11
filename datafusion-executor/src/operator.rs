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
    lit, EmptyRelation, Expr as DFExpr, Filter as DFFilter, LogicalPlan as DFLogicalPlan,
    Values as DFValues,
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
/// DataFusion input edges share immutable plans through [`Arc`], so retaining the same handle here
/// lets downstream operators reuse a parent without cloning its plan node. The kernel schema
/// remains authoritative for expression lowering because converting DataFusion's inferred Arrow
/// schema back would lose kernel-specific type semantics.
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
    let lowered = DFValues {
        schema: df_schema,
        values: rows?,
    };
    let plan = DFLogicalPlan::Values(lowered);
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
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
    use datafusion::common::ScalarValue as DFScalarValue;
    use delta_kernel::expressions::{col, Predicate as KernelPredicate};
    use delta_kernel::schema::{DataType, StructField, StructType};
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

    /// Lowers a `Values` over [`test_schema`] holding `rows`.
    fn lower_rows(rows: Vec<Vec<KernelScalar>>) -> Result<Arc<DFLogicalPlan>, DataFusionError> {
        let values = KernelValues::new(test_schema(), rows);
        Ok(lower_operator(&KernelOperator::Values(values), &[])?.plan)
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

    /// The Arrow type kernel maps a timestamp to, in `tz`'s zone (`None` for `TIMESTAMP_NTZ`).
    fn timestamp_type(tz: Option<Arc<str>>) -> ArrowDataType {
        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, tz)
    }

    /// An empty parent relation with `schema`.
    fn parent_with_schema(schema: StructType) -> LoweredNode {
        lower_operator(
            &KernelOperator::Values(KernelValues::new(schema, vec![])),
            &[],
        )
        .unwrap()
    }

    // === Tests ===

    #[rstest]
    #[case::single_row(
        vec![vec![1i64.into(), "x".into()]],
        vec![vec![lit(1i64), lit("x")]]
    )]
    #[case::multiple_rows(
        vec![vec![1i64.into(), "x".into()], vec![2i64.into(), "y".into()]],
        vec![vec![lit(1i64), lit("x")], vec![lit(2i64), lit("y")]]
    )]
    fn values_lowers_to_literal_rows(
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] expected: Vec<Vec<DFExpr>>,
    ) -> Result<(), DataFusionError> {
        let plan = lower_rows(rows)?;
        let DFLogicalPlan::Values(values) = plan.as_ref() else {
            panic!("expected Values, got {plan:?}");
        };
        assert_eq!(values.values, expected);
        assert_eq!(output_names(&plan), ["a", "b"]);
        Ok(())
    }

    /// Kernel's absent relation builds to an empty `Values`, so this shape is reachable from any
    /// empty file set and must not be mistaken for a malformed plan.
    #[test]
    fn empty_values_lowers_to_empty_relation() {
        let plan = lower_rows(vec![]).unwrap();
        let DFLogicalPlan::EmptyRelation(empty) = plan.as_ref() else {
            panic!("expected EmptyRelation, got {plan:?}");
        };
        assert!(!empty.produce_one_row);
        assert_eq!(
            output_names(&plan),
            ["a", "b"],
            "schema survives an empty relation"
        );
    }

    /// Each literal is lowered as-is, with no cast to its declared field type: a kernel `Scalar`
    /// and the field it fills derive their Arrow types from the same kernel `DataType`, so a
    /// well-formed node already agrees. This asserts that agreement holds across the types
    /// whose Arrow mapping carries extra parameters, where a silent cast would be easiest to
    /// miss.
    #[rstest]
    #[case::long(
        1i64.into(),
        DFScalarValue::Int64(Some(1)),
        ArrowDataType::Int64
    )]
    #[case::timestamp(
        KernelScalar::Timestamp(1),
        DFScalarValue::TimestampMicrosecond(Some(1), Some("UTC".into())),
        timestamp_type(Some("UTC".into()))
    )]
    #[case::timestamp_ntz(
        KernelScalar::TimestampNtz(1),
        DFScalarValue::TimestampMicrosecond(Some(1), None),
        timestamp_type(None)
    )]
    #[case::date(
        KernelScalar::Date(1),
        DFScalarValue::Date32(Some(1)),
        ArrowDataType::Date32
    )]
    fn literal_type_matches_its_declared_field_without_casting(
        #[case] scalar: KernelScalar,
        #[case] expected_literal: DFScalarValue,
        #[case] expected_type: ArrowDataType,
    ) {
        let schema = StructType::try_new([StructField::nullable("a", scalar.data_type())]).unwrap();
        let values = KernelValues::new(schema, vec![vec![scalar]]);
        let plan = lower_operator(&KernelOperator::Values(values), &[])
            .unwrap()
            .plan;

        // The schema's declared type and the literal's own type must agree: a `Values` node whose
        // schema disagrees with its literals reports a type the node does not produce.
        assert_eq!(output_types(&plan), std::slice::from_ref(&expected_type));
        let DFLogicalPlan::Values(lowered) = plan.as_ref() else {
            panic!("expected Values, got {plan:?}");
        };
        let DFExpr::Literal(literal, _) = &lowered.values[0][0] else {
            panic!("expected a bare literal, got {:?}", lowered.values[0][0]);
        };
        assert_eq!(literal, &expected_literal);
        assert_eq!(literal.data_type(), expected_type);
    }

    #[test]
    fn filter_wraps_its_parent_and_inherits_kernel_schema() {
        let parent = parent_with_schema(test_schema());
        let filter = KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        };
        let lowered = lower_operator(&KernelOperator::Filter(filter), &[&parent]).unwrap();

        assert!(Arc::ptr_eq(&lowered.schema, &parent.schema));
        let DFLogicalPlan::Filter(filter) = lowered.plan.as_ref() else {
            panic!("expected Filter, got {:?}", lowered.plan);
        };
        assert!(Arc::ptr_eq(&filter.input, &parent.plan));
    }

    #[rstest]
    #[case::values(
        KernelOperator::Values(KernelValues::new(test_schema(), vec![])),
        0,
        1
    )]
    #[case::filter_missing(
        KernelOperator::Filter(KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        }),
        1,
        0
    )]
    #[case::filter_extra(
        KernelOperator::Filter(KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        }),
        1,
        2
    )]
    fn operator_rejects_wrong_parent_count(
        #[case] op: KernelOperator,
        #[case] expected: usize,
        #[case] actual: usize,
    ) {
        let parent = parent_with_schema(test_schema());
        let parents = vec![&parent; actual];
        let err = lower_operator(&op, &parents).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "{op} expects {expected} parent(s), but received {actual}"
            )),
            "{err}"
        );
    }
}
