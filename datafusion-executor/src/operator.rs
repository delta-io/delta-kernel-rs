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
    lit, EmptyRelation, Expr as DFExpr, LogicalPlan as DFLogicalPlan, Values as DFValues,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Scalar as KernelScalar;
use delta_kernel::plans::ir::nodes::{Operator as KernelOperator, Values as KernelValues};

use crate::scalar::to_df_scalar;

/// Lowers one kernel [`Operator`](KernelOperator) into the equivalent DataFusion
/// [`LogicalPlan`](DFLogicalPlan) node.
///
/// # Errors
/// Returns an error if the operator has no DataFusion lowering yet, or if lowering its payload
/// fails.
pub(crate) fn lower_operator(op: &KernelOperator) -> Result<DFLogicalPlan, DataFusionError> {
    match op {
        KernelOperator::Values(values) => lower_values(values),
        // TODO: lower the remaining operators (scans, Project/Filter/Load/Aggregate, SemiJoin,
        // UnionAll), each in its own change. Those read their inputs, so they will also need the
        // upstream nodes and the kernel schema each produces threaded in here.
        _ => Err(DataFusionError::NotImplemented(format!(
            "lowering operator {op} to a DataFusion LogicalPlan"
        ))),
    }
}

/// Lowers a [`Values`](KernelValues) node into literal rows carrying `schema`'s field names.
///
/// An empty `rows` is the uninhabited relation over `schema`, which DataFusion spells as an
/// `EmptyRelation` rather than a `Values`.
fn lower_values(values: &KernelValues) -> Result<DFLogicalPlan, DataFusionError> {
    let arrow_schema: ArrowSchema = values.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(DFSchema::try_from(arrow_schema)?);

    if values.rows.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        };
        return Ok(DFLogicalPlan::EmptyRelation(empty));
    }

    let rows: Result<Vec<Vec<DFExpr>>, DataFusionError> =
        values.rows.iter().map(|row| lower_row(row)).collect();
    let lowered = DFValues {
        schema: df_schema,
        values: rows?,
    };
    Ok(DFLogicalPlan::Values(lowered))
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
    fn lower_rows(rows: Vec<Vec<KernelScalar>>) -> Result<DFLogicalPlan, DataFusionError> {
        let values = KernelValues::new(test_schema(), rows);
        lower_operator(&KernelOperator::Values(values))
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

    // === Tests ===

    #[rstest]
    #[case::single_row(vec![vec![1i64.into(), "x".into()]], 1)]
    #[case::multiple_rows(
        vec![vec![1i64.into(), "x".into()], vec![2i64.into(), "y".into()]],
        2
    )]
    fn values_lowers_to_literal_rows(
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] expected_rows: usize,
    ) -> Result<(), DataFusionError> {
        let plan = lower_rows(rows)?;
        let DFLogicalPlan::Values(values) = &plan else {
            panic!("expected Values, got {plan:?}");
        };
        assert_eq!(values.values.len(), expected_rows);
        // The declared field names carry straight through, with no aliasing projection.
        assert_eq!(output_names(&plan), ["a", "b"]);
        Ok(())
    }

    /// Kernel's absent relation builds to an empty `Values`, so this shape is reachable from any
    /// empty file set and must not be mistaken for a malformed plan.
    #[test]
    fn empty_values_lowers_to_empty_relation() {
        let plan = lower_rows(vec![]).unwrap();
        let DFLogicalPlan::EmptyRelation(empty) = &plan else {
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
    #[case::long(1i64.into(), ArrowDataType::Int64)]
    #[case::timestamp(KernelScalar::Timestamp(1), timestamp_type(Some("UTC".into())))]
    #[case::timestamp_ntz(KernelScalar::TimestampNtz(1), timestamp_type(None))]
    #[case::date(KernelScalar::Date(1), ArrowDataType::Date32)]
    fn literal_type_matches_its_declared_field_without_casting(
        #[case] scalar: KernelScalar,
        #[case] expected: ArrowDataType,
    ) {
        let schema = StructType::try_new([StructField::nullable("a", scalar.data_type())]).unwrap();
        let values = KernelValues::new(schema, vec![vec![scalar]]);
        let plan = lower_operator(&KernelOperator::Values(values)).unwrap();

        // The schema's declared type and the literal's own type must agree: a `Values` node whose
        // schema disagrees with its literals reports a type the node does not produce.
        assert_eq!(output_types(&plan), std::slice::from_ref(&expected));
        let DFLogicalPlan::Values(lowered) = &plan else {
            panic!("expected Values, got {plan:?}");
        };
        let DFExpr::Literal(literal, _) = &lowered.values[0][0] else {
            panic!("expected a bare literal, got {:?}", lowered.values[0][0]);
        };
        assert_eq!(literal.data_type(), expected);
    }
}
