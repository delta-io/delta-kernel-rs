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

use datafusion::arrow::datatypes::{FieldRef as ArrowFieldRef, Schema as ArrowSchema};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::{
    lit, EmptyRelation, Expr as DFExpr, ExprSchemable, LogicalPlan as DFLogicalPlan,
    Values as DFValues,
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
/// `EmptyRelation` rather than a `Values` -- it rejects a values list with no rows.
fn lower_values(values: &KernelValues) -> Result<DFLogicalPlan, DataFusionError> {
    // `try_into_arrow` fails with an `ArrowError`, which `DataFusionError` converts from directly.
    let arrow_schema: ArrowSchema = values.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(DFSchema::try_from(arrow_schema)?);

    if values.rows.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        };
        return Ok(DFLogicalPlan::EmptyRelation(empty));
    }

    // Built directly rather than through `LogicalPlanBuilder::values_with_schema`, which renames
    // the columns `column1`, `column2`, ... and would need every one aliased back to its declared
    // name. That builder also casts each literal to its declared type, so `lower_row` does that
    // here: kernel guarantees each row's width but not that a literal's type matches the field it
    // fills, and a `Values` node whose schema disagrees with its literals reports types it does
    // not produce.
    let rows: Result<Vec<Vec<DFExpr>>, DataFusionError> = values
        .rows
        .iter()
        .map(|row| lower_row(row, &df_schema))
        .collect();
    let lowered = DFValues {
        schema: df_schema,
        values: rows?,
    };
    Ok(DFLogicalPlan::Values(lowered))
}

/// Lowers one row of literals into DataFusion expressions, one per column, each cast to the type
/// its column declares in `schema`.
///
/// # Errors
/// Returns an error for a literal with no DataFusion equivalent, or none that casts to its declared
/// type.
fn lower_row(row: &[KernelScalar], schema: &DFSchema) -> Result<Vec<DFExpr>, DataFusionError> {
    let lower_literal = |(scalar, field): (&KernelScalar, &ArrowFieldRef)| {
        // The orphan rule forbids a `From<KernelError>` impl on the foreign `DataFusionError`, so
        // wrap the kernel error as an opaque external one.
        let lowered =
            to_df_scalar(scalar).map_err(|err| DataFusionError::External(Box::new(err)))?;
        lit(lowered).cast_to(field.data_type(), schema)
    };
    let declared_fields = schema.fields();
    row.iter().zip(declared_fields).map(lower_literal).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use delta_kernel::expressions::StructData as KernelStructData;
    use delta_kernel::plans::ir::nodes::Filter as KernelFilter;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::Predicate;
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

    /// A single-field struct scalar, which has no conversion to a primitive column type.
    fn struct_scalar() -> KernelScalar {
        let data = KernelStructData::try_new(
            vec![StructField::nullable("inner", DataType::LONG)],
            vec![1i64.into()],
        )
        .unwrap();
        KernelScalar::Struct(data)
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

    /// A literal whose type differs from its declared field is coerced, not rejected: DataFusion
    /// casts it whenever the conversion is possible, so the output still matches the declared
    /// schema. Only an impossible conversion errors.
    #[rstest]
    #[case::coercible_string_to_long("12".into(), Some(ArrowDataType::Int64))]
    #[case::struct_has_no_conversion_to_long(struct_scalar(), None)]
    fn row_literal_is_coerced_to_its_declared_field_type_or_errors(
        #[case] first_column: KernelScalar,
        #[case] expected_type: Option<ArrowDataType>,
    ) {
        let lowered = lower_rows(vec![vec![first_column, "x".into()]]);
        let Some(expected_type) = expected_type else {
            let err = lowered.unwrap_err();
            return assert!(
                err.to_string().contains("Cannot automatically convert"),
                "{err}"
            );
        };
        let plan = lowered.unwrap();
        assert_eq!(output_types(&plan), [expected_type, ArrowDataType::Utf8]);
        // The literal must actually be cast, not merely declared: a `Values` schema that disagrees
        // with its own literals reports a type the node does not produce.
        let DFLogicalPlan::Values(values) = &plan else {
            panic!("expected Values, got {plan:?}");
        };
        let literal_types: Vec<_> = values.values[0]
            .iter()
            .map(|expr| expr.get_type(&DFSchema::empty()).unwrap())
            .collect();
        assert_eq!(literal_types, output_types(&plan));
    }

    #[test]
    fn unlowered_operator_reports_not_implemented() {
        let filter = KernelFilter {
            predicate: Arc::new(Predicate::literal(true)),
        };
        let err = lower_operator(&filter.into()).unwrap_err();
        assert!(
            matches!(err, DataFusionError::NotImplemented(_)),
            "expected NotImplemented, got {err}"
        );
        // The message names the operator, so a caller can tell which arm is missing.
        assert!(err.to_string().contains("Filter"), "{err}");
    }
}
