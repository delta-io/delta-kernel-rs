//! Conversion from a kernel [`Plan`](KernelPlan) to a DataFusion
//! [`LogicalPlan`](DFLogicalPlan).
//!
//! A DataFusion `LogicalPlan` is the *what* of a query: a tree of relational operators (scans,
//! filters, projections) that names the result without saying how to compute it. DataFusion
//! optimizes that tree and only then lowers it to an `ExecutionPlan`, the runnable form. It is the
//! natural target for a kernel plan, which is likewise declarative.
//!
//! This module owns the walk over a plan's nodes; lowering an individual node is
//! [`crate::operator`].

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use delta_kernel::plans::ir::plan::Plan as KernelPlan;

use crate::operator::{lower_operator, LoweredNode};

/// Lowers a kernel [`Plan`](KernelPlan) into the equivalent DataFusion
/// [`LogicalPlan`](DFLogicalPlan), returning the plan rooted at the kernel plan's terminal node.
///
/// # Errors
/// Returns an error if `plan` has no nodes, or if lowering any individual node fails.
pub(crate) fn to_df_plan(plan: &KernelPlan) -> Result<DFLogicalPlan, DataFusionError> {
    // A node is identified by its index, and `nodes` is topologically ordered: every input index is
    // strictly less than the node's own, so a single forward pass leaves each node's inputs already
    // lowered by the time it is reached.
    let mut lowered: Vec<LoweredNode> = Vec::with_capacity(plan.nodes.len());
    for (node_index, node) in plan.nodes.iter().enumerate() {
        let op = &node.op;
        let available = lowered.len();
        let invalid_parent = |parent_index| {
            DataFusionError::Plan(format!(
                "node {node_index} ({op}) references parent {parent_index}, but only {available} \
                 prior node(s) are available"
            ))
        };
        let parents: Result<Vec<_>, DataFusionError> = node
            .inputs
            .iter()
            .map(|&parent_index| {
                let Some(parent) = lowered.get(parent_index) else {
                    return Err(invalid_parent(parent_index));
                };
                Ok(parent)
            })
            .collect();
        let parents = parents?;
        lowered.push(lower_operator(op, &parents)?);
    }

    // The terminal node is the last one: no other node consumes it, and its rows are the plan's
    // output.
    match lowered.pop() {
        Some(terminal) => Ok(Arc::unwrap_or_clone(terminal.plan)),
        None => Err(DataFusionError::Plan(
            "cannot lower a plan with no nodes".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::lit;
    use delta_kernel::expressions::{col, Predicate as KernelPredicate, Scalar as KernelScalar};
    use delta_kernel::plans::ir::nodes::{Filter as KernelFilter, Values as KernelValues};
    use delta_kernel::plans::ir::plan::PlanNode as KernelPlanNode;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// A single-field `long` schema named `a`.
    fn test_schema() -> StructType {
        StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap()
    }

    /// A `Values` node over [`test_schema`] holding `rows` one-column rows.
    fn values_node(rows: Vec<Vec<KernelScalar>>) -> KernelPlanNode {
        KernelPlanNode::new(KernelValues::new(test_schema(), rows), vec![])
    }

    // === Tests ===

    #[test]
    fn empty_plan_is_rejected() {
        let err = to_df_plan(&KernelPlan { nodes: vec![] }).unwrap_err();
        assert!(err.to_string().contains("no nodes"), "{err}");
    }

    #[test]
    fn terminal_node_is_the_plans_output() -> Result<(), DataFusionError> {
        // Two independent sources; the last node is the terminal one, so its rows are the output.
        let plan = KernelPlan {
            nodes: vec![
                values_node(vec![vec![1i64.into()]]),
                values_node(vec![vec![2i64.into()], vec![3i64.into()]]),
            ],
        };
        let lowered = to_df_plan(&plan)?;
        let DFLogicalPlan::Values(values) = &lowered else {
            panic!("expected Values, got {lowered:?}");
        };
        // The rows are the terminal node's 2 and 3, not the first node's 1.
        let rows = format!("{:?}", values.values);
        assert!(
            rows.contains("Int64(2)") && rows.contains("Int64(3)"),
            "{rows}"
        );
        assert!(!rows.contains("Int64(1)"), "{rows}");
        Ok(())
    }

    #[rstest]
    #[case::self_reference(0)]
    #[case::forward_reference(1)]
    #[case::far_out_of_range(42)]
    fn invalid_parent_reference_is_rejected(#[case] parent_index: usize) {
        let node =
            KernelPlanNode::new(KernelValues::new(test_schema(), vec![]), vec![parent_index]);
        let err = to_df_plan(&KernelPlan { nodes: vec![node] }).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("node 0 (Values)"), "{message}");
        assert!(
            message.contains(&format!("references parent {parent_index}")),
            "{message}"
        );
        assert!(message.contains("0 prior node(s)"), "{message}");
    }

    #[test]
    fn filter_uses_its_declared_parent_index() {
        let filter = KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        };
        let plan = KernelPlan {
            nodes: vec![
                values_node(vec![vec![1i64.into()]]),
                values_node(vec![vec![2i64.into()]]),
                KernelPlanNode::new(filter, vec![0]),
            ],
        };
        let lowered = to_df_plan(&plan).unwrap();
        let DFLogicalPlan::Filter(filter) = lowered else {
            panic!("expected Filter");
        };
        let DFLogicalPlan::Values(values) = filter.input.as_ref() else {
            panic!("expected Values parent");
        };
        assert_eq!(values.values, [vec![lit(1i64)]]);
    }
}
