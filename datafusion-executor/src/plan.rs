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
use itertools::Itertools;

use crate::operator::lower_operator;

/// Lowers a kernel [`Plan`](KernelPlan) into the equivalent DataFusion
/// [`LogicalPlan`](DFLogicalPlan), returning the plan rooted at the kernel plan's terminal node.
///
/// # Errors
/// Returns an error if `plan` has no nodes, or if lowering any individual node fails.
pub(crate) fn to_df_plan(plan: &KernelPlan) -> Result<DFLogicalPlan, DataFusionError> {
    // A node is identified by its index, and `nodes` is topologically ordered: every input index is
    // strictly less than the node's own, so a single forward pass leaves each node's inputs already
    // lowered by the time it is reached.
    let mut lowered: Vec<Arc<DFLogicalPlan>> = Vec::with_capacity(plan.nodes().len());
    for (node_index, node) in plan.nodes().iter().enumerate() {
        let op = node.operator();
        let available = lowered.len();
        let invalid_input = |input_index| {
            DataFusionError::Plan(format!(
                "node {node_index} ({op}) references input {input_index}, but only {available} \
                 prior node(s) are available"
            ))
        };
        let inputs: Vec<_> = node
            .inputs()
            .iter()
            .map(|&input_index| {
                let Some(input) = lowered.get(input_index) else {
                    return Err(invalid_input(input_index));
                };
                Ok(Arc::clone(input))
            })
            .try_collect()?;
        lowered.push(Arc::new(lower_operator(op, &inputs)?));
    }

    // The terminal node is the last one: no other node consumes it, and its rows are the plan's
    // output.
    match lowered.pop() {
        Some(terminal) => Ok(Arc::unwrap_or_clone(terminal)),
        None => Err(DataFusionError::Plan(
            "cannot lower a plan with no nodes".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::{
        col, Expression as KernelExpr, Predicate as KernelPredicate, Scalar as KernelScalar,
    };
    use delta_kernel::schema::{schema, DataType, StructField, StructType};
    use delta_kernel::PlanBuilder;

    use super::*;

    // === Shared helpers ===

    /// A single-field `long` schema named `a`.
    fn test_schema() -> StructType {
        schema! { nullable "a": LONG }
    }

    async fn execute(plan: &KernelPlan) -> Result<Vec<RecordBatch>, DataFusionError> {
        SessionContext::new()
            .execute_logical_plan(to_df_plan(plan)?)
            .await?
            .collect()
            .await
    }

    // === Tests ===

    #[tokio::test]
    async fn filter_plan_executes() {
        let plan = PlanBuilder::values(
            Arc::new(test_schema()),
            vec![vec![KernelScalar::null(DataType::LONG)], vec![2i64.into()]],
        )
        .unwrap()
        .filter(KernelPredicate::is_null(col!("a")))
        .unwrap()
        .build()
        .unwrap();
        let batches = execute(&plan).await.unwrap();
        assert_batches_eq!(&["+---+", "| a |", "+---+", "|   |", "+---+"], &batches);
    }

    #[tokio::test]
    async fn values_project_filter_composes_through_declared_project_schema() {
        let project_schema = Arc::new(
            StructType::try_new([StructField::nullable("projected", DataType::LONG)]).unwrap(),
        );
        let plan = PlanBuilder::values(Arc::new(test_schema()), vec![vec![KernelScalar::Long(1)]])
            .unwrap()
            .project(
                KernelExpr::struct_from([col!("a")]),
                Arc::clone(&project_schema),
            )
            .unwrap()
            .filter(KernelPredicate::is_not_null(col!("projected")))
            .unwrap()
            .build()
            .unwrap();

        let batches = execute(&plan).await.unwrap();
        assert_batches_eq!(
            &[
                "+-----------+",
                "| projected |",
                "+-----------+",
                "| 1         |",
                "+-----------+",
            ],
            &batches
        );
    }
}
