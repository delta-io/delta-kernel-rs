use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::plans::ir::nodes::Operator;
use crate::plans::{Operation, Plan, PlanExecutor, PlanExecutorCapabilities, PlanResult};
use crate::{Engine, KernelError, KernelResult};

/// Executes I/O operations and file-scan plans through [`SyncPlanExecutor`].
///
/// Rejects relational plans so tests detect execution beyond the declared I/O-only capabilities.
pub(crate) struct IOOnlyPlanExecutor(pub SyncPlanExecutor);

impl PlanExecutor for IOOnlyPlanExecutor {
    fn capabilities(&self) -> PlanExecutorCapabilities {
        PlanExecutorCapabilities::IoOnly
    }

    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        if let Operation::QueryPlan(plan) = &op {
            let supported = match plan.nodes.as_slice() {
                [node] if node.inputs.is_empty() => match &node.op {
                    Operator::ScanJson(_) | Operator::ScanParquet(_) => true,
                    // Handler reads use build(), which turns empty scans into empty Values plans.
                    Operator::Values(values) => values.rows.is_empty(),
                    _ => false,
                },
                _ => false,
            };
            if !supported {
                return Err(KernelError::unsupported(
                    "executor only supports file scans",
                ));
            }
        }
        self.0.execute_op(op)
    }
}

/// Executes a declarative plan and returns its Arrow batches.
pub(crate) fn execute_plan(engine: &dyn Engine, plan: Plan) -> KernelResult<Vec<RecordBatch>> {
    engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(|batch| batch?.try_into_record_batch())
        .collect()
}
