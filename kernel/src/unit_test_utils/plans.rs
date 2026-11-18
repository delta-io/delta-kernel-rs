use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::plans::ir::nodes::Operator;
use crate::plans::{Operation, Plan, PlanExecutor, PlanResult};
use crate::{Engine, KernelError, KernelResult};

/// Executes I/O operations and file-scan plans through [`SyncPlanExecutor`].
///
/// Disables declarative protocol and metadata replay and rejects relational plans so tests detect
/// incorrect P&M dispatch.
pub(crate) struct IOOnlyPlanExecutor(pub SyncPlanExecutor);

impl PlanExecutor for IOOnlyPlanExecutor {
    fn support_query_plans(&self) -> bool {
        false
    }

    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        if let Operation::QueryPlan(plan) = &op {
            let supported = match plan.nodes.as_slice() {
                [node] if node.inputs.is_empty() => match &node.op {
                    Operator::ScanJson(_) | Operator::ScanParquet(_) => true,
                    // A scan with no input files builds as an empty Values plan.
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
