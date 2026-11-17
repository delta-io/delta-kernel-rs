use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::plans::{Operation, Plan};
use crate::{Engine, KernelResult};

/// Executes a declarative plan and returns its Arrow batches.
pub(crate) fn execute_plan(engine: &dyn Engine, plan: Plan) -> KernelResult<Vec<RecordBatch>> {
    engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(|batch| batch?.try_into_record_batch())
        .collect()
}
