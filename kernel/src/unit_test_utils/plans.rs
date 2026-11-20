use serde_json::Value;

use crate::arrow::json::WriterBuilder;
use crate::arrow::json::writer::JsonArray;
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

/// Canonicalizes JSON rows while retaining duplicate rows and the requested null representation.
pub(crate) fn json_rows(batches: &[RecordBatch], explicit_nulls: bool) -> KernelResult<Vec<Value>> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(explicit_nulls)
        .build::<_, JsonArray>(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    let mut rows: Vec<Value> = serde_json::from_slice(&writer.into_inner())?;
    rows.iter_mut().for_each(Value::sort_all_objects);
    rows.sort_by_cached_key(Value::to_string);
    Ok(rows)
}
