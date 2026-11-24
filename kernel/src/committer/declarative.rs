use delta_kernel_derive::internal_api;
use url::Url;

use crate::plans::{
    Operation, PlanBuilder, RelationRef, ScopedPlanExecutor, file_meta_from_batches,
};
use crate::{FileMeta, KernelResult};

/// Writes out ordered [`RelationRef`] inputs as JSON and returns metadata for the written file.
///
/// - `executor` holds the relations and runs the write plan.
/// - `actions` yields the commit action relations in write order.
/// - `path` is the destination JSON file URL.
#[internal_api]
pub(crate) fn write_commit_json_declarative(
    executor: &dyn ScopedPlanExecutor,
    actions: impl IntoIterator<Item = RelationRef>,
    path: Url,
) -> KernelResult<FileMeta> {
    let plan = PlanBuilder::write_json(
        actions.into_iter().map(PlanBuilder::relation_source),
        path.clone(),
        false, // overwrite
        true,  // ordered
    )?
    .build()?;
    let file_meta = file_meta_from_batches(
        executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?,
    )?;
    debug_assert_eq!(file_meta.location, path);
    Ok(file_meta)
}
