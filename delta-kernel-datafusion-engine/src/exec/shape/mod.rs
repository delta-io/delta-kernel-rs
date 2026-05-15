pub(crate) mod consume_kdf;
pub(crate) mod load_sink;
pub(crate) mod nullability_validation;
pub(crate) mod ordered_union;

use std::sync::Arc;

pub use consume_kdf::KernelConsumeByKdfExec;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::ExecutionPlan;
pub use load_sink::KernelLoadSinkExec;
pub use nullability_validation::NullabilityValidationExec;
pub use ordered_union::OrderedUnionExec;

/// Verify that `with_new_children` was called with exactly one replacement child
/// and return that child. Errors with a `DataFusionError::Plan` carrying `exec_name`
/// when the count is wrong. Used by every single-input Exec shape in this module.
pub(crate) fn expect_single_child(
    children: Vec<Arc<dyn ExecutionPlan>>,
    exec_name: &'static str,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if children.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "{exec_name} requires exactly one child"
        )));
    }
    Ok(children.into_iter().next().unwrap())
}
