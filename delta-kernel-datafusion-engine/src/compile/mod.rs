//! Kernel SSA plan -> DataFusion [`datafusion_expr::LogicalPlan`] compilation.

use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use delta_kernel::Engine;
use uuid::Uuid;

pub mod expr_translator;
mod json_parse;
pub mod logical;
pub mod stamp_udf;

pub use logical::compile_ssa;

/// Context shared by the compiler for leaf nodes that need runtime side state.
///
/// Carries only static / shared bits -- there is no per-phase mutable accumulator
/// here. Drained consumer state for `Consume` steps flows directly out of
/// [`DataFusionExecutor::run_phase`](crate::executor::DataFusionExecutor) as a
/// [`StepPayload::Consumer`](delta_kernel::plans::operations::framework::step_payload::StepPayload::Consumer)
/// after the executor finishes the sink locally.
#[derive(Clone)]
pub struct CompileContext {
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers
    /// (`Node::Load`).
    pub engine: Arc<dyn Engine>,
    /// Owning state machine's identity. Stamped onto any `Consume` handle drained during the
    /// phase. Synthesized to `("standalone", "execute")` with a fresh `sm_id` for tests and
    /// SM-less entry points.
    pub sm_id: Uuid,
    pub sm_kind: &'static str,
    pub step_name: &'static str,
}

impl CompileContext {
    /// Build a context for SM-less inspection / standalone driving (benchmark plan printers,
    /// integration tests that lower a `ResultPlan` directly).
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self {
            engine,
            sm_id: Uuid::new_v4(),
            sm_kind: "standalone",
            step_name: "execute",
        }
    }
}

pub(super) fn expand_projection_columns(
    columns: &[Arc<delta_kernel::expressions::Expression>],
    expected_output_fields: usize,
) -> Result<Vec<Arc<delta_kernel::expressions::Expression>>, DataFusionError> {
    let mut expanded = Vec::new();
    for (idx, expr) in columns.iter().enumerate() {
        let remaining_output = expected_output_fields
            .checked_sub(expanded.len())
            .ok_or_else(|| crate::error::plan_compilation("Projection expansion overflow"))?;
        let remaining_expr = columns.len() - idx;
        let extra_needed = remaining_output
            .checked_sub(remaining_expr)
            .ok_or_else(|| {
                crate::error::plan_compilation(format!(
                    "Projection has too many expressions: expected {expected_output_fields} output fields, got at least {}",
                    expanded.len() + remaining_expr
                ))
            })?;

        match expr.as_ref() {
            delta_kernel::expressions::Expression::Struct(children, _) => {
                let spread_extra = children.len().saturating_sub(1);
                if spread_extra > 0 && spread_extra <= extra_needed {
                    expanded.extend(children.iter().cloned());
                } else {
                    expanded.push(Arc::clone(expr));
                }
            }
            _ => expanded.push(Arc::clone(expr)),
        }
    }

    if expanded.len() != expected_output_fields {
        return Err(crate::error::plan_compilation(format!(
            "Projection output schema has {} fields but expanded to {} expressions",
            expected_output_fields,
            expanded.len()
        )));
    }
    Ok(expanded)
}
