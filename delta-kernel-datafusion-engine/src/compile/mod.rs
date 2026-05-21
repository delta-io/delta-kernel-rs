//! Kernel [`delta_kernel::plans::ir::Plan`] → DataFusion [`datafusion_expr::LogicalPlan`]
//! compilation. Sinks are routed by [`crate::executor::DataFusionExecutor`] (lazy
//! `Relation` / `Load` providers; eager `Consume`).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion_common::error::DataFusionError;
use delta_kernel::plans::operations::framework::step_result::StepResult;
use delta_kernel::Engine;
use uuid::Uuid;

pub mod expr_translator;
mod json_parse;
pub mod logical;
pub mod stamp_udf;

pub use logical::compile_plan_logical;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    /// Relations available to [`delta_kernel::plans::ir::DeclarativePlanNode::RelationRef`]
    /// leaves, keyed by [`delta_kernel::plans::ir::nodes::RelationHandle`]'s `id` field. The
    /// executor passes a snapshot of its live registry here at compile time, so every plan in
    /// a batch sees the relations produced by its predecessors. An empty map is fine when the
    /// plan being compiled does not reference any relations (or for inspection-only paths like
    /// benchmark physical-plan dumps).
    pub relation_providers: Arc<HashMap<String, Arc<dyn TableProvider>>>,
    /// Active phase's [`StepResult`] (`Some` while a phase is executing). `Consume`
    /// drains submit their finalized handles here; `None` means the executor is not inside a
    /// phase and any consume sink encountered surfaces an internal error.
    pub step_result: Option<StepResult>,
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers
    /// ([`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load)).
    pub engine: Arc<dyn Engine>,
    /// Owning state machine's identity. Stamped onto any `Consume` handle drained during the
    /// phase. Synthesized to `("standalone", "execute")` with a fresh `sm_id` for tests and
    /// SM-less entry points.
    pub sm_id: Uuid,
    pub sm_kind: &'static str,
    pub step_name: &'static str,
}

impl CompileContext {
    /// Build a context without an active phase state. Useful for inspection-only paths (e.g.
    /// benchmark plan printers) that do not execute a plan.
    pub fn new(
        relation_providers: Arc<HashMap<String, Arc<dyn TableProvider>>>,
        engine: Arc<dyn Engine>,
    ) -> Self {
        Self {
            relation_providers,
            step_result: None,
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
