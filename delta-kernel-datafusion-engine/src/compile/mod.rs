//! Declarative [`Plan`] -> DataFusion [`LogicalPlan`] compilation.
//!
//! Every kernel IR shape lowers to a [`LogicalPlan`] via [`compile_plan_logical`]; the executor
//! optimizes + materializes physical execution and wraps sink-specific [`ExecutionPlan`]s on
//! top of that.
//!
//! Sinks: [`SinkType::Relation`] / [`SinkType::Load`] (drain + the executor materializes the
//! result as a [`datafusion::datasource::MemTable`] under
//! [`crate::executor::DataFusionExecutor::session_ctx`] for downstream
//! [`DeclarativePlanNode::RelationRef`] leaves), [`SinkType::Consume`] (drained directly by
//! the executor through a [`delta_kernel::plans::kdf::ConsumerKdf`] handle).
//!
//! [`SinkType::Relation`]: delta_kernel::plans::ir::nodes::SinkType::Relation
//! [`SinkType::Load`]: delta_kernel::plans::ir::nodes::SinkType::Load
//! [`SinkType::Consume`]: delta_kernel::plans::ir::nodes::SinkType::Consume

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion_common::error::DataFusionError;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::lit;
use datafusion_functions::core::expr_fn::{get_field, named_struct};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::Engine;

pub mod expr_translator;
mod json_parse;
pub mod logical;

pub use logical::compile_plan_logical;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    /// Relations referenced by [`DeclarativePlanNode::RelationRef`] leaves, prefetched from
    /// the executor's
    /// [`SessionContext`](datafusion::execution::context::SessionContext) catalog before
    /// compilation begins. Keyed by [`RelationHandle::id`]. An empty map is fine when the
    /// plan being compiled does not reference any relations (or for inspection-only paths
    /// like benchmark physical-plan dumps).
    pub relation_providers: Arc<HashMap<String, Arc<dyn TableProvider>>>,
    /// Active phase's [`PhaseState`] (`Some` while a phase is executing). `Consume`
    /// drains submit their finalized handles here; `None` means the executor is not inside a
    /// phase and any consume sink encountered surfaces an internal error.
    pub phase_state: Option<PhaseState>,
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers
    /// ([`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load)).
    pub engine: Arc<dyn Engine>,
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
            phase_state: None,
            engine,
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

pub(super) fn translate_projection_expr(
    expr: &delta_kernel::expressions::Expression,
    output_field: &delta_kernel::schema::StructField,
) -> Result<datafusion_expr::Expr, DataFusionError> {
    if let delta_kernel::expressions::Expression::ParseJson(parse_json) = expr {
        let json_expr = expr_translator::kernel_expr_to_df(parse_json.json_expr.as_ref())?;
        let target_struct = match output_field.data_type() {
            delta_kernel::schema::DataType::Struct(target_struct) => target_struct,
            other => {
                return Err(crate::error::plan_compilation(format!(
                    "ParseJson projection requires Struct output type, got {other:?}"
                )));
            }
        };
        let extractions = json_parse::generate_schema_extractions(&json_expr, target_struct)?;
        let mut struct_args = Vec::with_capacity(extractions.len() * 2);
        for (parsed_expr, field_name) in extractions {
            struct_args.push(lit(field_name));
            struct_args.push(parsed_expr);
        }
        return Ok(named_struct(struct_args));
    }
    if let delta_kernel::expressions::Expression::MapToStruct(map_to_struct) = expr {
        if let delta_kernel::schema::DataType::Struct(target_struct) = output_field.data_type() {
            let map_expr = expr_translator::kernel_expr_to_df(map_to_struct.map_expr.as_ref())?;
            let mut args = Vec::with_capacity(target_struct.fields().count() * 2);
            for target_field in target_struct.fields() {
                let arrow_ty: delta_kernel::arrow::datatypes::DataType =
                    target_field.data_type().try_into_arrow().map_err(|e| {
                        crate::error::plan_compilation(format!(
                            "MapToStruct target field `{}` type conversion failed: {e}",
                            target_field.name()
                        ))
                    })?;
                // Intentionally rely on native DataFusion map access semantics for MapToStruct.
                // Delta partitionValues maps are expected to have unique keys; duplicate keys are
                // treated as corrupt/undefined input, and we do not enforce a custom duplicate-key
                // policy in this compiler path.
                let raw_value = get_field(map_expr.clone(), target_field.name().to_string());
                let coerced_value = cast(raw_value, arrow_ty);
                args.push(lit(target_field.name().to_string()));
                args.push(coerced_value);
            }
            return Ok(named_struct(args));
        }
        return Err(crate::error::plan_compilation(format!(
            "MapToStruct projection requires Struct output type, got {:?}",
            output_field.data_type()
        )));
    }
    if let delta_kernel::expressions::Expression::Struct(children, nullability_predicate) = expr {
        if nullability_predicate.is_some() {
            return Err(crate::error::unsupported(
                "Struct projection with nullability predicate is not yet supported",
            ));
        }
        if let delta_kernel::schema::DataType::Struct(target_struct) = output_field.data_type() {
            if target_struct.fields().count() == children.len() {
                let mut args = Vec::with_capacity(children.len() * 2);
                for (child_expr, child_field) in children.iter().zip(target_struct.fields()) {
                    args.push(lit(child_field.name().to_string()));
                    args.push(translate_projection_expr(child_expr.as_ref(), child_field)?);
                }
                return Ok(named_struct(args));
            }
        }
    }
    expr_translator::kernel_expr_to_df(expr)
}
