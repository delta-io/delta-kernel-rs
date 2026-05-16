//! Declarative [`Plan`] -> DataFusion [`LogicalPlan`] compilation.
//!
//! Every kernel IR shape lowers to a [`LogicalPlan`] via [`compile_plan_logical`]; the executor
//! optimizes + materializes physical execution and wraps sink-specific [`ExecutionPlan`]s on
//! top of that. There is no parallel physical compile path.
//!
//! Sinks: [`SinkType::Results`] (stream batches), [`SinkType::Relation`] /
//! [`SinkType::Load`] (drain + the executor materializes the result as a
//! [`datafusion::datasource::MemTable`] under
//! [`crate::executor::DataFusionExecutor::session_ctx`] for downstream
//! [`DeclarativePlanNode::RelationRef`] leaves), [`SinkType::ConsumeByKdf`] (drained directly by
//! the executor through a [`delta_kernel::plans::kdf::ConsumerKdf`] handle).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::catalog::TableProvider;
use datafusion_common::error::DataFusionError;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::lit;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_functions::core::expr_fn::{get_field, named_struct};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::JoinType;
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};
use delta_kernel::plans::kdf::FinishedHandle;
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::schema::SchemaRef;
use delta_kernel::Engine;

pub mod expr_translator;
mod json_parse;
pub mod logical;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    /// Relations referenced by [`DeclarativePlanNode::RelationRef`] leaves, prefetched from
    /// the executor's
    /// [`SessionContext`](datafusion::execution::context::SessionContext) catalog before
    /// compilation begins. Keyed by [`RelationHandle::id`]. An empty map is fine when the
    /// plan being compiled does not reference any relations (or for inspection-only paths
    /// like benchmark physical-plan dumps).
    pub relation_providers: Arc<HashMap<u64, Arc<dyn TableProvider>>>,
    /// Latest finalized [`FinishedHandle`] from a [`SinkType::ConsumeByKdf`] plan run on this
    /// executor.
    pub kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    /// When [`Some`], [`SinkType::ConsumeByKdf`] pipelines submit finalized handles into this
    /// [`PhaseState`] for
    /// [`StateMachine`](delta_kernel::plans::state_machines::framework::state_machine::StateMachine)
    /// phases instead of populating [`Self::kdf_harvest_slot`] only.
    ///
    /// [`None`] preserves single-plan harvesting via
    /// [`crate::executor::DataFusionExecutor::take_last_kdf_finished`].
    pub phase_state: Option<PhaseState>,
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers ([`SinkType::Load`]).
    pub engine: Arc<dyn Engine>,
}

impl CompileContext {
    pub fn new(
        relation_providers: Arc<HashMap<u64, Arc<dyn TableProvider>>>,
        kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
        engine: Arc<dyn Engine>,
    ) -> Self {
        Self {
            relation_providers,
            kdf_harvest_slot,
            phase_state: None,
            engine,
        }
    }
}

/// Compile a complete [`Plan`] to a DataFusion [`LogicalPlan`]. Always succeeds for valid
/// kernel IR or surfaces a typed [`DataFusionError`].
pub fn compile_plan_logical(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    logical::compile_plan_logical(plan, ctx)
}

pub(crate) fn node_output_schema(node: &DeclarativePlanNode) -> Result<SchemaRef, DataFusionError> {
    match node {
        DeclarativePlanNode::Scan(n) => n.effective_output_schema().map_err(|e| {
            crate::error::plan_compilation(format!(
                "scan output schema with row index is invalid: {e}"
            ))
        }),
        DeclarativePlanNode::Values(n) => Ok(n.schema.clone()),
        DeclarativePlanNode::RelationRef(h) => Ok(h.schema.clone()),
        DeclarativePlanNode::Project { node, .. } => Ok(node.output_schema.clone()),
        DeclarativePlanNode::Union { children, .. } => {
            let Some(first) = children.first() else {
                return Err(crate::error::unsupported("Union has no children"));
            };
            node_output_schema(first)
        }
        DeclarativePlanNode::Filter { child, .. } => node_output_schema(child),
        DeclarativePlanNode::Window { child, node } => {
            use delta_kernel::schema::{DataType, StructField, StructType};
            let input_schema = node_output_schema(child)?;
            let mut fields: Vec<StructField> = input_schema.fields().cloned().collect();
            for wf in &node.functions {
                fields.push(StructField::new(
                    wf.output_col.clone(),
                    DataType::LONG,
                    false,
                ));
            }
            StructType::try_new(fields)
                .map(Arc::new)
                .map_err(|e| crate::error::plan_compilation(format!("window output schema: {e}")))
        }
        DeclarativePlanNode::Join { build, probe, node } => match node.join_type {
            JoinType::LeftAnti => node_output_schema(probe),
            JoinType::Inner => {
                let build_schema = node_output_schema(build)?;
                let probe_schema = node_output_schema(probe)?;
                build_schema
                    .as_ref()
                    .add(probe_schema.fields().cloned())
                    .map(Arc::new)
                    .map_err(|e| {
                        crate::error::plan_compilation(format!(
                            "inner join combined output schema is invalid: {e}"
                        ))
                    })
            }
        },
        DeclarativePlanNode::FileListing(_) => Err(crate::error::unsupported(
            "FileListing schema inference for Filter/Project is not wired yet",
        )),
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
