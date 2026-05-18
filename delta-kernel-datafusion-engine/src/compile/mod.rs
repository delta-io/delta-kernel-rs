//! Declarative [`Plan`] -> DataFusion [`LogicalPlan`] compilation.
//!
//! Every kernel IR shape lowers to a [`LogicalPlan`] via [`compile_plan_logical`]; the executor
//! optimizes + materializes physical execution and wraps sink-specific [`ExecutionPlan`]s on
//! top of that.
//!
//! Sinks: [`SinkType::Relation`] / [`SinkType::Load`] (drain + the executor materializes the
//! result as a [`datafusion::datasource::MemTable`] inserted into the executor's
//! `relation_providers` map for downstream
//! [`DeclarativePlanNode::RelationRef`] leaves), [`SinkType::Consume`] (drained directly by
//! the executor through a [`delta_kernel::plans::kdf::ConsumerKdf`] handle).
//!
//! [`SinkType::Relation`]: delta_kernel::plans::ir::nodes::SinkType::Relation
//! [`SinkType::Load`]: delta_kernel::plans::ir::nodes::SinkType::Load
//! [`SinkType::Consume`]: delta_kernel::plans::ir::nodes::SinkType::Consume

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Fields as ArrowFields, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::Case;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::{lit, Expr};
use datafusion_functions::core::expr_fn::{get_field, named_struct};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{ColumnName, Expression};
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Engine;
use uuid::Uuid;

pub mod expr_translator;
mod json_parse;
pub mod logical;

pub use logical::compile_plan_logical;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    /// Relations available to [`DeclarativePlanNode::RelationRef`] leaves, keyed by
    /// [`RelationHandle::id`]. The executor passes a snapshot of its live registry here at
    /// compile time, so every plan in a batch sees the relations produced by its predecessors.
    /// An empty map is fine when the plan being compiled does not reference any relations (or for
    /// inspection-only paths like benchmark physical-plan dumps).
    pub relation_providers: Arc<HashMap<String, Arc<dyn TableProvider>>>,
    /// Active phase's [`PhaseState`] (`Some` while a phase is executing). `Consume`
    /// drains submit their finalized handles here; `None` means the executor is not inside a
    /// phase and any consume sink encountered surfaces an internal error.
    pub phase_state: Option<PhaseState>,
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers
    /// ([`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load)).
    pub engine: Arc<dyn Engine>,
    /// Owning state machine's identity. Stamped onto any `Consume` handle drained during the
    /// phase. Synthesized to `("standalone", "execute")` with a fresh `sm_id` for tests and
    /// SM-less entry points.
    pub sm_id: Uuid,
    pub sm_kind: &'static str,
    pub phase_name: &'static str,
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
            sm_id: Uuid::new_v4(),
            sm_kind: "standalone",
            phase_name: "execute",
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

/// Translate a kernel projection [`Expression`] to a DataFusion [`Expr`] that produces the
/// projection's declared logical [`output_field`].
///
/// `input_schema` is the Arrow schema of the producing child plan; it is needed by the
/// identity-[`Expression::Transform`] arm to resolve the physical names of the source struct's
/// fields when emitting the `named_struct(get_field(col(path), src_name), ...)` rebuild that
/// renames physical -> logical for column mapping.
///
/// Special arms (each returning a `named_struct` shaped per `output_field`):
/// - [`Expression::ParseJson`]: JSON-extraction chain per leaf of the target struct.
/// - [`Expression::MapToStruct`]: per-target-field `get_field` against the map argument.
/// - [`Expression::Struct`]: ordinal recursion into kernel-built children.
/// - [`Expression::Transform`] (identity, `input_path = Some(..)`, target `Struct`): mirrors
///   kernel's `(Transform(t), Struct(_)) if t.is_identity()` evaluator arm which calls
///   `apply_schema` -- here we lower it to a recursive `named_struct(get_field(...), ...)` that
///   adopts the projection's logical field names (and, transitively, nested names). This is the
///   physical -> logical rename for column-mapped struct columns.
pub(super) fn translate_projection_expr(
    expr: &Expression,
    output_field: &StructField,
    input_schema: &ArrowSchema,
) -> Result<Expr, DataFusionError> {
    if let Expression::ParseJson(parse_json) = expr {
        let json_expr = expr_translator::kernel_expr_to_df(parse_json.json_expr.as_ref())?;
        let target_struct = match output_field.data_type() {
            DataType::Struct(target_struct) => target_struct,
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
    if let Expression::MapToStruct(map_to_struct) = expr {
        if let DataType::Struct(target_struct) = output_field.data_type() {
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
    if let Expression::Struct(children, nullability_predicate) = expr {
        if nullability_predicate.is_some() {
            return Err(crate::error::unsupported(
                "Struct projection with nullability predicate is not yet supported",
            ));
        }
        if let DataType::Struct(target_struct) = output_field.data_type() {
            if target_struct.fields().count() == children.len() {
                let mut args = Vec::with_capacity(children.len() * 2);
                for (child_expr, child_field) in children.iter().zip(target_struct.fields()) {
                    args.push(lit(child_field.name().to_string()));
                    args.push(translate_projection_expr(
                        child_expr.as_ref(),
                        child_field,
                        input_schema,
                    )?);
                }
                return Ok(named_struct(args));
            }
        }
    }
    if let Expression::Transform(transform) = expr {
        if !transform.is_identity() {
            return Err(crate::error::unsupported(
                "Non-identity Transform expressions are not yet supported in projection lowering",
            ));
        }
        let target_struct = match output_field.data_type() {
            DataType::Struct(target_struct) => target_struct.as_ref(),
            other => {
                return Err(crate::error::plan_compilation(format!(
                    "Identity Transform projection requires Struct output type, got {other:?}"
                )));
            }
        };
        let input_path = transform.input_path().ok_or_else(|| {
            crate::error::unsupported(
                "Top-level identity Transform without input_path is not supported in projection \
                 lowering",
            )
        })?;
        let source_fields = lookup_struct_fields_via_path(input_schema, input_path)?;
        let input_expr = expr_translator::kernel_expr_to_df(&Expression::Column(input_path.clone()))?;
        return rebuild_struct_with_target_names(input_expr, &source_fields, target_struct);
    }
    expr_translator::kernel_expr_to_df(expr)
}

/// Resolve a kernel [`ColumnName`] path against `input_schema` and return the Arrow [`Fields`]
/// of the resulting struct type. Returns an error if any segment does not resolve to a struct.
fn lookup_struct_fields_via_path(
    input_schema: &ArrowSchema,
    path: &ColumnName,
) -> Result<ArrowFields, DataFusionError> {
    let segments = path.path();
    let Some((first, rest)) = segments.split_first() else {
        return Err(crate::error::plan_compilation(
            "Identity Transform input_path must have at least one segment",
        ));
    };
    let first_field = input_schema
        .fields()
        .iter()
        .find(|f| f.name() == first.as_str())
        .ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "Identity Transform input_path root `{first}` not found in input schema"
            ))
        })?;
    let mut current = first_field.data_type();
    for segment in rest {
        let ArrowDataType::Struct(fields) = current else {
            return Err(crate::error::plan_compilation(format!(
                "Identity Transform input_path traverses non-struct field at `{segment}`"
            )));
        };
        let next = fields
            .iter()
            .find(|f| f.name() == segment.as_str())
            .ok_or_else(|| {
                crate::error::plan_compilation(format!(
                    "Identity Transform input_path segment `{segment}` not found in struct"
                ))
            })?;
        current = next.data_type();
    }
    match current {
        ArrowDataType::Struct(fields) => Ok(fields.clone()),
        other => Err(crate::error::plan_compilation(format!(
            "Identity Transform input_path must resolve to a struct, found {other:?}"
        ))),
    }
}

/// Recursively rebuild a struct expression with target (logical) field names, adopting source
/// (physical) field names via `get_field`. The source and target struct shapes must agree in
/// field count; matching is positional, mirroring kernel's `apply_schema` semantics.
///
/// The naive `named_struct(get_field(col, "a"), get_field(col, "b"))` construction drops the
/// parent struct's null bitmap -- every row becomes a non-null struct of (possibly null)
/// children, instead of preserving rows where the whole struct was NULL. Workloads like
/// inserting a literal NULL struct (`MS_011_null_struct_insert`) depend on the parent-NULL
/// being observable in the projected output, so we wrap with `CASE WHEN col IS NULL THEN NULL
/// ELSE named_struct(...) END`. DataFusion infers the CASE result type from the THEN branch
/// (the rebuilt struct), and the ELSE NULL is coerced to that same struct type.
fn rebuild_struct_with_target_names(
    input_expr: Expr,
    source_fields: &ArrowFields,
    target_struct: &StructType,
) -> Result<Expr, DataFusionError> {
    let target_count = target_struct.fields().count();
    if source_fields.len() != target_count {
        return Err(crate::error::plan_compilation(format!(
            "Identity Transform field count mismatch: source struct has {} fields, target \
             projection schema has {target_count} fields",
            source_fields.len(),
        )));
    }
    let mut args: Vec<Expr> = Vec::with_capacity(target_count * 2);
    for (source_field, target_field) in source_fields.iter().zip(target_struct.fields()) {
        let child_expr = get_field(input_expr.clone(), source_field.name().to_string());
        let renamed = match (source_field.data_type(), target_field.data_type()) {
            (ArrowDataType::Struct(src_nested), DataType::Struct(tgt_nested)) => {
                rebuild_struct_with_target_names(child_expr, src_nested, tgt_nested.as_ref())?
            }
            _ => child_expr,
        };
        args.push(lit(target_field.name().to_string()));
        args.push(renamed);
    }
    let rebuilt = named_struct(args);
    Ok(Expr::Case(Case::new(
        None,
        vec![(
            Box::new(Expr::IsNotNull(Box::new(input_expr))),
            Box::new(rebuilt),
        )],
        Some(Box::new(lit(ScalarValue::Null))),
    )))
}
