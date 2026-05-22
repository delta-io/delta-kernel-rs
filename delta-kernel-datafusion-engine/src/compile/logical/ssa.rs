//! SSA [`Plan`](delta_kernel::plans::ir::plan::Plan) -> DataFusion [`LogicalPlan`] lowering.
//!
//! Topological walk over [`PlanNode`](delta_kernel::plans::ir::plan::PlanNode)s. Each statement's
//! output [`Ref`](delta_kernel::plans::ir::plan::Ref) is mapped to a freshly built
//! [`LogicalPlan`]. Inputs are guaranteed to be earlier in `plan.stmts` than outputs by
//! [`Plan::push`](delta_kernel::plans::ir::plan::Plan::push), so a single forward pass works.
//!
//! Every [`NodeKind`](delta_kernel::plans::ir::plan::NodeKind) variant wraps a payload struct
//! defined in [`delta_kernel::plans::ir::nodes`]; engine helpers consume those payload structs
//! by reference (`&LoadNode`, `&ScanNode`, etc.) without repacking. Cross-statement data flow
//! happens entirely through DataFusion's logical-plan tree (no relation registry, no named
//! handles).
//!
//! # Schema policy
//!
//! SSA `Plan`s do not carry per-Ref kernel schemas (those live on the
//! [`ContextState`](crate::plans::state_machines::framework::plan_context) only during
//! construction). DataFusion derives output schemas from `LogicalPlan` shape and arrow
//! types; the only place a kernel [`SchemaRef`] is reconstructed engine-side is
//! [`NodeKind::Load`], whose output schema is computed here from the upstream's arrow shape
//! via [`StructType::try_from_arrow`] and threaded into [`LoadTableProvider::try_new`].
//!
//! [`NodeKind::Load`]: delta_kernel::plans::ir::plan::NodeKind::Load
//! [`SchemaRef`]: delta_kernel::schema::SchemaRef
//! [`StructType::try_from_arrow`]: delta_kernel::engine::arrow_conversion::TryFromArrow
//! [`LoadTableProvider`]: crate::exec::LoadTableProvider

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan, Values};
use datafusion_expr::{lit, Expr, ExprFunctionExt, JoinType as DfJoinType, LogicalPlanBuilder};
use datafusion_functions_window::row_number::row_number;
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::expressions::{ColumnName, Expression};
use delta_kernel::plans::ir::nodes::{
    EquiJoinNode, LoadNode, MaxByVersionNode, UnionNode, ValuesNode,
};
use delta_kernel::plans::ir::plan::{JoinKind, NodeKind, PlanNode, Ref};
use delta_kernel::schema::{SchemaRef, StructType};

use super::canonicalize::canonicalize_output_to_kernel_schema;
use super::ordered_union::compile_ordered_union;
use super::project::compile_project_node;
use super::providers::file_listing_to_logical_plan;
use super::scan::scan_to_listing_logical_plan;
use crate::compile::expr_translator::{
    kernel_expr_to_df_untyped, kernel_exprs_to_df_untyped, kernel_pred_to_df,
};
use crate::compile::CompileContext;
use crate::error::plan_compilation;
use crate::exec::LoadTableProvider;

/// Compile a slice of SSA [`PlanNode`]s to a DataFusion [`LogicalPlan`] rooted at `terminal`.
///
/// Walks `stmts` in order, lowering each statement and threading the resulting `LogicalPlan`
/// into a `Ref`-keyed map. The plan returned for `terminal` is then handed back. Statements
/// unreachable from `terminal` are still compiled (DCE is the builder's job, not the engine's);
/// engines relying on dead-code elimination should call
/// [`Plan::reachable_from`](delta_kernel::plans::ir::plan::Plan::reachable_from) before passing
/// the stmts in. Taking `&[PlanNode]` rather than `&Plan` avoids needing a `Plan::from_stmts`
/// constructor; both the [`ResultPlan`](delta_kernel::plans::ir::plan::ResultPlan)-returning
/// drive path (where the caller already has a `Plan`) and the [`EngineRequest::Consume`] dispatch
/// (where the executor only sees raw stmts) share this entry point.
///
/// [`EngineRequest::Consume`]: delta_kernel::plans::state_machines::framework::step::EngineRequest::Consume
pub fn compile_ssa(
    stmts: &[PlanNode],
    terminal: Ref,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let mut built: HashMap<Ref, LogicalPlan> = HashMap::with_capacity(stmts.len());
    for stmt in stmts {
        let logical = lower_stmt(stmt, &built, ctx)?;
        built.insert(stmt.output, logical);
    }
    built.remove(&terminal).ok_or_else(|| {
        plan_compilation(format!(
            "compile_ssa: terminal {terminal:?} is not produced by any stmt in the plan",
        ))
    })
}

/// Look up a previously compiled child plan; the caller clones for ownership.
fn lookup(built: &HashMap<Ref, LogicalPlan>, r: Ref) -> Result<&LogicalPlan, DataFusionError> {
    built.get(&r).ok_or_else(|| {
        plan_compilation(format!(
            "compile_ssa: input {r:?} not yet compiled (out-of-order stmts?)",
        ))
    })
}

fn lower_stmt(
    stmt: &PlanNode,
    built: &HashMap<Ref, LogicalPlan>,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    match &stmt.kind {
        // === Sources ====================================================================
        NodeKind::ListFiles(node) => file_listing_to_logical_plan(node),
        NodeKind::Scan(node) => scan_to_listing_logical_plan(node),
        NodeKind::Values(node) => lower_values(node),

        // === Unary transforms ===========================================================
        NodeKind::Filter(node) => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            let pred = kernel_pred_to_df(node.predicate.as_ref())?;
            LogicalPlanBuilder::from(child).filter(pred)?.build()
        }
        NodeKind::Project(node) => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            compile_project_node(child, node)
        }
        NodeKind::Load(node) => lower_load(built, expect_one_input(stmt)?, node, ctx),
        NodeKind::MaxByVersion(node) => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            lower_max_by_version(child, node)
        }

        // === N-ary ======================================================================
        NodeKind::Union(node) => lower_union(stmt, built, node),
        NodeKind::EquiJoin(node) => lower_equi_join(stmt, built, node),
    }
}

fn lower_union(
    stmt: &PlanNode,
    built: &HashMap<Ref, LogicalPlan>,
    node: &UnionNode,
) -> Result<LogicalPlan, DataFusionError> {
    if stmt.inputs.is_empty() {
        return Err(plan_compilation(
            "compile_ssa: Union with zero inputs is not a valid SSA shape",
        ));
    }
    let children: Vec<LogicalPlan> = stmt
        .inputs
        .iter()
        .map(|r| lookup(built, *r).cloned())
        .collect::<Result<_, _>>()?;
    if children.len() == 1 {
        return Ok(children.into_iter().next().unwrap());
    }
    if node.ordered {
        compile_ordered_union(children)
    } else {
        let mut iter = children.into_iter();
        let first = iter.next().unwrap();
        iter.try_fold(first, |acc, right| {
            LogicalPlanBuilder::from(acc).union(right)?.build()
        })
    }
}

fn expect_one_input(stmt: &PlanNode) -> Result<Ref, DataFusionError> {
    match stmt.inputs.as_slice() {
        [r] => Ok(*r),
        other => Err(plan_compilation(format!(
            "compile_ssa: {:?} expects exactly one input, got {}",
            stmt.kind,
            other.len()
        ))),
    }
}

/// Convert a [`LogicalPlan`]'s arrow schema back to a kernel [`StructType`]. Used at
/// compile time when downstream lowerings (Project's collision avoidance, Load's output
/// schema) need a kernel-typed view of the upstream output.
fn kernel_schema_from_logical(plan: &LogicalPlan) -> Result<StructType, DataFusionError> {
    let arrow: ArrowSchema = plan.schema().as_arrow().clone();
    StructType::try_from_arrow(&arrow).map_err(|e| {
        plan_compilation(format!(
            "compile_ssa: arrow -> kernel schema conversion failed: {e}",
        ))
    })
}

/// Walk a kernel struct schema for a (possibly nested) column path, returning the leaf
/// data type.
fn walk_column_type(
    schema: &StructType,
    col: &ColumnName,
) -> Option<delta_kernel::schema::DataType> {
    use delta_kernel::schema::DataType;
    let path = col.path();
    if path.is_empty() {
        return None;
    }
    let mut current = schema.field(path.first()?)?.data_type().clone();
    for seg in &path[1..] {
        match current {
            DataType::Struct(s) => {
                current = s.field(seg.as_str())?.data_type().clone();
            }
            _ => return None,
        }
    }
    Some(current)
}

fn lower_values(node: &ValuesNode) -> Result<LogicalPlan, DataFusionError> {
    let arrow_schema: ArrowSchema = node.schema.as_ref().try_into_arrow().map_err(|e| {
        plan_compilation(format!(
            "compile_ssa: Values arrow schema conversion failed: {e}"
        ))
    })?;
    let df_schema = Arc::new(
        DFSchema::try_from(arrow_schema)
            .map_err(|e| plan_compilation(format!("compile_ssa: Values DF schema: {e}")))?,
    );
    let translated = node
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|s| kernel_expr_to_df_untyped(&Expression::literal(s.clone())))
                .collect::<Result<Vec<_>, DataFusionError>>()
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    Ok(if translated.is_empty() {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        })
    } else {
        LogicalPlan::Values(Values {
            schema: df_schema,
            values: translated,
        })
    })
}

fn lower_load(
    built: &HashMap<Ref, LogicalPlan>,
    upstream_ref: Ref,
    node: &LoadNode,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let upstream_logical = lookup(built, upstream_ref)?.clone();
    let upstream_kernel = kernel_schema_from_logical(&upstream_logical)?;
    let output_kernel_schema = build_load_output_kernel_schema(
        &node.file_schema,
        &node.passthrough_columns,
        &upstream_kernel,
    )?;
    let provider: Arc<dyn TableProvider> = Arc::new(LoadTableProvider::try_new(
        upstream_logical,
        Arc::new(node.clone()),
        Arc::clone(&ctx.engine),
        output_kernel_schema,
    )?);
    LogicalPlanBuilder::scan("ssa_load", provider_as_source(provider), None)?.build()
}

/// Build the kernel-typed output schema for an SSA `NodeKind::Load`: the file_schema fields
/// followed by one field per passthrough column whose type is looked up by walking the
/// upstream's kernel schema.
fn build_load_output_kernel_schema(
    file_schema: &SchemaRef,
    passthrough_columns: &[ColumnName],
    upstream: &StructType,
) -> Result<SchemaRef, DataFusionError> {
    use delta_kernel::schema::StructField;
    let mut fields: Vec<StructField> = file_schema.fields().cloned().collect();
    for col in passthrough_columns {
        let ty = walk_column_type(upstream, col).ok_or_else(|| {
            plan_compilation(format!(
                "compile_ssa: Load passthrough column {col:?} not found in upstream schema",
            ))
        })?;
        let leaf = col.path().last().ok_or_else(|| {
            plan_compilation("compile_ssa: Load passthrough column path is empty".to_string())
        })?;
        fields.push(StructField::nullable(leaf.clone(), ty));
    }
    StructType::try_new(fields).map(Arc::new).map_err(|e| {
        plan_compilation(format!(
            "compile_ssa: Load output schema construction failed: {e}",
        ))
    })
}

/// Lower SSA `NodeKind::MaxByVersion` to `row_number() OVER (PARTITION BY ... ORDER BY
/// version DESC)` followed by `WHERE rn = 1` and a final projection narrowing to the
/// `value_columns`. DataFusion mints a long version-dependent schema name for the window
/// column (e.g. `row_number() PARTITION BY [...] ROWS BETWEEN ...`); rather than try to
/// synthesize that name we read it back from the resulting plan's schema (it's the last
/// column appended by [`LogicalPlanBuilder::window_plan`]).
fn lower_max_by_version(
    child: LogicalPlan,
    node: &MaxByVersionNode,
) -> Result<LogicalPlan, DataFusionError> {
    if node.value_columns.is_empty() {
        return Err(plan_compilation(
            "compile_ssa: MaxByVersion with zero value_columns is invalid",
        ));
    }
    let partition_by = kernel_exprs_to_df_untyped(&node.group_by)?;
    let order_by_expr = kernel_expr_to_df_untyped(node.version_column.as_ref())?;
    let row_number_expr = row_number()
        .partition_by(partition_by)
        .order_by(vec![order_by_expr.sort(false /* descending */, false)])
        .build()?;
    let window_plan = LogicalPlanBuilder::window_plan(child, vec![row_number_expr])?;
    let rn_column = window_plan
        .schema()
        .columns()
        .into_iter()
        .next_back()
        .ok_or_else(|| {
            plan_compilation("compile_ssa: MaxByVersion window_plan produced an empty schema")
        })?;
    let filtered = LogicalPlanBuilder::from(window_plan)
        .filter(Expr::Column(rn_column).eq(lit(1u64)))?
        .build()?;
    let projection: Vec<Expr> = node
        .value_columns
        .iter()
        .map(|n| Expr::Column(Column::new_unqualified(n)))
        .collect();
    LogicalPlanBuilder::from(filtered)
        .project(projection)?
        .build()
}

fn lower_equi_join(
    stmt: &PlanNode,
    built: &HashMap<Ref, LogicalPlan>,
    node: &EquiJoinNode,
) -> Result<LogicalPlan, DataFusionError> {
    if stmt.inputs.len() != 2 {
        return Err(plan_compilation(format!(
            "compile_ssa: EquiJoin expects 2 inputs, got {}",
            stmt.inputs.len()
        )));
    }
    if node.key_pairs.is_empty() {
        return Err(plan_compilation(
            "compile_ssa: EquiJoin requires at least one key pair",
        ));
    }
    let left_plan = lookup(built, stmt.inputs[0])?.clone();
    let right_plan = lookup(built, stmt.inputs[1])?.clone();
    let left_keys: Vec<Expr> = node
        .key_pairs
        .iter()
        .map(|(l, _)| kernel_expr_to_df_untyped(l.as_ref()))
        .collect::<Result<_, _>>()?;
    let right_keys: Vec<Expr> = node
        .key_pairs
        .iter()
        .map(|(_, r)| kernel_expr_to_df_untyped(r.as_ref()))
        .collect::<Result<_, _>>()?;
    let (df_kind, build_plan, probe_plan, build_keys, probe_keys) = match node.kind {
        // SSA `Inner`: emit `(left, right)` rows whose keys match. Build = left.
        JoinKind::Inner => (
            DfJoinType::Inner,
            left_plan,
            right_plan,
            left_keys,
            right_keys,
        ),
        // SSA `LeftAnti`: emit each left row whose key matches no right row. Output schema
        // mirrors the left side. DataFusion's `LeftAnti` semantics match this directly with
        // build = left.
        JoinKind::LeftAnti => (
            DfJoinType::LeftAnti,
            left_plan,
            right_plan,
            left_keys,
            right_keys,
        ),
    };
    let plan = LogicalPlanBuilder::from(build_plan)
        .join_with_expr_keys(probe_plan, df_kind, (build_keys, probe_keys), None)?
        .build()?;

    // Canonicalize column order for `Inner` joins to match the builder's declared output
    // (`left.fields ++ right.fields`). DataFusion may produce a different physical
    // ordering depending on join build/probe choice. For `LeftAnti`, the output mirrors
    // the left input -- DataFusion's natural output ordering matches.
    if matches!(node.kind, JoinKind::Inner) {
        let kernel_left = kernel_schema_from_logical(lookup(built, stmt.inputs[0])?)?;
        let kernel_right = kernel_schema_from_logical(lookup(built, stmt.inputs[1])?)?;
        let mut combined: Vec<delta_kernel::schema::StructField> =
            kernel_left.fields().cloned().collect();
        combined.extend(kernel_right.fields().cloned());
        let target = StructType::try_new(combined).map(Arc::new).map_err(|e| {
            plan_compilation(format!(
                "compile_ssa: EquiJoin output schema construction failed: {e}",
            ))
        })?;
        canonicalize_output_to_kernel_schema(plan, &target)
    } else {
        Ok(plan)
    }
}
