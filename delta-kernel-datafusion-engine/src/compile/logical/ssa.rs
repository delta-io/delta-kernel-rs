//! SSA [`Plan`](delta_kernel::plans::ir::plan::Plan) -> DataFusion [`LogicalPlan`] lowering.
//!
//! Topological walk over [`PlanNode`](delta_kernel::plans::ir::plan::PlanNode)s. Each statement's
//! output [`Ref`](delta_kernel::plans::ir::plan::Ref) is mapped to a freshly built
//! [`LogicalPlan`]. Inputs are guaranteed to be earlier in `plan.stmts` than outputs by
//! [`Plan::push`](delta_kernel::plans::ir::plan::Plan::push), so a single forward pass works.
//!
//! All [`NodeKind`](delta_kernel::plans::ir::plan::NodeKind) variants compile to a `LogicalPlan` --
//! sources, transforms, and the per-row file reader [`NodeKind::Load`]. Cross-statement data
//! flow happens entirely through DataFusion's logical-plan tree (no relation registry,
//! no named handles).
//!
//! # Schema policy
//!
//! SSA `Plan`s do not carry per-Ref kernel schemas (those live on the
//! [`ContextState`](crate::plans::state_machines::framework::plan_context) only during
//! construction). DataFusion derives output schemas from `LogicalPlan` shape and arrow
//! types; the only place a kernel [`SchemaRef`] is reconstructed engine-side is
//! [`NodeKind::Load`], which feeds into [`LoadTableProvider`] (legacy plumbing still needs the
//! `RelationHandle.schema` for its arrow-output materialization). The compiled
//! kernel schema is rebuilt from the upstream's arrow schema on the fly via
//! [`StructType::try_from_arrow`].
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
    FileListingNode, FileType, LoadSink, ProjectNode, RelationHandle, ScanFileColumns, ScanNode,
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
        NodeKind::ListFiles { start_from } => file_listing_to_logical_plan(&FileListingNode {
            path: start_from.clone(),
        }),
        NodeKind::Scan {
            file_type,
            files,
            schema,
        } => scan_to_listing_logical_plan(&ScanNode::new(
            *file_type,
            files.clone(),
            Arc::clone(schema),
        )),
        NodeKind::Values { schema, rows } => lower_values(schema, rows),

        // === Unary transforms ===========================================================
        NodeKind::Filter { predicate } => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            let pred = kernel_pred_to_df(predicate.as_ref())?;
            LogicalPlanBuilder::from(child).filter(pred)?.build()
        }
        NodeKind::Project {
            named_exprs,
            output_schema,
        } => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            // SSA `NodeKind::Project` carries its declared output schema; the engine
            // does not re-infer it. Reuse the legacy `compile_project_node` helper
            // (it consumes a `ProjectNode` whose `output_schema` informs collision
            // avoidance + name canonicalization).
            let project_node = ProjectNode {
                columns: named_exprs.iter().map(|(_, e)| Arc::clone(e)).collect(),
                output_schema: Arc::clone(output_schema),
            };
            compile_project_node(child, &project_node)
        }
        NodeKind::Load {
            file_schema,
            file_type,
            base_url,
            passthrough_columns,
            file_meta,
            dv_ref,
        } => lower_load(
            built,
            expect_one_input(stmt)?,
            file_schema,
            *file_type,
            base_url.as_ref(),
            passthrough_columns,
            file_meta,
            dv_ref.as_ref(),
            ctx,
        ),
        NodeKind::MaxByVersion {
            group_by,
            version_column,
            value_columns,
        } => lower_max_by_version(
            lookup(built, expect_one_input(stmt)?)?.clone(),
            group_by,
            version_column,
            value_columns,
        ),

        // === N-ary ======================================================================
        NodeKind::Union { ordered } => {
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
            if *ordered {
                compile_ordered_union(children)
            } else {
                let mut iter = children.into_iter();
                let first = iter.next().unwrap();
                iter.try_fold(first, |acc, right| {
                    LogicalPlanBuilder::from(acc).union(right)?.build()
                })
            }
        }
        NodeKind::EquiJoin { kind, key_pairs } => lower_equi_join(stmt, built, *kind, key_pairs),
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

fn lower_values(
    schema: &SchemaRef,
    rows: &[Vec<delta_kernel::expressions::Scalar>],
) -> Result<LogicalPlan, DataFusionError> {
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().map_err(|e| {
        plan_compilation(format!(
            "compile_ssa: Values arrow schema conversion failed: {e}"
        ))
    })?;
    let df_schema = Arc::new(
        DFSchema::try_from(arrow_schema)
            .map_err(|e| plan_compilation(format!("compile_ssa: Values DF schema: {e}")))?,
    );
    let translated = rows
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

#[allow(clippy::too_many_arguments)]
fn lower_load(
    built: &HashMap<Ref, LogicalPlan>,
    upstream_ref: Ref,
    file_schema: &SchemaRef,
    file_type: FileType,
    base_url: Option<&url::Url>,
    passthrough_columns: &[ColumnName],
    file_meta: &ScanFileColumns,
    dv_ref: Option<&delta_kernel::plans::ir::nodes::DvRef>,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let upstream_logical = lookup(built, upstream_ref)?.clone();
    let upstream_kernel = kernel_schema_from_logical(&upstream_logical)?;
    let output_schema =
        build_load_output_kernel_schema(file_schema, passthrough_columns, &upstream_kernel)?;
    // `LoadTableProvider` was authored against the legacy `LoadSink` shape; SSA Load has
    // no concept of a `RelationHandle`, so we synthesize an ephemeral one purely so the
    // provider can derive its arrow output schema. The handle's `id` and `name` are
    // throwaway -- nothing registers it.
    let synthetic_handle = RelationHandle::fresh("ssa_load", output_schema);
    let mut sink = LoadSink::new(synthetic_handle, Arc::clone(file_schema), file_type);
    if let Some(url) = base_url {
        sink = sink.with_base_url(url.clone());
    }
    sink = sink.with_passthrough_columns(passthrough_columns.to_vec());
    sink = sink.with_file_meta(file_meta.clone());
    if let Some(dv) = dv_ref {
        sink = sink.with_dv_ref(dv.clone());
    }
    let provider: Arc<dyn TableProvider> = Arc::new(LoadTableProvider::try_new(
        upstream_logical,
        Arc::new(sink),
        Arc::clone(&ctx.engine),
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
    group_by: &[Arc<Expression>],
    version_column: &Arc<Expression>,
    value_columns: &[String],
) -> Result<LogicalPlan, DataFusionError> {
    if value_columns.is_empty() {
        return Err(plan_compilation(
            "compile_ssa: MaxByVersion with zero value_columns is invalid",
        ));
    }
    let partition_by = kernel_exprs_to_df_untyped(group_by)?;
    let order_by_expr = kernel_expr_to_df_untyped(version_column.as_ref())?;
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
    let projection: Vec<Expr> = value_columns
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
    kind: JoinKind,
    key_pairs: &[(Arc<Expression>, Arc<Expression>)],
) -> Result<LogicalPlan, DataFusionError> {
    if stmt.inputs.len() != 2 {
        return Err(plan_compilation(format!(
            "compile_ssa: EquiJoin expects 2 inputs, got {}",
            stmt.inputs.len()
        )));
    }
    if key_pairs.is_empty() {
        return Err(plan_compilation(
            "compile_ssa: EquiJoin requires at least one key pair",
        ));
    }
    let left_plan = lookup(built, stmt.inputs[0])?.clone();
    let right_plan = lookup(built, stmt.inputs[1])?.clone();
    let left_keys: Vec<Expr> = key_pairs
        .iter()
        .map(|(l, _)| kernel_expr_to_df_untyped(l.as_ref()))
        .collect::<Result<_, _>>()?;
    let right_keys: Vec<Expr> = key_pairs
        .iter()
        .map(|(_, r)| kernel_expr_to_df_untyped(r.as_ref()))
        .collect::<Result<_, _>>()?;
    let (df_kind, build_plan, probe_plan, build_keys, probe_keys) = match kind {
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
    if matches!(kind, JoinKind::Inner) {
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
