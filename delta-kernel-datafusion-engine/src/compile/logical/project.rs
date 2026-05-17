//! Lowering for
//! [`DeclarativePlanNode::Project`](delta_kernel::plans::ir::DeclarativePlanNode::Project) plus the
//! pre-CSE column-path hoisting and root-rename visitors that the projection arm depends on.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_common::Column;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    ColumnName, Expression, MapToStructExpression, ParseJsonExpression,
};
use delta_kernel::plans::ir::nodes::ProjectNode;
use delta_kernel::transforms::ExpressionTransform;

use crate::compile::expr_translator::kernel_expr_to_df;
use crate::error::plan_compilation;

/// Walk every kernel projection expression and collect the set of unique top-level column
/// roots (the first segment of any [`ColumnName`] reference). Used by the `Project` lowering
/// to detect potential collisions with output field names.
fn collect_top_level_column_roots(exprs: &[Arc<Expression>]) -> HashSet<String> {
    let mut collector = TopLevelRootCollector {
        roots: HashSet::new(),
    };
    for expr in exprs {
        let _ = collector.transform_expr(expr.as_ref());
    }
    collector.roots
}

struct TopLevelRootCollector {
    roots: HashSet<String>,
}

impl<'a> ExpressionTransform<'a> for TopLevelRootCollector {
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        if let Some(first) = name.path().first() {
            self.roots.insert(first.to_string());
        }
        Some(Cow::Borrowed(name))
    }
}

/// Rewrite the root segment of any [`ColumnName`] reference whose first segment matches a key
/// in `rename`. Non-matching columns and nested path segments are left untouched.
struct RewriteRootColumn<'a> {
    rename: &'a BTreeMap<String, String>,
}

impl<'a> ExpressionTransform<'a> for RewriteRootColumn<'_> {
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        let path = name.path();
        if let Some(first) = path.first() {
            if let Some(renamed) = self.rename.get(first.as_str()) {
                let mut new_path = Vec::with_capacity(path.len());
                new_path.push(renamed.clone());
                new_path.extend(path.iter().skip(1).map(|s| s.to_string()));
                return Some(Cow::Owned(ColumnName::new(new_path)));
            }
        }
        Some(Cow::Borrowed(name))
    }
}

/// Visitor that counts how many times each [`ColumnName`] path appears across one or more
/// kernel projection expressions. Used to drive engine-side hoisting that replaces repeated
/// nested struct/json access subexpressions with stable column references *before* DataFusion's
/// own CSE runs. This avoids a known interaction between `CommonSubexprEliminate` and
/// `PushDownLeafProjections` where the merge step in `build_extraction_projection_impl`
/// produces duplicate `__common_expr_*` schema fields when an output struct column with many
/// `get_field(...)` references is filtered on by its parent `Filter`.
#[derive(Default)]
struct ColumnPathCounter {
    counts: BTreeMap<Vec<String>, usize>,
}

impl<'a> ExpressionTransform<'a> for ColumnPathCounter {
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        let path: Vec<String> = name.path().to_vec();
        *self.counts.entry(path).or_insert(0) += 1;
        Some(Cow::Borrowed(name))
    }

    /// `ParseJson(arg, schema)` expands during DataFusion translation into one
    /// `json_get_*(arg, ...)` call per leaf field of `schema`. Charge `arg` once per leaf field
    /// so the hoist analysis sees the true number of times its column references appear in the
    /// DataFusion plan that CSE will visit.
    fn transform_expr_parse_json(
        &mut self,
        expr: &'a ParseJsonExpression,
    ) -> Option<Cow<'a, ParseJsonExpression>> {
        let multiplier = leaf_field_count(expr.output_schema.as_ref()).max(1);
        for _ in 0..multiplier {
            let _ = self.transform_expr(expr.json_expr.as_ref());
        }
        Some(Cow::Borrowed(expr))
    }

    /// `MapToStruct(arg)` expands during DataFusion translation into one `get_field(arg, key)`
    /// call per target struct field. We do not have access to the target schema at this layer
    /// (it lives in the parent projection's output field), so pessimistically count `arg` twice:
    /// any nested column it references will then trigger a hoist if the parent projection has
    /// at least two such map-to-struct expansions or repeats the map column elsewhere.
    fn transform_expr_map_to_struct(
        &mut self,
        expr: &'a MapToStructExpression,
    ) -> Option<Cow<'a, MapToStructExpression>> {
        for _ in 0..2 {
            let _ = self.transform_expr(expr.map_expr.as_ref());
        }
        Some(Cow::Borrowed(expr))
    }
}

/// Count the number of leaf (non-struct) fields in `schema`, descending into nested struct
/// fields. This matches the
/// [`json_parse::generate_schema_extractions`](crate::compile::json_parse::generate_schema_extractions)
/// expansion that the engine emits for
/// [`ParseJson`](delta_kernel::expressions::Expression::ParseJson).
fn leaf_field_count(schema: &delta_kernel::schema::StructType) -> usize {
    schema
        .fields()
        .map(|f| match f.data_type() {
            delta_kernel::schema::DataType::Struct(inner) => leaf_field_count(inner.as_ref()),
            _ => 1,
        })
        .sum()
}

/// Rewrite a [`ColumnName`] reference whose path begins with a hoisted prefix to use the
/// hoisted column name. The longest matching prefix wins, so chained hoists are honored.
struct RewriteHoistedPath<'a> {
    hoist_map: &'a BTreeMap<Vec<String>, String>,
}

impl<'a> ExpressionTransform<'a> for RewriteHoistedPath<'_> {
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        let path = name.path();
        let mut best: Option<(&Vec<String>, &String)> = None;
        for (hoist_path, hoist_name) in self.hoist_map {
            if hoist_path.len() <= path.len()
                && path
                    .iter()
                    .take(hoist_path.len())
                    .zip(hoist_path.iter())
                    .all(|(a, b)| a == b)
                && best.is_none_or(|(p, _)| p.len() < hoist_path.len())
            {
                best = Some((hoist_path, hoist_name));
            }
        }
        if let Some((hoist_path, hoist_name)) = best {
            let mut new_path = Vec::with_capacity(path.len() - hoist_path.len() + 1);
            new_path.push(hoist_name.clone());
            new_path.extend(path.iter().skip(hoist_path.len()).cloned());
            return Some(Cow::Owned(ColumnName::new(new_path)));
        }
        Some(Cow::Borrowed(name))
    }
}

/// Pre-CSE hoisting for the engine `Project` lowering.
///
/// Identifies kernel column references whose path is at least two segments deep and that appear
/// at least twice across `columns`. For the *shallowest* such repeated prefix on each chain
/// (so we never double-hoist a path and its ancestor), materializes the corresponding DataFusion
/// `get_field(...)` chain in an intermediate `Projection` over `working_plan` under a stable
/// `__dk_hoist_<idx>` name and rewrites every kernel reference using that prefix to point at
/// the hoisted column.
///
/// When no candidate qualifies, returns `working_plan` and `columns` unchanged.
fn hoist_repeated_column_paths(
    working_plan: LogicalPlan,
    columns: Vec<Arc<Expression>>,
) -> Result<(LogicalPlan, Vec<Arc<Expression>>), DataFusionError> {
    let mut counter = ColumnPathCounter::default();
    for expr in &columns {
        let _ = counter.transform_expr(expr.as_ref());
    }

    let mut candidates: Vec<Vec<String>> = counter
        .counts
        .into_iter()
        .filter_map(|(path, count)| (count >= 2 && path.len() >= 2).then_some(path))
        .collect();
    candidates.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));

    let mut kept: Vec<Vec<String>> = Vec::new();
    for cand in candidates {
        let dominated = kept
            .iter()
            .any(|k| k.len() < cand.len() && cand.starts_with(k.as_slice()));
        if !dominated {
            kept.push(cand);
        }
    }

    if kept.is_empty() {
        return Ok((working_plan, columns));
    }

    let hoist_map: BTreeMap<Vec<String>, String> = kept
        .into_iter()
        .enumerate()
        .map(|(idx, path)| (path, format!("__dk_hoist_{idx}")))
        .collect();

    let pass_through: Vec<Expr> = working_plan
        .schema()
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    let mut proj_exprs = pass_through;
    for (path, hoist_name) in &hoist_map {
        let kernel_col = Expression::Column(ColumnName::new(path.iter().cloned()));
        let df_expr = kernel_expr_to_df(&kernel_col)?;
        proj_exprs.push(df_expr.alias(hoist_name.clone()));
    }

    let hoisted_plan = LogicalPlanBuilder::from(working_plan)
        .project(proj_exprs)?
        .build()?;

    let mut rewriter = RewriteHoistedPath {
        hoist_map: &hoist_map,
    };
    let rewritten = columns
        .iter()
        .map(|expr| {
            Arc::new(
                rewriter
                    .transform_expr(expr.as_ref())
                    .map(Cow::into_owned)
                    .unwrap_or_else(|| expr.as_ref().clone()),
            )
        })
        .collect::<Vec<_>>();

    Ok((hoisted_plan, rewritten))
}

/// Lower a [`DeclarativePlanNode::Project`](delta_kernel::plans::ir::DeclarativePlanNode::Project)
/// arm to a DataFusion [`LogicalPlan`]. `child_plan` is the already-compiled child plan; this
/// helper handles input/output name collision avoidance, pre-CSE hoisting, and the final
/// projection expression construction.
pub(super) fn compile_project_node(
    child_plan: LogicalPlan,
    node: &ProjectNode,
) -> Result<LogicalPlan, DataFusionError> {
    let expanded_columns = crate::compile::expand_projection_columns(
        &node.columns,
        node.output_schema.fields().count(),
    )?;
    let output_arrow_schema: ArrowSchema =
        node.output_schema.as_ref().try_into_arrow().map_err(|e| {
            plan_compilation(format!(
                "Logical projection output schema conversion failed: {e}"
            ))
        })?;

    // Insulate input names from output names to avoid DataFusion optimizer ambiguity.
    // When a kernel projection produces an output field whose name equals an unqualified
    // column in the child schema, `push_down_leaf_projections` builds intermediate
    // schemas that carry both the qualified upstream column (e.g. `relation_X.add`) and
    // the unqualified projected column (`add`). DataFusion's `DFSchema` rejects that as
    // `AmbiguousReference`. We pre-rename the colliding inputs to `__dk_in_<name>` and
    // rewrite the kernel expression roots to match. After the rename layer, no kernel
    // output name appears as an input column anywhere in the resolved schema.
    let output_names: HashSet<String> = node
        .output_schema
        .fields()
        .map(|f| f.name().to_string())
        .collect();
    let referenced_roots = collect_top_level_column_roots(&expanded_columns);
    let child_field_names: HashSet<String> = child_plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let mut colliding_inputs: BTreeMap<String, String> = BTreeMap::new();
    for name in referenced_roots {
        if output_names.contains(&name) && child_field_names.contains(&name) {
            let renamed = format!("__dk_in_{name}");
            colliding_inputs.insert(name, renamed);
        }
    }

    let (working_plan, rewritten_columns): (LogicalPlan, Vec<Arc<Expression>>) =
        if colliding_inputs.is_empty() {
            (child_plan, expanded_columns.clone())
        } else {
            let rename_projection: Vec<Expr> = child_plan
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    let name = f.name();
                    match colliding_inputs.get(name) {
                        Some(renamed) => {
                            Expr::Column(Column::new_unqualified(name)).alias(renamed.clone())
                        }
                        None => Expr::Column(Column::new_unqualified(name)),
                    }
                })
                .collect();
            let renamed_plan = LogicalPlanBuilder::from(child_plan)
                .project(rename_projection)?
                .build()?;
            let mut rewriter = RewriteRootColumn {
                rename: &colliding_inputs,
            };
            let rewritten = expanded_columns
                .iter()
                .map(|expr| {
                    Arc::new(
                        rewriter
                            .transform_expr(expr.as_ref())
                            .map(Cow::into_owned)
                            .unwrap_or_else(|| expr.as_ref().clone()),
                    )
                })
                .collect::<Vec<_>>();
            (renamed_plan, rewritten)
        };

    // Pre-CSE hoist: materialize repeated nested struct/json access subexpressions in an
    // intermediate `Projection` so DataFusion's CSE pass has nothing to factor out below
    // this projection. See [`hoist_repeated_column_paths`] for the rationale.
    let (working_plan, rewritten_columns) =
        hoist_repeated_column_paths(working_plan, rewritten_columns)?;

    let projection: Vec<Expr> = rewritten_columns
        .iter()
        .zip(
            node.output_schema
                .fields()
                .zip(output_arrow_schema.fields()),
        )
        .map(|(kernel_expr, (field, output_arrow_field))| {
            let base_logical =
                crate::compile::translate_projection_expr(kernel_expr.as_ref(), field)?;
            // Struct-shaped expressions whose target is also a struct are passed through
            // without a logical-plan cast: the cast would surface nullability mismatches
            // between kernel-built fields (always nullable) and the target schema's per-field
            // nullability declarations, which DataFusion's `validate_struct_compatibility`
            // rejects. Per-field metadata (e.g., column-mapping annotations) is applied at
            // relation-registration time via `arrow_columns_align_to_schema`, which rebuilds
            // nested struct/list/map arrays against the relation's declared schema positionally.
            let logical = if matches!(
                (kernel_expr.as_ref(), field.data_type()),
                (
                    delta_kernel::expressions::Expression::Column(_),
                    delta_kernel::schema::DataType::Struct(_)
                )
            ) || matches!(
                (kernel_expr.as_ref(), field.data_type()),
                (
                    delta_kernel::expressions::Expression::Struct(_, _),
                    delta_kernel::schema::DataType::Struct(_)
                )
            ) || matches!(
                (kernel_expr.as_ref(), field.data_type()),
                (
                    delta_kernel::expressions::Expression::MapToStruct(_),
                    delta_kernel::schema::DataType::Struct(_)
                )
            ) || matches!(
                (kernel_expr.as_ref(), field.data_type()),
                (
                    delta_kernel::expressions::Expression::ParseJson(_),
                    delta_kernel::schema::DataType::Struct(_)
                )
            ) {
                base_logical
            } else {
                cast(base_logical, output_arrow_field.data_type().clone())
            };
            Ok::<Expr, DataFusionError>(logical.alias(field.name().to_string()))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    LogicalPlanBuilder::from(working_plan)
        .project(projection)?
        .build()
}
