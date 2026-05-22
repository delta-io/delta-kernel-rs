//! Lowering for [`NodeKind::Project`](delta_kernel::plans::ir::plan::NodeKind::Project) plus the
//! root-rename visitor that handles input/output name collisions.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_common::Column;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder};
use delta_kernel::expressions::{ColumnName, Expression};
use delta_kernel::plans::ir::nodes::ProjectNode;
use delta_kernel::transforms::ExpressionTransform;

use crate::compile::expr_translator::{kernel_expr_to_df, TranslationContext};

/// Walk every kernel projection expression and collect the set of unique top-level column
/// roots (the first segment of any [`ColumnName`] reference). Detects potential collisions
/// with output field names during `Project` lowering.
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

/// Apply `rewriter` to every expression in `exprs`, returning a fresh `Vec<Arc<Expression>>`
/// where each entry is either the rewriter's owned output or a clone of the original (when
/// the rewriter returned `None`).
fn rewrite_expressions<'a, R: ExpressionTransform<'a>>(
    exprs: &'a [Arc<Expression>],
    rewriter: &mut R,
) -> Vec<Arc<Expression>> {
    exprs
        .iter()
        .map(|expr| {
            Arc::new(
                rewriter
                    .transform_expr(expr.as_ref())
                    .map(Cow::into_owned)
                    .unwrap_or_else(|| expr.as_ref().clone()),
            )
        })
        .collect()
}

/// Lower a [`NodeKind::Project`](delta_kernel::plans::ir::plan::NodeKind::Project) arm to a
/// DataFusion [`LogicalPlan`]. `child_plan` is the already-compiled child plan; this helper
/// handles input/output name collision avoidance, pre-CSE hoisting, and the final projection
/// expression construction.
pub(super) fn compile_project_node(
    child_plan: LogicalPlan,
    node: &ProjectNode,
) -> Result<LogicalPlan, DataFusionError> {
    let expanded_columns = crate::compile::expand_projection_columns(
        &node.columns,
        node.output_schema.fields().count(),
    )?;

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
            let rewritten = rewrite_expressions(
                &expanded_columns,
                &mut RewriteRootColumn {
                    rename: &colliding_inputs,
                },
            );
            (renamed_plan, rewritten)
        };

    let working_arrow_schema: ArrowSchema = working_plan.schema().as_arrow().clone();
    let projection: Vec<Expr> = rewritten_columns
        .iter()
        .zip(node.output_schema.fields())
        .map(|(kernel_expr, field)| {
            let cx = TranslationContext::typed(field, &working_arrow_schema);
            Ok::<Expr, DataFusionError>(
                kernel_expr_to_df(kernel_expr.as_ref(), &cx)?.alias(field.name().to_string()),
            )
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    LogicalPlanBuilder::from(working_plan)
        .project(projection)?
        .build()
}
