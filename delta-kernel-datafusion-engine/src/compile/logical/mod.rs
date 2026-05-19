//! Declarative-plan -> DataFusion [`LogicalPlan`] lowering.
//!
//! Every kernel IR shape compiles to a [`LogicalPlan`] or surfaces a typed error. Invariants
//! the caller is responsible for (non-Hash join hints, empty Union, empty Window functions,
//! ...) error here rather than silently bail; long-term those checks belong on the kernel IR
//! constructors so engines can rely on a validated input.

use std::sync::Arc;

use datafusion_common::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion_common::error::DataFusionError;
use datafusion_common::DFSchema;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan, Values};
use datafusion_expr::{Expr, ExprFunctionExt, JoinType as DfJoinType, LogicalPlanBuilder};
use datafusion_functions_window::row_number::row_number;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Expression;
use delta_kernel::plans::ir::nodes::JoinType as KernelJoinType;
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};

use super::CompileContext;
use crate::compile::expr_translator::{kernel_expr_to_df, kernel_exprs_to_df, TranslationContext};
use crate::error::plan_compilation;

mod canonicalize;
mod ordered_union;
mod project;
mod providers;
mod scan;

use canonicalize::canonicalize_output_to_kernel_schema;
use ordered_union::compile_ordered_union;
use project::compile_project_node;
use providers::file_listing_to_logical_plan;
use scan::scan_to_listing_logical_plan;

/// Lower an entire [`Plan`] to a DataFusion [`LogicalPlan`]. Always succeeds or surfaces a
/// typed [`DataFusionError`].
pub fn compile_plan_logical(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let _ = &plan.sink; // sink-type dispatch happens at the caller.
    compile_declarative_node_logical(&plan.root, ctx)
}

fn compile_declarative_node_logical(
    plan_node: &DeclarativePlanNode,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    match plan_node {
        DeclarativePlanNode::Values(values) => {
            let arrow_schema: ArrowSchema =
                values.schema.as_ref().try_into_arrow().map_err(|e| {
                    plan_compilation(format!("Logical Values schema conversion failed: {e}"))
                })?;
            let df_schema = Arc::new(
                DFSchema::try_from(arrow_schema)
                    .map_err(|e| plan_compilation(format!("Logical Values DF schema: {e}")))?,
            );
            let rows = values
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|s| {
                            kernel_expr_to_df(
                                &Expression::literal(s.clone()),
                                &TranslationContext::untyped(),
                            )
                        })
                        .collect::<Result<Vec<_>, DataFusionError>>()
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            Ok(if rows.is_empty() {
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: df_schema,
                })
            } else {
                LogicalPlan::Values(Values {
                    schema: df_schema,
                    values: rows,
                })
            })
        }
        DeclarativePlanNode::Filter { child, node } => {
            let child_plan = compile_declarative_node_logical(child, ctx)?;
            let predicate =
                kernel_expr_to_df(node.predicate.as_ref(), &TranslationContext::untyped())?;
            LogicalPlanBuilder::from(child_plan)
                .filter(predicate)?
                .build()
        }
        DeclarativePlanNode::Project { child, node } => {
            let child_plan = compile_declarative_node_logical(child, ctx)?;
            compile_project_node(child_plan, node)
        }
        DeclarativePlanNode::Union { children, node } => {
            if children.is_empty() {
                return Err(plan_compilation(
                    "Union with zero children is not a valid kernel IR shape",
                ));
            }
            let mut compiled: Vec<LogicalPlan> = children
                .iter()
                .map(|child| compile_declarative_node_logical(child, ctx))
                .collect::<Result<_, DataFusionError>>()?;
            if compiled.len() == 1 {
                return Ok(compiled.remove(0));
            }
            if node.ordered {
                compile_ordered_union(compiled)
            } else {
                let first = compiled.remove(0);
                compiled.into_iter().try_fold(first, |acc, right| {
                    LogicalPlanBuilder::from(acc).union(right)?.build()
                })
            }
        }
        DeclarativePlanNode::RelationRef(handle) => {
            let provider = ctx
                .relation_providers
                .get(handle.id.as_str())
                .ok_or_else(|| {
                    plan_compilation(format!(
                        "RelationRef references unregistered handle id {} (name `{}`); the \
                     producing plan must run before any consumer compiles",
                        handle.id, handle.name
                    ))
                })?;
            LogicalPlanBuilder::scan(
                format!("relation_{}", handle.id),
                datafusion::datasource::provider_as_source(Arc::clone(provider)),
                None,
            )?
            .build()
        }
        DeclarativePlanNode::Scan(node) => scan_to_listing_logical_plan(node),
        DeclarativePlanNode::Join {
            build,
            probe,
            node: join_node,
        } => {
            if join_node.left_keys.is_empty()
                || join_node.left_keys.len() != join_node.right_keys.len()
            {
                return Err(plan_compilation(format!(
                    "Join requires non-empty matched-arity keys; got left={} right={}",
                    join_node.left_keys.len(),
                    join_node.right_keys.len()
                )));
            }
            let build_plan = compile_declarative_node_logical(build, ctx)?;
            let probe_plan = compile_declarative_node_logical(probe, ctx)?;
            let left_keys =
                kernel_exprs_to_df(&join_node.left_keys, &TranslationContext::untyped())?;
            let right_keys =
                kernel_exprs_to_df(&join_node.right_keys, &TranslationContext::untyped())?;
            let join_type = match join_node.join_type {
                KernelJoinType::Inner => DfJoinType::Inner,
                KernelJoinType::LeftAnti => DfJoinType::RightAnti,
            };
            let plan = LogicalPlanBuilder::from(build_plan)
                .join_with_expr_keys(probe_plan, join_type, (left_keys, right_keys), None)?
                .build()?;
            canonicalize_output_to_kernel_schema(plan, &join_node.output_schema)
        }
        DeclarativePlanNode::Window { child, node } => {
            if node.functions.is_empty() {
                return Err(plan_compilation(
                    "Window with zero functions is not a valid kernel IR shape",
                ));
            }
            if node.order_by.is_empty() {
                return Err(plan_compilation(
                    "Window with empty ORDER BY is not a valid kernel IR shape",
                ));
            }
            let child_plan = compile_declarative_node_logical(child, ctx)?;
            let partition_by =
                kernel_exprs_to_df(&node.partition_by, &TranslationContext::untyped())?;
            let order_by = node
                .order_by
                .iter()
                .map(|spec| {
                    let expr = kernel_expr_to_df(
                        &Expression::from(spec.column.clone()),
                        &TranslationContext::untyped(),
                    )?;
                    Ok(expr.sort(!spec.descending, false))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            // Multiple identical row_number() PARTITION BY / ORDER BY exprs collide on
            // DataFusion's "windows require unique expression names" validation. Emit a single
            // window expression and replicate its output to each requested output_col via the
            // post-window projection below.
            let row_number_expr = row_number()
                .partition_by(partition_by.clone())
                .order_by(order_by.clone())
                .build()?;
            let expr_name = row_number_expr.name_for_alias()?;
            let window_exprs = vec![row_number_expr];
            let window_expr_names: Vec<(String, String)> = node
                .functions
                .iter()
                .map(|wf| (expr_name.clone(), wf.output_col.clone()))
                .collect();
            // Snapshot columns before moving `child_plan` into the builder.
            let child_cols = child_plan.schema().columns();
            let input_col_count = child_cols.len();
            let window_plan = LogicalPlanBuilder::window_plan(child_plan, window_exprs)?;
            // Single window expression covers all output_cols (see comment above). Each kernel
            // output_col aliases the same source column, cast to Int64 (kernel WindowFunction emits
            // LONG; row_number_udf emits UInt64).
            let src_col = window_plan
                .schema()
                .columns()
                .get(input_col_count)
                .cloned()
                .ok_or_else(|| {
                    plan_compilation(format!(
                        "logical window: missing computed window output at index {input_col_count}"
                    ))
                })?;
            let pass_through = child_cols.into_iter().map(Expr::Column);
            let window_outputs = window_expr_names.iter().map(|(_from, to)| {
                cast(Expr::Column(src_col.clone()), ArrowDataType::Int64).alias(to.clone())
            });
            LogicalPlanBuilder::from(window_plan)
                .project(pass_through.chain(window_outputs))?
                .build()
        }
        DeclarativePlanNode::FileListing(node) => file_listing_to_logical_plan(node),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion_common::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};

    use super::providers::NullabilityEnforcingTableProvider;
    use super::scan::relax_nested_nullability_for_scan;

    /// Schema with a top-level struct `meta` (nullable) containing a NOT NULL child `id`, plus
    /// a top-level int field `x`. Returns the strict schema; the matching relaxed schema used
    /// by the inner provider widens `meta.id` to nullable so file decoders accept it.
    fn schemas() -> (Arc<ArrowSchema>, Arc<ArrowSchema>) {
        let strict_meta_children: Fields =
            vec![Arc::new(Field::new("id", DataType::Utf8, false))].into();
        let strict_meta = Field::new("meta", DataType::Struct(strict_meta_children), true);
        let strict_x = Field::new("x", DataType::Int64, false);
        let strict = Arc::new(ArrowSchema::new(vec![strict_meta, strict_x]));
        let relaxed = relax_nested_nullability_for_scan(strict.as_ref());
        (strict, relaxed)
    }

    /// Both the unprojected scan and any projected scan must return a schema equal to
    /// `strict` (or `strict.project(...)`), so `NullabilityValidationExec`'s positional zip
    /// stays aligned with the inner plan's columns. `projection = None` -> full strict;
    /// `projection = Some(&[1])` (drop `meta`, keep `x`) -> single-column projected strict.
    #[rstest::rstest]
    #[case::unprojected(None)]
    #[case::projected_keep_x(Some(vec![1]))]
    #[tokio::test]
    async fn wrapper_scan_returns_strict_schema(#[case] projection: Option<Vec<usize>>) {
        let (strict, relaxed) = schemas();
        let inner: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(Arc::clone(&relaxed), vec![vec![]]).unwrap());
        let provider = NullabilityEnforcingTableProvider::new(inner, Arc::clone(&strict));
        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), projection.as_ref(), &[], None)
            .await
            .unwrap();
        let expected = match projection {
            Some(p) => Arc::new(strict.project(&p).unwrap()),
            None => Arc::clone(&strict),
        };
        assert_eq!(plan.schema().as_ref(), expected.as_ref());
    }
}
