//! Logical-plan lowering scaffold for declarative plan nodes.
//!
//! This module intentionally starts narrow: it lowers a subset of IR operators to
//! DataFusion [`LogicalPlan`] and returns `Ok(None)` for unsupported shapes so callers can
//! fall back to the legacy physical compiler while migration is in progress.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::DataType as ArrowDataType;
use datafusion_common::{Column, DFSchema};
use datafusion_datasource_json::file_format::JsonFormat;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan};
use datafusion_expr::{lit, Expr, ExprFunctionExt, JoinType as DfJoinType, LogicalPlanBuilder};
use datafusion_functions_window::row_number::row_number;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    ColumnName, Expression, MapToStructExpression, ParseJsonExpression,
};
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::SinkType::{ConsumeByKdf, Load, Relation, Results};
use delta_kernel::plans::ir::nodes::{FileType, JoinType as KernelJoinType, ScanNode};
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::transforms::ExpressionTransform;

use super::CompileContext;
use crate::compile::expr_translator::kernel_expr_to_df;
use crate::error::plan_compilation;
use crate::exec::build_relation_ref_logical;

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
/// fields. This matches the [`json_parse::generate_schema_extractions`] expansion that the
/// engine emits for [`ParseJson`].
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
) -> Result<(LogicalPlan, Vec<Arc<Expression>>), DeltaError> {
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
        let df_expr = super::expr_translator::kernel_expr_to_df(&kernel_col)?;
        proj_exprs.push(df_expr.alias(hoist_name.clone()));
    }

    let hoisted_plan = LogicalPlanBuilder::from(working_plan)
        .project(proj_exprs)
        .map_err(crate::error::datafusion_err_to_delta)?
        .build()
        .map_err(crate::error::datafusion_err_to_delta)?;

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

fn canonicalize_output_to_kernel_schema(
    plan: LogicalPlan,
    kernel_schema: &KernelSchemaRef,
) -> Result<LogicalPlan, DeltaError> {
    let source_cols = plan.schema().columns().to_vec();
    let target_len = kernel_schema.fields().count();
    if source_cols.len() < target_len {
        return Err(plan_compilation(format!(
            "canonicalization requires at least {target_len} source columns, found {}",
            source_cols.len()
        )));
    }
    let projection = kernel_schema
        .fields()
        .zip(source_cols)
        .map(|(field, source_col)| Expr::Column(source_col).alias(field.name().to_string()))
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(plan)
        .project(projection)
        .map_err(crate::error::datafusion_err_to_delta)?
        .build()
        .map_err(crate::error::datafusion_err_to_delta)
}

fn scan_to_listing_logical_plan(node: &ScanNode) -> Result<LogicalPlan, DeltaError> {
    if node.files.is_empty() {
        let arrow_schema: datafusion_common::arrow::datatypes::Schema =
            node.schema.as_ref().try_into_arrow().map_err(|e| {
                plan_compilation(format!("Logical Scan schema conversion failed: {e}"))
            })?;
        let df_schema = Arc::new(DFSchema::try_from(arrow_schema).map_err(|e| {
            plan_compilation(format!("Logical Scan DF schema conversion failed: {e}"))
        })?);
        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        }));
    }
    let full_schema: datafusion_common::arrow::datatypes::Schema = node
        .schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| plan_compilation(format!("Logical Scan schema conversion failed: {e}")))?;
    let row_index_name = node.row_index_column.clone();
    let file_ordinal_col = "__dk_file_ordinal";
    let build_listing_scan = |files: &[delta_kernel::FileMeta]| -> Result<LogicalPlan, DeltaError> {
        let file_schema = Arc::new(full_schema.clone());
        let file_schema = if matches!(node.file_type, FileType::Json) {
            crate::compile::scan::relax_nested_nullability_for_scan(file_schema.as_ref())
        } else {
            file_schema
        };
        let partition_cols: Vec<(String, ArrowDataType)> = Vec::new();
        let format: Arc<dyn datafusion_datasource::file_format::FileFormat> = match node.file_type {
            FileType::Parquet => Arc::new(ParquetFormat::default()),
            FileType::Json => Arc::new(JsonFormat::default().with_newline_delimited(true)),
        };
        let options = ListingOptions::new(format)
            .with_file_extension(match node.file_type {
                FileType::Parquet => ".parquet",
                FileType::Json => ".json",
            })
            .with_table_partition_cols(partition_cols)
            .with_collect_stat(true)
            .with_target_partitions(1);
        let paths = files
            .iter()
            .map(|f| {
                ListingTableUrl::parse(f.location.as_str())
                    .map_err(crate::error::datafusion_err_to_delta)
            })
            .collect::<Result<Vec<_>, DeltaError>>()?;
        let config = ListingTableConfig::new_with_multi_paths(paths)
            .with_listing_options(options)
            .with_schema(file_schema);
        let provider =
            Arc::new(ListingTable::try_new(config).map_err(crate::error::datafusion_err_to_delta)?);
        LogicalPlanBuilder::scan("scan", provider_as_source(provider), None)
            .map_err(crate::error::datafusion_err_to_delta)?
            .build()
            .map_err(crate::error::datafusion_err_to_delta)
    };
    let mut scan_plan = if node.ordered || row_index_name.is_some() {
        let mut it = node
            .files
            .iter()
            .map(|f| build_listing_scan(std::slice::from_ref(f)))
            .collect::<Result<Vec<_>, DeltaError>>()?
            .into_iter();
        let mut acc = if let Some(first) = it.next() {
            if let Some(name) = row_index_name.as_ref() {
                append_constant_i64_column(
                    append_row_index_column(first, name)?,
                    file_ordinal_col,
                    0,
                )?
            } else {
                first
            }
        } else {
            return Err(plan_compilation(
                "logical scan expected at least one file branch after planning",
            ));
        };
        for (idx, branch) in it.enumerate() {
            let branch = if let Some(name) = row_index_name.as_ref() {
                append_constant_i64_column(
                    append_row_index_column(branch, name)?,
                    file_ordinal_col,
                    1 + idx as i64,
                )?
            } else {
                branch
            };
            acc = LogicalPlanBuilder::from(acc)
                .union(branch)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
        }
        acc
    } else {
        build_listing_scan(&node.files)?
    };
    if let Some(predicate) = &node.predicate {
        let pred = kernel_expr_to_df(predicate.as_ref())?;
        // Kernel NULL semantics keep a row when the predicate references a NULL value (SQL
        // three-valued logic would drop it). We wrap the predicate with `pred OR pred IS NULL` so
        // the downstream Filter behaves like kernel scan-skipping. Do NOT swap this for parquet
        // filter pushdown — pushdown applies SQL semantics and would silently change kernel
        // behavior on NULL.
        let null_preserving = pred.clone().or(pred.is_null());
        scan_plan = LogicalPlanBuilder::from(scan_plan)
            .filter(null_preserving)
            .map_err(crate::error::datafusion_err_to_delta)?
            .build()
            .map_err(crate::error::datafusion_err_to_delta)?;
    }
    if let Some(row_index_name) = row_index_name.as_ref() {
        let ordinal_col = plan_column_by_name(&scan_plan, file_ordinal_col)?;
        let rid_col = plan_column_by_name(&scan_plan, row_index_name)?;
        scan_plan = LogicalPlanBuilder::from(scan_plan)
            .sort(vec![
                Expr::Column(ordinal_col).sort(true, true),
                Expr::Column(rid_col).sort(true, true),
            ])
            .map_err(crate::error::datafusion_err_to_delta)?
            .build()
            .map_err(crate::error::datafusion_err_to_delta)?;
        scan_plan = drop_named_column(scan_plan, file_ordinal_col)?;
    }
    Ok(scan_plan)
}

fn append_row_index_column(
    plan: LogicalPlan,
    row_index_name: &str,
) -> Result<LogicalPlan, DeltaError> {
    let input_col_count = plan.schema().columns().len();
    let row_number_expr = row_number();
    let window_plan = LogicalPlanBuilder::window_plan(plan.clone(), vec![row_number_expr])
        .map_err(crate::error::datafusion_err_to_delta)?;
    let row_num_col = window_plan
        .schema()
        .columns()
        .get(input_col_count)
        .cloned()
        .ok_or_else(|| {
            plan_compilation(format!(
                "logical scan row index: missing row_number output at index {input_col_count}"
            ))
        })?;
    let mut projection = plan
        .schema()
        .columns()
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    projection.push(
        (cast(Expr::Column(row_num_col), ArrowDataType::Int64) - lit(1_i64))
            .alias(row_index_name.to_string()),
    );
    LogicalPlanBuilder::from(window_plan)
        .project(projection)
        .map_err(crate::error::datafusion_err_to_delta)?
        .build()
        .map_err(crate::error::datafusion_err_to_delta)
}

fn append_constant_i64_column(
    plan: LogicalPlan,
    column_name: &str,
    value: i64,
) -> Result<LogicalPlan, DeltaError> {
    let mut projection = plan
        .schema()
        .columns()
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    projection.push(lit(value).alias(column_name.to_string()));
    LogicalPlanBuilder::from(plan)
        .project(projection)
        .map_err(crate::error::datafusion_err_to_delta)?
        .build()
        .map_err(crate::error::datafusion_err_to_delta)
}

fn plan_column_by_name(
    plan: &LogicalPlan,
    name: &str,
) -> Result<datafusion_common::Column, DeltaError> {
    plan.schema()
        .columns()
        .iter()
        .find(|col| col.name == name)
        .cloned()
        .ok_or_else(|| {
            plan_compilation(format!(
                "logical scan expected column `{name}` in schema {:?}",
                plan.schema()
            ))
        })
}

fn drop_named_column(plan: LogicalPlan, drop_name: &str) -> Result<LogicalPlan, DeltaError> {
    let projection = plan
        .schema()
        .columns()
        .iter()
        .filter(|col| col.name != drop_name)
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(plan)
        .project(projection)
        .map_err(crate::error::datafusion_err_to_delta)?
        .build()
        .map_err(crate::error::datafusion_err_to_delta)
}

/// Attempt to lower an entire plan to a DataFusion logical plan.
///
/// Returns `Ok(None)` when this plan shape is not yet supported by the logical compiler.
pub fn compile_plan_logical(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<Option<LogicalPlan>, DeltaError> {
    if !matches!(
        plan.sink.sink_type,
        Results(_) | Relation(_) | ConsumeByKdf(_) | Load(_)
    ) {
        return Ok(None);
    }
    compile_declarative_node_logical(&plan.root, ctx)
}

fn compile_declarative_node_logical(
    node: &DeclarativePlanNode,
    ctx: &CompileContext,
) -> Result<Option<LogicalPlan>, DeltaError> {
    match node {
        DeclarativePlanNode::Values(values) => {
            let arrow_schema: datafusion_common::arrow::datatypes::Schema =
                values.schema.as_ref().try_into_arrow().map_err(|e| {
                    plan_compilation(format!("Logical Values schema conversion failed: {e}"))
                })?;
            let df_schema = Arc::new(
                DFSchema::try_from(arrow_schema)
                    .map_err(|e| plan_compilation(format!("Logical Values DF schema: {e}")))?,
            );
            if values.rows.is_empty() {
                return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: df_schema,
                })));
            }
            let rows = values
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|s| kernel_expr_to_df(&Expression::literal(s.clone())))
                        .collect::<Result<Vec<_>, DeltaError>>()
                })
                .collect::<Result<Vec<_>, DeltaError>>()?;
            let plan = LogicalPlanBuilder::values_with_schema(rows, &df_schema)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
            canonicalize_output_to_kernel_schema(plan, &values.schema).map(Some)
        }
        DeclarativePlanNode::Filter { child, node } => {
            let Some(child_plan) = compile_declarative_node_logical(child, ctx)? else {
                return Ok(None);
            };
            let predicate = kernel_expr_to_df(node.predicate.as_ref())?;
            let plan = LogicalPlanBuilder::from(child_plan)
                .filter(predicate)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
            Ok(Some(plan))
        }
        DeclarativePlanNode::Assert { child, .. } => {
            let Some(child_plan) = compile_declarative_node_logical(child, ctx)? else {
                return Ok(None);
            };
            Ok(Some(child_plan))
        }
        DeclarativePlanNode::Project { child, node } => {
            let Some(child_plan) = compile_declarative_node_logical(child, ctx)? else {
                return Ok(None);
            };
            let expanded_columns = super::expand_projection_columns(
                &node.columns,
                node.output_schema.fields().count(),
            )?;
            let output_arrow_schema: datafusion_common::arrow::datatypes::Schema =
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
                                Some(renamed) => Expr::Column(Column::new_unqualified(name))
                                    .alias(renamed.clone()),
                                None => Expr::Column(Column::new_unqualified(name)),
                            }
                        })
                        .collect();
                    let renamed_plan = LogicalPlanBuilder::from(child_plan)
                        .project(rename_projection)
                        .map_err(crate::error::datafusion_err_to_delta)?
                        .build()
                        .map_err(crate::error::datafusion_err_to_delta)?;
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
                        super::translate_projection_expr(kernel_expr.as_ref(), field)?;
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
                    Ok::<Expr, DeltaError>(logical.alias(field.name().to_string()))
                })
                .collect::<Result<Vec<_>, DeltaError>>()?;
            let plan = LogicalPlanBuilder::from(working_plan)
                .project(projection)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
            Ok(Some(plan))
        }
        DeclarativePlanNode::Union { children, node } => {
            if node.ordered || children.is_empty() {
                return Ok(None);
            }
            let mut compiled_children = Vec::with_capacity(children.len());
            for child in children {
                let Some(plan) = compile_declarative_node_logical(child, ctx)? else {
                    return Ok(None);
                };
                compiled_children.push(plan);
            }
            let first = compiled_children.remove(0);
            let target_fields = first.schema().fields().iter().cloned().collect::<Vec<_>>();
            for (child_idx, plan) in compiled_children.iter().enumerate() {
                let fields = plan.schema().fields();
                if fields.len() != target_fields.len() {
                    return Err(plan_compilation(format!(
                        "Logical Union schema mismatch: child0 has {} columns but child{} has {} columns",
                        target_fields.len(),
                        child_idx + 1,
                        fields.len()
                    )));
                }
                for (col_idx, (expected, actual)) in
                    target_fields.iter().zip(fields.iter()).enumerate()
                {
                    if expected.name() != actual.name() {
                        return Err(plan_compilation(format!(
                            "Logical Union schema mismatch at column {col_idx}: expected name `{}` from child0, got `{}` from child{}",
                            expected.name(),
                            actual.name(),
                            child_idx + 1
                        )));
                    }
                    if expected.data_type() != actual.data_type()
                        || expected.is_nullable() != actual.is_nullable()
                    {
                        return Err(plan_compilation(format!(
                            "Logical Union schema mismatch at column `{}` (index {col_idx}) between child0 and child{}: expected type {:?} nullable={}, got type {:?} nullable={}",
                            expected.name(),
                            child_idx + 1,
                            expected.data_type(),
                            expected.is_nullable(),
                            actual.data_type(),
                            actual.is_nullable()
                        )));
                    }
                }
            }
            let unioned = compiled_children
                .into_iter()
                .try_fold(first, |acc, right| {
                    LogicalPlanBuilder::from(acc)
                        .union(right)
                        .map_err(crate::error::datafusion_err_to_delta)?
                        .build()
                        .map_err(crate::error::datafusion_err_to_delta)
                })?;
            Ok(Some(unioned))
        }
        DeclarativePlanNode::RelationRef(handle) => {
            build_relation_ref_logical(handle, &ctx.relation_registry).map(Some)
        }
        DeclarativePlanNode::Scan(node) => {
            let plan = scan_to_listing_logical_plan(node)?;
            Ok(Some(plan))
        }
        DeclarativePlanNode::Join { build, probe, node } => {
            let Some(build_plan) = compile_declarative_node_logical(build, ctx)? else {
                return Ok(None);
            };
            let Some(probe_plan) = compile_declarative_node_logical(probe, ctx)? else {
                return Ok(None);
            };
            if node.hint != delta_kernel::plans::ir::nodes::JoinHint::Hash {
                return Ok(None);
            }
            if node.build_keys.is_empty() || node.build_keys.len() != node.probe_keys.len() {
                return Ok(None);
            }
            let left_keys = node
                .build_keys
                .iter()
                .map(|e| kernel_expr_to_df(e.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            let right_keys = node
                .probe_keys
                .iter()
                .map(|e| kernel_expr_to_df(e.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            let join_type = match node.join_type {
                KernelJoinType::Inner => DfJoinType::Inner,
                KernelJoinType::LeftAnti => DfJoinType::RightAnti,
            };
            let plan = LogicalPlanBuilder::from(build_plan)
                .join_with_expr_keys(probe_plan, join_type, (left_keys, right_keys), None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
            let target_schema = match node.join_type {
                KernelJoinType::LeftAnti => super::node_output_schema(probe)?,
                KernelJoinType::Inner => {
                    let build_schema = super::node_output_schema(build)?;
                    let probe_schema = super::node_output_schema(probe)?;
                    build_schema
                        .as_ref()
                        .add(probe_schema.fields().cloned())
                        .map(Arc::new)
                        .map_err(|e| {
                            plan_compilation(format!(
                                "logical inner join combined output schema is invalid: {e}"
                            ))
                        })?
                }
            };
            canonicalize_output_to_kernel_schema(plan, &target_schema).map(Some)
        }
        DeclarativePlanNode::Window { child, node } => {
            if node.functions.is_empty() || node.order_by.is_empty() {
                return Ok(None);
            }
            let Some(child_plan) = compile_declarative_node_logical(child, ctx)? else {
                return Ok(None);
            };
            let partition_by = node
                .partition_by
                .iter()
                .map(|e| kernel_expr_to_df(e.as_ref()))
                .collect::<Result<Vec<_>, DeltaError>>()?;
            let order_by = node
                .order_by
                .iter()
                .map(|spec| {
                    let expr = kernel_expr_to_df(&Expression::from(spec.column.clone()))?;
                    Ok(expr.sort(!spec.descending, spec.nulls_first))
                })
                .collect::<Result<Vec<_>, DeltaError>>()?;
            // Multiple identical row_number() PARTITION BY / ORDER BY exprs collide on
            // DataFusion's "windows require unique expression names" validation. Emit a single
            // window expression and replicate its output to each requested output_col via the
            // post-window projection below.
            let row_number_expr = row_number()
                .partition_by(partition_by.clone())
                .order_by(order_by.clone())
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
            let expr_name = row_number_expr
                .name_for_alias()
                .map_err(crate::error::datafusion_err_to_delta)?;
            let window_exprs = vec![row_number_expr];
            let window_expr_names: Vec<(String, String)> = node
                .functions
                .iter()
                .map(|wf| (expr_name.clone(), wf.output_col.clone()))
                .collect();
            let window_plan = LogicalPlanBuilder::window_plan(child_plan.clone(), window_exprs)
                .map_err(crate::error::datafusion_err_to_delta)?;
            let input_col_count = child_plan.schema().columns().len();
            let mut projection = child_plan
                .schema()
                .columns()
                .iter()
                .map(|col| Expr::Column(col.clone()))
                .collect::<Vec<_>>();
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
            projection.extend(window_expr_names.iter().map(|(_from, to)| {
                cast(
                    Expr::Column(src_col.clone()),
                    datafusion::arrow::datatypes::DataType::Int64,
                )
                .alias(to.clone())
            }));
            let plan = LogicalPlanBuilder::from(window_plan)
                .project(projection)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build()
                .map_err(crate::error::datafusion_err_to_delta)?;
            Ok(Some(plan))
        }
        _ => Ok(None),
    }
}
