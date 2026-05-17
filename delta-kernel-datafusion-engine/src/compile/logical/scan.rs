//! Lowering for [`DeclarativePlanNode::Scan`](delta_kernel::plans::ir::DeclarativePlanNode::Scan)
//! plus row-index plumbing helpers shared with [`super::ordered_union`].

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_common::DFSchema;
use datafusion_datasource_json::file_format::JsonFormat;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan};
use datafusion_expr::{lit, Expr, LogicalPlanBuilder};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{FileType, ScanNode};
use delta_kernel::schema::MetadataColumnSpec;
use parquet::arrow::RowNumber;

use super::canonicalize::canonicalize_output_to_kernel_schema;
use super::providers::NullabilityEnforcingTableProvider;
use crate::compile::expr_translator::kernel_expr_to_df;
use crate::error::plan_compilation;

/// DataFusion file sources fail planning when decoded nested field nullability is wider than
/// the declared scan schema (for example nullable parquet/json children flowing into kernel
/// protocol NOT NULL children). Relax nested nullability before handing the schema to a file
/// source so planning succeeds; downstream operators carry the relaxed nullability.
pub(super) fn relax_nested_nullability_for_scan(schema: &ArrowSchema) -> Arc<ArrowSchema> {
    use datafusion_common::arrow::datatypes::{DataType, Field};
    fn relax_field(field: &Arc<Field>, force_nullable: bool) -> Arc<Field> {
        let relaxed_dt = relax_data_type(field.data_type());
        Arc::new(
            Field::new(
                field.name(),
                relaxed_dt,
                if force_nullable {
                    true
                } else {
                    field.is_nullable()
                },
            )
            .with_metadata(field.metadata().clone()),
        )
    }
    fn relax_data_type(dt: &DataType) -> DataType {
        match dt {
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(|f| relax_field(f, true)).collect())
            }
            DataType::List(inner) => DataType::List(relax_field(inner, true)),
            DataType::LargeList(inner) => DataType::LargeList(relax_field(inner, true)),
            DataType::FixedSizeList(inner, n) => {
                DataType::FixedSizeList(relax_field(inner, true), *n)
            }
            DataType::Map(entries, sorted) => {
                let relaxed_entries = match entries.data_type() {
                    DataType::Struct(entry_fields) if entry_fields.len() == 2 => {
                        let key = relax_field(&entry_fields[0], false);
                        let val = relax_field(&entry_fields[1], true);
                        Arc::new(
                            Field::new(
                                entries.name(),
                                DataType::Struct(vec![key, val].into()),
                                entries.is_nullable(),
                            )
                            .with_metadata(entries.metadata().clone()),
                        )
                    }
                    _ => relax_field(entries, true),
                };
                DataType::Map(relaxed_entries, *sorted)
            }
            other => other.clone(),
        }
    }
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| relax_field(f, false))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

pub(super) fn scan_to_listing_logical_plan(
    node: &ScanNode,
) -> Result<LogicalPlan, DataFusionError> {
    if node.files.is_empty() {
        let arrow_schema: ArrowSchema =
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
    let full_schema = build_scan_arrow_schema(node)?;
    let build_listing_scan =
        |files: &[delta_kernel::FileMeta]| -> Result<LogicalPlan, DataFusionError> {
            let file_schema = Arc::new(full_schema.clone());
            let file_schema = if node.file_type == FileType::Json {
                relax_nested_nullability_for_scan(file_schema.as_ref())
            } else {
                file_schema
            };
            let partition_cols: Vec<(String, ArrowDataType)> = Vec::new();
            let format: Arc<dyn datafusion_datasource::file_format::FileFormat> =
                match node.file_type {
                    FileType::Parquet => Arc::new(ParquetFormat::default()),
                    FileType::Json => Arc::new(JsonFormat::default().with_newline_delimited(true)),
                };
            let options = ListingOptions::new(format)
                .with_file_extension(match node.file_type {
                    FileType::Parquet => ".parquet",
                    FileType::Json => ".json",
                })
                .with_table_partition_cols(partition_cols)
                // Match the upstream `collect_statistics` default (apache/datafusion PR #16080).
                // DataFusion's own stats collector (`statistics_from_parquet_metadata`) looks columns up
                // by name on the logical file schema: when a logical name doesn't exist physically
                // (column-mapping rename, Parquet field-ID matching), it stamps the column as
                // `null_count == num_rows`, and `constant_columns_from_stats` then rewrites the
                // projection's column reference into `Literal::NULL` BEFORE the field-id root rename
                // (see `field_id_projection.rs` in the fork) can take effect. Kernel does its own
                // file-level skipping, so the DF stats path is redundant here.
                .with_collect_stat(false)
                .with_target_partitions(1);
            let paths = files
                .iter()
                .map(|f| ListingTableUrl::parse(f.location.as_str()))
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            let config = ListingTableConfig::new_with_multi_paths(paths)
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema));
            let listing: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
            // JSON file sources don't enforce declared nullability and DataFusion's own
            // `check_not_null_constraints` only covers top-level columns. Wrap JSON scans with a
            // provider that re-asserts the strict kernel schema (top-level + nested) at the scan
            // boundary. Parquet's decoder handles nullability natively, so it scans naked.
            let provider: Arc<dyn TableProvider> = if node.file_type == FileType::Json {
                Arc::new(NullabilityEnforcingTableProvider::new(
                    listing,
                    Arc::new(full_schema.clone()),
                ))
            } else {
                listing
            };
            LogicalPlanBuilder::scan("scan", provider_as_source(provider), None)?.build()
        };
    let mut scan_plan = build_listing_scan(&node.files)?;
    if let Some(predicate) = &node.predicate {
        let pred = kernel_expr_to_df(predicate.as_ref())?;
        // Kernel NULL semantics keep a row when the predicate references a NULL value (SQL
        // three-valued logic would drop it). We wrap the predicate with `pred OR pred IS NULL` so
        // the downstream Filter behaves like kernel scan-skipping. Do NOT swap this for parquet
        // filter pushdown -- pushdown applies SQL semantics and would silently change kernel
        // behavior on NULL.
        let null_preserving = pred.clone().or(pred.is_null());
        scan_plan = LogicalPlanBuilder::from(scan_plan)
            .filter(null_preserving)?
            .build()?;
    }
    canonicalize_output_to_kernel_schema(scan_plan, &node.schema)
}

fn build_scan_arrow_schema(node: &ScanNode) -> Result<ArrowSchema, DataFusionError> {
    let schema: ArrowSchema = node
        .schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| plan_compilation(format!("Logical Scan schema conversion failed: {e}")))?;
    if node.file_type != FileType::Parquet {
        return Ok(schema);
    }
    let Some(idx) = node
        .schema
        .index_of_metadata_column(&MetadataColumnSpec::RowIndex)
    else {
        return Ok(schema);
    };
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            if i == *idx {
                Arc::new(
                    ArrowField::new(field.name(), ArrowDataType::Int64, false)
                        .with_metadata(field.metadata().clone())
                        .with_extension_type(RowNumber),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    Ok(ArrowSchema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Build a projection that passes through every column of `pass_through_schema_src` and
/// appends `new_column` aliased as `new_column_name`. Used for the "all-existing-columns plus
/// one synthesized column" pattern that drives row-index plumbing and ordered-union ordinals.
fn project_pass_through_with(
    plan: LogicalPlan,
    pass_through_schema_src: &LogicalPlan,
    new_column: Expr,
    new_column_name: &str,
) -> Result<LogicalPlan, DataFusionError> {
    let mut projection = pass_through_schema_src
        .schema()
        .columns()
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    projection.push(new_column.alias(new_column_name.to_string()));
    LogicalPlanBuilder::from(plan).project(projection)?.build()
}

pub(super) fn append_constant_i64_column(
    plan: LogicalPlan,
    column_name: &str,
    value: i64,
) -> Result<LogicalPlan, DataFusionError> {
    let plan_clone = plan.clone();
    project_pass_through_with(plan_clone, &plan, lit(value), column_name)
}

pub(super) fn plan_column_by_name(
    plan: &LogicalPlan,
    name: &str,
) -> Result<datafusion_common::Column, DataFusionError> {
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

pub(super) fn drop_named_column(
    plan: LogicalPlan,
    drop_name: &str,
) -> Result<LogicalPlan, DataFusionError> {
    let projection = plan
        .schema()
        .columns()
        .iter()
        .filter(|col| col.name != drop_name)
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(plan).project(projection)?.build()
}
