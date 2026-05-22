//! Lowering for [`NodeKind::Scan`](delta_kernel::plans::ir::plan::NodeKind::Scan) plus row-index
//! plumbing helpers shared with [`super::ordered_union`].

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
use datafusion_expr::LogicalPlanBuilder;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{FileType, ScanNode};
use delta_kernel::schema::MetadataColumnSpec;
use parquet::arrow::RowNumber;

use super::canonicalize::canonicalize_output_to_kernel_schema;
use crate::error::plan_compilation;
use crate::exec::FieldIdPhysicalExprAdapterFactory;

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
            // File-source planning rejects schemas stricter than the physical files (parquet
            // checkpoints commonly write `add.path` as nullable; JSON drops declared NOT NULL
            // on nested children). Relax before passing in; `NullabilityEnforcingTableProvider`
            // re-asserts the strict contract per-batch.
            let file_schema = Arc::new(full_schema.clone());
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
                // DataFusion's own stats collector (`statistics_from_parquet_metadata`) looks
                // columns up by name on the logical file schema: when a logical
                // name doesn't exist physically (column-mapping rename, Parquet
                // field-ID matching), it stamps the column as `null_count ==
                // num_rows`, and `constant_columns_from_stats` then rewrites the
                // projection's column reference into `Literal::NULL` BEFORE the field-id root
                // rename (see `field_id_projection.rs` in the fork) can take
                // effect. Kernel does its own file-level skipping, so the DF stats
                // path is redundant here.
                .with_collect_stat(false)
                .with_target_partitions(1);
            let paths = files
                .iter()
                .map(|f| ListingTableUrl::parse(f.location.as_str()))
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            // Wire `FieldIdPhysicalExprAdapterFactory` so the parquet/json opener does
            // column-mapping-aware decode reshape (logical name + nested rename via
            // `PARQUET:field_id` / `delta.columnMapping.physicalName`). Eliminates the need for
            // any post-scan structural realignment.
            let config = ListingTableConfig::new_with_multi_paths(paths)
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema))
                .with_expr_adapter_factory(Arc::new(FieldIdPhysicalExprAdapterFactory));
            let listing: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
            LogicalPlanBuilder::scan("scan", provider_as_source(listing), None)?.build()
        };
    let scan_plan = build_listing_scan(&node.files)?;
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
    Ok(ArrowSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}
