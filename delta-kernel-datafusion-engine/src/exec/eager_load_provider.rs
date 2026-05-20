//! [`EagerLoadTableProvider`] — the compile-time-eager counterpart to
//! [`super::LoadTableProvider`].
//!
//! Constructed in [`crate::executor::DataFusionExecutor::register_load_relation`] when the
//! kernel-IR `LoadSink`'s upstream compiles down to a bare [`LogicalPlan::Values`] AND the
//! sink has no deletion-vector reference. The provider is "eager" in that the upstream
//! literal is fully drained at *registration* time -- file paths, sizes (when present), and
//! per-row passthrough scalars are extracted up-front into a `Vec<PartitionedFile>`. At
//! `scan()` time we just hand DataFusion a single [`DataSourceExec`] over the file group;
//! all of DataFusion's native machinery applies (projection / limit pushdown via the
//! `scan()` arguments, multi-partition fan-out via [`DataSourceExec::repartitioned`]).
//!
//! ## Why not `ListingTable`?
//!
//! [`datafusion::datasource::listing::ListingTable`] only supports Hive-style partition
//! columns derived from directory paths (e.g. `/date=2025-01-10/region=us/`). Our
//! passthrough columns are per-row literal scalars from the upstream `Values` and have
//! nothing to do with file paths. DataFusion's actual mechanism for per-file constant
//! broadcast is [`PartitionedFile::partition_values`], which is what the streaming
//! [`super::LoadExec`] uses today. This provider lifts that same mechanism into a
//! single-shot scan.
//!
//! ## Relation to [`super::LoadTableProvider`]
//!
//! Both providers share the helpers in [`super::load_helpers`]:
//! [`build_file_source`], [`into_partitioned_file`], [`resolve_size_if_unknown`],
//! [`adapter_factory_for`]. The split is on `scan()` semantics:
//!
//! - `LoadTableProvider`: drains an arbitrary upstream `LogicalPlan` row-by-row at scan
//!   time via [`super::LoadExec`]'s `buffer_unordered` merger. Handles DV via per-file
//!   `FilterExec`. Used for non-Values upstreams or when DV is present.
//! - `EagerLoadTableProvider` (this): pre-drained file list at construction; single
//!   `DataSourceExec` at scan time. Used for bare `Values` upstream + no DV.
//!
//! Filter pushdown is currently off in both ([`TableProvider::supports_filters_pushdown`]
//! returns `Unsupported`). A follow-up can route filters into
//! [`datafusion_datasource_parquet::source::ParquetSource::with_predicate`] for both
//! providers.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::logical_plan::Values;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};

use crate::exec::load_helpers::{
    adapter_factory_for, build_file_source, into_partitioned_file, resolve_size_if_unknown,
    sink_base_url,
};

/// Eager-dispatch [`TableProvider`] for [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load)
/// over a literal-`Values` upstream with no DV. See module docs.
pub(crate) struct EagerLoadTableProvider {
    /// Output schema = file_fields ++ passthrough_fields. What downstream consumers see
    /// via [`TableProvider::schema`] and what `scan()` projects against.
    output_schema: ArrowSchemaRef,
    /// Number of file fields in `output_schema`. Anything past this index is a passthrough
    /// column (registered as a DataFusion `table_partition_col` on the per-scan
    /// [`TableSchema`]).
    file_field_count: usize,
    /// Object-store URL shared by all files in this sink (derived from `LoadSink::base_url`
    /// at construction). All `partitioned_files` paths are relative to this URL.
    object_store_url: ObjectStoreUrl,
    /// Pre-drained file metadata: one entry per upstream Values row. `object_meta.size`
    /// may be 0 (caller didn't populate `LoadSink::file_meta.size`); resolved lazily via
    /// async HEAD in `scan()`. `partition_values` carries the row's passthrough scalars.
    partitioned_files: Vec<PartitionedFile>,
    /// Parquet vs JSON. Forwards to the right [`FileSource`] in `scan()`.
    file_type: FileType,
}

impl EagerLoadTableProvider {
    /// Drain an upstream literal [`Values`] node into a `Vec<PartitionedFile>` with
    /// `partition_values` populated per row. Caller has already verified that
    /// `sink.dv_ref.is_none()` and that `logical` is a bare `LogicalPlan::Values` (so
    /// every entry in `values.values` is an `Expr::Literal`).
    pub(crate) fn try_new(sink: &LoadSink, values: &Values) -> Result<Self, DataFusionError> {
        let dfschema = values.schema.as_ref();

        // Resolve column indexes once.
        let path_idx = column_index_in_dfschema(dfschema, &sink.file_meta.path)?;
        let size_idx = sink
            .file_meta
            .size
            .as_ref()
            .map(|cn| column_index_in_dfschema(dfschema, cn))
            .transpose()?;
        let passthrough_indices: Vec<usize> = sink
            .passthrough_columns
            .iter()
            .map(|cn| column_index_in_dfschema(dfschema, cn))
            .collect::<Result<_, _>>()?;

        let base_url = sink_base_url(sink)?;

        let mut partitioned_files: Vec<PartitionedFile> = Vec::with_capacity(values.values.len());
        let mut object_store_url: Option<ObjectStoreUrl> = None;
        for (row_idx, row) in values.values.iter().enumerate() {
            let path_str = literal_utf8(row, path_idx, "path", row_idx)?;
            let size = match size_idx {
                Some(idx) => literal_int64_or_zero(row, idx, "size", row_idx)?,
                None => 0,
            };
            let url = base_url.join(path_str.trim()).map_err(|e| {
                crate::error::plan_compilation(format!(
                    "EagerLoadTableProvider: row {row_idx} could not join base_url \
                     `{base_url}` with path `{path_str}`: {e}"
                ))
            })?;
            let (this_obj_store_url, mut pf) = into_partitioned_file(&url, size)?;
            // All files must share an object_store_url -- a `LoadSink` reads files relative
            // to a single `base_url`, so the prefix authority is constant.
            match &object_store_url {
                None => object_store_url = Some(this_obj_store_url),
                Some(existing) if existing == &this_obj_store_url => {}
                Some(existing) => {
                    return Err(crate::error::plan_compilation(format!(
                        "EagerLoadTableProvider: heterogeneous object stores in single \
                         LoadSink: row {row_idx} resolves to `{this_obj_store_url:?}` but \
                         earlier rows had `{existing:?}`"
                    )));
                }
            }
            // Collect per-file passthrough scalars in `sink.passthrough_columns` order.
            let mut partition_values: Vec<ScalarValue> =
                Vec::with_capacity(passthrough_indices.len());
            for &pt_idx in &passthrough_indices {
                let scalar = literal_scalar(row, pt_idx, "passthrough", row_idx)?;
                partition_values.push(scalar);
            }
            pf.partition_values = partition_values;
            partitioned_files.push(pf);
        }

        // Build output_schema = file_fields ++ passthrough_fields. The kernel-declared
        // `output_relation.schema` already encodes this combined shape (Phase 4 contract).
        let arrow_schema: delta_kernel::arrow::datatypes::Schema = sink
            .output_relation
            .schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e: delta_kernel::arrow::error::ArrowError| {
                crate::error::plan_compilation(format!(
                    "EagerLoadTableProvider: output schema -> arrow conversion failed: {e}"
                ))
            })?;
        let output_schema: ArrowSchemaRef = Arc::new(arrow_schema);
        let file_field_count = sink.file_schema.fields().len();
        debug_assert_eq!(
            output_schema.fields().len(),
            file_field_count + sink.passthrough_columns.len(),
            "EagerLoadTableProvider: output_relation.schema must equal file_schema ++ \
             passthrough_columns",
        );

        // For an empty Values list we still need *some* object_store_url so DataFusion can
        // route the (zero-row) scan. Derive one from `base_url` directly.
        let object_store_url = match object_store_url {
            Some(u) => u,
            None => {
                let placeholder =
                    datafusion_datasource::ListingTableUrl::parse(base_url.as_str())?;
                placeholder.object_store()
            }
        };

        Ok(Self {
            output_schema,
            file_field_count,
            object_store_url,
            partitioned_files,
            file_type: sink.file_type,
        })
    }
}

impl std::fmt::Debug for EagerLoadTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EagerLoadTableProvider")
            .field("output_fields", &self.output_schema.fields().len())
            .field("file_field_count", &self.file_field_count)
            .field("file_type", &self.file_type)
            .field("file_count", &self.partitioned_files.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for EagerLoadTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Resolve any unknown sizes via async object-store HEAD. Done once per scan; the
        // results aren't cached on the provider (a future optimization could cache via a
        // OnceCell, but typical Load workloads scan once per registration).
        let object_store = state.runtime_env().object_store(&self.object_store_url)?;
        let mut files = self.partitioned_files.clone();
        for pf in &mut files {
            resolve_size_if_unknown(pf, Arc::clone(&object_store)).await?;
        }
        // build_file_source registers passthrough fields as DataFusion `table_partition_cols`
        // on the underlying [`TableSchema`] so each `PartitionedFile.partition_values` is
        // broadcast as a constant column (parquet `replace_columns_with_literals`, json
        // `ProjectionOpener`). `include_row_number=false` because eager bypasses DV.
        let file_source = build_file_source(
            self.file_type,
            &self.output_schema,
            self.file_field_count,
            projection.map(|v| v.as_slice()),
            /* include_row_number */ false,
        )?;
        let mut builder = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
            .with_file_group(FileGroup::from_iter(files))
            .with_expr_adapter(adapter_factory_for(self.file_type));
        if let Some(n) = limit {
            builder = builder.with_limit(Some(n));
        }
        let config = builder.build();
        Ok(Arc::new(DataSourceExec::new(Arc::new(config))))
    }

    /// Filter pushdown is left off in v1 (matches [`super::LoadTableProvider`]). Future:
    /// route into [`datafusion_datasource_parquet::source::ParquetSource::with_predicate`]
    /// for row-group pruning + `RowFilter`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

// ============================================================================
// Helpers: column index resolution + literal extraction from Values rows
// ============================================================================

/// Resolve a kernel `ColumnName` to its index in a DataFusion `DFSchema`. Only flat
/// column references are supported -- nested struct paths (e.g. `add.path`) require
/// projection in the upstream kernel IR before reaching this point.
fn column_index_in_dfschema(
    dfschema: &datafusion_common::DFSchema,
    cn: &ColumnName,
) -> Result<usize, DataFusionError> {
    let path = cn.path();
    if path.len() != 1 {
        return Err(crate::error::plan_compilation(format!(
            "EagerLoadTableProvider: nested column path `{cn}` is not supported; \
             upstream kernel IR must project nested fields before reaching the LoadSink"
        )));
    }
    let name = path.first().expect("checked length above");
    dfschema
        .index_of_column_by_name(None, name)
        .ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "EagerLoadTableProvider: column `{name}` not in upstream Values schema \
                 ({:?})",
                dfschema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>(),
            ))
        })
}

fn literal_scalar(
    row: &[Expr],
    idx: usize,
    label: &str,
    row_idx: usize,
) -> Result<ScalarValue, DataFusionError> {
    let expr = row.get(idx).ok_or_else(|| {
        crate::error::plan_compilation(format!(
            "EagerLoadTableProvider: row {row_idx} missing {label} column at index {idx}"
        ))
    })?;
    match expr {
        Expr::Literal(scalar, _) => Ok(scalar.clone()),
        other => Err(crate::error::plan_compilation(format!(
            "EagerLoadTableProvider: row {row_idx} {label} column must be a literal; \
             got {other:?}"
        ))),
    }
}

fn literal_utf8(
    row: &[Expr],
    idx: usize,
    label: &str,
    row_idx: usize,
) -> Result<String, DataFusionError> {
    match literal_scalar(row, idx, label, row_idx)? {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s),
        ScalarValue::Utf8View(Some(s)) => Ok(s),
        other => Err(crate::error::plan_compilation(format!(
            "EagerLoadTableProvider: row {row_idx} {label} scalar must be Utf8; got \
             {other:?}"
        ))),
    }
}

fn literal_int64_or_zero(
    row: &[Expr],
    idx: usize,
    label: &str,
    row_idx: usize,
) -> Result<i64, DataFusionError> {
    match literal_scalar(row, idx, label, row_idx)? {
        ScalarValue::Int64(Some(v)) => Ok(v),
        ScalarValue::Int64(None) => Ok(0),
        ScalarValue::Int32(Some(v)) => Ok(v as i64),
        ScalarValue::Int32(None) => Ok(0),
        other => Err(crate::error::plan_compilation(format!(
            "EagerLoadTableProvider: row {row_idx} {label} scalar must be Int64/Int32; \
             got {other:?}"
        ))),
    }
}
