//! Compile-time-eager [`TableProvider`] for `LoadSink` over a bare `LogicalPlan::Values`
//! upstream with no DV. Drains the literal at registration time into a
//! `Vec<PartitionedFile>` (per-row `partition_values` carry the passthrough scalars), then
//! `scan()` returns a single `DataSourceExec` so DataFusion's native projection/limit
//! pushdown and multi-partition fan-out apply directly. Anything else falls through to the
//! streaming [`super::LoadTableProvider`].

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
    adapter_factory_for, build_file_source, into_partitioned_file, resolve_file_location,
    resolve_size_if_unknown, sink_base_url,
};

pub(crate) struct EagerLoadTableProvider {
    /// `file_fields ++ passthrough_fields`.
    output_schema: ArrowSchemaRef,
    /// `output_schema[file_field_count..]` are passthrough columns broadcast as
    /// `PartitionedFile::partition_values`.
    file_field_count: usize,
    object_store_url: ObjectStoreUrl,
    /// One entry per upstream Values row; `partition_values` carries passthrough scalars.
    partitioned_files: Vec<PartitionedFile>,
    file_type: FileType,
}

impl EagerLoadTableProvider {
    /// Drain a literal [`Values`] node into a `Vec<PartitionedFile>`. Caller guarantees
    /// `sink.dv_ref.is_none()` and that every row in `values.values` is `Expr::Literal`.
    pub(crate) fn try_new(sink: &LoadSink, values: &Values) -> Result<Self, DataFusionError> {
        let dfschema = values.schema.as_ref();
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

        let mut partitioned_files: Vec<PartitionedFile> = Vec::with_capacity(values.values.len());
        let mut object_store_url: Option<ObjectStoreUrl> = None;
        for (row_idx, row) in values.values.iter().enumerate() {
            let path_str = match literal_at(row, path_idx, "path", row_idx)? {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => s,
                other => {
                    return Err(crate::error::plan_compilation(format!(
                        "row {row_idx} path scalar must be Utf8; got {other:?}"
                    )))
                }
            };
            let size = match size_idx {
                Some(idx) => match literal_at(row, idx, "size", row_idx)? {
                    ScalarValue::Int64(Some(v)) => v,
                    ScalarValue::Int32(Some(v)) => v as i64,
                    ScalarValue::Int64(None) | ScalarValue::Int32(None) => 0,
                    other => {
                        return Err(crate::error::plan_compilation(format!(
                            "row {row_idx} size scalar must be Int64/Int32; got {other:?}"
                        )))
                    }
                },
                None => 0,
            };
            let url = resolve_file_location(sink, &path_str)?;
            let (this_obj_store_url, mut pf) = into_partitioned_file(&url, size)?;
            match &object_store_url {
                None => object_store_url = Some(this_obj_store_url),
                Some(existing) if existing == &this_obj_store_url => {}
                Some(existing) => {
                    return Err(crate::error::plan_compilation(format!(
                        "heterogeneous object stores in LoadSink: row {row_idx} \
                         {this_obj_store_url:?} vs earlier {existing:?}"
                    )));
                }
            }
            pf.partition_values = passthrough_indices
                .iter()
                .map(|&i| literal_at(row, i, "passthrough", row_idx))
                .collect::<Result<_, _>>()?;
            partitioned_files.push(pf);
        }

        let arrow_schema: delta_kernel::arrow::datatypes::Schema = sink
            .output_relation
            .schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e: delta_kernel::arrow::error::ArrowError| {
                crate::error::plan_compilation(format!("output schema -> arrow: {e}"))
            })?;
        let output_schema: ArrowSchemaRef = Arc::new(arrow_schema);
        let file_field_count = sink.file_schema.fields().len();
        debug_assert_eq!(
            output_schema.fields().len(),
            file_field_count + sink.passthrough_columns.len(),
        );

        // Empty Values: still need *some* `object_store_url` for routing; derive from `base_url`.
        let object_store_url = match object_store_url {
            Some(u) => u,
            None => datafusion_datasource::ListingTableUrl::parse(sink_base_url(sink)?.as_str())?
                .object_store(),
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
        let object_store = state.runtime_env().object_store(&self.object_store_url)?;
        let mut files = self.partitioned_files.clone();
        for pf in &mut files {
            resolve_size_if_unknown(pf, Arc::clone(&object_store)).await?;
        }
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
        Ok(Arc::new(DataSourceExec::new(Arc::new(builder.build()))))
    }

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

/// Resolve a flat kernel `ColumnName` to its index in `dfschema`. Nested paths must already
/// be projected in the upstream IR.
fn column_index_in_dfschema(
    dfschema: &datafusion_common::DFSchema,
    cn: &ColumnName,
) -> Result<usize, DataFusionError> {
    let path = cn.path();
    if path.len() != 1 {
        return Err(crate::error::plan_compilation(format!(
            "EagerLoadTableProvider: nested column path `{cn}` not supported"
        )));
    }
    let name = path.first().expect("checked above");
    dfschema.index_of_column_by_name(None, name).ok_or_else(|| {
        crate::error::plan_compilation(format!(
            "column `{name}` not in upstream Values schema ({:?})",
            dfschema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
        ))
    })
}

/// Extract the [`ScalarValue`] at `row[idx]`. Caller has guaranteed `row[idx]` is an
/// `Expr::Literal`.
fn literal_at(
    row: &[Expr],
    idx: usize,
    label: &str,
    row_idx: usize,
) -> Result<ScalarValue, DataFusionError> {
    match row.get(idx) {
        Some(Expr::Literal(scalar, _)) => Ok(scalar.clone()),
        Some(other) => Err(crate::error::plan_compilation(format!(
            "row {row_idx} {label} must be Expr::Literal; got {other:?}"
        ))),
        None => Err(crate::error::plan_compilation(format!(
            "row {row_idx} missing {label} column at index {idx}"
        ))),
    }
}
