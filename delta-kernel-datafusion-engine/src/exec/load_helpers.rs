//! Shared helpers for the streaming [`super::LoadExec`] and the eager
//! [`super::EagerLoadTableProvider`] dispatch in [`crate::executor`].

use std::sync::Arc;

use chrono::TimeZone;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{ListingTableUrl, PartitionedFile, TableSchema};
use datafusion_datasource_json::source::JsonSource;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::TaskContext;
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion_physical_expr::expressions::{cast, col};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::types::{Int32Type, Int64Type};
use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt64Array,
};
use delta_kernel::arrow::compute::cast as arrow_cast;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::Engine;
use parquet::arrow::RowNumber;
use roaring::RoaringTreemap;
use url::Url;

use crate::exec::field_id_adapter::FieldIdPhysicalExprAdapterFactory;

/// DataFusion's parquet/json openers require a non-`None` batch size before
/// `create_file_opener` is called.
const DEFAULT_OPENER_BATCH_SIZE: usize = 8192;

const ROW_NUMBER_COL: &str = "_row_number";

/// Resolve a `LoadSink`'s `base_url`. Scan-emitted plans always set it; the runtime check
/// only catches IR-level misuse.
pub(crate) fn sink_base_url(sink: &LoadSink) -> Result<&Url, DataFusionError> {
    sink.base_url
        .as_ref()
        .ok_or_else(|| crate::error::plan_compilation("LoadSink.base_url must be set"))
}

/// Join `path_str` onto the sink's `base_url`.
pub(crate) fn resolve_file_location(
    sink: &LoadSink,
    path_str: &str,
) -> Result<Url, DataFusionError> {
    let base = sink_base_url(sink)?;
    base.join(path_str.trim()).map_err(|e| {
        crate::error::plan_compilation(format!(
            "Load sink could not join base_url `{base}` with path `{path_str}`: {e}"
        ))
    })
}

/// Walk a nested struct path out of `batch`, returning the array at the leaf.
pub(crate) fn extract_column_array(
    batch: &RecordBatch,
    cn: &ColumnName,
) -> Result<ArrayRef, DataFusionError> {
    let mut parts = cn.path().iter();
    let head = parts
        .next()
        .ok_or_else(|| crate::error::plan_compilation(format!("empty column path `{cn}`")))?;
    let mut current = batch.column_by_name(head).cloned().ok_or_else(|| {
        crate::error::plan_compilation(format!(
            "batch schema {:?} missing top-level `{head}` while extracting `{cn}`",
            batch.schema(),
        ))
    })?;
    for seg in parts {
        let sa = current.as_struct_opt().ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "expected struct while extracting `{cn}` segment `{seg}`"
            ))
        })?;
        current = sa.column_by_name(seg).cloned().ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "struct missing `{seg}` while extracting `{cn}`"
            ))
        })?;
    }
    Ok(current)
}

/// Per-row inputs the open future captures: file URL, advisory size, optional DV descriptor,
/// and projected passthrough values (in projected order).
#[derive(Debug, Clone)]
pub(crate) struct RowInputs {
    pub url: Url,
    pub size: i64,
    pub dv_descriptor: Option<DeletionVectorDescriptor>,
    pub partition_values: Vec<ScalarValue>,
}

/// Extract one upstream row into a [`RowInputs`]. `projected_passthrough` is the precomputed
/// set of `sink.passthrough_columns` indices to materialize, in projected order.
pub(crate) fn extract_row_inputs(
    batch: &RecordBatch,
    row: usize,
    sink: &LoadSink,
    projected_passthrough: &[usize],
) -> Result<RowInputs, DataFusionError> {
    let path_cn = &sink.file_meta.path;
    let path_arr = extract_column_array(batch, path_cn)?;
    if path_arr.is_null(row) {
        return Err(crate::error::plan_compilation(format!(
            "Load sink path column `{path_cn}` was NULL at upstream row {row}"
        )));
    }
    // Path columns are always Utf8 per kernel's scan_live_actions_schema.
    let url = resolve_file_location(sink, path_arr.as_string::<i32>().value(row))?;
    let size = match sink.file_meta.size.as_ref() {
        Some(sz_cn) => {
            let arr = extract_column_array(batch, sz_cn)?;
            if arr.is_null(row) {
                0
            } else {
                arr.as_primitive::<Int64Type>().value(row)
            }
        }
        None => 0,
    };
    let dv_descriptor = read_optional_dv_descriptor(batch, sink, row)?;
    let partition_values = projected_passthrough
        .iter()
        .map(|&i| {
            let cn = &sink.passthrough_columns[i];
            let arr = extract_column_array(batch, cn)?;
            ScalarValue::try_from_array(arr.as_ref(), row)
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(RowInputs {
        url,
        size,
        dv_descriptor,
        partition_values,
    })
}

/// Read the optional per-row [`DeletionVectorDescriptor`] from `batch`. Returns `None` when
/// `sink.dv_ref` is unset or the row's DV column is NULL. Field types are fixed by kernel's
/// [`DeletionVectorDescriptor::to_schema()`] so we downcast directly via [`AsArray`].
fn read_optional_dv_descriptor(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
) -> Result<Option<DeletionVectorDescriptor>, DataFusionError> {
    let Some(ref dv_cn) = sink.dv_ref else {
        return Ok(None);
    };
    let arr = extract_column_array(batch, &dv_cn.column)?;
    if arr.is_null(row) {
        return Ok(None);
    }
    let sa = arr.as_struct_opt().ok_or_else(|| {
        crate::error::internal_error(format!(
            "Load sink dv_ref column `{}` must be a struct matching DeletionVectorDescriptor",
            dv_cn.column
        ))
    })?;
    let col = |name: &str| {
        sa.column_by_name(name).ok_or_else(|| {
            crate::error::internal_error(format!("deletion vector struct missing `{name}` column"))
        })
    };
    let storage_type: DeletionVectorStorageType = col("storageType")?
        .as_string::<i32>()
        .value(row)
        .trim()
        .parse()
        .map_err(|e: delta_kernel::Error| crate::error::internal_error(e.to_string()))?;
    let path_or_inline_dv = col("pathOrInlineDv")?
        .as_string::<i32>()
        .value(row)
        .to_string();
    let offset_arr = col("offset")?.as_primitive::<Int32Type>();
    let offset = (!offset_arr.is_null(row)).then(|| offset_arr.value(row));
    let size_in_bytes = col("sizeInBytes")?.as_primitive::<Int32Type>().value(row);
    let cardinality = col("cardinality")?.as_primitive::<Int64Type>().value(row);
    Ok(Some(DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv,
        offset,
        size_in_bytes,
        cardinality,
    }))
}

/// Async-resolve a deletion vector via [`tokio::task::spawn_blocking`] around kernel's sync
/// [`DeletionVectorDescriptor::read`]. The returned bitmap is wrapped in `Arc` so cheap clones
/// travel into the per-file UDF closure.
pub(crate) async fn resolve_dv_async(
    descriptor: DeletionVectorDescriptor,
    base_url: Url,
    engine: Arc<dyn Engine>,
) -> Result<Arc<RoaringTreemap>, DataFusionError> {
    let join =
        tokio::task::spawn_blocking(move || descriptor.read(engine.storage_handler(), &base_url))
            .await
            .map_err(|e| {
                crate::error::internal_error(format!("DV resolve task join failed: {e}"))
            })?;
    let treemap =
        join.map_err(|e| crate::error::internal_error(format!("DV resolve failed: {e}")))?;
    Ok(Arc::new(treemap))
}

/// `not_in_dv(UInt64) -> Boolean` [`ScalarUDF`] over an [`Arc<RoaringTreemap>`] of deleted
/// row IDs; returns `true` when the row should be kept, so it plugs directly into a
/// [`FilterExec`] predicate.
pub(crate) fn make_not_in_dv_udf(dv: Arc<RoaringTreemap>) -> ScalarUDF {
    let dv_for_closure = Arc::clone(&dv);
    let f = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arr: ArrayRef = match &args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        // Cast to UInt64 so the closure can receive Int64 (parquet RowNumber) without forcing
        // callers to wrap in a Cast PhysicalExpr. Cheap; null preserving.
        let cast_arr = arrow_cast(arr.as_ref(), &ArrowDataType::UInt64)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let row_idx: &UInt64Array = cast_arr.as_primitive();
        let mask: BooleanArray = row_idx
            .iter()
            .map(|v| v.map(|i| !dv_for_closure.contains(i)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(mask) as ArrayRef))
    };
    datafusion_expr::expr_fn::create_udf(
        "not_in_dv",
        vec![ArrowDataType::UInt64],
        ArrowDataType::Boolean,
        Volatility::Immutable,
        Arc::new(f),
    )
}

/// Build a [`FileSource`] for the load. Layout is `[file, partition, virtual]`: file fields
/// are split off `full_schema`; passthrough fields become DataFusion *partition columns* (the
/// per-file constant-broadcast mechanism via `PartitionedFile.partition_values`); the parquet
/// `_row_number` virtual column is appended when `include_row_number` is set, declared via
/// [`TableSchema::with_virtual_columns`] (apache/datafusion#22026) so it stays out of parquet's
/// supplied-schema validation. Projection is pushed into the source via
/// `with_projection_indices`; when row-number is enabled it gets appended for the predicate
/// side.
pub(crate) fn build_file_source(
    file_type: FileType,
    full_schema: &ArrowSchemaRef,
    file_field_count: usize,
    projection: Option<&[usize]>,
    include_row_number: bool,
) -> DfResult<Arc<dyn FileSource>> {
    let (file_fields, passthrough_fields) = full_schema.fields().split_at(file_field_count);
    let file_arrow_schema: ArrowSchemaRef = Arc::new(
        ArrowSchema::new(file_fields.to_vec()).with_metadata(full_schema.metadata().clone()),
    );
    // Partition-col broadcast (`replace_columns_with_literals` / `ProjectionOpener`) synthesizes
    // each output array's `data_type` from the upstream-extracted `ScalarValue`, which never
    // carries kernel's `delta.columnMapping.*` / `PARQUET:field_id` metadata; we strip it here
    // and reapply per-batch in the metadata stamper.
    let stripped_passthrough_fields: Vec<FieldRef> = passthrough_fields
        .iter()
        .map(|f| Arc::new(strip_field_metadata_recursive(f.as_ref())))
        .collect();
    let mut table_schema = TableSchema::new(file_arrow_schema, stripped_passthrough_fields);
    if include_row_number && matches!(file_type, FileType::Parquet) {
        let virt_field: FieldRef = Arc::new(
            ArrowField::new(ROW_NUMBER_COL, ArrowDataType::Int64, false)
                .with_extension_type(RowNumber),
        );
        table_schema = table_schema.with_virtual_columns(vec![virt_field]);
    }
    let source: Arc<dyn FileSource> = match file_type {
        FileType::Parquet => Arc::new(ParquetSource::new(table_schema)),
        FileType::Json => Arc::new(JsonSource::new(table_schema)),
    };
    let source = source.with_batch_size(DEFAULT_OPENER_BATCH_SIZE);
    let Some(proj) = projection else {
        return Ok(source);
    };
    // Caller's projection indexes into [file ++ passthrough]. When row-number is enabled, the
    // virtual sits at `file_field_count + passthrough_count`; append it for the predicate.
    let translated_proj: Vec<usize> = if include_row_number {
        let row_number_idx = file_field_count + passthrough_fields.len();
        let mut out = Vec::with_capacity(proj.len() + 1);
        out.extend(proj.iter().copied());
        out.push(row_number_idx);
        out
    } else {
        proj.to_vec()
    };
    let projected_config = FileScanConfigBuilder::new(
        // Placeholder URL; `with_projection_indices` only mutates the file_source.
        ObjectStoreUrl::local_filesystem(),
        source,
    )
    .with_projection_indices(Some(translated_proj))?
    .build();
    Ok(Arc::clone(projected_config.file_source()))
}

/// Recursively strip per-field metadata so the resulting field's `data_type` matches what
/// DataFusion's partition-col broadcast produces at runtime.
fn strip_field_metadata_recursive(
    field: &delta_kernel::arrow::datatypes::Field,
) -> delta_kernel::arrow::datatypes::Field {
    use delta_kernel::arrow::datatypes::{DataType, Field, Fields};
    let strip_inner = |f: &Arc<Field>| Arc::new(strip_field_metadata_recursive(f.as_ref()));
    let stripped_dt = match field.data_type() {
        DataType::Struct(fs) => DataType::Struct(Fields::from_iter(fs.iter().map(strip_inner))),
        DataType::List(inner) => DataType::List(strip_inner(inner)),
        DataType::LargeList(inner) => DataType::LargeList(strip_inner(inner)),
        DataType::FixedSizeList(inner, n) => DataType::FixedSizeList(strip_inner(inner), *n),
        DataType::Map(entry, sorted) => DataType::Map(strip_inner(entry), *sorted),
        other => other.clone(),
    };
    Field::new(field.name(), stripped_dt, field.is_nullable())
}

/// Parquet decoding uses field-id/column-mapping aware adaptation; JSON keeps DataFusion's
/// default name-based adapter.
pub(crate) fn adapter_factory_for(
    file_type: FileType,
) -> Option<Arc<dyn datafusion_physical_expr_adapter::PhysicalExprAdapterFactory>> {
    match file_type {
        FileType::Parquet => Some(Arc::new(FieldIdPhysicalExprAdapterFactory)),
        FileType::Json => None,
    }
}

/// Translate `url` + `size` into a DataFusion [`ObjectStoreUrl`] + [`PartitionedFile`] pair.
/// `size <= 0` means "unknown" and the caller must resolve it via [`resolve_size_if_unknown`]
/// before opening (parquet's footer reader needs a non-zero size).
pub(crate) fn into_partitioned_file(
    url: &Url,
    size: i64,
) -> Result<(ObjectStoreUrl, PartitionedFile), DataFusionError> {
    let listing = ListingTableUrl::parse(url.as_str())?;
    let object_store_url = listing.object_store();
    let store_path = listing.prefix().clone();
    let object_meta = delta_kernel::object_store::ObjectMeta {
        location: store_path,
        last_modified: chrono::Utc.timestamp_nanos(0),
        size: u64::try_from(size.max(0)).unwrap_or(0),
        e_tag: None,
        version: None,
    };
    Ok((
        object_store_url,
        PartitionedFile::new_from_meta(object_meta),
    ))
}

/// HEAD-resolve `pf.object_meta.size` if it's currently unknown (0). No-op otherwise.
pub(crate) async fn resolve_size_if_unknown(
    pf: &mut PartitionedFile,
    object_store: Arc<dyn delta_kernel::object_store::ObjectStore>,
) -> Result<(), DataFusionError> {
    use delta_kernel::object_store::ObjectStoreExt;
    if pf.object_meta.size > 0 {
        return Ok(());
    }
    let meta = object_store
        .head(&pf.object_meta.location)
        .await
        .map_err(|e| {
            crate::error::internal_error(format!(
                "object-store HEAD failed for `{}`: {e}",
                pf.object_meta.location
            ))
        })?;
    pf.object_meta.size = meta.size;
    Ok(())
}

/// Per-file [`ExecutionPlan`]: bare `DataSourceExec` when `dv` is `None`; otherwise
/// `DataSourceExec` (with `_row_number` virtual) → `FilterExec(not_in_dv(_row_number))` →
/// `ProjectionExec` (drops `_row_number`). `file_source` must already be projection-pushed
/// and DV-configured.
pub(crate) async fn build_per_file_plan(
    inputs: RowInputs,
    dv: Option<Arc<RoaringTreemap>>,
    file_source: Arc<dyn FileSource>,
    file_type: FileType,
    output_schema: &ArrowSchemaRef,
    task_context: &TaskContext,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let RowInputs {
        url,
        size,
        dv_descriptor: _,
        partition_values,
    } = inputs;
    let (object_store_url, mut partitioned_file) = into_partitioned_file(&url, size)?;
    partitioned_file.partition_values = partition_values;
    let object_store = task_context.runtime_env().object_store(&object_store_url)?;
    resolve_size_if_unknown(&mut partitioned_file, object_store).await?;
    let config_options = Arc::clone(task_context.session_config().options());
    let config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_group(FileGroup::from_iter([partitioned_file]))
        .with_expr_adapter(adapter_factory_for(file_type))
        .build();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(config)));

    let Some(dv) = dv else {
        debug_assert_eq!(
            plan.schema().fields().len(),
            output_schema.fields().len(),
            "build_per_file_plan: DataSourceExec schema mismatch with output_schema (no DV)"
        );
        return Ok(plan);
    };

    let input_schema = plan.schema();
    let row_num_col = col(ROW_NUMBER_COL, &input_schema).map_err(|e| {
        crate::error::internal_error(format!(
            "build_per_file_plan: missing `{ROW_NUMBER_COL}` virtual column on \
             DataSourceExec schema {input_schema:?}: {e}"
        ))
    })?;
    // UDF accepts UInt64; parquet RowNumber emits Int64.
    let row_num_u64 = cast(row_num_col, &input_schema, ArrowDataType::UInt64)?;
    let udf = Arc::new(make_not_in_dv_udf(dv));
    let return_field = Arc::new(ArrowField::new("not_in_dv", ArrowDataType::Boolean, true));
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        "not_in_dv",
        udf,
        vec![row_num_u64],
        return_field,
        config_options,
    ));
    let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, plan)?);

    // Project each output_schema field by name from the filtered schema; this drops `_row_number`.
    let filtered_schema = filtered.schema();
    let proj_exprs = output_schema
        .fields()
        .iter()
        .map(|out_field| {
            let name = out_field.name();
            let expr = col(name, &filtered_schema).map_err(|e| {
                crate::error::internal_error(format!(
                    "build_per_file_plan: output column `{name}` missing from filtered schema \
                     {filtered_schema:?}: {e}"
                ))
            })?;
            Ok(ProjectionExpr {
                expr,
                alias: name.clone(),
            })
        })
        .collect::<DfResult<Vec<_>>>()?;
    let projected: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(proj_exprs, filtered)?);
    Ok(projected)
}
