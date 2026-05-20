//! Helpers shared by [`super::LoadExec`] and the eager listing-table dispatch in
//! [`crate::executor::DataFusionExecutor::register_load_relation`].
//!
//! Three responsibilities:
//!
//! 1. Walk a nested struct path out of a metadata [`RecordBatch`] by [`ColumnName`]
//!    ([`extract_column_array`]).
//! 2. Extract per-upstream-row inputs into a [`RowInputs`] for opening a single file
//!    ([`extract_row_inputs`]).
//! 3. Resolve the optional per-row deletion-vector via kernel's
//!    [`DeletionVectorDescriptor::read`] on a blocking-pool thread ([`resolve_dv_async`]),
//!    then build a per-file [`ExecutionPlan`] stack that filters via a `not_in_dv`
//!    [`ScalarUDF`] over parquet's `_row_number` virtual column ([`build_per_file_plan`]).
//!
//! The async DV path runs `DeletionVectorDescriptor::read(storage, base_url)` on
//! [`tokio::task::spawn_blocking`] because kernel's storage handler is sync I/O. Future
//! kernel API: a true-async variant that shares the body fetch's `ObjectStore` handle.

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
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, StructArray, UInt64Array,
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

/// Default opener batch size. DataFusion's parquet/json openers require a non-`None` batch
/// size before `create_file_opener` is called.
const DEFAULT_OPENER_BATCH_SIZE: usize = 8192;

/// Name of the parquet virtual `_row_number` column we request when a file has a deletion
/// vector. Matches the existing pattern in [`crate::compile::logical::scan`].
const ROW_NUMBER_COL: &str = "_row_number";

// ============================================================================
// Section 1: column / row extraction
// ============================================================================

/// Resolve a `LoadSink`'s `base_url`, returning a plan-compilation error if unset. Scan plans
/// always populate `base_url` with the snapshot's table root; the kernel `LoadSink` API typing
/// (`Option<Url>`) is the only reason we re-check at runtime.
pub(crate) fn sink_base_url(sink: &LoadSink) -> Result<&Url, DataFusionError> {
    sink.base_url.as_ref().ok_or_else(|| {
        crate::error::plan_compilation(
            "LoadSink.base_url must be set: scan-emitted plans always populate it with the \
             snapshot's table root, which is also the deletion-vector resolution root.",
        )
    })
}

/// Resolve a per-row path against `sink.base_url`. The path is `Url::join`-ed onto the base,
/// matching how the kernel scan plan emits relative paths under `snapshot.table_root()`.
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

/// Walk a nested struct path (`ColumnName`) out of `batch`, returning the array at the leaf.
/// Each segment after the first is descended into via `as_struct_opt().column_by_name(...)`.
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

/// Read one [`DeletionVectorDescriptor`] row out of a struct array. Field types are fixed by
/// kernel's [`DeletionVectorDescriptor::to_schema()`] (`Utf8 / Utf8 / Int32 / Int32 / Int64`)
/// so we downcast directly via [`AsArray`] -- no defensive type dispatch.
fn dv_from_row(sa: &StructArray, row: usize) -> Result<DeletionVectorDescriptor, DataFusionError> {
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
    Ok(DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv,
        offset,
        size_in_bytes,
        cardinality,
    })
}

/// Read the optional per-row [`DeletionVectorDescriptor`] from `batch`. Returns `None` when
/// `sink.dv_ref` is unset or the row's DV column is NULL.
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
    let struct_arr = arr.as_struct_opt().ok_or_else(|| {
        crate::error::internal_error(format!(
            "Load sink dv_ref column `{}` must be a struct matching DeletionVectorDescriptor",
            dv_cn.column
        ))
    })?;
    Ok(Some(dv_from_row(struct_arr, row)?))
}

/// Read the per-row file size from `batch`, defaulting to 0 when the size column is absent or
/// null. `PartitionedFile.size` is advisory; "0" means "unknown" and the parquet/json opener
/// re-resolves the actual size from object-store metadata at open time. **No** `std::fs::metadata`
/// fallback (Phase 3): blocking sync I/O has no place inside an async future.
fn file_size_for_row(batch: &RecordBatch, sink: &LoadSink, row: usize) -> DfResult<i64> {
    let Some(sz_cn) = sink.file_meta.size.as_ref() else {
        return Ok(0);
    };
    let arr = extract_column_array(batch, sz_cn)?;
    Ok(if arr.is_null(row) {
        0
    } else {
        arr.as_primitive::<Int64Type>().value(row)
    })
}

// ============================================================================
// Section 2: RowInputs + extract_row_inputs
// ============================================================================

/// Owned per-row inputs the open future captures. Constructed sync from one upstream row;
/// owns its file URL, advisory size, optional DV descriptor, and the *projected* passthrough
/// values (Phase 4 -- only the kept columns, in projected order).
#[derive(Debug, Clone)]
pub(crate) struct RowInputs {
    pub url: Url,
    pub size: i64,
    pub dv_descriptor: Option<DeletionVectorDescriptor>,
    pub partition_values: Vec<ScalarValue>,
}

/// Pure, sync extraction of one upstream row's worth of inputs.
///
/// `projected_passthrough` is global indices into `sink.passthrough_columns`, in the order the
/// projected source declared them. This is computed once at LoadExec construction (Phase 4);
/// here we just iterate it and extract one ScalarValue per kept passthrough column.
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
    // Path columns are always Utf8 per kernel's scan_live_actions_schema (STRING).
    let path_raw = path_arr.as_string::<i32>().value(row);
    let url = resolve_file_location(sink, path_raw)?;
    let size = file_size_for_row(batch, sink, row)?;
    let dv_descriptor = read_optional_dv_descriptor(batch, sink, row)?;
    let partition_values = projected_passthrough
        .iter()
        .map(|&i| {
            let cn = sink.passthrough_columns.get(i).ok_or_else(|| {
                crate::error::internal_error(format!(
                    "projected_passthrough index {i} out of range for sink.passthrough_columns \
                     (len={})",
                    sink.passthrough_columns.len()
                ))
            })?;
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

// ============================================================================
// Section 3: DV resolution + UDF + per-file plan stack
// ============================================================================

/// Async-resolve a deletion vector via [`tokio::task::spawn_blocking`] around kernel's sync
/// [`DeletionVectorDescriptor::read`]. Returns the raw [`RoaringTreemap`] (deleted row IDs)
/// wrapped in `Arc` so cheap clones travel into the per-file UDF closure.
///
/// Kernel's `read` does sync object-store IO via the `StorageHandler`; spawning on the blocking
/// pool keeps tokio worker threads non-blocked. A proper async kernel API would let us share
/// the body fetch's ObjectStore handle and avoid the thread-pool hop.
pub(crate) async fn resolve_dv_async(
    descriptor: DeletionVectorDescriptor,
    base_url: Url,
    engine: Arc<dyn Engine>,
) -> Result<Arc<RoaringTreemap>, DataFusionError> {
    let join = tokio::task::spawn_blocking(move || {
        descriptor.read(engine.storage_handler(), &base_url)
    })
    .await
    .map_err(|e| crate::error::internal_error(format!("DV resolve task join failed: {e}")))?;
    let treemap =
        join.map_err(|e| crate::error::internal_error(format!("DV resolve failed: {e}")))?;
    Ok(Arc::new(treemap))
}

/// Build a `not_in_dv(UInt64) -> Boolean` [`ScalarUDF`] closing over an
/// [`Arc<RoaringTreemap>`] of deleted row IDs. Returns `true` when the row is **NOT** in the
/// DV (i.e. should be kept), so the UDF can be plugged directly as the predicate of a
/// [`FilterExec`].
///
/// We store the bitmap in `Arc` (not `Vec<bool>`) for two reasons: (a) typical DVs are sparse
/// so the bitmap is dramatically smaller, and (b) kernel's
/// [`DeletionVectorDescriptor::read`] returns a `RoaringTreemap` directly, so there's no
/// reason to materialize a dense bool vector.
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

// ============================================================================
// Section 4: file-source / file-arrow-schema / partitioned-file builders
// ============================================================================

/// Build a [`FileSource`] for the load's file type whose [`TableSchema`] = file fields ++
/// passthrough fields. Optionally appends the parquet `_row_number` virtual column to the
/// file fields when `include_row_number` is true; this is the Int64 + `RowNumber` extension
/// type pattern the parquet decoder injects, matching
/// [`crate::compile::logical::scan`].
///
/// Passthrough fields are registered as DataFusion *partition columns* so the opener
/// broadcasts their per-file values from `PartitionedFile.partition_values` (parquet via
/// `replace_columns_with_literals`, json via `ProjectionOpener`). Projection, when present,
/// is pushed straight into the source via [`with_projection_indices`] -- which narrows both
/// the parquet read schema and the partition column materialization.
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
    // Strip per-field metadata from passthrough fields before handing them to `TableSchema`.
    // DataFusion's partition-col broadcast (`replace_columns_with_literals` / `ProjectionOpener`)
    // synthesizes the output array's `data_type` from the upstream-extracted `ScalarValue`, which
    // never carries the kernel's `delta.columnMapping.*` / `PARQUET:field_id` per-field metadata.
    // The kernel-declared metadata is reapplied per-batch in the metadata stamper.
    let stripped_passthrough_fields: Vec<FieldRef> = passthrough_fields
        .iter()
        .map(|f| Arc::new(strip_field_metadata_recursive(f.as_ref())))
        .collect();
    // Layout: [file, partition, virtual]. The parquet `_row_number` virtual column is
    // declared via [`TableSchema::with_virtual_columns`] (apache/datafusion#22026), which
    // keeps it OUT of the supplied schema fed to parquet's [`with_supplied_schema`]
    // validation while still exposing it in [`TableSchema::table_schema()`] (the schema
    // DataSourceExec exposes downstream). The opener forwards virtuals to
    // [`ArrowReaderOptions::with_virtual_columns`] so the decoder injects them.
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
    // Caller's projection indexes into LoadExec's full_schema = file_fields ++
    // passthrough_fields (no `_row_number`). pr-22026's TableSchema layout puts
    // `_row_number` **after** passthrough (file, partition, virtual), so passthrough
    // indices line up 1:1 with the caller's projection. When DV is in play we
    // additionally need `_row_number` available on the FilterExec predicate side;
    // append its index (= file_field_count + passthrough_count).
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

/// Recursively strip per-field metadata from `field` so the resulting field's `data_type`
/// matches structurally what DataFusion's partition-col broadcast (literal-substituted via
/// `ScalarValue`) produces at runtime.
pub(crate) fn strip_field_metadata_recursive(
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

/// Select the [`PhysicalExprAdapterFactory`](datafusion_physical_expr_adapter::PhysicalExprAdapterFactory)
/// for a given file type. Parquet decoding goes through field-id/column-mapping aware
/// adaptation; JSON keeps DataFusion's default name-based adapter.
pub(crate) fn adapter_factory_for(
    file_type: FileType,
) -> Option<Arc<dyn datafusion_physical_expr_adapter::PhysicalExprAdapterFactory>> {
    match file_type {
        FileType::Parquet => Some(Arc::new(FieldIdPhysicalExprAdapterFactory)),
        FileType::Json => None,
    }
}

/// Translate `url` + `size` into a DataFusion [`ObjectStoreUrl`] + [`PartitionedFile`] pair.
///
/// When `size <= 0`, the resulting `PartitionedFile.object_meta.size` is set to 0; callers
/// should resolve it via [`resolve_size_if_unknown`] before constructing the per-file
/// [`ExecutionPlan`] (parquet's footer reader needs a non-zero size). Kernel-emitted scan
/// plans always populate `LoadSink::file_meta.size`, but the IR allows `None`.
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

/// Resolve a [`PartitionedFile`]'s size via async object-store HEAD when the kernel-supplied
/// size is 0 (i.e. `LoadSink::file_meta.size` was unset or null). Mutates `pf` in place.
///
/// Async by design: object-store HEAD is async, and avoiding `std::fs::metadata` keeps tokio
/// worker threads non-blocked for any storage scheme. Kernel-emitted scan plans always
/// populate `LoadSink::file_meta.size` so this path is uncommon outside of tests.
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

// ============================================================================
// Section 5: per-file ExecutionPlan stack
// ============================================================================

/// Build the per-file [`ExecutionPlan`] stack for one upstream row.
///
/// When `dv` is `None`: just a single-file [`DataSourceExec`].
///
/// When `dv` is `Some`: `DataSourceExec` (file-arrow-schema includes `_row_number`) →
/// [`FilterExec`] applying the [`make_not_in_dv_udf`] predicate over `_row_number` →
/// [`ProjectionExec`] dropping `_row_number` from the output.
///
/// `output_schema` is the LoadExec's final projected output schema (file fields ++ projected
/// passthrough fields, with `_row_number` already excluded). When DV is present, the
/// ProjectionExec maps each `output_schema` field by name from the FilterExec's schema.
///
/// `file_source` MUST already be projection-pushed and (when DV is present) configured with
/// the `_row_number` virtual column appended to its file fields.
///
/// `config_options` is a snapshot of the running session's [`ConfigOptions`]; threaded
/// through to [`ScalarFunctionExpr::new`] via the FilterExec predicate.
///
/// **Async** because we may need to resolve the file's size via an object-store HEAD when
/// `LoadSink::file_meta.size` is unset (parquet's footer reader needs a non-zero size).
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
        // No DV path: single-file DataSourceExec. The plan's output schema must already match
        // `output_schema` (caller is responsible for not requesting `_row_number` when DV is
        // absent).
        debug_assert_eq!(
            plan.schema().fields().len(),
            output_schema.fields().len(),
            "build_per_file_plan: DataSourceExec schema mismatch with output_schema (no DV)"
        );
        return Ok(plan);
    };

    // DV path: wrap with FilterExec(not_in_dv(_row_number)) -> ProjectionExec(drop _row_number).
    let input_schema = plan.schema();
    let row_num_col = col(ROW_NUMBER_COL, &input_schema).map_err(|e| {
        crate::error::internal_error(format!(
            "build_per_file_plan: missing `{ROW_NUMBER_COL}` virtual column on \
             DataSourceExec schema {input_schema:?}: {e}"
        ))
    })?;
    // UDF accepts UInt64; parquet RowNumber emits Int64. Cast at predicate boundary.
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

    // ProjectionExec: pass each output_schema field through by name from the filtered schema,
    // implicitly dropping `_row_number`.
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
    let projected: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(proj_exprs, filtered)?);
    Ok(projected)
}
