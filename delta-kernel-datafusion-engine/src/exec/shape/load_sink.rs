//! Stream [`LoadSink`] output batches produced by kernel parquet/json handlers (passthrough
//! columns + optional DV masking). The executor collects the stream and registers the result
//! in the [`SessionContext`](datafusion::execution::context::SessionContext) under
//! `__dk_rel_{handle_id}` so downstream
//! [`RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef) leaves can scan
//! it as a [`MemTable`](datafusion::datasource::MemTable).

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::types::{Int32Type, Int64Type};
use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, LargeListArray, ListArray, MapArray, RecordBatch,
    StructArray, UInt32Array,
};
use delta_kernel::arrow::compute::{filter_record_batch, take};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::scan::selection_vector;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::{Engine, FileMeta};
use futures::{Stream, StreamExt};
use url::Url;

fn kernel_err(e: delta_kernel::Error) -> DeltaError {
    crate::error::internal_error(e.to_string())
}

pub struct KernelLoadSinkExec {
    child: Arc<dyn ExecutionPlan>,
    sink: LoadSink,
    engine: Arc<dyn Engine>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
    physical_read_schema: KernelSchemaRef,
    properties: Arc<PlanProperties>,
}

impl KernelLoadSinkExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        sink: LoadSink,
        engine: Arc<dyn Engine>,
        physical_read_schema: KernelSchemaRef,
    ) -> Result<Self, DeltaError> {
        let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef = Arc::new(
            sink.output_relation
                .schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e| {
                    crate::error::plan_compilation(format!(
                        "Load sink output schema conversion: {e}"
                    ))
                })?,
        );
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            child,
            sink,
            engine,
            arrow_schema,
            physical_read_schema,
            properties,
        })
    }
}

impl fmt::Debug for KernelLoadSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelLoadSinkExec")
            .field("handle_id", &self.sink.output_relation.id)
            .field("file_type", &self.sink.file_type)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for KernelLoadSinkExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KernelLoadSinkExec(handle_id={}, format={:?})",
            self.sink.output_relation.id, self.sink.file_type
        )
    }
}

impl ExecutionPlan for KernelLoadSinkExec {
    fn name(&self) -> &str {
        "KernelLoadSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let child = super::expect_single_child(children, "KernelLoadSinkExec")?;
        Ok(Arc::new(
            Self::try_new(
                child,
                self.sink.clone(),
                Arc::clone(&self.engine),
                self.physical_read_schema.clone(),
            )
            .map_err(crate::error::wrap_delta_err)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "KernelLoadSinkExec supports partition 0 only, got {partition}",
            )));
        }
        let inner = self.child.execute(partition, context)?;
        Ok(Box::pin(KernelLoadSinkStream {
            inner,
            sink: self.sink.clone(),
            engine: Arc::clone(&self.engine),
            physical_read_schema: self.physical_read_schema.clone(),
            output_arrow_schema: self.arrow_schema.clone(),
            pending: Vec::new(),
        }))
    }
}

struct KernelLoadSinkStream {
    inner: SendableRecordBatchStream,
    sink: LoadSink,
    engine: Arc<dyn Engine>,
    physical_read_schema: KernelSchemaRef,
    output_arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
    pending: Vec<RecordBatch>,
}

impl Stream for KernelLoadSinkStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if !self.pending.is_empty() {
                return Poll::Ready(Some(Ok(self.pending.remove(0))));
            }
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    match materialize_upstream_batch(
                        &batch,
                        &self.sink,
                        self.engine.as_ref(),
                        self.physical_read_schema.clone(),
                    ) {
                        Ok(outs) => self.pending = outs,
                        Err(e) => {
                            return Poll::Ready(Some(Err(crate::error::wrap_delta_err(e))));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for KernelLoadSinkStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.output_arrow_schema.clone()
    }
}

fn dv_table_root(load: &LoadSink, file_url: &Url) -> Url {
    load.base_url
        .clone()
        .unwrap_or_else(|| parent_directory_url(file_url))
}

fn parent_directory_url(url: &Url) -> Url {
    let mut out = url.clone();
    let path = url.path();
    if let Some(pos) = path.rfind('/') {
        let prefix = if pos == 0 { "/" } else { &path[..pos] };
        out.set_path(prefix);
    }
    out
}

fn resolve_file_location(base: &Option<Url>, path_str: &str) -> Result<Url, DeltaError> {
    let path_str = path_str.trim();
    if let Some(base) = base {
        base.join(path_str).map_err(|e| {
            crate::error::plan_compilation(format!(
                "Load sink could not join base_url `{base}` with path `{path_str}`: {e}"
            ))
        })
    } else {
        Url::parse(path_str).map_err(|e| {
            crate::error::plan_compilation(format!(
                "Load sink expected absolute file URL in path column, got `{path_str}`: {e}"
            ))
        })
    }
}

pub(crate) fn extract_column_array(
    batch: &RecordBatch,
    cn: &ColumnName,
) -> Result<ArrayRef, DeltaError> {
    let parts = cn.path();
    if parts.is_empty() {
        return Err(crate::error::plan_compilation(format!(
            "empty column path while extracting `{}`",
            cn
        )));
    }
    let mut current = batch
        .column_by_name(&parts[0])
        .ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "batch schema {:?} missing top-level `{}` while extracting `{}`",
                batch.schema(),
                parts[0],
                cn
            ))
        })?
        .clone();
    for seg in &parts[1..] {
        let sa = current.as_struct_opt().ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "expected struct column while extracting `{}` segment `{}`",
                cn, seg
            ))
        })?;
        current = sa
            .column_by_name(seg)
            .ok_or_else(|| {
                crate::error::plan_compilation(format!(
                    "struct missing `{}` while extracting `{}`",
                    seg, cn
                ))
            })?
            .clone();
    }
    Ok(current)
}

fn utf8_value_at(arr: &ArrayRef, row: usize) -> Result<Option<String>, DeltaError> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        ArrowDataType::Utf8 => Ok(Some(arr.as_string::<i32>().value(row).to_string())),
        ArrowDataType::LargeUtf8 => Ok(Some(arr.as_string::<i64>().value(row).to_string())),
        ArrowDataType::Utf8View => Ok(Some(arr.as_string_view().value(row).to_string())),
        other => Err(crate::error::internal_error(format!(
            "expected string column for Load path/size fragment, got {other:?}"
        ))),
    }
}

fn column_pref(sa: &StructArray, snake: &str, camel: &str) -> Result<ArrayRef, DeltaError> {
    sa.column_by_name(snake)
        .or_else(|| sa.column_by_name(camel))
        .cloned()
        .ok_or_else(|| {
            crate::error::internal_error(format!(
                "deletion vector struct missing `{snake}` / `{camel}` columns"
            ))
        })
}

fn dv_descriptor_from_struct_row(
    sa: &StructArray,
    row: usize,
) -> Result<DeletionVectorDescriptor, DeltaError> {
    let storage_arr = column_pref(sa, "storage_type", "storageType")?;
    let storage_raw = utf8_value_at(&storage_arr, row)?.ok_or_else(|| {
        crate::error::internal_error("Load sink dv_ref.storage_type was NULL".to_string())
    })?;
    let storage_type = DeletionVectorStorageType::from_str(storage_raw.trim()).map_err(|e| {
        crate::error::internal_error(format!("invalid DV storage_type `{storage_raw}`: {e}"))
    })?;

    let path_arr = column_pref(sa, "path_or_inline_dv", "pathOrInlineDv")?;
    let path_or_inline_dv = utf8_value_at(&path_arr, row)?.ok_or_else(|| {
        crate::error::internal_error("Load sink dv_ref.path_or_inline_dv was NULL".to_string())
    })?;

    let offset = read_optional_i32(sa, row, "offset")?;

    let size_arr = column_pref(sa, "size_in_bytes", "sizeInBytes")?;
    let size_in_bytes = read_required_i32(&size_arr, row, "size_in_bytes")?;

    let card_arr = column_pref(sa, "cardinality", "cardinality")?;
    let cardinality = read_required_i64(&card_arr, row, "cardinality")?;

    Ok(DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv,
        offset,
        size_in_bytes,
        cardinality,
    })
}

fn read_optional_i32(sa: &StructArray, row: usize, field: &str) -> Result<Option<i32>, DeltaError> {
    let arr = sa.column_by_name(field).cloned().ok_or_else(|| {
        crate::error::internal_error(format!(
            "deletion vector struct missing `{field}` for optional integer field"
        ))
    })?;
    if arr.is_null(row) {
        return Ok(None);
    }
    Ok(Some(read_required_i32(&arr, row, field)?))
}

fn read_required_i32(arr: &ArrayRef, row: usize, label: &str) -> Result<i32, DeltaError> {
    match arr.data_type() {
        ArrowDataType::Int32 => Ok(arr.as_primitive::<Int32Type>().value(row)),
        ArrowDataType::Int64 => Ok(arr
            .as_primitive::<Int64Type>()
            .value(row)
            .try_into()
            .map_err(|_| crate::error::internal_error(format!("DV `{label}` does not fit i32")))?),
        other => Err(crate::error::internal_error(format!(
            "DV `{label}` expected INT32/INT64, got {other:?}"
        ))),
    }
}

fn read_required_i64(arr: &ArrayRef, row: usize, label: &str) -> Result<i64, DeltaError> {
    match arr.data_type() {
        ArrowDataType::Int64 => Ok(arr.as_primitive::<Int64Type>().value(row)),
        ArrowDataType::Int32 => Ok(i64::from(arr.as_primitive::<Int32Type>().value(row))),
        other => Err(crate::error::internal_error(format!(
            "DV `{label}` expected INT64/INT32, got {other:?}"
        ))),
    }
}

fn optional_selection_vector_for_row(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    engine: &dyn Engine,
    table_root: &Url,
) -> Result<Option<Vec<bool>>, DeltaError> {
    let Some(ref dv_cn) = sink.dv_ref else {
        return Ok(None);
    };
    let dv_column = &dv_cn.column;
    let arr = extract_column_array(batch, dv_column)?;
    if arr.is_null(row) {
        return Ok(None);
    }
    let struct_arr = arr.as_struct_opt().ok_or_else(|| {
        crate::error::internal_error(format!(
            "Load sink dv_ref column `{}` must be a struct matching DeletionVectorDescriptor",
            dv_column
        ))
    })?;
    let descriptor = dv_descriptor_from_struct_row(struct_arr, row)?;
    Ok(Some(
        selection_vector(engine, &descriptor, table_root).map_err(kernel_err)?,
    ))
}

fn dv_mask_slice(mask: &[bool], start: usize, len: usize) -> Vec<bool> {
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let idx = start + i;
        out.push(mask.get(idx).copied().unwrap_or(true));
    }
    out
}

fn apply_optional_dv(
    rb: RecordBatch,
    mask: Option<&Vec<bool>>,
    row_offset: &mut usize,
) -> Result<RecordBatch, DeltaError> {
    let Some(mask) = mask else {
        *row_offset += rb.num_rows();
        return Ok(rb);
    };
    let start = *row_offset;
    let len = rb.num_rows();
    let slice = dv_mask_slice(mask, start, len);
    *row_offset += len;
    filter_record_batch(&rb, &BooleanArray::from(slice))
        .map_err(|e| crate::error::internal_error(format!("DV filter_record_batch failed: {e}")))
}

fn replicate_row(arr: &ArrayRef, row: usize, len: usize) -> Result<ArrayRef, DeltaError> {
    let idx = UInt32Array::from(vec![row as u32; len]);
    take(arr.as_ref(), &idx, None)
        .map_err(|e| crate::error::internal_error(format!("broadcast take failed: {e}")))
}

fn concat_with_passthrough(
    base: RecordBatch,
    passthrough: Vec<ArrayRef>,
    passthrough_names: &[String],
) -> Result<RecordBatch, DeltaError> {
    debug_assert_eq!(passthrough.len(), passthrough_names.len());
    let mut cols = base.columns().to_vec();
    let mut fields: Vec<delta_kernel::arrow::datatypes::Field> = base
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for (name, arr) in passthrough_names.iter().zip(passthrough.iter()) {
        fields.push(delta_kernel::arrow::datatypes::Field::new(
            name,
            arr.data_type().clone(),
            arr.null_count() > 0,
        ));
        cols.push(arr.clone());
    }
    let schema = Arc::new(delta_kernel::arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(schema, cols)
        .map_err(|e| crate::error::internal_error(format!("passthrough concat batch failed: {e}")))
}

fn broadcast_passthrough_for_batch(
    upstream_batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    target_len: usize,
) -> Result<(Vec<String>, Vec<ArrayRef>), DeltaError> {
    let mut names = Vec::with_capacity(sink.passthrough_columns.len());
    let mut arrays = Vec::with_capacity(sink.passthrough_columns.len());
    for cn in &sink.passthrough_columns {
        let arr = extract_column_array(upstream_batch, cn)?;
        names.push(cn.to_string());
        arrays.push(replicate_row(&arr, row, target_len)?);
    }
    Ok((names, arrays))
}

fn read_rows_for_location(
    sink: &LoadSink,
    engine: &dyn Engine,
    url: Url,
    size: i64,
    mask: Option<&Vec<bool>>,
    physical_read_schema: KernelSchemaRef,
) -> Result<Vec<RecordBatch>, DeltaError> {
    let meta = FileMeta::new(url, 0, u64::try_from(size.max(0)).unwrap_or(0));
    let mut out = Vec::new();
    match sink.file_type {
        FileType::Parquet => {
            let iter = engine
                .parquet_handler()
                .read_parquet_files(&[meta], physical_read_schema.clone(), None)
                .map_err(kernel_err)?;
            let mut row_offset = 0usize;
            for next in iter {
                let rb = next
                    .map_err(kernel_err)?
                    .try_into_record_batch()
                    .map_err(kernel_err)?;
                out.push(apply_optional_dv(rb, mask, &mut row_offset)?);
            }
        }
        FileType::Json => {
            let iter = engine
                .json_handler()
                .read_json_files(&[meta], physical_read_schema.clone(), None)
                .map_err(kernel_err)?;
            let mut row_offset = 0usize;
            for next in iter {
                let rb = next
                    .map_err(kernel_err)?
                    .try_into_record_batch()
                    .map_err(kernel_err)?;
                out.push(apply_optional_dv(rb, mask, &mut row_offset)?);
            }
        }
    }
    Ok(out)
}

fn optional_i64(
    batch: &RecordBatch,
    cn: &ColumnName,
    row: usize,
) -> Result<Option<i64>, DeltaError> {
    let arr = extract_column_array(batch, cn)?;
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        ArrowDataType::Int64 => Ok(Some(arr.as_primitive::<Int64Type>().value(row))),
        ArrowDataType::Int32 => Ok(Some(i64::from(arr.as_primitive::<Int32Type>().value(row)))),
        other => Err(crate::error::internal_error(format!(
            "expected LONG/INTEGER column `{}`, got {other:?}",
            cn
        ))),
    }
}

fn materialize_upstream_batch(
    batch: &RecordBatch,
    sink: &LoadSink,
    engine: &dyn Engine,
    physical_read_schema: KernelSchemaRef,
) -> Result<Vec<RecordBatch>, DeltaError> {
    let mut merged = Vec::new();
    let path_cn = &sink.file_meta.path;
    let maybe_size_cn = sink.file_meta.size.as_ref();
    let maybe_rc_cn = sink.file_meta.record_count.as_ref();

    for row in 0..batch.num_rows() {
        let path_arr = extract_column_array(batch, path_cn)?;
        let path_raw = utf8_value_at(&path_arr, row)?.ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "Load sink path column `{}` was NULL at upstream row {row}",
                path_cn
            ))
        })?;
        let url = resolve_file_location(&sink.base_url, &path_raw)?;
        let table_root = dv_table_root(sink, &url);

        let mask = optional_selection_vector_for_row(batch, sink, row, engine, &table_root)?;

        let size = if let Some(sz_cn) = maybe_size_cn {
            optional_i64(batch, sz_cn, row)?.unwrap_or(0)
        } else if url.scheme() == "file" {
            let p = url.to_file_path().map_err(|()| {
                crate::error::plan_compilation(format!(
                    "file URL could not be converted to path: {url}"
                ))
            })?;
            std::fs::metadata(&p).map(|m| m.len() as i64).unwrap_or(0)
        } else {
            0
        };

        let _ = maybe_rc_cn;

        let file_batches = read_rows_for_location(
            sink,
            engine,
            url.clone(),
            size,
            mask.as_ref(),
            physical_read_schema.clone(),
        )?;

        let target_schema_arrow: delta_kernel::arrow::datatypes::SchemaRef = Arc::new(
            sink.output_relation
                .schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e| crate::error::internal_error(format!("output schema arrow: {e}")))?,
        );

        for rb in file_batches {
            let (names, arrays) = broadcast_passthrough_for_batch(batch, sink, row, rb.num_rows())?;
            let mut extended = concat_with_passthrough(rb, arrays, &names)?;
            if extended.schema().as_ref() != target_schema_arrow.as_ref() {
                extended = arrow_columns_align_to_schema(extended, target_schema_arrow.clone())?;
            }
            merged.push(extended);
        }
    }

    Ok(merged)
}

fn arrow_columns_align_to_schema(
    batch: RecordBatch,
    wanted: delta_kernel::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, DeltaError> {
    if batch.schema().as_ref() == wanted.as_ref() {
        return Ok(batch);
    }
    let mut cols = Vec::with_capacity(wanted.fields().len());
    for f in wanted.fields() {
        let (idx, _) = batch.schema().column_with_name(f.name()).ok_or_else(|| {
            crate::error::internal_error(format!(
                "cannot align Load batch to output schema: missing column `{}`",
                f.name()
            ))
        })?;
        cols.push(realign_array_to_field(batch.column(idx), f.as_ref())?);
    }
    RecordBatch::try_new(wanted, cols)
        .map_err(|e| crate::error::internal_error(format!("align schema batch failed: {e}")))
}

fn realign_array_to_field(array: &ArrayRef, target_field: &Field) -> Result<ArrayRef, DeltaError> {
    match target_field.data_type() {
        ArrowDataType::Struct(target_fields) => realign_struct_array(array, target_fields),
        ArrowDataType::List(target_inner) => realign_list_array(array, target_inner.as_ref()),
        ArrowDataType::LargeList(target_inner) => {
            realign_large_list_array(array, target_inner.as_ref())
        }
        ArrowDataType::Map(target_entries, _) => realign_map_array(array, target_entries.as_ref()),
        _ => Ok(array.clone()),
    }
}

fn realign_struct_array(array: &ArrayRef, target_fields: &Fields) -> Result<ArrayRef, DeltaError> {
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| crate::error::internal_error("expected StructArray while aligning"))?;
    let new_columns = target_fields
        .iter()
        .zip(struct_arr.columns())
        .map(|(f, c)| realign_array_to_field(c, f.as_ref()))
        .collect::<Result<Vec<_>, DeltaError>>()?;
    Ok(Arc::new(StructArray::new(
        target_fields.clone(),
        new_columns,
        struct_arr.nulls().cloned(),
    )))
}

fn realign_list_array(
    array: &ArrayRef,
    target_element_field: &Field,
) -> Result<ArrayRef, DeltaError> {
    let list_arr = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| crate::error::internal_error("expected ListArray while aligning"))?;
    let values = realign_array_to_field(list_arr.values(), target_element_field)?;
    Ok(Arc::new(ListArray::new(
        Arc::new(target_element_field.clone()),
        list_arr.offsets().clone(),
        values,
        list_arr.nulls().cloned(),
    )))
}

fn realign_large_list_array(
    array: &ArrayRef,
    target_element_field: &Field,
) -> Result<ArrayRef, DeltaError> {
    let list_arr = array
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| crate::error::internal_error("expected LargeListArray while aligning"))?;
    let values = realign_array_to_field(list_arr.values(), target_element_field)?;
    Ok(Arc::new(LargeListArray::new(
        Arc::new(target_element_field.clone()),
        list_arr.offsets().clone(),
        values,
        list_arr.nulls().cloned(),
    )))
}

fn realign_map_array(
    array: &ArrayRef,
    target_entries_field: &Field,
) -> Result<ArrayRef, DeltaError> {
    let map_arr = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| crate::error::internal_error("expected MapArray while aligning"))?;
    let ArrowDataType::Struct(target_entry_fields) = target_entries_field.data_type() else {
        return Err(crate::error::internal_error(
            "map entries target must be struct",
        ));
    };
    let entries_ref: ArrayRef = Arc::new(map_arr.entries().clone());
    let new_entries = realign_struct_array(&entries_ref, target_entry_fields)?;
    let entries_struct = new_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| crate::error::internal_error("aligned map entries not struct"))?;
    Ok(Arc::new(MapArray::new(
        Arc::new(target_entries_field.clone()),
        map_arr.offsets().clone(),
        entries_struct.clone(),
        map_arr.nulls().cloned(),
        false,
    )))
}
