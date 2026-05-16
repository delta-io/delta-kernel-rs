//! Helpers used by the executor to materialize a [`SinkType::Load`] sink.
//!
//! For each upstream row, [`materialize_upstream_batch`] resolves a file URL +
//! optional deletion vector + per-row passthrough columns, hands the file URL to
//! the kernel parquet/json handler via [`read_rows_for_location`], and merges
//! the broadcast passthrough columns onto each emitted file row.
//!
//! These helpers are pure batch-in / batch-out; the executor's
//! [`crate::executor::DataFusionExecutor::drain_load`] driver supplies the
//! single-partition stream and the kernel [`Engine`] used for I/O.
//!
//! [`SinkType::Load`]: delta_kernel::plans::ir::nodes::SinkType::Load

use std::str::FromStr;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::types::{Int32Type, Int64Type};
use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, MapArray, RecordBatch, StructArray, UInt32Array,
};
use delta_kernel::arrow::compute::{filter_record_batch, take};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::scan::selection_vector;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::{Engine, FileMeta};
use url::Url;

/// Per-file column projection that the parquet / json kernel handler should
/// request when reading rows for a [`LoadSink`].
pub(crate) fn physical_read_schema(load: &LoadSink) -> Result<KernelSchemaRef, DataFusionError> {
    Ok(load.file_schema.clone())
}

fn kernel_err(e: delta_kernel::Error) -> DataFusionError {
    crate::error::internal_error(e.to_string())
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

fn resolve_file_location(base: &Option<Url>, path_str: &str) -> Result<Url, DataFusionError> {
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
) -> Result<ArrayRef, DataFusionError> {
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

fn utf8_value_at(arr: &ArrayRef, row: usize) -> Result<Option<String>, DataFusionError> {
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

fn column_pref(sa: &StructArray, snake: &str, camel: &str) -> Result<ArrayRef, DataFusionError> {
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
) -> Result<DeletionVectorDescriptor, DataFusionError> {
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

fn read_optional_i32(
    sa: &StructArray,
    row: usize,
    field: &str,
) -> Result<Option<i32>, DataFusionError> {
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

fn read_required_i32(arr: &ArrayRef, row: usize, label: &str) -> Result<i32, DataFusionError> {
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

fn read_required_i64(arr: &ArrayRef, row: usize, label: &str) -> Result<i64, DataFusionError> {
    match arr.data_type() {
        ArrowDataType::Int64 => Ok(arr.as_primitive::<Int64Type>().value(row)),
        ArrowDataType::Int32 => Ok(i64::from(arr.as_primitive::<Int32Type>().value(row))),
        other => Err(crate::error::internal_error(format!(
            "DV `{label}` expected INT64/INT32, got {other:?}"
        ))),
    }
}

pub(crate) fn optional_selection_vector_for_row(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    engine: &dyn Engine,
    table_root: &Url,
) -> Result<Option<Vec<bool>>, DataFusionError> {
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

pub(crate) fn apply_optional_dv(
    rb: RecordBatch,
    mask: Option<&Vec<bool>>,
    row_offset: &mut usize,
) -> Result<RecordBatch, DataFusionError> {
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

fn replicate_row(arr: &ArrayRef, row: usize, len: usize) -> Result<ArrayRef, DataFusionError> {
    let idx = UInt32Array::from(vec![row as u32; len]);
    take(arr.as_ref(), &idx, None)
        .map_err(|e| crate::error::internal_error(format!("broadcast take failed: {e}")))
}

fn concat_with_passthrough(
    base: RecordBatch,
    passthrough: Vec<ArrayRef>,
    passthrough_names: &[String],
) -> Result<RecordBatch, DataFusionError> {
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
) -> Result<(Vec<String>, Vec<ArrayRef>), DataFusionError> {
    let mut names = Vec::with_capacity(sink.passthrough_columns.len());
    let mut arrays = Vec::with_capacity(sink.passthrough_columns.len());
    for cn in &sink.passthrough_columns {
        let arr = extract_column_array(upstream_batch, cn)?;
        names.push(cn.to_string());
        arrays.push(replicate_row(&arr, row, target_len)?);
    }
    Ok((names, arrays))
}

pub(crate) fn read_rows_for_location(
    sink: &LoadSink,
    engine: &dyn Engine,
    url: Url,
    size: i64,
    mask: Option<&Vec<bool>>,
    physical_read_schema: KernelSchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
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
) -> Result<Option<i64>, DataFusionError> {
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

/// Read one upstream batch's worth of file rows. For each upstream row, resolve
/// the file URL + optional DV + size hint, ask the kernel handler for that
/// file's row groups, broadcast the upstream row's passthrough columns onto
/// every emitted file row, and align the resulting columns to the sink's
/// declared output schema.
pub(crate) fn materialize_upstream_batch(
    batch: &RecordBatch,
    sink: &LoadSink,
    engine: &dyn Engine,
    physical_read_schema: KernelSchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
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
) -> Result<RecordBatch, DataFusionError> {
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

/// Walk `array` against the shape of `target_field`, rebuilding any nested
/// struct/list/map containers so the result's element/field types match the target. Primitive
/// arrays pass through unchanged. The recursion handles nullability propagation: each container
/// is rebuilt with the target child field(s), so any tightening or relaxing of nested
/// nullability lands in the output without rewriting actual values.
fn realign_array_to_field(
    array: &ArrayRef,
    target_field: &Field,
) -> Result<ArrayRef, DataFusionError> {
    match target_field.data_type() {
        ArrowDataType::Struct(target_fields) => realign_struct(array, target_fields),
        ArrowDataType::List(target_inner) => realign_list::<i32>(array, target_inner.as_ref()),
        ArrowDataType::LargeList(target_inner) => realign_list::<i64>(array, target_inner.as_ref()),
        ArrowDataType::Map(target_entries, _) => realign_map(array, target_entries.as_ref()),
        _ => Ok(array.clone()),
    }
}

fn realign_struct(array: &ArrayRef, target_fields: &Fields) -> Result<ArrayRef, DataFusionError> {
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| crate::error::internal_error("expected StructArray while aligning"))?;
    let new_columns = target_fields
        .iter()
        .zip(struct_arr.columns())
        .map(|(f, c)| realign_array_to_field(c, f.as_ref()))
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    Ok(Arc::new(StructArray::new(
        target_fields.clone(),
        new_columns,
        struct_arr.nulls().cloned(),
    )))
}

/// Generic recursion for `ListArray` / `LargeListArray`. `O = i32` recovers the
/// standard `ListArray`; `O = i64` recovers `LargeListArray`. Both downcast to
/// `GenericListArray<O>` and rebuild it with the target element field.
fn realign_list<O>(
    array: &ArrayRef,
    target_element_field: &Field,
) -> Result<ArrayRef, DataFusionError>
where
    O: delta_kernel::arrow::array::OffsetSizeTrait,
{
    let list_arr = array
        .as_any()
        .downcast_ref::<delta_kernel::arrow::array::GenericListArray<O>>()
        .ok_or_else(|| {
            crate::error::internal_error(format!(
                "expected GenericListArray<{}> while aligning",
                std::any::type_name::<O>()
            ))
        })?;
    let values = realign_array_to_field(list_arr.values(), target_element_field)?;
    Ok(Arc::new(
        delta_kernel::arrow::array::GenericListArray::<O>::new(
            Arc::new(target_element_field.clone()),
            list_arr.offsets().clone(),
            values,
            list_arr.nulls().cloned(),
        ),
    ))
}

fn realign_map(
    array: &ArrayRef,
    target_entries_field: &Field,
) -> Result<ArrayRef, DataFusionError> {
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
    let new_entries = realign_struct(&entries_ref, target_entry_fields)?;
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
