//! Pure batch-in / batch-out helpers shared by [`super::LoadExec`].
//!
//! Each helper is a pure transformation: extract a column from a batch by [`ColumnName`] path,
//! parse a [`DeletionVectorDescriptor`] out of a struct row and resolve its mask via kernel's
//! [`selection_vector`], apply a DV mask to one decoded file batch, or broadcast a single
//! upstream row's passthrough value to `len` output rows. None of them touch parquet/json
//! decoding -- that's the DataFusion opener's job inside [`super::LoadExec`].

use std::str::FromStr;

use datafusion_common::error::DataFusionError;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::types::{Int32Type, Int64Type};
use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, StructArray, UInt32Array,
};
use delta_kernel::arrow::compute::{filter_record_batch, take};
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::ir::nodes::LoadSink;
use delta_kernel::scan::selection_vector;
use delta_kernel::Engine;
use url::Url;

fn kernel_err(e: delta_kernel::Error) -> DataFusionError {
    crate::error::internal_error(e.to_string())
}

/// Resolve a `LoadSink`'s `base_url`, returning a plan-compilation error if unset. Scan plans
/// always populate `base_url` with the snapshot's table root; the kernel `LoadSink` API typing
/// (`Option<Url>`) is the only reason we re-check at runtime.
fn sink_base_url(sink: &LoadSink) -> Result<&Url, DataFusionError> {
    sink.base_url.as_ref().ok_or_else(|| {
        crate::error::plan_compilation(
            "LoadSink.base_url must be set: scan-emitted plans always populate it with the \
             snapshot's table root, which is also the deletion-vector resolution root.",
        )
    })
}

/// Resolve a per-row path against `sink.base_url`. The path is `Url::join`-ed onto the base,
/// matching how the kernel scan plan emits relative paths under `snapshot.table_root()`.
pub(crate) fn resolve_file_location(sink: &LoadSink, path_str: &str) -> Result<Url, DataFusionError> {
    let base = sink_base_url(sink)?;
    let path_str = path_str.trim();
    base.join(path_str).map_err(|e| {
        crate::error::plan_compilation(format!(
            "Load sink could not join base_url `{base}` with path `{path_str}`: {e}"
        ))
    })
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

pub(crate) fn utf8_value_at(arr: &ArrayRef, row: usize) -> Result<Option<String>, DataFusionError> {
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

/// Read the optional per-row deletion-vector mask. Returns `None` when [`LoadSink::dv_ref`] is
/// unset or the row's DV column is NULL. Otherwise materializes a row-aligned selection vector
/// by calling [`selection_vector`] against the kernel engine, resolving the DV file paths
/// against `sink.base_url` (the snapshot table root) -- the same base used by
/// [`resolve_file_location`] for data files.
pub(crate) fn optional_selection_vector_for_row(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    engine: &dyn Engine,
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
    let table_root = sink_base_url(sink)?;
    Ok(Some(
        selection_vector(engine, &descriptor, table_root).map_err(kernel_err)?,
    ))
}

/// Apply the optional per-row DV mask to a single file-iterator batch. `start` is the file-local
/// row offset of `rb`'s first row (the caller tracks the cursor across successive batches from
/// the same file). Bits beyond the materialized mask default to `true` (retain) -- matches the
/// pre-streaming semantics where `mask.get(idx).copied().unwrap_or(true)` covered short masks.
pub(crate) fn apply_optional_dv(
    rb: RecordBatch,
    mask: Option<&[bool]>,
    start: usize,
) -> Result<RecordBatch, DataFusionError> {
    let Some(mask) = mask else {
        return Ok(rb);
    };
    let len = rb.num_rows();
    let slice: BooleanArray = (0..len)
        .map(|i| Some(mask.get(start + i).copied().unwrap_or(true)))
        .collect();
    filter_record_batch(&rb, &slice)
        .map_err(|e| crate::error::internal_error(format!("DV filter_record_batch failed: {e}")))
}

pub(crate) fn replicate_row(
    arr: &ArrayRef,
    row: usize,
    len: usize,
) -> Result<ArrayRef, DataFusionError> {
    let idx = UInt32Array::from(vec![row as u32; len]);
    take(arr.as_ref(), &idx, None)
        .map_err(|e| crate::error::internal_error(format!("broadcast take failed: {e}")))
}

pub(crate) fn optional_i64(
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

