//! Pure batch-in / batch-out helpers shared by [`super::LoadExec`].
//!
//! Two responsibilities:
//!
//! 1. Walk a nested struct path out of a metadata [`RecordBatch`] by [`ColumnName`]
//!    ([`extract_column_array`]).
//! 2. Resolve the optional per-row deletion-vector mask via kernel's [`selection_vector`]
//!    ([`optional_selection_vector_for_row`]) and apply it to one decoded file batch
//!    ([`apply_optional_dv`]).
//!
//! All other field reads use Arrow [`AsArray`] downcasts directly at the call site (the column
//! types are fixed by kernel's `to_schema()` derives). Passthrough-column broadcast is delegated
//! to DataFusion's `partition_values` plumbing -- see [`super::LoadExec`] docs.

use datafusion_common::error::DataFusionError;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::types::{Int32Type, Int64Type};
use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, StructArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::ir::nodes::LoadSink;
use delta_kernel::scan::selection_vector;
use delta_kernel::Engine;
use url::Url;

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
    let head = parts.next().ok_or_else(|| {
        crate::error::plan_compilation(format!("empty column path `{cn}`"))
    })?;
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
fn dv_from_row(
    sa: &StructArray,
    row: usize,
) -> Result<DeletionVectorDescriptor, DataFusionError> {
    let col = |name: &str| {
        sa.column_by_name(name).ok_or_else(|| {
            crate::error::internal_error(format!(
                "deletion vector struct missing `{name}` column"
            ))
        })
    };
    let storage_type: DeletionVectorStorageType = col("storageType")?
        .as_string::<i32>()
        .value(row)
        .trim()
        .parse()
        .map_err(|e: delta_kernel::Error| crate::error::internal_error(e.to_string()))?;
    let path_or_inline_dv = col("pathOrInlineDv")?.as_string::<i32>().value(row).to_string();
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

/// Read the optional per-row deletion-vector mask. Returns `None` when [`LoadSink::dv_ref`] is
/// unset or the row's DV column is NULL. Otherwise materializes a row-aligned selection vector
/// by calling [`selection_vector`] against the kernel engine, resolving DV file paths against
/// `sink.base_url` (the snapshot table root) -- the same base used by [`resolve_file_location`]
/// for data files.
pub(crate) fn optional_selection_vector_for_row(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    engine: &dyn Engine,
) -> Result<Option<Vec<bool>>, DataFusionError> {
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
    let descriptor = dv_from_row(struct_arr, row)?;
    Ok(Some(
        selection_vector(engine, &descriptor, sink_base_url(sink)?)
            .map_err(|e| crate::error::internal_error(e.to_string()))?,
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
