//! Convert a [`CustomEngineData`] (engine-owned columnar batch reachable via
//! `materialize_columns` upcalls) into an [`ArrowEngineData`] so the kernel's
//! default Arrow-based eval handler can operate on JSON-derived batches
//! without each engine having to implement evaluation itself.
//!
//! This is the boundary that unblocks log replay through custom handlers: the
//! kernel reads the JSON log via the custom `JsonHandler`, every yielded batch
//! is converted here into an `ArrowEngineData` with the physical schema the
//! handler was asked for, and from there the Arrow eval handler takes over
//! exactly as it does for the default engine.
//!
//! Coverage: the column types log replay touches against the public Delta
//! protocol -- bool, int, long, string, `list<string>`, `map<string,string>`,
//! and structs of the above. Other types fall through with a clear error so
//! callers see "kernel handed CustomEngineData with unsupported type X for
//! column Y" rather than silent zero-rows or panic.
//!
//! Wire format reference: see [`ColumnBuffers`] and [`ColumnTypeTag`] in
//! [`crate::custom_io_engine::json`]. Layout is intentionally Arrow-shaped so
//! the conversion below is mostly buffer copies, with one bit-pack step for
//! booleans (engines emit one byte per row; Arrow expects bit-packed).

use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{ArrayType, DataType, MapType, PrimitiveType, StructType};
use delta_kernel::{DeltaResult, Error};

use crate::custom_io_engine::{
    ColumnBuffers, ColumnTypeTag, CustomEngineData, VisitRowsRequest, VisitRowsResult,
};
use crate::KernelStringSlice;

/// Convert an engine-owned batch into an [`ArrowEngineData`] matching `schema`.
///
/// Calls `materialize_columns` once with all leaf paths in the schema, then
/// folds the returned [`ColumnBuffers`] back into a nested `StructArray`
/// matching the schema's shape. Frees the engine buffers via `free_columns`
/// before returning, regardless of success or failure.
pub(crate) fn custom_engine_data_to_record_batch(
    data: &CustomEngineData,
    schema: &StructType,
) -> DeltaResult<RecordBatch> {
    let leaves = collect_leaves(schema, &mut Vec::new());
    let leaf_paths: Vec<String> = leaves.iter().map(|(p, _)| p.clone()).collect();
    let path_slices: Vec<KernelStringSlice> = leaf_paths
        .iter()
        // SAFETY: `leaf_paths` outlives this synchronous upcall.
        .map(|s| unsafe { KernelStringSlice::new_unsafe(s.as_str()) })
        .collect();

    let request = VisitRowsRequest {
        batch_handle: data.batch_handle,
        paths_len: path_slices.len(),
        paths: path_slices.as_ptr(),
    };
    let mut result = VisitRowsResult {
        columns: std::ptr::null(),
        columns_len: 0,
        error: 0,
    };
    (data.state.callbacks.materialize_columns)(
        data.state.engine_state,
        &request as *const _,
        &mut result as *mut _,
    );
    if result.error != 0 {
        return Err(Error::generic(format!(
            "materialize_columns signalled error {} during arrow bridge",
            result.error
        )));
    }
    if result.columns_len != leaves.len() {
        // Always free what the engine returned before erroring.
        (data.state.callbacks.free_columns)(
            data.state.engine_state,
            result.columns,
            result.columns_len,
        );
        return Err(Error::generic(format!(
            "arrow bridge: engine returned {} columns; kernel requested {} for schema {:?}",
            result.columns_len,
            leaves.len(),
            schema
        )));
    }
    let columns_slice = if result.columns_len == 0 {
        &[][..]
    } else {
        // SAFETY: engine guarantees `columns` valid for `columns_len`
        // entries until `free_columns` is called.
        unsafe { std::slice::from_raw_parts(result.columns, result.columns_len) }
    };

    // Convert each leaf to an ArrayRef. `convert_outcome` is computed eagerly
    // before `free_columns` so we can release engine memory regardless of
    // whether assembly succeeds.
    let convert_outcome = leaves
        .iter()
        .zip(columns_slice.iter())
        .map(|((path, dtype), buf)| {
            column_buffers_to_array(buf, dtype)
                .map_err(|e| {
                    Error::generic(format!(
                        "arrow bridge: failed to convert column {:?}: {}",
                        path, e
                    ))
                })
                .map(|arr| (path.clone(), arr))
        })
        .collect::<DeltaResult<Vec<_>>>();

    // Free engine buffers before propagating any conversion error.
    (data.state.callbacks.free_columns)(
        data.state.engine_state,
        result.columns,
        result.columns_len,
    );

    let leaf_arrays: std::collections::HashMap<String, ArrayRef> =
        convert_outcome?.into_iter().collect();

    // Re-assemble the nested struct shape.
    let mut prefix = Vec::new();
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .map(|f| {
            build_field_array(
                f.name(),
                f.data_type(),
                f.is_nullable(),
                &leaf_arrays,
                &mut prefix,
            )
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    let arrow_schema = delta_kernel::arrow::datatypes::Schema::try_from_kernel(schema)
        .map_err(|e| Error::generic(format!("arrow bridge: failed to build arrow schema: {e}")))?;
    RecordBatch::try_new(Arc::new(arrow_schema), arrays)
        .map_err(|e| Error::generic(format!("arrow bridge: failed to build record batch: {e}")))
}

/// Same as [`custom_engine_data_to_record_batch`] but wraps the result in an
/// [`ArrowEngineData`] for return through the `EngineData` trait.
pub(crate) fn custom_engine_data_to_arrow_engine_data(
    data: &CustomEngineData,
    schema: &StructType,
) -> DeltaResult<Box<dyn delta_kernel::EngineData>> {
    let batch = custom_engine_data_to_record_batch(data, schema)?;
    Ok(Box::new(ArrowEngineData::new(batch)))
}

// ============================================================================
// Leaf path collection
// ============================================================================

/// Recursively walks a `StructType` and returns `(dotted_path, leaf_type)`
/// for every leaf the column-materialization protocol expects.
///
/// Match `StructType::leaves` semantics: `StructType` fields are traversed,
/// while `Array`, `Map`, `Variant`, and primitive fields are leaves -- the
/// engine encodes them as a single column with nested children inside one
/// `ColumnBuffers` entry.
fn collect_leaves(schema: &StructType, prefix: &mut Vec<String>) -> Vec<(String, DataType)> {
    let mut out = Vec::new();
    for field in schema.fields() {
        prefix.push(field.name().to_string());
        match field.data_type() {
            DataType::Struct(inner) => {
                out.extend(collect_leaves(inner, prefix));
            }
            other => {
                out.push((prefix.join("."), other.clone()));
            }
        }
        prefix.pop();
    }
    out
}

// ============================================================================
// Struct rebuild
// ============================================================================

fn build_field_array(
    field_name: &str,
    dtype: &DataType,
    nullable: bool,
    leaves: &std::collections::HashMap<String, ArrayRef>,
    prefix: &mut Vec<String>,
) -> DeltaResult<ArrayRef> {
    prefix.push(field_name.to_string());
    let result = match dtype {
        DataType::Struct(inner) => build_struct_from_leaves(inner, leaves, prefix),
        _ => {
            let path = prefix.join(".");
            leaves.get(&path).cloned().ok_or_else(|| {
                Error::generic(format!(
                    "arrow bridge: leaf array missing for path {:?}",
                    path
                ))
            })
        }
    };
    prefix.pop();
    let _ = nullable; // Nullability is encoded in the per-leaf null bitmap.
    result
}

fn build_struct_from_leaves(
    schema: &StructType,
    leaves: &std::collections::HashMap<String, ArrayRef>,
    prefix: &mut Vec<String>,
) -> DeltaResult<ArrayRef> {
    let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(schema.num_fields());
    let mut child_fields: Vec<Arc<ArrowField>> = Vec::with_capacity(schema.num_fields());
    for field in schema.fields() {
        let arr = build_field_array(
            field.name(),
            field.data_type(),
            field.is_nullable(),
            leaves,
            prefix,
        )?;
        child_fields.push(Arc::new(ArrowField::new(
            field.name(),
            ArrowDataType::try_from_kernel(field.data_type()).map_err(|e| {
                Error::generic(format!(
                    "arrow bridge: failed to convert field {:?} to arrow type: {}",
                    field.name(),
                    e
                ))
            })?,
            field.is_nullable(),
        )));
        child_arrays.push(arr);
    }
    // Compute the parent struct's null mask: a row is null iff every child is
    // null at that row. JSON action rows look exactly like this -- a row that
    // represents an "add" has null at every "protocol.*" / "metaData.*" /
    // "remove.*" leaf. Without this mask Arrow rejects null bits on
    // non-nullable child fields like protocol.minReaderVersion.
    let row_count = child_arrays.first().map(|a| a.len()).unwrap_or(0);
    let struct_null = struct_null_buffer(&child_arrays, row_count);
    let struct_array = StructArray::try_new(child_fields.into(), child_arrays, struct_null)
        .map_err(|e| Error::generic(format!("arrow bridge: failed to assemble struct: {e}")))?;
    Ok(Arc::new(struct_array))
}

/// Build a null buffer for a struct: row R is null iff every child is null at
/// R. Returns `None` if no row is fully null (Arrow optimization -- absent
/// null buffer means "no nulls").
fn struct_null_buffer(children: &[ArrayRef], row_count: usize) -> Option<NullBuffer> {
    if children.is_empty() || row_count == 0 {
        return None;
    }
    let mut packed = vec![0u8; row_count.div_ceil(8)];
    let mut any_struct_null = false;
    for i in 0..row_count {
        let any_valid = children.iter().any(|c| c.is_valid(i));
        if any_valid {
            packed[i / 8] |= 1 << (i % 8);
        } else {
            any_struct_null = true;
        }
    }
    if any_struct_null {
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from(packed),
            0,
            row_count,
        )))
    } else {
        None
    }
}

// ============================================================================
// ColumnBuffers -> ArrayRef
// ============================================================================

fn column_buffers_to_array(col: &ColumnBuffers, dtype: &DataType) -> DeltaResult<ArrayRef> {
    let null_buf = read_null_buffer(col)?;
    match dtype {
        DataType::Primitive(p) => primitive_to_array(col, p, null_buf),
        DataType::Array(arr) => list_to_array(col, arr, null_buf),
        DataType::Map(m) => map_to_array(col, m, null_buf),
        DataType::Struct(_) => Err(Error::generic(
            "arrow bridge: struct columns must be assembled from sub-leaves, not via \
             column_buffers_to_array",
        )),
        other => Err(Error::generic(format!(
            "arrow bridge: column type {:?} not supported",
            other
        ))),
    }
}

fn primitive_to_array(
    col: &ColumnBuffers,
    p: &PrimitiveType,
    null_buf: Option<NullBuffer>,
) -> DeltaResult<ArrayRef> {
    let n = col.row_count;
    if col.type_tag == ColumnTypeTag::Null as u8 {
        // All-null payload from the engine -- materialize as a typed
        // null-only array so downstream eval doesn't crash on type checks.
        return all_null_primitive_array(p, n);
    }
    // Coercion table: the materializer makes a best-guess type from the parsed
    // value (Integer fits, otherwise Long, Double, etc.), and the schema
    // specifies the target. Most coercions are widenings (Int -> Long,
    // Int -> Date, Long -> Timestamp), which we handle here.
    match p {
        PrimitiveType::Boolean => {
            require_tag(col, ColumnTypeTag::Bool, "Boolean")?;
            let bool_buf = decode_bool_primary(col, n)?;
            Ok(Arc::new(BooleanArray::new(bool_buf, null_buf)))
        }
        PrimitiveType::Byte => {
            // Accept Int (promoted) or Byte. JSON has no concept of byte
            // distinct from int, so values come in as Int.
            let v = read_signed_int_as_i64(col, n)?;
            let mut bytes: Vec<i8> = Vec::with_capacity(n);
            for x in v {
                if x < i8::MIN as i64 || x > i8::MAX as i64 {
                    return Err(Error::generic(format!(
                        "arrow bridge: value {x} out of range for Byte"
                    )));
                }
                bytes.push(x as i8);
            }
            Ok(Arc::new(Int8Array::new(
                ScalarBuffer::<i8>::from(bytes),
                null_buf,
            )))
        }
        PrimitiveType::Short => {
            let v = read_signed_int_as_i64(col, n)?;
            let mut shorts: Vec<i16> = Vec::with_capacity(n);
            for x in v {
                if x < i16::MIN as i64 || x > i16::MAX as i64 {
                    return Err(Error::generic(format!(
                        "arrow bridge: value {x} out of range for Short"
                    )));
                }
                shorts.push(x as i16);
            }
            Ok(Arc::new(Int16Array::new(
                ScalarBuffer::<i16>::from(shorts),
                null_buf,
            )))
        }
        PrimitiveType::Integer => {
            require_tag(col, ColumnTypeTag::Int, "Integer")?;
            let bytes = primary_slice(col, n.checked_mul(4).ok_or_else(overflow)?)?;
            let buf = Buffer::from(bytes);
            let scalar = ScalarBuffer::<i32>::new(buf, 0, n);
            Ok(Arc::new(Int32Array::new(scalar, null_buf)))
        }
        PrimitiveType::Long => {
            let v = read_signed_int_as_i64(col, n)?;
            Ok(Arc::new(Int64Array::new(
                ScalarBuffer::<i64>::from(v),
                null_buf,
            )))
        }
        PrimitiveType::Float => {
            // Engines emit Double for any fractional JSON value; cast.
            let doubles = read_fractional_as_f64(col, n)?;
            let floats: Vec<f32> = doubles.into_iter().map(|d| d as f32).collect();
            Ok(Arc::new(Float32Array::new(
                ScalarBuffer::<f32>::from(floats),
                null_buf,
            )))
        }
        PrimitiveType::Double => {
            let doubles = read_fractional_as_f64(col, n)?;
            Ok(Arc::new(Float64Array::new(
                ScalarBuffer::<f64>::from(doubles),
                null_buf,
            )))
        }
        PrimitiveType::String => {
            require_tag(col, ColumnTypeTag::String, "String")?;
            string_array(col, null_buf)
        }
        PrimitiveType::Date => {
            // Days since epoch. JSON value is typically an integer.
            let v = read_signed_int_as_i64(col, n)?;
            let mut days: Vec<i32> = Vec::with_capacity(n);
            for x in v {
                if x < i32::MIN as i64 || x > i32::MAX as i64 {
                    return Err(Error::generic(format!(
                        "arrow bridge: value {x} out of range for Date (days)"
                    )));
                }
                days.push(x as i32);
            }
            Ok(Arc::new(Date32Array::new(
                ScalarBuffer::<i32>::from(days),
                null_buf,
            )))
        }
        PrimitiveType::Timestamp => {
            // Microseconds since epoch, UTC.
            let v = read_signed_int_as_i64(col, n)?;
            Ok(Arc::new(
                TimestampMicrosecondArray::new(ScalarBuffer::<i64>::from(v), null_buf)
                    .with_timezone("UTC"),
            ))
        }
        PrimitiveType::TimestampNtz => {
            let v = read_signed_int_as_i64(col, n)?;
            Ok(Arc::new(TimestampMicrosecondArray::new(
                ScalarBuffer::<i64>::from(v),
                null_buf,
            )))
        }
        PrimitiveType::Binary => {
            // Some engines emit binary as a String column; for our wire format
            // Binary uses the same offsets+aux layout as String, just without
            // UTF-8 validation.
            require_tag_any(
                col,
                &[ColumnTypeTag::Binary, ColumnTypeTag::String],
                "Binary",
            )?;
            let n = col.row_count;
            let offset_bytes = primary_slice(col, (n + 1).checked_mul(4).ok_or_else(overflow)?)?;
            let offsets_buf = Buffer::from(offset_bytes);
            let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::new(offsets_buf, 0, n + 1));
            let value_bytes = if col.aux.is_null() || col.aux_len == 0 {
                Buffer::from(Vec::<u8>::new())
            } else {
                let raw = unsafe { std::slice::from_raw_parts(col.aux, col.aux_len) };
                Buffer::from(raw)
            };
            Ok(Arc::new(
                BinaryArray::try_new(offsets, value_bytes, null_buf)
                    .map_err(|e| Error::generic(format!("arrow bridge: build binary: {e}")))?,
            ))
        }
        other => Err(Error::generic(format!(
            "arrow bridge: primitive type {:?} not yet implemented",
            other
        ))),
    }
}

/// Read the column's primary buffer as a `Vec<i64>`, accepting Int or Long
/// tags. Used by every signed-integer-shaped target type (Long, Date,
/// Timestamp, Byte, Short).
fn read_signed_int_as_i64(col: &ColumnBuffers, n: usize) -> DeltaResult<Vec<i64>> {
    match col.type_tag {
        t if t == ColumnTypeTag::Long as u8 => {
            let bytes = primary_slice(col, n.checked_mul(8).ok_or_else(overflow)?)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let mut tmp = [0u8; 8];
                tmp.copy_from_slice(&bytes[i * 8..(i + 1) * 8]);
                out.push(i64::from_ne_bytes(tmp));
            }
            Ok(out)
        }
        t if t == ColumnTypeTag::Int as u8 => {
            let bytes = primary_slice(col, n.checked_mul(4).ok_or_else(overflow)?)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let mut tmp = [0u8; 4];
                tmp.copy_from_slice(&bytes[i * 4..(i + 1) * 4]);
                out.push(i32::from_ne_bytes(tmp) as i64);
            }
            Ok(out)
        }
        t if t == ColumnTypeTag::Short as u8 => {
            let bytes = primary_slice(col, n.checked_mul(2).ok_or_else(overflow)?)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let mut tmp = [0u8; 2];
                tmp.copy_from_slice(&bytes[i * 2..(i + 1) * 2]);
                out.push(i16::from_ne_bytes(tmp) as i64);
            }
            Ok(out)
        }
        t if t == ColumnTypeTag::Byte as u8 => {
            let bytes = primary_slice(col, n)?;
            Ok(bytes.iter().map(|&b| b as i8 as i64).collect())
        }
        tag => Err(Error::generic(format!(
            "arrow bridge: expected integer-shaped tag for signed-int target, got tag={}",
            tag
        ))),
    }
}

/// Read the column's primary buffer as a `Vec<f64>`, accepting Float or
/// Double tags. Promotes Float to Double.
fn read_fractional_as_f64(col: &ColumnBuffers, n: usize) -> DeltaResult<Vec<f64>> {
    match col.type_tag {
        t if t == ColumnTypeTag::Double as u8 => {
            let bytes = primary_slice(col, n.checked_mul(8).ok_or_else(overflow)?)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let mut tmp = [0u8; 8];
                tmp.copy_from_slice(&bytes[i * 8..(i + 1) * 8]);
                out.push(f64::from_ne_bytes(tmp));
            }
            Ok(out)
        }
        t if t == ColumnTypeTag::Float as u8 => {
            let bytes = primary_slice(col, n.checked_mul(4).ok_or_else(overflow)?)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let mut tmp = [0u8; 4];
                tmp.copy_from_slice(&bytes[i * 4..(i + 1) * 4]);
                out.push(f32::from_ne_bytes(tmp) as f64);
            }
            Ok(out)
        }
        // Allow promoting integer -> double in case the engine emitted a long
        // because every value happened to be a whole number.
        t if t == ColumnTypeTag::Long as u8 || t == ColumnTypeTag::Int as u8 => {
            let ints = read_signed_int_as_i64(col, n)?;
            Ok(ints.into_iter().map(|x| x as f64).collect())
        }
        tag => Err(Error::generic(format!(
            "arrow bridge: expected fractional or integer tag for float target, got tag={}",
            tag
        ))),
    }
}

fn decode_bool_primary(col: &ColumnBuffers, n: usize) -> DeltaResult<BooleanBuffer> {
    let bytes = primary_slice(col, n)?;
    let mut packed = vec![0u8; n.div_ceil(8)];
    for (i, &b) in bytes.iter().enumerate().take(n) {
        if b != 0 {
            packed[i / 8] |= 1 << (i % 8);
        }
    }
    Ok(BooleanBuffer::new(Buffer::from(packed), 0, n))
}

fn require_tag_any(
    col: &ColumnBuffers,
    expected: &[ColumnTypeTag],
    label: &str,
) -> DeltaResult<()> {
    if expected.iter().any(|t| col.type_tag == *t as u8) {
        return Ok(());
    }
    Err(Error::generic(format!(
        "arrow bridge: expected {} (one of {:?}) but got tag={}",
        label,
        expected.iter().map(|t| *t as u8).collect::<Vec<_>>(),
        col.type_tag
    )))
}

fn list_to_array(
    col: &ColumnBuffers,
    arr: &ArrayType,
    null_buf: Option<NullBuffer>,
) -> DeltaResult<ArrayRef> {
    if col.type_tag != ColumnTypeTag::List as u8 {
        // Allow null sentinel for empty/null payloads.
        if col.type_tag == ColumnTypeTag::Null as u8 {
            // Build an all-null list of the right type.
            let element_arrow = ArrowDataType::try_from_kernel(&arr.element_type).map_err(|e| {
                Error::generic(format!(
                    "arrow bridge: failed to build list element type: {e}"
                ))
            })?;
            let field = Arc::new(ArrowField::new("element", element_arrow, arr.contains_null));
            let n = col.row_count;
            let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0i32; n + 1]));
            let empty_values = empty_array_for(&arr.element_type)?;
            return Ok(Arc::new(
                ListArray::try_new(field, offsets, empty_values, null_buf).map_err(|e| {
                    Error::generic(format!("arrow bridge: failed to build empty list: {e}"))
                })?,
            ));
        }
        return Err(Error::generic(format!(
            "arrow bridge: expected List tag for array type, got tag={}",
            col.type_tag
        )));
    }
    let n = col.row_count;
    let offset_bytes = primary_slice(col, (n + 1).checked_mul(4).ok_or_else(overflow)?)?;
    let offsets_buf = Buffer::from(offset_bytes);
    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::new(offsets_buf, 0, n + 1));
    if col.children_len != 1 || col.children.is_null() {
        return Err(Error::generic(
            "arrow bridge: list column must have exactly one child",
        ));
    }
    // SAFETY: engine guarantees `children` valid for `children_len` entries
    // for the duration of this call.
    let child_buf = unsafe { &*col.children };
    let child_array = column_buffers_to_array(child_buf, &arr.element_type)?;
    let element_arrow = ArrowDataType::try_from_kernel(&arr.element_type).map_err(|e| {
        Error::generic(format!(
            "arrow bridge: failed to build list element type: {e}"
        ))
    })?;
    let field = Arc::new(ArrowField::new("element", element_arrow, arr.contains_null));
    Ok(Arc::new(
        ListArray::try_new(field, offsets, child_array, null_buf)
            .map_err(|e| Error::generic(format!("arrow bridge: failed to build list: {e}")))?,
    ))
}

fn map_to_array(
    col: &ColumnBuffers,
    m: &MapType,
    null_buf: Option<NullBuffer>,
) -> DeltaResult<ArrayRef> {
    if col.type_tag != ColumnTypeTag::Map as u8 {
        if col.type_tag == ColumnTypeTag::Null as u8 {
            // All-null map column: emit empty.
            let key_arrow = ArrowDataType::try_from_kernel(&m.key_type).map_err(|e| {
                Error::generic(format!("arrow bridge: failed to build map key type: {e}"))
            })?;
            let val_arrow = ArrowDataType::try_from_kernel(&m.value_type).map_err(|e| {
                Error::generic(format!("arrow bridge: failed to build map value type: {e}"))
            })?;
            let key_field = Arc::new(ArrowField::new("key", key_arrow, false));
            let val_field = Arc::new(ArrowField::new("value", val_arrow, m.value_contains_null));
            let entries_field = Arc::new(ArrowField::new(
                "key_value",
                ArrowDataType::Struct(vec![(*key_field).clone(), (*val_field).clone()].into()),
                false,
            ));
            let n = col.row_count;
            let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0i32; n + 1]));
            let empty_keys = empty_array_for(&m.key_type)?;
            let empty_vals = empty_array_for(&m.value_type)?;
            let entries = StructArray::try_new(
                vec![key_field.clone(), val_field.clone()].into(),
                vec![empty_keys, empty_vals],
                None,
            )
            .map_err(|e| Error::generic(format!("arrow bridge: empty map struct: {e}")))?;
            return Ok(Arc::new(
                MapArray::try_new(entries_field, offsets, entries, null_buf, false)
                    .map_err(|e| Error::generic(format!("arrow bridge: empty map: {e}")))?,
            ));
        }
        return Err(Error::generic(format!(
            "arrow bridge: expected Map tag for map type, got tag={}",
            col.type_tag
        )));
    }
    let n = col.row_count;
    let offset_bytes = primary_slice(col, (n + 1).checked_mul(4).ok_or_else(overflow)?)?;
    let offsets_buf = Buffer::from(offset_bytes);
    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::new(offsets_buf, 0, n + 1));
    if col.children_len != 2 || col.children.is_null() {
        return Err(Error::generic(
            "arrow bridge: map column must have exactly two children",
        ));
    }
    // SAFETY: engine guarantees children buffer valid for children_len entries.
    let children_slice = unsafe { std::slice::from_raw_parts(col.children, 2) };
    let key_array = column_buffers_to_array(&children_slice[0], &m.key_type)?;
    let val_array = column_buffers_to_array(&children_slice[1], &m.value_type)?;

    let key_arrow = ArrowDataType::try_from_kernel(&m.key_type)
        .map_err(|e| Error::generic(format!("arrow bridge: failed to build map key type: {e}")))?;
    let val_arrow = ArrowDataType::try_from_kernel(&m.value_type).map_err(|e| {
        Error::generic(format!("arrow bridge: failed to build map value type: {e}"))
    })?;
    let key_field = Arc::new(ArrowField::new("key", key_arrow, false));
    let val_field = Arc::new(ArrowField::new("value", val_arrow, m.value_contains_null));
    let entries_field = Arc::new(ArrowField::new(
        "key_value",
        ArrowDataType::Struct(vec![(*key_field).clone(), (*val_field).clone()].into()),
        false,
    ));
    let entries = StructArray::try_new(
        vec![key_field.clone(), val_field.clone()].into(),
        vec![key_array, val_array],
        None,
    )
    .map_err(|e| Error::generic(format!("arrow bridge: failed to build map entries: {e}")))?;
    Ok(Arc::new(
        MapArray::try_new(entries_field, offsets, entries, null_buf, false)
            .map_err(|e| Error::generic(format!("arrow bridge: failed to build map: {e}")))?,
    ))
}

fn string_array(col: &ColumnBuffers, null_buf: Option<NullBuffer>) -> DeltaResult<ArrayRef> {
    let n = col.row_count;
    let offset_bytes = primary_slice(col, (n + 1).checked_mul(4).ok_or_else(overflow)?)?;
    let offsets_buf = Buffer::from(offset_bytes);
    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::new(offsets_buf, 0, n + 1));
    let value_bytes = if col.aux.is_null() || col.aux_len == 0 {
        Buffer::from(Vec::<u8>::new())
    } else {
        // SAFETY: engine guarantees aux buffer valid for aux_len bytes.
        let raw = unsafe { std::slice::from_raw_parts(col.aux, col.aux_len) };
        Buffer::from(raw)
    };
    Ok(Arc::new(
        StringArray::try_new(offsets, value_bytes, null_buf)
            .map_err(|e| Error::generic(format!("arrow bridge: failed to build string: {e}")))?,
    ))
}

fn all_null_primitive_array(p: &PrimitiveType, n: usize) -> DeltaResult<ArrayRef> {
    let nulls = NullBuffer::new_null(n);
    let arr: ArrayRef = match p {
        PrimitiveType::Boolean => Arc::new(BooleanArray::new(
            BooleanBuffer::new(Buffer::from(vec![0u8; n.div_ceil(8)]), 0, n),
            Some(nulls),
        )),
        PrimitiveType::Byte => Arc::new(Int8Array::new(
            ScalarBuffer::<i8>::from(vec![0i8; n]),
            Some(nulls),
        )),
        PrimitiveType::Short => Arc::new(Int16Array::new(
            ScalarBuffer::<i16>::from(vec![0i16; n]),
            Some(nulls),
        )),
        PrimitiveType::Integer => Arc::new(Int32Array::new(
            ScalarBuffer::<i32>::from(vec![0i32; n]),
            Some(nulls),
        )),
        PrimitiveType::Long => Arc::new(Int64Array::new(
            ScalarBuffer::<i64>::from(vec![0i64; n]),
            Some(nulls),
        )),
        PrimitiveType::Float => Arc::new(Float32Array::new(
            ScalarBuffer::<f32>::from(vec![0f32; n]),
            Some(nulls),
        )),
        PrimitiveType::Double => Arc::new(Float64Array::new(
            ScalarBuffer::<f64>::from(vec![0f64; n]),
            Some(nulls),
        )),
        PrimitiveType::Date => Arc::new(Date32Array::new(
            ScalarBuffer::<i32>::from(vec![0i32; n]),
            Some(nulls),
        )),
        PrimitiveType::Timestamp => Arc::new(
            TimestampMicrosecondArray::new(ScalarBuffer::<i64>::from(vec![0i64; n]), Some(nulls))
                .with_timezone("UTC"),
        ),
        PrimitiveType::TimestampNtz => Arc::new(TimestampMicrosecondArray::new(
            ScalarBuffer::<i64>::from(vec![0i64; n]),
            Some(nulls),
        )),
        PrimitiveType::String => {
            let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0i32; n + 1]));
            Arc::new(
                StringArray::try_new(offsets, Buffer::from(Vec::<u8>::new()), Some(nulls))
                    .map_err(|e| Error::generic(format!("arrow bridge: null-string array: {e}")))?,
            )
        }
        PrimitiveType::Binary => {
            let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0i32; n + 1]));
            Arc::new(
                BinaryArray::try_new(offsets, Buffer::from(Vec::<u8>::new()), Some(nulls))
                    .map_err(|e| Error::generic(format!("arrow bridge: null-binary array: {e}")))?,
            )
        }
        other => {
            return Err(Error::generic(format!(
                "arrow bridge: all-null array for primitive {:?} not implemented",
                other
            )))
        }
    };
    Ok(arr)
}

fn empty_array_for(dtype: &DataType) -> DeltaResult<ArrayRef> {
    match dtype {
        DataType::Primitive(p) => all_null_primitive_array(p, 0),
        other => Err(Error::generic(format!(
            "arrow bridge: empty array for type {:?} not implemented",
            other
        ))),
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn read_null_buffer(col: &ColumnBuffers) -> DeltaResult<Option<NullBuffer>> {
    if col.null_bitmap.is_null() || col.null_bitmap_len == 0 {
        return Ok(None);
    }
    // SAFETY: engine guarantees bitmap valid for null_bitmap_len bytes.
    let raw = unsafe { std::slice::from_raw_parts(col.null_bitmap, col.null_bitmap_len) };
    let buf = Buffer::from(raw);
    Ok(Some(NullBuffer::new(BooleanBuffer::new(
        buf,
        0,
        col.row_count,
    ))))
}

fn primary_slice(col: &ColumnBuffers, expected_len: usize) -> DeltaResult<&[u8]> {
    if col.primary.is_null() {
        if expected_len == 0 {
            return Ok(&[]);
        }
        return Err(Error::generic(
            "arrow bridge: column has null primary but non-empty payload expected",
        ));
    }
    if col.primary_len < expected_len {
        return Err(Error::generic(format!(
            "arrow bridge: primary buffer too short: have {}, need {}",
            col.primary_len, expected_len
        )));
    }
    // SAFETY: engine guarantees primary buffer valid for primary_len bytes.
    Ok(unsafe { std::slice::from_raw_parts(col.primary, expected_len) })
}

fn require_tag(col: &ColumnBuffers, expected: ColumnTypeTag, label: &str) -> DeltaResult<()> {
    if col.type_tag != expected as u8 {
        return Err(Error::generic(format!(
            "arrow bridge: expected {} (tag={}) but got tag={}",
            label, expected as u8, col.type_tag
        )));
    }
    Ok(())
}

fn overflow() -> Error {
    Error::generic("arrow bridge: row count overflows usize")
}
