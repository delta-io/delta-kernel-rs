//! The `KernelSchemaVisitor` defines a visitor system to allow engines to build kernel-native
//! representations of schemas for projection pushdown during scans.
//!
//! Building a schema requires creating elements in dependency order. Referenced elements must be
//! constructed before the elements that reference them. In other words, children must be created
//! before parents.
//!
//! The model is ID based. When the engine wants to create a schema element (a [`StructField`] in
//! kernel terms) it calls the appropriate visitor function which constructs the analogous kernel
//! schema field and returns an `id` (`usize`) that identifies the field. That ID can be passed to
//! other visitor functions to reference that element when building complex types.
//!
//! The final schema is built by visiting a struct field combining the field IDs of the top-level
//! fields.
//!
//! Note: Schemas are structs but can also contain struct fields. Use `visit_field_struct` for both
//! the root schema and for named struct fields. The name of the root struct is ignored and can be
//! anything.
//!
//! IDs are consumed when used. Each element takes ownership of its referenced child
//! elements. Trying to pass an ID more than once to a complex field visitor will result in an
//! error.

use std::collections::HashMap;
use std::ffi::c_void;

use delta_kernel::schema::{
    ArrayType, DataType, DecimalType, MapType, MetadataValue, PrimitiveType, StructField,
    StructType,
};
use delta_kernel::{DeltaResult, Error};
use tracing::warn;

use crate::scan::{CMetadataMap, CMetadataValueKind};
use crate::{
    AllocateErrorFn, ExternResult, IntoExternResult, KernelStringSlice, ReferenceSet,
    TryFromStringSlice,
};

#[derive(Default)]
pub struct KernelSchemaVisitorState {
    elements: ReferenceSet<StructField>,
}

/// An engine-owned metadata object and the callback that visits its values.
///
/// Field visitors may omit this descriptor when a field has no metadata. When provided, they
/// borrow it for the call and invoke `visitor` synchronously with a fresh Kernel-owned state. Its
/// opaque `metadata` must satisfy the callback's validity requirements until the callback returns.
/// The callback must not retain the state or unwind across the C ABI. Returning `false` rejects
/// the field.
#[repr(C)]
pub struct EngineMetadata {
    /// Opaque engine-owned metadata representation, borrowed for the callback duration.
    /// The callback may mutate the engine-owned metadata context; Kernel only forwards the
    /// pointer.
    pub metadata: *mut c_void,
    /// Visits metadata values into the provided state and reports whether the visit succeeded.
    pub visitor: extern "C" fn(metadata: *mut c_void, state: &mut CMetadataMap) -> bool,
}

/// Visit a metadata value encoded as UTF-8 text and a [`CMetadataValueKind`].
///
/// Numbers are signed decimal `i64` values, strings are passed through without JSON decoding, and
/// Booleans must be `true` or `false`. JSON integers in the `i64` range, strings, and Booleans
/// become typed metadata values; other JSON values remain opaque JSON. Prefer the matching typed
/// kind when available; use `MetadataJson` for values without a typed variant.
///
/// Returns `Ok(true)` after insertion. Invalid UTF-8, numeric text, or JSON returns an allocated
/// `Utf8Error`, `ParseIntError`, or `MalformedJsonError`, respectively. Invalid Boolean text and
/// duplicate keys return `SchemaError`. Errors leave the state unchanged.
///
/// # Safety
///
/// Caller must pass the active map supplied to `EngineMetadata::visitor`, valid key and value data
/// for the call duration, a valid `kind` discriminant, and a valid error allocator. The state must
/// not be aliased or retained beyond the callback.
#[no_mangle]
pub unsafe extern "C" fn visit_metadata_value(
    state: &mut CMetadataMap,
    key: KernelStringSlice,
    kind: CMetadataValueKind,
    value: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    let key = unsafe { TryFromStringSlice::try_from_slice(&key) };
    let value = unsafe { TryFromStringSlice::try_from_slice(&value) };
    visit_metadata_value_impl(state, key, kind, value)
        .map(|()| true)
        .into_extern_result(&allocate_error)
}

fn visit_engine_metadata(
    engine_metadata: Option<&EngineMetadata>,
) -> DeltaResult<HashMap<String, MetadataValue>> {
    let Some(engine_metadata) = engine_metadata else {
        return Ok(HashMap::new());
    };

    let mut state = CMetadataMap::default();
    if !(engine_metadata.visitor)(engine_metadata.metadata, &mut state) {
        return Err(Error::schema("Engine metadata visitor failed"));
    }

    Ok(state.into_values())
}

fn visit_metadata_value_impl(
    state: &mut CMetadataMap,
    key: DeltaResult<&str>,
    kind: CMetadataValueKind,
    value: DeltaResult<&str>,
) -> DeltaResult<()> {
    let key = key?;
    let value = value?;
    let value = match kind {
        CMetadataValueKind::MetadataNumber => MetadataValue::Number(value.parse()?),
        CMetadataValueKind::MetadataString => MetadataValue::String(value.to_owned()),
        CMetadataValueKind::MetadataBoolean => {
            MetadataValue::Boolean(value.parse().map_err(|_| {
                Error::schema("Invalid Boolean metadata value: expected true or false")
            })?)
        }
        CMetadataValueKind::MetadataJson => serde_json::from_str(value)?,
    };
    state.insert(key.to_owned(), value)
}

/// Extract the final schema from the visitor state.
///
/// This validates that the schema was properly constructed by ensuring:
/// 1. The schema_id points to a DataType::Struct (the root schema)
/// 2. No other elements remain in the state (all field IDs are consumed)
pub fn extract_kernel_schema(
    state: &mut KernelSchemaVisitorState,
    schema_id: usize,
) -> DeltaResult<StructType> {
    let schema_element = state
        .elements
        .take(schema_id)
        .ok_or_else(|| Error::schema("Nonexistent id passed to extract_kernel_schema"))?;
    let DataType::Struct(struct_type) = schema_element.data_type else {
        warn!("Final returned id was not a struct, schema is invalid");
        return Err(Error::schema(
            "Final returned id was not a struct, schema is invalid",
        ));
    };
    if !state.elements.is_empty() {
        warn!("Didn't consume all visited fields, schema is invalid.");
        Err(Error::schema(
            "Didn't consume all visited fields, schema is invalid.",
        ))
    } else {
        Ok(*struct_type)
    }
}

fn wrap_field(state: &mut KernelSchemaVisitorState, field: StructField) -> usize {
    state.elements.insert(field)
}

fn unwrap_field(state: &mut KernelSchemaVisitorState, field_id: usize) -> Option<StructField> {
    state.elements.take(field_id)
}

// =============================================================================
// FFI Visitor Functions for field creation - Primitive Types
// =============================================================================

/// Generic helper to create primitive fields
fn visit_field_primitive_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    primitive_type: PrimitiveType,
    nullable: bool,
    metadata: DeltaResult<HashMap<String, MetadataValue>>,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let metadata = metadata?;
    let field = StructField::new(name_str, DataType::Primitive(primitive_type), nullable)
        .with_metadata(metadata);
    Ok(wrap_field(state, field))
}

// TODO: turn all the primitive visitors below into a macro once cbindgen can run on macro expanded
// code
/// Visit a string field. Strings can hold arbitrary UTF-8 text data.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_string(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::String, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a long field. Long fields store 64-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_long(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Long, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit an integer field. Integer fields store 32-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_integer(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Integer, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a short field. Short fields store 16-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_short(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Short, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a byte field. Byte fields store 8-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_byte(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Byte, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a float field. Float fields store 32-bit floating point numbers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_float(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Float, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a double field. Double fields store 64-bit floating point numbers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_double(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Double, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a boolean field. Boolean fields store true/false values.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_boolean(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Boolean, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a binary field. Binary fields store arbitrary byte arrays.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_binary(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Binary, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a date field. Date fields store calendar dates without time information.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_date(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Date, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a timestamp field. Timestamp fields store date and time with microsecond precision in UTC.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_timestamp(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(
        state,
        name_str,
        PrimitiveType::Timestamp,
        nullable,
        metadata,
    )
    .into_extern_result(&allocate_error)
}

/// Visit a timestamp_ntz field. Similar to timestamp but without timezone information.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_timestamp_ntz(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(
        state,
        name_str,
        PrimitiveType::TimestampNtz,
        nullable,
        metadata,
    )
    .into_extern_result(&allocate_error)
}

/// Visit an interval year-month field. Values store signed month counts.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_interval_year_month(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(
        state,
        name_str,
        PrimitiveType::IntervalYearMonth,
        nullable,
        metadata,
    )
    .into_extern_result(&allocate_error)
}

/// Visit an interval day-time field. Values store signed microsecond durations.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_interval_day_time(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(
        state,
        name_str,
        PrimitiveType::IntervalDayTime,
        nullable,
        metadata,
    )
    .into_extern_result(&allocate_error)
}

/// Visit a void field. Void fields are not materialized in data files and read as all-null columns.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_void(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_primitive_impl(state, name_str, PrimitiveType::Void, nullable, metadata)
        .into_extern_result(&allocate_error)
}

/// Visit a decimal field. Decimal fields store fixed-precision decimal numbers with specified
/// precision and scale.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer. When non-null, `metadata` must point to a valid
/// descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_decimal(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    precision: u8,
    scale: u8,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_decimal_impl(state, name_str, precision, scale, nullable, metadata)
        .into_extern_result(&allocate_error)
}

fn visit_field_decimal_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    precision: u8,
    scale: u8,
    nullable: bool,
    metadata: DeltaResult<HashMap<String, MetadataValue>>,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let metadata = metadata?;

    let decimal_type = DecimalType::try_new(precision, scale)?;
    let field = StructField::new(
        name_str,
        DataType::Primitive(PrimitiveType::Decimal(decimal_type)),
        nullable,
    )
    .with_metadata(metadata);
    Ok(wrap_field(state, field))
}

// =============================================================================
// FFI Visitor Functions for field creation - Complex Types
// =============================================================================

/// Visit a struct field. Struct fields contain nested fields organized as ordered key-value pairs.
///
/// Note: This creates a named struct field (e.g. `address: struct<street, city>`). This function
/// should _also_ be used to create the final schema element, where the field IDs of the top-level
/// fields should be passed as `field_ids`. The name for the final schema element is ignored.
///
/// The `field_ids` array must contain IDs from previous `visit_field_*` field creation calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `field_ids` array pointing
/// to valid field IDs previously returned by this visitor, and `allocate_error` function pointer.
/// When non-null, `metadata` must point to a valid descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_struct(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    field_ids: *const usize,
    field_count: usize,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str: Result<&str, Error> = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    let field_ids = unsafe { std::slice::from_raw_parts(field_ids, field_count) };

    visit_field_struct_impl(state, name_str, field_ids, nullable, metadata)
        .into_extern_result(&allocate_error)
}

// Helper to create struct DataType from field IDs
fn create_struct_data_type(
    state: &mut KernelSchemaVisitorState,
    field_ids: &[usize],
) -> DeltaResult<DataType> {
    let field_vec = field_ids
        .iter()
        .map(|&field_id| {
            unwrap_field(state, field_id)
                .ok_or_else(|| Error::generic(format!("Invalid field ID {field_id} in struct")))
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    let struct_type = StructType::try_new(field_vec)?;
    Ok(DataType::from(struct_type))
}

fn visit_field_struct_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    field_ids: &[usize],
    nullable: bool,
    metadata: DeltaResult<HashMap<String, MetadataValue>>,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let metadata = metadata?;
    let data_type = create_struct_data_type(state, field_ids)?;
    let field = StructField::new(name_str, data_type, nullable).with_metadata(metadata);
    Ok(wrap_field(state, field))
}

/// Visit an array field. Array fields store ordered sequences of elements of the same type.
///
/// The `element_type_id` must reference a field created by a previous `visit_field_*`. Elements of
/// the array can be null if and only if the field referenced by `element_type_id` is nullable.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `element_type_id` from
/// a previous field visitor call, and `allocate_error` function pointer. When non-null, `metadata`
/// must point to a valid descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_array(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    element_type_id: usize,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_array_impl(state, name_str, element_type_id, nullable, metadata)
        .into_extern_result(&allocate_error)
}

fn visit_field_array_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    element_type_id: usize,
    nullable: bool,
    metadata: DeltaResult<HashMap<String, MetadataValue>>,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let metadata = metadata?;
    let element_field = unwrap_field(state, element_type_id).ok_or_else(|| {
        Error::generic(format!(
            "Invalid element type ID {element_type_id} for array"
        ))
    })?;

    let array_type = ArrayType::new(element_field.data_type, element_field.nullable);
    let field = StructField::new(name_str, array_type, nullable).with_metadata(metadata);
    Ok(wrap_field(state, field))
}

/// Visit a map field. Map fields store key-value pairs where all keys have the same type and all
/// values have the same type.
///
/// Both `key_type_id` and `value_type_id` must reference fields created by previous `visit_field_*`
/// calls. The map can contain null values if and only if the field referenced by `value_type_id` is
/// nullable.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `key_type_id` and
/// `value_type_id` from previous field visitor calls, and `allocate_error` function pointer. When
/// non-null, `metadata` must point to a valid descriptor and callback.
#[no_mangle]
pub unsafe extern "C" fn visit_field_map(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    key_type_id: usize,
    value_type_id: usize,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_map_impl(
        state,
        name_str,
        key_type_id,
        value_type_id,
        nullable,
        metadata,
    )
    .into_extern_result(&allocate_error)
}

fn visit_field_map_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    key_type_id: usize,
    value_type_id: usize,
    nullable: bool,
    metadata: DeltaResult<HashMap<String, MetadataValue>>,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let metadata = metadata?;

    let key_field = unwrap_field(state, key_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid key type ID {key_type_id} for map")))?;

    if key_field.nullable {
        return Err(Error::generic("Delta Map keys may not be nullable"));
    }

    let value_field = unwrap_field(state, value_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid value type ID {value_type_id} for map")))?;

    let map_type = MapType::new(
        key_field.data_type,
        value_field.data_type,
        value_field.nullable,
    );
    let field = StructField::new(name_str, map_type, nullable).with_metadata(metadata);
    Ok(wrap_field(state, field))
}

/// Visit a variant field.
///
/// Takes a struct type ID that defines the variant schema. This must reference a field created by
/// previous `visit_field_struct` call.
///
/// # Safety
///
/// Caller must ensure:
/// - All base parameters are valid as per visit_field_string
/// - `variant_struct_id` is a valid struct type ID from a previous visitor call
#[no_mangle]
pub unsafe extern "C" fn visit_field_variant(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    variant_struct_id: usize,
    nullable: bool,
    metadata: *const EngineMetadata,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let metadata = visit_engine_metadata(unsafe { metadata.as_ref() });
    visit_field_variant_impl(state, name_str, variant_struct_id, nullable, metadata)
        .into_extern_result(&allocate_error)
}

fn visit_field_variant_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    variant_struct_id: usize,
    nullable: bool,
    metadata: DeltaResult<HashMap<String, MetadataValue>>,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let metadata = metadata?;
    let data_type = create_variant_data_type(state, variant_struct_id)?;
    let field = StructField::new(name_str, data_type, nullable).with_metadata(metadata);
    Ok(wrap_field(state, field))
}

// Helper to create variant DataType
fn create_variant_data_type(
    state: &mut KernelSchemaVisitorState,
    struct_type_id: usize,
) -> DeltaResult<DataType> {
    let Some(DataType::Struct(variant_struct)) =
        state.elements.take(struct_type_id).map(|f| f.data_type)
    else {
        return Err(Error::generic(format!(
            "Invalid variant struct ID {struct_type_id} - must be DataType::Struct"
        )));
    };
    Ok(DataType::Variant(variant_struct))
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::ptr::{null, NonNull};

    use delta_kernel::schema::{DataType, MetadataValue, PrimitiveType};
    use rstest::rstest;

    use super::*;
    use crate::error::{EngineError, KernelError};
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_with_message, ok_or_panic,
    };
    use crate::scan::visit_metadata_map;
    use crate::{KernelStringSlice, NullableCvoid};

    #[derive(Default)]
    struct TestMetadata {
        visited: bool,
        values: HashMap<String, MetadataValue>,
    }

    impl TestMetadata {
        fn from<K>(values: impl IntoIterator<Item = (K, MetadataValue)>) -> Self
        where
            K: Into<String>,
        {
            Self {
                visited: false,
                values: values
                    .into_iter()
                    .map(|(key, value)| (key.into(), value))
                    .collect(),
            }
        }
    }

    extern "C" fn visit_all_metadata(metadata: *mut c_void, state: &mut CMetadataMap) -> bool {
        let metadata = unsafe { &mut *metadata.cast::<TestMetadata>() };
        metadata.visited = true;

        for (key, value) in &metadata.values {
            let encoded = value.to_string();
            unsafe {
                ok_or_panic(visit_metadata_value(
                    state,
                    KernelStringSlice::new_unsafe(key),
                    CMetadataValueKind::from(value),
                    KernelStringSlice::new_unsafe(&encoded),
                    allocate_err,
                ));
            }
        }
        true
    }

    extern "C" fn reject_metadata(_metadata: *mut c_void, _state: &mut CMetadataMap) -> bool {
        false
    }

    fn rejected_engine_metadata() -> EngineMetadata {
        EngineMetadata {
            metadata: std::ptr::null_mut(),
            visitor: reject_metadata,
        }
    }

    fn test_engine_metadata(metadata: &mut TestMetadata) -> EngineMetadata {
        EngineMetadata {
            metadata: std::ptr::from_mut(metadata).cast(),
            visitor: visit_all_metadata,
        }
    }

    #[test]
    fn rejected_metadata_does_not_insert_field() {
        let mut state = KernelSchemaVisitorState::default();
        let result = unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("field"),
                false,
                &EngineMetadata {
                    metadata: std::ptr::null_mut(),
                    visitor: reject_metadata,
                },
                allocate_err,
            )
        };

        assert_extern_result_error_with_message(result, KernelError::SchemaError, None);
        assert!(state.elements.is_empty());
    }

    #[test]
    fn missing_engine_metadata_creates_field_with_empty_metadata() {
        let mut state = KernelSchemaVisitorState::default();
        let field_id = unsafe {
            ok_or_panic(visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("field"),
                false,
                null(),
                allocate_err,
            ))
        };
        let field = unwrap_field(&mut state, field_id).unwrap();

        assert!(field.metadata().is_empty());
    }

    #[rstest]
    #[case(
        CMetadataValueKind::MetadataNumber,
        "9223372036854775808",
        KernelError::ParseIntError
    )]
    #[case(
        CMetadataValueKind::MetadataNumber,
        "-9223372036854775809",
        KernelError::ParseIntError
    )]
    #[case(CMetadataValueKind::MetadataNumber, "1.5", KernelError::ParseIntError)]
    #[case(CMetadataValueKind::MetadataNumber, "", KernelError::ParseIntError)]
    #[case(
        CMetadataValueKind::MetadataNumber,
        "not-a-number",
        KernelError::ParseIntError
    )]
    #[case(CMetadataValueKind::MetadataBoolean, "TRUE", KernelError::SchemaError)]
    #[case(CMetadataValueKind::MetadataBoolean, "1", KernelError::SchemaError)]
    #[case(
        CMetadataValueKind::MetadataBoolean,
        "false ",
        KernelError::SchemaError
    )]
    #[case(CMetadataValueKind::MetadataBoolean, "", KernelError::SchemaError)]
    #[case(
        CMetadataValueKind::MetadataJson,
        "not-json",
        KernelError::MalformedJsonError
    )]
    fn metadata_value_rejects_invalid_text_without_inserting_value(
        #[case] kind: CMetadataValueKind,
        #[case] value: &str,
        #[case] expected_error: KernelError,
    ) {
        let mut state = CMetadataMap::default();
        let result = unsafe {
            visit_metadata_value(
                &mut state,
                KernelStringSlice::new_unsafe("key"),
                kind,
                KernelStringSlice::new_unsafe(value),
                allocate_err,
            )
        };

        assert_extern_result_error_with_message(result, expected_error, None);
        assert!(state.into_values().is_empty());
    }

    #[rstest]
    fn metadata_value_rejects_invalid_utf8_without_inserting_value(
        #[values(
            CMetadataValueKind::MetadataNumber,
            CMetadataValueKind::MetadataString,
            CMetadataValueKind::MetadataBoolean,
            CMetadataValueKind::MetadataJson
        )]
        kind: CMetadataValueKind,
        #[values(false, true)] invalid_key: bool,
    ) {
        let invalid = [0xff_u8];
        let invalid_slice = KernelStringSlice {
            ptr: invalid.as_ptr().cast(),
            len: invalid.len(),
        };
        let mut state = CMetadataMap::default();
        let result = unsafe {
            let valid_slice = KernelStringSlice::new_unsafe("1");
            let (key, value) = if invalid_key {
                (invalid_slice, valid_slice)
            } else {
                (valid_slice, invalid_slice)
            };
            visit_metadata_value(&mut state, key, kind, value, allocate_err)
        };

        assert_extern_result_error_with_message(result, KernelError::Utf8Error, None);
        assert!(state.into_values().is_empty());
    }

    #[rstest]
    #[case(CMetadataValueKind::MetadataNumber, "2")]
    #[case(CMetadataValueKind::MetadataString, "replacement")]
    #[case(CMetadataValueKind::MetadataBoolean, "true")]
    #[case(CMetadataValueKind::MetadataJson, "{}")]
    fn duplicate_metadata_key_is_rejected_without_replacing_value(
        #[case] kind: CMetadataValueKind,
        #[case] value: &str,
    ) {
        let mut state = CMetadataMap::default();
        unsafe {
            ok_or_panic(visit_metadata_value(
                &mut state,
                KernelStringSlice::new_unsafe("key"),
                CMetadataValueKind::MetadataNumber,
                KernelStringSlice::new_unsafe("1"),
                allocate_err,
            ));
        }

        let result = unsafe {
            visit_metadata_value(
                &mut state,
                KernelStringSlice::new_unsafe("key"),
                kind,
                KernelStringSlice::new_unsafe(value),
                allocate_err,
            )
        };

        assert_extern_result_error_with_message(result, KernelError::SchemaError, None);
        assert_eq!(
            state.into_values().get("key"),
            Some(&MetadataValue::Number(1))
        );
    }

    #[rstest]
    #[case(CMetadataValueKind::MetadataNumber, "17", MetadataValue::Number(17))]
    #[case(CMetadataValueKind::MetadataString, "value", MetadataValue::String("value".to_string()))]
    #[case(
        CMetadataValueKind::MetadataBoolean,
        "true",
        MetadataValue::Boolean(true)
    )]
    #[case(CMetadataValueKind::MetadataJson, "17", MetadataValue::Number(17))]
    #[case(CMetadataValueKind::MetadataJson, r#""value""#, MetadataValue::String("value".to_string()))]
    #[case(CMetadataValueKind::MetadataJson, "true", MetadataValue::Boolean(true))]
    #[case(CMetadataValueKind::MetadataJson, r#"{"nested":1}"#, MetadataValue::Other(serde_json::json!({"nested": 1})))]
    fn metadata_value_decodes_text_and_normalizes_json_scalars(
        #[case] kind: CMetadataValueKind,
        #[case] value: &str,
        #[case] expected: MetadataValue,
    ) {
        let mut state = CMetadataMap::default();
        unsafe {
            ok_or_panic(visit_metadata_value(
                &mut state,
                KernelStringSlice::new_unsafe("key"),
                kind,
                KernelStringSlice::new_unsafe(value),
                allocate_err,
            ));
        }
        assert_eq!(state.into_values().get("key"), Some(&expected));
    }

    extern "C" fn copy_metadata_entry(
        context: NullableCvoid,
        key: KernelStringSlice,
        kind: CMetadataValueKind,
        value: KernelStringSlice,
    ) {
        let mut state = context.unwrap().cast::<CMetadataMap>();
        unsafe {
            ok_or_panic(visit_metadata_value(
                state.as_mut(),
                key,
                kind,
                value,
                allocate_err,
            ));
        }
    }

    extern "C" fn visit_metadata_from_map(metadata: *mut c_void, state: &mut CMetadataMap) -> bool {
        let source = unsafe { &*metadata.cast::<CMetadataMap>() };
        unsafe {
            visit_metadata_map(
                source,
                Some(NonNull::from(state).cast()),
                copy_metadata_entry,
            );
        }
        true
    }

    #[test]
    fn outbound_metadata_round_trips_through_field_visitor() {
        let values = HashMap::from([
            ("number".to_string(), MetadataValue::Number(17)),
            (
                "string".to_string(),
                MetadataValue::String("true".to_string()),
            ),
            ("boolean".to_string(), MetadataValue::Boolean(true)),
            (
                "array".to_string(),
                MetadataValue::Other(serde_json::json!([1, true])),
            ),
            (
                "object".to_string(),
                MetadataValue::Other(serde_json::json!({"nested": 1})),
            ),
            (
                "float".to_string(),
                MetadataValue::Other(serde_json::json!(2.5)),
            ),
            (
                "null".to_string(),
                MetadataValue::Other(serde_json::Value::Null),
            ),
            (
                "large".to_string(),
                MetadataValue::Other(serde_json::json!(9_223_372_036_854_775_808_u64)),
            ),
            (
                "json_number".to_string(),
                MetadataValue::Other(serde_json::json!(17)),
            ),
            (
                "json_string".to_string(),
                MetadataValue::Other(serde_json::json!("value")),
            ),
            (
                "json_boolean".to_string(),
                MetadataValue::Other(serde_json::json!(true)),
            ),
        ]);
        let mut expected = values.clone();
        expected.insert("json_number".to_string(), MetadataValue::Number(17));
        expected.insert(
            "json_string".to_string(),
            MetadataValue::String("value".to_string()),
        );
        expected.insert("json_boolean".to_string(), MetadataValue::Boolean(true));
        let mut source = CMetadataMap::from(values.clone());
        let metadata = EngineMetadata {
            metadata: std::ptr::from_mut(&mut source).cast(),
            visitor: visit_metadata_from_map,
        };
        let mut state = KernelSchemaVisitorState::default();
        let field_id = unsafe {
            ok_or_panic(visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("field"),
                false,
                &metadata,
                allocate_err,
            ))
        };
        let field = unwrap_field(&mut state, field_id).unwrap();

        assert_eq!(field.metadata(), &expected);
        assert_eq!(source.into_values(), values);
    }

    macro_rules! visit_field {
        ($type:ident, $state:ident, $name:expr, $nullable:tt, $metadata:expr) => {{
            paste::paste! { ok_or_panic(unsafe {
                [<visit_field_ $type>](
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    $nullable,
                    $metadata,
                    allocate_err,
                )
            }) }
        }};

        ($type:ident, $state:ident, $name:expr, $arg1:expr, $nullable:tt, $metadata:expr) => {{
            paste::paste! { ok_or_panic(#[allow(unused_unsafe)] unsafe {
                let arg1 = $arg1;
                [<visit_field_ $type>](
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    arg1,
                    $nullable,
                    $metadata,
                    allocate_err,
                )
            }) }
        }};

        ($type:ident, $state:ident, $name:expr, $arg1:expr, $arg2:expr, $nullable:tt, $metadata:expr) => {{
            paste::paste! { ok_or_panic(#[allow(unused_unsafe)] unsafe {
                let arg1 = $arg1;
                let arg2 = $arg2;
                [<visit_field_ $type>](
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    arg1,
                    arg2,
                    $nullable,
                    $metadata,
                    allocate_err,
                )
            }) }
        }};
    }

    macro_rules! visit_array_field {
        ($state:ident, $name:expr, $nullable:tt, $elem_field:expr, $metadata:expr) => {{
            let ef = $elem_field;
            ok_or_panic(unsafe {
                visit_field_array(
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    ef,
                    $nullable,
                    $metadata,
                    allocate_err,
                )
            })
        }};
    }

    macro_rules! visit_map_field {
        ($state:ident, $name:expr, $nullable:tt, $key_field:expr, $val_field:expr, $metadata:expr) => {{
            let kf = $key_field;
            let vf = $val_field;
            ok_or_panic(unsafe {
                visit_field_map(
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    kf,
                    vf,
                    $nullable,
                    $metadata,
                    allocate_err,
                )
            })
        }};
    }

    macro_rules! visit_struct_field {
        ($state:ident, $name:expr, $nullable:tt, [$($fields:expr),* $(,)?], $metadata:expr) => {{
            let fields = vec![$($fields),*];
            let field_count = fields.len();
            ok_or_panic(unsafe {
                visit_field_struct(
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    fields.as_ptr(),
                    field_count,
                    $nullable,
                    $metadata,
                    allocate_err,
                )
            })
        }};
    }

    macro_rules! visit_variant_field {
        ($state:ident, $name:expr, $nullable:tt, $metadata:expr) => {{
            visit_field!(
                variant,
                $state,
                $name,
                visit_struct_field!(
                    $state,
                    "variant",
                    false,
                    [
                        visit_field!(binary, $state, "metadata", false, null()),
                        visit_field!(binary, $state, "value", false, null()),
                    ],
                    null()
                ),
                false,
                $metadata
            )
        }};
    }

    #[test]
    fn field_metadata_preserves_typed_values_and_callback_context() {
        let mut state = KernelSchemaVisitorState::default();
        let expected = HashMap::from([
            ("number".to_string(), MetadataValue::Number(17)),
            (
                "string".to_string(),
                MetadataValue::String("value".to_string()),
            ),
            ("boolean".to_string(), MetadataValue::Boolean(true)),
            (
                "json".to_string(),
                MetadataValue::Other(serde_json::json!({
                    "nested": [1, null, 2.5]
                })),
            ),
        ]);
        let mut metadata = TestMetadata::from(expected.clone());
        let field_id = visit_field!(
            string,
            state,
            "mapped",
            true,
            &test_engine_metadata(&mut metadata)
        );
        let field = unwrap_field(&mut state, field_id).unwrap();

        assert!(metadata.visited);
        assert_eq!(field.metadata(), &expected);
    }

    #[test]
    fn every_primitive_and_decimal_field_preserves_metadata() {
        macro_rules! assert_field_metadata {
            ($type:ident, $name:literal $(, $arg:expr)*) => {{
                let mut state = KernelSchemaVisitorState::default();
                let field_id = visit_field!(
                    $type,
                    state,
                    $name,
                    $($arg,)*
                    false,
                    &test_engine_metadata(&mut TestMetadata::from([(
                        "number",
                        MetadataValue::Number(17),
                    )]))
                );
                let field = unwrap_field(&mut state, field_id).unwrap();
                assert_eq!(
                    field.metadata().get("number"),
                    Some(&MetadataValue::Number(17)),
                    "{name} field metadata",
                    name = $name,
                );
            }};
        }

        assert_field_metadata!(string, "string");
        assert_field_metadata!(long, "long");
        assert_field_metadata!(integer, "integer");
        assert_field_metadata!(short, "short");
        assert_field_metadata!(byte, "byte");
        assert_field_metadata!(float, "float");
        assert_field_metadata!(double, "double");
        assert_field_metadata!(boolean, "boolean");
        assert_field_metadata!(binary, "binary");
        assert_field_metadata!(date, "date");
        assert_field_metadata!(timestamp, "timestamp");
        assert_field_metadata!(timestamp_ntz, "timestamp_ntz");
        assert_field_metadata!(interval_year_month, "interval_year_month");
        assert_field_metadata!(interval_day_time, "interval_day_time");
        assert_field_metadata!(void, "void");
        assert_field_metadata!(decimal, "decimal", 10, 2);
    }

    #[test]
    fn complex_fields_keep_parent_and_child_metadata_isolated() {
        let mut state = KernelSchemaVisitorState::default();

        let schema_id = visit_struct_field!(
            state,
            "schema",
            false,
            [
                visit_struct_field!(
                    state,
                    "parent",
                    false,
                    [visit_field!(
                        string,
                        state,
                        "child",
                        true,
                        &test_engine_metadata(&mut TestMetadata::from([(
                            "number",
                            MetadataValue::Number(1),
                        )]))
                    )],
                    &test_engine_metadata(&mut TestMetadata::from([(
                        "number",
                        MetadataValue::Number(2),
                    )]))
                ),
                visit_field!(
                    array,
                    state,
                    "array",
                    visit_field!(string, state, "element", true, null()),
                    true,
                    &test_engine_metadata(&mut TestMetadata::from([(
                        "number",
                        MetadataValue::Number(3),
                    )]))
                ),
                visit_field!(
                    map,
                    state,
                    "map",
                    visit_field!(string, state, "key", false, null()),
                    visit_field!(long, state, "value", true, null()),
                    true,
                    &test_engine_metadata(&mut TestMetadata::from([(
                        "number",
                        MetadataValue::Number(4),
                    )]))
                ),
                visit_field!(
                    variant,
                    state,
                    "variant",
                    visit_struct_field!(
                        state,
                        "variant_struct",
                        false,
                        [visit_field!(string, state, "value", true, null())],
                        null()
                    ),
                    true,
                    &test_engine_metadata(&mut TestMetadata::from([(
                        "number",
                        MetadataValue::Number(5),
                    )]))
                ),
            ],
            null()
        );

        let schema = extract_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();
        assert_eq!(fields.len(), 4);

        let DataType::Struct(children) = fields[0].data_type() else {
            panic!("expected struct")
        };
        let child = children.fields().next().unwrap();
        assert_eq!(
            child.metadata().get("number"),
            Some(&MetadataValue::Number(1))
        );

        let expected_fields = [("parent", 2), ("array", 3), ("map", 4), ("variant", 5)];
        for (field, (name, expected)) in fields.iter().zip(expected_fields) {
            assert_eq!(field.name(), name);
            assert_eq!(
                field.metadata().get("number"),
                Some(&MetadataValue::Number(expected))
            );
        }
    }

    #[test]
    fn rejected_complex_field_metadata_preserves_child_ids_for_retry() {
        let mut state = KernelSchemaVisitorState::default();

        let child = visit_field!(string, state, "child", true, null());
        let result = unsafe {
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("parent"),
                [child].as_ptr(),
                1,
                false,
                &rejected_engine_metadata(),
                allocate_err,
            )
        };
        assert_extern_result_error_with_message(result, KernelError::SchemaError, None);
        let parent = visit_struct_field!(state, "parent", false, [child], null());

        let element = visit_field!(string, state, "element", true, null());
        let result = unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("array"),
                element,
                false,
                &rejected_engine_metadata(),
                allocate_err,
            )
        };
        assert_extern_result_error_with_message(result, KernelError::SchemaError, None);
        let array = visit_array_field!(state, "array", false, element, null());

        let key = visit_field!(string, state, "key", false, null());
        let value = visit_field!(long, state, "value", true, null());
        let result = unsafe {
            visit_field_map(
                &mut state,
                KernelStringSlice::new_unsafe("map"),
                key,
                value,
                false,
                &rejected_engine_metadata(),
                allocate_err,
            )
        };
        assert_extern_result_error_with_message(result, KernelError::SchemaError, None);
        let map = visit_map_field!(state, "map", false, key, value, null());

        let child = visit_field!(binary, state, "value", false, null());
        let variant_struct = visit_struct_field!(state, "variant_struct", false, [child], null());
        let result = unsafe {
            visit_field_variant(
                &mut state,
                KernelStringSlice::new_unsafe("variant"),
                variant_struct,
                false,
                &rejected_engine_metadata(),
                allocate_err,
            )
        };
        assert_extern_result_error_with_message(result, KernelError::SchemaError, None);
        let variant = visit_field!(variant, state, "variant", variant_struct, false, null());

        let schema_id = visit_struct_field!(
            state,
            "schema",
            false,
            [parent, array, map, variant],
            null()
        );
        let schema = extract_kernel_schema(&mut state, schema_id).unwrap();
        assert_eq!(
            schema.fields().map(StructField::name).collect::<Vec<_>>(),
            ["parent", "array", "map", "variant"]
        );
    }

    fn assert_array(field: &StructField, element_type: DataType, contains_null: bool) {
        let DataType::Array(array_type) = field.data_type() else {
            panic!("Expected array type");
        };
        assert_eq!(
            array_type.element_type(),
            &element_type,
            "Mismatch on array element type"
        );
        assert_eq!(
            array_type.contains_null(),
            contains_null,
            "Mismatch on array element nullability"
        );
    }

    fn assert_map(
        field: &StructField,
        key_type: DataType,
        value_type: DataType,
        contains_null: bool,
    ) {
        let DataType::Map(map_type) = field.data_type() else {
            panic!("Expected map type");
        };
        assert_eq!(map_type.key_type(), &key_type, "Mismatch on map key type");
        assert_eq!(
            map_type.value_type(),
            &value_type,
            "Mismatch on map value type"
        );
        assert_eq!(
            map_type.value_contains_null(),
            contains_null,
            "Mismatch on map value nullability"
        );
    }

    fn assert_struct(field: &StructField, inner_type: DataType, inner_is_nullable: bool) {
        let DataType::Struct(struct_type) = field.data_type() else {
            panic!("Expected struct type");
        };
        let inner_fields: Vec<_> = struct_type.fields().collect();
        assert_eq!(inner_fields.len(), 1);
        assert_eq!(inner_fields[0].name(), "inner");
        assert_eq!(
            inner_fields[0].data_type(),
            &inner_type,
            "Mismatch on inner field type"
        );
        assert_eq!(inner_fields[0].is_nullable(), inner_is_nullable);
    }

    #[test]
    fn test_schema_all_types() {
        // Schema: struct<
        //   col_string: string,
        //   col_long: long,
        //   col_int: int,
        //   col_short: short,
        //   col_byte: byte,
        //   col_double: double,
        //   col_float: float,
        //   col_boolean: boolean,
        //   col_binary: binary,
        //   col_date: date,
        //   col_timestamp: timestamp,
        //   col_timestamp_ntz: timestamp_ntz,
        //   col_interval_year_month: interval year to month,
        //   col_interval_day_time: interval day to second,
        //   col_void: void,
        //   col_decimal: decimal(10,2),
        //   col_array: array<string>,
        //   col_map: map<string, long>,
        //   col_struct: struct<inner: string>,
        //   col_variant: variant<metadata: binary, value: binary>
        // >

        let mut state = KernelSchemaVisitorState::default();

        // Create all primitive fields
        let col_string = visit_field!(string, state, "col_string", false, null());
        let col_long = visit_field!(long, state, "col_long", false, null());
        let col_int = visit_field!(integer, state, "col_int", false, null());
        let col_short = visit_field!(short, state, "col_short", false, null());
        let col_byte = visit_field!(byte, state, "col_byte", false, null());
        let col_double = visit_field!(double, state, "col_double", false, null());
        let col_float = visit_field!(float, state, "col_float", false, null());
        let col_boolean = visit_field!(boolean, state, "col_boolean", false, null());
        let col_binary = visit_field!(binary, state, "col_binary", false, null());
        let col_date = visit_field!(date, state, "col_date", false, null());
        let col_timestamp = visit_field!(timestamp, state, "col_timestamp", false, null());
        let col_timestamp_ntz =
            visit_field!(timestamp_ntz, state, "col_timestamp_ntz", false, null());
        let col_interval_year_month = visit_field!(
            interval_year_month,
            state,
            "col_interval_year_month",
            false,
            null()
        );
        let col_interval_day_time = visit_field!(
            interval_day_time,
            state,
            "col_interval_day_time",
            false,
            null()
        );
        let col_void = visit_field!(void, state, "col_void", false, null());
        let col_decimal = visit_field!(decimal, state, "col_decimal", 10, 2, false, null());

        // Create array<string>
        let col_array = visit_array_field!(
            state,
            "col_array",
            false,
            visit_field!(string, state, "element", false, null()),
            null()
        );

        // Create map<string, long>
        let col_map = visit_map_field!(
            state,
            "col_map",
            false,
            visit_field!(string, state, "key", false, null()),
            visit_field!(long, state, "value", false, null()),
            null()
        );

        // Create struct<inner_name: string>
        let col_struct = visit_struct_field!(
            state,
            "col_struct",
            false,
            [visit_field!(string, state, "inner", false, null())],
            null()
        );

        // Create variant<metadata: binary, value: binary>
        let col_variant = visit_variant_field!(state, "col_variant", false, null());

        // Build the final schema
        let all_columns = [
            col_string,
            col_long,
            col_int,
            col_short,
            col_byte,
            col_double,
            col_float,
            col_boolean,
            col_binary,
            col_date,
            col_timestamp,
            col_timestamp_ntz,
            col_interval_year_month,
            col_interval_day_time,
            col_void,
            col_decimal,
            col_array,
            col_map,
            col_struct,
            col_variant,
        ];
        let schema_id = ok_or_panic(unsafe {
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("schema"),
                all_columns.as_ptr(),
                all_columns.len(),
                false,
                null(),
                allocate_err,
            )
        });

        // Verify the schema
        let schema = extract_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();
        assert_eq!(fields.len(), 20);

        // Validate the primitive fields
        let primitive_field_expectations = [
            ("col_string", PrimitiveType::String),
            ("col_long", PrimitiveType::Long),
            ("col_int", PrimitiveType::Integer),
            ("col_short", PrimitiveType::Short),
            ("col_byte", PrimitiveType::Byte),
            ("col_double", PrimitiveType::Double),
            ("col_float", PrimitiveType::Float),
            ("col_boolean", PrimitiveType::Boolean),
            ("col_binary", PrimitiveType::Binary),
            ("col_date", PrimitiveType::Date),
            ("col_timestamp", PrimitiveType::Timestamp),
            ("col_timestamp_ntz", PrimitiveType::TimestampNtz),
            ("col_interval_year_month", PrimitiveType::IntervalYearMonth),
            ("col_interval_day_time", PrimitiveType::IntervalDayTime),
            ("col_void", PrimitiveType::Void),
        ];

        for (index, (expected_name, expected_type)) in
            primitive_field_expectations.iter().enumerate()
        {
            assert_eq!(fields[index].name(), *expected_name);
            assert_eq!(
                fields[index].data_type(),
                &DataType::Primitive(expected_type.clone())
            );
            assert!(!fields[index].is_nullable());
        }

        assert_eq!(fields[15].name(), "col_decimal");
        let DataType::Primitive(PrimitiveType::Decimal(decimal_type)) = fields[15].data_type()
        else {
            panic!("Field col_decimal is not a decimal type");
        };
        assert_eq!(decimal_type.precision(), 10);
        assert_eq!(decimal_type.scale(), 2);

        assert_eq!(fields[16].name(), "col_array");
        assert_array(fields[16], DataType::STRING, false);

        assert_eq!(fields[17].name(), "col_map");
        assert_map(fields[17], DataType::STRING, DataType::LONG, false);

        assert_eq!(fields[18].name(), "col_struct");
        assert_struct(fields[18], DataType::STRING, false);

        assert_eq!(fields[19].name(), "col_variant");
        let DataType::Variant(variant_type) = fields[19].data_type() else {
            panic!("Expected variant type for col_variant");
        };
        let variant_fields: Vec<_> = variant_type.fields().collect();
        assert_eq!(variant_fields.len(), 2);
        assert_eq!(variant_fields[0].name(), "metadata");
        assert_eq!(
            variant_fields[0].data_type(),
            &DataType::Primitive(PrimitiveType::Binary)
        );
        assert_eq!(variant_fields[1].name(), "value");
        assert_eq!(
            variant_fields[1].data_type(),
            &DataType::Primitive(PrimitiveType::Binary)
        );
    }

    #[test]
    fn test_deeply_nested_structures() {
        let mut state = KernelSchemaVisitorState::default();

        // This creates a deeply nested structure that tests every type containing every other type:
        // - Arrays containing maps, structs, other arrays
        // - Maps with complex keys (struct, variant) and complex values
        // - Structs containing arrays, maps, variants, other structs
        // - Variants with proper metadata/value binary fields
        //
        // Structure with clear numbering (same level = a,b,c):
        // struct<
        //   col_nested: 1.array<2.map<2a.struct<key_id: long>, 2b.struct<
        //     inner_arrays: 3.array<4.struct<
        //       deep_maps: 4a.map<4a1.variant<metadata: binary, value: binary>,
        //                  4a2.array<decimal(10,2)>>,
        //       variant_data: 4b.variant<metadata: binary, value: binary>,
        //       nested_struct: 4c.struct<
        //         final_array: 5.array<6.map<6a.struct<coord: double>, 6b.double>>
        //       >
        //     >>
        //   >>>
        // >

        let schema_id = visit_struct_field!(
            state,
            "top_struct",
            false,
            [visit_array_field!(
                state, // nested field in struct is an array
                "col_nested",
                true,
                visit_map_field!(
                    state, // array element is a map
                    "element",
                    false,
                    visit_struct_field!(
                        state, // map key is a struct
                        "key",
                        false,
                        [visit_field!(long, state, "key_id", false, null())],
                        null()
                    ),
                    visit_struct_field!(
                        state, // map value is a struct
                        "value",
                        true,
                        [visit_array_field!(
                            state, // even more nested array
                            "inner_arrays",
                            false,
                            visit_struct_field!(
                                state, // inner array element is a struct
                                "element",
                                true,
                                [
                                    visit_map_field!(
                                        state, // struct field 1 is map
                                        "deep_maps",
                                        true,
                                        visit_variant_field!(
                                            state,
                                            "key", // key is variant
                                            false,
                                            null()
                                        ),
                                        visit_array_field!(
                                            state, // value is an array
                                            "value",
                                            false,
                                            visit_field!(
                                                decimal, // array element is decimal
                                                state,
                                                "element",
                                                10,
                                                2,
                                                true,
                                                null()
                                            ),
                                            null()
                                        ),
                                        null()
                                    ),
                                    visit_variant_field!(
                                        state, // struct field 2 is variant
                                        "variant_data",
                                        false,
                                        null()
                                    ),
                                    visit_struct_field!(
                                        state, // struct field 3 is nested_struct
                                        "nested_struct",
                                        true,
                                        [visit_array_field!(
                                            state,
                                            "final_array",
                                            false,
                                            visit_map_field!(
                                                state,
                                                "element",
                                                false,
                                                visit_struct_field!(
                                                    state,
                                                    "key",
                                                    false,
                                                    [visit_field!(
                                                        double,
                                                        state,
                                                        "coord",
                                                        false,
                                                        null()
                                                    )],
                                                    null()
                                                ),
                                                visit_field!(double, state, "value", false, null()),
                                                null()
                                            ),
                                            null()
                                        )],
                                        null()
                                    ),
                                ],
                                null()
                            ),
                            null()
                        )],
                        null()
                    ),
                    null()
                ),
                null()
            )],
            null()
        );

        let schema = extract_kernel_schema(&mut state, schema_id).unwrap();

        let root_fields: Vec<_> = schema.fields().collect();
        assert_eq!(root_fields.len(), 1);
        assert_eq!(root_fields[0].name(), "col_nested");
        assert!(root_fields[0].is_nullable());

        // 1: col_nested: array<...>
        let DataType::Array(level1_array) = root_fields[0].data_type() else {
            panic!("Expected array type for col_nested (level 1)");
        };
        assert!(!level1_array.contains_null());

        // 2: array element: map<struct<key_id: long>, ...>
        let DataType::Map(level2_map) = level1_array.element_type() else {
            panic!("Expected map type (level 2)");
        };
        assert!(level2_map.value_contains_null());

        // 2a: map key: struct<key_id: long>
        let DataType::Struct(level2a_key_struct) = level2_map.key_type() else {
            panic!("Expected struct type for map key (level 2a)");
        };
        let level2a_key_fields: Vec<_> = level2a_key_struct.fields().collect();
        assert_eq!(level2a_key_fields.len(), 1);
        assert_eq!(level2a_key_fields[0].name(), "key_id");
        assert_eq!(
            level2a_key_fields[0].data_type(),
            &DataType::Primitive(PrimitiveType::Long)
        );
        assert!(!level2a_key_fields[0].is_nullable());

        // 2b: map value: struct<inner_arrays: ...>
        let DataType::Struct(level2b_value_struct) = level2_map.value_type() else {
            panic!("Expected struct type for map value (level 2b)");
        };
        let level2b_value_fields: Vec<_> = level2b_value_struct.fields().collect();
        assert_eq!(level2b_value_fields.len(), 1);
        assert_eq!(level2b_value_fields[0].name(), "inner_arrays");
        assert!(!level2b_value_fields[0].is_nullable());

        // 3: inner_arrays: array<struct<...>>
        let DataType::Array(level3_array) = level2b_value_fields[0].data_type() else {
            panic!("Expected array type (level 3)");
        };
        assert!(level3_array.contains_null());

        // 4: array element: struct<deep_maps, variant_data, nested_struct>
        let DataType::Struct(level4_struct) = level3_array.element_type() else {
            panic!("Expected struct type (level 4)");
        };
        let level4_fields: Vec<_> = level4_struct.fields().collect();
        assert_eq!(level4_fields.len(), 3);
        assert_eq!(level4_fields[0].name(), "deep_maps");
        assert_eq!(level4_fields[1].name(), "variant_data");
        assert_eq!(level4_fields[2].name(), "nested_struct");

        // 4a: deep_maps: map<variant<metadata, value>, array<decimal>>
        assert!(level4_fields[0].is_nullable());
        let DataType::Map(level4a_map) = level4_fields[0].data_type() else {
            panic!("Expected map type (level 4a)");
        };
        assert!(!level4a_map.value_contains_null());

        // 4a1: map key: variant<metadata: binary, value: binary>
        let DataType::Variant(level4a1_key_variant) = level4a_map.key_type() else {
            panic!("Expected variant type for map key (level 4a1)");
        };
        let level4a1_key_fields: Vec<_> = level4a1_key_variant.fields().collect();
        assert_eq!(level4a1_key_fields.len(), 2);
        assert_eq!(level4a1_key_fields[0].name(), "metadata");
        assert_eq!(
            level4a1_key_fields[0].data_type(),
            &DataType::Primitive(PrimitiveType::Binary)
        );
        assert!(!level4a1_key_fields[0].is_nullable());
        assert_eq!(level4a1_key_fields[1].name(), "value");
        assert_eq!(
            level4a1_key_fields[1].data_type(),
            &DataType::Primitive(PrimitiveType::Binary)
        );
        assert!(!level4a1_key_fields[1].is_nullable());

        // 4a2: map value: array<decimal(10,2)>
        let DataType::Array(level4a2_array) = level4a_map.value_type() else {
            panic!("Expected array type (level 4a2)");
        };
        assert!(level4a2_array.contains_null());
        let DataType::Primitive(PrimitiveType::Decimal(decimal_type)) =
            level4a2_array.element_type()
        else {
            panic!("Expected decimal type in array (level 4a2)");
        };
        assert_eq!(decimal_type.precision(), 10);
        assert_eq!(decimal_type.scale(), 2);

        // 4b: variant_data: variant<metadata: binary, value: binary>
        assert!(!level4_fields[1].is_nullable());
        let DataType::Variant(level4b_variant) = level4_fields[1].data_type() else {
            panic!("Expected variant type (level 4b)");
        };
        let level4b_fields: Vec<_> = level4b_variant.fields().collect();
        assert_eq!(level4b_fields.len(), 2);
        assert_eq!(level4b_fields[0].name(), "metadata");
        assert_eq!(
            level4b_fields[0].data_type(),
            &DataType::Primitive(PrimitiveType::Binary)
        );
        assert!(!level4b_fields[0].is_nullable());
        assert_eq!(level4b_fields[1].name(), "value");
        assert_eq!(
            level4b_fields[1].data_type(),
            &DataType::Primitive(PrimitiveType::Binary)
        );
        assert!(!level4b_fields[1].is_nullable());

        // 4c: nested_struct: struct<final_array: ...>
        assert!(level4_fields[2].is_nullable());
        let DataType::Struct(level4c_struct) = level4_fields[2].data_type() else {
            panic!("Expected struct type (level 4c)");
        };
        let level4c_fields: Vec<_> = level4c_struct.fields().collect();
        assert_eq!(level4c_fields.len(), 1);
        assert_eq!(level4c_fields[0].name(), "final_array");
        assert!(!level4c_fields[0].is_nullable());

        // 5: final_array: array<...>
        let DataType::Array(level5_array) = level4c_fields[0].data_type() else {
            panic!("Expected array type (level 5)");
        };
        assert!(!level5_array.contains_null());

        // 6: array element: map<struct<coord: double>, double>
        let DataType::Map(level6_map) = level5_array.element_type() else {
            panic!("Expected map type (level 6)");
        };

        // 6b: map value: double
        assert_eq!(
            level6_map.value_type(),
            &DataType::Primitive(PrimitiveType::Double)
        );
        assert!(!level6_map.value_contains_null());

        // 6a: map key: struct<coord: double>
        let DataType::Struct(level6a_key_struct) = level6_map.key_type() else {
            panic!("Expected struct type for map key (level 6a)");
        };
        let level6a_key_fields: Vec<_> = level6a_key_struct.fields().collect();
        assert_eq!(level6a_key_fields.len(), 1);
        assert_eq!(level6a_key_fields[0].name(), "coord");
        assert_eq!(
            level6a_key_fields[0].data_type(),
            &DataType::Primitive(PrimitiveType::Double)
        );
        assert!(!level6a_key_fields[0].is_nullable());
    }

    #[test]
    fn test_nullability_combinations() {
        let mut state = KernelSchemaVisitorState::default();

        // Test more nullability cases:
        // Schema:
        // struct<
        //   col_required_string: string NOT NULL,
        //   col_nullable_string: string NULL,
        //   col_nullable_array_non_null_elements: array<string NOT NULL>,
        //   col_non_null_array_nullable_elements: array<string> NOT NULL,
        //   col_nullable_map_nullable_values: map<string, integer> ,
        //   col_non_null_map_non_null_values: map<string, integer NOT NULL> NOT NULL,
        //   col_nullable_struct: struct<inner: string> NULL,
        //   col_non_null_struct_nullable_field: struct<inner: string> NOT NULL
        // >

        // Required string field
        let col_required_string = visit_field!(string, state, "col_required_string", false, null());
        let col_nullable_string = visit_field!(string, state, "col_nullable_string", true, null());

        // Nullable array with non-null elements: array<string> NULL (elements NOT NULL)
        let col_nullable_array_non_null_elements = visit_array_field!(
            state,
            "col_nullable_array_non_null_elements",
            true, // array can be null
            visit_field!(
                string,
                state,
                "element",
                false, // elements cannot be null
                null()
            ),
            null()
        );

        // Non-null array with nullable elements: array<string> NOT NULL (elements NULL)
        let col_non_null_array_nullable_elements = visit_array_field!(
            state,
            "col_non_null_array_nullable_elements",
            false, // array not null
            visit_field!(
                string,
                state,
                "element",
                true, // elements can be null
                null()
            ),
            null()
        );

        // Nullable map with nullable values: map<string, integer> NULL (values NULL)
        let col_nullable_map_nullable_values = visit_map_field!(
            state,
            "col_nullable_map_nullable_values",
            true, // map can be null
            visit_field!(string, state, "key", false, null()),
            visit_field!(
                integer,
                state,
                "value",
                true, // values can be null
                null()
            ),
            null()
        );

        // Non-null map with non-null values: map<string, integer> NOT NULL (values NOT NULL)
        let col_non_null_map_non_null_values = visit_map_field!(
            state,
            "col_non_null_map_non_null_values",
            false, // map cannot be null
            visit_field!(string, state, "key", false, null()),
            visit_field!(
                integer,
                state,
                "value",
                false, // values cannot be null
                null()
            ),
            null()
        );

        let col_nullable_struct = visit_struct_field!(
            state,
            "col_nullable_struct",
            true, // struct is nullable
            [visit_field!(
                string,
                state,
                "inner",
                false, // inner is not nullable
                null()
            )],
            null()
        );

        // Non-null struct with nullable field: struct<inner: string NULL> NOT NULL
        let col_non_null_struct_nullable_field = visit_struct_field!(
            state,
            "col_non_null_struct_nullable_field",
            false, // struct not null
            [visit_field!(
                string,
                state,
                "inner",
                true, // inner is nullable
                null()
            )],
            null()
        );

        // Build final schema
        let schema_id = visit_struct_field!(
            state,
            "top_struct",
            false,
            [
                col_required_string,
                col_nullable_string,
                col_nullable_array_non_null_elements,
                col_non_null_array_nullable_elements,
                col_nullable_map_nullable_values,
                col_non_null_map_non_null_values,
                col_nullable_struct,
                col_non_null_struct_nullable_field,
            ],
            null()
        );

        // Verify nullability settings
        let schema = extract_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();
        assert_eq!(fields.len(), 8);

        let expected_names_and_nulls = [
            ("col_required_string", false),
            ("col_nullable_string", true),
            ("col_nullable_array_non_null_elements", true),
            ("col_non_null_array_nullable_elements", false),
            ("col_nullable_map_nullable_values", true),
            ("col_non_null_map_non_null_values", false),
            ("col_nullable_struct", true),
            ("col_non_null_struct_nullable_field", false),
        ];

        for (field, (name, nullability)) in fields.iter().zip(expected_names_and_nulls) {
            assert_eq!(field.name(), name);
            assert_eq!(
                field.is_nullable(),
                nullability,
                "Nullablity didn't match for {}",
                field.name()
            );
        }

        assert_array(fields[2], DataType::STRING, false);
        assert_array(fields[3], DataType::STRING, true);

        assert_map(fields[4], DataType::STRING, DataType::INTEGER, true);
        assert_map(fields[5], DataType::STRING, DataType::INTEGER, false);

        assert_struct(fields[6], DataType::STRING, false);
        assert_struct(fields[7], DataType::STRING, true);
    }

    #[test]
    fn cannot_use_nullable_as_map_keys() {
        // Error allocator for tests that panics when invoked. It is used in tests where we don't
        // expect errors.
        #[no_mangle]
        extern "C" fn ensure_map_err(
            _etype: KernelError,
            msg: crate::KernelStringSlice,
        ) -> *mut EngineError {
            let msg = unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(msg.ptr.cast(), msg.len))
            };
            assert_eq!(
                msg,
                "Generic delta kernel error: Delta Map keys may not be nullable"
            );
            std::ptr::null_mut()
        }

        let mut state = KernelSchemaVisitorState::default();
        let kf = visit_field!(string, state, "key", true, null());
        let vf = visit_field!(integer, state, "value", false, null());
        let res = unsafe {
            visit_field_map(
                &mut state,
                KernelStringSlice::new_unsafe("map_check"),
                kf,
                vf,
                false,
                null(),
                ensure_map_err,
            )
        };
        assert!(res.is_err());
    }
}
