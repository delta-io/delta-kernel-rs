//! The `KernelSchemaVisitor` defines a visitor system to allow engines to build kernel-native
//! representations of schemas for projection pushdown during scans.
//! 
//! The model is ID based. When the engine wants to create a schema element, it calls the appropriate
//! visitor function which constructs the analgous kernel schema element and returns an `id` (`usize`).
//! That ID is passed to other visitor functions to reference that element when building complex types.
//! 
//! Every schema element belongs to one of two categories. Fields (named elements with nullability)
//! or DataTypes (anonymous type information for use in complex types).
//! 1. **Fields** are created by `visit_schema_*` functions and represent named schema columns.
//! 2. **DataTypes** are created by `visit_*_type` functions and represent type information for use
//!    as array elements, map keys/values, or struct field type.
//! 3. **Complex types** (arrays, maps, structs) reference other elements by their IDs
//! 4. The final schema is built by combining the fields IDs of the top-level schema, and is represented
//!     as a DataType::Struct.
//! 
//! Note: IDs are consumed when used. Each element takes ownership of its referenced child elements.
//! 
//! Building a schema requires creating elements in dependency order. Referenced elements must be constructed
//! before the elements that reference them. In other words, children must be created before parents.

use crate::{AllocateErrorFn, ExternResult, IntoExternResult, KernelStringSlice, ReferenceSet, TryFromStringSlice};
use delta_kernel::schema::{
    ArrayType, DataType, DecimalType, MapType, PrimitiveType, StructField, StructType,
};
use delta_kernel::{DeltaResult, Error};

/// The different types of schema elements.
/// 1. **Field** is a complete field (name + type + nullability)
/// 2. **DataType** is a data type (primitive, array, map, struct, etc.)
/// The final schema is represented as a DataType::Struct.
pub(crate) enum SchemaElement {
    /// A complete field (name + type + nullability)
    Field(StructField),
    /// A data type (primitive, array, map, struct, etc.)
    /// For structs, this includes both nested structs and top-level schemas
    DataType(DataType),
}

#[derive(Default)]
pub struct KernelSchemaVisitorState {
    elements: ReferenceSet<SchemaElement>,
}

/// Helper to insert a StructField and return its ID
fn wrap_field(state: &mut KernelSchemaVisitorState, field: StructField) -> usize {
    let element = SchemaElement::Field(field);
    state.elements.insert(element)
}

/// Helper to insert a DataType and return its ID
fn wrap_data_type(state: &mut KernelSchemaVisitorState, data_type: DataType) -> usize {
    let element = SchemaElement::DataType(data_type);
    state.elements.insert(element)
}


/// Extract a StructField from the visitor state
fn unwrap_field(state: &mut KernelSchemaVisitorState, field_id: usize) -> Option<StructField> {
    match state.elements.take(field_id)? {
        SchemaElement::Field(field) => Some(field),
        SchemaElement::DataType(_) => None, // DataType cannot be converted to Field
    }
}

/// Extract a DataType from the visitor state
fn unwrap_data_type(state: &mut KernelSchemaVisitorState, type_id: usize) -> Option<DataType> {
    match state.elements.take(type_id)? {
        SchemaElement::DataType(data_type) => Some(data_type),
        SchemaElement::Field(_) => None, // Field cannot be converted to DataType
    }
}

/// Extract the final schema from the visitor state.
/// The schema must be stored as DataType::Struct. No coercion is performed.
pub fn unwrap_kernel_schema(
    state: &mut KernelSchemaVisitorState,
    schema_id: usize,
) -> Option<StructType> {
    match state.elements.take(schema_id)? {
        SchemaElement::DataType(DataType::Struct(struct_type)) => Some(*struct_type),
        _ => None, // Only DataType::Struct can be extracted as a schema
    }
}

// =============================================================================
// FFI Visitor Functions - Primitive Types
// =============================================================================

/// Generic helper to create primitive fields
fn visit_schema_primitive_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    primitive_type: PrimitiveType,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let field = StructField::new(name_str, DataType::Primitive(primitive_type), nullable);
    Ok(wrap_field(state, field))
}

/*
// Alternative Implementation: Macro-Based Approach to Reduce Boilerplate
//
// The explicit functions below could be generated using this macro to eliminate repetition:
//
// macro_rules! generate_primitive_visitor {
//     ($fn_name:ident, $primitive_type:expr, $doc:expr) => {
//         #[doc = $doc]
//         #[doc = ""]
//         #[doc = "# Safety"]
//         #[doc = ""]
//         #[doc = "Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,"]
//         #[doc = "and `allocate_error` function pointer."]
//         #[no_mangle]
//         pub unsafe extern "C" fn $fn_name(
//             state: &mut KernelSchemaVisitorState,
//             name: KernelStringSlice,
//             nullable: bool,
//             allocate_error: AllocateErrorFn,
//         ) -> ExternResult<usize> {
//             let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
//             visit_schema_primitive_impl(state, name_str, $primitive_type, nullable)
//                 .into_extern_result(&allocate_error)
//         }
//     };
// }
//
// Then generate all primitive functions:
// generate_primitive_visitor!(visit_schema_string, PrimitiveType::String, "Visit a string field. Strings in Delta Lake can hold arbitrary UTF-8 text data.");
// generate_primitive_visitor!(visit_schema_long, PrimitiveType::Long, "Visit a long field. Long fields store 64-bit signed integers.");
// ... etc for all primitive types
//
// However, we use explicit functions below for better generated documentation.
*/

/// Visit a string field. Strings in Delta Lake can hold arbitrary UTF-8 text data.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_string(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::String, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a long field. Long fields store 64-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_long(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Long, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit an integer field. Integer fields store 32-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_integer(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Integer, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a short field. Short fields store 16-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_short(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Short, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a byte field. Byte fields store 8-bit signed integers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_byte(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Byte, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a float field. Float fields store 32-bit floating point numbers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_float(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Float, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a double field. Double fields store 64-bit floating point numbers.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_double(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Double, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a boolean field. Boolean fields store true/false values.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_boolean(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Boolean, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a binary field. Binary fields store arbitrary byte arrays.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_binary(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Binary, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a date field. Date fields store calendar dates without time information.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_date(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Date, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a timestamp field. Timestamp fields store date and time with microsecond precision in UTC.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_timestamp(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::Timestamp, nullable)
        .into_extern_result(&allocate_error)
}

/// Visit a timestamp_ntz field. Similar to timestamp but without timezone information.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_timestamp_ntz(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_primitive_impl(state, name_str, PrimitiveType::TimestampNtz, nullable)
        .into_extern_result(&allocate_error)
}


/// Visit a decimal field. Decimal fields store fixed-precision decimal numbers with specified precision and scale.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_decimal(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    precision: u8,
    scale: u8,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_decimal_impl(state, name_str, precision, scale, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_schema_decimal_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    precision: u8,
    scale: u8,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    let decimal_type = DecimalType::try_new(precision, scale)
        .map_err(|e| Error::generic(format!("Invalid decimal type precision/scale: {}", e)))?;
    let field = StructField::new(name_str, DataType::Primitive(PrimitiveType::Decimal(decimal_type)), nullable);

    Ok(wrap_field(state, field))
}

// =============================================================================
// FFI Visitor Functions - Complex Types
// =============================================================================

/// Visit a struct field. Struct fields contain nested fields organized as key-value pairs.
///
/// The `field_ids` array must contain IDs from previous `visit_schema_*` field creation calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `field_ids` array pointing
/// to valid field IDs previously returned by this visitor, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_struct(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    field_ids: *const usize,
    field_count: usize,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let field_slice = unsafe { std::slice::from_raw_parts(field_ids, field_count) };
    visit_schema_struct_impl(state, name_str, field_slice, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_schema_struct_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    field_ids: &[usize],
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    // Extract fields from IDs and build struct
    let mut field_vec = Vec::new();
    for &field_id in field_ids {
        if let Some(field) = unwrap_field(state, field_id) {
            field_vec.push(field);
        } else {
            return Err(Error::generic(format!("Invalid field ID {} in struct", field_id)));
        }
    }

    let struct_type = StructType::new(field_vec);
    let field = StructField::new(name_str, struct_type, nullable);

    Ok(wrap_field(state, field))
}

/// Visit an array field. Array fields store ordered sequences of elements of the same type.
///
/// The `element_type_id` must reference a DataType created by a previous `visit_*_type` call.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `element_type_id` from
/// previous `visit_*_type` call, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_array(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    element_type_id: usize,
    contains_null: bool,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_array_impl(state, name_str, element_type_id, contains_null, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_schema_array_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    element_type_id: usize,
    contains_null: bool,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    let element_type = unwrap_data_type(state, element_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid element type ID {} for array", element_type_id)))?;

    let array_type = ArrayType::new(element_type, contains_null);
    let field = StructField::new(name_str, array_type, nullable);

    Ok(wrap_field(state, field))
}

/// Visit a map field. Map fields store key-value pairs where all keys have the same type and all values have the same type.
///
/// Both `key_type_id` and `value_type_id` must reference DataTypes created by previous `visit_*_type` calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `key_type_id` and `value_type_id`
/// from previous `visit_*_type` calls, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_map(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_map_impl(state, name_str, key_type_id, value_type_id, value_contains_null, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_schema_map_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    let key_type = unwrap_data_type(state, key_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid key type ID {} for map", key_type_id)))?;

    let value_type = unwrap_data_type(state, value_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid value type ID {} for map", value_type_id)))?;

    let map_type = MapType::new(key_type, value_type, value_contains_null);
    let field = StructField::new(name_str, map_type, nullable);

    Ok(wrap_field(state, field))
}

/// Create a Variant field (for semi-structured data)
/// Takes a struct type ID that defines the variant schema
///
/// # Safety
///
/// Caller must ensure:
/// - All base parameters are valid as per visit_schema_string
/// - `variant_struct_id` is a valid struct type ID from a previous visitor call
#[no_mangle]
pub unsafe extern "C" fn visit_schema_variant(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    variant_struct_id: usize,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_schema_variant_impl(state, name_str, variant_struct_id, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_schema_variant_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    variant_struct_id: usize,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    // Extract the struct type for the variant
    let variant_struct = match state.elements.take(variant_struct_id) {
        Some(SchemaElement::DataType(DataType::Struct(s))) => *s,
        _ => return Err(Error::generic(format!("Invalid variant struct ID {} - must be DataType::Struct", variant_struct_id))),
    };

    let field = StructField::new(name_str, DataType::Variant(Box::new(variant_struct)), nullable);

    Ok(wrap_field(state, field))
}

// =============================================================================
// FFI Functions - Schema Building
// =============================================================================

/// Build the final schema from a collection of field IDs. This creates the top-level table schema
/// by combining all the root-level fields into a complete structure.
///
/// This is typically the final step in schema construction, taking field IDs from previous
/// `visit_schema_*` calls and assembling them into a complete `DataType::Struct` schema.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `field_ids` array pointing to valid field IDs
/// previously returned by `visit_schema_*` calls, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn build_kernel_schema(
    state: &mut KernelSchemaVisitorState,
    field_ids: *const usize,
    field_count: usize,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let field_slice = unsafe { std::slice::from_raw_parts(field_ids, field_count) };
    build_kernel_schema_impl(state, field_slice)
        .into_extern_result(&allocate_error)
}

fn build_kernel_schema_impl(
    state: &mut KernelSchemaVisitorState,
    field_ids: &[usize],
) -> DeltaResult<usize> {
    let mut field_vec = Vec::new();

    for &field_id in field_ids {
        if let Some(field) = unwrap_field(state, field_id) {
            field_vec.push(field);
        } else {
            return Err(Error::generic(format!("Invalid field ID {} in schema", field_id)));
        }
    }

    let schema = StructType::new(field_vec);
    let data_type = DataType::Struct(Box::new(schema));
    Ok(wrap_data_type(state, data_type))
}

// =============================================================================
// Helper Functions for Type-Only Building (No Field Names)
// =============================================================================

/// Generic helper to visit DataType objects for use in complex types
fn visit_primitive_data_type_impl(
    state: &mut KernelSchemaVisitorState,
    primitive_type: PrimitiveType,
) -> DeltaResult<usize> {
    let data_type = DataType::Primitive(primitive_type);
    Ok(wrap_data_type(state, data_type))
}

/*
// Alternative Implementation: Macro-Based Approach for Type Creation Functions
//
// The explicit type creation functions below could be generated using this macro:
//
// macro_rules! generate_type_creator {
//     ($fn_name:ident, $primitive_type:expr, $doc:expr) => {
//         #[doc = $doc]
//         #[no_mangle]
//         pub extern "C" fn $fn_name(
//             state: &mut KernelSchemaVisitorState,
//             allocate_error: AllocateErrorFn,
//         ) -> ExternResult<usize> {
//             unsafe {
//                 visit_primitive_data_type_impl(state, $primitive_type)
//                     .into_extern_result(&allocate_error)
//             }
//         }
//     };
// }
//
// Then generate all type creation functions:
// generate_type_creator!(visit_string_type, PrimitiveType::String, "Create a string DataType for use as array elements, map keys, or map values.");
// generate_type_creator!(visit_double_type, PrimitiveType::Double, "Create a double DataType for 64-bit floating point numbers in complex types.");
// ... etc for all primitive types
//
// However, we use explicit functions below for better generated documentation.
*/

/// Visit a string DataType for use as array elements, map keys, or map values.
#[no_mangle]
pub extern "C" fn visit_string_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::String)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a long DataType for 64-bit integers in complex types.
#[no_mangle]
pub extern "C" fn visit_long_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Long)
            .into_extern_result(&allocate_error)
    }
}

/// Create an integer DataType for 32-bit integers in complex types.
#[no_mangle]
pub extern "C" fn visit_integer_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Integer)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a short DataType for 16-bit integers in complex types.
#[no_mangle]
pub extern "C" fn visit_short_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Short)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a byte DataType for 8-bit integers in complex types.
#[no_mangle]
pub extern "C" fn visit_byte_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Byte)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a float DataType for 32-bit floating point numbers in complex types.
#[no_mangle]
pub extern "C" fn visit_float_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Float)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a double DataType for 64-bit floating point numbers in complex types.
#[no_mangle]
pub extern "C" fn visit_double_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Double)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a boolean DataType for true/false values in complex types.
#[no_mangle]
pub extern "C" fn visit_boolean_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Boolean)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a binary DataType for byte arrays in complex types.
#[no_mangle]
pub extern "C" fn visit_binary_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Binary)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a date DataType for calendar dates in complex types.
#[no_mangle]
pub extern "C" fn visit_date_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Date)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a timestamp DataType with microsecond precision in UTC for complex types.
#[no_mangle]
pub extern "C" fn visit_timestamp_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::Timestamp)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a timestamp_ntz DataType without timezone information for complex types.
#[no_mangle]
pub extern "C" fn visit_timestamp_ntz_type(
    state: &mut KernelSchemaVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_primitive_data_type_impl(state, PrimitiveType::TimestampNtz)
            .into_extern_result(&allocate_error)
    }
}

/// Visit a decimal DataType with specified precision and scale for use in complex types.
#[no_mangle]
pub extern "C" fn visit_decimal_type(
    state: &mut KernelSchemaVisitorState,
    precision: u8,
    scale: u8,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe { visit_decimal_type_impl(state, precision, scale)
        .into_extern_result(&allocate_error) }
}

fn visit_decimal_type_impl(
    state: &mut KernelSchemaVisitorState,
    precision: u8,
    scale: u8,
) -> DeltaResult<usize> {
    let decimal_type = DecimalType::try_new(precision, scale)
        .map_err(|e| Error::generic(format!("Invalid decimal type precision/scale: {}", e)))?;
    let data_type = DataType::Primitive(PrimitiveType::Decimal(decimal_type));
    Ok(wrap_data_type(state, data_type))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel_string_slice;
    use crate::error::{KernelError, EngineError};
    use crate::ffi_test_utils::ok_or_panic;
    use delta_kernel::schema::{DataType, PrimitiveType};

    // Error allocator for tests that panics when invoked. It is used in tests where we don't expect errors.
    #[no_mangle]
    extern "C" fn test_allocate_error(etype: KernelError, msg: crate::KernelStringSlice) -> *mut EngineError {
        panic!("Error allocator called with type {:?}, message: {:?}", etype, unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(msg.ptr as *const u8, msg.len)) });
    }

    #[test]
    fn test_schema_all_types() {
        let mut state = KernelSchemaVisitorState::default();
        
        // Schema: struct<
        //   f_string: string,
        //   f_long: long,
        //   f_int: int,
        //   f_short: short,
        //   f_byte: byte,
        //   f_double: double,
        //   f_float: float,
        //   f_boolean: boolean,
        //   f_binary: binary,
        //   f_date: date,
        //   f_timestamp: timestamp,
        //   f_timestamp_ntz: timestamp_ntz,
        //   f_decimal: decimal(10,2),
        //   f_array: array<string>,
        //   f_map: map<string, long>,
        //   f_struct: struct<inner: string>,
        //   f_variant: variant<variant_field: string>
        // >

        let f_string_name = "f_string".to_string();
        let f_long_name = "f_long".to_string();
        let f_int_name = "f_int".to_string();
        let f_short_name = "f_short".to_string();
        let f_byte_name = "f_byte".to_string();
        let f_double_name = "f_double".to_string();
        let f_float_name = "f_float".to_string();
        let f_boolean_name = "f_boolean".to_string();
        let f_binary_name = "f_binary".to_string();
        let f_date_name = "f_date".to_string();
        let f_timestamp_name = "f_timestamp".to_string();
        let f_timestamp_ntz_name = "f_timestamp_ntz".to_string();
        let f_decimal_name = "f_decimal".to_string();
        let f_array_name = "f_array".to_string();
        let f_map_name = "f_map".to_string();
        let f_struct_name = "f_struct".to_string();
        let f_variant_name = "f_variant".to_string();

        // create primitive fields
        // immediately unwrap the results
        let f_string = ok_or_panic(unsafe { visit_schema_string(&mut state, kernel_string_slice!(f_string_name), false, test_allocate_error) });
        let f_long = ok_or_panic(unsafe { visit_schema_long(&mut state, kernel_string_slice!(f_long_name), false, test_allocate_error) });
        let f_int = ok_or_panic(unsafe { visit_schema_integer(&mut state, kernel_string_slice!(f_int_name), false, test_allocate_error) });
        let f_short = ok_or_panic(unsafe { visit_schema_short(&mut state, kernel_string_slice!(f_short_name), false, test_allocate_error) });
        let f_byte = ok_or_panic(unsafe { visit_schema_byte(&mut state, kernel_string_slice!(f_byte_name), false, test_allocate_error) });
        let f_double = ok_or_panic(unsafe { visit_schema_double(&mut state, kernel_string_slice!(f_double_name), false, test_allocate_error) });
        let f_float = ok_or_panic(unsafe { visit_schema_float(&mut state, kernel_string_slice!(f_float_name), false, test_allocate_error) });
        let f_boolean = ok_or_panic(unsafe { visit_schema_boolean(&mut state, kernel_string_slice!(f_boolean_name), false, test_allocate_error) });
        let f_binary = ok_or_panic(unsafe { visit_schema_binary(&mut state, kernel_string_slice!(f_binary_name), false, test_allocate_error) });
        let f_date = ok_or_panic(unsafe { visit_schema_date(&mut state, kernel_string_slice!(f_date_name), false, test_allocate_error) });
        let f_timestamp = ok_or_panic(unsafe { visit_schema_timestamp(&mut state, kernel_string_slice!(f_timestamp_name), false, test_allocate_error) });
        let f_timestamp_ntz = ok_or_panic(unsafe { visit_schema_timestamp_ntz(&mut state, kernel_string_slice!(f_timestamp_ntz_name), false, test_allocate_error) });
        let f_decimal = ok_or_panic(unsafe { visit_schema_decimal(&mut state, kernel_string_slice!(f_decimal_name), 10, 2, false, test_allocate_error) });

        // create array<string>
        let string_type_id = ok_or_panic(visit_string_type(&mut state, test_allocate_error));
        let f_array = ok_or_panic(unsafe { visit_schema_array(&mut state, kernel_string_slice!(f_array_name), string_type_id, false, false, test_allocate_error) });

        // create map<string, long>
        let string_type_id = ok_or_panic(visit_string_type(&mut state, test_allocate_error));
        let long_type_id = ok_or_panic(visit_long_type(&mut state, test_allocate_error));
        let f_map = ok_or_panic(unsafe { visit_schema_map(&mut state, kernel_string_slice!(f_map_name), string_type_id, long_type_id, false, false, test_allocate_error) });

        // create struct<inner: string>
        let inner_name = "inner".to_string();
        let inner_field = ok_or_panic(unsafe { visit_schema_string(&mut state, kernel_string_slice!(inner_name), false, test_allocate_error) });
        let f_struct = ok_or_panic(unsafe { visit_schema_struct(&mut state, kernel_string_slice!(f_struct_name), &inner_field, 1, false, test_allocate_error) });

        // create variant
        let variant_inner_name = "variant_field".to_string();
        let variant_inner_field = ok_or_panic(unsafe { visit_schema_string(&mut state, kernel_string_slice!(variant_inner_name), false, test_allocate_error) });

        // For variant, we need to manually create a DataType::Struct (not a named field)
        let variant_field = unwrap_field(&mut state, variant_inner_field).expect("Variant field should exist");
        let variant_struct_type = StructType::new(vec![variant_field]);
        let variant_struct_data_type = DataType::Struct(Box::new(variant_struct_type));
        let variant_struct_type_id = wrap_data_type(&mut state, variant_struct_data_type);

        let f_variant = ok_or_panic(unsafe { visit_schema_variant(&mut state, kernel_string_slice!(f_variant_name), variant_struct_type_id, false, test_allocate_error) });

        // create the final schema
        let field_ids = vec![
            f_string, f_long, f_int, f_short, f_byte, f_double, f_float, f_boolean, f_binary, f_date, f_timestamp, f_timestamp_ntz, f_decimal, f_array, f_map, f_struct, f_variant
        ];
        let schema_id = ok_or_panic(unsafe { build_kernel_schema(&mut state, field_ids.as_ptr(), field_ids.len(), test_allocate_error) });
        let schema = unwrap_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();
        assert_eq!(fields.len(), 17);

        // validate the constructed schema
        let primitive_field_expectations = [
            ("f_string", PrimitiveType::String),
            ("f_long", PrimitiveType::Long),
            ("f_int", PrimitiveType::Integer),
            ("f_short", PrimitiveType::Short),
            ("f_byte", PrimitiveType::Byte),
            ("f_double", PrimitiveType::Double),
            ("f_float", PrimitiveType::Float),
            ("f_boolean", PrimitiveType::Boolean),
            ("f_binary", PrimitiveType::Binary),
            ("f_date", PrimitiveType::Date),
            ("f_timestamp", PrimitiveType::Timestamp),
            ("f_timestamp_ntz", PrimitiveType::TimestampNtz),
        ];

        for (i, (expected_name, expected_primitive)) in primitive_field_expectations.iter().enumerate() {
            assert_eq!(fields[i].name(), *expected_name, "Field at index {} has wrong name", i);
            assert_eq!(fields[i].data_type(), &DataType::Primitive(expected_primitive.clone()), "Field {} has wrong primitive type", expected_name);
            assert!(!fields[i].is_nullable(), "Field {} should not be nullable", expected_name);
        }

        // validate the decimal field
        assert_eq!(fields[12].name(), "f_decimal");
        let DataType::Primitive(PrimitiveType::Decimal(decimal_type)) = fields[12].data_type() else {
            panic!("Field f_decimal is not a decimal type");
        };
        assert_eq!(decimal_type.precision(), 10);
        assert_eq!(decimal_type.scale(), 2);

        // validate array field: f_array: array<string>
        assert_eq!(fields[13].name(), "f_array");
        let DataType::Array(array_type) = fields[13].data_type() else {
            panic!("Field f_array is not an array type");
        };
        assert_eq!(array_type.element_type(), &DataType::Primitive(PrimitiveType::String));
        assert!(!array_type.contains_null());
        assert!(!fields[13].is_nullable());

        // validate map field: f_map: map<string, long>
        assert_eq!(fields[14].name(), "f_map");
        let DataType::Map(map_type) = fields[14].data_type() else {
            panic!("Field f_map is not a map type");
        };
        assert_eq!(map_type.key_type(), &DataType::Primitive(PrimitiveType::String));
        assert_eq!(map_type.value_type(), &DataType::Primitive(PrimitiveType::Long));
        assert!(!map_type.value_contains_null());
        assert!(!fields[14].is_nullable());

        // validate struct field: f_struct: struct<inner: string>
        assert_eq!(fields[15].name(), "f_struct");
        let DataType::Struct(struct_type) = fields[15].data_type() else {
            panic!("Field f_struct is not a struct type");
        };
        let struct_fields: Vec<_> = struct_type.fields().collect();
        assert_eq!(struct_fields.len(), 1);
        assert_eq!(struct_fields[0].name(), "inner");
        assert_eq!(struct_fields[0].data_type(), &DataType::Primitive(PrimitiveType::String));
        assert!(!struct_fields[0].is_nullable());
        assert!(!fields[15].is_nullable());

        // validate variant field: f_variant: variant<variant_field: string>
        assert_eq!(fields[16].name(), "f_variant");
        let DataType::Variant(variant_struct) = fields[16].data_type() else {
            panic!("Field f_variant is not a variant type");
        };
        let variant_fields: Vec<_> = variant_struct.fields().collect();
        assert_eq!(variant_fields.len(), 1);
        assert_eq!(variant_fields[0].name(), "variant_field");
        assert_eq!(variant_fields[0].data_type(), &DataType::Primitive(PrimitiveType::String));
        assert!(!variant_fields[0].is_nullable());
        assert!(!fields[16].is_nullable());

    }

    #[test]
    fn test_deeply_nested_schema() {
        // Test: Complex nesting like array<struct<city: string, scores: map<string, long>>>
        let mut state = KernelSchemaVisitorState::default();

        // Build from innermost types outward (post-order):
        // 1. Build struct fields: city (string) and scores (map<string, long>)
        let city_name = "city".to_string();
        let city_field = unsafe { visit_schema_string(&mut state, kernel_string_slice!(city_name), false, test_allocate_error) };

        // 2. Build map<string, long> for scores field
        let string_type_id = ok_or_panic(visit_string_type(&mut state, test_allocate_error));
        let long_type_id = ok_or_panic(visit_long_type(&mut state, test_allocate_error));
        let scores_name = "scores".to_string();
        let scores_field = unsafe { visit_schema_map(&mut state, kernel_string_slice!(scores_name), string_type_id, long_type_id, false, false, test_allocate_error) };

        // 3. Build struct<city: string, scores: map<string, long>>
        let struct_fields = vec![ok_or_panic(city_field), ok_or_panic(scores_field)];
        let location_name = "location".to_string();
        let location_struct_field = unsafe { visit_schema_struct(&mut state, kernel_string_slice!(location_name), struct_fields.as_ptr(), 2, false, test_allocate_error) };

        // Build final schema with just the nested struct field
        let schema_id = ok_or_panic(unsafe { build_kernel_schema(&mut state, &ok_or_panic(location_struct_field), 1, test_allocate_error) });
        let schema = unwrap_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "location");

        // Validate the nested structure
        let DataType::Struct(struct_type) = fields[0].data_type() else {
            panic!("Expected struct type for location field");
        };
        let nested_fields: Vec<_> = struct_type.fields().collect();
        assert_eq!(nested_fields.len(), 2);
        assert_eq!(nested_fields[0].name(), "city");
        assert_eq!(nested_fields[0].data_type(), &DataType::Primitive(PrimitiveType::String));

        assert_eq!(nested_fields[1].name(), "scores");
        let DataType::Map(map_type) = nested_fields[1].data_type() else {
            panic!("Expected map type for scores field");
        };
        assert_eq!(map_type.key_type(), &DataType::Primitive(PrimitiveType::String));
        assert_eq!(map_type.value_type(), &DataType::Primitive(PrimitiveType::Long));
    }

    #[test]
    fn test_nullability_variations() {
        // Test: Different nullability combinations to ensure nullable flags work correctly
        let mut state = KernelSchemaVisitorState::default();

        let nullable_name = "nullable".to_string();
        let required_name = "required".to_string();
        let tags_name = "tags".to_string();

        let nullable_string = unsafe { visit_schema_string(&mut state, kernel_string_slice!(nullable_name), true, test_allocate_error) };
        let non_nullable_string = unsafe { visit_schema_string(&mut state, kernel_string_slice!(required_name), false, test_allocate_error) };

        // Test array with different nullability settings
        let string_type_id = ok_or_panic(visit_string_type(&mut state, test_allocate_error));
        let nullable_array_non_null_elements = unsafe { visit_schema_array(&mut state, kernel_string_slice!(tags_name), string_type_id, false, true, test_allocate_error) };

        let field_ids = vec![ok_or_panic(nullable_string), ok_or_panic(non_nullable_string), ok_or_panic(nullable_array_non_null_elements)];
        let schema_id = ok_or_panic(unsafe { build_kernel_schema(&mut state, field_ids.as_ptr(), 3, test_allocate_error) });
        let schema = unwrap_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();

        assert_eq!(fields.len(), 3);

        assert_eq!(fields[0].name(), "nullable");
        assert!(fields[0].is_nullable());

        assert_eq!(fields[1].name(), "required");
        assert!(!fields[1].is_nullable());

        assert_eq!(fields[2].name(), "tags");
        assert!(fields[2].is_nullable()); // Field itself is nullable
        let DataType::Array(array_type) = fields[2].data_type() else {
            panic!("Expected array type for tags field");
        };
        assert!(!array_type.contains_null()); // But elements are not nullable
    }
}
