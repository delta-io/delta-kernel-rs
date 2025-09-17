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


// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::kernel_string_slice;
//     use crate::error::{KernelError, EngineError};
//     use crate::ffi_test_utils::ok_or_panic;

//     // Test helper - dummy error allocator
//     #[no_mangle]
//     extern "C" fn test_allocate_error(_: KernelError, _: crate::KernelStringSlice) -> *mut EngineError {
//         std::ptr::null_mut()
//     }

//     #[test]
//     fn test_schema_visitor_state_management() {
//         let mut state = KernelSchemaVisitorState::default();

//         // Test basic field creation and ID allocation
//         let test_field = "test_field".to_string();
//         let name_slice = kernel_string_slice!(test_field);
//         let field_result = unsafe { visit_schema_string(&mut state, name_slice, false, test_allocate_error) };
//         assert!(field_result.is_ok());
//         let field_id = ok_or_panic(field_result);

//         // Test schema building from field IDs
//         let field_ids = vec![field_id];
//         let schema_result = unsafe { build_kernel_schema(&mut state, field_ids.as_ptr(), 1, test_allocate_error) };
//         assert!(schema_result.is_ok());
//         let schema_id = ok_or_panic(schema_result);

//         // Test schema extraction
//         let schema = unwrap_kernel_schema(&mut state, schema_id);
//         assert!(schema.is_some());
//     }

//     #[test]
//     fn test_primitive_type_creation() {
//         let mut state = KernelSchemaVisitorState::default();

//         // Test various primitive types
//         let id_name = "id".to_string();
//         let name_name = "name".to_string();
//         let active_name = "active".to_string();

//         let long_id = ok_or_panic(unsafe { visit_schema_long(&mut state, kernel_string_slice!(id_name), false, test_allocate_error) });
//         let string_id = ok_or_panic(unsafe { visit_schema_string(&mut state, kernel_string_slice!(name_name), true, test_allocate_error) });
//         let bool_id = ok_or_panic(unsafe { visit_schema_boolean(&mut state, kernel_string_slice!(active_name), false, test_allocate_error) });

//         // Verify IDs are different
//         assert_ne!(long_id, string_id);
//         assert_ne!(string_id, bool_id);
//         assert_ne!(long_id, bool_id);
//     }

// }
