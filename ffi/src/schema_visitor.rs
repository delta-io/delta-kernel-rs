//! The `KernelSchemaVisitor` defines a visitor system to allow engines to build kernel-native
//! representations of schemas for projection pushdown during scans.
//!
//! The model is ID based. When the engine wants to create a schema element, it calls the appropriate
//! visitor function which constructs the analogous kernel schema element and returns an `id` (`usize`).
//! That ID is passed to other visitor functions to reference that element when building complex types.
//!
//! Every schema element belongs to one of two categories:
//!     1. **Fields** are created by `visit_schema_*` functions and represent named schema columns.
//!     2. **DataTypes** are created by `visit_*_type` functions and represent type information for use
//!         as array elements, map keys/values, or struct field types.
//! **Complex types** (arrays, maps, structs) reference other elements by their IDs
//! The final schema is built by combining field IDs of the top-level fields, and is represented
//! as a DataType::Struct.
//!
//! Note: Schemas are structs but can also contain struct fields. Use `visit_struct_type` for both the root
//! schema and anonymous struct types, and `visit_field_struct` for named struct fields.
//!
//! IDs are consumed when used. Each element takes ownership of its referenced child elements.
//!
//! Building a schema requires creating elements in dependency order. Referenced elements must be constructed
//! before the elements that reference them. In other words, children must be created before parents.

use crate::{
    AllocateErrorFn, ExternResult, IntoExternResult, KernelStringSlice, ReferenceSet,
    TryFromStringSlice,
};
use delta_kernel::schema::{
    ArrayType, DataType, DecimalType, MapType, PrimitiveType, StructField, StructType,
};
use delta_kernel::{DeltaResult, Error};

/// The different types of schema elements.
/// 1. **Field** is a complete field (name + type + nullability)
/// 2. **DataType** is a data type (primitive, array, map, struct, etc.)
///
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

/// Extract the final schema from the visitor state.
///
/// This validates that the schema was properly constructed by ensuring:
/// 1. The schema_id points to a DataType::Struct (the root schema)
/// 2. No other elements remain in the state (all field IDs are consumed)
pub fn unwrap_kernel_schema(
    state: &mut KernelSchemaVisitorState,
    schema_id: usize,
) -> Option<StructType> {
    let schema_element = state.elements.take(schema_id)?;

    let struct_type = match schema_element {
        SchemaElement::DataType(DataType::Struct(struct_type)) => struct_type,
        _ => return None,
    };

    if !state.elements.is_empty() {
        return None;
    }

    Some(*struct_type)
}

/// Helper to insert a StructField and return its ID
fn wrap_field(state: &mut KernelSchemaVisitorState, field: StructField) -> usize {
    let element = SchemaElement::Field(field);
    state.elements.insert(element)
}

// Helper to insert a DataType and return its ID
fn wrap_data_type(state: &mut KernelSchemaVisitorState, data_type: DataType) -> usize {
    let element = SchemaElement::DataType(data_type);
    state.elements.insert(element)
}

// Helper to extract a StructField from the visitor state
fn unwrap_field(state: &mut KernelSchemaVisitorState, field_id: usize) -> Option<StructField> {
    match state.elements.take(field_id)? {
        SchemaElement::Field(field) => Some(field),
        SchemaElement::DataType(_) => None,
    }
}

// Helper to extract a DataType from the visitor state
fn unwrap_data_type(state: &mut KernelSchemaVisitorState, type_id: usize) -> Option<DataType> {
    match state.elements.take(type_id)? {
        SchemaElement::DataType(data_type) => Some(data_type),
        SchemaElement::Field(_) => None,
    }
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
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let field = StructField::new(name_str, DataType::Primitive(primitive_type), nullable);
    Ok(wrap_field(state, field))
}

// Macro to generate schema field visitor functions for primitive types
macro_rules! generate_primitive_schema_visitors {
    ($(($fn_name:ident, $primitive_type:expr, $doc:expr)),* $(,)?) => {
        $(
            #[doc = $doc]
            #[doc = ""]
            #[doc = "# Safety"]
            #[doc = ""]
            #[doc = "Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,"]
            #[doc = "and `allocate_error` function pointer."]
            #[no_mangle]
            pub unsafe extern "C" fn $fn_name(
                state: &mut KernelSchemaVisitorState,
                name: KernelStringSlice,
                nullable: bool,
                allocate_error: AllocateErrorFn,
            ) -> ExternResult<usize> {
                let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
                visit_field_primitive_impl(state, name_str, $primitive_type, nullable)
                    .into_extern_result(&allocate_error)
            }
        )*
    };
}

generate_primitive_schema_visitors! {
    (visit_field_string, PrimitiveType::String, "Visit a string field. Strings in Delta Lake can hold arbitrary UTF-8 text data."),
    (visit_field_long, PrimitiveType::Long, "Visit a long field. Long fields store 64-bit signed integers."),
    (visit_field_integer, PrimitiveType::Integer, "Visit an integer field. Integer fields store 32-bit signed integers."),
    (visit_field_short, PrimitiveType::Short, "Visit a short field. Short fields store 16-bit signed integers."),
    (visit_field_byte, PrimitiveType::Byte, "Visit a byte field. Byte fields store 8-bit signed integers."),
    (visit_field_float, PrimitiveType::Float, "Visit a float field. Float fields store 32-bit floating point numbers."),
    (visit_field_double, PrimitiveType::Double, "Visit a double field. Double fields store 64-bit floating point numbers."),
    (visit_field_boolean, PrimitiveType::Boolean, "Visit a boolean field. Boolean fields store true/false values."),
    (visit_field_binary, PrimitiveType::Binary, "Visit a binary field. Binary fields store arbitrary byte arrays."),
    (visit_field_date, PrimitiveType::Date, "Visit a date field. Date fields store calendar dates without time information."),
    (visit_field_timestamp, PrimitiveType::Timestamp, "Visit a timestamp field. Timestamp fields store date and time with microsecond precision in UTC."),
    (visit_field_timestamp_ntz, PrimitiveType::TimestampNtz, "Visit a timestamp_ntz field. Similar to timestamp but without timezone information."),
}

/// Visit a decimal field. Decimal fields store fixed-precision decimal numbers with specified precision and scale.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `name` slice with valid UTF-8 data,
/// and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_field_decimal(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    precision: u8,
    scale: u8,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };

    visit_field_decimal_impl(state, name_str, precision, scale, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_field_decimal_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    precision: u8,
    scale: u8,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    let decimal_type = DecimalType::try_new(precision, scale)?;
    let field = StructField::new(
        name_str,
        DataType::Primitive(PrimitiveType::Decimal(decimal_type)),
        nullable,
    );
    Ok(wrap_field(state, field))
}

// =============================================================================
// FFI Visitor Functions for field creation - Complex Types
// =============================================================================

/// Visit a struct field. Struct fields contain nested fields organized as ordered key-value pairs.
///
/// Note: This creates a named struct field (e.g. `address: struct<street, city>`), different from the
/// `visit_data_type_struct` which creates anonymous struct DataTypes.
///
/// The `field_ids` array must contain IDs from previous `visit_field_*` field creation calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `field_ids` array pointing
/// to valid field IDs previously returned by this visitor, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_field_struct(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    field_ids: *const usize,
    field_count: usize,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str: Result<&str, Error> = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let field_ids = unsafe { std::slice::from_raw_parts(field_ids, field_count) };

    visit_field_struct_impl(state, name_str, field_ids, nullable)
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

    let struct_type = StructType::new(field_vec);
    Ok(DataType::Struct(Box::new(struct_type)))
}

fn visit_field_struct_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    field_ids: &[usize],
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let data_type = create_struct_data_type(state, field_ids)?;
    let field = StructField::new(name_str, data_type, nullable);
    Ok(wrap_field(state, field))
}

/// Visit an array field. Array fields store ordered sequences of elements of the same type.
///
/// The `element_type_id` must reference a DataType created by a previous `visit_data_type_*` call.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `element_type_id` from
/// previous `visit_data_type_*` call, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_field_array(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    element_type_id: usize,
    contains_null: bool,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_field_array_impl(state, name_str, element_type_id, contains_null, nullable)
        .into_extern_result(&allocate_error)
}

// Helper to create array DataType
fn create_array_data_type(
    state: &mut KernelSchemaVisitorState,
    element_type_id: usize,
    contains_null: bool,
) -> DeltaResult<DataType> {
    let element_type = unwrap_data_type(state, element_type_id).ok_or_else(|| {
        Error::generic(format!(
            "Invalid element type ID {element_type_id} for array"
        ))
    })?;

    let array_type = ArrayType::new(element_type, contains_null);
    Ok(DataType::Array(Box::new(array_type)))
}

fn visit_field_array_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    element_type_id: usize,
    contains_null: bool,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let data_type = create_array_data_type(state, element_type_id, contains_null)?;
    let field = StructField::new(name_str, data_type, nullable);
    Ok(wrap_field(state, field))
}

/// Visit a map field. Map fields store key-value pairs where all keys have the same type and all values have the same type.
///
/// Both `key_type_id` and `value_type_id` must reference DataTypes created by previous `visit_data_type_*` calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `name` slice, `key_type_id` and `value_type_id`
/// from previous `visit_data_type_*` calls, and `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_field_map(
    state: &mut KernelSchemaVisitorState,
    name: KernelStringSlice,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_field_map_impl(
        state,
        name_str,
        key_type_id,
        value_type_id,
        value_contains_null,
        nullable,
    )
    .into_extern_result(&allocate_error)
}

// Helper to create map DataType
fn create_map_data_type(
    state: &mut KernelSchemaVisitorState,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
) -> DeltaResult<DataType> {
    let key_type = unwrap_data_type(state, key_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid key type ID {key_type_id} for map")))?;

    let value_type = unwrap_data_type(state, value_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid value type ID {value_type_id} for map")))?;

    let map_type = MapType::new(key_type, value_type, value_contains_null);
    Ok(DataType::Map(Box::new(map_type)))
}

fn visit_field_map_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let data_type = create_map_data_type(state, key_type_id, value_type_id, value_contains_null)?;
    let field = StructField::new(name_str, data_type, nullable);
    Ok(wrap_field(state, field))
}

/// Visit a variant field (for semi-structured data)
/// Takes a struct type ID that defines the variant schema
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
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_field_variant_impl(state, name_str, variant_struct_id, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_field_variant_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    variant_struct_id: usize,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let data_type = create_variant_data_type(state, variant_struct_id)?;
    let field = StructField::new(name_str, data_type, nullable);
    Ok(wrap_field(state, field))
}

// Helper to create variant DataType
fn create_variant_data_type(
    state: &mut KernelSchemaVisitorState,
    struct_type_id: usize,
) -> DeltaResult<DataType> {
    let variant_struct = match state.elements.take(struct_type_id) {
        Some(SchemaElement::DataType(DataType::Struct(s))) => *s,
        _ => {
            return Err(Error::generic(format!(
                "Invalid variant struct ID {} - must be DataType::Struct",
                struct_type_id
            )))
        }
    };

    Ok(DataType::Variant(Box::new(variant_struct)))
}

// =============================================================================
// FFI Visitor Functions for data type creation - Primitive Types
// =============================================================================

/// Generic helper to create primitive types
fn visit_primitive_data_type_impl(
    state: &mut KernelSchemaVisitorState,
    primitive_type: PrimitiveType,
) -> DeltaResult<usize> {
    let data_type = DataType::Primitive(primitive_type);
    Ok(wrap_data_type(state, data_type))
}

// Macro to generate primitive DataType visitor functions
macro_rules! generate_primitive_type_visitors {
    ($(($fn_name:ident, $primitive_type:expr, $doc:expr)),* $(,)?) => {
        $(
            #[doc = $doc]
            #[no_mangle]
            pub extern "C" fn $fn_name(
                state: &mut KernelSchemaVisitorState,
                allocate_error: AllocateErrorFn,
            ) -> ExternResult<usize> {
                unsafe {
                    visit_primitive_data_type_impl(state, $primitive_type)
                        .into_extern_result(&allocate_error)
                }
            }
        )*
    };
}

// Generate all primitive DataType visitor functions (except decimal which has different signature)
generate_primitive_type_visitors! {
    (visit_data_type_string, PrimitiveType::String, "Create a string DataType for text data in complex types."),
    (visit_data_type_long, PrimitiveType::Long, "Create a long DataType for 64-bit integers in complex types."),
    (visit_data_type_integer, PrimitiveType::Integer, "Create an integer DataType for 32-bit integers in complex types."),
    (visit_data_type_short, PrimitiveType::Short, "Create a short DataType for 16-bit integers in complex types."),
    (visit_data_type_byte, PrimitiveType::Byte, "Create a byte DataType for 8-bit integers in complex types."),
    (visit_data_type_float, PrimitiveType::Float, "Create a float DataType for 32-bit floating point numbers in complex types."),
    (visit_data_type_double, PrimitiveType::Double, "Create a double DataType for 64-bit floating point numbers in complex types."),
    (visit_data_type_boolean, PrimitiveType::Boolean, "Create a boolean DataType for true/false values in complex types."),
    (visit_data_type_binary, PrimitiveType::Binary, "Create a binary DataType for byte arrays in complex types."),
    (visit_data_type_date, PrimitiveType::Date, "Create a date DataType for calendar dates in complex types."),
    (visit_data_type_timestamp, PrimitiveType::Timestamp, "Create a timestamp DataType for timestamps with timezone in complex types."),
    (visit_data_type_timestamp_ntz, PrimitiveType::TimestampNtz, "Create a timestamp_ntz DataType for timestamps without timezone in complex types."),
}

// =============================================================================
// FFI Visitor Functions for data type creation - Complex Types
// =============================================================================

/// Visit a decimal DataType with specified precision and scale for use in complex types.
///
/// # Safety
///
/// Caller is responsible for providing a valid `state`, `precision` and `scale`,
/// and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_data_type_decimal(
    state: &mut KernelSchemaVisitorState,
    precision: u8,
    scale: u8,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_data_type_decimal_impl(state, precision, scale).into_extern_result(&allocate_error)
    }
}

fn visit_data_type_decimal_impl(
    state: &mut KernelSchemaVisitorState,
    precision: u8,
    scale: u8,
) -> DeltaResult<usize> {
    let decimal_type = DecimalType::try_new(precision, scale)
        .map_err(|e| Error::generic(format!("Invalid decimal type precision/scale: {}", e)))?;
    let data_type = DataType::Primitive(PrimitiveType::Decimal(decimal_type));
    Ok(wrap_data_type(state, data_type))
}

/// Create a struct DataType from field IDs for use in complex types.
/// This creates an anonymous struct type that can be used as array elements,
/// map values, or nested within other structs.
///
/// Note: This is also the final step for building the root schema. Create all your
/// top-level fields with `visit_field_*` functions, then call this function with
/// those field IDs to build the root schema struct.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `field_ids` array pointing
/// to valid field IDs previously returned by `visit_field_*` calls, and
/// `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_data_type_struct(
    state: &mut KernelSchemaVisitorState,
    field_ids: *const usize,
    field_count: usize,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let field_slice = unsafe { std::slice::from_raw_parts(field_ids, field_count) };
    let result = create_struct_data_type(state, field_slice)
        .map(|data_type| wrap_data_type(state, data_type));
    result.into_extern_result(&allocate_error)
}

/// Create an array DataType for use in complex types.
/// This creates an anonymous array type that can be used as struct field types,
/// map values, or nested within other arrays.
///
/// The `element_type_id` must reference a DataType created by a previous `visit_data_type_*` call.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `element_type_id` from
/// previous `visit_data_type_*` call, and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_data_type_array(
    state: &mut KernelSchemaVisitorState,
    element_type_id: usize,
    contains_null: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        let result = create_array_data_type(state, element_type_id, contains_null)
            .map(|data_type| wrap_data_type(state, data_type));
        result.into_extern_result(&allocate_error)
    }
}

/// Create a map DataType for use in complex types.
/// This creates an anonymous map type that can be used as struct field types,
/// array elements, or nested within other maps.
///
/// Both `key_type_id` and `value_type_id` must reference DataTypes created by previous `visit_data_type_*` calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `key_type_id` and `value_type_id`
/// from previous `visit_data_type_*` calls, and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_data_type_map(
    state: &mut KernelSchemaVisitorState,
    key_type_id: usize,
    value_type_id: usize,
    value_contains_null: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        let result = create_map_data_type(state, key_type_id, value_type_id, value_contains_null)
            .map(|data_type| wrap_data_type(state, data_type));
        result.into_extern_result(&allocate_error)
    }
}

/// Create a variant DataType for use in complex types.
/// This creates an anonymous variant type that can be used as struct field types,
/// array elements, or map values.
///
/// The `struct_type_id` must reference a struct DataType created by a previous `visit_data_type_struct` call.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `struct_type_id` from
/// previous `visit_data_type_struct` call, and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_data_type_variant(
    state: &mut KernelSchemaVisitorState,
    struct_type_id: usize,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        let result = create_variant_data_type(state, struct_type_id)
            .map(|data_type| wrap_data_type(state, data_type));
        result.into_extern_result(&allocate_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{EngineError, KernelError};
    use crate::ffi_test_utils::ok_or_panic;
    use crate::KernelStringSlice;
    use delta_kernel::schema::{DataType, PrimitiveType};

    // Error allocator for tests that panics when invoked. It is used in tests where we don't expect errors.
    #[no_mangle]
    extern "C" fn test_allocate_error(
        etype: KernelError,
        msg: crate::KernelStringSlice,
    ) -> *mut EngineError {
        panic!(
            "Error allocator called with type {:?}, message: {:?}",
            etype,
            unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                    msg.ptr as *const u8,
                    msg.len,
                ))
            }
        );
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
        //   col_decimal: decimal(10,2),
        //   col_array: array<string>,
        //   col_map: map<string, long>,
        //   col_struct: struct<inner: string>,
        //   col_variant: variant<metadata: binary, value: binary>
        // >

        let mut state = KernelSchemaVisitorState::default();

        // Create all primitive fields
        let col_string = ok_or_panic(unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("col_string"),
                false,
                test_allocate_error,
            )
        });
        let col_long = ok_or_panic(unsafe {
            visit_field_long(
                &mut state,
                KernelStringSlice::new_unsafe("col_long"),
                false,
                test_allocate_error,
            )
        });
        let col_int = ok_or_panic(unsafe {
            visit_field_integer(
                &mut state,
                KernelStringSlice::new_unsafe("col_int"),
                false,
                test_allocate_error,
            )
        });
        let col_short = ok_or_panic(unsafe {
            visit_field_short(
                &mut state,
                KernelStringSlice::new_unsafe("col_short"),
                false,
                test_allocate_error,
            )
        });
        let col_byte = ok_or_panic(unsafe {
            visit_field_byte(
                &mut state,
                KernelStringSlice::new_unsafe("col_byte"),
                false,
                test_allocate_error,
            )
        });
        let col_double = ok_or_panic(unsafe {
            visit_field_double(
                &mut state,
                KernelStringSlice::new_unsafe("col_double"),
                false,
                test_allocate_error,
            )
        });
        let col_float = ok_or_panic(unsafe {
            visit_field_float(
                &mut state,
                KernelStringSlice::new_unsafe("col_float"),
                false,
                test_allocate_error,
            )
        });
        let col_boolean = ok_or_panic(unsafe {
            visit_field_boolean(
                &mut state,
                KernelStringSlice::new_unsafe("col_boolean"),
                false,
                test_allocate_error,
            )
        });
        let col_binary = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("col_binary"),
                false,
                test_allocate_error,
            )
        });
        let col_date = ok_or_panic(unsafe {
            visit_field_date(
                &mut state,
                KernelStringSlice::new_unsafe("col_date"),
                false,
                test_allocate_error,
            )
        });
        let col_timestamp = ok_or_panic(unsafe {
            visit_field_timestamp(
                &mut state,
                KernelStringSlice::new_unsafe("col_timestamp"),
                false,
                test_allocate_error,
            )
        });
        let col_timestamp_ntz = ok_or_panic(unsafe {
            visit_field_timestamp_ntz(
                &mut state,
                KernelStringSlice::new_unsafe("col_timestamp_ntz"),
                false,
                test_allocate_error,
            )
        });
        let col_decimal = ok_or_panic(unsafe {
            visit_field_decimal(
                &mut state,
                KernelStringSlice::new_unsafe("col_decimal"),
                10,
                2,
                false,
                test_allocate_error,
            )
        });

        // Create array<string>
        let string_element_type =
            ok_or_panic(visit_data_type_string(&mut state, test_allocate_error));
        let col_array = ok_or_panic(unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("col_array"),
                string_element_type,
                false,
                false,
                test_allocate_error,
            )
        });

        // Create map<string, long>
        let map_key_type = ok_or_panic(visit_data_type_string(&mut state, test_allocate_error));
        let map_value_type = ok_or_panic(visit_data_type_long(&mut state, test_allocate_error));
        let col_map = ok_or_panic(unsafe {
            visit_field_map(
                &mut state,
                KernelStringSlice::new_unsafe("col_map"),
                map_key_type,
                map_value_type,
                false,
                false,
                test_allocate_error,
            )
        });

        // Create struct<inner_name: string>
        let struct_inner_field = ok_or_panic(unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("inner_name"),
                false,
                test_allocate_error,
            )
        });
        let col_struct = ok_or_panic(unsafe {
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("col_struct"),
                &struct_inner_field,
                1,
                false,
                test_allocate_error,
            )
        });

        // Create variant<metadata: binary, value: binary>
        let variant_inner_field1 = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("metadata"),
                false,
                test_allocate_error,
            )
        });
        let variant_inner_field2 = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("value"),
                false,
                test_allocate_error,
            )
        });
        let variant_inner_fields = vec![variant_inner_field1, variant_inner_field2];
        let variant_struct_type = ok_or_panic(unsafe {
            visit_data_type_struct(
                &mut state,
                variant_inner_fields.as_ptr(),
                2,
                test_allocate_error,
            )
        });
        let col_variant = ok_or_panic(unsafe {
            visit_field_variant(
                &mut state,
                KernelStringSlice::new_unsafe("col_variant"),
                variant_struct_type,
                false,
                test_allocate_error,
            )
        });

        // Build the final schema
        let all_columns = vec![
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
            col_decimal,
            col_array,
            col_map,
            col_struct,
            col_variant,
        ];
        let schema_id = ok_or_panic(unsafe {
            visit_data_type_struct(
                &mut state,
                all_columns.as_ptr(),
                all_columns.len(),
                test_allocate_error,
            )
        });

        // Verify the schema
        let schema = unwrap_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();
        assert_eq!(fields.len(), 17);

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

        // Validate the decimal field
        assert_eq!(fields[12].name(), "col_decimal");
        let DataType::Primitive(PrimitiveType::Decimal(decimal_type)) = fields[12].data_type()
        else {
            panic!("Field col_decimal is not a decimal type");
        };
        assert_eq!(decimal_type.precision(), 10);
        assert_eq!(decimal_type.scale(), 2);

        // Validate array field: array<string>
        assert_eq!(fields[13].name(), "col_array");
        let DataType::Array(array_type) = fields[13].data_type() else {
            panic!("Expected array type for col_array");
        };
        assert_eq!(
            array_type.element_type(),
            &DataType::Primitive(PrimitiveType::String)
        );
        assert!(!array_type.contains_null());

        // Validate map field: map<string, long>
        assert_eq!(fields[14].name(), "col_map");
        let DataType::Map(map_type) = fields[14].data_type() else {
            panic!("Expected map type for col_map");
        };
        assert_eq!(
            map_type.key_type(),
            &DataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            map_type.value_type(),
            &DataType::Primitive(PrimitiveType::Long)
        );
        assert!(!map_type.value_contains_null());

        // Validate struct field: struct<inner_name: string>
        assert_eq!(fields[15].name(), "col_struct");
        let DataType::Struct(struct_type) = fields[15].data_type() else {
            panic!("Expected struct type for col_struct");
        };
        let struct_fields: Vec<_> = struct_type.fields().collect();
        assert_eq!(struct_fields.len(), 1);
        assert_eq!(struct_fields[0].name(), "inner_name");
        assert_eq!(
            struct_fields[0].data_type(),
            &DataType::Primitive(PrimitiveType::String)
        );

        // Validate variant field: variant<metadata: string, value: string>
        assert_eq!(fields[16].name(), "col_variant");
        let DataType::Variant(variant_type) = fields[16].data_type() else {
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

    // TODO: manndp review by hand (vibe-coded).
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
        //       deep_maps: 4a.map<4a1.variant<metadata: binary, value: binary>, 4a2.array<decimal(10,2)>>,
        //       variant_data: 4b.variant<metadata: binary, value: binary>,
        //       nested_struct: 4c.struct<
        //         final_array: 5.array<6.map<6a.struct<coord: double>, 6b.double>>
        //       >
        //     >>
        //   >>>
        // >

        // Build from deepest level outward

        // 6b: double for final map values
        let double_type = ok_or_panic(visit_data_type_double(&mut state, test_allocate_error));

        // 6a: Complex key for final map: struct<coord: double>
        let coord_field = ok_or_panic(unsafe {
            visit_field_double(
                &mut state,
                KernelStringSlice::new_unsafe("coord"),
                false,
                test_allocate_error,
            )
        });
        let coord_struct_type = ok_or_panic(unsafe {
            visit_data_type_struct(&mut state, &coord_field, 1, test_allocate_error)
        });

        // 6: map<struct<coord: double>, double>
        let final_map_type = ok_or_panic(visit_data_type_map(
            &mut state,
            coord_struct_type,
            double_type,
            false,
            test_allocate_error,
        ));

        // 4c: struct<final_array: array<map<struct<coord: double>, double>>>
        let final_array_field = ok_or_panic(unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("final_array"),
                final_map_type, // Pass the map type as the array element type
                false,
                false,
                test_allocate_error,
            )
        });

        // 4: Create the inner struct with all three complex fields

        // 4a: deep_maps field - create key and value types fresh for this field
        let variant_key_metadata_field = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("metadata"),
                false,
                test_allocate_error,
            )
        });
        let variant_key_value_field = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("value"),
                false,
                test_allocate_error,
            )
        });
        let variant_key_fields = vec![variant_key_metadata_field, variant_key_value_field];
        let variant_key_struct_type = ok_or_panic(unsafe {
            visit_data_type_struct(
                &mut state,
                variant_key_fields.as_ptr(),
                variant_key_fields.len(),
                test_allocate_error,
            )
        });
        let variant_key_type = ok_or_panic(visit_data_type_variant(
            &mut state,
            variant_key_struct_type,
            test_allocate_error,
        ));

        let decimal_type = ok_or_panic(visit_data_type_decimal(
            &mut state,
            10,
            2,
            test_allocate_error,
        ));
        let decimal_array_type = ok_or_panic(visit_data_type_array(
            &mut state,
            decimal_type,
            true,
            test_allocate_error,
        ));

        let deep_maps_field = ok_or_panic(unsafe {
            visit_field_map(
                &mut state,
                KernelStringSlice::new_unsafe("deep_maps"),
                variant_key_type,
                decimal_array_type,
                false,
                true,
                test_allocate_error,
            )
        });
        // 4b: Create fresh variant struct for variant_data field
        let variant_data_metadata_field = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("metadata"),
                false,
                test_allocate_error,
            )
        });
        let variant_data_value_field = ok_or_panic(unsafe {
            visit_field_binary(
                &mut state,
                KernelStringSlice::new_unsafe("value"),
                false,
                test_allocate_error,
            )
        });
        let variant_data_fields = vec![variant_data_metadata_field, variant_data_value_field];
        let variant_data_struct_type = ok_or_panic(unsafe {
            visit_data_type_struct(
                &mut state,
                variant_data_fields.as_ptr(),
                variant_data_fields.len(),
                test_allocate_error,
            )
        });

        let variant_data_field = ok_or_panic(unsafe {
            visit_field_variant(
                &mut state,
                KernelStringSlice::new_unsafe("variant_data"),
                variant_data_struct_type,
                false,
                test_allocate_error,
            )
        });
        let nested_struct_field = ok_or_panic(unsafe {
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("nested_struct"),
                [final_array_field].as_ptr(),
                1,
                true,
                test_allocate_error,
            )
        });

        let inner_struct_fields = vec![deep_maps_field, variant_data_field, nested_struct_field];
        let inner_struct_type = ok_or_panic(unsafe {
            visit_data_type_struct(
                &mut state,
                inner_struct_fields.as_ptr(),
                inner_struct_fields.len(),
                test_allocate_error,
            )
        });

        // 2b: struct field for inner_arrays: array<struct<deep_maps: ..., variant_data: ..., nested_struct: ...>>
        let inner_arrays_field = ok_or_panic(unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("inner_arrays"),
                inner_struct_type, // Pass the struct type as the array element type
                true,
                false,
                test_allocate_error,
            )
        });
        let middle_struct_type = ok_or_panic(unsafe {
            visit_data_type_struct(&mut state, &inner_arrays_field, 1, test_allocate_error)
        });

        // 2a: Complex key for outer map: struct<key_id: long>
        let key_id_field = ok_or_panic(unsafe {
            visit_field_long(
                &mut state,
                KernelStringSlice::new_unsafe("key_id"),
                false,
                test_allocate_error,
            )
        });
        let outer_map_key_type = ok_or_panic(unsafe {
            visit_data_type_struct(&mut state, &key_id_field, 1, test_allocate_error)
        });

        // 2: map<struct<key_id: long>, struct<inner_arrays: ...>>
        let outer_map_type = ok_or_panic(visit_data_type_map(
            &mut state,
            outer_map_key_type,
            middle_struct_type,
            true,
            test_allocate_error,
        ));

        // Final column: col_nested: array<map<struct<key_id: long>, struct<...>>>
        let col_nested = ok_or_panic(unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("col_nested"),
                outer_map_type, // Pass the map type directly as the array element type
                false,
                true,
                test_allocate_error,
            )
        });

        // Build final schema
        let schema_id = ok_or_panic(unsafe {
            visit_data_type_struct(&mut state, &col_nested, 1, test_allocate_error)
        });

        // Verify the deeply nested structure step by step
        let schema = unwrap_kernel_schema(&mut state, schema_id).unwrap();
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

    // TODO: manndp review by hand (vibe-coded).
    #[test]
    fn test_nullability_combinations() {
        let mut state = KernelSchemaVisitorState::default();

        // Test all the tricky nullability cases:
        // - Field-level nullability vs element/value-level nullability
        // - Required fields vs nullable fields
        // - Nullable collections with non-null elements
        // - Non-null collections with nullable elements
        // - Mixed nullability in nested structures
        //
        // Schema:
        // struct<
        //   col_required_string: string NOT NULL,
        //   col_nullable_string: string NULL,
        //   col_nullable_array_non_null_elements: array<string> NULL (elements NOT NULL),
        //   col_non_null_array_nullable_elements: array<string> NOT NULL (elements NULL),
        //   col_nullable_map_nullable_values: map<string, integer> NULL (values NULL),
        //   col_non_null_map_non_null_values: map<string, integer> NOT NULL (values NOT NULL),
        //   col_nullable_struct: struct<inner: string> NULL,
        //   col_non_null_struct_nullable_field: struct<inner: string NULL> NOT NULL
        // >

        // Required string field
        let col_required_string = ok_or_panic(unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("col_required_string"),
                false,
                test_allocate_error,
            )
        });

        // Nullable string field
        let col_nullable_string = ok_or_panic(unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("col_nullable_string"),
                true,
                test_allocate_error,
            )
        });

        // Nullable array with non-null elements: array<string> NULL (elements NOT NULL)
        let string_type_for_nullable_array =
            ok_or_panic(visit_data_type_string(&mut state, test_allocate_error));
        let col_nullable_array_non_null_elements = ok_or_panic(unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("col_nullable_array_non_null_elements"),
                string_type_for_nullable_array,
                false, // contains_null = false (elements NOT NULL)
                true,  // field_nullable = true (array itself NULL)
                test_allocate_error,
            )
        });

        // Non-null array with nullable elements: array<string> NOT NULL (elements NULL)
        let string_type_for_non_null_array =
            ok_or_panic(visit_data_type_string(&mut state, test_allocate_error));
        let col_non_null_array_nullable_elements = ok_or_panic(unsafe {
            visit_field_array(
                &mut state,
                KernelStringSlice::new_unsafe("col_non_null_array_nullable_elements"),
                string_type_for_non_null_array,
                true,  // contains_null = true (elements NULL)
                false, // field_nullable = false (array itself NOT NULL)
                test_allocate_error,
            )
        });

        // Nullable map with nullable values: map<string, integer> NULL (values NULL)
        let string_key_type_nullable_map =
            ok_or_panic(visit_data_type_string(&mut state, test_allocate_error));
        let integer_value_type_nullable_map =
            ok_or_panic(visit_data_type_integer(&mut state, test_allocate_error));
        let col_nullable_map_nullable_values = ok_or_panic(unsafe {
            visit_field_map(
                &mut state,
                KernelStringSlice::new_unsafe("col_nullable_map_nullable_values"),
                string_key_type_nullable_map,
                integer_value_type_nullable_map,
                true, // value_contains_null = true (values NULL)
                true, // field_nullable = true (map itself NULL)
                test_allocate_error,
            )
        });

        // Non-null map with non-null values: map<string, integer> NOT NULL (values NOT NULL)
        let string_key_type_non_null_map =
            ok_or_panic(visit_data_type_string(&mut state, test_allocate_error));
        let integer_value_type_non_null_map =
            ok_or_panic(visit_data_type_integer(&mut state, test_allocate_error));
        let col_non_null_map_non_null_values = ok_or_panic(unsafe {
            visit_field_map(
                &mut state,
                KernelStringSlice::new_unsafe("col_non_null_map_non_null_values"),
                string_key_type_non_null_map,
                integer_value_type_non_null_map,
                false, // value_contains_null = false (values NOT NULL)
                false, // field_nullable = false (map itself NOT NULL)
                test_allocate_error,
            )
        });

        // Nullable struct: struct<inner: string> NULL
        let struct_inner_field_nullable_struct = ok_or_panic(unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("inner"),
                false,
                test_allocate_error,
            )
        });
        let col_nullable_struct = ok_or_panic(unsafe {
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("col_nullable_struct"),
                &struct_inner_field_nullable_struct,
                1,
                true, // field_nullable = true (struct itself NULL)
                test_allocate_error,
            )
        });

        // Non-null struct with nullable field: struct<inner: string NULL> NOT NULL
        let struct_nullable_inner_field = ok_or_panic(unsafe {
            visit_field_string(
                &mut state,
                KernelStringSlice::new_unsafe("inner"),
                true, // inner field is nullable
                test_allocate_error,
            )
        });
        let col_non_null_struct_nullable_field = ok_or_panic(unsafe {
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("col_non_null_struct_nullable_field"),
                &struct_nullable_inner_field,
                1,
                false, // field_nullable = false (struct itself NOT NULL)
                test_allocate_error,
            )
        });

        // Build final schema
        let all_columns = vec![
            col_required_string,
            col_nullable_string,
            col_nullable_array_non_null_elements,
            col_non_null_array_nullable_elements,
            col_nullable_map_nullable_values,
            col_non_null_map_non_null_values,
            col_nullable_struct,
            col_non_null_struct_nullable_field,
        ];
        let schema_id = ok_or_panic(unsafe {
            visit_data_type_struct(
                &mut state,
                all_columns.as_ptr(),
                all_columns.len(),
                test_allocate_error,
            )
        });

        // Verify nullability settings
        let schema = unwrap_kernel_schema(&mut state, schema_id).unwrap();
        let fields: Vec<_> = schema.fields().collect();
        assert_eq!(fields.len(), 8);

        // Required string
        assert_eq!(fields[0].name(), "col_required_string");
        assert!(!fields[0].is_nullable());

        // Nullable string
        assert_eq!(fields[1].name(), "col_nullable_string");
        assert!(fields[1].is_nullable());

        // Nullable array with non-null elements
        assert_eq!(fields[2].name(), "col_nullable_array_non_null_elements");
        assert!(fields[2].is_nullable()); // Array field itself is nullable
        let DataType::Array(array_type_nullable_field) = fields[2].data_type() else {
            panic!("Expected array type");
        };
        assert!(!array_type_nullable_field.contains_null()); // But elements are not nullable

        // Non-null array with nullable elements
        assert_eq!(fields[3].name(), "col_non_null_array_nullable_elements");
        assert!(!fields[3].is_nullable()); // Array field itself is not nullable
        let DataType::Array(array_type_non_null_field) = fields[3].data_type() else {
            panic!("Expected array type");
        };
        assert!(array_type_non_null_field.contains_null()); // But elements are nullable

        // Nullable map with nullable values
        assert_eq!(fields[4].name(), "col_nullable_map_nullable_values");
        assert!(fields[4].is_nullable()); // Map field itself is nullable
        let DataType::Map(map_type_nullable_field) = fields[4].data_type() else {
            panic!("Expected map type");
        };
        assert!(map_type_nullable_field.value_contains_null()); // Values are nullable

        // Non-null map with non-null values
        assert_eq!(fields[5].name(), "col_non_null_map_non_null_values");
        assert!(!fields[5].is_nullable()); // Map field itself is not nullable
        let DataType::Map(map_type_non_null_field) = fields[5].data_type() else {
            panic!("Expected map type");
        };
        assert!(!map_type_non_null_field.value_contains_null()); // Values are not nullable

        // Nullable struct
        assert_eq!(fields[6].name(), "col_nullable_struct");
        assert!(fields[6].is_nullable()); // Struct field itself is nullable

        // Non-null struct with nullable inner field
        assert_eq!(fields[7].name(), "col_non_null_struct_nullable_field");
        assert!(!fields[7].is_nullable()); // Struct field itself is not nullable
        let DataType::Struct(struct_type_non_null_field) = fields[7].data_type() else {
            panic!("Expected struct type");
        };
        let inner_fields: Vec<_> = struct_type_non_null_field.fields().collect();
        assert_eq!(inner_fields.len(), 1);
        assert_eq!(inner_fields[0].name(), "inner");
        assert!(inner_fields[0].is_nullable()); // But inner field is nullable

        // Success! This proves that nullability works correctly at all levels:
        // - Field-level nullability is independent of element/value nullability
        // - Arrays can be nullable with non-null elements or vice versa
        // - Maps can be nullable with non-null values or vice versa
        // - Structs can be nullable with non-null fields or vice versa
        // - Nullability propagates correctly through nested structures
    }
}
