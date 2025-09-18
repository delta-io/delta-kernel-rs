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
//! schema and anonymous struct types, and `visit_schema_struct` for named struct fields.
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

    if state.elements.is_empty() {
        return None;
    }

    Some(*struct_type)
}

// =============================================================================
// FFI Visitor Functions for field creation - Primitive Types
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
                visit_schema_primitive_impl(state, name_str, $primitive_type, nullable)
                    .into_extern_result(&allocate_error)
            }
        )*
    };
}

generate_primitive_schema_visitors! {
    (visit_schema_string, PrimitiveType::String, "Visit a string field. Strings in Delta Lake can hold arbitrary UTF-8 text data."),
    (visit_schema_long, PrimitiveType::Long, "Visit a long field. Long fields store 64-bit signed integers."),
    (visit_schema_integer, PrimitiveType::Integer, "Visit an integer field. Integer fields store 32-bit signed integers."),
    (visit_schema_short, PrimitiveType::Short, "Visit a short field. Short fields store 16-bit signed integers."),
    (visit_schema_byte, PrimitiveType::Byte, "Visit a byte field. Byte fields store 8-bit signed integers."),
    (visit_schema_float, PrimitiveType::Float, "Visit a float field. Float fields store 32-bit floating point numbers."),
    (visit_schema_double, PrimitiveType::Double, "Visit a double field. Double fields store 64-bit floating point numbers."),
    (visit_schema_boolean, PrimitiveType::Boolean, "Visit a boolean field. Boolean fields store true/false values."),
    (visit_schema_binary, PrimitiveType::Binary, "Visit a binary field. Binary fields store arbitrary byte arrays."),
    (visit_schema_date, PrimitiveType::Date, "Visit a date field. Date fields store calendar dates without time information."),
    (visit_schema_timestamp, PrimitiveType::Timestamp, "Visit a timestamp field. Timestamp fields store date and time with microsecond precision in UTC."),
    (visit_schema_timestamp_ntz, PrimitiveType::TimestampNtz, "Visit a timestamp_ntz field. Similar to timestamp but without timezone information."),
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
/// `visit_struct_type` which creates anonymous struct DataTypes.
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
    let name_str: Result<&str, Error> = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let field_ids = unsafe { std::slice::from_raw_parts(field_ids, field_count) };

    visit_schema_struct_impl(state, name_str, field_ids, nullable)
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

fn visit_schema_struct_impl(
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

fn visit_schema_array_impl(
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
    visit_schema_map_impl(
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

fn visit_schema_map_impl(
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
    (visit_string_type, PrimitiveType::String, "Create a string DataType for text data in complex types."),
    (visit_long_type, PrimitiveType::Long, "Create a long DataType for 64-bit integers in complex types."),
    (visit_integer_type, PrimitiveType::Integer, "Create an integer DataType for 32-bit integers in complex types."),
    (visit_short_type, PrimitiveType::Short, "Create a short DataType for 16-bit integers in complex types."),
    (visit_byte_type, PrimitiveType::Byte, "Create a byte DataType for 8-bit integers in complex types."),
    (visit_float_type, PrimitiveType::Float, "Create a float DataType for 32-bit floating point numbers in complex types."),
    (visit_double_type, PrimitiveType::Double, "Create a double DataType for 64-bit floating point numbers in complex types."),
    (visit_boolean_type, PrimitiveType::Boolean, "Create a boolean DataType for true/false values in complex types."),
    (visit_binary_type, PrimitiveType::Binary, "Create a binary DataType for byte arrays in complex types."),
    (visit_date_type, PrimitiveType::Date, "Create a date DataType for calendar dates in complex types."),
    (visit_timestamp_type, PrimitiveType::Timestamp, "Create a timestamp DataType for timestamps with timezone in complex types."),
    (visit_timestamp_ntz_type, PrimitiveType::TimestampNtz, "Create a timestamp_ntz DataType for timestamps without timezone in complex types."),
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
pub extern "C" fn visit_decimal_data_type(
    state: &mut KernelSchemaVisitorState,
    precision: u8,
    scale: u8,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    unsafe {
        visit_decimal_data_type_impl(state, precision, scale).into_extern_result(&allocate_error)
    }
}

fn visit_decimal_data_type_impl(
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
/// top-level fields with `visit_schema_*` functions, then call this function with
/// those field IDs to build the root schema struct.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `field_ids` array pointing
/// to valid field IDs previously returned by `visit_schema_*` calls, and
/// `allocate_error` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_struct_data_type(
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
/// The `element_type_id` must reference a DataType created by a previous `visit_*_type` call.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `element_type_id` from
/// previous `visit_*_type` call, and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_array_data_type(
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
/// Both `key_type_id` and `value_type_id` must reference DataTypes created by previous `visit_*_type` calls.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `key_type_id` and `value_type_id`
/// from previous `visit_*_type` calls, and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_map_data_type(
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
/// The `struct_type_id` must reference a struct DataType created by a previous `visit_struct_type` call.
///
/// # Safety
///
/// Caller is responsible for providing valid `state`, `struct_type_id` from
/// previous `visit_struct_type` call, and `allocate_error` function pointer.
#[no_mangle]
pub extern "C" fn visit_variant_data_type(
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
