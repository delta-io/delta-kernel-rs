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
use tracing::warn;

#[derive(Default)]
pub struct KernelSchemaVisitorState {
    elements: ReferenceSet<StructField>,
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

    if let DataType::Struct(struct_type) = schema_element.data_type {
        if !state.elements.is_empty() {
            warn!("Didn't consume all visited fields, schema is invalid.");
            return None;
        }
        Some(*struct_type)
    } else {
        warn!("Final returned id was not a struct, schema is invalid");
        None
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
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let field = StructField::new(name_str, DataType::Primitive(primitive_type), nullable);
    Ok(wrap_field(state, field))
}

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
    (visit_field_string, PrimitiveType::String, "Visit a string field. Strings can hold arbitrary UTF-8 text data."),
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

    let struct_type = StructType::try_new(field_vec)?;
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
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_field_array_impl(state, name_str, element_type_id, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_field_array_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    element_type_id: usize,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();
    let element_field = unwrap_field(state, element_type_id).ok_or_else(|| {
        Error::generic(format!(
            "Invalid element type ID {element_type_id} for array"
        ))
    })?;

    let array_type = ArrayType::new(element_field.data_type, element_field.nullable);
    let field = StructField::new(name_str, array_type, nullable);
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
    nullable: bool,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name_str = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_field_map_impl(state, name_str, key_type_id, value_type_id, nullable)
        .into_extern_result(&allocate_error)
}

fn visit_field_map_impl(
    state: &mut KernelSchemaVisitorState,
    name: DeltaResult<&str>,
    key_type_id: usize,
    value_type_id: usize,
    nullable: bool,
) -> DeltaResult<usize> {
    let name_str = name?.to_string();

    let key_field = unwrap_field(state, key_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid key type ID {key_type_id} for map")))?;

    let value_field = unwrap_field(state, value_type_id)
        .ok_or_else(|| Error::generic(format!("Invalid value type ID {value_type_id} for map")))?;

    let map_type = MapType::new(
        key_field.data_type,
        value_field.data_type,
        value_field.nullable,
    );
    let field = StructField::new(name_str, map_type, nullable);
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
    let Some(DataType::Struct(variant_struct)) =
        state.elements.take(struct_type_id).map(|f| f.data_type)
    else {
        return Err(Error::generic(format!(
            "Invalid variant struct ID {} - must be DataType::Struct",
            struct_type_id
        )));
    };
    Ok(DataType::Variant(variant_struct))
}

// // =============================================================================
// // FFI Visitor Functions for data type creation - Primitive Types
// // =============================================================================

// /// Generic helper to create primitive types
// fn visit_primitive_data_type_impl(
//     state: &mut KernelSchemaVisitorState,
//     primitive_type: PrimitiveType,
// ) -> DeltaResult<usize> {
//     let data_type = DataType::Primitive(primitive_type);
//     Ok(wrap_data_type(state, data_type))
// }

// // Macro to generate primitive DataType visitor functions
// macro_rules! generate_primitive_type_visitors {
//     ($(($fn_name:ident, $primitive_type:expr, $doc:expr)),* $(,)?) => {
//         $(
//             #[doc = $doc]
//             #[no_mangle]
//             pub extern "C" fn $fn_name(
//                 state: &mut KernelSchemaVisitorState,
//                 allocate_error: AllocateErrorFn,
//             ) -> ExternResult<usize> {
//                 unsafe {
//                     visit_primitive_data_type_impl(state, $primitive_type)
//                         .into_extern_result(&allocate_error)
//                 }
//             }
//         )*
//     };
// }

// // Generate all primitive DataType visitor functions (except decimal which has different signature)
// generate_primitive_type_visitors! {
//     (visit_data_type_string, PrimitiveType::String, "Create a string DataType for text data in complex types."),
//     (visit_data_type_long, PrimitiveType::Long, "Create a long DataType for 64-bit integers in complex types."),
//     (visit_data_type_integer, PrimitiveType::Integer, "Create an integer DataType for 32-bit integers in complex types."),
//     (visit_data_type_short, PrimitiveType::Short, "Create a short DataType for 16-bit integers in complex types."),
//     (visit_data_type_byte, PrimitiveType::Byte, "Create a byte DataType for 8-bit integers in complex types."),
//     (visit_data_type_float, PrimitiveType::Float, "Create a float DataType for 32-bit floating point numbers in complex types."),
//     (visit_data_type_double, PrimitiveType::Double, "Create a double DataType for 64-bit floating point numbers in complex types."),
//     (visit_data_type_boolean, PrimitiveType::Boolean, "Create a boolean DataType for true/false values in complex types."),
//     (visit_data_type_binary, PrimitiveType::Binary, "Create a binary DataType for byte arrays in complex types."),
//     (visit_data_type_date, PrimitiveType::Date, "Create a date DataType for calendar dates in complex types."),
//     (visit_data_type_timestamp, PrimitiveType::Timestamp, "Create a timestamp DataType for timestamps with timezone in complex types."),
//     (visit_data_type_timestamp_ntz, PrimitiveType::TimestampNtz, "Create a timestamp_ntz DataType for timestamps without timezone in complex types."),
// }

// // =============================================================================
// // FFI Visitor Functions for data type creation - Complex Types
// // =============================================================================

// /// Visit a decimal DataType with specified precision and scale for use in complex types.
// ///
// /// # Safety
// ///
// /// Caller is responsible for providing a valid `state`, `precision` and `scale`,
// /// and `allocate_error` function pointer.
// #[no_mangle]
// pub extern "C" fn visit_data_type_decimal(
//     state: &mut KernelSchemaVisitorState,
//     precision: u8,
//     scale: u8,
//     allocate_error: AllocateErrorFn,
// ) -> ExternResult<usize> {
//     unsafe {
//         visit_data_type_decimal_impl(state, precision, scale).into_extern_result(&allocate_error)
//     }
// }

// fn visit_data_type_decimal_impl(
//     state: &mut KernelSchemaVisitorState,
//     precision: u8,
//     scale: u8,
// ) -> DeltaResult<usize> {
//     let decimal_type = DecimalType::try_new(precision, scale)
//         .map_err(|e| Error::generic(format!("Invalid decimal type precision/scale: {}", e)))?;
//     let data_type = DataType::Primitive(PrimitiveType::Decimal(decimal_type));
//     Ok(wrap_data_type(state, data_type))
// }

// /// Create a struct DataType from field IDs for use in complex types.
// /// This creates an anonymous struct type that can be used as array elements,
// /// map values, or nested within other structs.
// ///
// /// Note: This is also the final step for building the root schema. Create all your
// /// top-level fields with `visit_field_*` functions, then call this function with
// /// those field IDs to build the root schema struct.
// ///
// /// # Safety
// ///
// /// Caller is responsible for providing valid `state`, `field_ids` array pointing
// /// to valid field IDs previously returned by `visit_field_*` calls, and
// /// `allocate_error` function pointer.
// #[no_mangle]
// pub unsafe extern "C" fn visit_data_type_struct(
//     state: &mut KernelSchemaVisitorState,
//     field_ids: *const usize,
//     field_count: usize,
//     allocate_error: AllocateErrorFn,
// ) -> ExternResult<usize> {
//     let field_slice = unsafe { std::slice::from_raw_parts(field_ids, field_count) };
//     let result = create_struct_data_type(state, field_slice)
//         .map(|data_type| wrap_data_type(state, data_type));
//     result.into_extern_result(&allocate_error)
// }

// /// Create an array DataType for use in complex types.
// /// This creates an anonymous array type that can be used as struct field types,
// /// map values, or nested within other arrays.
// ///
// /// The `element_type_id` must reference a DataType created by a previous `visit_data_type_*` call.
// ///
// /// # Safety
// ///
// /// Caller is responsible for providing valid `state`, `element_type_id` from
// /// previous `visit_data_type_*` call, and `allocate_error` function pointer.
// #[no_mangle]
// pub extern "C" fn visit_data_type_array(
//     state: &mut KernelSchemaVisitorState,
//     element_type_id: usize,
//     contains_null: bool,
//     allocate_error: AllocateErrorFn,
// ) -> ExternResult<usize> {
//     unsafe {
//         let result = create_array_data_type(state, element_type_id, contains_null)
//             .map(|data_type| wrap_data_type(state, data_type));
//         result.into_extern_result(&allocate_error)
//     }
// }

// /// Create a map DataType for use in complex types.
// /// This creates an anonymous map type that can be used as struct field types,
// /// array elements, or nested within other maps.
// ///
// /// Both `key_type_id` and `value_type_id` must reference DataTypes created by previous `visit_data_type_*` calls.
// ///
// /// # Safety
// ///
// /// Caller is responsible for providing valid `state`, `key_type_id` and `value_type_id`
// /// from previous `visit_data_type_*` calls, and `allocate_error` function pointer.
// #[no_mangle]
// pub extern "C" fn visit_data_type_map(
//     state: &mut KernelSchemaVisitorState,
//     key_type_id: usize,
//     value_type_id: usize,
//     value_contains_null: bool,
//     allocate_error: AllocateErrorFn,
// ) -> ExternResult<usize> {
//     unsafe {
//         let result = create_map_data_type(state, key_type_id, value_type_id, value_contains_null)
//             .map(|data_type| wrap_data_type(state, data_type));
//         result.into_extern_result(&allocate_error)
//     }
// }

// /// Create a variant DataType for use in complex types.
// /// This creates an anonymous variant type that can be used as struct field types,
// /// array elements, or map values.
// ///
// /// The `struct_type_id` must reference a struct DataType created by a previous `visit_data_type_struct` call.
// ///
// /// # Safety
// ///
// /// Caller is responsible for providing valid `state`, `struct_type_id` from
// /// previous `visit_data_type_struct` call, and `allocate_error` function pointer.
// #[no_mangle]
// pub extern "C" fn visit_data_type_variant(
//     state: &mut KernelSchemaVisitorState,
//     struct_type_id: usize,
//     allocate_error: AllocateErrorFn,
// ) -> ExternResult<usize> {
//     unsafe {
//         let result = create_variant_data_type(state, struct_type_id)
//             .map(|data_type| wrap_data_type(state, data_type));
//         result.into_extern_result(&allocate_error)
//     }
// }

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

    macro_rules! visit_field {
        ($type:ident, $state:ident, $name:expr, $nullable:tt) => {
            paste::paste! { ok_or_panic(unsafe {
                [<visit_field_ $type>](
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    $nullable,
                    test_allocate_error,
                )
            }) }
        };

        ($type:ident, $state:ident, $name:expr, $arg1:expr, $nullable:tt) => {
            paste::paste! { ok_or_panic(#[allow(unused_unsafe)] unsafe {
                let arg1 = $arg1;
                [<visit_field_ $type>](
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    arg1,
                    $nullable,
                    test_allocate_error,
                )
            }) }
        };

        ($type:ident, $state:ident, $name:expr, $arg1:expr, $arg2:expr, $nullable:tt) => {
            paste::paste! { ok_or_panic(#[allow(unused_unsafe)] unsafe {
                let arg1 = $arg1;
                let arg2 = $arg2;
                [<visit_field_ $type>](
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    arg1,
                    arg2,
                    $nullable,
                    test_allocate_error,
                )
            }) }
        };
    }

    macro_rules! visit_array_field {
        ($state:ident, $name:expr, $nullable:tt, $elem_field:expr) => {{
            let ef = $elem_field;
            ok_or_panic(unsafe {
                visit_field_array(
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    ef,
                    $nullable,
                    test_allocate_error,
                )
            }) 
        }};
    }

    macro_rules! visit_map_field {
        ($state:ident, $name:expr, $nullable:tt, $key_field:expr, $val_field:expr) => {{
            let kf = $key_field;
            let vf = $val_field;
            ok_or_panic(unsafe {
                visit_field_map(
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    kf,
                    vf,
                    $nullable,
                    test_allocate_error,
                )
            }) 
        }};
    }

    macro_rules! visit_struct_field {
        ($state:ident, $name:expr, $nullable:tt, $($fields:expr),* $(,)?) => {{
            let fields = vec![$($fields),*];
            let field_count = fields.len();
            ok_or_panic(unsafe {
                visit_field_struct(
                    &mut $state,
                    KernelStringSlice::new_unsafe($name),
                    fields.as_ptr(),
                    field_count,
                    $nullable,
                    test_allocate_error,
                )
            })
        }};
    }

    macro_rules! visit_variant_field {
        ($state:ident, $name:expr, $nullable:tt) => {{
            visit_field!(
                variant,
                $state,
                $name,
                visit_struct_field!(
                    $state,
                    "variant",
                    false,
                    visit_field!(binary, $state, "metadata", false),
                    visit_field!(binary, $state, "value", false),
                ),
                false
            )
        }};
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
        let col_string = visit_field!(string, state, "col_string", false);
        let col_long = visit_field!(long, state, "col_long", false);
        let col_int = visit_field!(integer, state, "col_int", false);
        let col_short = visit_field!(short, state, "col_short", false);
        let col_byte = visit_field!(byte, state, "col_byte", false);
        let col_double = visit_field!(double, state, "col_double", false);
        let col_float = visit_field!(float, state, "col_float", false);
        let col_boolean = visit_field!(boolean, state, "col_boolean", false);
        let col_binary = visit_field!(binary, state, "col_binary", false);
        let col_date = visit_field!(date, state, "col_date", false);
        let col_timestamp = visit_field!(timestamp, state, "col_timestamp", false);
        let col_timestamp_ntz = visit_field!(timestamp_ntz, state, "col_timestamp_ntz", false);
        let col_decimal = visit_field!(decimal, state, "col_decimal", 10, 2, false);

        // Create array<string>
        let col_array = visit_array_field!(
            state,
            "col_array",
            false,
            visit_field!(string, state, "element", false)
        );

        // Create map<string, long>
        let col_map = visit_map_field!(
            state,
            "col_map",
            false,
            visit_field!(string, state, "key", false),
            visit_field!(long, state, "value", false)
        );

        // Create struct<inner_name: string>
        let col_struct = visit_struct_field!(
            state,
            "col_struct",
            false,
            visit_field!(string, state, "inner_name", false),
        );

        // Create variant<metadata: binary, value: binary>
        let col_variant = visit_variant_field!(
            state,
            "col_variant",
            false
        );

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
            visit_field_struct(
                &mut state,
                KernelStringSlice::new_unsafe("schema"),
                all_columns.as_ptr(),
                all_columns.len(),
                false,
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


        let schema_id = visit_struct_field!(
            state,
            "top_struct",
            false,
            visit_array_field!( // nested field in struct is an array
                state,
                "col_nested",
                true,
                visit_map_field!( // array element is a map
                    state,
                    "element",
                    false,
                    visit_struct_field!( // map key is a struct
                        state,
                        "key",
                        false,
                        visit_field!(long, state, "key_id", false),
                    ),
                    visit_struct_field!( // map value is a struct
                        state,
                        "value",
                        true,
                        visit_array_field!( // even more nested array
                            state,
                            "inner_arrays",
                            false,
                            visit_struct_field!( // inner array element is a struct
                                state,
                                "element",
                                true,
                                visit_map_field!( // struct field 1 is map
                                    state,
                                    "deep_maps",
                                    true,
                                    visit_variant_field!( // key is variant
                                        state,
                                        "key",
                                        false
                                    ),
                                    visit_array_field!( // value is an array
                                        state,
                                        "value",
                                        false,
                                        visit_field!( // array element is decimal
                                            decimal,
                                            state,
                                            "element",
                                            10,
                                            2,
                                            true
                                        )
                                    )
                                ),
                                visit_variant_field!( // struct field 2 is variant
                                    state,
                                    "variant_data",
                                    false
                                ),
                                visit_struct_field!( // struct field 3 is nested_struct
                                    state,
                                    "nested_struct",
                                    true,
                                    visit_array_field!(
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
                                                visit_field!(double, state, "coord", false),
                                            ),
                                            visit_field!(double, state, "value", false)
                                        )
                                    ),
                                ),
                            )
                        )
                    )
                )
            )
        );

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

    // // TODO: manndp review by hand (vibe-coded).
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
        let col_required_string = visit_field!(string, state, "col_required_string", false);
        let col_nullable_string = visit_field!(string, state, "col_nullable_string", true);


        // Nullable array with non-null elements: array<string> NULL (elements NOT NULL)
        let col_nullable_array_non_null_elements = visit_array_field!(
            state,
            "col_nullable_array_non_null_elements",
            true, // array can be null
            visit_field!(string, state, "element", false) // elements cannot be null
        );

        // Non-null array with nullable elements: array<string> NOT NULL (elements NULL)
        let col_non_null_array_nullable_elements = visit_array_field!(
            state,
            "col_non_null_array_nullable_elements",
            false, // array not null
            visit_field!(string, state, "element", true) // elements can be null
        );

        // Nullable map with nullable values: map<string, integer> NULL (values NULL)
        let col_nullable_map_nullable_values = visit_map_field!(
            state,
            "col_nullable_map_nullable_values",
            true, // map can be null
            visit_field!(string, state, "key", false),
            visit_field!(integer, state, "value", true) // values can be null
        );

        // Non-null map with non-null values: map<string, integer> NOT NULL (values NOT NULL)
        let col_non_null_map_non_null_values = visit_map_field!(
            state,
            "col_non_null_map_non_null_values",
            false, // map cannot be null
            visit_field!(string, state, "key", false),
            visit_field!(integer, state, "value", false) // values cannot be null
        );

        let col_nullable_struct = visit_struct_field!(
            state,
            "col_nullable_struct",
            true, // struct is nullable
            visit_field!(string, state, "inner", false), // inner is not nullable
        );

        // Non-null struct with nullable field: struct<inner: string NULL> NOT NULL
        let col_non_null_struct_nullable_field = visit_struct_field!(
            state,
            "col_non_null_struct_nullable_field",
            false, // struct not null
            visit_field!(string, state, "inner", true), // inner is nullable
        );

        // Build final schema
        let schema_id = visit_struct_field!(
            state,
            "top_struct",
            false,
            col_required_string,
            col_nullable_string,
            col_nullable_array_non_null_elements,
            col_non_null_array_nullable_elements,
            col_nullable_map_nullable_values,
            col_non_null_map_non_null_values,
            col_nullable_struct,
            col_non_null_struct_nullable_field,
        );

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
