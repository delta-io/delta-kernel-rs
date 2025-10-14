//! Utility traits that support the [`delta_kernel_derive::ToSchema`] macro.
///
/// Not intended for use by normal code.
use std::collections::{HashMap, HashSet};

use crate::schema::{ArrayType, DataType, MapType, StructField, ToSchema};

use delta_kernel_derive::internal_api;

/// Converts a type to a [`DataType`]. Implemented for the primitive types and automatically derived
/// for all types that implement [`ToSchema`].
#[internal_api]
pub(crate) trait ToDataType {
    fn to_data_type() -> DataType;
}

// Blanket impl for all types that implement `ToSchema`
impl<T: ToSchema> ToDataType for T {
    fn to_data_type() -> DataType {
        T::to_schema().into()
    }
}

// Helper macro to implement `ToDataType` for primitive types
macro_rules! impl_to_data_type {
    ( $(($rust_type: ty, $data_type: expr)), * ) => {
        $(
            impl ToDataType for $rust_type {
                fn to_data_type() -> DataType {
                    $data_type
                }
            }
        )*
    };
}

impl_to_data_type!(
    (String, DataType::STRING),
    (i64, DataType::LONG),
    (i32, DataType::INTEGER),
    (i16, DataType::SHORT),
    (char, DataType::BYTE),
    (f32, DataType::FLOAT),
    (f64, DataType::DOUBLE),
    (bool, DataType::BOOLEAN)
);

// ToDataType impl for non-nullable array types
impl<T: ToDataType> ToDataType for Vec<T> {
    fn to_data_type() -> DataType {
        ArrayType::new(T::to_data_type(), false).into()
    }
}

// ToDataType impl for non-nullable set types
impl<T: ToDataType> ToDataType for HashSet<T> {
    fn to_data_type() -> DataType {
        ArrayType::new(T::to_data_type(), false).into()
    }
}

// ToDataType impl for non-nullable map types
impl<K: ToDataType, V: ToDataType> ToDataType for HashMap<K, V> {
    fn to_data_type() -> DataType {
        MapType::new(K::to_data_type(), V::to_data_type(), false).into()
    }
}

// ToDataType impl for maps with nullable values
impl<K: ToDataType, V: ToDataType> ToDataType for HashMap<K, Option<V>> {
    fn to_data_type() -> DataType {
        MapType::new(K::to_data_type(), V::to_data_type(), true).into()
    }
}

/// The [`delta_kernel_derive::ToSchema`] macro uses this to convert a struct field's name + type
/// into a `StructField` definition. A blanket impl for `Option<T: ToDataType>` supports nullable
/// struct fields, which otherwise default to non-nullable.
#[internal_api]
pub(crate) trait GetStructField {
    fn get_struct_field(name: impl Into<String>) -> StructField;
}

// Normal types produce non-nullable fields
impl<T: ToDataType> GetStructField for T {
    fn get_struct_field(name: impl Into<String>) -> StructField {
        StructField::not_null(name, T::to_data_type())
    }
}

// Option types produce nullable fields
impl<T: ToDataType> GetStructField for Option<T> {
    fn get_struct_field(name: impl Into<String>) -> StructField {
        StructField::nullable(name, T::to_data_type())
    }
}

/// The [`delta_kernel_derive::ToSchema`] macro uses this trait to implement the
/// `allow_null_container_values` attribute. It is similar to [`ToDataType`], except the containers
/// it produces have nullable elements, e.g. [`MapType::value_contains_null`] is true.
pub(crate) trait ToNullableContainerType {
    fn to_nullable_container_type() -> DataType;
}

// Blanket impl for maps with nullable values
impl<K: ToDataType, V: ToDataType> ToNullableContainerType for HashMap<K, V> {
    fn to_nullable_container_type() -> DataType {
        MapType::new(K::to_data_type(), V::to_data_type(), true).into()
    }
}

// The [`delta_kernel_derive::ToSchema`] macro uses this to convert a struct field's name + type
// into a `StructField` definition for a container with nullable values, when the struct field was
// annotated with the `allow_null_container_values` attribute.
#[internal_api]
pub(crate) trait GetNullableContainerStructField {
    fn get_nullable_container_struct_field(name: impl Into<String>) -> StructField;
}

// Blanket impl for all container types with nullable values
impl<T: ToNullableContainerType> GetNullableContainerStructField for T {
    fn get_nullable_container_struct_field(name: impl Into<String>) -> StructField {
        StructField::not_null(name, T::to_nullable_container_type())
    }
}
