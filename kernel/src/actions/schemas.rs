//! Schema definitions for action types

use std::collections::{HashMap, HashSet};

use crate::schema::{ArrayType, DataType, MapType, StructField};

pub(crate) trait ToDataType {
    fn to_data_type() -> DataType;
}

macro_rules! impl_to_data_type {
    ( $(($rust_type: ty, $kernel_type: expr)), * ) => {
        $(
            impl ToDataType for $rust_type {
                fn to_data_type() -> DataType {
                    $kernel_type
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

pub(crate) trait GetStructField {
    fn get_struct_field(name: impl Into<String>) -> StructField;
}

// Normal types produce non-nullable fields
impl<T: ToDataType> GetStructField for T {
    fn get_struct_field(name: impl Into<String>) -> StructField {
        StructField::new(name, T::to_data_type(), false)
    }
}

// Option types produce nullable fields
impl<T: ToDataType> GetStructField for Option<T> {
    fn get_struct_field(name: impl Into<String>) -> StructField {
        StructField::new(name, T::to_data_type(), true)
    }
}
