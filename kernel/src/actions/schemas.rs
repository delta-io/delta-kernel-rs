//! Schema definitions for action types

use std::collections::HashMap;

use crate::schema::{ArrayType, DataType, MapType, StructField};

/// A trait that allows getting a `StructField` based on the provided name and nullability
pub(crate) trait GetField {
    fn get_field(name: impl Into<String>) -> StructField;
}

macro_rules! impl_get_field {
    ( $(($rust_type: ty, $kernel_type: expr)), * ) => {
        $(
            impl GetField for $rust_type {
                fn get_field(name: impl Into<String>) -> StructField {
                    StructField::new(name, $kernel_type, false)
                }
            }
        )*
    };
}

impl_get_field!(
    (String, DataType::STRING),
    (i64, DataType::LONG),
    (i32, DataType::INTEGER),
    (i16, DataType::SHORT),
    (char, DataType::BYTE),
    (f32, DataType::FLOAT),
    (f64, DataType::DOUBLE),
    (bool, DataType::BOOLEAN),
    (HashMap<String, String>, MapType::new(DataType::STRING, DataType::STRING, false)),
    (HashMap<String, Option<String>>, MapType::new(DataType::STRING, DataType::STRING, true)),
    (Vec<String>, ArrayType::new(DataType::STRING, false))
);

impl<T: GetField> GetField for Option<T> {
    fn get_field(name: impl Into<String>) -> StructField {
        let mut inner = T::get_field(name);
        inner.nullable = true;
        inner
    }
}
