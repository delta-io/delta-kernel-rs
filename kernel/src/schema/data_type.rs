use serde::de::{self, IntoDeserializer};
use serde::{Deserialize, Deserializer};

#[cfg(feature = "udt-in-dev")]
use super::UserDefinedType;
use super::{
    ArrayType, DataType, FILE_FIELDS, MapType, PrimitiveType, StructType, VARIANT_FIELDS,
    unsupported_delta_type_error,
};
use crate::memory::{JsonValue, Owned};

fn boxed<T, E: de::Error>(value: T) -> Result<Owned<T>, E> {
    Owned::try_new(value).map_err(|error| {
        crate::memory::record_failure(error);
        E::custom(error)
    })
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<DataType, D::Error> {
    let value = JsonValue::deserialize(deserializer)?;
    if let JsonValue::String(name) = &value {
        return match name.as_str() {
            "variant" => {
                boxed(VARIANT_FIELDS.try_clone().map_err(kernel_error)?).map(DataType::Variant)
            }
            "file" => boxed(FILE_FIELDS.try_clone().map_err(kernel_error)?).map(DataType::File),
            name => PrimitiveType::deserialize(de::value::BorrowedStrDeserializer::<
                de::value::Error,
            >::new(name))
            .map(DataType::Primitive)
            .map_err(de::Error::custom),
        };
    }
    if let Some(JsonValue::String(name)) = value.member("type") {
        let input = serde_stacker::Deserializer::new((&value).into_deserializer());
        return match name.as_str() {
            "array" => ArrayType::deserialize(input)
                .map_err(de::Error::custom)
                .and_then(boxed)
                .map(DataType::Array),
            "struct" => StructType::deserialize(input)
                .map_err(de::Error::custom)
                .and_then(boxed)
                .map(DataType::Struct),
            "map" => MapType::deserialize(input)
                .map_err(de::Error::custom)
                .and_then(boxed)
                .map(DataType::Map),
            #[cfg(feature = "udt-in-dev")]
            "udt" => UserDefinedType::deserialize(input)
                .map(DataType::UserDefined)
                .map_err(de::Error::custom),
            _ => Err(unsupported_delta_type_error(name.as_str())),
        };
    }
    Err(de::Error::custom("Invalid data type"))
}

fn kernel_error<E: de::Error>(error: crate::KernelError) -> E {
    if let crate::KernelError::Memory(failure) = error.without_backtrace() {
        crate::memory::record_failure(*failure);
    }
    E::custom(error)
}
