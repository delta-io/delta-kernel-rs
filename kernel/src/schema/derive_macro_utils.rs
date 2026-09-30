//! Utility traits that support the [`delta_kernel_derive::ToSchema`] macro.
///
/// Not intended for use by normal code.
use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use delta_kernel_derive::internal_api;

use crate::error::add_scalar_path_context;
use crate::expressions::{Scalar, StructData};
use crate::schema::{ArrayType, DataType, MapType, StructField, StructType, ToSchema};
use crate::utils::require;
use crate::{DeltaResult, KernelError};

/// Converts a type to a [`DataType`]. Implemented for the primitive types and automatically derived
/// for all types that implement [`ToSchema`].
///
/// # Warning
///
/// If a type implementing this trait also implements `Into<Scalar>`, then for every value `v`, the
/// scalar `s: Scalar = v.into()` **must** satisfy:
/// - `!s.is_null()`, and
/// - `s.data_type() == Self::to_data_type()`.
///
/// `IntoScalar` automatically marks every type with both impls, and infallible conversions like
/// `impl<T: IntoScalar> From<Vec<T>> for Scalar` rely on this contract without runtime validation.
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
    (Bytes, DataType::BINARY),
    (i64, DataType::LONG),
    (i32, DataType::INTEGER),
    (i16, DataType::SHORT),
    (i8, DataType::BYTE),
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

// ToDataType impl for arrays that may contain null elements
impl<T: ToDataType> ToDataType for Vec<Option<T>> {
    fn to_data_type() -> DataType {
        ArrayType::new(T::to_data_type(), true).into()
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

// Optional container types produce nullable fields with nullable values.
impl<T: ToNullableContainerType> GetNullableContainerStructField for Option<T> {
    fn get_nullable_container_struct_field(name: impl Into<String>) -> StructField {
        StructField::nullable(name, T::to_nullable_container_type())
    }
}

/// Named fields consumed by the [`delta_kernel_derive::TryFromStructData`] macro.
///
/// Field conversion errors acquire their path element as they unwind. Successful conversion does
/// not allocate or maintain path state.
#[internal_api]
pub(crate) struct StructDataFields {
    expected: StructType,
    fields: HashMap<String, (StructField, Scalar)>,
}

impl StructDataFields {
    pub(crate) fn try_new(data: StructData, expected: StructType) -> DeltaResult<Self> {
        let (actual_fields, values) = data.into_parts();
        require!(
            actual_fields.len() == values.len(),
            KernelError::scalar_conversion(
                format!("{} struct values", actual_fields.len()),
                format!("{} struct values", values.len()),
            )
        );
        require!(
            actual_fields.len() == expected.num_fields(),
            KernelError::scalar_conversion(
                format!("struct with {} fields", expected.num_fields()),
                format!("struct with {} fields", actual_fields.len()),
            )
        );
        let mut fields = HashMap::with_capacity(actual_fields.len());
        for (field, value) in actual_fields.into_iter().zip(values) {
            let name = field.name().clone();
            match fields.entry(name) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert((field, value));
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    return Err(add_scalar_path_context(
                        KernelError::scalar_conversion("one field", "duplicate fields"),
                        entry.key().clone(),
                    ));
                }
            }
        }
        Ok(Self { expected, fields })
    }

    pub(crate) fn take_field<T: TryFrom<Scalar, Error = KernelError>>(
        &mut self,
        field_name: &str,
    ) -> DeltaResult<T> {
        let expected = self.expected.field(field_name).ok_or_else(|| {
            KernelError::InternalError(format!(
                "Derived schema does not contain generated field {field_name:?}"
            ))
        })?;
        let (actual_field, value) = self.fields.remove(field_name).ok_or_else(|| {
            add_scalar_path_context(
                KernelError::scalar_conversion("present field", "missing field"),
                field_name,
            )
        })?;
        require!(
            actual_field.is_nullable() == expected.is_nullable(),
            add_scalar_path_context(
                KernelError::scalar_conversion(
                    if expected.is_nullable() {
                        "nullable field"
                    } else {
                        "non-nullable field"
                    },
                    if actual_field.is_nullable() {
                        "nullable field"
                    } else {
                        "non-nullable field"
                    },
                ),
                field_name,
            )
        );

        T::try_from(value).map_err(|error| add_scalar_path_context(error, field_name))
    }

    /// Verifies that every named field was consumed.
    pub(crate) fn finish(self) -> DeltaResult<()> {
        if self.fields.is_empty() {
            return Ok(());
        }
        let mut extra: Vec<_> = self.fields.keys().collect();
        extra.sort_unstable();
        Err(KernelError::scalar_conversion(
            "no additional fields",
            format!("fields {extra:?}"),
        ))
    }
}
