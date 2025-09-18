//! Provides utilities to perform comparisons between a [`Schema`]s. The api used to check schema
//! compatibility is [`can_read_as`] that is exposed through the [`SchemaComparison`] trait.
//!
//! # Examples
//!  ```rust, ignore
//!  # use delta_kernel::schema::StructType;
//!  # use delta_kernel::schema::StructField;
//!  # use delta_kernel::schema::DataType;
//!  let schema = StructType::try_new([
//!     StructField::new("id", DataType::LONG, false),
//!     StructField::new("value", DataType::STRING, true),
//!  ])?;
//!  let read_schema = StructType::try_new([
//!     StructField::new("id", DataType::LONG, true),
//!     StructField::new("value", DataType::STRING, true),
//!     StructField::new("year", DataType::INTEGER, true),
//!  ])?;
//!  // Schemas are compatible since the `read_schema` adds a nullable column `year`
//!  assert!(schema.can_read_as(&read_schema).is_ok());
//!  ````
//!
//! [`Schema`]: crate::schema::Schema
use std::collections::{HashMap, HashSet};

use crate::utils::require;

use super::{DataType, StructField, StructType};

/// The nullability flag of a schema's field. This can be compared with a read schema field's
/// nullability flag using [`Nullable::can_read_as`].
#[allow(unused)]
#[derive(Clone, Copy)]
pub(crate) struct Nullable(bool);

/// Represents the ways a schema comparison can fail.
#[allow(unused)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("The nullability was tightened for a field")]
    NullabilityTightening,
    #[error("Field names do not match")]
    FieldNameMismatch,
    #[error("Schema is invalid")]
    InvalidSchema,
    #[error("The read schema is missing a column present in the schema")]
    MissingColumn,
    #[error("Read schema has a non-nullable column that is not present in the schema")]
    NewNonNullableColumn,
    #[error("Types for two schema fields did not match")]
    TypeMismatch,
}

/// A [`std::result::Result`] that has the schema comparison [`Error`] as the error variant.
#[allow(unused)]
pub(crate) type SchemaComparisonResult = Result<(), Error>;

/// Represents a schema compatibility check for the type. If `self` can be read as `read_type`,
/// this function returns `Ok(())`. Otherwise, this function returns `Err`.
///
/// TODO (Oussama): Remove the `allow(unused)` once this is used in CDF.
#[allow(unused)]
pub(crate) trait SchemaComparison {
    fn can_read_as(&self, read_type: &Self) -> SchemaComparisonResult;
}

impl SchemaComparison for Nullable {
    /// Represents a nullability comparison between two schemas' fields. Returns true if the
    /// read nullability is the same or wider than the nullability of self.
    fn can_read_as(&self, read_nullable: &Nullable) -> SchemaComparisonResult {
        // The case to avoid is when the column is nullable, but the read schema specifies the
        // column as non-nullable. So we avoid the case where !read_nullable && nullable
        // Hence we check that !(!read_nullable && existing_nullable)
        // == read_nullable || !existing_nullable
        require!(read_nullable.0 || !self.0, Error::NullabilityTightening);
        Ok(())
    }
}

impl SchemaComparison for StructField {
    /// Returns `Ok` if this [`StructField`] can be read as `read_field`. Three requirements must
    /// be satisfied:
    ///     1. The read schema field mustn't be non-nullable if this [`StructField`] is nullable.
    ///     2. The both this field and `read_field` must have the same name.
    ///     3. You can read this data type as the `read_field`'s data type.
    fn can_read_as(&self, read_field: &Self) -> SchemaComparisonResult {
        Nullable(self.nullable).can_read_as(&Nullable(read_field.nullable))?;
        require!(self.name() == read_field.name(), Error::FieldNameMismatch);
        self.data_type().can_read_as(read_field.data_type())?;
        Ok(())
    }
}
impl SchemaComparison for StructType {
    /// Returns `Ok` if this [`StructType`] can be read as `read_type`. This is the case when:
    ///     1. The set of fields in this struct type are a subset of the `read_type`.
    ///     2. For each field in this struct, you can read it as the `read_type`'s field. See
    ///        [`StructField::can_read_as`].
    ///     3. If a field in `read_type` is not present in this struct, then it must be nullable.
    ///     4. Both [`StructTypes`] must be valid schemas. No two fields of a structs may share a
    ///        name that only differs by case. TODO: This check should be moved into the constructor
    ///        for [`StructType`].
    fn can_read_as(&self, read_type: &Self) -> SchemaComparisonResult {
        let lowercase_field_map: HashMap<String, &StructField> = self
            .fields
            .iter()
            .map(|(name, field)| (name.to_lowercase(), field))
            .collect();
        require!(
            lowercase_field_map.len() == self.fields.len(),
            Error::InvalidSchema
        );

        let lowercase_read_field_names: HashSet<String> =
            read_type.fields.keys().map(|x| x.to_lowercase()).collect();
        require!(
            lowercase_read_field_names.len() == read_type.fields.len(),
            Error::InvalidSchema
        );

        // Check that the field names are a subset of the read fields.
        if lowercase_field_map
            .keys()
            .any(|name| !lowercase_read_field_names.contains(name))
        {
            return Err(Error::MissingColumn);
        }
        for read_field in read_type.fields() {
            match lowercase_field_map.get(&read_field.name().to_lowercase()) {
                Some(existing_field) => existing_field.can_read_as(read_field)?,
                None => {
                    // Note: Delta spark does not perform the following check. Hence it ignores
                    // non-null fields that exist in the read schema that aren't in this schema.
                    require!(read_field.is_nullable(), Error::NewNonNullableColumn);
                }
            }
        }
        Ok(())
    }
}

impl SchemaComparison for DataType {
    /// Returns `Ok` if this [`DataType`] can be read as `read_type`. This is the case when:
    ///     1. The data types are the same. Note: This condition will be relaxed to include
    ///        compatible data types with type widening. See issue [`#623`]
    ///     2. For complex data types, the nested types must be compatible as defined by [`SchemaComparison`]
    ///     3. For array data types, the nullability may not be tightened in the `read_type`. See
    ///        [`Nullable::can_read_as`]
    ///
    /// [`#623`]: <https://github.com/delta-io/delta-kernel-rs/issues/623>
    fn can_read_as(&self, read_type: &Self) -> SchemaComparisonResult {
        match (self, read_type) {
            (Self::Array(self_array), Self::Array(read_array)) => {
                Nullable(self_array.contains_null())
                    .can_read_as(&Nullable(read_array.contains_null()))?;
                self_array
                    .element_type()
                    .can_read_as(read_array.element_type())?;
            }
            (Self::Struct(self_struct), Self::Struct(read_struct)) => {
                self_struct.can_read_as(read_struct)?
            }
            (Self::Map(self_map), Self::Map(read_map)) => {
                Nullable(self_map.value_contains_null())
                    .can_read_as(&Nullable(read_map.value_contains_null()))?;
                self_map.key_type().can_read_as(read_map.key_type())?;
                self_map.value_type().can_read_as(read_map.value_type())?;
            }
            (a, b) => {
                // TODO: In the future, we will change this to support type widening.
                // See: #623
                require!(a == b, Error::TypeMismatch);
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::compare::{Error, SchemaComparison};
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    #[test]
    fn can_read_is_reflexive() {
        let map_key = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let map_value =
            StructType::new_unchecked([StructField::new("age", DataType::INTEGER, true)]);
        let map_type = MapType::new(map_key, map_value, true);
        let array_type = ArrayType::new(DataType::TIMESTAMP, false);
        let nested_struct = StructType::new_unchecked([
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("map", map_type, false),
            StructField::new("array", array_type, false),
            StructField::new("nested_struct", nested_struct, false),
        ]);

        assert!(schema.can_read_as(&schema).is_ok());
    }
    #[test]
    fn add_nullable_column_to_map_key_and_value() {
        let existing_map_key = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        let existing_map_value =
            StructType::new_unchecked([StructField::new("age", DataType::INTEGER, false)]);
        let existing_schema = StructType::new_unchecked([StructField::new(
            "map",
            MapType::new(existing_map_key, existing_map_value, false),
            false,
        )]);

        let read_map_key = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("location", DataType::STRING, true),
        ]);
        let read_map_value = StructType::new_unchecked([
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("years_of_experience", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new_unchecked([StructField::new(
            "map",
            MapType::new(read_map_key, read_map_value, false),
            false,
        )]);

        assert!(existing_schema.can_read_as(&read_schema).is_ok());
    }
    #[test]
    fn map_value_becomes_non_nullable_fails() {
        let map_key = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let map_value =
            StructType::new_unchecked([StructField::new("age", DataType::INTEGER, true)]);
        let existing_schema = StructType::new_unchecked([StructField::new(
            "map",
            MapType::new(map_key, map_value, false),
            false,
        )]);

        let map_key = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let map_value =
            StructType::new_unchecked([StructField::new("age", DataType::INTEGER, false)]);
        let read_schema = StructType::new_unchecked([StructField::new(
            "map",
            MapType::new(map_key, map_value, false),
            false,
        )]);

        assert!(matches!(
            existing_schema.can_read_as(&read_schema),
            Err(Error::NullabilityTightening)
        ));
    }
    #[test]
    fn different_field_name_case_fails() {
        // names differing only in case are not the same
        let existing_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new_unchecked([
            StructField::new("Id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(matches!(
            existing_schema.can_read_as(&read_schema),
            Err(Error::FieldNameMismatch)
        ));
    }
    #[test]
    fn different_type_fails() {
        let existing_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(matches!(
            existing_schema.can_read_as(&read_schema),
            Err(Error::TypeMismatch)
        ));
    }
    #[test]
    fn set_nullable_to_true() {
        let existing_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_ok());
    }
    #[test]
    fn set_nullable_to_false_fails() {
        let existing_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, false),
        ]);
        assert!(matches!(
            existing_schema.can_read_as(&read_schema),
            Err(Error::NullabilityTightening)
        ));
    }
    #[test]
    fn differ_by_nullable_column() {
        let a = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let b = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("location", DataType::STRING, true),
        ]);

        // Read `a` as `b`. `b` adds a new nullable column. This is compatible with `a`'s schema.
        assert!(a.can_read_as(&b).is_ok());

        // Read `b` as `a`. `a` is missing a column that is present in `b`.
        assert!(matches!(b.can_read_as(&a), Err(Error::MissingColumn)));
    }
    #[test]
    fn differ_by_non_nullable_column() {
        let a = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let b = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("location", DataType::STRING, false),
        ]);

        // Read `a` as `b`. `b` has an extra non-nullable column.
        assert!(matches!(
            a.can_read_as(&b),
            Err(Error::NewNonNullableColumn)
        ));

        // Read `b` as `a`. `a` is missing a column that is present in `b`.
        assert!(matches!(b.can_read_as(&a), Err(Error::MissingColumn)));
    }

    #[test]
    fn duplicate_field_modulo_case() {
        let existing_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("Id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let read_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("Id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(matches!(
            existing_schema.can_read_as(&read_schema),
            Err(Error::InvalidSchema)
        ));

        // Checks in the inverse order
        assert!(matches!(
            read_schema.can_read_as(&existing_schema),
            Err(Error::InvalidSchema)
        ));
    }
}
