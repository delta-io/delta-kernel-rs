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

use super::{DataType, StructField, StructType};
use crate::utils::require;

/// The nullability flag of a schema's field. This can be compared with a read schema field's
/// nullability flag using [`Nullable::can_read_as`].
#[derive(Clone, Copy)]
pub(crate) struct Nullable(bool);

/// Represents the ways a schema comparison can fail.
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
pub(crate) type SchemaComparisonResult = Result<(), Error>;

/// Represents a schema compatibility check for the type. If `self` can be read as `read_type`,
/// this function returns `Ok(())`. Otherwise, this function returns `Err`.
pub(crate) trait SchemaComparison {
    fn can_read_as(&self, read_type: &Self) -> SchemaComparisonResult {
        self.can_read_as_internal(read_type, true)
    }

    fn can_read_as_without_type_widening(&self, read_type: &Self) -> SchemaComparisonResult {
        self.can_read_as_internal(read_type, false)
    }

    fn can_read_as_internal(
        &self,
        read_type: &Self,
        allow_type_widening: bool,
    ) -> SchemaComparisonResult;
}

impl SchemaComparison for Nullable {
    /// Represents a nullability comparison between two schemas' fields. Returns true if the
    /// read nullability is the same or wider than the nullability of self.
    fn can_read_as_internal(
        &self,
        read_nullable: &Nullable,
        _allow_type_widening: bool,
    ) -> SchemaComparisonResult {
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
    fn can_read_as_internal(
        &self,
        read_field: &Self,
        allow_type_widening: bool,
    ) -> SchemaComparisonResult {
        Nullable(self.nullable)
            .can_read_as_internal(&Nullable(read_field.nullable), allow_type_widening)?;
        require!(self.name() == read_field.name(), Error::FieldNameMismatch);
        self.data_type()
            .can_read_as_internal(read_field.data_type(), allow_type_widening)?;
        Ok(())
    }
}
impl SchemaComparison for StructType {
    /// Returns `Ok` if this [`StructType`] can be read as `read_type`. This is the case when:
    ///     1. The set of fields in this struct type are a subset of the `read_type`.
    ///     2. For each field in this struct, you can read it as the `read_type`'s field. See
    ///        [`StructField::can_read_as`].
    ///     3. If a field in `read_type` is not present in this struct, then it must be nullable.
    ///     4. Both [`StructTypes`] must be valid schemas. No two fields of a struct may share a
    ///        name that only differs by case.
    fn can_read_as_internal(
        &self,
        read_type: &Self,
        allow_type_widening: bool,
    ) -> SchemaComparisonResult {
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
                Some(existing_field) => {
                    existing_field.can_read_as_internal(read_field, allow_type_widening)?
                }
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
    ///     1. The data types are the same, OR the source type can be widened to the target type
    ///        (see [`PrimitiveType::can_widen_to`])
    ///     2. For complex data types, the nested types must be compatible as defined by
    ///        [`SchemaComparison`]
    ///     3. For array data types, the nullability may not be tightened in the `read_type`. See
    ///        [`Nullable::can_read_as`]
    ///
    /// [`PrimitiveType::can_widen_to`]: super::PrimitiveType::can_widen_to
    fn can_read_as_internal(
        &self,
        read_type: &Self,
        allow_type_widening: bool,
    ) -> SchemaComparisonResult {
        match (self, read_type) {
            (Self::Array(self_array), Self::Array(read_array)) => {
                Nullable(self_array.contains_null()).can_read_as_internal(
                    &Nullable(read_array.contains_null()),
                    allow_type_widening,
                )?;
                self_array
                    .element_type()
                    .can_read_as_internal(read_array.element_type(), allow_type_widening)?;
            }
            (Self::Struct(self_struct), Self::Struct(read_struct)) => {
                self_struct.can_read_as_internal(read_struct, allow_type_widening)?
            }
            (Self::Map(self_map), Self::Map(read_map)) => {
                Nullable(self_map.value_contains_null()).can_read_as_internal(
                    &Nullable(read_map.value_contains_null()),
                    allow_type_widening,
                )?;
                self_map
                    .key_type()
                    .can_read_as_internal(read_map.key_type(), allow_type_widening)?;
                self_map
                    .value_type()
                    .can_read_as_internal(read_map.value_type(), allow_type_widening)?;
            }
            // Exact match
            (a, b) if a == b => {}
            // Type widening: smaller primitive types can be read as larger ones
            (Self::Primitive(a), Self::Primitive(b))
                if allow_type_widening && a.can_widen_to(b) => {}
            // Any other type change is incompatible
            _ => return Err(Error::TypeMismatch),
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::schema::compare::{Error, SchemaComparison, SchemaComparisonResult};
    use crate::schema::{
        schema, ArrayType, DataType, MapType, PrimitiveType, StructField, StructType,
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ComparisonMode {
        AllowTypeWidening,
        ForbidTypeWidening,
    }

    impl ComparisonMode {
        fn can_read_as<T: SchemaComparison>(
            self,
            existing: &T,
            read: &T,
        ) -> SchemaComparisonResult {
            match self {
                Self::AllowTypeWidening => existing.can_read_as(read),
                Self::ForbidTypeWidening => existing.can_read_as_without_type_widening(read),
            }
        }

        fn allows_type_widening(self) -> bool {
            self == Self::AllowTypeWidening
        }
    }

    #[test]
    fn can_read_is_reflexive() {
        let schema = schema! {
            not_null "id": LONG,
            not_null "map": {
                { not_null "id": LONG, not_null "name": STRING }
                    => nullable { nullable "age": INTEGER }
            },
            not_null "array": [ not_null TIMESTAMP ],
            not_null "nested_struct": { not_null "name": STRING, nullable "age": INTEGER },
        };

        assert!(schema.can_read_as(&schema).is_ok());
    }
    #[rstest]
    fn add_nullable_column_to_map_key_and_value(
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
        let existing_schema = schema! {
            not_null "map": {
                { not_null "id": LONG, nullable "name": STRING }
                    => not_null { not_null "age": INTEGER }
            },
        };

        let read_schema = schema! {
            not_null "map": {
                { not_null "id": LONG, nullable "name": STRING, nullable "location": STRING }
                    => not_null { nullable "age": INTEGER, nullable "years_of_experience": INTEGER }
            },
        };

        assert!(mode.can_read_as(&existing_schema, &read_schema).is_ok());
    }
    #[test]
    fn map_value_becomes_non_nullable_fails() {
        let existing_schema = schema! {
            not_null "map": {
                { not_null "id": LONG, not_null "name": STRING } => not_null { nullable "age": INTEGER }
            },
        };

        let read_schema = schema! {
            not_null "map": {
                { not_null "id": LONG, not_null "name": STRING } => not_null { not_null "age": INTEGER }
            },
        };

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
    #[rstest]
    fn differ_by_non_nullable_column(
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
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
            mode.can_read_as(&a, &b),
            Err(Error::NewNonNullableColumn)
        ));

        // Read `b` as `a`. `a` is missing a column that is present in `b`.
        assert!(matches!(
            mode.can_read_as(&b, &a),
            Err(Error::MissingColumn)
        ));
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

    #[rstest]
    #[case::byte_to_short(DataType::BYTE, DataType::SHORT)]
    #[case::byte_to_integer(DataType::BYTE, DataType::INTEGER)]
    #[case::byte_to_long(DataType::BYTE, DataType::LONG)]
    #[case::short_to_integer(DataType::SHORT, DataType::INTEGER)]
    #[case::short_to_long(DataType::SHORT, DataType::LONG)]
    #[case::integer_to_long(DataType::INTEGER, DataType::LONG)]
    #[case::float_to_double(DataType::FLOAT, DataType::DOUBLE)]
    #[case::timestamp_to_timestamp_ntz(DataType::TIMESTAMP, DataType::TIMESTAMP_NTZ)]
    #[case::timestamp_ntz_to_timestamp(DataType::TIMESTAMP_NTZ, DataType::TIMESTAMP)]
    fn primitive_type_widening(
        #[case] source: DataType,
        #[case] target: DataType,
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
        assert_eq!(
            mode.can_read_as(&source, &target).is_ok(),
            mode.allows_type_widening()
        );
    }

    #[rstest]
    #[case::long_to_integer(DataType::LONG, DataType::INTEGER)]
    #[case::integer_to_short(DataType::INTEGER, DataType::SHORT)]
    #[case::short_to_byte(DataType::SHORT, DataType::BYTE)]
    #[case::double_to_float(DataType::DOUBLE, DataType::FLOAT)]
    fn primitive_type_narrowing_is_rejected(
        #[case] source: DataType,
        #[case] target: DataType,
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
        assert!(matches!(
            mode.can_read_as(&source, &target),
            Err(Error::TypeMismatch)
        ));
    }

    #[rstest]
    // Physical type reinterpretation (not general widening -- can_read_as rejects these)
    #[case::integer_to_date(PrimitiveType::Integer, PrimitiveType::Date, true)]
    #[case::long_to_timestamp(PrimitiveType::Long, PrimitiveType::Timestamp, true)]
    #[case::long_to_timestamp_ntz(PrimitiveType::Long, PrimitiveType::TimestampNtz, true)]
    // Reverse (narrowing) is rejected
    #[case::date_to_integer(PrimitiveType::Date, PrimitiveType::Integer, false)]
    #[case::timestamp_to_long(PrimitiveType::Timestamp, PrimitiveType::Long, false)]
    #[case::timestamp_ntz_to_long(PrimitiveType::TimestampNtz, PrimitiveType::Long, false)]
    // Cross-type combinations are rejected
    #[case::long_to_date(PrimitiveType::Long, PrimitiveType::Date, false)]
    #[case::integer_to_timestamp(PrimitiveType::Integer, PrimitiveType::Timestamp, false)]
    #[case::integer_to_timestamp_ntz(PrimitiveType::Integer, PrimitiveType::TimestampNtz, false)]
    #[case::byte_to_date(PrimitiveType::Byte, PrimitiveType::Date, false)]
    // Identity
    #[case::date_identity(PrimitiveType::Date, PrimitiveType::Date, true)]
    #[case::timestamp_identity(PrimitiveType::Timestamp, PrimitiveType::Timestamp, true)]
    #[case::long_identity(PrimitiveType::Long, PrimitiveType::Long, true)]
    // Widening (superset of can_widen_to)
    #[case::byte_to_long(PrimitiveType::Byte, PrimitiveType::Long, true)]
    #[case::short_to_integer(PrimitiveType::Short, PrimitiveType::Integer, true)]
    #[case::float_to_double(PrimitiveType::Float, PrimitiveType::Double, true)]
    #[case::timestamp_to_ntz(PrimitiveType::Timestamp, PrimitiveType::TimestampNtz, true)]
    fn stats_type_compatibility(
        #[case] source: PrimitiveType,
        #[case] target: PrimitiveType,
        #[case] expected: bool,
    ) {
        assert_eq!(
            source.is_stats_type_compatible_with(&target),
            expected,
            "{source:?} -> {target:?} should be {expected}"
        );
    }

    #[rstest]
    fn type_widening_in_struct(
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
        let source = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("value", DataType::FLOAT, true),
        ]);
        let target = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false),
            StructField::new("value", DataType::DOUBLE, true),
        ]);

        assert_eq!(
            mode.can_read_as(&source, &target).is_ok(),
            mode.allows_type_widening()
        );

        assert!(matches!(
            mode.can_read_as(&target, &source),
            Err(Error::TypeMismatch)
        ));
    }

    #[rstest]
    #[case::array(
        DataType::from(ArrayType::new(DataType::INTEGER, false)),
        DataType::from(ArrayType::new(DataType::LONG, false))
    )]
    #[case::map_key(
        DataType::from(MapType::new(DataType::INTEGER, DataType::STRING, false)),
        DataType::from(MapType::new(DataType::LONG, DataType::STRING, false))
    )]
    #[case::map_value(
        DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, false)),
        DataType::from(MapType::new(DataType::STRING, DataType::LONG, false))
    )]
    fn type_widening_in_complex_types(
        #[case] source: DataType,
        #[case] target: DataType,
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
        let result = mode.can_read_as(&source, &target);
        match mode {
            ComparisonMode::AllowTypeWidening => assert!(result.is_ok()),
            ComparisonMode::ForbidTypeWidening => {
                assert!(matches!(result, Err(Error::TypeMismatch)))
            }
        }
    }

    #[rstest]
    fn incompatible_type_change(
        #[values(ComparisonMode::AllowTypeWidening, ComparisonMode::ForbidTypeWidening)]
        mode: ComparisonMode,
    ) {
        // Cannot change between incompatible types
        assert!(matches!(
            mode.can_read_as(&DataType::STRING, &DataType::INTEGER),
            Err(Error::TypeMismatch)
        ));
        assert!(matches!(
            mode.can_read_as(&DataType::INTEGER, &DataType::STRING),
            Err(Error::TypeMismatch)
        ));
        assert!(matches!(
            mode.can_read_as(&DataType::BOOLEAN, &DataType::INTEGER),
            Err(Error::TypeMismatch)
        ));
    }
}
