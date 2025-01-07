use std::collections::{HashMap, HashSet};

use crate::schema::{DataType, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, Error};

struct NullabilityCheck {
    nullable: bool,
    read_nullable: bool,
}

impl NullabilityCheck {
    fn is_compatible(&self) -> DeltaResult<()> {
        // The case to avoid is when the column is nullable, but the read schema specifies the
        // column as non-nullable. So we avoid the case where !read_nullable && nullable
        // Hence we check that !(!read_nullable && existing_nullable)
        // == read_nullable || !existing_nullable
        require!(
            self.read_nullable || !self.nullable,
            Error::generic("Read field is non-nullable while this field is nullable")
        );
        Ok(())
    }
}

impl StructField {
    fn can_read_as(&self, read_field: &StructField) -> DeltaResult<()> {
        require!(
            self.name() == read_field.name(),
            Error::generic(format!(
                "Struct field with name {} cannot be read with name {}",
                self.name(),
                read_field.name()
            ))
        );

        NullabilityCheck {
            nullable: self.nullable,
            read_nullable: read_field.nullable,
        }
        .is_compatible()?;

        self.data_type().can_read_as(read_field.data_type())?;
        Ok(())
    }
}
impl StructType {
    #[allow(unused)]
    pub(crate) fn can_read_as(&self, read_type: &StructType) -> DeltaResult<()> {
        let field_map: HashMap<String, &StructField> = self
            .fields
            .iter()
            .map(|(name, field)| (name.to_lowercase(), field))
            .collect();
        // Delta tables do not allow fields that differ in name only by case
        require!(
            field_map.len() == self.fields.len(),
            Error::generic("Delta tables don't allow field names that only differ by case")
        );

        let read_field_names: HashSet<String> =
            read_type.fields.keys().map(|x| x.to_lowercase()).collect();
        require!(
            read_field_names.len() == read_type.fields.len(),
            Error::generic("Delta tables don't allow field names that only differ by case")
        );

        // Check that the field names are a subset of the read fields.
        if !field_map.keys().all(|name| read_field_names.contains(name)) {
            return Err(Error::generic(
                "Struct has column that does not exist in the read schema",
            ));
        }
        for read_field in read_type.fields() {
            match field_map.get(&read_field.name().to_lowercase()) {
                Some(existing_field) => existing_field.can_read_as(read_field)?,
                None => {
                    // Note: Delta spark does not perform the following check. Hence it ignores fields
                    // that exist in the read schema that aren't in this schema.
                    require!(
                        read_field.is_nullable(),
                        Error::generic(
                            "read type has non-nullable column that does not exist in this struct",
                        )
                    );
                }
            }
        }
        Ok(())
    }
}

impl DataType {
    fn can_read_as(&self, read_type: &DataType) -> DeltaResult<()> {
        match (self, read_type) {
            (DataType::Array(self_array), DataType::Array(read_array)) => {
                NullabilityCheck {
                    nullable: self_array.contains_null(),
                    read_nullable: read_array.contains_null(),
                }
                .is_compatible()?;
                self_array
                    .element_type()
                    .can_read_as(read_array.element_type())?;
            }
            (DataType::Struct(self_struct), DataType::Struct(read_struct)) => {
                self_struct.can_read_as(read_struct)?
            }
            (DataType::Map(self_map), DataType::Map(read_map)) => {
                NullabilityCheck {
                    nullable: self_map.value_contains_null(),
                    read_nullable: read_map.value_contains_null(),
                }
                .is_compatible()?;
                self_map.key_type().can_read_as(read_map.key_type())?;
                self_map.value_type().can_read_as(read_map.value_type())?;
            }
            (a, b) => {
                // TODO: In the future, we will change this to support type widening.
                // See: https://github.com/delta-io/delta-kernel-rs/issues/623
                require!(
                    a == b,
                    Error::generic(format!("Types {} and {} are not compatible", a, b))
                );
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    #[test]
    fn can_read_is_reflexive() {
        let map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let map_value = StructType::new([StructField::new("age", DataType::INTEGER, true)]);
        let map_type = MapType::new(map_key, map_value, true);
        let array_type = ArrayType::new(DataType::TIMESTAMP, false);
        let nested_struct = StructType::new([
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("map", map_type, false),
            StructField::new("array", array_type, false),
            StructField::new("nested_struct", nested_struct, false),
        ]);

        assert!(schema.can_read_as(&schema).is_ok());
    }
    #[test]
    fn add_nullable_column_to_map_key_and_value() {
        let existing_map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        let existing_map_value =
            StructType::new([StructField::new("age", DataType::INTEGER, false)]);
        let existing_schema = StructType::new([StructField::new(
            "map",
            MapType::new(existing_map_key, existing_map_value, false),
            false,
        )]);

        let read_map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("location", DataType::STRING, true),
        ]);
        let read_map_value = StructType::new([
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("years_of_experience", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([StructField::new(
            "map",
            MapType::new(read_map_key, read_map_value, false),
            false,
        )]);

        assert!(existing_schema.can_read_as(&read_schema).is_ok());
    }
    #[test]
    fn map_value_becomes_non_nullable_fails() {
        let map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let map_value = StructType::new([StructField::new("age", DataType::INTEGER, true)]);
        let existing_schema = StructType::new([StructField::new(
            "map",
            MapType::new(map_key, map_value, false),
            false,
        )]);

        let map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let map_value = StructType::new([StructField::new("age", DataType::INTEGER, false)]);
        let read_schema = StructType::new([StructField::new(
            "map",
            MapType::new(map_key, map_value, false),
            false,
        )]);

        assert!(existing_schema.can_read_as(&read_schema).is_err());
    }
    #[test]
    fn different_field_name_case_fails() {
        // names differing only in case are not the same
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([
            StructField::new("Id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_err());
    }
    #[test]
    fn different_type_fails() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_err());
    }
    #[test]
    fn set_nullable_to_true() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_ok());
    }
    #[test]
    fn set_nullable_to_false_fails() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, false),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_err());
    }
    #[test]
    fn new_nullable_column() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let read_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("location", DataType::STRING, true),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_ok());
    }
    #[test]
    fn new_non_nullable_column_fails() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let read_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("location", DataType::STRING, false),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_err());
    }
    #[test]
    fn duplicate_field_modulo_case() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("Id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let read_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("Id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(existing_schema.can_read_as(&read_schema).is_err());
    }
}
