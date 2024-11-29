use std::collections::{HashMap, HashSet};

use crate::schema::{DataType, Schema, StructField, StructType};

fn is_nullability_compatible(existing_nullable: bool, read_nullable: bool) -> bool {
    // The case to avoid is when the read_schema is non-nullable and the existing one is nullable.
    // So we avoid the case where !read_nullable && existing_nullable
    // Hence we check that !(!read_nullable && existing_nullable)
    // == read_nullable || !existing_nullable
    read_nullable || !existing_nullable
}
fn is_struct_read_compatible(existing: &StructType, read_type: &StructType) -> bool {
    // Delta tables do not allow fields that differ in name only by case
    let existing_fields: HashMap<&String, &StructField> = existing
        .fields()
        .map(|field| (field.name(), field))
        .collect();

    let existing_names: HashSet<String> = existing
        .fields()
        .map(|field| field.name().clone())
        .collect();
    let read_names: HashSet<String> = read_type
        .fields()
        .map(|field| field.name().clone())
        .collect();
    if !existing_names.is_subset(&read_names) {
        return false;
    }
    read_type
        .fields()
        .all(|read_field| match existing_fields.get(read_field.name()) {
            Some(existing_field) => {
                let name_equal = existing_field.name() == read_field.name();

                let nullability_equal =
                    is_nullability_compatible(existing_field.nullable, read_field.nullable);
                let data_type_equal =
                    is_datatype_read_compatible(existing_field.data_type(), read_field.data_type());
                println!(
                    "name_equal {} nullability: {}, datatype: {}",
                    name_equal, nullability_equal, data_type_equal
                );
                name_equal && nullability_equal && data_type_equal
            }
            None => read_field.is_nullable(),
        })
}
fn is_datatype_read_compatible(existing: &DataType, read_type: &DataType) -> bool {
    match (existing, read_type) {
        // TODO: Add support for type widening
        (DataType::Array(a), DataType::Array(b)) => {
            is_datatype_read_compatible(a.element_type(), b.element_type())
                && is_nullability_compatible(a.contains_null(), b.contains_null())
        }
        (DataType::Struct(a), DataType::Struct(b)) => is_struct_read_compatible(a, b),
        (DataType::Map(a), DataType::Map(b)) => {
            is_nullability_compatible(a.value_contains_null(), b.value_contains_null())
                && is_datatype_read_compatible(a.key_type(), b.key_type())
                && is_datatype_read_compatible(a.value_type(), b.value_type())
        }
        (a, b) => a == b,
    }
}

fn is_partition_compatible(
    existing_partition_cols: &[String],
    read_partition_cols: &[String],
) -> bool {
    existing_partition_cols == read_partition_cols
}

fn is_schema_compatible(existing_schema: &Schema, read_schema: &Schema) -> bool {
    is_struct_read_compatible(existing_schema, read_schema)
}
#[cfg(test)]
mod tests {

    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    use super::is_schema_compatible;

    #[test]
    fn equal_schema() {
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

        assert!(is_schema_compatible(&schema, &schema));
    }

    #[test]
    fn different_schema_fails() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([
            StructField::new("company", DataType::STRING, false),
            StructField::new("employee_name", DataType::STRING, false),
            StructField::new("salary", DataType::LONG, false),
            StructField::new("position_name", DataType::STRING, true),
        ]);
        assert!(!is_schema_compatible(&existing_schema, &read_schema));
    }

    #[test]
    fn map_nullability_and_ok_schema_evolution() {
        let existing_map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let existing_map_value =
            StructType::new([StructField::new("age", DataType::INTEGER, true)]);
        let existing_schema = StructType::new([StructField::new(
            "map",
            MapType::new(existing_map_key, existing_map_value, false),
            false,
        )]);

        let read_map_key = StructType::new([
            StructField::new("id", DataType::LONG, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("location", DataType::STRING, true),
        ]);
        let read_map_value = StructType::new([
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("years_of_experience", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([StructField::new(
            "map",
            MapType::new(read_map_key, read_map_value, true),
            false,
        )]);

        assert!(is_schema_compatible(&existing_schema, &read_schema));
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

        assert!(!is_schema_compatible(&existing_schema, &read_schema));
    }
    #[test]
    fn map_schema_new_non_nullable_value_fails() {
        let existing_map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let existing_map_value =
            StructType::new([StructField::new("age", DataType::INTEGER, true)]);
        let existing_schema = StructType::new([StructField::new(
            "map",
            MapType::new(existing_map_key, existing_map_value, false),
            false,
        )]);

        let read_map_key = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
        ]);
        let read_map_value = StructType::new([
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("years_of_experience", DataType::INTEGER, false),
        ]);
        let read_schema = StructType::new([StructField::new(
            "map",
            MapType::new(read_map_key, read_map_value, false),
            false,
        )]);

        assert!(!is_schema_compatible(&existing_schema, &read_schema));
    }

    #[test]
    fn different_field_name_fails() {
        let existing_schema = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        let read_schema = StructType::new([
            StructField::new("new_id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);
        assert!(!is_schema_compatible(&existing_schema, &read_schema));
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
        assert!(!is_schema_compatible(&existing_schema, &read_schema));
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
        assert!(is_schema_compatible(&existing_schema, &read_schema));
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
        assert!(!is_schema_compatible(&existing_schema, &read_schema));
    }
    #[test]
    fn new_nullable_column() {
        let existing = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let read = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("location", DataType::STRING, true),
        ]);
        assert!(is_schema_compatible(&existing, &read));
    }

    #[test]
    fn new_non_nullable_column_fails() {
        let existing = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
        ]);

        let read = StructType::new([
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, false),
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("location", DataType::STRING, false),
        ]);
        assert!(!is_schema_compatible(&existing, &read));
    }
}
