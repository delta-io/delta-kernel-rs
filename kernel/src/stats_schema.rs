//! Schema generation helpers for parsed statistics.
//!
//! This module provides functionality to generate the schema for the `stats_parsed` column
//! in Delta checkpoints based on the table schema.

use crate::schema::{DataType, StructField, StructType};

/// Check if a data type is eligible for min/max statistics.
///
/// Delta spec: Only certain types have meaningful min/max values.
/// Complex types (arrays, maps) and some binary types are excluded.
pub fn is_stats_eligible_type(data_type: &DataType) -> bool {
    use crate::schema::PrimitiveType;
    use DataType::*;
    matches!(
        data_type,
        Primitive(PrimitiveType::Boolean)
            | Primitive(PrimitiveType::Byte)
            | Primitive(PrimitiveType::Short)
            | Primitive(PrimitiveType::Integer)
            | Primitive(PrimitiveType::Long)
            | Primitive(PrimitiveType::Float)
            | Primitive(PrimitiveType::Double)
            | Primitive(PrimitiveType::String)
            | Primitive(PrimitiveType::Binary)
            | Primitive(PrimitiveType::Date)
            | Primitive(PrimitiveType::Timestamp)
            | Primitive(PrimitiveType::TimestampNtz)
            | Primitive(PrimitiveType::Decimal(_))
    )
}

/// Build the min/max stats schema from table schema.
///
/// Returns a struct schema containing only stats-eligible columns.
/// Nested structs are recursively processed.
///
/// IMPORTANT: Uses PHYSICAL column names, not logical names.
///
/// # Example
/// ```rust,ignore
/// use delta_kernel::schema::{DataType, StructField, StructType};
/// use delta_kernel::stats_schema::build_min_max_stats_schema;
///
/// let table_schema = StructType::new_unchecked(vec![
///     StructField::new("id", DataType::INTEGER, false),
///     StructField::new("name", DataType::STRING, true),
///     StructField::new("data", DataType::Array(Box::new(DataType::INTEGER)), true),
/// ]);
///
/// let stats_schema = build_min_max_stats_schema(&table_schema);
/// // Result contains only "id" and "name" (array excluded)
/// ```
pub fn build_min_max_stats_schema(table_schema: &StructType) -> StructType {
    let mut fields = Vec::new();

    for field in table_schema.fields() {
        match field.data_type() {
            DataType::Struct(nested) => {
                // Recursively process nested structs
                let nested_stats = build_min_max_stats_schema(nested);
                if nested_stats.fields().len() > 0 {
                    fields.push(StructField::new(
                        field.physical_name(),
                        DataType::Struct(Box::new(nested_stats)),
                        true, // Stats fields are always nullable
                    ));
                }
            }
            dt if is_stats_eligible_type(dt) => {
                // Include stats-eligible leaf columns
                fields.push(StructField::new(
                    field.physical_name(),
                    dt.clone(),
                    true, // Stats fields are always nullable
                ));
            }
            _ => {
                // Skip arrays, maps, and other non-eligible types
            }
        }
    }

    StructType::new_unchecked(fields)
}

/// Build the null count stats schema from table schema.
///
/// Returns a struct schema with all leaf columns as LONG (null counts).
///
/// IMPORTANT: Uses PHYSICAL column names, not logical names.
///
/// # Example
/// ```rust,ignore
/// use delta_kernel::schema::{DataType, StructField, StructType};
/// use delta_kernel::stats_schema::build_null_count_stats_schema;
///
/// let table_schema = StructType::new_unchecked(vec![
///     StructField::new("id", DataType::INTEGER, false),
///     StructField::new("name", DataType::STRING, true),
///     StructField::new("tags", DataType::Array(Box::new(DataType::STRING)), true),
/// ]);
///
/// let stats_schema = build_null_count_stats_schema(&table_schema);
/// // Result contains all columns as LONG type
/// ```
pub fn build_null_count_stats_schema(table_schema: &StructType) -> StructType {
    let mut fields = Vec::new();

    for field in table_schema.fields() {
        match field.data_type() {
            DataType::Struct(nested) => {
                // Recursively process nested structs
                let nested_stats = build_null_count_stats_schema(nested);
                if nested_stats.fields().len() > 0 {
                    fields.push(StructField::new(
                        field.physical_name(),
                        DataType::Struct(Box::new(nested_stats)),
                        true,
                    ));
                }
            }
            _ => {
                // All leaf columns get null count (always LONG)
                fields.push(StructField::new(
                    field.physical_name(),
                    DataType::LONG,
                    true,
                ));
            }
        }
    }

    StructType::new_unchecked(fields)
}

/// Generate the complete stats_parsed schema.
///
/// This is the schema for the stats_parsed column in checkpoint files.
///
/// Structure:
/// ```text
/// stats_parsed: struct<
///     numRecords: long,
///     minValues: struct<...>,
///     maxValues: struct<...>,
///     nullCount: struct<...>,
///     tightBounds: boolean (optional)
/// >
/// ```
pub fn generate_stats_parsed_schema(
    table_schema: &StructType,
    include_tight_bounds: bool,
) -> StructType {
    let mut fields = vec![StructField::new("numRecords", DataType::LONG, false)];

    // Add min/max schemas
    let min_max_schema = build_min_max_stats_schema(table_schema);
    if min_max_schema.fields().len() > 0 {
        fields.push(StructField::new(
            "minValues",
            DataType::Struct(Box::new(min_max_schema.clone())),
            true,
        ));
        fields.push(StructField::new(
            "maxValues",
            DataType::Struct(Box::new(min_max_schema)),
            true,
        ));
    }

    // Add null count schema
    let null_count_schema = build_null_count_stats_schema(table_schema);
    if null_count_schema.fields().len() > 0 {
        fields.push(StructField::new(
            "nullCount",
            DataType::Struct(Box::new(null_count_schema)),
            true,
        ));
    }

    // Optionally add tightBounds
    if include_tight_bounds {
        fields.push(StructField::new("tightBounds", DataType::BOOLEAN, true));
    }

    StructType::new_unchecked(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    #[test]
    fn test_is_stats_eligible() {
        use crate::schema::PrimitiveType;

        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Boolean
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Byte
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Short
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Integer
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Long
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Float
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Double
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::String
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Binary
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Date
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Timestamp
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::TimestampNtz
        )));
        assert!(is_stats_eligible_type(&DataType::Primitive(
            PrimitiveType::Decimal(crate::schema::DecimalType::try_new(10, 2).unwrap())
        )));

        // Non-eligible types
        assert!(!is_stats_eligible_type(&DataType::Array(Box::new(
            ArrayType::new(DataType::INTEGER, false)
        ))));
        assert!(!is_stats_eligible_type(&DataType::Map(Box::new(
            MapType::new(DataType::STRING, DataType::INTEGER, false)
        ))));
        assert!(!is_stats_eligible_type(&DataType::Struct(Box::new(
            StructType::new_unchecked(vec![])
        ))));
    }

    #[test]
    fn test_build_min_max_schema() {
        let table_schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
                true,
            ),
        ]);

        let stats_schema = build_min_max_stats_schema(&table_schema);

        assert_eq!(stats_schema.fields().len(), 2); // id and name only
        assert_eq!(stats_schema.field("id").unwrap().name(), "id");
        assert_eq!(
            stats_schema.field("id").unwrap().data_type(),
            &DataType::INTEGER
        );
        assert_eq!(stats_schema.field("name").unwrap().name(), "name");
        assert_eq!(
            stats_schema.field("name").unwrap().data_type(),
            &DataType::STRING
        );
        assert!(stats_schema.field("tags").is_none()); // Arrays excluded
    }

    #[test]
    fn test_build_min_max_schema_nested() {
        let nested_schema = StructType::new_unchecked(vec![
            StructField::new("x", DataType::INTEGER, false),
            StructField::new("y", DataType::STRING, true),
        ]);

        let table_schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("nested", DataType::Struct(Box::new(nested_schema)), true),
        ]);

        let stats_schema = build_min_max_stats_schema(&table_schema);

        assert_eq!(stats_schema.fields().len(), 2);
        assert!(stats_schema.field("id").is_some());
        assert!(stats_schema.field("nested").is_some());

        // Check nested struct
        if let DataType::Struct(nested) = stats_schema.field("nested").unwrap().data_type() {
            assert_eq!(nested.fields().len(), 2);
            assert!(nested.field("x").is_some());
            assert!(nested.field("y").is_some());
        } else {
            panic!("Expected nested to be a struct");
        }
    }

    #[test]
    fn test_build_null_count_schema() {
        let table_schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
                true,
            ),
        ]);

        let stats_schema = build_null_count_stats_schema(&table_schema);

        assert_eq!(stats_schema.fields().len(), 3); // All columns included
        assert_eq!(
            stats_schema.field("id").unwrap().data_type(),
            &DataType::LONG
        );
        assert_eq!(
            stats_schema.field("name").unwrap().data_type(),
            &DataType::LONG
        );
        assert_eq!(
            stats_schema.field("tags").unwrap().data_type(),
            &DataType::LONG
        );
    }

    #[test]
    fn test_generate_stats_parsed_schema() {
        let table_schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        let stats_schema = generate_stats_parsed_schema(&table_schema, true);

        assert_eq!(stats_schema.fields().len(), 5); // numRecords, minValues, maxValues, nullCount, tightBounds
        assert!(stats_schema.field("numRecords").is_some());
        assert!(stats_schema.field("minValues").is_some());
        assert!(stats_schema.field("maxValues").is_some());
        assert!(stats_schema.field("nullCount").is_some());
        assert!(stats_schema.field("tightBounds").is_some());

        // Without tight bounds
        let stats_schema_no_tb = generate_stats_parsed_schema(&table_schema, false);
        assert_eq!(stats_schema_no_tb.fields().len(), 4);
        assert!(stats_schema_no_tb.field("tightBounds").is_none());
    }
}
