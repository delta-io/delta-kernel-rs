//! Partition key and type validation.
//!
//! Validates that the connector provided the correct partition column names and value types
//! before serialization.
//!
//! ```text
//! Step 1 (THIS MODULE):  {"YEAR": Integer(2024)} --> {"Year": Integer(2024)}
//!                         (case-normalize keys, check types against schema)
//! Step 2 (serialization): Integer(2024) --> "2024"
//! Step 3 (path encoding): "Year=2024/"
//! ```
//!
//! See the encoding tables in the [`super`] module for the full set of partition-eligible
//! types and their expected serializations.
//!
//! The primary entry point is [`validate_partition_values`], which combines key validation
//! and type checking. The two phases are also exposed individually for testing:
//!
//! - [`validate_keys`]: checks key completeness (case-insensitive matching,
//!   normalizes to schema case, detects post-normalization duplicates).
//! - [`validate_types`]: checks that each `Scalar`'s type matches the
//!   partition column's schema type. Null scalars skip the type check.

#![allow(dead_code)] // callers are in a later PR in the stack

use std::collections::HashMap;

use crate::expressions::Scalar;
use crate::schema::{DataType, StructType};
use crate::{DeltaResult, Error};

/// Validates and normalizes partition keys and value types against the table schema.
/// Returns the map re-keyed to schema case.
///
/// This is the primary entry point for partition validation. It combines [`validate_keys`]
/// (key completeness and case normalization) with [`validate_types`] (scalar type checking).
///
/// # Parameters
/// - `logical_partition_columns`: logical partition column names from kernel's table metadata.
/// - `logical_schema`: the table's logical schema.
/// - `logical_partition_values`: connector-provided map from logical column names (any
///   case) to typed values.
pub(crate) fn validate_partition_values(
    logical_partition_columns: &[String],
    logical_schema: &StructType,
    logical_partition_values: HashMap<String, Scalar>,
) -> DeltaResult<HashMap<String, Scalar>> {
    let normalized = validate_keys(logical_partition_columns, logical_partition_values)?;
    validate_types(logical_schema, &normalized)?;
    Ok(normalized)
}

/// Validates that a connector-provided partition value map contains exactly the expected
/// partition columns, with case-insensitive key matching. Returns the map re-keyed to
/// the logical schema case.
///
/// Keys in the input map are logical column names provided by the connector, which may
/// use any casing (e.g., "YEAR" or "year" for a schema column named "Year"). The returned
/// map uses the exact logical names from the schema.
///
/// # Parameters
/// - `logical_partition_columns`: logical partition column names from kernel's table metadata.
/// - `logical_partition_values`: connector-provided map from logical column names (any
///   case) to typed values.
///
/// # Errors
/// - A partition column is missing from the map
/// - An extra key is present that is not a partition column
/// - Two keys collide after case normalization (e.g., "COL" and "col" both provided)
fn validate_keys(
    logical_partition_columns: &[String],
    logical_partition_values: HashMap<String, Scalar>,
) -> DeltaResult<HashMap<String, Scalar>> {
    let schema_lookup: HashMap<String, &str> = logical_partition_columns
        .iter()
        .map(|name| (name.to_lowercase(), name.as_str()))
        .collect();

    let mut normalized = HashMap::with_capacity(logical_partition_values.len());
    for (key, value) in logical_partition_values {
        let lower_key = key.to_lowercase();
        let schema_name = schema_lookup.get(&lower_key).ok_or_else(|| {
            Error::invalid_partition_values(format!(
                "unknown partition column '{key}'. Expected one of: [{}]",
                logical_partition_columns.join(", ")
            ))
        })?;
        // Detect post-normalization duplicates (e.g., "COL" and "col" both provided).
        if normalized.contains_key(*schema_name) {
            return Err(Error::invalid_partition_values(format!(
                "duplicate partition column '{key}' (normalized to same key as a previously provided entry)"
            )));
        }
        normalized.insert(schema_name.to_string(), value);
    }

    for col in logical_partition_columns {
        if !normalized.contains_key(col.as_str()) {
            return Err(Error::invalid_partition_values(format!(
                "missing partition column '{col}'. Provided: [{}]",
                normalized.keys().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
    }

    Ok(normalized)
}

/// Validates that each [`Scalar`] value's type is compatible with the corresponding
/// partition column's type in the table schema. Null scalars skip the type check
/// (null is valid for any partition column type).
///
/// # Parameters
/// - `logical_schema`: the table's logical schema.
/// - `logical_partition_values`: map from logical column names (schema case) to typed
///   values. Keys must use the exact logical names from the schema, as returned by
///   [`validate_keys`]. This function uses case-sensitive schema lookup.
///
/// # Errors
/// - A partition column is not found in the table schema
/// - A partition column's schema type is not a primitive (struct, array, map are rejected)
/// - A non-null value's type does not match the partition column's schema type
fn validate_types(
    logical_schema: &StructType,
    logical_partition_values: &HashMap<String, Scalar>,
) -> DeltaResult<()> {
    for (col_name, value) in logical_partition_values {
        let field = logical_schema.field(col_name).ok_or_else(|| {
            Error::invalid_partition_values(format!(
                "partition column '{col_name}' not found in table schema"
            ))
        })?;
        let expected_type = field.data_type();
        if matches!(
            expected_type,
            DataType::Struct(_) | DataType::Array(_) | DataType::Map(_)
        ) {
            return Err(Error::invalid_partition_values(format!(
                "partition column '{col_name}' has non-primitive type {expected_type:?}. \
                 Partition columns must be primitive types."
            )));
        }
        if value.is_null() {
            continue;
        }
        let actual_type = value.data_type();
        if *expected_type != actual_type {
            return Err(Error::invalid_partition_values(format!(
                "partition column '{col_name}' has type {expected_type:?} but got \
                 value of type {actual_type:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    use crate::expressions::Scalar;
    use crate::schema::{ArrayType, DataType, MapType, StructField};

    fn assert_type_ok(data_type: DataType, value: Scalar) {
        let schema = StructType::new_unchecked(vec![StructField::not_null("p", data_type)]);
        let values = HashMap::from([("p".to_string(), value)]);
        validate_types(&schema, &values).unwrap();
    }

    fn assert_type_err(data_type: DataType, value: Scalar) -> String {
        let schema = StructType::new_unchecked(vec![StructField::not_null("p", data_type)]);
        let values = HashMap::from([("p".to_string(), value)]);
        validate_types(&schema, &values).unwrap_err().to_string()
    }

    // ============================================================================
    // validate_keys
    // ============================================================================

    #[test]
    fn test_validate_partition_keys_matching_keys_returns_ok() {
        let cols = vec!["year".to_string(), "region".to_string()];
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        let result = validate_keys(&cols, values).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("year"), Some(&Scalar::Integer(2024)));
        assert_eq!(result.get("region"), Some(&Scalar::String("US".into())));
    }

    #[test]
    fn test_validate_partition_keys_empty_columns_and_values_returns_ok() {
        let cols: Vec<String> = vec![];
        let values = HashMap::new();
        let result = validate_keys(&cols, values).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_validate_partition_keys_missing_key_returns_error() {
        let cols = vec!["year".to_string(), "region".to_string()];
        let values = HashMap::from([("year".to_string(), Scalar::Integer(2024))]);
        let result = validate_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing partition column 'region'"), "{err}");
    }

    #[test]
    fn test_validate_partition_keys_extra_key_returns_error() {
        let cols = vec!["year".to_string()];
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        let result = validate_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown partition column 'region'"), "{err}");
    }

    #[test]
    fn test_validate_partition_keys_case_normalizes_to_schema_case() {
        let cols = vec!["Year".to_string()];
        let values = HashMap::from([("YEAR".to_string(), Scalar::Integer(2024))]);
        let result = validate_keys(&cols, values).unwrap();
        assert!(result.contains_key("Year"));
        assert!(!result.contains_key("YEAR"));
    }

    #[test]
    fn test_validate_partition_keys_duplicate_after_normalization_returns_error() {
        let cols = vec!["col".to_string()];
        let values = HashMap::from([
            ("COL".to_string(), Scalar::Integer(1)),
            ("col".to_string(), Scalar::Integer(2)),
        ]);
        let result = validate_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
    }

    // ============================================================================
    // validate_types
    // ============================================================================
    //
    // Covers every partition-eligible type from the encoding tables in `partition/mod.rs`.
    // Each row tests that the correct Scalar variant passes type validation against the
    // corresponding schema DataType.

    /// Every non-null row from the "Encoding by type" table in `partition/mod.rs`.
    /// Row numbers reference that table. Validation only checks types (not values), but
    /// covering every row mirrors the reference table 1:1 for completeness.
    #[rstest]
    // INT (rows 1-4)
    #[case(DataType::INTEGER, Scalar::Integer(0))] // row 1
    #[case(DataType::INTEGER, Scalar::Integer(-1))] // row 2
    #[case(DataType::INTEGER, Scalar::Integer(i32::MAX))] // row 3
    #[case(DataType::INTEGER, Scalar::Integer(i32::MIN))] // row 4
    // BIGINT (rows 6-7)
    #[case(DataType::LONG, Scalar::Long(i64::MAX))] // row 6
    #[case(DataType::LONG, Scalar::Long(i64::MIN))] // row 7
    // TINYINT (rows 9-10)
    #[case(DataType::BYTE, Scalar::Byte(127))] // row 9
    #[case(DataType::BYTE, Scalar::Byte(-128))] // row 10
    // SMALLINT (rows 12-13)
    #[case(DataType::SHORT, Scalar::Short(32767))] // row 12
    #[case(DataType::SHORT, Scalar::Short(-32768))] // row 13
    // DOUBLE (rows 15-21)
    #[case(DataType::DOUBLE, Scalar::Double(0.0))] // row 15
    #[case(DataType::DOUBLE, Scalar::Double(-0.0))] // row 16
    #[case(DataType::DOUBLE, Scalar::Double(f64::MAX))] // row 17
    #[case(DataType::DOUBLE, Scalar::Double(f64::MIN_POSITIVE))] // row 18
    #[case(DataType::DOUBLE, Scalar::Double(f64::NAN))] // row 19
    #[case(DataType::DOUBLE, Scalar::Double(f64::INFINITY))] // row 20
    #[case(DataType::DOUBLE, Scalar::Double(f64::NEG_INFINITY))] // row 21
    // FLOAT (rows 23-26)
    #[case(DataType::FLOAT, Scalar::Float(0.0))] // row 23
    #[case(DataType::FLOAT, Scalar::Float(f32::NAN))] // row 24
    #[case(DataType::FLOAT, Scalar::Float(f32::INFINITY))] // row 25
    #[case(DataType::FLOAT, Scalar::Float(f32::NEG_INFINITY))] // row 26
    // BOOLEAN (rows 28-29)
    #[case(DataType::BOOLEAN, Scalar::Boolean(true))] // row 28
    #[case(DataType::BOOLEAN, Scalar::Boolean(false))] // row 29
    // DECIMAL(38,18) (rows 31-33)
    #[case(DataType::decimal(38, 18).unwrap(), Scalar::decimal(0, 38, 18).unwrap())] // row 31
    #[case(DataType::decimal(38, 18).unwrap(), Scalar::decimal(1_230_000_000_000_000_000i128, 38, 18).unwrap())] // row 32
    #[case(DataType::decimal(38, 18).unwrap(), Scalar::decimal(-1_230_000_000_000_000_000i128, 38, 18).unwrap())] // row 33
    // DATE (rows 35-38)
    #[case(DataType::DATE, Scalar::Date(19723))] // row 35: 2024-01-01
    #[case(DataType::DATE, Scalar::Date(0))] // row 36: 1970-01-01 (epoch)
    #[case(DataType::DATE, Scalar::Date(-719_162))] // row 37: 0001-01-01
    #[case(DataType::DATE, Scalar::Date(2_932_896))] // row 38: 9999-12-31
    // TIMESTAMP (rows 40-42)
    #[case(DataType::TIMESTAMP, Scalar::Timestamp(1_718_451_045_000_000))] // row 40
    #[case(DataType::TIMESTAMP, Scalar::Timestamp(0))] // row 41
    #[case(DataType::TIMESTAMP, Scalar::Timestamp(1_718_499_599_999_999))] // row 42
    // TIMESTAMP_NTZ (rows 44-45)
    #[case(DataType::TIMESTAMP_NTZ, Scalar::TimestampNtz(1_718_451_045_000_000))] // row 44
    #[case(DataType::TIMESTAMP_NTZ, Scalar::TimestampNtz(0))] // row 45
    fn test_validate_types_encoding_table_rows_return_ok(
        #[case] data_type: DataType,
        #[case] value: Scalar,
    ) {
        assert_type_ok(data_type, value);
    }

    #[cfg(feature = "nanosecond-timestamps")]
    /// Nanosecond timestamps encoding validation.
    #[rstest]
    #[case(
        DataType::TIMESTAMP_NANOS,
        Scalar::TimestampNanos(1_718_451_045_000_000_000)
    )]
    #[case(DataType::TIMESTAMP_NANOS, Scalar::TimestampNanos(0))]
    #[case(
        DataType::TIMESTAMP_NANOS,
        Scalar::TimestampNanos(1_718_499_599_999_999_999)
    )]
    #[case(DataType::TIMESTAMP_NANOS, Scalar::Null(DataType::TIMESTAMP_NANOS))]
    fn test_validate_nanos_encoding_table_rows_return_ok(
        #[case] data_type: DataType,
        #[case] value: Scalar,
    ) {
        assert_type_ok(data_type, value);
    }

    /// Every non-null row from the "String and binary encoding" table in
    /// `partition/mod.rs`. Row numbers reference that table.
    #[rstest]
    // STRING rows
    #[case(DataType::STRING, Scalar::String("\x00".into()))] // row 47
    #[case(DataType::STRING, Scalar::String("before\x00after".into()))] // row 48
    #[case(DataType::STRING, Scalar::String("a{b".into()))] // row 54
    #[case(DataType::STRING, Scalar::String("a}b".into()))] // row 55
    #[case(DataType::STRING, Scalar::String("hello world".into()))] // row 56
    #[case(DataType::STRING, Scalar::String("M\u{00FC}nchen".into()))] // row 57
    #[case(DataType::STRING, Scalar::String("\u{65E5}\u{672C}\u{8A9E}".into()))] // row 58
    #[case(DataType::STRING, Scalar::String("\u{1F3B5}\u{1F3B6}".into()))] // row 59
    #[case(DataType::STRING, Scalar::String("a<b>c|d".into()))] // row 60
    #[case(DataType::STRING, Scalar::String("a@b!c(d)".into()))] // row 61
    #[case(DataType::STRING, Scalar::String("a&b+c$d;e,f".into()))] // row 62
    #[case(DataType::STRING, Scalar::String("Serbia/srb%".into()))] // row 63
    #[case(DataType::STRING, Scalar::String("100%25".into()))] // row 64
    #[case(DataType::STRING, Scalar::String("".into()))] // row 65
    #[case(DataType::STRING, Scalar::String(" ".into()))] // row 67
    #[case(DataType::STRING, Scalar::String("  ".into()))] // row 68
    // BINARY rows
    #[case(DataType::BINARY, Scalar::Binary(vec![]))] // row 49
    #[case(DataType::BINARY, Scalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]))] // row 50
    #[case(DataType::BINARY, Scalar::Binary(vec![0x48, 0x45, 0x4C, 0x4C, 0x4F]))] // row 51
    #[case(DataType::BINARY, Scalar::Binary(vec![0x00, 0xFF]))] // row 52
    #[case(DataType::BINARY, Scalar::Binary(vec![0x2F, 0x3D, 0x25]))] // row 53
    fn test_validate_types_string_binary_table_rows_return_ok(
        #[case] data_type: DataType,
        #[case] value: Scalar,
    ) {
        assert_type_ok(data_type, value);
    }

    /// NULL rows from the encoding table. Null is valid for every partition column type,
    /// regardless of the Scalar::Null inner type.
    #[rstest]
    #[case(DataType::INTEGER, Scalar::Null(DataType::INTEGER))] // row 5
    #[case(DataType::LONG, Scalar::Null(DataType::LONG))] // row 8
    #[case(DataType::BYTE, Scalar::Null(DataType::BYTE))] // row 11
    #[case(DataType::SHORT, Scalar::Null(DataType::SHORT))] // row 14
    #[case(DataType::DOUBLE, Scalar::Null(DataType::DOUBLE))] // row 22
    #[case(DataType::FLOAT, Scalar::Null(DataType::FLOAT))] // row 27
    #[case(DataType::BOOLEAN, Scalar::Null(DataType::BOOLEAN))] // row 30
    #[case(DataType::decimal(38, 18).unwrap(), Scalar::Null(DataType::decimal(38, 18).unwrap()))] // row 34
    #[case(DataType::DATE, Scalar::Null(DataType::DATE))] // row 39
    #[case(DataType::TIMESTAMP, Scalar::Null(DataType::TIMESTAMP))] // row 43
    #[case(DataType::TIMESTAMP_NTZ, Scalar::Null(DataType::TIMESTAMP_NTZ))] // row 46
    #[case(DataType::STRING, Scalar::Null(DataType::STRING))] // row 66
    #[case(DataType::BINARY, Scalar::Null(DataType::BINARY))] // (binary NULL)
    fn test_validate_types_null_returns_ok(#[case] data_type: DataType, #[case] value: Scalar) {
        assert_type_ok(data_type, value);
    }

    /// Null skips the type check even when the Scalar::Null inner type does not match
    /// the schema column type. This is intentional: null is valid for any partition
    /// column regardless of what type the Null carries.
    #[test]
    fn test_validate_types_null_with_mismatched_inner_type_returns_ok() {
        assert_type_ok(DataType::INTEGER, Scalar::Null(DataType::STRING));
    }

    /// Type mismatch: every non-null Scalar variant against the wrong schema type.
    #[rstest]
    #[case(DataType::STRING, Scalar::Integer(1))]
    #[case(DataType::INTEGER, Scalar::String("x".into()))]
    #[case(DataType::INTEGER, Scalar::Long(1))]
    #[case(DataType::LONG, Scalar::Integer(1))]
    #[case(DataType::DOUBLE, Scalar::Float(1.0))]
    #[case(DataType::FLOAT, Scalar::Double(1.0))]
    #[case(DataType::DATE, Scalar::Timestamp(0))]
    #[case(DataType::TIMESTAMP, Scalar::TimestampNtz(0))]
    #[case(DataType::STRING, Scalar::Binary(vec![0x41]))]
    #[case(DataType::BINARY, Scalar::String("A".into()))]
    #[case(DataType::BOOLEAN, Scalar::Integer(1))]
    fn test_validate_types_mismatch_returns_error(
        #[case] data_type: DataType,
        #[case] value: Scalar,
    ) {
        let err = assert_type_err(data_type, value);
        assert!(err.contains("p"), "{err}");
    }

    /// Complex types (struct, array, map) are rejected as partition column types.
    #[rstest]
    #[case(
        DataType::Struct(Box::new(StructType::new_unchecked(vec![
            StructField::not_null("x", DataType::INTEGER),
        ]))),
        Scalar::Null(DataType::STRING)
    )]
    #[case(
        DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
        Scalar::Null(DataType::STRING)
    )]
    #[case(
        DataType::Map(Box::new(MapType::new(DataType::STRING, DataType::INTEGER, false))),
        Scalar::Null(DataType::STRING)
    )]
    fn test_validate_types_complex_type_returns_error(
        #[case] data_type: DataType,
        #[case] value: Scalar,
    ) {
        let err = assert_type_err(data_type, value);
        assert!(err.contains("non-primitive type"), "{err}");
    }

    #[test]
    fn test_validate_types_column_not_in_schema_returns_error() {
        let schema =
            StructType::new_unchecked(vec![StructField::not_null("year", DataType::INTEGER)]);
        let values = HashMap::from([("nonexistent".to_string(), Scalar::Integer(42))]);
        let result = validate_types(&schema, &values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found in table schema"), "{err}");
    }

    /// validate_partition_values normalizes keys to schema case and type-checks in one call.
    #[test]
    fn test_validate_partition_values_with_case_mismatch_succeeds() {
        let partition_cols = vec!["Year".to_string(), "Region".to_string()];
        let schema = StructType::new_unchecked(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::not_null("Year", DataType::INTEGER),
            StructField::nullable("Region", DataType::STRING),
        ]);
        let values = HashMap::from([
            ("YEAR".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        let result = validate_partition_values(&partition_cols, &schema, values).unwrap();
        assert_eq!(result.get("Year"), Some(&Scalar::Integer(2024)));
        assert_eq!(result.get("Region"), Some(&Scalar::String("US".into())));
    }
}
