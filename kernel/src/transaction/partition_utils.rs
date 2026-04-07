//! Partition value serialization and validation utilities for the write path.
//!
//! These are engine-agnostic helpers used by [`Transaction::partitioned_write_context`] to
//! validate, serialize, and translate partition values before they are stored in a
//! [`WriteContext`].
//!
//! [`Transaction::partitioned_write_context`]: super::Transaction::partitioned_write_context
//! [`WriteContext`]: super::WriteContext

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate};

use crate::expressions::Scalar;
use crate::schema::StructType;
use crate::{DeltaResult, Error};

// === Partition Value Serialization ===
//
// Serialization rules per Delta protocol spec (Partition Value Serialization section):
//
// Type           Serialization                             Notes
// Null           None
// String         as-is; "" -> None
// Boolean        "true" / "false"
// Byte           to_string()
// Short          to_string()
// Integer        to_string()
// Long           to_string()
// Float          Java Float.toString() format              format_f32
// Double         Java Double.toString() format             format_f64
// Date           "YYYY-MM-DD"                              chrono
// Timestamp      "YYYY-MM-DDTHH:MM:SS.ffffffZ" (ISO 8601) ISO 8601 (T separator, microseconds, Z suffix)
// TimestampNtz   "YYYY-MM-DD HH:MM:SS.ffffff" (space)
// Decimal        "42.00", "-0.05"                          preserves scale
// Binary         Raw UTF-8: String::from_utf8              strict, error on invalid
// Struct/Array/Map  Error                                  not valid partition types

/// Serializes a [`Scalar`] partition value to a protocol-compliant string for the
/// `partitionValues` map in Add actions.
///
/// Returns `Ok(None)` for null values and empty strings (the Delta protocol treats both as
/// null partition values). Returns `Err` for types that cannot be partition columns (Struct,
/// Array, Map) or for binary values that are not valid UTF-8.
///
/// This function is for producing values in the `partitionValues` JSON map in commit log
/// entries. It is NOT for Hive-style file path encoding. For path encoding, see the
/// [`partition`] module's [`escape_partition_value`] and [`build_partition_path`].
///
/// [`partition`]: crate::partition
/// [`escape_partition_value`]: crate::partition::escape_partition_value
/// [`build_partition_path`]: crate::partition::build_partition_path
pub(crate) fn serialize_partition_value(value: &Scalar) -> DeltaResult<Option<String>> {
    match value {
        Scalar::Null(_) => Ok(None),
        Scalar::String(s) => Ok(if s.is_empty() { None } else { Some(s.clone()) }),
        Scalar::Boolean(v) => Ok(Some(v.to_string())),
        Scalar::Byte(v) => Ok(Some(v.to_string())),
        Scalar::Short(v) => Ok(Some(v.to_string())),
        Scalar::Integer(v) => Ok(Some(v.to_string())),
        Scalar::Long(v) => Ok(Some(v.to_string())),
        Scalar::Float(v) => Ok(Some(format_f32(*v))),
        Scalar::Double(v) => Ok(Some(format_f64(*v))),
        Scalar::Date(days) => Ok(Some(format_date(*days)?)),
        Scalar::Timestamp(us) => Ok(Some(format_timestamp(*us)?)),
        Scalar::TimestampNtz(us) => Ok(Some(format_timestamp_ntz(*us)?)),
        Scalar::Decimal(d) => Ok(Some(format_decimal(d))),
        Scalar::Binary(b) => {
            if b.is_empty() {
                Ok(None)
            } else {
                Ok(Some(format_binary(b)?))
            }
        }
        Scalar::Struct(_) | Scalar::Array(_) | Scalar::Map(_) => Err(Error::generic(format!(
            "cannot serialize partition value: type {:?} is not a valid partition column type",
            value.data_type()
        ))),
    }
}

// === Float/Double: match Java's Float.toString() / Double.toString() ===
// Decimal notation for [1e-3, 1e7), scientific notation otherwise.
// No new dependencies needed.

fn format_f64(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let abs = v.abs();
    if (1e-3..1e7).contains(&abs) {
        let s = v.to_string();
        if s.contains('.') {
            s
        } else {
            format!("{s}.0")
        }
    } else {
        let s = format!("{v:e}");
        let s = s.replace('e', "E");
        if s.contains('.') {
            s
        } else {
            s.replacen('E', ".0E", 1)
        }
    }
}

fn format_f32(v: f32) -> String {
    // Identical logic to format_f64 but operates on f32 to preserve precision.
    // f32::MAX formats as "3.4028235E38" natively but "3.4028234663852886E38" when cast to f64.
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let abs = v.abs();
    if (1e-3..1e7).contains(&abs) {
        let s = v.to_string();
        if s.contains('.') {
            s
        } else {
            format!("{s}.0")
        }
    } else {
        let s = format!("{v:e}");
        let s = s.replace('e', "E");
        if s.contains('.') {
            s
        } else {
            s.replacen('E', ".0E", 1)
        }
    }
}

/// Formats a date value (days since UNIX epoch) as "YYYY-MM-DD".
fn format_date(days: i32) -> DeltaResult<String> {
    const UNIX_EPOCH_CE_DAYS: i32 = 719_163;
    let ce_days = UNIX_EPOCH_CE_DAYS.checked_add(days).ok_or_else(|| {
        Error::generic(format!("date value {days} days from epoch is out of range"))
    })?;
    NaiveDate::from_num_days_from_ce_opt(ce_days)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .ok_or_else(|| Error::generic(format!("date value {days} days from epoch is out of range")))
}

/// Converts microseconds since epoch to a [`DateTime`], returning an error if out of range.
fn micros_to_datetime(micros: i64, label: &str) -> DeltaResult<DateTime<chrono::Utc>> {
    let secs = micros.div_euclid(1_000_000);
    let subsec_nanos = (micros.rem_euclid(1_000_000) as u32) * 1000;
    DateTime::from_timestamp(secs, subsec_nanos).ok_or_else(|| {
        Error::generic(format!(
            "{label} value {micros} microseconds from epoch is out of range"
        ))
    })
}

/// Formats a timestamp (microseconds since epoch) as ISO 8601: "YYYY-MM-DDTHH:MM:SS.ffffffZ".
/// ISO 8601 format matching modern Delta writers (T separator, microseconds, Z suffix).
fn format_timestamp(micros: i64) -> DeltaResult<String> {
    micros_to_datetime(micros, "timestamp")
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
}

/// Formats a timestamp without timezone (microseconds) as "YYYY-MM-DD HH:MM:SS.ffffff".
/// Space separator, no Z suffix (there is no timezone).
fn format_timestamp_ntz(micros: i64) -> DeltaResult<String> {
    micros_to_datetime(micros, "timestamp_ntz")
        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

/// Formats a decimal value preserving scale (trailing zeros).
/// decimal(10,2) with value 42 serializes as "42.00", not "42".
/// Correctly handles values in the (-1, 0) range (e.g., -0.05).
fn format_decimal(d: &crate::expressions::DecimalData) -> String {
    let scale = d.scale();
    if scale == 0 {
        return d.bits().to_string();
    }
    let sign = if d.bits() < 0 { "-" } else { "" };
    let abs = d.bits().unsigned_abs();
    let divisor = 10_u128.pow(scale as u32);
    let int_part = abs / divisor;
    let frac_part = abs % divisor;
    format!(
        "{sign}{int_part}.{frac_part:0>width$}",
        width = scale as usize
    )
}

/// Formats binary data as raw UTF-8, matching Java's `new String(bytes, UTF_8)`.
/// Returns an error if the bytes are not valid UTF-8.
fn format_binary(bytes: &[u8]) -> DeltaResult<String> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|e| Error::generic(format!("binary partition value is not valid UTF-8: {e}")))
}

// === Partition Key Validation ===

/// Validates that `partition_values` contains exactly the expected partition columns,
/// with case-insensitive key matching. Returns the map re-keyed to schema case.
///
/// Errors if:
/// - A partition column is missing from the map
/// - An extra key is present that is not a partition column
/// - Two keys collide after case normalization (e.g., "COL" and "col" both provided)
pub(crate) fn validate_partition_keys(
    logical_partition_columns: &[String],
    partition_values: HashMap<String, Scalar>,
) -> DeltaResult<HashMap<String, Scalar>> {
    // Build a case-insensitive lookup from the schema's partition column names.
    let schema_lookup: HashMap<String, &str> = logical_partition_columns
        .iter()
        .map(|name| (name.to_lowercase(), name.as_str()))
        .collect();

    let mut normalized = HashMap::with_capacity(partition_values.len());
    for (key, value) in partition_values {
        let lower_key = key.to_lowercase();
        let schema_name = schema_lookup.get(&lower_key).ok_or_else(|| {
            Error::generic(format!(
                "unknown partition column '{key}'. Expected one of: [{}]",
                logical_partition_columns.join(", ")
            ))
        })?;
        // Detect post-normalization duplicates (e.g., "COL" and "col" both provided).
        if normalized.contains_key(*schema_name) {
            return Err(Error::generic(format!(
                "duplicate partition column '{key}' (case-insensitive match with existing key)"
            )));
        }
        normalized.insert(schema_name.to_string(), value);
    }

    // Check that all partition columns are present.
    for col in logical_partition_columns {
        if !normalized.contains_key(col.as_str()) {
            return Err(Error::generic(format!(
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
pub(crate) fn validate_partition_value_types(
    logical_schema: &StructType,
    partition_values: &HashMap<String, Scalar>,
) -> DeltaResult<()> {
    for (col_name, value) in partition_values {
        if value.is_null() {
            continue;
        }
        let field = logical_schema.field(col_name).ok_or_else(|| {
            Error::generic(format!(
                "partition column '{col_name}' not found in table schema"
            ))
        })?;
        let expected_type = field.data_type();
        let actual_type = value.data_type();
        if *expected_type != actual_type {
            return Err(Error::generic(format!(
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
    use crate::expressions::Scalar;
    use crate::schema::{DataType, PrimitiveType, StructField};
    use chrono::TimeZone;

    // === serialize_partition_value tests ===

    #[test]
    fn test_serialize_partition_value_null_returns_none() {
        let result = serialize_partition_value(&Scalar::Null(DataType::STRING)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_serialize_partition_value_null_integer_returns_none() {
        let result = serialize_partition_value(&Scalar::Null(DataType::INTEGER)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_serialize_partition_value_null_complex_type_returns_none() {
        // Null serialization returns None regardless of inner type. Complex-typed partition
        // columns are rejected by validate_partition_value_types before reaching serialization.
        let result =
            serialize_partition_value(&Scalar::Null(DataType::struct_type_unchecked(vec![])));
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_serialize_partition_value_string_returns_value() {
        let result = serialize_partition_value(&Scalar::String("hello".into())).unwrap();
        assert_eq!(result, Some("hello".into()));
    }

    #[test]
    fn test_serialize_partition_value_empty_string_returns_none() {
        let result = serialize_partition_value(&Scalar::String(String::new())).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_serialize_partition_value_boolean_true() {
        assert_eq!(
            serialize_partition_value(&Scalar::Boolean(true)).unwrap(),
            Some("true".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_boolean_false() {
        assert_eq!(
            serialize_partition_value(&Scalar::Boolean(false)).unwrap(),
            Some("false".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_byte() {
        assert_eq!(
            serialize_partition_value(&Scalar::Byte(42)).unwrap(),
            Some("42".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Byte(-128)).unwrap(),
            Some("-128".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_short() {
        assert_eq!(
            serialize_partition_value(&Scalar::Short(1234)).unwrap(),
            Some("1234".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_integer() {
        assert_eq!(
            serialize_partition_value(&Scalar::Integer(42)).unwrap(),
            Some("42".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Integer(-2_147_483_648)).unwrap(),
            Some("-2147483648".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_long() {
        assert_eq!(
            serialize_partition_value(&Scalar::Long(9_876_543_210)).unwrap(),
            Some("9876543210".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_float_normal() {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(3.125)).unwrap(),
            Some("3.125".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_float_nan() {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(f32::NAN)).unwrap(),
            Some("NaN".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_float_infinity() {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(f32::INFINITY)).unwrap(),
            Some("Infinity".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Float(f32::NEG_INFINITY)).unwrap(),
            Some("-Infinity".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_float_zero() {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(0.0)).unwrap(),
            Some("0.0".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Float(-0.0)).unwrap(),
            Some("-0.0".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_float_scientific_notation() {
        // Values outside [1e-3, 1e7) use scientific notation
        assert_eq!(
            serialize_partition_value(&Scalar::Float(1e-4)).unwrap(),
            Some("1.0E-4".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Float(1e8)).unwrap(),
            Some("1.0E8".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_double_normal() {
        assert_eq!(
            serialize_partition_value(&Scalar::Double(3.125)).unwrap(),
            Some("3.125".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_double_nan() {
        assert_eq!(
            serialize_partition_value(&Scalar::Double(f64::NAN)).unwrap(),
            Some("NaN".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_double_infinity() {
        assert_eq!(
            serialize_partition_value(&Scalar::Double(f64::INFINITY)).unwrap(),
            Some("Infinity".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Double(f64::NEG_INFINITY)).unwrap(),
            Some("-Infinity".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_double_zero() {
        assert_eq!(
            serialize_partition_value(&Scalar::Double(0.0)).unwrap(),
            Some("0.0".into())
        );
        assert_eq!(
            serialize_partition_value(&Scalar::Double(-0.0)).unwrap(),
            Some("-0.0".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_date() {
        // 20178 days since epoch = 2025-03-31
        assert_eq!(
            serialize_partition_value(&Scalar::Date(20178)).unwrap(),
            Some("2025-03-31".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_date_pre_epoch() {
        assert_eq!(
            serialize_partition_value(&Scalar::Date(-1)).unwrap(),
            Some("1969-12-31".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_date_epoch() {
        assert_eq!(
            serialize_partition_value(&Scalar::Date(0)).unwrap(),
            Some("1970-01-01".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_timestamp_iso8601() {
        // 2025-03-31T15:30:00.000000Z in micros
        let ts = chrono::NaiveDate::from_ymd_opt(2025, 3, 31)
            .unwrap()
            .and_hms_opt(15, 30, 0)
            .unwrap();
        let micros = chrono::Utc
            .from_utc_datetime(&ts)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_microseconds()
            .unwrap();
        assert_eq!(
            serialize_partition_value(&Scalar::Timestamp(micros)).unwrap(),
            Some("2025-03-31T15:30:00.000000Z".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_timestamp_with_micros() {
        let ts = chrono::NaiveDate::from_ymd_opt(2025, 3, 31)
            .unwrap()
            .and_hms_micro_opt(15, 30, 0, 123456)
            .unwrap();
        let micros = chrono::Utc
            .from_utc_datetime(&ts)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_microseconds()
            .unwrap();
        assert_eq!(
            serialize_partition_value(&Scalar::Timestamp(micros)).unwrap(),
            Some("2025-03-31T15:30:00.123456Z".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_timestamp_ntz_uses_space_separator() {
        let ts = chrono::NaiveDate::from_ymd_opt(2025, 3, 31)
            .unwrap()
            .and_hms_micro_opt(15, 30, 0, 123456)
            .unwrap();
        let micros = chrono::Utc
            .from_utc_datetime(&ts)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_microseconds()
            .unwrap();
        let result = serialize_partition_value(&Scalar::TimestampNtz(micros)).unwrap();
        assert_eq!(result, Some("2025-03-31 15:30:00.123456".into()));
        // Verify no T or Z
        let s = result.unwrap();
        assert!(!s.contains('T'));
        assert!(!s.contains('Z'));
    }

    #[test]
    fn test_serialize_partition_value_decimal_with_scale() {
        let d = Scalar::decimal(12345, 5, 2).unwrap();
        assert_eq!(
            serialize_partition_value(&d).unwrap(),
            Some("123.45".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_decimal_negative_with_scale() {
        let d = Scalar::decimal(-12345i128, 5, 2).unwrap();
        assert_eq!(
            serialize_partition_value(&d).unwrap(),
            Some("-123.45".into())
        );
    }

    #[test]
    fn test_serialize_partition_value_decimal_negative_between_zero_and_one_preserves_sign() {
        let d = Scalar::decimal(-5i128, 3, 2).unwrap();
        assert_eq!(serialize_partition_value(&d).unwrap(), Some("-0.05".into()));
    }

    #[test]
    fn test_serialize_partition_value_decimal_preserves_trailing_zeros() {
        let d = Scalar::decimal(4200i128, 5, 2).unwrap();
        assert_eq!(serialize_partition_value(&d).unwrap(), Some("42.00".into()));
    }

    #[test]
    fn test_serialize_partition_value_decimal_zero_scale() {
        let d = Scalar::decimal(42i128, 5, 0).unwrap();
        assert_eq!(serialize_partition_value(&d).unwrap(), Some("42".into()));
    }

    #[test]
    fn test_serialize_partition_value_binary_valid_utf8() {
        let result = serialize_partition_value(&Scalar::Binary(b"Hello".to_vec())).unwrap();
        assert_eq!(result, Some("Hello".into()));
    }

    #[test]
    fn test_serialize_partition_value_binary_invalid_utf8_returns_error() {
        let result = serialize_partition_value(&Scalar::Binary(vec![0xFF, 0xFE]));
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_partition_value_empty_binary_returns_none() {
        let result = serialize_partition_value(&Scalar::Binary(vec![])).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_serialize_partition_value_null_with_array_type_returns_none() {
        // Cannot construct a non-null Array Scalar (ArrayData has private fields), so we
        // verify that Null(Array) still returns Ok(None).
        let val = Scalar::Null(DataType::Array(Box::new(crate::schema::ArrayType::new(
            DataType::INTEGER,
            false,
        ))));
        assert_eq!(serialize_partition_value(&val).unwrap(), None);
    }

    // === Roundtrip tests: serialize then parse ===

    #[test]
    fn test_serialize_partition_value_date_roundtrip_with_parse_scalar() {
        let days = 20178i32;
        let serialized = serialize_partition_value(&Scalar::Date(days))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::Date.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, Scalar::Date(days));
    }

    #[test]
    fn test_serialize_partition_value_integer_roundtrip_with_parse_scalar() {
        let val = 42;
        let serialized = serialize_partition_value(&Scalar::Integer(val))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::Integer.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, Scalar::Integer(val));
    }

    #[test]
    fn test_serialize_partition_value_timestamp_ntz_roundtrip_with_parse_scalar() {
        let ts = chrono::NaiveDate::from_ymd_opt(2025, 3, 31)
            .unwrap()
            .and_hms_micro_opt(15, 30, 0, 123456)
            .unwrap();
        let micros = chrono::Utc
            .from_utc_datetime(&ts)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_microseconds()
            .unwrap();
        let serialized = serialize_partition_value(&Scalar::TimestampNtz(micros))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::TimestampNtz
            .parse_scalar(&serialized)
            .unwrap();
        assert_eq!(parsed, Scalar::TimestampNtz(micros));
    }

    #[test]
    fn test_serialize_partition_value_timestamp_roundtrip_with_parse_scalar() {
        let ts = chrono::NaiveDate::from_ymd_opt(2025, 3, 31)
            .unwrap()
            .and_hms_micro_opt(15, 30, 0, 123456)
            .unwrap();
        let micros = chrono::Utc
            .from_utc_datetime(&ts)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_microseconds()
            .unwrap();
        let serialized = serialize_partition_value(&Scalar::Timestamp(micros))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::Timestamp.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, Scalar::Timestamp(micros));
    }

    // === validate_partition_keys tests ===

    #[test]
    fn test_validate_partition_keys_matching_keys_returns_ok() {
        let cols = vec!["year".to_string(), "region".to_string()];
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        let result = validate_partition_keys(&cols, values).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("year"), Some(&Scalar::Integer(2024)));
    }

    #[test]
    fn test_validate_partition_keys_missing_key_returns_error() {
        let cols = vec!["year".to_string(), "region".to_string()];
        let values = HashMap::from([("year".to_string(), Scalar::Integer(2024))]);
        let result = validate_partition_keys(&cols, values);
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
        let result = validate_partition_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown partition column 'region'"), "{err}");
    }

    #[test]
    fn test_validate_partition_keys_case_normalizes_to_schema_case() {
        let cols = vec!["Year".to_string()];
        let values = HashMap::from([("YEAR".to_string(), Scalar::Integer(2024))]);
        let result = validate_partition_keys(&cols, values).unwrap();
        // Key should be normalized to "Year" (the schema case), not "YEAR"
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
        let result = validate_partition_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
    }

    // === validate_partition_value_types tests ===

    #[test]
    fn test_validate_partition_value_types_matching_types_returns_ok() {
        let schema = StructType::new_unchecked(vec![
            StructField::not_null("year", DataType::INTEGER),
            StructField::nullable("region", DataType::STRING),
        ]);
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        validate_partition_value_types(&schema, &values).unwrap();
    }

    #[test]
    fn test_validate_partition_value_types_mismatch_returns_error() {
        let schema =
            StructType::new_unchecked(vec![StructField::not_null("year", DataType::INTEGER)]);
        let values = HashMap::from([("year".to_string(), Scalar::String("2024".into()))]);
        let result = validate_partition_value_types(&schema, &values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("year"), "{err}");
    }

    #[test]
    fn test_validate_partition_value_types_null_skips_type_check() {
        let schema =
            StructType::new_unchecked(vec![StructField::not_null("year", DataType::INTEGER)]);
        let values = HashMap::from([("year".to_string(), Scalar::Null(DataType::STRING))]);
        // Null skips the type check even though the null's inner type is STRING, not INTEGER.
        validate_partition_value_types(&schema, &values).unwrap();
    }
}
