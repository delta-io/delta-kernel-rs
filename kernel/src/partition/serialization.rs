//! Partition value serialization for the Delta log.
//!
//! This module converts typed [`Scalar`] partition values into the strings that appear in
//! `AddFile.partitionValues` in the Delta commit log.
//!
//! # When to use this module
//!
//! This module handles **Delta log serialization only**. It is completely independent from
//! the Hive-style file path encoding in the [`hive`] module. The same partition value
//! goes through two separate transformations:
//!
//! ```text
//! Scalar::String("US/East")
//!         |
//!         |  (1) THIS MODULE: serialize for the Delta log
//!         v
//!   Some("US/East")           <- stored in AddFile.partitionValues as-is
//!         |
//!         |  (2) hive module: encode for the file system path
//!         v
//!   "US%2FEast"               <- used in directory name: region=US%2FEast/
//! ```
//!
//! The slash in `"US/East"` is NOT encoded here. This module produces the raw protocol
//! value. File path encoding is a separate concern handled by [`hive`].
//!
//! # Serialization examples
//!
//! ```text
//! Scalar::Integer(42)           -> Some("42")
//! Scalar::Date(20178)           -> Some("2025-03-31")
//! Scalar::Timestamp(micros)     -> Some("2025-03-31T15:30:00.000000Z")
//! Scalar::Float(f32::INFINITY)  -> Some("Infinity")
//! Scalar::Decimal(-5, scale=2)  -> Some("-0.05")
//! Scalar::String("US/East")     -> Some("US/East")        no encoding
//! Scalar::String("")            -> None                    protocol: empty = null
//! Scalar::Null(any)             -> None
//! ```
//!
//! [`Scalar`]: crate::expressions::Scalar
//! [`hive`]: super::hive

use chrono::{DateTime, NaiveDate};

use crate::expressions::Scalar;
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
/// [`hive`] module's [`escape_partition_value`] and [`build_partition_path`].
///
/// [`hive`]: super::hive
/// [`escape_partition_value`]: super::hive::escape_partition_value
/// [`build_partition_path`]: super::hive::build_partition_path
pub fn serialize_partition_value(value: &Scalar) -> DeltaResult<Option<String>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Scalar;
    use crate::schema::{DataType, PrimitiveType};
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
}
