//! Step 1: Partition value serialization for the Delta log.
//!
//! A partition value goes through several transformation steps before reaching the
//! Delta log (see the [`super`] module for the full pipeline and encoding tables).
//! This module implements Step 1: converting typed [`Scalar`] values into the strings
//! that appear in `AddFile.partitionValues`.
//!
//! ```text
//! Step 1 (THIS MODULE):  Scalar::String("US/East")  ->Some("US/East")  (partitionValues)
//! Step 2 (hive module):  "US/East"                  ->"US%2FEast"      (directory name)
//! ```
//!
//! [`Scalar`]: crate::expressions::Scalar

#![allow(dead_code)] // callers are in a later PR in the stack

use chrono::{DateTime, NaiveDate, Utc};

use crate::expressions::{DecimalData, Scalar};
use crate::{DeltaResult, Error};

// === Partition Value Serialization ===
//
// Serialization rules per Delta protocol spec (Partition Value Serialization section).
// Float and Double formatting aligns with Java's Float.toString() / Double.toString()
// format, which is also what Delta-Spark uses (the reference table in mod.rs was
// generated from Delta-Spark). The only known divergence is -0.0: Delta-Spark
// normalizes it to "0.0" at the SQL layer, while Java's toString() returns "-0.0".
//
// Type              Serialization                           Notes
// Null              None
// String            as-is; "" -> None
// Boolean           "true" / "false"
// Byte/Short/Int    to_string()
// Long              to_string()
// Float             Java Float.toString() format            format_java_float! macro
// Double            Java Double.toString() format           format_java_float! macro
// Date              "YYYY-MM-DD"
// Timestamp         "YYYY-MM-DDTHH:MM:SS.ffffffZ"          ISO 8601 with microseconds
// TimestampNtz      "YYYY-MM-DD HH:MM:SS.ffffff"            space separator, no Z
// Decimal           "42.00", "-0.05"                        preserves scale
// Binary            Raw UTF-8: String::from_utf8            strict, error on invalid
// Struct/Array/Map  Error                                   not valid partition types

/// Serializes a [`Scalar`] partition value to a protocol-compliant string for the
/// `partitionValues` map in Add actions.
///
/// Returns `Ok(None)` for null values (regardless of the null's data type), empty strings,
/// and empty binary (the Delta protocol treats all three as null partition values). Returns
/// `Err` for non-null values of types that cannot be partition columns (Struct, Array, Map)
/// or for binary values that are not valid UTF-8.
///
/// The inverse of [`PrimitiveType::parse_scalar`].
///
/// [`PrimitiveType::parse_scalar`]: crate::schema::PrimitiveType::parse_scalar
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

// === Float/Double formatting ===
//
// Matches Java's Float.toString() / Double.toString(): decimal notation for values
// in [1e-3, 1e7), scientific notation otherwise. Separate f32/f64 entry points
// preserve native precision (f32::MAX formats as "3.4028235E38" natively but
// "3.4028234663852886E38" when cast to f64).

// This macro uses `return` and must only be used as the sole body of a function.
macro_rules! format_java_float {
    ($v:expr) => {{
        let v = $v;
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
            let s = format!("{v:e}").replace('e', "E");
            if s.contains('.') {
                s
            } else {
                s.replacen('E', ".0E", 1)
            }
        }
    }};
}

fn format_f32(v: f32) -> String {
    format_java_float!(v)
}

fn format_f64(v: f64) -> String {
    format_java_float!(v)
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
fn micros_to_datetime(micros: i64, label: &str) -> DeltaResult<DateTime<Utc>> {
    let secs = micros.div_euclid(1_000_000);
    let subsec_nanos = (micros.rem_euclid(1_000_000) as u32) * 1000;
    DateTime::from_timestamp(secs, subsec_nanos).ok_or_else(|| {
        Error::generic(format!(
            "{label} value {micros} microseconds from epoch is out of range"
        ))
    })
}

/// Formats a timestamp (microseconds since epoch) as ISO 8601: "YYYY-MM-DDTHH:MM:SS.ffffffZ".
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
/// For example, decimal(10,2) with value 42 serializes as "42.00", not "42".
fn format_decimal(d: &DecimalData) -> String {
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
///
/// Returns an error if the bytes are not valid UTF-8. For example, `[0x48, 0x49]`
/// ("HI") succeeds, but `[0xDE, 0xAD]` fails because those bytes are not valid UTF-8.
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
    use rstest::rstest;

    // ============================================================================
    // Spark reference table: Type encoding (mod.rs rows 1-46)
    // ============================================================================
    //
    // Tests validate serialize_partition_value against the `partitionValues.p` column
    // from the encoding table in partition/mod.rs, derived from real Spark-written
    // Delta tables. Divergences from Spark are noted inline.

    /// Null partition values return None regardless of type (rows 5, 8, 11, 14, 22,
    /// 27, 30, 39, 43, 46).
    #[rstest]
    #[case::row05_int(DataType::INTEGER)]
    #[case::row08_bigint(DataType::LONG)]
    #[case::row11_tinyint(DataType::BYTE)]
    #[case::row14_smallint(DataType::SHORT)]
    #[case::row22_double(DataType::DOUBLE)]
    #[case::row27_float(DataType::FLOAT)]
    #[case::row30_boolean(DataType::BOOLEAN)]
    #[case::row39_date(DataType::DATE)]
    #[case::row43_timestamp(DataType::TIMESTAMP)]
    #[case::row46_timestamp_ntz(DataType::TIMESTAMP_NTZ)]
    fn test_spark_ref_null_returns_none(#[case] dtype: DataType) {
        assert_eq!(
            serialize_partition_value(&Scalar::Null(dtype)).unwrap(),
            None
        );
    }

    /// Row 34: DECIMAL(38,18) NULL -> null.
    #[test]
    fn test_spark_ref_row34_decimal_null_returns_none() {
        let dtype = DataType::decimal(38, 18).unwrap();
        assert_eq!(
            serialize_partition_value(&Scalar::Null(dtype)).unwrap(),
            None
        );
    }

    /// Null serialization returns None even for complex types. Complex-typed partition
    /// columns should be rejected by validation before reaching serialization.
    #[test]
    fn test_serialize_partition_value_null_complex_type_returns_none() {
        let val = Scalar::Null(DataType::Array(Box::new(crate::schema::ArrayType::new(
            DataType::INTEGER,
            false,
        ))));
        assert_eq!(serialize_partition_value(&val).unwrap(), None);
    }

    /// INT (rows 1-4), BIGINT (rows 6-7), TINYINT (rows 9-10), SMALLINT (rows 12-13).
    #[rstest]
    #[case::row01_int_zero(Scalar::Integer(0), "0")]
    #[case::row02_int_neg(Scalar::Integer(-1), "-1")]
    #[case::row03_int_max(Scalar::Integer(i32::MAX), "2147483647")]
    #[case::row04_int_min(Scalar::Integer(i32::MIN), "-2147483648")]
    #[case::row06_bigint_max(Scalar::Long(i64::MAX), "9223372036854775807")]
    #[case::row07_bigint_min(Scalar::Long(i64::MIN), "-9223372036854775808")]
    #[case::row09_tinyint_max(Scalar::Byte(i8::MAX), "127")]
    #[case::row10_tinyint_min(Scalar::Byte(i8::MIN), "-128")]
    #[case::row12_smallint_max(Scalar::Short(i16::MAX), "32767")]
    #[case::row13_smallint_min(Scalar::Short(i16::MIN), "-32768")]
    fn test_spark_ref_integer_types(#[case] input: Scalar, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&input).unwrap(),
            Some(expected.to_string())
        );
    }

    /// DOUBLE (rows 15-21).
    ///
    /// Row 16 divergence: Spark normalizes -0.0 to "0.0" at the SQL expression layer,
    /// but Java's Double.toString(-0.0) returns "-0.0". We match Java's behavior since
    /// the Delta spec references Java's toString format.
    #[rstest]
    #[case::row15_zero(0.0, "0.0")]
    #[case::row16_neg_zero(-0.0, "-0.0")] // Spark: "0.0" (SQL-level normalization)
    #[case::row17_max(f64::MAX, "1.7976931348623157E308")]
    #[case::row17b_min_positive_normal(f64::MIN_POSITIVE, "2.2250738585072014E-308")]
    // Row 18: Spark shows "4.9E-324" (Java's Double.MIN_VALUE = smallest subnormal).
    // Rust formats the same bit pattern as "5E-324" due to a different shortest-
    // representation algorithm. Both parse back to the same f64. This is a known
    // Rust-vs-Java formatting divergence for subnormal values.
    #[case::row18_min_subnormal(5e-324f64, "5.0E-324")]
    #[case::row19_nan(f64::NAN, "NaN")]
    #[case::row20_inf(f64::INFINITY, "Infinity")]
    #[case::row21_neg_inf(f64::NEG_INFINITY, "-Infinity")]
    fn test_spark_ref_double(#[case] input: f64, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Double(input)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// FLOAT (rows 23-26).
    #[rstest]
    #[case::row23_zero(0.0f32, "0.0")]
    #[case::row24_nan(f32::NAN, "NaN")]
    #[case::row25_inf(f32::INFINITY, "Infinity")]
    #[case::row26_neg_inf(f32::NEG_INFINITY, "-Infinity")]
    fn test_spark_ref_float(#[case] input: f32, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(input)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// BOOLEAN (rows 28-29).
    #[rstest]
    #[case::row28_true(true, "true")]
    #[case::row29_false(false, "false")]
    fn test_spark_ref_boolean(#[case] input: bool, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Boolean(input)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// DECIMAL(38,18) (rows 31-33). Spark always preserves the full scale.
    #[rstest]
    #[case::row31_zero(0, "0.000000000000000000")]
    #[case::row32_positive(1_230_000_000_000_000_000, "1.230000000000000000")]
    #[case::row33_negative(-1_230_000_000_000_000_000, "-1.230000000000000000")]
    fn test_spark_ref_decimal_38_18(#[case] bits: i128, #[case] expected: &str) {
        let d = Scalar::decimal(bits, 38, 18).unwrap();
        assert_eq!(
            serialize_partition_value(&d).unwrap(),
            Some(expected.to_string())
        );
    }

    /// DATE (rows 35-38). Days since UNIX epoch precomputed from chrono.
    #[rstest]
    #[case::row35_recent(19723, "2024-01-01")]
    #[case::row36_epoch(0, "1970-01-01")]
    #[case::row37_year_one(-719_162, "0001-01-01")]
    #[case::row38_year_9999(2_932_896, "9999-12-31")]
    fn test_spark_ref_date(#[case] days: i32, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Date(days)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// TIMESTAMP (rows 40-42). Microseconds since epoch precomputed from chrono.
    /// The Spark reference table was generated in PDT (UTC-7), so the input local times
    /// are shifted to UTC in partitionValues. Our Scalar::Timestamp already stores UTC
    /// microseconds, so we use the UTC values directly.
    #[rstest]
    #[case::row40_afternoon(1_718_479_845_000_000, "2024-06-15T19:30:45.000000Z")]
    #[case::row41_epoch_offset(28_800_000_000, "1970-01-01T08:00:00.000000Z")]
    #[case::row42_with_micros(1_718_521_199_999_999, "2024-06-16T06:59:59.999999Z")]
    fn test_spark_ref_timestamp(#[case] micros: i64, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Timestamp(micros)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// TIMESTAMP_NTZ (rows 44-45). Microseconds since epoch precomputed from chrono.
    ///
    /// Divergence: Spark omits ".000000" when sub-seconds are zero (e.g.,
    /// "2024-06-15 12:30:45"), but our implementation always includes microsecond
    /// precision ("2024-06-15 12:30:45.000000"). Both formats are valid per the Delta
    /// spec and round-trip correctly through parse_scalar.
    #[rstest]
    #[case::row44_afternoon(1_718_454_645_000_000, "2024-06-15 12:30:45.000000")]
    #[case::row45_epoch(0, "1970-01-01 00:00:00.000000")]
    fn test_spark_ref_timestamp_ntz(#[case] micros: i64, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::TimestampNtz(micros)).unwrap(),
            Some(expected.to_string())
        );
    }

    // ============================================================================
    // Spark reference table: String and binary encoding (mod.rs rows 47-68)
    // ============================================================================
    //
    // Tests validate the `partitionValues.p` column for string/binary inputs.
    // partitionValues stores raw values with no encoding.

    /// String values pass through as-is with no encoding (rows 54-64, 67-68).
    #[rstest]
    #[case::row54_left_brace("a{b", "a{b")]
    #[case::row55_right_brace("a}b", "a}b")]
    #[case::row56_space("hello world", "hello world")]
    #[case::row57_umlaut("M\u{00FC}nchen", "M\u{00FC}nchen")]
    #[case::row58_cjk("\u{65E5}\u{672C}\u{8A9E}", "\u{65E5}\u{672C}\u{8A9E}")]
    #[case::row59_emoji("\u{1F3B5}\u{1F3B6}", "\u{1F3B5}\u{1F3B6}")]
    #[case::row60_angle_pipe("a<b>c|d", "a<b>c|d")]
    #[case::row61_at_bang_parens("a@b!c(d)", "a@b!c(d)")]
    #[case::row62_special_ascii("a&b+c$d;e,f", "a&b+c$d;e,f")]
    #[case::row63_slash_percent("Serbia/srb%", "Serbia/srb%")]
    #[case::row64_percent_literal("100%25", "100%25")]
    #[case::row67_single_space(" ", " ")]
    #[case::row68_double_space("  ", "  ")]
    fn test_spark_ref_string_passthrough(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::String(input.to_string())).unwrap(),
            Some(expected.to_string())
        );
    }

    /// Rows 47-48: Strings with NUL bytes. Spark fails at mkdirs, but our
    /// serialization succeeds because NUL is a valid string character at the
    /// partition value layer. The filesystem constraint is a separate concern.
    #[rstest]
    #[case::row47_nul("\x00", "\x00")]
    #[case::row48_embedded_nul("before\x00after", "before\x00after")]
    fn test_spark_ref_string_with_nul_serializes_raw(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::String(input.to_string())).unwrap(),
            Some(expected.to_string())
        );
    }

    /// Row 65: empty string -> null. Row 66: NULL -> null.
    #[test]
    fn test_spark_ref_row65_empty_string_returns_none() {
        assert_eq!(
            serialize_partition_value(&Scalar::String(String::new())).unwrap(),
            None
        );
    }

    #[test]
    fn test_spark_ref_row66_null_string_returns_none() {
        assert_eq!(
            serialize_partition_value(&Scalar::Null(DataType::STRING)).unwrap(),
            None
        );
    }

    /// Row 49: empty binary -> null.
    #[test]
    fn test_spark_ref_row49_empty_binary_returns_none() {
        assert_eq!(
            serialize_partition_value(&Scalar::Binary(vec![])).unwrap(),
            None
        );
    }

    /// Row 50: non-UTF-8 binary (X'DEADBEEF') -> our code returns an error because
    /// we require strict UTF-8. Spark writes corrupt data for this case.
    #[test]
    fn test_spark_ref_row50_non_utf8_binary_returns_error() {
        let result = serialize_partition_value(&Scalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert!(result.is_err());
    }

    /// Row 51: UTF-8 binary (X'48454C4C4F' = "HELLO") -> "HELLO".
    #[test]
    fn test_spark_ref_row51_utf8_binary_returns_string() {
        assert_eq!(
            serialize_partition_value(&Scalar::Binary(b"HELLO".to_vec())).unwrap(),
            Some("HELLO".to_string())
        );
    }

    /// Row 52: binary with NUL (X'00FF') -> our code returns an error because
    /// 0xFF is not valid UTF-8. Spark fails at mkdirs for NUL bytes.
    #[test]
    fn test_spark_ref_row52_binary_with_nul_and_high_byte_returns_error() {
        let result = serialize_partition_value(&Scalar::Binary(vec![0x00, 0xFF]));
        assert!(result.is_err());
    }

    /// Row 53: binary X'2F3D25' (/=%) -> "/=%".
    #[test]
    fn test_spark_ref_row53_binary_special_chars() {
        assert_eq!(
            serialize_partition_value(&Scalar::Binary(vec![0x2F, 0x3D, 0x25])).unwrap(),
            Some("/=%".to_string())
        );
    }

    // ============================================================================
    // Additional edge cases not in the Spark reference table
    // ============================================================================

    /// Float -0.0 (not in Spark table). Matches Java's Float.toString(-0.0) = "-0.0".
    #[test]
    fn test_float_neg_zero_preserves_sign() {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(-0.0)).unwrap(),
            Some("-0.0".to_string())
        );
    }

    /// Float values outside [1e-3, 1e7) use scientific notation.
    #[rstest]
    #[case::small(1e-4f32, "1.0E-4")]
    #[case::large(1e8f32, "1.0E8")]
    #[case::f32_max(f32::MAX, "3.4028235E38")]
    fn test_float_scientific_notation(#[case] input: f32, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Float(input)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// Decimal edge cases: scale=0, negative in (-1, 0), trailing zeros.
    #[rstest]
    #[case::scale_zero(42, 5, 0, "42")]
    #[case::trailing_zeros(4200, 5, 2, "42.00")]
    #[case::neg_between_zero_and_one(-5, 3, 2, "-0.05")]
    #[case::neg_with_scale(-12345, 5, 2, "-123.45")]
    #[case::pos_with_scale(12345, 5, 2, "123.45")]
    fn test_decimal_edge_cases(
        #[case] bits: i128,
        #[case] precision: u8,
        #[case] scale: u8,
        #[case] expected: &str,
    ) {
        let d = Scalar::decimal(bits, precision, scale).unwrap();
        assert_eq!(
            serialize_partition_value(&d).unwrap(),
            Some(expected.to_string())
        );
    }

    /// Date edge cases: pre-epoch, epoch.
    #[rstest]
    #[case::pre_epoch(-1, "1969-12-31")]
    #[case::epoch(0, "1970-01-01")]
    fn test_date_edge_cases(#[case] days: i32, #[case] expected: &str) {
        assert_eq!(
            serialize_partition_value(&Scalar::Date(days)).unwrap(),
            Some(expected.to_string())
        );
    }

    /// Out-of-range date returns error.
    #[test]
    fn test_date_out_of_range_returns_error() {
        assert!(serialize_partition_value(&Scalar::Date(i32::MAX)).is_err());
    }

    /// Out-of-range timestamp returns error.
    #[test]
    fn test_timestamp_out_of_range_returns_error() {
        assert!(serialize_partition_value(&Scalar::Timestamp(i64::MAX)).is_err());
    }

    /// Out-of-range timestamp_ntz returns error.
    #[test]
    fn test_timestamp_ntz_out_of_range_returns_error() {
        assert!(serialize_partition_value(&Scalar::TimestampNtz(i64::MAX)).is_err());
    }

    // ============================================================================
    // Roundtrip tests: serialize then parse_scalar
    // ============================================================================

    #[rstest]
    #[case::integer(Scalar::Integer(42), PrimitiveType::Integer)]
    #[case::integer_neg(Scalar::Integer(-1), PrimitiveType::Integer)]
    #[case::long(Scalar::Long(9_876_543_210), PrimitiveType::Long)]
    #[case::boolean_true(Scalar::Boolean(true), PrimitiveType::Boolean)]
    #[case::boolean_false(Scalar::Boolean(false), PrimitiveType::Boolean)]
    #[case::string(Scalar::String("hello".into()), PrimitiveType::String)]
    #[case::date(Scalar::Date(19723), PrimitiveType::Date)]
    fn test_roundtrip_simple_types(#[case] input: Scalar, #[case] ptype: PrimitiveType) {
        let serialized = serialize_partition_value(&input).unwrap().unwrap();
        let parsed = ptype.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, input);
    }

    #[test]
    fn test_roundtrip_timestamp_with_micros() {
        let micros = 1_718_521_199_999_999i64; // 2024-06-16T06:59:59.999999Z
        let serialized = serialize_partition_value(&Scalar::Timestamp(micros))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::Timestamp.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, Scalar::Timestamp(micros));
    }

    #[test]
    fn test_roundtrip_timestamp_ntz_with_micros() {
        let micros = 1_718_454_645_000_000i64; // 2024-06-15 12:30:45
        let serialized = serialize_partition_value(&Scalar::TimestampNtz(micros))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::TimestampNtz
            .parse_scalar(&serialized)
            .unwrap();
        assert_eq!(parsed, Scalar::TimestampNtz(micros));
    }

    #[test]
    fn test_roundtrip_decimal() {
        let input = Scalar::decimal(1_230_000_000_000_000_000i128, 38, 18).unwrap();
        let serialized = serialize_partition_value(&input).unwrap().unwrap();
        let parsed = PrimitiveType::decimal(38, 18)
            .unwrap()
            .parse_scalar(&serialized)
            .unwrap();
        assert_eq!(parsed, input);
    }

    /// Float roundtrip: serialize then parse back. Note that f32 has limited precision,
    /// so only values that round-trip exactly through string representation are tested.
    #[rstest]
    #[case::normal(3.125f32)]
    #[case::one(1.0f32)]
    #[case::scientific(1e-4f32)]
    fn test_roundtrip_float(#[case] input: f32) {
        let serialized = serialize_partition_value(&Scalar::Float(input))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::Float.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, Scalar::Float(input));
    }

    /// Double roundtrip.
    #[rstest]
    #[case::normal(3.125f64)]
    #[case::max(f64::MAX)]
    #[case::min_positive(f64::MIN_POSITIVE)]
    fn test_roundtrip_double(#[case] input: f64) {
        let serialized = serialize_partition_value(&Scalar::Double(input))
            .unwrap()
            .unwrap();
        let parsed = PrimitiveType::Double.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, Scalar::Double(input));
    }

    #[rstest]
    #[case::byte(Scalar::Byte(42), PrimitiveType::Byte)]
    #[case::short(Scalar::Short(-1000), PrimitiveType::Short)]
    #[case::binary(Scalar::Binary(b"HELLO".to_vec()), PrimitiveType::Binary)]
    fn test_roundtrip_byte_short_binary(#[case] input: Scalar, #[case] ptype: PrimitiveType) {
        let serialized = serialize_partition_value(&input).unwrap().unwrap();
        let parsed = ptype.parse_scalar(&serialized).unwrap();
        assert_eq!(parsed, input);
    }

    // ============================================================================
    // Error path tests
    // ============================================================================

    /// Non-null Struct, Array, and Map values return an error.
    #[test]
    fn test_serialize_partition_value_non_null_struct_returns_error() {
        use crate::expressions::StructData;
        use crate::schema::StructField;
        let data = StructData::try_new(
            vec![StructField::new("x", DataType::INTEGER, true)],
            vec![Scalar::Integer(1)],
        )
        .unwrap();
        assert!(serialize_partition_value(&Scalar::Struct(data)).is_err());
    }

    #[test]
    fn test_serialize_partition_value_non_null_array_returns_error() {
        use crate::expressions::ArrayData;
        use crate::schema::ArrayType;
        let data = ArrayData::try_new(ArrayType::new(DataType::INTEGER, false), [1i32]).unwrap();
        assert!(serialize_partition_value(&Scalar::Array(data)).is_err());
    }

    #[test]
    fn test_serialize_partition_value_non_null_map_returns_error() {
        use crate::expressions::MapData;
        use crate::schema::MapType;
        let data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            [("k".to_string(), 1i32)],
        )
        .unwrap();
        assert!(serialize_partition_value(&Scalar::Map(data)).is_err());
    }

    /// The second error path in format_date: checked_add succeeds but
    /// from_num_days_from_ce_opt returns None.
    #[test]
    fn test_date_min_value_out_of_range_returns_error() {
        assert!(serialize_partition_value(&Scalar::Date(i32::MIN)).is_err());
    }

    /// Pre-epoch timestamp exercises div_euclid/rem_euclid for negative values.
    #[test]
    fn test_timestamp_pre_epoch_formats_correctly() {
        assert_eq!(
            serialize_partition_value(&Scalar::Timestamp(-1)).unwrap(),
            Some("1969-12-31T23:59:59.999999Z".to_string())
        );
    }

    /// Integer-valued double in the decimal range exercises the "no dot" branch.
    #[test]
    fn test_double_integer_valued_appends_dot_zero() {
        assert_eq!(
            serialize_partition_value(&Scalar::Double(1.0)).unwrap(),
            Some("1.0".to_string())
        );
    }
}
