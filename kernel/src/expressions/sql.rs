//! Parse a SQL string into a kernel [`Expression`].
//!
//! Delta stores column defaults, check constraints, and generated column definitions as SQL
//! strings in table metadata. This module turns those strings into kernel [`Expression`] values
//! so the kernel can interpret them without depending on a full SQL parser.
//!
//! This is an intentionally light start: a small internal parser covering only the literal forms
//! Delta metadata contains today. If the supported SQL surface grows, options include moving
//! parsing behind the [`Engine`](crate::Engine) trait or adopting an existing SQL parser library.

// `parse_sql` has no in-crate caller yet; it will be wired up by the column-defaults work
// (#2630).
#![allow(dead_code)]

use std::borrow::Cow;

use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, PrimitiveType};
use crate::{DeltaResult, Error};

/// Parse a SQL string into an [`Expression`] that yields a value of the given [`DataType`]
/// (e.g. the type of the column whose default is being parsed).
///
/// # Accepted grammar
///
/// Leading and trailing whitespace are ignored. Keywords (`NULL`, `TRUE`, `FALSE`, `DATE`,
/// `TIMESTAMP`, `X`) are case-insensitive.
///
/// - `NULL`: valid for any data type
/// - String: `'foo'`, with `''` interpreted as an embedded single quote
/// - Integer / Long / Short / Byte / Float / Double / Decimal: bare numeric literal with optional
///   leading `+` or `-`
/// - Boolean: `TRUE` / `FALSE`
/// - Date: `'2024-01-01'` or `DATE '2024-01-01'`
/// - Timestamp / TimestampNtz: `'2024-01-01 12:00:00[.fff]'` or `TIMESTAMP '...'`. `Timestamp` also
///   accepts ISO 8601 / RFC 3339 form with a `Z` (UTC) suffix (e.g. `'1970-01-01T00:00:00.123Z'`);
///   non-UTC offsets are not yet supported. `TimestampNtz` does not accept ISO form.
/// - Binary: `X'deadbeef'` (even number of hex digits)
///
/// # Errors
///
/// Returns an error if the input is not a SQL form this parser accepts, or if the parsed value
/// is not compatible with `data_type` (incompatible type, out of range, etc.).
pub(crate) fn parse_sql(sql: &str, data_type: &DataType) -> DeltaResult<Expression> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(Error::generic("empty SQL literal"));
    }
    // NULL is valid for any data type, including non-primitive ones.
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(Expression::literal(Scalar::Null(data_type.clone())));
    }
    // TODO(#2630): support SQL function calls (e.g. `current_date()`) when column defaults
    // need them.
    parse_literal(trimmed, data_type, sql)
}

/// Parse `trimmed` as a SQL literal of the given primitive `data_type`. NULL is handled in
/// [`parse_sql`], so this never sees a bare `NULL`.
fn parse_literal(trimmed: &str, data_type: &DataType, sql: &str) -> DeltaResult<Expression> {
    let DataType::Primitive(primitive) = data_type else {
        return Err(Error::generic(format!(
            "SQL literal parsing only supports primitive types, got {data_type:?}"
        )));
    };

    // Binary uses a dedicated X'...' form. String is built directly from the unquoted body. Both
    // bypass parse_scalar, which treats an empty input as SQL NULL (partition-value convention),
    // whereas a SQL empty string `''` must round-trip as `Scalar::String("")`, distinct from NULL.
    match primitive {
        PrimitiveType::Binary => {
            let bytes = decode_binary_literal(trimmed)?;
            return Ok(Expression::literal(Scalar::Binary(bytes)));
        }
        PrimitiveType::String => {
            let unquoted = unquote_string(trimmed)?;
            return Ok(Expression::literal(Scalar::String(unquoted)));
        }
        _ => {}
    }

    // Strip the SQL syntax envelope per target type, then delegate to the existing
    // `PrimitiveType::parse_scalar` for the actual value parsing.
    let raw: Cow<'_, str> = match primitive {
        PrimitiveType::Date => Cow::Owned(strip_typed_prefix_and_unquote(trimmed, &["DATE"])?),
        PrimitiveType::Timestamp | PrimitiveType::TimestampNtz => Cow::Owned(
            strip_typed_prefix_and_unquote(trimmed, &["TIMESTAMP_NTZ", "TIMESTAMP"])?,
        ),
        // Numeric and Boolean: parse_scalar handles signed numbers and case-insensitive
        // TRUE/FALSE directly. Reject any input that looks like a quoted SQL string to avoid
        // accidentally treating `'42'` as the integer 42.
        _ => {
            if trimmed.starts_with('\'') {
                return Err(Error::generic(format!(
                    "expected a bare {primitive:?} literal, got quoted string: {sql}"
                )));
            }
            Cow::Borrowed(trimmed)
        }
    };

    // Trim the value inside the quotes to match Spark, whose stringToDate/stringToTimestamp
    // trim their input. A no-op for the bare numeric/boolean path, which was trimmed with the
    // whole input.
    let raw = raw.trim();

    // parse_scalar maps empty input to NULL (partition-value convention), but an empty quoted
    // body like `DATE ''` is invalid SQL, not NULL. `NULL` is only accepted as a bare keyword,
    // handled in parse_sql. Note that String is an exception which can be '' and is handled
    // above, with its body kept verbatim.
    if raw.is_empty() {
        return Err(Error::generic(format!(
            "empty {primitive:?} literal: {sql}"
        )));
    }

    // TODO(#2733): parse_scalar matches an RFC 3339 offset then drops it, so `14:30:00+05:00`
    // reads as 14:30 UTC instead of 09:30. Reject non-Z offsets for now (only follow the `T`/`t`).
    let has_non_utc_offset = *primitive == PrimitiveType::Timestamp
        && raw
            .split_once(['T', 't'])
            .is_some_and(|(_, time)| time.contains(['+', '-']));
    if has_non_utc_offset {
        return Err(Error::generic(format!(
            "timestamp literal with a non-UTC offset is not yet supported; use 'Z' (UTC): {sql}"
        )));
    }

    let scalar = primitive.parse_scalar(raw)?;
    Ok(Expression::literal(scalar))
}

/// Strip surrounding single quotes from a SQL string literal and un-escape the doubled-quote
/// sequence `''` -> `'`. Errors if the input is not surrounded by single quotes or contains an
/// unescaped interior quote.
fn unquote_string(input: &str) -> DeltaResult<String> {
    let inner = input
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .ok_or_else(|| {
            Error::generic(format!("expected a single-quoted SQL string, got: {input}"))
        })?;

    // After splitting on `'`, every chunk except the last must be empty, meaning the quote was
    // doubled (SQL's escape for an embedded single quote).
    let mut out = String::with_capacity(inner.len());
    let mut chunks = inner.split('\'').peekable();
    while let Some(chunk) = chunks.next() {
        out.push_str(chunk);
        match chunks.peek() {
            None => break,
            Some(&"") => {
                // Doubled quote: consume the empty chunk and emit a single quote.
                chunks.next();
                out.push('\'');
            }
            Some(_) => {
                return Err(Error::generic(format!(
                    "unescaped single quote in SQL string literal: {input}"
                )));
            }
        }
    }
    Ok(out)
}

/// Strip an optional typed-literal keyword prefix (e.g. `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`)
/// and then unwrap the required `'...'` quoted body. Any of the given `keywords` may appear
/// as the prefix; case-insensitive.
fn strip_typed_prefix_and_unquote(input: &str, keywords: &[&str]) -> DeltaResult<String> {
    let body = match input.split_once(char::is_whitespace) {
        Some((prefix, rest)) if keywords.iter().any(|kw| prefix.eq_ignore_ascii_case(kw)) => {
            rest.trim_start()
        }
        _ => input,
    };
    unquote_string(body)
}

/// Decode a `X'hex'` SQL binary literal into a byte vector. The leading `X` is case-insensitive;
/// the body must be an even-length sequence of hex digits.
fn decode_binary_literal(input: &str) -> DeltaResult<Vec<u8>> {
    let err = || {
        Error::generic(format!(
            "expected a SQL binary literal like X'..', got: {input}"
        ))
    };
    let hex = input
        .strip_prefix(['x', 'X'])
        .and_then(|rest| rest.strip_prefix('\''))
        .and_then(|rest| rest.strip_suffix('\''))
        .ok_or_else(err)?;
    if !hex.len().is_multiple_of(2) {
        return Err(Error::generic(format!(
            "binary literal must contain an even number of hex digits: {input}"
        )));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char)
                .to_digit(16)
                .ok_or_else(|| Error::generic(format!("invalid hex digit in {input}")))?;
            let lo = (pair[1] as char)
                .to_digit(16)
                .ok_or_else(|| Error::generic(format!("invalid hex digit in {input}")))?;
            Ok((hi << 4 | lo) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
    use rstest::rstest;

    use super::*;
    use crate::expressions::{DecimalData, Expression};
    use crate::schema::{ArrayType, DataType, DecimalType, MapType, StructField};

    fn date_days(year: i32, month: u32, day: u32) -> i32 {
        let nd = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        Utc.from_utc_datetime(&nd)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_days() as i32
    }

    fn ts_micros(s: &str) -> i64 {
        let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        Utc.from_utc_datetime(&ndt)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_microseconds()
            .unwrap()
    }

    fn decimal_type(precision: u8, scale: u8) -> DataType {
        DataType::Primitive(PrimitiveType::Decimal(
            DecimalType::try_new(precision, scale).unwrap(),
        ))
    }

    #[rstest]
    #[case("42", DataType::INTEGER, Scalar::Integer(42))]
    #[case(" -7 ", DataType::INTEGER, Scalar::Integer(-7))]
    #[case("+5", DataType::INTEGER, Scalar::Integer(5))]
    #[case("127", DataType::BYTE, Scalar::Byte(127))]
    #[case("-32768", DataType::SHORT, Scalar::Short(i16::MIN))]
    #[case("9223372036854775807", DataType::LONG, Scalar::Long(i64::MAX))]
    #[case("2.5", DataType::DOUBLE, Scalar::Double(2.5))]
    #[case("0.5", DataType::FLOAT, Scalar::Float(0.5))]
    #[case("TRUE", DataType::BOOLEAN, Scalar::Boolean(true))]
    #[case("false", DataType::BOOLEAN, Scalar::Boolean(false))]
    #[case("'hello'", DataType::STRING, Scalar::String("hello".into()))]
    #[case("' hi '", DataType::STRING, Scalar::String(" hi ".into()))] // strings are not trimmed
    #[case("''", DataType::STRING, Scalar::String(String::new()))]
    #[case("'it''s'", DataType::STRING, Scalar::String("it's".into()))]
    #[case("'a''b''c'", DataType::STRING, Scalar::String("a'b'c".into()))]
    #[case("'''", DataType::STRING, Scalar::String("'".into()))]
    #[case("'''hello'", DataType::STRING, Scalar::String("'hello".into()))]
    #[case("'hello'''", DataType::STRING, Scalar::String("hello'".into()))]
    #[case("'''''", DataType::STRING, Scalar::String("''".into()))]
    #[case("'''bad'''", DataType::STRING, Scalar::String("'bad'".into()))]
    #[case(
        "1.23",
        decimal_type(5, 2),
        Scalar::Decimal(DecimalData::try_new(123, DecimalType::try_new(5, 2).unwrap()).unwrap()),
    )]
    #[case(
        "-12345.67",
        decimal_type(10, 2),
        Scalar::Decimal(
            DecimalData::try_new(-1234567, DecimalType::try_new(10, 2).unwrap()).unwrap()
        ),
    )]
    fn parses_basic_literals(#[case] sql: &str, #[case] ty: DataType, #[case] expected: Scalar) {
        let got = parse_sql(sql, &ty).unwrap();
        assert_eq!(got, Expression::literal(expected));
    }

    #[rstest]
    #[case("'2024-01-01'", date_days(2024, 1, 1))]
    #[case("DATE '2024-01-01'", date_days(2024, 1, 1))]
    #[case("date  '1970-01-02'", date_days(1970, 1, 2))]
    #[case("' 2024-01-01 '", date_days(2024, 1, 1))] // body is trimmed, matching Spark
    #[case("DATE ' 2024-01-01 '", date_days(2024, 1, 1))]
    fn parses_date_literals(#[case] sql: &str, #[case] expected_days: i32) {
        let got = parse_sql(sql, &DataType::DATE).unwrap();
        assert_eq!(got, Expression::literal(Scalar::Date(expected_days)));
    }

    #[rstest]
    #[case("'2024-01-01 12:34:56'", "2024-01-01 12:34:56")]
    #[case("TIMESTAMP '2024-01-01 12:34:56.789'", "2024-01-01 12:34:56.789")]
    #[case("TIMESTAMP_NTZ '2024-01-01 12:34:56'", "2024-01-01 12:34:56")]
    #[case("timestamp_ntz '2024-01-01 12:34:56.789'", "2024-01-01 12:34:56.789")]
    #[case("' 2024-01-01 12:34:56 '", "2024-01-01 12:34:56")] // body is trimmed, matching Spark
    fn parses_timestamp_literals(#[case] sql: &str, #[case] equivalent: &str) {
        let got = parse_sql(sql, &DataType::TIMESTAMP).unwrap();
        assert_eq!(
            got,
            Expression::literal(Scalar::Timestamp(ts_micros(equivalent)))
        );

        let got_ntz = parse_sql(sql, &DataType::TIMESTAMP_NTZ).unwrap();
        assert_eq!(
            got_ntz,
            Expression::literal(Scalar::TimestampNtz(ts_micros(equivalent)))
        );
    }

    // `PrimitiveType::parse_scalar` falls back to ISO 8601 / RFC 3339 (`%+`) only for the
    // `Timestamp` variant; `TimestampNtz` has no such fallback and rejects ISO form. Pin the
    // asymmetry so a future refactor that unifies the two paths gets caught.
    #[rstest]
    #[case("'1970-01-01T00:00:00.123Z'", "1970-01-01 00:00:00.123")]
    #[case("'2024-06-15T14:30:00Z'", "2024-06-15 14:30:00")]
    #[case("TIMESTAMP '2024-06-15T14:30:00.456Z'", "2024-06-15 14:30:00.456")]
    fn iso_8601_form_accepted_only_for_timestamp(#[case] sql: &str, #[case] equivalent: &str) {
        let got = parse_sql(sql, &DataType::TIMESTAMP).unwrap();
        assert_eq!(
            got,
            Expression::literal(Scalar::Timestamp(ts_micros(equivalent)))
        );
        parse_sql(sql, &DataType::TIMESTAMP_NTZ).unwrap_err();
    }

    #[rstest]
    #[case("X''", vec![])]
    #[case("X'00'", vec![0x00])]
    #[case("X'DeAdBeEf'", vec![0xde, 0xad, 0xbe, 0xef])]
    #[case("x'01ff'", vec![0x01, 0xff])]
    fn parses_binary_literals(#[case] sql: &str, #[case] expected: Vec<u8>) {
        let got = parse_sql(sql, &DataType::BINARY).unwrap();
        assert_eq!(got, Expression::literal(Scalar::Binary(expected)));
    }

    #[rstest]
    #[case(DataType::INTEGER)]
    #[case(DataType::STRING)]
    #[case(DataType::BOOLEAN)]
    #[case(DataType::DATE)]
    #[case(DataType::BINARY)]
    fn null_is_accepted_for_any_primitive(#[case] ty: DataType) {
        let got = parse_sql("NULL", &ty).unwrap();
        assert_eq!(got, Expression::literal(Scalar::Null(ty.clone())));
        // also case-insensitive
        let got_lower = parse_sql(" null ", &ty).unwrap();
        assert_eq!(got_lower, Expression::literal(Scalar::Null(ty)));
    }

    /// As the parser grows new capabilities (typed numeric suffixes, CAST, foldable
    /// function calls), cases should be removed from this list and moved into the positive tests
    /// above.
    #[rstest]
    #[case("1L", DataType::LONG)]
    #[case("1.23BD", decimal_type(5, 2))]
    #[case("1.5F", DataType::FLOAT)]
    #[case("CAST('2024-01-01' AS DATE)", DataType::DATE)]
    #[case("CAST(NULL AS INT)", DataType::INTEGER)]
    #[case("current_date()", DataType::DATE)]
    #[case("current_timestamp()", DataType::TIMESTAMP)]
    #[case("now()", DataType::TIMESTAMP)]
    #[case("1 + 1", DataType::INTEGER)]
    #[case("concat('a', 'b')", DataType::STRING)]
    fn currently_unsupported_valid_sql(#[case] sql: &str, #[case] ty: DataType) {
        let result = parse_sql(sql, &ty);
        assert!(
            result.is_err(),
            "expected error for currently-unsupported SQL {sql:?} as {ty:?}, got {result:?}"
        );
    }

    #[rstest]
    #[case("", DataType::INTEGER)]
    #[case("   ", DataType::INTEGER)]
    #[case("'42'", DataType::INTEGER)] // quoted number for int
    #[case("+", DataType::INTEGER)] // lone sign
    #[case("UnknownFn()", DataType::INTEGER)] // function call
    #[case("42", DataType::STRING)] // unquoted number for string
    #[case("foo", DataType::STRING)] // unquoted string
    #[case("'unterminated", DataType::STRING)]
    #[case("'bad'quote'", DataType::STRING)] // unescaped interior quote
    #[case("nope", DataType::BOOLEAN)]
    #[case("'TRUE'", DataType::BOOLEAN)] // quoted boolean
    #[case("'2024-13-01'", DataType::DATE)] // bad month
    #[case("not-a-date", DataType::DATE)]
    #[case("''", DataType::DATE)] // empty quoted body must not parse as NULL
    #[case("DATE ''", DataType::DATE)]
    #[case("' '", DataType::DATE)] // whitespace-only body trims to empty
    #[case("DATE ' '", DataType::DATE)]
    #[case("''", DataType::TIMESTAMP)]
    #[case("' '", DataType::TIMESTAMP)]
    #[case("TIMESTAMP ''", DataType::TIMESTAMP)]
    #[case("TIMESTAMP_NTZ ''", DataType::TIMESTAMP_NTZ)]
    #[case("'2024-06-15T14:30:00+05:00'", DataType::TIMESTAMP)] // non-Z offset, see TODO(#2733)
    #[case("TIMESTAMP '2024-06-15T14:30:00-05:00'", DataType::TIMESTAMP)] // non-Z offset
    #[case("  now()  ", DataType::TIMESTAMP)] // function call with padding
    #[case("X'0'", DataType::BINARY)] // odd number of hex digits
    #[case("X'gg'", DataType::BINARY)] // non-hex chars
    #[case("'deadbeef'", DataType::BINARY)] // missing X prefix
    fn rejects_invalid_input(#[case] sql: &str, #[case] ty: DataType) {
        let result = parse_sql(sql, &ty);
        assert!(
            result.is_err(),
            "expected error for {sql:?} as {ty:?}, got {result:?}"
        );
    }

    fn struct_ty() -> DataType {
        DataType::try_struct_type([StructField::nullable("a", DataType::INTEGER)]).unwrap()
    }

    fn array_ty() -> DataType {
        DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true)))
    }

    fn map_ty() -> DataType {
        DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            DataType::INTEGER,
            true,
        )))
    }

    #[rstest]
    #[case::struct_target(struct_ty())]
    #[case::array_target(array_ty())]
    #[case::map_target(map_ty())]
    fn rejects_non_primitive_target(#[case] ty: DataType) {
        assert!(parse_sql("'foo'", &ty).is_err());
    }

    // NULL is special-cased and accepted before the primitive check, matching the protocol's
    // stance that any column can be null.
    #[rstest]
    #[case::struct_target(struct_ty())]
    #[case::array_target(array_ty())]
    #[case::map_target(map_ty())]
    fn null_is_accepted_for_non_primitive_target(#[case] ty: DataType) {
        let got = parse_sql("NULL", &ty).unwrap();
        assert_eq!(got, Expression::literal(Scalar::Null(ty)));
    }
}
