//! Parse a SQL string into a kernel [`Expression`].
//!
//! Delta stores column defaults, check constraints, and generated column definitions as SQL
//! strings in table metadata. This module turns those strings into kernel [`Expression`] values
//! so the kernel can interpret them without depending on a full SQL parser.
//!
//! The grammar follows the Spark SQL standard: this parser implements a subset of Spark's SQL
//! grammar rather than defining a kernel-specific dialect, so the forms it accepts match what
//! Spark reads and writes.
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
/// Leading and trailing whitespace are ignored. `NULL` (case-insensitive) is accepted for any
/// data type. All other input is parsed as a typed literal, which is supported only for primitive
/// types.
///
/// # Examples
///
/// The SQL comes from table metadata. A column declared `c DATE DEFAULT DATE '2024-01-01'` stores
/// the string `DATE '2024-01-01'` as its default, and `parse_sql("DATE '2024-01-01'", &DATE)`
/// parses it into `Expression::literal(Scalar::Date(..))`. The bare form `'2024-01-01'` is
/// equivalent; the `DATE` keyword is optional.
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

/// Parse `trimmed` as a SQL literal of the given primitive `data_type`. Errors on a non-primitive
/// `data_type`, or input that is not one of the accepted literal forms below.
///
/// # Accepted grammar
///
/// Keywords (`TRUE`, `FALSE`, `DATE`, `TIMESTAMP`, `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ`, `X`) are
/// case-insensitive.
///
/// - String: single-quoted `'foo'`, with `''` an embedded single quote (e.g. `'it''s'` -> `it's`).
///   A backslash is rejected: Spark applies backslash escapes (`\n`, `\\`, ...) to string literals,
///   not yet implemented here. Double-quoted strings (`"foo"`) are also rejected: Spark accepts
///   them under its default config, but kernel does not yet.
/// - Integer / Long / Short / Byte / Float / Double / Decimal: bare numeric literal with optional
///   leading `+` or `-`. Non-finite floats (`NaN`, `Infinity`) are rejected: Spark only emits those
///   via an explicit cast (e.g. `CAST('NaN' AS DOUBLE)`), never as a bare literal. A plain
///   negative-zero float (`-0.0`) is normalized to `+0.0` to match Spark, where `-0.0` is a decimal
///   `0.0` that folds to `+0.0`; an exponent form like `-0.0E0` is a Spark double literal that
///   keeps its sign, so it is left untouched.
/// - Boolean: `TRUE` / `FALSE`
/// - Date: `'2024-01-01'`, `DATE '2024-01-01'`, or `DATE'2024-01-01'` (the keyword may butt against
///   the quote)
/// - TimestampNtz: zoneless `'2024-01-01 12:00:00[.fff]'` or `TIMESTAMP_NTZ '...'` (wall-clock, no
///   timezone)
/// - Timestamp: ISO 8601 / RFC 3339 form with an explicit UTC `Z` suffix, e.g.
///   `'1970-01-01T00:00:00.123Z'`, `TIMESTAMP '...Z'`, or `TIMESTAMP_LTZ '...Z'` (`TIMESTAMP_LTZ`
///   is an explicit spelling of LTZ). Zoneless literals are rejected (Spark resolves them against
///   the session timezone, which kernel cannot know); literals carrying a numeric offset (e.g.
///   `+05:00`, including `+00:00`) are rejected because `parse_scalar` silently drops the offset
///   (TODO(#2733)).
/// - Binary: `X'deadbeef'` (even number of hex digits)
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
        // Each timestamp type accepts only its own keyword(s): a mismatched keyword (e.g. an NTZ
        // literal on an LTZ column) carries different timezone semantics and must not be reused.
        // `TIMESTAMP_LTZ` is an explicit spelling of LTZ (== bare `TIMESTAMP`), so both route here
        // through the same UTC-`Z` guard below.
        PrimitiveType::Timestamp => Cow::Owned(strip_typed_prefix_and_unquote(
            trimmed,
            &["TIMESTAMP", "TIMESTAMP_LTZ"],
        )?),
        PrimitiveType::TimestampNtz => {
            Cow::Owned(strip_typed_prefix_and_unquote(trimmed, &["TIMESTAMP_NTZ"])?)
        }
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

    // Trim the value inside the quotes to match Spark, whose stringToDate/stringToTimestamp trim
    // their input (e.g. `DATE ' 2024-01-01 '` -> `2024-01-01`). A no-op for the bare
    // numeric/boolean path, which was already trimmed with the whole input.
    let raw = raw.trim();

    // parse_scalar maps empty input to NULL (partition-value convention), but an empty quoted
    // body like `DATE ''` is invalid SQL, not NULL. `NULL` is only accepted as a bare keyword,
    // handled in parse_sql.
    if raw.is_empty() {
        return Err(Error::generic(format!(
            "empty {primitive:?} literal: {sql}"
        )));
    }

    // A TIMESTAMP (LTZ) literal must pin an absolute instant, but parse_scalar only honors a `Z`
    // (UTC) suffix today. TimestampNtz carries no zone, so it is unaffected.
    if *primitive == PrimitiveType::Timestamp && !raw.ends_with(['Z', 'z']) {
        // An explicit offset lives in the time part after the date/time separator (`T`/space).
        let has_offset = raw
            .split_once(['T', 't', ' '])
            .is_some_and(|(_, time)| time.contains(['+', '-']));
        return Err(if has_offset {
            // Only `Z` is honored; parse_scalar's `%+` path silently drops any numeric offset
            // (including `+00:00`) and reads the wall clock as UTC (TODO(#2733)).
            Error::generic(format!(
                "TIMESTAMP literal with an explicit offset is not yet supported; use 'Z' (UTC): {sql}"
            ))
        } else {
            Error::generic(format!(
                "zoneless TIMESTAMP literal is not yet supported; use an explicit 'Z' (UTC) suffix \
                 since Spark resolves a bare timestamp against the session timezone: {sql}"
            ))
        });
    }

    let scalar = primitive.parse_scalar(raw)?;

    // Reconcile Float/Double with Spark's constant folding, which Rust's float parser does not
    // match (see the grammar doc above). Non-finite (`NaN`/`Infinity`): Spark only produces these
    // via an explicit cast, never as a bare literal, so reject them. Negative zero: `+ 0.0` folds a
    // plain `-0.0` to `+0.0` (a no-op for every other value); exponent forms keep their sign, so
    // skip them.
    let normalize_neg_zero = !raw.contains(['e', 'E']);
    let non_finite_error = || {
        Error::generic(format!(
            "non-finite float literal requires an explicit cast, e.g. CAST('NaN' AS DOUBLE): {sql}"
        ))
    };
    let scalar = match scalar {
        Scalar::Float(f) if !f.is_finite() => return Err(non_finite_error()),
        Scalar::Double(d) if !d.is_finite() => return Err(non_finite_error()),
        Scalar::Float(f) if normalize_neg_zero => Scalar::Float(f + 0.0),
        Scalar::Double(d) if normalize_neg_zero => Scalar::Double(d + 0.0),
        other => other,
    };

    Ok(Expression::literal(scalar))
}

/// Unquote a SQL string literal: strip the surrounding single quotes and un-escape each
/// doubled-quote sequence `''` into a single `'`. Errors if `input` is not a properly
/// terminated single-quoted string, including a missing closing quote or trailing characters
/// after it. Also errors on a backslash, since Spark applies backslash escapes (`\n`, `\\`, ...)
/// to string literals and this parser does not yet implement them.
fn unquote_string(input: &str) -> DeltaResult<String> {
    let body = input.strip_prefix('\'').ok_or_else(|| {
        Error::generic(format!("expected a single-quoted SQL string, got: {input}"))
    })?;

    // Walk the body after the opening quote. A `'` either escapes a literal quote (`''`) or
    // closes the string; reaching the end without a closing quote is an unterminated literal.
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            return Err(Error::generic(format!(
                "backslash escapes in SQL string literals are not yet supported: {input}"
            )));
        }
        if c != '\'' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => return Ok(out),
            Some('\'') => out.push('\''),
            Some(_) => {
                return Err(Error::generic(format!(
                    "unexpected characters after closing quote in SQL string literal: {input}"
                )))
            }
        }
    }
    Err(Error::generic(format!(
        "unterminated SQL string literal: {input}"
    )))
}

/// Strip an optional typed-literal keyword prefix (e.g. `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`) and
/// unwrap the required `'...'` quoted body. Any of `keywords` may appear as the prefix
/// (case-insensitive), separated from the quote by optional whitespace, so `DATE '2024-01-01'`,
/// `DATE'2024-01-01'`, and bare `'2024-01-01'` are all accepted.
fn strip_typed_prefix_and_unquote(input: &str, keywords: &[&str]) -> DeltaResult<String> {
    // Match a keyword only as a complete token: it must be followed by the opening quote or
    // whitespace, never more identifier characters (so `DATEX '..'` is not read as `DATE`).
    let body = keywords.iter().find_map(|kw| {
        let prefix = input.get(..kw.len())?;
        let rest = &input[kw.len()..];
        let is_token = rest.starts_with('\'') || rest.starts_with(char::is_whitespace);
        (prefix.eq_ignore_ascii_case(kw) && is_token).then(|| rest.trim_start())
    });
    unquote_string(body.unwrap_or(input))
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
    #[case("'''hello'", DataType::STRING, Scalar::String("'hello".into()))]
    #[case("'hello'''", DataType::STRING, Scalar::String("hello'".into()))]
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
    #[case("DATE'2024-01-01'", date_days(2024, 1, 1))] // keyword butted against the quote
    #[case("date  '1970-01-02'", date_days(1970, 1, 2))]
    #[case("' 2024-01-01 '", date_days(2024, 1, 1))] // body is trimmed, matching Spark
    #[case("DATE ' 2024-01-01 '", date_days(2024, 1, 1))]
    fn parses_date_literals(#[case] sql: &str, #[case] expected_days: i32) {
        let got = parse_sql(sql, &DataType::DATE).unwrap();
        assert_eq!(got, Expression::literal(Scalar::Date(expected_days)));
    }

    #[rstest]
    #[case("'2024-01-01 12:34:56'", "2024-01-01 12:34:56")]
    #[case("TIMESTAMP_NTZ '2024-01-01 12:34:56'", "2024-01-01 12:34:56")]
    #[case("TIMESTAMP_NTZ'2024-01-01 12:34:56'", "2024-01-01 12:34:56")] // keyword butted against quote
    #[case("timestamp_ntz '2024-01-01 12:34:56.789'", "2024-01-01 12:34:56.789")]
    #[case("' 2024-01-01 12:34:56 '", "2024-01-01 12:34:56")] // body is trimmed, matching Spark
    fn parses_zoneless_timestamp_ntz_literals(#[case] sql: &str, #[case] equivalent: &str) {
        let got = parse_sql(sql, &DataType::TIMESTAMP_NTZ).unwrap();
        assert_eq!(
            got,
            Expression::literal(Scalar::TimestampNtz(ts_micros(equivalent)))
        );
    }

    #[rstest]
    #[case("'2024-01-01 12:34:56'")] // zoneless
    #[case("TIMESTAMP '2024-01-01 12:34:56.789'")] // zoneless, with keyword
    #[case("TIMESTAMP'2024-01-01 12:34:56'")] // zoneless, keyword butted against quote
    #[case("TIMESTAMP_LTZ '2024-01-01 12:34:56'")] // zoneless, explicit LTZ keyword
    #[case("' 2024-01-01 12:34:56 '")] // zoneless, padded
    #[case("'2024-06-15T14:30:00+05:00'")] // offset (parse_scalar drops it, see TODO(#2733))
    #[case("TIMESTAMP '2024-06-15T14:30:00-05:00'")] // offset
    #[case("'2024-06-15T14:30:00+00:00'")] // +00:00 is UTC-valued but dropped, so still rejected
    fn rejects_zoneless_and_offset_timestamp_ltz(#[case] sql: &str) {
        let result = parse_sql(sql, &DataType::TIMESTAMP);
        assert!(
            result.is_err(),
            "expected error for zoneless/offset TIMESTAMP {sql:?}, got {result:?}"
        );
    }

    // The two LTZ rejections carry distinct messages; assert on the message so the
    // zoneless-vs-offset split stays pinned (both otherwise just return an error).
    #[rstest]
    #[case("'2024-01-01 12:34:56'", "zoneless")]
    #[case("TIMESTAMP '2024-01-01 12:34:56'", "zoneless")]
    #[case("'2024-06-15T14:30:00+05:00'", "offset")]
    #[case("'2024-06-15T14:30:00+00:00'", "offset")] // +00:00 is UTC-valued but dropped, so rejected
    fn timestamp_ltz_rejection_distinguishes_zoneless_from_offset(
        #[case] sql: &str,
        #[case] needle: &str,
    ) {
        let err = parse_sql(sql, &DataType::TIMESTAMP)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(needle),
            "{sql:?} message missing {needle:?}: {err}"
        );
    }

    #[rstest]
    #[case("TIMESTAMP_NTZ '1970-01-01T00:00:00Z'", DataType::TIMESTAMP)] // NTZ keyword, LTZ target
    #[case("TIMESTAMP '2024-01-01 12:34:56'", DataType::TIMESTAMP_NTZ)] // LTZ keyword, NTZ target
    #[case("TIMESTAMP_LTZ '1970-01-01T00:00:00Z'", DataType::TIMESTAMP_NTZ)] // LTZ keyword, NTZ target
    fn rejects_mismatched_timestamp_keyword(#[case] sql: &str, #[case] ty: DataType) {
        let result = parse_sql(sql, &ty);
        assert!(
            result.is_err(),
            "expected error for mismatched timestamp keyword {sql:?} as {ty:?}, got {result:?}"
        );
    }

    // `PrimitiveType::parse_scalar` falls back to ISO 8601 / RFC 3339 (`%+`) only for the
    // `Timestamp` variant; `TimestampNtz` has no such fallback and rejects ISO form. Pin the
    // asymmetry so a future refactor that unifies the two paths gets caught.
    #[rstest]
    #[case("'1970-01-01T00:00:00.123Z'", "1970-01-01 00:00:00.123")]
    #[case("'2024-06-15T14:30:00Z'", "2024-06-15 14:30:00")]
    #[case("TIMESTAMP '2024-06-15T14:30:00.456Z'", "2024-06-15 14:30:00.456")]
    #[case("TIMESTAMP_LTZ '2024-06-15T14:30:00Z'", "2024-06-15 14:30:00")] // explicit LTZ spelling
    #[case("TIMESTAMP_LTZ'1970-01-01T00:00:00.123Z'", "1970-01-01 00:00:00.123")] // butted against quote
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
    #[case("0", decimal_type(10, 2))] // parse_scalar requires the literal's scale to match exactly
    #[case("1.2", decimal_type(5, 2))]
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
    #[case("'bad'quote'", DataType::STRING)] // characters after the closing quote
    #[case("'''", DataType::STRING)] // unterminated: odd number of quotes
    #[case("'''''", DataType::STRING)] // unterminated: odd number of quotes
    #[case("'ab''", DataType::STRING)] // unterminated: trailing escaped quote, no close
    #[case("nope", DataType::BOOLEAN)]
    #[case("'TRUE'", DataType::BOOLEAN)] // quoted boolean
    #[case("'2024-13-01'", DataType::DATE)] // bad month
    #[case("not-a-date", DataType::DATE)]
    #[case("''", DataType::DATE)] // empty quoted body must not parse as NULL
    #[case("DATE ''", DataType::DATE)]
    #[case("' '", DataType::DATE)] // whitespace-only body trims to empty
    #[case("DATE ' '", DataType::DATE)]
    #[case("DATEX '2024-01-01'", DataType::DATE)] // extra chars after keyword, not a token
    #[case("DATEX'2024-01-01'", DataType::DATE)] // ditto, no space
    #[case("''", DataType::TIMESTAMP)]
    #[case("' '", DataType::TIMESTAMP)]
    #[case("TIMESTAMP ''", DataType::TIMESTAMP)]
    #[case("TIMESTAMPX '2024-01-01 12:34:56'", DataType::TIMESTAMP)] // extra chars after keyword
    #[case("TIMESTAMP_NTZ ''", DataType::TIMESTAMP_NTZ)]
    #[case("timestamp_ntza'2024-01-01 12:34:56'", DataType::TIMESTAMP_NTZ)] // extra chars, no space
    #[case("  now()  ", DataType::TIMESTAMP)] // function call with padding
    #[case("X'0'", DataType::BINARY)] // odd number of hex digits
    #[case("X'gg'", DataType::BINARY)] // non-hex chars
    #[case("'deadbeef'", DataType::BINARY)] // missing X prefix
    #[case("128", DataType::BYTE)] // out of range for i8
    #[case("2147483648", DataType::INTEGER)] // out of range for i32
    fn rejects_invalid_input(#[case] sql: &str, #[case] ty: DataType) {
        let result = parse_sql(sql, &ty);
        assert!(
            result.is_err(),
            "expected error for {sql:?} as {ty:?}, got {result:?}"
        );
    }

    #[rstest]
    fn rejects_bare_non_finite_floats(
        #[values(
            "NaN",
            "nan",
            "Infinity",
            "infinity",
            "inf",
            "-inf",
            "+inf",
            "-Infinity",
            "1e999",  // overflows to infinity
            "-1e999"
        )]
        sql: &str,
        #[values(DataType::FLOAT, DataType::DOUBLE)] ty: DataType,
    ) {
        let result = parse_sql(sql, &ty);
        assert!(
            result.is_err(),
            "expected error for bare non-finite literal {sql:?} as {ty:?}, got {result:?}"
        );
    }

    // A plain `-0.0` is a Spark decimal that folds to `+0.0`, so the parser normalizes the sign.
    // `assert_eq` on `Scalar` can't see it (`-0.0 == 0.0`), so check the sign bit directly.
    #[rstest]
    #[case("-0.0")]
    #[case("-0")]
    #[case("-0.00")]
    fn normalizes_negative_zero_to_positive(#[case] sql: &str) {
        let Expression::Literal(Scalar::Double(d)) = parse_sql(sql, &DataType::DOUBLE).unwrap()
        else {
            panic!("expected a Double literal for {sql:?}");
        };
        assert!(
            d == 0.0 && d.is_sign_positive(),
            "DOUBLE {sql:?} kept the sign: {d}"
        );

        let Expression::Literal(Scalar::Float(f)) = parse_sql(sql, &DataType::FLOAT).unwrap()
        else {
            panic!("expected a Float literal for {sql:?}");
        };
        assert!(
            f == 0.0 && f.is_sign_positive(),
            "FLOAT {sql:?} kept the sign: {f}"
        );
    }

    // An exponent literal like `-0.0E0` is a Spark double literal that keeps its sign, so the
    // parser must not normalize it.
    #[rstest]
    #[case("-0.0E0")]
    #[case("-0E0")]
    #[case("-0.0e10")]
    fn preserves_negative_zero_with_exponent(#[case] sql: &str) {
        let Expression::Literal(Scalar::Double(d)) = parse_sql(sql, &DataType::DOUBLE).unwrap()
        else {
            panic!("expected a Double literal for {sql:?}");
        };
        assert!(
            d == 0.0 && d.is_sign_negative(),
            "DOUBLE {sql:?} should keep the negative sign: {d}"
        );

        let Expression::Literal(Scalar::Float(f)) = parse_sql(sql, &DataType::FLOAT).unwrap()
        else {
            panic!("expected a Float literal for {sql:?}");
        };
        assert!(
            f == 0.0 && f.is_sign_negative(),
            "FLOAT {sql:?} should keep the negative sign: {f}"
        );
    }

    #[rstest]
    #[case(r"'a\nb'")] // would-be newline escape
    #[case(r"'c:\temp'")] // literal backslash in a path
    #[case(r"'\\'")] // doubled backslash
    fn rejects_backslash_in_string_literal(#[case] sql: &str) {
        let result = parse_sql(sql, &DataType::STRING);
        assert!(
            result.is_err(),
            "expected error for backslash in string literal {sql:?}, got {result:?}"
        );
    }

    // Spark accepts double-quoted strings; kernel does not yet.
    #[rstest]
    #[case(r#""foo""#)]
    #[case(r#""it's""#)] // embedded single quote
    fn rejects_double_quoted_string(#[case] sql: &str) {
        let result = parse_sql(sql, &DataType::STRING);
        assert!(
            result.is_err(),
            "expected error for double-quoted string {sql:?}, got {result:?}"
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

    #[rstest]
    #[case::struct_target(struct_ty())]
    #[case::array_target(array_ty())]
    #[case::map_target(map_ty())]
    fn null_is_accepted_for_non_primitive_target(#[case] ty: DataType) {
        let got = parse_sql("NULL", &ty).unwrap();
        assert_eq!(got, Expression::literal(Scalar::Null(ty)));
    }
}
