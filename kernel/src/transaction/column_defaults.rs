//! Parsing and validation of `CURRENT_DEFAULT` column-default expressions.
//!
//! Per the Delta protocol [Default Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#default-columns)
//! feature, a field may carry a `CURRENT_DEFAULT` metadata entry whose value is a SQL
//! expression string. Writers substitute the result of evaluating that expression
//! whenever a row lacks an explicit value for the column.
//!
//! This prototype supports only **literal** SQL expressions. Non-literal expressions
//! (e.g. `current_timestamp()`) are rejected by the built-in parser; connectors that
//! need them should evaluate `StructField::raw_default_value()` themselves and supply
//! fully-materialized columns.
//!
//! The [`DefaultExpressionParser`] trait is the extensibility seam: a richer parser
//! (full SQL grammar) can replace [`LiteralDefaultExpressionParser`] without touching
//! the write path.
//!
//! Mirrors the validation rules in kernel-java's `ColumnDefaults.java`.

use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, PrimitiveType, StructField};
use crate::{DeltaResult, Error};

/// A column's `CURRENT_DEFAULT` expression, surfaced to connectors via
/// [`Transaction::columns_with_defaults`]. Connectors use this struct to decide, per
/// column, whether to evaluate the default themselves (Mode A: read `raw_sql` and use a
/// connector-side SQL engine) or to delegate to kernel via
/// [`Transaction::with_default_filled_columns`] (Mode B: rely on `parsed`).
///
/// `parsed` is `None` when kernel's built-in parser cannot interpret `raw_sql` (e.g. for
/// non-literal expressions like `current_timestamp()`). Connectors that need such
/// expressions must run them through their own evaluator.
///
/// [`Transaction::columns_with_defaults`]: super::Transaction::columns_with_defaults
/// [`Transaction::with_default_filled_columns`]: super::Transaction::with_default_filled_columns
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefault {
    /// The raw SQL expression as stored in the field's `CURRENT_DEFAULT` metadata.
    pub raw_sql: String,
    /// The parsed default as a kernel [`Expression`], or `None` if kernel's literal-only
    /// parser could not interpret `raw_sql`.
    pub parsed: Option<Expression>,
}

/// Attempts to extract a [`ColumnDefault`] from a [`StructField`].
///
/// Returns `None` if the field does not carry a `CURRENT_DEFAULT` metadata entry. If the
/// entry is present but kernel's built-in literal parser cannot interpret it, the parse
/// error is swallowed and `parsed` is set to `None` -- discovery surfaces the raw SQL so
/// connectors can decide what to do, and parseability is re-validated only when the
/// connector explicitly opts into kernel-side fill via
/// [`Transaction::with_default_filled_columns`].
///
/// [`Transaction::with_default_filled_columns`]: super::Transaction::with_default_filled_columns
pub(super) fn try_parse(field: &StructField) -> Option<ColumnDefault> {
    let raw_sql = field.raw_default_value()?.to_string();
    let parsed = LiteralDefaultExpressionParser
        .parse(&raw_sql, field.data_type())
        .ok();
    Some(ColumnDefault { raw_sql, parsed })
}

/// Parses a `CURRENT_DEFAULT` SQL expression string into a kernel [`Expression`],
/// validated against the column's target [`DataType`].
///
/// Implementations must be pure -- parsing the same `(raw, data_type)` must always
/// yield the same result -- because kernel caches the result on the write context.
pub(crate) trait DefaultExpressionParser: Send + Sync {
    /// Parse `raw` as a default-value expression for a column of type `data_type`.
    ///
    /// Returns an error if `raw` cannot be represented as an expression of the
    /// requested type by this parser.
    fn parse(&self, raw: &str, data_type: &DataType) -> DeltaResult<Expression>;
}

/// Literal-only default-expression parser. Accepts the same shape of literals as
/// kernel-java's `ColumnDefaults.validateLiteral`:
///
/// - `null` (case-insensitive) -> `Scalar::Null`
/// - quoted strings (single or double) -> `Scalar::String` / `Scalar::Binary`
/// - numeric literals -> `Scalar::{Byte,Short,Integer,Long,Float,Double,Decimal}`
/// - `true` / `false` (case-insensitive) -> `Scalar::Boolean`
/// - ISO date / timestamp literals -> `Scalar::{Date,Timestamp,TimestampNtz}`
/// - Variant columns: must be the literal `null`.
///
/// Struct/Array/Map targets are rejected.
pub(crate) struct LiteralDefaultExpressionParser;

impl DefaultExpressionParser for LiteralDefaultExpressionParser {
    fn parse(&self, raw: &str, data_type: &DataType) -> DeltaResult<Expression> {
        let scalar = parse_literal(raw, data_type)?;
        Ok(Expression::literal(scalar))
    }
}

fn parse_literal(raw: &str, data_type: &DataType) -> DeltaResult<Scalar> {
    // The protocol treats `null` as a valid default for any nullable type. Variant columns
    // MUST default to null per the spec.
    if raw.trim().eq_ignore_ascii_case("null") {
        return Ok(Scalar::Null(data_type.clone()));
    }

    match data_type {
        DataType::Primitive(p) => parse_primitive_literal(raw, p),
        DataType::Variant(_) => Err(unsupported(
            raw,
            data_type,
            "variant columns must default to null",
        )),
        DataType::Struct(_) | DataType::Array(_) | DataType::Map(_) => Err(unsupported(
            raw,
            data_type,
            "nested-typed columns cannot have literal defaults",
        )),
    }
}

fn parse_primitive_literal(raw: &str, ptype: &PrimitiveType) -> DeltaResult<Scalar> {
    match ptype {
        PrimitiveType::String => {
            let unquoted = strip_quotes(raw).ok_or_else(|| {
                unsupported(
                    raw,
                    &DataType::Primitive(ptype.clone()),
                    "string default must be enclosed in single or double quotes",
                )
            })?;
            Ok(Scalar::String(unquoted.to_string()))
        }
        PrimitiveType::Binary => {
            let unquoted = strip_quotes(raw).ok_or_else(|| {
                unsupported(
                    raw,
                    &DataType::Primitive(ptype.clone()),
                    "binary default must be enclosed in single or double quotes",
                )
            })?;
            Ok(Scalar::Binary(unquoted.as_bytes().to_vec()))
        }
        // Numeric, boolean, date, and timestamp literals all delegate to the existing
        // protocol-compliant scalar parser.
        _ => ptype.parse_scalar(raw),
    }
}

/// If `s` is wrapped in matching single or double quotes, return the inner slice.
/// Otherwise return `None`.
fn strip_quotes(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

fn unsupported(raw: &str, data_type: &DataType, reason: &str) -> Error {
    Error::generic(format!(
        "unsupported column default value {raw:?} for column of type {data_type}: {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{DataType, DecimalType, PrimitiveType};

    fn parse(raw: &str, dt: DataType) -> DeltaResult<Scalar> {
        parse_literal(raw, &dt)
    }

    #[rstest]
    #[case::quoted_single("'hello'", "hello")]
    #[case::quoted_double("\"hello\"", "hello")]
    #[case::empty_single("''", "")]
    fn string_literal_ok(#[case] raw: &str, #[case] expected: &str) {
        let scalar = parse(raw, DataType::STRING).unwrap();
        assert_eq!(scalar, Scalar::String(expected.to_string()));
    }

    #[rstest]
    #[case::unquoted("hello")]
    #[case::mismatched_quotes("'hello\"")]
    #[case::single_char_quote("'")]
    fn string_literal_err(#[case] raw: &str) {
        parse(raw, DataType::STRING).unwrap_err();
    }

    #[test]
    fn null_literal_is_scalar_null() {
        let scalar = parse("null", DataType::INTEGER).unwrap();
        assert_eq!(scalar, Scalar::Null(DataType::INTEGER));
        let scalar = parse("NULL", DataType::STRING).unwrap();
        assert_eq!(scalar, Scalar::Null(DataType::STRING));
    }

    #[rstest]
    #[case::positive_int(DataType::INTEGER, "42", Scalar::Integer(42))]
    #[case::negative_long(DataType::LONG, "-9000000000", Scalar::Long(-9_000_000_000))]
    #[case::boolean_true(DataType::BOOLEAN, "true", Scalar::Boolean(true))]
    #[case::boolean_upper(DataType::BOOLEAN, "FALSE", Scalar::Boolean(false))]
    #[case::date(DataType::DATE, "2024-01-01", Scalar::Date(19723))]
    fn primitive_literal_ok(#[case] dt: DataType, #[case] raw: &str, #[case] expected: Scalar) {
        assert_eq!(parse(raw, dt).unwrap(), expected);
    }

    #[rstest]
    #[case::int_overflow(DataType::INTEGER, "9999999999")]
    #[case::bool_garbage(DataType::BOOLEAN, "yes")]
    #[case::date_garbage(DataType::DATE, "not-a-date")]
    fn primitive_literal_err(#[case] dt: DataType, #[case] raw: &str) {
        parse(raw, dt).unwrap_err();
    }

    #[test]
    fn decimal_literal_ok() {
        let dt = DataType::Primitive(PrimitiveType::Decimal(DecimalType::try_new(5, 2).unwrap()));
        let s = parse("12.34", dt).unwrap();
        match s {
            Scalar::Decimal(d) => assert_eq!(d.bits(), 1234),
            other => panic!("expected decimal, got {other:?}"),
        }
    }

    #[test]
    fn variant_only_null_allowed() {
        let variant = DataType::unshredded_variant();
        parse("null", variant.clone()).unwrap();
        parse("'foo'", variant).unwrap_err();
    }

    #[test]
    fn non_literal_expression_rejected() {
        let err = parse("current_timestamp()", DataType::TIMESTAMP).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("current_timestamp"), "got: {msg}");
    }

    #[test]
    fn nested_types_rejected() {
        use crate::schema::{StructField, StructType};
        let dt = DataType::Struct(Box::new(StructType::new_unchecked(vec![
            StructField::nullable("x", DataType::INTEGER),
        ])));
        parse("'whatever'", dt).unwrap_err();
    }

    #[test]
    fn literal_parser_wraps_expression() {
        let parser = LiteralDefaultExpressionParser;
        let expr = parser.parse("'hello'", &DataType::STRING).unwrap();
        assert_eq!(expr, Expression::literal(Scalar::String("hello".into())));
    }

    // ==================== try_parse / ColumnDefault tests ====================

    use crate::schema::StructField;

    #[test]
    fn try_parse_returns_none_when_no_default() {
        let field = StructField::nullable("x", DataType::INTEGER);
        assert!(try_parse(&field).is_none());
    }

    #[test]
    fn try_parse_populates_parsed_for_literal() {
        use crate::schema::ColumnMetadataKey;

        let field = StructField::nullable("greet", DataType::STRING)
            .add_metadata([(ColumnMetadataKey::CurrentDefault.as_ref(), "'hello'")]);
        let result = try_parse(&field).unwrap();
        assert_eq!(result.raw_sql, "'hello'");
        assert_eq!(
            result.parsed,
            Some(Expression::literal(Scalar::String("hello".into())))
        );
    }

    #[test]
    fn try_parse_leaves_parsed_none_for_non_literal() {
        use crate::schema::ColumnMetadataKey;

        let field = StructField::nullable("ts", DataType::TIMESTAMP).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref(),
            "current_timestamp()",
        )]);
        let result = try_parse(&field).unwrap();
        assert_eq!(result.raw_sql, "current_timestamp()");
        assert!(
            result.parsed.is_none(),
            "non-literal SQL should leave parsed = None, got: {:?}",
            result.parsed
        );
    }
}
