//! Single-comparison CHECK-constraint parser: `<operand> <op> <operand>` into a [`Predicate`].
//!
//! The grammar is exactly one comparison. This splits the string at its single top-level
//! comparison operator, then reuses the crate's existing parsers for each side:
//! [`ColumnName::from_str`] for a (dotted, backtick-quoted) column path, and [`super::parse_sql`]
//! for a literal. A literal's type is inferred from the column on the other side of the
//! comparison. Column references resolve case-insensitively, matching Delta/Spark and kernel's
//! other column-resolution paths.
//!
//! Junctions (`AND`/`OR`/`NOT`), parentheses, and `IS [NOT] NULL` are out of grammar. They fail
//! either at the split (no operator, or more than one) or at operand classification, and the caller
//! treats any error as "not kernel-parsable" (connector-enforced, fail-closed).

use std::str::FromStr;

use super::parse_sql;
use crate::expressions::{ColumnName, Expression, Predicate};
use crate::schema::{DataType, StructType};
use crate::{DeltaResult, Error};

/// Comparison operators and the [`CmpOp`] each produces. `==` aliases `=`; `<>` and `!=` both mean
/// not-equal. [`munch`] picks the longest match, so overlapping spellings (`<`, `<=`, `<=>`) need
/// no particular order here.
const OPERATORS: &[(&str, CmpOp)] = &[
    ("<=>", CmpOp::NullSafeEq),
    ("<=", CmpOp::Le),
    (">=", CmpOp::Ge),
    ("<>", CmpOp::Ne),
    ("!=", CmpOp::Ne),
    ("==", CmpOp::Eq),
    ("<", CmpOp::Lt),
    (">", CmpOp::Gt),
    ("=", CmpOp::Eq),
];

/// A comparison operator. Kernel has no native `<=`/`>=`/`!=`/`<=>`; [`lower`] maps each to the
/// corresponding [`Predicate`] constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    /// `<=>`, null-safe equality (`NULL <=> NULL` is true); lowers to a negated distinct
    /// predicate.
    NullSafeEq,
}

/// A classified comparison side: a column resolved against the schema (canonical schema-cased path
/// and leaf type), or a literal's raw source text (typed by [`lower`] from the column it is
/// compared against).
enum Operand<'a> {
    Column {
        canonical: Vec<String>,
        data_type: DataType,
    },
    Literal(&'a str),
}

/// Parse a single-comparison CHECK-constraint SQL string into a kernel [`Predicate`], resolving
/// column references against `schema` and inferring each literal's type from the column it is
/// compared against.
///
/// The supported grammar is exactly one comparison `<operand> <op> <operand>`, where each operand
/// is a column reference (case-insensitive, dotted paths allowed), a literal, or `NULL`, and
/// `<op>` is one of `< <= > >= = == != <> <=>`. Literal leaves are parsed by [`super::parse_sql`],
/// typed from the column on the other side of the comparison; a comparison must therefore
/// reference at least one column.
///
/// Junctions (`AND`/`OR`/`NOT`), parentheses, and `IS [NOT] NULL` are intentionally out of scope
/// and surface as errors.
///
/// The returned [`Predicate`] carries no CHECK-constraint NULL convention on its own. A SQL CHECK
/// constraint fails a row only when the predicate evaluates to FALSE; a TRUE *or NULL* result
/// passes (e.g. `CHECK (x > 0)` passes when `x` is NULL). The caller enforcing the constraint must
/// therefore reject a row only on FALSE, not "keep rows where the predicate is TRUE"; the latter
/// wrongly drops every NULL row.
///
/// # Errors
///
/// Returns an error for any input outside the supported grammar (junctions, functions, arithmetic,
/// `IN`, `BETWEEN`, ...), for unknown columns, and for type-incompatible literals. Callers treat
/// that error as the signal that a constraint is *not kernel-parsable*.
// TODO: remove the allow once check-constraints discovery calls this. It has no in-crate caller,
//       so dead-code analysis flags the entry point until then.
#[allow(dead_code)]
pub(crate) fn parse_sql_simple_predicate(sql: &str, schema: &StructType) -> DeltaResult<Predicate> {
    let (left, op, right) = split_at_operator(sql)?;
    lower(op, classify(left, schema)?, classify(right, schema)?)
}

/// Split `sql` at its single top-level comparison operator, ignoring operator characters inside a
/// `'...'` string or `` `...` `` backtick-quoted identifier. Returns the trimmed left text, the
/// operator, and the trimmed right text.
///
/// Errors if there is no operator or more than one (a chained comparison like `0 < a < 10` is not a
/// single comparison, so it is out of grammar).
fn split_at_operator(sql: &str) -> DeltaResult<(&str, CmpOp, &str)> {
    let mut span: Option<char> = None;
    let mut found: Option<(usize, usize, CmpOp)> = None;
    let mut chars = sql.char_indices();
    while let Some((i, c)) = chars.next() {
        match (span, c) {
            // Inside a quoted span, only the matching close-quote is significant.
            (Some(quote), c) if c == quote => span = None,
            (Some(_), _) => {}
            (None, '\'' | '`') => span = Some(c),
            (None, _) => {
                if let Some((op, len)) = munch(&sql[i..]) {
                    if found.replace((i, i + len, op)).is_some() {
                        return Err(Error::generic(format!(
                            "CHECK constraint supports only a single comparison: {sql}"
                        )));
                    }
                    // Skip the operator's remaining characters so `<=` is not re-read as `<` then
                    // `=`. Operators are ASCII, so one `next()` per byte past the first.
                    for _ in 1..len {
                        chars.next();
                    }
                }
            }
        }
    }
    let (start, end, op) =
        found.ok_or_else(|| Error::generic(format!("no comparison operator in: {sql}")))?;
    Ok((sql[..start].trim(), op, sql[end..].trim()))
}

/// The longest operator `rest` begins with, and its byte length, or `None` if none. Maximal munch:
/// `<=>` wins over `<=` wins over `<`, so a prefix operator never shadows a longer one.
fn munch(rest: &str) -> Option<(CmpOp, usize)> {
    OPERATORS
        .iter()
        .filter(|(spelling, _)| rest.starts_with(spelling))
        .max_by_key(|(spelling, _)| spelling.len())
        .map(|(spelling, op)| (*op, spelling.len()))
}

/// Classify one side of the comparison against `schema`.
///
/// `TRUE`/`FALSE`/`NULL` are literals even when a same-named column exists: Spark treats them as
/// keywords and requires backticks to reference such a column, so a bare spelling is never a column
/// reference. Otherwise, text that parses as a [`ColumnName`] and resolves in `schema` is a column;
/// anything else is a literal for [`super::parse_sql`] to decode.
fn classify<'a>(text: &'a str, schema: &StructType) -> DeltaResult<Operand<'a>> {
    let is_reserved = ["true", "false", "null"]
        .iter()
        .any(|kw| text.eq_ignore_ascii_case(kw));
    if !is_reserved {
        if let Ok(column) = ColumnName::from_str(text) {
            if let Ok((canonical, data_type)) = resolve_column(&column, schema) {
                return Ok(Operand::Column {
                    canonical,
                    data_type,
                });
            }
        }
    }
    Ok(Operand::Literal(text))
}

/// Resolve a column path against `schema`, returning the *canonical* path (the schema's stored
/// field names) and the leaf field's type. The canonical names are what the engine sees in the
/// logical batch, so the emitted column reference must use them rather than the as-written casing.
fn resolve_column(
    column: &ColumnName,
    schema: &StructType,
) -> DeltaResult<(Vec<String>, DataType)> {
    let fields = schema.fields_of_path_ci(column)?;
    let canonical = fields.iter().map(|f| f.name().to_string()).collect();
    // `fields_of_path_ci` errors on an empty path, so on success there is always a leaf.
    let leaf = fields
        .last()
        .ok_or_else(|| Error::generic("empty column path"))?;
    Ok((canonical, leaf.data_type().clone()))
}

/// Lower a classified comparison into a kernel [`Predicate`].
///
/// Kernel can only lower a comparison it can evaluate to the *same* boolean Spark would; anything
/// else is rejected so the caller treats the constraint as not-kernel-parsable (connector-
/// enforced). The operands must therefore satisfy all of:
/// - at least one operand is a column, so a literal can be typed from it;
/// - every column operand is primitive-typed (struct/array/map have no scalar comparison);
/// - the two operands share a single comparison type, because the engine compares only matching
///   types (no implicit numeric coercion). See [`type_literal_from_column`] (FLOAT-vs-literal) and
///   the column-vs-column arm (cross-type columns).
///
/// Even a predicate that lowers can still diverge from Spark at *evaluation* time, on comparison
/// semantics the parser cannot re-express as a `Predicate`. These are engine-wide gaps (not
/// constraint-specific) and are left to the enforcement layer:
/// - NaN: Spark SQL makes `NaN = NaN` true and orders NaN above every value; kernel compares floats
///   with IEEE semantics (NaN unordered), so a float/double constraint on a NaN-bearing column can
///   silently disagree, affecting even the DOUBLE and FLOAT-vs-FLOAT cases that lower here.
/// - String collation: kernel compares bytewise; a non-default collation (e.g. `UTF8_LCASE`) would
///   compare differently under Spark.
fn lower(op: CmpOp, left: Operand<'_>, right: Operand<'_>) -> DeltaResult<Predicate> {
    for operand in [&left, &right] {
        if let Operand::Column {
            canonical,
            data_type,
        } = operand
        {
            if !matches!(data_type, DataType::Primitive(_)) {
                return Err(Error::generic(format!(
                    "CHECK constraint can only compare primitive-typed columns, but '{}' has type {data_type:?}",
                    canonical.join(".")
                )));
            }
        }
    }
    // Resolve the single type the comparison happens at. A literal is typed from the column on
    // the other side, so at least one operand must be a column. The engine compares only matching
    // Arrow types (no numeric coercion), so a cross-type column-vs-column comparison Spark would
    // coerce is rejected; the full-`DataType` check also rejects mismatched decimal
    // precision/scale and TIMESTAMP vs TIMESTAMP_NTZ.
    let cmp_type = match (&left, &right) {
        (
            Operand::Column {
                canonical: l,
                data_type: l_type,
            },
            Operand::Column {
                canonical: r,
                data_type: r_type,
            },
        ) if l_type != r_type => {
            return Err(Error::generic(format!(
                "CHECK constraint comparing columns of different types is not supported: \
                 '{}' has type {l_type:?}, '{}' has type {r_type:?}",
                l.join("."),
                r.join(".")
            )))
        }
        (Operand::Column { data_type, .. }, _) | (_, Operand::Column { data_type, .. }) => {
            data_type.clone()
        }
        (Operand::Literal(_), Operand::Literal(_)) => {
            return Err(Error::generic(
                "CHECK constraint comparison must reference at least one column",
            ))
        }
    };
    // A column becomes a reference; a literal is typed from `cmp_type` via `parse_sql`.
    let convert = |operand: Operand<'_>| match operand {
        Operand::Column { canonical, .. } => Ok(Expression::column(canonical)),
        Operand::Literal(raw) => type_literal_from_column(raw, &cmp_type),
    };
    let (left, right) = (convert(left)?, convert(right)?);
    Ok(match op {
        CmpOp::Eq => Predicate::eq(left, right),
        CmpOp::Ne => Predicate::ne(left, right),
        CmpOp::Lt => Predicate::lt(left, right),
        CmpOp::Le => Predicate::le(left, right),
        CmpOp::Gt => Predicate::gt(left, right),
        CmpOp::Ge => Predicate::ge(left, right),
        // Null-safe equal: `a <=> b` is "a is not distinct from b". Kernel has no direct
        // constructor, so negate the distinct predicate.
        CmpOp::NullSafeEq => Predicate::not(Predicate::distinct(left, right)),
    })
}

/// Type a literal from the column it is compared with (via [`parse_sql`]), rejecting the
/// FLOAT-column-vs-DECIMAL/DOUBLE-literal cases where kernel would obviously disagree with Spark.
///
/// Kernel types a literal from the compared column, so against a FLOAT column it narrows the
/// literal to f32 and compares at f32. Spark instead types the literal on its own and coerces to a
/// common type:
/// - DECIMAL/DOUBLE literal (has a `.`/exponent, or an integer beyond i64): the common type is
///   DOUBLE, so Spark widens the *column* to f64 and compares at f64. For a literal not exactly
///   representable in f32 (e.g. `0.1`) this reaches the opposite result from kernel's f32 compare,
///   silently. Kernel has no cast expression and the engine cannot compare f32 to f64, so it cannot
///   reproduce Spark's widening; these are rejected and left to the connector.
/// - INT/LONG literal (an integer in i64 range): allowed. Under legacy coercion the common type is
///   FLOAT, so Spark casts the *literal* to f32 and compares at f32, matching kernel. Under ANSI
///   coercion (the default) INT+FLOAT widens to DOUBLE, so Spark compares the widened column at
///   f64; that disagrees with kernel's f32 compare only for a literal not exactly representable in
///   f32 (`|literal| > 2^24`), where the connector's enforcement layer is the backstop.
///
/// `NULL` is allowed (a null comparison involves no f32/f64 rounding). The i64 gate is exactly
/// Spark's INT/LONG-vs-DECIMAL literal boundary. Only FLOAT is affected; a DOUBLE column already
/// compares at f64, matching Spark.
fn type_literal_from_column(raw: &str, column_type: &DataType) -> DeltaResult<Expression> {
    let is_null = raw.eq_ignore_ascii_case("null");
    let is_int_or_long_literal = raw.parse::<i64>().is_ok();
    if column_type == &DataType::FLOAT && !is_null && !is_int_or_long_literal {
        return Err(Error::generic(format!(
            "CHECK constraint comparing a FLOAT column against the literal '{raw}' is not supported: \
             Spark widens the column to DOUBLE (compares at f64) while kernel compares at f32"
        )));
    }
    parse_sql(raw, column_type)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::parse_sql_simple_predicate;
    use crate::expressions::{Expression, Predicate, Scalar};
    use crate::schema::{DataType, StructField, StructType};

    fn schema() -> StructType {
        StructType::new_unchecked([
            StructField::nullable("amount", DataType::LONG),
            StructField::nullable("amount2", DataType::LONG),
            StructField::nullable("price", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("active", DataType::BOOLEAN),
            StructField::nullable("ratio", DataType::DOUBLE),
            StructField::nullable("weight", DataType::FLOAT),
            StructField::nullable("height", DataType::FLOAT),
            StructField::nullable("created", DataType::DATE),
            StructField::nullable("data", DataType::BINARY),
            StructField::nullable("ts", DataType::TIMESTAMP),
            StructField::nullable("tsn", DataType::TIMESTAMP_NTZ),
            StructField::nullable(
                "nested",
                DataType::try_struct_type([StructField::nullable("inner", DataType::LONG)])
                    .unwrap(),
            ),
        ])
    }

    fn col(name: &str) -> Expression {
        Expression::column([name])
    }

    /// Kernel-parsable inputs: exactly one `operand <op> operand` comparison between a column and a
    /// column, a literal, or `NULL`, for every comparison operator and across the primitive literal
    /// types. Each literal is typed from the column it is compared against (via `parse_sql`), and
    /// column names resolve case-insensitively to their canonical casing. All of these lower to a
    /// `Predicate`.
    #[rstest]
    // Every comparison operator (`=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`).
    #[case::gt("amount > 0", Predicate::gt(col("amount"), Expression::literal(0i64)))]
    #[case::lt("amount < 0", Predicate::lt(col("amount"), Expression::literal(0i64)))]
    #[case::ge("amount >= 0", Predicate::ge(col("amount"), Expression::literal(0i64)))]
    #[case::le("amount <= 0", Predicate::le(col("amount"), Expression::literal(0i64)))]
    #[case::eq("amount = 0", Predicate::eq(col("amount"), Expression::literal(0i64)))]
    #[case::eq_double("amount == 0", Predicate::eq(col("amount"), Expression::literal(0i64)))]
    #[case::ne_bang("amount != 0", Predicate::ne(col("amount"), Expression::literal(0i64)))]
    #[case::ne_angle("amount <> 0", Predicate::ne(col("amount"), Expression::literal(0i64)))]
    // Whitespace between tokens is optional.
    #[case::no_whitespace("amount>0", Predicate::gt(col("amount"), Expression::literal(0i64)))]
    // A literal is typed from the column on the other side: `price` is INTEGER, not the default
    // Long.
    #[case::literal_typed_from_column(
        "price <= 10",
        Predicate::le(col("price"), Expression::literal(10i32))
    )]
    // Literal forms across the primitive type matrix, each typed from its column. Exercises the
    // bareword (`TRUE`), fractional-number, typed-literal (`DATE '...'`), and binary (`X'..'`)
    // classification paths.
    #[case::boolean_literal(
        "active = TRUE",
        Predicate::eq(col("active"), Expression::literal(Scalar::Boolean(true)))
    )]
    #[case::double_literal(
        "ratio < 1.5",
        Predicate::lt(col("ratio"), Expression::literal(Scalar::Double(1.5)))
    )]
    #[case::typed_date_literal(
        "created = DATE '1970-01-02'",
        Predicate::eq(col("created"), Expression::literal(Scalar::Date(1)))
    )]
    #[case::binary_literal(
        "data = X'01ff'",
        Predicate::eq(col("data"), Expression::literal(Scalar::Binary(vec![0x01, 0xff])))
    )]
    // Leading-dot and exponent numeric forms each classify as a single numeric literal.
    #[case::leading_dot_decimal(
        "ratio > .5",
        Predicate::gt(col("ratio"), Expression::literal(Scalar::Double(0.5)))
    )]
    #[case::exponent_number(
        "ratio < 1e3",
        Predicate::lt(col("ratio"), Expression::literal(Scalar::Double(1000.0)))
    )]
    // Signed numbers: a leading sign, and a signed exponent, each stay part of one numeric literal.
    #[case::negative_literal(
        "amount = -5",
        Predicate::eq(col("amount"), Expression::literal(-5i64))
    )]
    #[case::signed_exponent(
        "ratio >= -2e+1",
        Predicate::ge(col("ratio"), Expression::literal(Scalar::Double(-20.0)))
    )]
    // A leading sign immediately followed by a leading-dot decimal stays one numeric literal.
    #[case::negative_leading_dot_decimal(
        "ratio > -.5",
        Predicate::gt(col("ratio"), Expression::literal(Scalar::Double(-0.5)))
    )]
    // Timestamp typed literals: LTZ (bare `TIMESTAMP` or explicit `TIMESTAMP_LTZ`, both requiring a
    // `Z` suffix) and zoneless `TIMESTAMP_NTZ`.
    #[case::typed_timestamp(
        "ts = TIMESTAMP '1970-01-01T00:00:00Z'",
        Predicate::eq(col("ts"), Expression::literal(Scalar::Timestamp(0)))
    )]
    #[case::typed_timestamp_ltz(
        "ts = TIMESTAMP_LTZ '1970-01-01T00:00:00Z'",
        Predicate::eq(col("ts"), Expression::literal(Scalar::Timestamp(0)))
    )]
    #[case::typed_timestamp_ntz(
        "tsn = TIMESTAMP_NTZ '1970-01-01 00:00:00'",
        Predicate::eq(col("tsn"), Expression::literal(Scalar::TimestampNtz(0)))
    )]
    // Column casing in the source is normalized to the schema's stored casing.
    #[case::case_insensitive_column(
        "AMOUNT >= 0",
        Predicate::ge(col("amount"), Expression::literal(0i64))
    )]
    #[case::string_with_doubled_quote(
        "name = 'O''Brien'",
        Predicate::eq(col("name"), Expression::literal("O'Brien"))
    )]
    // An operator character inside a string literal is not the comparison operator: the split
    // skips the quoted span, so the sole operator is the `=`.
    #[case::operator_char_inside_string_literal(
        "name = '< >='",
        Predicate::eq(col("name"), Expression::literal("< >="))
    )]
    // Same-type column-vs-column comparisons are allowed; cross-type ones are rejected (see the
    // not-kernel-parsable cases below). FLOAT-vs-FLOAT stays at f32, matching Spark.
    #[case::column_vs_column("amount2 < amount", Predicate::lt(col("amount2"), col("amount")))]
    #[case::float_column_vs_column("weight < height", Predicate::lt(col("weight"), col("height")))]
    #[case::literal_on_left("0 < amount", Predicate::lt(Expression::literal(0i64), col("amount")))]
    // An INT/LONG-range literal against a FLOAT column is safe: Spark casts the literal to f32 and
    // compares at f32, exactly as kernel does (only fractional/DOUBLE/`>i64` literals diverge).
    #[case::float_column_vs_int_literal(
        "weight > 0",
        Predicate::gt(col("weight"), Expression::literal(Scalar::Float(0.0)))
    )]
    #[case::float_column_int_literal_on_left(
        "5 < weight",
        Predicate::lt(Expression::literal(Scalar::Float(5.0)), col("weight"))
    )]
    // `NULL` is an operand, typed from the column it is compared against.
    #[case::null_operand(
        "name = NULL",
        Predicate::eq(col("name"), Expression::literal(Scalar::Null(DataType::STRING)))
    )]
    // Null-safe equality `<=>` lowers to a negated distinct predicate, for both a column-column
    // and a column-NULL comparison (the latter is the operator's whole point: `x <=> NULL` is a
    // total, non-NULL test rather than the always-unknown `x = NULL`).
    #[case::null_safe_eq_columns(
        "amount <=> amount2",
        Predicate::not(Predicate::distinct(col("amount"), col("amount2")))
    )]
    #[case::null_safe_eq_null(
        "name <=> NULL",
        Predicate::not(Predicate::distinct(
            col("name"),
            Expression::literal(Scalar::Null(DataType::STRING))
        ))
    )]
    // Dotted paths resolve into nested structs.
    #[case::nested_column(
        "nested.inner > 0",
        Predicate::gt(Expression::column(["nested", "inner"]), Expression::literal(0i64))
    )]
    fn kernel_parsable_comparison_lowers_to_expected_predicate(
        #[case] sql: &str,
        #[case] expected: Predicate,
    ) {
        assert_eq!(
            parse_sql_simple_predicate(sql, &schema()).unwrap(),
            expected
        );
    }

    /// Not-kernel-parsable inputs: everything outside the single-comparison grammar must error, so
    /// the caller treats the constraint as not-kernel-parsable (connector-enforced / fail-closed).
    /// Covers boolean junctions, parentheses, `IS [NOT] NULL`, multi-operand predicates
    /// (`IN`/`BETWEEN`/`LIKE`), functions, arithmetic, comparisons with no column,
    /// type-incompatible literals (kernel applies no implicit casts), and malformed input.
    #[rstest]
    // Boolean structure beyond a single comparison.
    #[case::and_junction("amount > 0 AND price < 10")]
    #[case::or_junction("amount > 0 OR price < 10")]
    #[case::not_prefix("NOT active")]
    #[case::parentheses("(amount > 0)")]
    #[case::is_null("name IS NULL")]
    #[case::is_not_null("name IS NOT NULL")]
    // Multi-operand predicate forms are not comparisons.
    #[case::in_list("amount IN (1, 2)")]
    #[case::between("amount BETWEEN 0 AND 10")]
    #[case::like("name LIKE 'a%'")]
    // A bare operand is not a comparison (no `<op>`), even when boolean-typed.
    #[case::bare_boolean_column("active")]
    // Operands richer than a column/literal/NULL.
    #[case::function_call("length(name) > 0")]
    #[case::arithmetic("amount + 1 > 0")]
    // A chained comparison is more than one operator, so it is out of grammar.
    #[case::chained_comparison("0 < amount < 10")]
    // A comparison must reference at least one column (so a literal can be typed).
    #[case::two_literals("1 > 0")]
    #[case::two_nulls("NULL = NULL")]
    #[case::unknown_column("nope > 0")]
    // Cross-type column-vs-column comparisons are rejected: the engine compares only matching types
    // (no coercion), while Spark coerces to a common type. Same-type pairs lower (see above).
    #[case::cross_type_int_long_columns("price < amount")]
    #[case::cross_type_float_double_columns("weight < ratio")]
    // Non-primitive (struct/array/map) columns have no scalar comparison and are rejected on either
    // side of the operator, symmetric with the literal path, which rejects non-primitive targets.
    #[case::struct_column_vs_struct_column("nested = nested")]
    #[case::struct_column_vs_null("nested = NULL")]
    #[case::struct_column_vs_literal("nested = 0")]
    // A non-primitive column on the right is rejected too (the guard checks both operands).
    #[case::primitive_column_vs_struct_column("amount = nested")]
    // No implicit casts: a literal whose SQL form does not match the compared column's type is
    // rejected, including a quoted number for a numeric column, which Spark would coerce.
    #[case::string_literal_for_numeric_column("amount = 'foo'")]
    #[case::quoted_number_for_numeric_column("amount = '10'")]
    #[case::literal_out_of_range_for_column("price = 9999999999")]
    // A FLOAT column against a DECIMAL/DOUBLE literal (a `.`/exponent, or an integer beyond i64) is
    // rejected: Spark widens the column to f64 while kernel compares at f32, disagreeing silently
    // for an f32-inexact literal. The exponent and `>i64` cases are rejected conservatively even
    // though they would happen to agree; INT/LONG-range literals are allowed (see above).
    #[case::float_column_inexact_decimal_literal("weight = 0.1")]
    #[case::float_column_decimal_literal_on_left("0.1 < weight")]
    #[case::float_column_f32_exact_decimal_literal("weight = 0.5")]
    #[case::float_column_exponent_literal("weight = 1e3")]
    #[case::float_column_integer_beyond_i64("weight > 99999999999999999999")]
    // Malformed input.
    #[case::unterminated_string("name = 'oops")]
    #[case::bang_without_eq("amount ! 0")]
    #[case::malformed_dotted_path("nested..inner > 0")]
    #[case::missing_operator("amount price")]
    #[case::trailing_tokens("amount > 0 extra")]
    #[case::operator_only(">")]
    #[case::empty("")]
    fn not_kernel_parsable_input_is_rejected(#[case] sql: &str) {
        assert!(
            parse_sql_simple_predicate(sql, &schema()).is_err(),
            "expected {sql:?} to be rejected as not kernel-parsable"
        );
    }

    /// A bare `TRUE`/`FALSE`/`NULL` is a literal even when a same-named column exists, matching
    /// Spark, which treats these as keywords. A backtick-quoted `` `true` `` instead references the
    /// column. Pinned because operand classification resolves columns against the schema, so
    /// without the reserved-keyword guard a bare `TRUE` would wrongly bind to the column.
    #[rstest]
    #[case::bare_true_is_literal(
        "active = TRUE",
        Predicate::eq(col("active"), Expression::literal(Scalar::Boolean(true)))
    )]
    #[case::backtick_true_is_column("active = `true`", Predicate::eq(col("active"), col("true")))]
    fn reserved_keyword_is_literal_unless_backtick_quoted(
        #[case] sql: &str,
        #[case] expected: Predicate,
    ) {
        let schema = StructType::new_unchecked([
            StructField::nullable("active", DataType::BOOLEAN),
            StructField::nullable("true", DataType::BOOLEAN),
        ]);
        assert_eq!(parse_sql_simple_predicate(sql, &schema).unwrap(), expected);
    }

    /// An operator character inside a backtick-quoted column name is part of the identifier, not
    /// the comparison operator: the split skips the backtick span, so the sole operator is the `=`.
    #[test]
    fn operator_char_inside_backtick_column_is_not_the_comparison_operator() {
        let schema = StructType::new_unchecked([StructField::nullable("a>b", DataType::LONG)]);
        assert_eq!(
            parse_sql_simple_predicate("`a>b` = 0", &schema).unwrap(),
            Predicate::eq(Expression::column(["a>b"]), Expression::literal(0i64))
        );
    }
}
