//! Lowering: resolve a parsed [`Comparison`] against the table schema into a kernel [`Predicate`].
//!
//! Column references resolve case-insensitively (matching Delta/Spark and kernel's other
//! column-resolution paths). A literal's type is inferred from the column on the other side of the
//! comparison, and the literal itself is parsed by reusing [`super::parse_sql`].

// WIP feature behind `check-constraints-in-dev`; some items have no caller until discovery lands.
// TODO(#2896): remove this allow once check-constraint discovery wires up a caller.
#![allow(dead_code)]

use super::parse_sql;
use super::parser::{CmpOp, Comparison, Operand};
use crate::expressions::{ColumnName, Expression, Predicate};
use crate::schema::{DataType, StructType};
use crate::{DeltaResult, Error};

/// Lower a parsed [`Comparison`] into a kernel [`Predicate`], resolving each operand against
/// `schema`.
///
/// Kernel only lowers a comparison it can evaluate to the *same* boolean the writer would;
/// anything else returns `Err` (what the caller does with an un-lowerable constraint is the
/// caller's contract, not this function's). The resolved operands must therefore satisfy all of:
/// - at least one operand is a column, so a literal can be typed from it;
/// - every column operand is primitive-typed (struct/array/map have no scalar comparison);
/// - the two operands share a single comparison type, because the engine compares only matching
///   types (no implicit numeric coercion) -- see [`type_literal_from_column`] (FLOAT-vs-literal)
///   and the column-vs-column arm (cross-type columns).
///
/// Even a predicate that lowers can still diverge from Spark at *evaluation* time, on comparison
/// semantics the parser cannot re-express as a `Predicate`. These are engine-wide gaps (not
/// constraint-specific) and are left to the enforcement layer rather than rejected here, which
/// would forgo nearly all float and string constraints:
/// - Float semantics: the default engine compares floats via arrow's IEEE-754 *totalOrder*
///   (bitwise), not Spark/ANSI float equality. The two mostly agree -- kernel makes `NaN = NaN`
///   TRUE and orders NaN as the maximum, matching Spark -- but `-0.0 = 0.0` is FALSE under
///   totalOrder (the bits differ) where Spark returns TRUE. So a float/double `=`/`<>`/`<=>` on a
///   column holding `-0.0` can silently disagree, affecting even the DOUBLE and FLOAT-vs-FLOAT
///   cases that lower here. The enforcement layer must normalize signed zero (Spark's
///   `NormalizeFloatingNumbers`) before evaluating, or reject float equality outright.
/// - String collation: kernel compares bytewise; a non-default collation (e.g. `UTF8_LCASE`) would
///   compare differently under Spark.
pub(super) fn lower(comparison: &Comparison, schema: &StructType) -> DeltaResult<Predicate> {
    let Comparison { op, left, right } = comparison;
    let left = resolve_operand(left, schema)?;
    let right = resolve_operand(right, schema)?;
    // Reject non-primitive columns explicitly: `parse_sql("NULL", <struct>)` succeeds (NULL is
    // valid for any type), so without this check `nested = NULL` would lower silently. (`nested =
    // 0` is already caught by `parse_sql`, which rejects a non-primitive literal target.)
    for operand in [&left, &right] {
        if let ResolvedOperand::Column {
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
    // A literal is typed from the column on the other side, so at least one operand must be a
    // column.
    let (left_expr, right_expr) = match (left, right) {
        (
            ResolvedOperand::Column {
                canonical: l,
                data_type: l_type,
            },
            ResolvedOperand::Column {
                canonical: r,
                data_type: r_type,
            },
        ) => {
            // The engine compares only matching Arrow types -- it applies no numeric coercion (it
            // would error on e.g. INT vs LONG). Spark instead coerces both columns to a common type
            // and compares there, so a cross-type comparison kernel cannot reproduce is rejected
            // (left to the connector). The check is on the full `DataType`, so it also rejects
            // mismatched decimal precision/scale and TIMESTAMP vs TIMESTAMP_NTZ.
            if l_type != r_type {
                return Err(Error::generic(format!(
                    "CHECK constraint comparing columns of different types is not supported: \
                     '{}' has type {l_type:?}, '{}' has type {r_type:?}",
                    l.join("."),
                    r.join(".")
                )));
            }
            (Expression::column(l), Expression::column(r))
        }
        (
            ResolvedOperand::Column {
                canonical,
                data_type,
            },
            ResolvedOperand::Literal(raw),
        ) => (
            Expression::column(canonical),
            type_literal_from_column(&raw, &data_type)?,
        ),
        (
            ResolvedOperand::Literal(raw),
            ResolvedOperand::Column {
                canonical,
                data_type,
            },
        ) => (
            type_literal_from_column(&raw, &data_type)?,
            Expression::column(canonical),
        ),
        (ResolvedOperand::Literal(_), ResolvedOperand::Literal(_)) => {
            return Err(Error::generic(
                "CHECK constraint comparison must reference at least one column",
            ))
        }
    };
    Ok(match op {
        CmpOp::Eq => Predicate::eq(left_expr, right_expr),
        CmpOp::Ne => Predicate::ne(left_expr, right_expr),
        CmpOp::Lt => Predicate::lt(left_expr, right_expr),
        CmpOp::Le => Predicate::le(left_expr, right_expr),
        CmpOp::Gt => Predicate::gt(left_expr, right_expr),
        CmpOp::Ge => Predicate::ge(left_expr, right_expr),
        // Null-safe equal: `a <=> b` is "a is not distinct from b" -- kernel has no direct
        // constructor, so negate the distinct predicate.
        CmpOp::NullSafeEq => Predicate::not(Predicate::distinct(left_expr, right_expr)),
    })
}

/// Type a literal from the column it is compared with (via [`parse_sql`]), rejecting the
/// FLOAT-column cases where kernel would silently disagree with Spark.
///
/// Kernel types a literal from the compared column, so against a FLOAT column it narrows the
/// literal to f32 and compares at f32. Spark (ANSI, the DBR / Spark-4 default) instead widens the
/// *column* to f64 for any numeric literal and compares at f64. The two agree only when the literal
/// is exactly representable in f32: an integer qualifies iff it round-trips through f32 unchanged
/// (`v as f32 as i64 == v`), which fails past 2^24 even well within i64 range (e.g. `16777217`
/// rounds to `16777216.0`). Any literal with a `.`/exponent, or an out-of-i64 integer, is likewise
/// not gated in and is rejected -- left to the connector.
///
/// Kernel has no cast expression and cannot compare f32 to f64, so it cannot reproduce Spark's
/// widening. `NULL` is allowed (a null comparison involves no f32/f64 rounding). Only FLOAT is
/// affected; a DOUBLE column already compares at f64, matching Spark. (The signed-zero divergence
/// in [`lower`]'s doc is orthogonal: it is about the column's stored value, and applies to any
/// float comparison regardless of this gate.)
// TODO(#2896): once kernel has a cast expression, lower `cast(col AS DOUBLE) <op> lit` and drop
// this gate, matching Spark's f64 compare for every literal.
fn type_literal_from_column(raw: &str, column_type: &DataType) -> DeltaResult<Expression> {
    let is_null = raw.eq_ignore_ascii_case("null");
    // A FLOAT column narrows the literal to f32, but Spark widens the column to f64, so the two
    // compares agree only when the literal is f32-exact. For an integer that means round-tripping
    // through f32 unchanged (which also rejects out-of-i64 integers, since those fail to parse).
    let is_f32_exact_int = raw.parse::<i64>().is_ok_and(|v| v as f32 as i64 == v);
    if column_type == &DataType::FLOAT && !is_null && !is_f32_exact_int {
        return Err(Error::generic(format!(
            "CHECK constraint comparing a FLOAT column against the literal '{raw}' is not \
             supported: kernel compares at f32 while Spark (ANSI) widens the column to f64"
        )));
    }
    parse_sql(raw, column_type)
}

/// A comparison operand after schema resolution: a column (its canonical schema-cased path and
/// resolved leaf type) or a not-yet-typed literal (typed by [`lower`] from the column on the other
/// side).
enum ResolvedOperand {
    Column {
        canonical: Vec<String>,
        data_type: DataType,
    },
    Literal(String),
}

/// Resolve an operand against `schema`, walking a column path at most once: a column yields its
/// canonical (schema-cased) path and leaf type; a literal is carried as raw text and typed by
/// [`lower`]. Resolution is kept separate from comparison policy (the primitive-only check) because
/// [`lower`] must inspect *both* resolved operands jointly before it can judge them.
fn resolve_operand(operand: &Operand, schema: &StructType) -> DeltaResult<ResolvedOperand> {
    match operand {
        Operand::Literal(raw) => Ok(ResolvedOperand::Literal(raw.clone())),
        Operand::Column(column) => resolve_column(column, schema),
    }
}

/// Resolve a (case-insensitive) column reference against `schema` into a [`ResolvedOperand`],
/// carrying the *canonical* (schema-cased) path and the leaf field's type. See
/// [`StructType::resolve_path_ci`] for why the canonical casing matters.
fn resolve_column(column: &ColumnName, schema: &StructType) -> DeltaResult<ResolvedOperand> {
    let (canonical, leaf) = schema.resolve_path_ci(column)?;
    Ok(ResolvedOperand::Column {
        canonical,
        data_type: leaf.data_type().clone(),
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::parse_sql_simple_predicate;
    use crate::expressions::{DecimalData, Expression, Predicate, Scalar};
    use crate::schema::{DataType, DecimalType, StructField, StructType};

    fn decimal_scalar(unscaled: i128, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(
            DecimalData::try_new(unscaled, DecimalType::try_new(precision, scale).unwrap())
                .unwrap(),
        )
    }

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
            StructField::nullable("cost", DataType::decimal(10, 2).unwrap()),
            StructField::nullable("cost2", DataType::decimal(12, 4).unwrap()),
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
    // Spark's `!>`/`!<` aliases tokenize to `<=`/`>=`; drive them end-to-end so the alias survives
    // parse and lowering, not just tokenization.
    #[case::not_gt_alias("amount !> 0", Predicate::le(col("amount"), Expression::literal(0i64)))]
    #[case::not_lt_alias("amount !< 0", Predicate::ge(col("amount"), Expression::literal(0i64)))]
    // Whitespace between tokens is optional.
    #[case::no_whitespace("amount>0", Predicate::gt(col("amount"), Expression::literal(0i64)))]
    // A literal is typed from the column on the other side: `price` is INTEGER, not the default
    // Long.
    #[case::literal_typed_from_column(
        "price <= 10",
        Predicate::le(col("price"), Expression::literal(10i32))
    )]
    // A DECIMAL literal whose scale matches the column lowers; `parse_scalar` requires an exact
    // scale match, so the literal must be written with the column's scale (`10.00`, not `10`). The
    // scale-mismatch case is rejected below.
    #[case::decimal_matching_scale(
        "cost <= 10.00",
        Predicate::le(col("cost"), Expression::literal(decimal_scalar(1000, 10, 2)))
    )]
    // Literal forms across the primitive type matrix, each typed from its column. Exercises the
    // tokenizer's bareword (`TRUE`), fractional-number, typed-literal (`DATE '...'`), and binary
    // (`X'..'`) paths.
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
    // Leading-dot and exponent numeric forms each tokenize as a single numeric literal.
    #[case::leading_dot_decimal(
        "ratio > .5",
        Predicate::gt(col("ratio"), Expression::literal(Scalar::Double(0.5)))
    )]
    #[case::exponent_number(
        "ratio < 1e3",
        Predicate::lt(col("ratio"), Expression::literal(Scalar::Double(1000.0)))
    )]
    // Signed numbers: the tokenizer emits the sign as a separate operator and the parser folds it
    // back into the literal, so `-5` and `- 5` (whitespace-insensitive, matching Spark) both lower
    // to the same negative literal. A leading `+` is accepted and is a no-op.
    #[case::negative_literal(
        "amount = -5",
        Predicate::eq(col("amount"), Expression::literal(-5i64))
    )]
    #[case::negative_literal_with_space(
        "amount = - 5",
        Predicate::eq(col("amount"), Expression::literal(-5i64))
    )]
    #[case::positive_literal(
        "amount = +5",
        Predicate::eq(col("amount"), Expression::literal(5i64))
    )]
    #[case::signed_exponent(
        "ratio >= -2e+1",
        Predicate::ge(col("ratio"), Expression::literal(Scalar::Double(-20.0)))
    )]
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
    // Same-type column-vs-column comparisons are allowed; cross-type ones are rejected (see the
    // not-kernel-parsable cases below). FLOAT-vs-FLOAT stays at f32, matching Spark.
    #[case::column_vs_column("amount2 < amount", Predicate::lt(col("amount2"), col("amount")))]
    #[case::float_column_vs_column("weight < height", Predicate::lt(col("weight"), col("height")))]
    #[case::literal_on_left("0 < amount", Predicate::lt(Expression::literal(0i64), col("amount")))]
    // An f32-exact integer literal against a FLOAT column lowers: kernel's f32 compare matches
    // Spark's f64 compare because the value survives the narrowing. Non-f32-exact ints diverge and
    // are rejected below.
    #[case::float_column_vs_int_literal(
        "weight > 0",
        Predicate::gt(col("weight"), Expression::literal(Scalar::Float(0.0)))
    )]
    #[case::float_column_int_literal_on_left(
        "5 < weight",
        Predicate::lt(Expression::literal(Scalar::Float(5.0)), col("weight"))
    )]
    // A NULL literal against a FLOAT column is allowed: a null comparison involves no f32/f64
    // rounding, so the FLOAT-widening gate exempts it (the `!is_null` guard in
    // `type_literal_from_column`). Without the exemption this would wrongly error.
    #[case::float_column_vs_null(
        "weight = NULL",
        Predicate::eq(col("weight"), Expression::literal(Scalar::Null(DataType::FLOAT)))
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
    // A comparison must reference at least one column (so a literal can be typed).
    #[case::two_literals("1 > 0")]
    #[case::two_nulls("NULL = NULL")]
    #[case::unknown_column("nope > 0")]
    // Cross-type column-vs-column comparisons are rejected: the engine compares only matching types
    // (no coercion), while Spark coerces to a common type. Same-type pairs lower (see above). The
    // check is on the full `DataType`, so mismatched decimal precision/scale and TIMESTAMP vs
    // TIMESTAMP_NTZ are rejected too.
    #[case::cross_type_int_long_columns("price < amount")]
    #[case::cross_type_float_double_columns("weight < ratio")]
    #[case::cross_type_decimal_scale_columns("cost < cost2")]
    #[case::cross_type_timestamp_ntz_columns("ts < tsn")]
    // Non-primitive (struct/array/map) columns have no scalar comparison and are rejected on either
    // side of the operator -- symmetric with the literal path, which rejects non-primitive targets.
    #[case::struct_column_vs_struct_column("nested = nested")]
    #[case::struct_column_vs_null("nested = NULL")]
    #[case::struct_column_vs_literal("nested = 0")]
    // No implicit casts: a literal whose SQL form does not match the compared column's type is
    // rejected -- including a quoted number for a numeric column, which Spark would coerce.
    #[case::string_literal_for_numeric_column("amount = 'foo'")]
    #[case::quoted_number_for_numeric_column("amount = '10'")]
    #[case::literal_out_of_range_for_column("price = 9999999999")]
    // A DECIMAL literal must match the column's scale exactly (`parse_scalar` does not pad), so an
    // integer or wrong-scale literal against a DECIMAL(10,2) column is rejected where Spark would
    // read `0` as `0.00`. Left to the connector (see the `sql` module doc). `cost <= 10.00` lowers.
    #[case::decimal_integer_literal_scale_mismatch("cost >= 0")]
    #[case::decimal_wrong_scale_literal("cost >= 0.5")]
    // A FLOAT column against any literal not exactly representable in f32 is rejected: kernel
    // compares at f32 while Spark widens the column to f64, disagreeing silently. This covers
    // fractional/exponent literals, out-of-i64 integers, AND in-i64 integers past 2^24 that round
    // when narrowed to f32 (`16777217` -> `16777216.0`) -- the fail-open the old i64-only gate let
    // through. `weight = 0.5` (f32-exact) is still rejected: conservatively, only integers are
    // gated in, since a fractional literal's raw text may not round-trip its intended value.
    #[case::float_column_inexact_decimal_literal("weight = 0.1")]
    #[case::float_column_decimal_literal_on_left("0.1 < weight")]
    #[case::float_column_f32_exact_decimal_literal("weight = 0.5")]
    #[case::float_column_exponent_literal("weight = 1e3")]
    #[case::float_column_integer_beyond_i64("weight > 99999999999999999999")]
    #[case::float_column_non_f32_exact_int("weight = 16777217")]
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
}
