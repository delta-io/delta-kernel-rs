//! Lowering: resolve a parsed [`Comparison`] against the table schema into a kernel [`Predicate`].
//!
//! Column references resolve case-insensitively (matching Delta/Spark and kernel's other
//! column-resolution paths). A literal's type is inferred from the column on the other side of the
//! comparison, and the literal itself is parsed by reusing [`super::parse_sql`].

// WIP feature behind `check-constraints-in-dev`; some items have no caller until enforcement lands.
#![allow(dead_code)]

use super::parse_sql;
use super::parser::{CmpOp, Comparison, Operand};
use crate::expressions::{ColumnName, Expression, Predicate};
use crate::schema::{DataType, StructType};
use crate::{DeltaResult, Error};

/// The comparison must reference at least one column so that a literal
/// operand can be typed from the column on the other side.
pub(super) fn lower(comparison: &Comparison, schema: &StructType) -> DeltaResult<Predicate> {
    let Comparison { op, left, right } = comparison;
    let left = resolve_operand(left, schema)?;
    let right = resolve_operand(right, schema)?;

    let left_type = left.column_type().cloned();
    let right_type = right.column_type().cloned();
    if left_type.is_none() && right_type.is_none() {
        return Err(Error::generic(
            "CHECK constraint comparison must reference at least one column",
        ));
    }
    let left_expr = operand_expression(left, right_type.as_ref())?;
    let right_expr = operand_expression(right, left_type.as_ref())?;
    Ok(match op {
        CmpOp::Eq => Predicate::eq(left_expr, right_expr),
        CmpOp::Ne => Predicate::ne(left_expr, right_expr),
        CmpOp::Lt => Predicate::lt(left_expr, right_expr),
        CmpOp::Le => Predicate::le(left_expr, right_expr),
        CmpOp::Gt => Predicate::gt(left_expr, right_expr),
        CmpOp::Ge => Predicate::ge(left_expr, right_expr),
    })
}

/// A comparison operand after schema resolution: a column (its [`Expression`] and resolved leaf
/// type) or a not-yet-typed literal (typed later from the column it is compared against).
enum ResolvedOperand {
    Column {
        expr: Expression,
        data_type: DataType,
    },
    Literal(String),
}

impl ResolvedOperand {
    /// The column's resolved leaf type, or `None` for a literal.
    fn column_type(&self) -> Option<&DataType> {
        match self {
            ResolvedOperand::Column { data_type, .. } => Some(data_type),
            ResolvedOperand::Literal(_) => None,
        }
    }
}

/// Resolve an operand against `schema`, walking a column path at most once. A column resolves to
/// its canonical [`Expression`] and leaf type, which must be primitive: struct/array/map columns
/// have no scalar comparison semantics (and a literal operand is likewise restricted to primitives
/// by [`parse_sql`]), so a non-primitive column makes the constraint not kernel-parsable. A literal
/// is carried as raw text and typed later by [`operand_expression`].
fn resolve_operand(operand: &Operand, schema: &StructType) -> DeltaResult<ResolvedOperand> {
    match operand {
        Operand::Literal(raw) => Ok(ResolvedOperand::Literal(raw.clone())),
        Operand::Column(path) => {
            let (canonical, data_type) = resolve_column(path, schema)?;
            if !matches!(data_type, DataType::Primitive(_)) {
                return Err(Error::generic(format!(
                    "CHECK constraint can only compare primitive-typed columns, but '{}' has type {data_type:?}",
                    canonical.join(".")
                )));
            }
            Ok(ResolvedOperand::Column {
                expr: Expression::column(canonical),
                data_type,
            })
        }
    }
}

/// Build the kernel [`Expression`] for a resolved operand. A literal is parsed via [`parse_sql`]
/// against `sibling_type` -- the type of the column on the other side of the comparison.
fn operand_expression(
    operand: ResolvedOperand,
    sibling_type: Option<&DataType>,
) -> DeltaResult<Expression> {
    match operand {
        ResolvedOperand::Column { expr, .. } => Ok(expr),
        ResolvedOperand::Literal(raw) => {
            let data_type = sibling_type.ok_or_else(|| {
                Error::generic(format!(
                    "cannot type literal '{raw}': a CHECK constraint comparison must reference a column"
                ))
            })?;
            parse_sql(&raw, data_type)
        }
    }
}

/// Resolve a (case-insensitive) column path against `schema`, returning the *canonical* path (the
/// schema's stored field names) and the leaf field's type. The canonical names are what the engine
/// sees in the logical batch, so the emitted column reference must use them rather than the
/// as-written casing.
fn resolve_column(path: &[String], schema: &StructType) -> DeltaResult<(Vec<String>, DataType)> {
    let column = ColumnName::new(path.iter().cloned());
    let mut fields = Vec::with_capacity(path.len());
    schema.visit_fields_of_path_by(
        &column,
        |parent, name| {
            parent
                .fields()
                .find(|f| f.name().eq_ignore_ascii_case(name))
        },
        |field| fields.push(field),
    )?;
    let canonical: Vec<String> = fields.iter().map(|f| f.name().to_string()).collect();
    let leaf = fields
        .last()
        .ok_or_else(|| Error::generic("CHECK constraint references an empty column path"))?;
    Ok((canonical, leaf.data_type().clone()))
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
            StructField::nullable("price", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("active", DataType::BOOLEAN),
            StructField::nullable("ratio", DataType::DOUBLE),
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
    #[case::column_vs_column("price < amount", Predicate::lt(col("price"), col("amount")))]
    #[case::literal_on_left("0 < amount", Predicate::lt(Expression::literal(0i64), col("amount")))]
    // `NULL` is an operand, typed from the column it is compared against.
    #[case::null_operand(
        "name = NULL",
        Predicate::eq(col("name"), Expression::literal(Scalar::Null(DataType::STRING)))
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
    // Malformed input.
    #[case::unterminated_string("name = 'oops")]
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
