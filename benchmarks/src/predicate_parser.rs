//! Parses SQL WHERE clause expressions into kernel [`Predicate`] types.
//!
//! Supports a subset of SQL sufficient for benchmark predicates:
//! - Binary comparisons: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
//! - Null-safe equals: `<=>`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - Arithmetic operators: `+`, `-`, `*`, `/`, `%`
//! - `IS NULL`, `IS NOT NULL`
//! - `IN (...)`, `NOT IN (...)`
//! - `BETWEEN ... AND ...`
//! - Column references and literal values (integers, floats, strings, booleans)
//! - Typed literals: `DATE'...'`, `TIMESTAMP'...'`, `TIMESTAMP_NTZ'...'`
//!
//! Unsupported (returns error): `LIKE`, function calls (`HEX`, `size`, `length`),
//! `TIME '...'` typed literals.

use delta_kernel::expressions::{
    ArrayData, BinaryExpressionOp, ColumnName, Expression, Predicate, Scalar,
};
use delta_kernel::schema::{ArrayType, DataType, PrimitiveType, Schema};

use sqlparser::ast::{self, Expr, UnaryOperator, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Preprocesses SQL to strip the `TIMESTAMP_NTZ` prefix that sqlparser doesn't recognize.
///
/// Converts `TIMESTAMP_NTZ'...'` to plain string literal `'...'`. The actual type is
/// determined by schema-based type inference (the column's type determines parsing).
///
/// Note: `TIMESTAMP` and `DATE` typed literals are handled natively by sqlparser as
/// `TypedString` expressions, so they don't need preprocessing.
///
/// This uses case-insensitive matching. It may incorrectly match `TIMESTAMP_NTZ` inside
/// string literals (e.g., `str_col = 'TIMESTAMP_NTZ is cool'`), but this is acceptable
/// for benchmark predicates which don't contain such edge cases.
fn preprocess_timestamp_ntz(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.char_indices().peekable();
    let input_bytes = input.as_bytes();
    let keyword = b"TIMESTAMP_NTZ";

    while let Some((i, c)) = chars.next() {
        // Check if we're at a potential TIMESTAMP_NTZ match (case-insensitive)
        if input_bytes.len() >= i + keyword.len()
            && input_bytes[i..i + keyword.len()].eq_ignore_ascii_case(keyword)
        {
            // Look ahead for optional space followed by quote
            let after_keyword = &input[i + keyword.len()..];
            if after_keyword.starts_with('\'') || after_keyword.starts_with(" '") {
                // Skip the TIMESTAMP_NTZ keyword (keep the quote/space+quote)
                for _ in 0..keyword.len() - 1 {
                    chars.next();
                }
                continue;
            }
        }
        result.push(c);
    }
    result
}

/// Parses a SQL WHERE clause expression string into a kernel [`Predicate`] with type checking.
///
/// Performs bidirectional type checking using the provided schema:
/// - Column types are looked up in the schema
/// - Literals are checked against expected types from columns
/// - Type mismatches produce detailed error messages
///
/// # Note
///
/// `TIMESTAMP_NTZ` literals are preprocessed by stripping the prefix, since sqlparser doesn't
/// recognize this keyword. This preprocessing uses simple string matching and may incorrectly
/// match `TIMESTAMP_NTZ` inside string literals (e.g., `str_col = 'TIMESTAMP_NTZ is cool'`).
/// This is acceptable for benchmark predicates which don't contain such edge cases.
///
/// # Example
/// ```ignore
/// let schema = Schema::try_new(vec![
///     StructField::new("id", DataType::LONG, false),
///     StructField::new("name", DataType::STRING, false),
/// ])?;
/// let pred = parse_predicate("id < 500 AND name = 'alice'", &schema).unwrap();
/// ```
pub fn parse_predicate(
    sql: &str,
    schema: &Schema,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    let sql = preprocess_timestamp_ntz(sql);
    let dialect = GenericDialect {};
    let expr = Parser::new(&dialect)
        .try_with_sql(&sql)?
        .parse_expr()
        .map_err(|e| format!("Failed to parse predicate: {e}"))?;
    expr_to_predicate(schema, &expr)
}

////////////////////////////////////////////////////////////////////////
// Typed (bidirectional) conversion functions
////////////////////////////////////////////////////////////////////////

/// Tries to infer an expression, returning both the Expression and its DataType.
///
/// Returns `Some((Expression, DataType))` for:
/// - Column references: look up type in schema, return column expression
/// - Boolean literals: return literal expression with BOOLEAN type
///
/// Returns `None` if the expression needs type context (numeric literals, strings, NULL).
fn infer_expr(schema: &Schema, expr: &Expr) -> Option<(Expression, DataType)> {
    match expr {
        Expr::Identifier(ident) => {
            let ty = schema.field(ident.value.as_str())?.data_type().clone();
            let expr = Expression::column([ident.value.clone()]);
            Some((expr, ty))
        }
        Expr::CompoundIdentifier(parts) => {
            let col_name = ColumnName::new(parts.iter().map(|p| p.value.clone()));
            let ty = schema.resolve_column(&col_name)?.data_type().clone();
            let expr = Expression::column(parts.iter().map(|p| p.value.clone()));
            Some((expr, ty))
        }
        Expr::Value(v) => match &v.value {
            Value::Boolean(b) => Some((Scalar::Boolean(*b).into(), DataType::BOOLEAN)),
            _ => None, // Numeric, string, null need type context
        },
        // Typed literals need type context from the other side of comparison
        // because TIMESTAMP and TIMESTAMP_NTZ use the same syntax after preprocessing
        Expr::TypedString { .. } => None,
        Expr::Nested(inner) => infer_expr(schema, inner),
        Expr::BinaryOp { left, op, right } => {
            let (l, r, ty) = infer_binary_exprs(schema, left, right).ok()?;
            let expr = match op {
                ast::BinaryOperator::Plus => Expression::binary(BinaryExpressionOp::Plus, l, r),
                ast::BinaryOperator::Minus => Expression::binary(BinaryExpressionOp::Minus, l, r),
                ast::BinaryOperator::Multiply => {
                    Expression::binary(BinaryExpressionOp::Multiply, l, r)
                }
                ast::BinaryOperator::Divide => Expression::binary(BinaryExpressionOp::Divide, l, r),
                // Modulo: a % b = a - (b * (a / b))
                // This relies on kernel's Divide producing integer division for integer types,
                // which truncates toward zero (like Rust/C, matching Spark behavior).
                // Examples:
                //   7 % 3  =  7 - (3 * (7 / 3))  =  7 - (3 * 2)  =  1
                //  -7 % 3  = -7 - (3 * (-7 / 3)) = -7 - (3 * -2) = -1
                //   7 % -3 =  7 - (-3 * (7 / -3)) = 7 - (-3 * -2) = 1
                ast::BinaryOperator::Modulo => {
                    let a_div_b =
                        Expression::binary(BinaryExpressionOp::Divide, l.clone(), r.clone());
                    let b_times_quotient =
                        Expression::binary(BinaryExpressionOp::Multiply, r, a_div_b);
                    Expression::binary(BinaryExpressionOp::Minus, l, b_times_quotient)
                }
                _ => return None,
            };
            Some((expr, ty))
        }
        _ => None,
    }
}

/// Checks that an expression has the expected type, returning the typed Expression.
fn check_expr(
    schema: &Schema,
    expected_ty: &DataType,
    expr: &Expr,
) -> Result<Expression, Box<dyn std::error::Error>> {
    match expr {
        Expr::Value(v) => check_literal(expected_ty, &v.value, false),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(v) = inner.as_ref() {
                check_literal(expected_ty, &v.value, true)
            } else {
                Err(format!("Unsupported unary minus on: {expr}").into())
            }
        }
        // Typed literals like DATE'2024-01-01' and TIMESTAMP '2024-01-01 00:00:00'
        // Use expected_ty from column to parse with correct type (Timestamp vs TimestampNtz)
        Expr::TypedString { value, .. } => {
            let s = match value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s,
                _ => return Err(format!("Invalid typed string value: {value}").into()),
            };
            if let DataType::Primitive(prim) = expected_ty {
                let scalar = prim.parse_scalar(s).map_err(|e| e.to_string())?;
                Ok(scalar.into())
            } else {
                Err(format!("Typed literal cannot be used for type: {expected_ty:?}").into())
            }
        }
        Expr::Nested(inner) => check_expr(schema, expected_ty, inner),
        _ => {
            // For non-literals, infer and verify compatibility
            let (e, actual_ty) = infer_expr(schema, expr)
                .ok_or_else(|| format!("Cannot determine type for: {expr}"))?;
            if can_coerce(&actual_ty, expected_ty) {
                Ok(e)
            } else {
                Err(format!(
                    "Type mismatch: expected {:?}, got {:?}",
                    expected_ty, actual_ty
                )
                .into())
            }
        }
    }
}

/// Checks a literal value against an expected type and converts it to that type.
/// Uses kernel's `PrimitiveType::parse_scalar` for parsing.
fn check_literal(
    expected_ty: &DataType,
    value: &Value,
    is_negative: bool,
) -> Result<Expression, Box<dyn std::error::Error>> {
    use PrimitiveType::*;
    match (expected_ty, value) {
        // Numeric literals - only for numeric primitive types
        (
            DataType::Primitive(
                prim @ (Byte | Short | Integer | Long | Float | Double | Decimal(_)),
            ),
            Value::Number(n, _),
        ) => {
            let raw = if is_negative {
                format!("-{}", n)
            } else {
                n.clone()
            };
            let scalar = prim.parse_scalar(&raw).map_err(|e| e.to_string())?;
            Ok(scalar.into())
        }

        // String literals - use parse_scalar (handles String, Date, Timestamp, etc.)
        (
            DataType::Primitive(prim),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
        ) => {
            let scalar = prim.parse_scalar(s).map_err(|e| e.to_string())?;
            Ok(scalar.into())
        }

        // Boolean literals
        (DataType::Primitive(Boolean), Value::Boolean(b)) => Ok(Scalar::Boolean(*b).into()),

        // NULL can be any type
        (ty, Value::Null) => Ok(Scalar::Null(ty.clone()).into()),

        // Type mismatches
        (expected, actual) => Err(format!(
            "Type mismatch: cannot use {:?} for column of type {:?}",
            actual, expected
        )
        .into()),
    }
}

/// Checks if one type can be coerced to another using kernel's type widening rules.
fn can_coerce(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Primitive(f), DataType::Primitive(t)) => f == t || f.can_widen_to(t),
        _ => from == to,
    }
}

/// Synthesizes both operands of a binary operation and checks them against the widest common type.
/// Returns `Ok((left_expr, right_expr, common_type))` on success.
fn infer_binary_exprs(
    schema: &Schema,
    left: &Expr,
    right: &Expr,
) -> Result<(Expression, Expression, DataType), Box<dyn std::error::Error>> {
    let l_infer = infer_expr(schema, left);
    let r_infer = infer_expr(schema, right);

    // Find widest common type
    let common_ty = match (&l_infer, &r_infer) {
        (Some((_, l_ty)), Some((_, r_ty))) => {
            if l_ty == r_ty {
                l_ty.clone()
            } else if can_coerce(l_ty, r_ty) {
                r_ty.clone()
            } else if can_coerce(r_ty, l_ty) {
                l_ty.clone()
            } else {
                return Err(
                    format!("Type mismatch: cannot compare {:?} with {:?}", l_ty, r_ty).into(),
                );
            }
        }
        (Some((_, ty)), None) | (None, Some((_, ty))) => ty.clone(),
        (None, None) => {
            return Err(format!("Cannot determine types for: {:?} and {:?}", left, right).into())
        }
    };

    let l = match l_infer {
        Some((e, _)) => e,
        None => check_expr(schema, &common_ty, left)?,
    };
    let r = match r_infer {
        Some((e, _)) => e,
        None => check_expr(schema, &common_ty, right)?,
    };

    Ok((l, r, common_ty))
}

/// Converts a sqlparser AST expression into a kernel [`Predicate`] with type checking.
fn expr_to_predicate(
    schema: &Schema,
    expr: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    match expr {
        Expr::BinaryOp { left, op, right } => binary_op_to_predicate(schema, left, op, right),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(Predicate::not(expr_to_predicate(schema, expr)?)),
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            let (col_expr, _) = infer_expr(schema, e)
                .ok_or_else(|| format!("Cannot infer type for IS NULL operand: {e}"))?;
            if matches!(expr, Expr::IsNull(_)) {
                Ok(Predicate::is_null(col_expr))
            } else {
                Ok(Predicate::is_not_null(col_expr))
            }
        }
        Expr::Nested(inner) => expr_to_predicate(schema, inner),
        Expr::Value(v) => match &v.value {
            Value::Boolean(b) => Ok(Predicate::literal(*b)),
            _ => Err(format!("Unsupported literal in predicate position: {v}").into()),
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let (col_expr, _) =
                infer_expr(schema, expr).ok_or_else(|| format!("Unknown column: {expr}"))?;
            Ok(Predicate::from_expr(col_expr))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => in_list_to_predicate(schema, expr, list, *negated),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => between_to_predicate(schema, expr, *negated, low, high),
        _ => Err(format!("Unsupported expression: {expr}").into()),
    }
}

/// Converts a binary comparison into a kernel [`Predicate`] with bidirectional type checking.
fn binary_op_to_predicate(
    schema: &Schema,
    left: &Expr,
    op: &ast::BinaryOperator,
    right: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    match op {
        ast::BinaryOperator::Eq
        | ast::BinaryOperator::NotEq
        | ast::BinaryOperator::Lt
        | ast::BinaryOperator::LtEq
        | ast::BinaryOperator::Gt
        | ast::BinaryOperator::GtEq
        | ast::BinaryOperator::Spaceship => {
            use ast::BinaryOperator::*;
            let (l, r, _) = infer_binary_exprs(schema, left, right)?;
            Ok(match op {
                Eq => Predicate::eq(l, r),
                NotEq => Predicate::ne(l, r),
                Lt => Predicate::lt(l, r),
                LtEq => Predicate::le(l, r),
                Gt => Predicate::gt(l, r),
                GtEq => Predicate::ge(l, r),
                Spaceship => Predicate::not(Predicate::distinct(l, r)),
                _ => unreachable!(),
            })
        }
        ast::BinaryOperator::And => {
            let l = expr_to_predicate(schema, left)?;
            let r = expr_to_predicate(schema, right)?;
            Ok(Predicate::and(l, r))
        }
        ast::BinaryOperator::Or => {
            let l = expr_to_predicate(schema, left)?;
            let r = expr_to_predicate(schema, right)?;
            Ok(Predicate::or(l, r))
        }
        _ => Err(format!("Unsupported binary operator: {op}").into()),
    }
}

/// Converts an IN/NOT IN list into a kernel [`Predicate`] with type checking.
fn in_list_to_predicate(
    schema: &Schema,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    let (col_expr, col_ty) =
        infer_expr(schema, expr).ok_or_else(|| format!("IN requires a column, got: {expr}"))?;

    let scalars: Vec<Scalar> = list
        .iter()
        .map(|e| match check_expr(schema, &col_ty, e)? {
            Expression::Literal(s) => Ok(s),
            _ => Err(format!("IN list elements must be literals, got: {e}").into()),
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;

    // Check if any scalar is null to determine array nullability
    let contains_null = scalars.iter().any(|s| s.is_null());
    let array_data = ArrayData::try_new(ArrayType::new(col_ty, contains_null), scalars)?;
    let pred = Predicate::binary(
        delta_kernel::expressions::BinaryPredicateOp::In,
        col_expr,
        Expression::literal(Scalar::Array(array_data)),
    );
    Ok(if negated { Predicate::not(pred) } else { pred })
}

/// Converts BETWEEN into `col >= low AND col <= high` with type checking.
fn between_to_predicate(
    schema: &Schema,
    expr: &Expr,
    negated: bool,
    low: &Expr,
    high: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    let (expr_checked, low_checked, common_ty) = infer_binary_exprs(schema, expr, low)?;
    let high_checked = check_expr(schema, &common_ty, high)?;

    let pred = Predicate::and(
        Predicate::ge(expr_checked.clone(), low_checked),
        Predicate::le(expr_checked, high_checked),
    );
    Ok(if negated { Predicate::not(pred) } else { pred })
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::expressions::{column_name as col, Predicate as Pred, Scalar::*};
    use delta_kernel::schema::{MapType, Schema, StructField, StructType};
    use rstest::rstest;

    fn test_schema() -> Schema {
        Schema::new_unchecked(vec![
            // Primitive types
            StructField::new("byte_col", DataType::BYTE, true),
            StructField::new("short_col", DataType::SHORT, true),
            StructField::new("int_col", DataType::INTEGER, true),
            StructField::new("long_col", DataType::LONG, true),
            StructField::new("float_col", DataType::FLOAT, true),
            StructField::new("double_col", DataType::DOUBLE, true),
            StructField::new("str_col", DataType::STRING, true),
            StructField::new("bool_col", DataType::BOOLEAN, true),
            StructField::new("date_col", DataType::DATE, true),
            StructField::new("ts_col", DataType::TIMESTAMP, true),
            StructField::new("ts_ntz_col", DataType::TIMESTAMP_NTZ, true),
            // Struct type
            StructField::new(
                "struct_col",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("inner_int", DataType::INTEGER, true),
                    StructField::new("inner_str", DataType::STRING, true),
                ]))),
                true,
            ),
            // Array type
            StructField::new(
                "array_col",
                DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                true,
            ),
            // Map type
            StructField::new(
                "map_col",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::LONG,
                    true,
                ))),
                true,
            ),
        ])
    }

    // Comparisons with typed scalars
    #[rstest]
    #[case("long_col < 500", Pred::lt(col!("long_col"), Long(500)))]
    #[case("500 > long_col", Pred::gt(Long(500), col!("long_col")))]
    #[case("long_col = 999", Pred::eq(col!("long_col"), Long(999)))]
    #[case("int_col > 100", Pred::gt(col!("int_col"), Integer(100)))]
    #[case("short_col < 50", Pred::lt(col!("short_col"), Short(50)))]
    #[case("byte_col <= 127", Pred::le(col!("byte_col"), Byte(127)))]
    #[case("double_col >= 3.5", Pred::ge(col!("double_col"), Double(3.5)))]
    #[case("float_col < 1.5", Pred::lt(col!("float_col"), Float(1.5)))]
    #[case("str_col = 'hello'", Pred::eq(col!("str_col"), String("hello".to_string())))]
    #[case("bool_col = true", Pred::eq(col!("bool_col"), Boolean(true)))]
    #[case("bool_col = false", Pred::eq(col!("bool_col"), Boolean(false)))]
    #[case("long_col > -10", Pred::gt(col!("long_col"), Long(-10)))]
    #[case("long_col IS NULL", Pred::is_null(col!("long_col")))]
    #[case("long_col IS NOT NULL", Pred::is_not_null(col!("long_col")))]
    #[case("str_col IS NOT NULL", Pred::is_not_null(col!("str_col")))]
    fn parse_comparison(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Logical operators
    #[rstest]
    #[case(
        "long_col < 500 AND int_col > 10",
        Pred::and(
            Pred::lt(col!("long_col"), Long(500)),
            Pred::gt(col!("int_col"), Integer(10))
        )
    )]
    #[case(
        "long_col = 1 OR long_col = 2",
        Pred::or(
            Pred::eq(col!("long_col"), Long(1)),
            Pred::eq(col!("long_col"), Long(2))
        )
    )]
    #[case(
        "NOT long_col = 1",
        Pred::not(Pred::eq(col!("long_col"), Long(1)))
    )]
    #[case(
        "(long_col < 500) AND (int_col > 10)",
        Pred::and(
            Pred::lt(col!("long_col"), Long(500)),
            Pred::gt(col!("int_col"), Integer(10))
        )
    )]
    fn parse_logical(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // BETWEEN
    #[rstest]
    #[case(
        "long_col BETWEEN 10 AND 20",
        Pred::and(
            Pred::ge(col!("long_col"), Long(10)),
            Pred::le(col!("long_col"), Long(20))
        )
    )]
    #[case(
        "short_col BETWEEN 10 AND 20",
        Pred::and(
            Pred::ge(col!("short_col"), Short(10)),
            Pred::le(col!("short_col"), Short(20))
        )
    )]
    #[case(
        "int_col NOT BETWEEN 0 AND 10",
        Pred::not(Pred::and(
            Pred::ge(col!("int_col"), Integer(0)),
            Pred::le(col!("int_col"), Integer(10))
        ))
    )]
    fn parse_between(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    /// Helper to build an IN list predicate
    fn in_list_pred(
        col: impl Into<Expression>,
        element_type: DataType,
        values: Vec<Scalar>,
        negated: bool,
    ) -> Predicate {
        let array = ArrayData::try_new(ArrayType::new(element_type, false), values).unwrap();
        let pred = Pred::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            col.into(),
            Expression::literal(Array(array)),
        );
        if negated {
            Pred::not(pred)
        } else {
            pred
        }
    }

    // IN list
    #[rstest]
    #[case("long_col IN (1, 2, 3)", in_list_pred(col!("long_col"), DataType::LONG, vec![Long(1), Long(2), Long(3)], false))]
    #[case("long_col NOT IN (1, 2)", in_list_pred(col!("long_col"), DataType::LONG, vec![Long(1), Long(2)], true))]
    #[case("int_col IN (10, 20, 30)", in_list_pred(col!("int_col"), DataType::INTEGER, vec![Integer(10), Integer(20), Integer(30)], false))]
    fn parse_in_list(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Null-safe equals
    #[rstest]
    #[case(
        "long_col <=> 1",
        Pred::not(Pred::distinct(col!("long_col"), Long(1)))
    )]
    #[case(
        "long_col <=> NULL",
        Pred::not(Pred::distinct(col!("long_col"), Null(DataType::LONG)))
    )]
    fn parse_null_safe_equals(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Type errors
    #[rstest]
    #[case("long_col > 'CD'", "Failed to parse")] // String literal for Long column
    #[case("long_col < 'AB'", "Failed to parse")] // String literal for Long column
    #[case("long_col = false", "Type mismatch")] // Boolean vs Long
    #[case("long_col <= false", "Type mismatch")] // Boolean vs Long
    #[case("false = long_col", "Type mismatch")] // Boolean vs Long
    #[case("str_col = 123", "Type mismatch")] // Number literal for String column
    #[case("short_col < 100000", "Failed to parse")] // Overflow: 100000 > i16::MAX
    fn type_error_rejected(#[case] sql: &str, #[case] error_contains: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(result.is_err(), "Expected error for: {}", sql);
        assert!(
            result.unwrap_err().to_string().contains(error_contains),
            "Error should contain '{}'",
            error_contains
        );
    }

    // Unknown column produces an error
    #[test]
    fn unknown_column_produces_error() {
        let schema = test_schema();
        let result = parse_predicate("unknown_col < 500", &schema);
        assert!(result.is_err(), "Unknown columns should produce an error");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot determine type"));
    }

    // Nested struct tests
    #[rstest]
    #[case("struct_col.inner_int > 25", Pred::gt(col!("struct_col.inner_int"), Integer(25)))]
    #[case("struct_col.inner_str = 'alice'", Pred::eq(col!("struct_col.inner_str"), String("alice".to_string())))]
    #[case("struct_col.inner_int IS NULL", Pred::is_null(col!("struct_col.inner_int")))]
    fn parse_nested_struct(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_nested_struct_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("struct_col.inner_int IN (18, 21, 25)", &schema).unwrap();
        let expected = in_list_pred(
            col!("struct_col.inner_int"),
            DataType::INTEGER,
            vec![Integer(18), Integer(21), Integer(25)],
            false,
        );
        assert_eq!(pred, expected);
    }

    #[rstest]
    #[case(
        "struct_col.inner_int BETWEEN 18 AND 65",
        Pred::and(
            Pred::ge(col!("struct_col.inner_int"), Integer(18)),
            Pred::le(col!("struct_col.inner_int"), Integer(65))
        )
    )]
    fn parse_nested_struct_between(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // LIKE is not supported
    #[rstest]
    #[case("str_col LIKE 'b%'")]
    #[case("str_col like '%test%'")]
    fn unsupported_like(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(result.is_err(), "LIKE should not be supported: {}", sql);
    }

    // Function calls are not supported
    #[rstest]
    #[case("str_col = HEX('1111')")]
    #[case("size(array_col) > 2")]
    #[case("length(str_col) < 4")]
    fn unsupported_function_calls(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Function calls should not be supported: {}",
            sql
        );
    }

    // Arithmetic expressions
    #[rstest]
    #[case(
        "long_col + 10 > 100",
        Pred::gt(
            Expression::binary(BinaryExpressionOp::Plus, col!("long_col"), Long(10)),
            Long(100)
        )
    )]
    #[case(
        "long_col * 2 = 10",
        Pred::eq(
            Expression::binary(BinaryExpressionOp::Multiply, col!("long_col"), Long(2)),
            Long(10)
        )
    )]
    #[case(
        "int_col - 5 < 0",
        Pred::lt(
            Expression::binary(BinaryExpressionOp::Minus, col!("int_col"), Integer(5)),
            Integer(0)
        )
    )]
    #[case(
        "double_col / 2.0 >= 1.5",
        Pred::ge(
            Expression::binary(BinaryExpressionOp::Divide, col!("double_col"), Double(2.0)),
            Double(1.5)
        )
    )]
    fn parse_arithmetic(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Modulo is transformed to a - (b * (a / b))
    #[test]
    fn parse_modulo() {
        let schema = test_schema();
        let pred = parse_predicate("long_col % 100 < 10", &schema).unwrap();
        // long_col % 100 = long_col - (100 * (long_col / 100))
        let a = col!("long_col");
        let b: Expression = Long(100).into();
        let a_div_b = Expression::binary(BinaryExpressionOp::Divide, a.clone(), b.clone());
        let b_times_quotient = Expression::binary(BinaryExpressionOp::Multiply, b, a_div_b);
        let modulo = Expression::binary(BinaryExpressionOp::Minus, a, b_times_quotient);
        let expected = Pred::lt(modulo, Long(10));
        assert_eq!(pred, expected);
    }

    // Typed literals (TIME keyword) are not supported
    #[rstest]
    #[case("ts_col >= TIME '00:00:00'")]
    #[case("ts_col > TIME '12:00:00'")]
    fn unsupported_typed_literals(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Typed literals should not be supported: {}",
            sql
        );
    }

    // IS NULL on complex expressions (not a column) is not supported
    #[rstest]
    #[case("(long_col < 0) IS NULL")]
    #[case("(long_col > 0 AND int_col > 1) IS NULL")]
    fn unsupported_is_null_on_expr(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "IS NULL on expressions should not be supported: {}",
            sql
        );
    }

    // Modulo with negative operands (truncates toward zero like Rust/Spark)
    #[test]
    fn parse_modulo_with_negative_operands() {
        let schema = test_schema();

        // -7 % 3 = -7 - (3 * (-7 / 3)) = -7 - (3 * -2) = -7 - (-6) = -1
        let pred = parse_predicate("long_col % 3 = -1", &schema).unwrap();
        let a = col!("long_col");
        let b: Expression = Long(3).into();
        let a_div_b = Expression::binary(BinaryExpressionOp::Divide, a.clone(), b.clone());
        let b_times_quotient = Expression::binary(BinaryExpressionOp::Multiply, b, a_div_b);
        let modulo = Expression::binary(BinaryExpressionOp::Minus, a, b_times_quotient);
        let expected = Pred::eq(modulo, Long(-1));
        assert_eq!(pred, expected);
    }

    // preprocess_timestamp_ntz tests
    #[rstest]
    #[case("TIMESTAMP_NTZ'2024-01-01'", "'2024-01-01'")]
    #[case("timestamp_ntz'2024-01-01'", "'2024-01-01'")] // lowercase
    #[case("Timestamp_Ntz'2024-01-01'", "'2024-01-01'")] // mixed case
    #[case("TIMESTAMP_NTZ '2024-01-01'", " '2024-01-01'")] // space before quote
    #[case("col = TIMESTAMP_NTZ'2024-01-01'", "col = '2024-01-01'")] // in expression
    #[case("TIMESTAMP'2024-01-01'", "TIMESTAMP'2024-01-01'")] // TIMESTAMP preserved
    #[case("col = 'test'", "col = 'test'")] // no change
    fn preprocess_timestamp_ntz_strips_prefix(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(preprocess_timestamp_ntz(input), expected);
    }

    // DATE and TIMESTAMP typed literals are supported via TypedString
    #[test]
    fn parse_date_typed_literal() {
        let schema = test_schema();
        let pred = parse_predicate("date_col = DATE'2024-01-15'", &schema).unwrap();
        // Date is days since epoch: 2024-01-15 = 19737 days
        let expected = Pred::eq(col!("date_col"), Date(19737));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_timestamp_typed_literal() {
        let schema = test_schema();
        let pred = parse_predicate("ts_col = TIMESTAMP'2024-01-15 12:30:00'", &schema).unwrap();
        // Timestamp is microseconds since epoch
        // 2024-01-15 12:30:00 UTC = 1705321800000000 microseconds
        let expected = Pred::eq(col!("ts_col"), Timestamp(1705321800000000));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_timestamp_ntz_typed_literal() {
        let schema = test_schema();
        let pred =
            parse_predicate("ts_ntz_col = TIMESTAMP_NTZ'2024-01-15 12:30:00'", &schema).unwrap();
        // TimestampNtz is microseconds since epoch (same numeric value as Timestamp)
        let expected = Pred::eq(col!("ts_ntz_col"), TimestampNtz(1705321800000000));
        assert_eq!(pred, expected);
    }
}
