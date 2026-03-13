//! Parses SQL WHERE clause expressions into kernel [`Predicate`] types.
//!
//! Supports a subset of SQL sufficient for benchmark predicates:
//! - Binary comparisons: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
//! - Null-safe equals: `<=>`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - `IS NULL`, `IS NOT NULL`
//! - `IN (...)`, `NOT IN (...)`
//! - `BETWEEN ... AND ...`
//! - Column references and literal values (integers, floats, strings, booleans)
//!
//! Unsupported (returns error): `LIKE`, function calls (`HEX`, `size`, `length`),
//! arithmetic expressions (`a % 100`), typed literals (`TIME '...'`).

use delta_kernel::expressions::{
    ArrayData, BinaryPredicateOp, ColumnName, Expression, Predicate, Scalar,
};
use delta_kernel::schema::{ArrayType, DataType};

use sqlparser::ast::{self, Expr, UnaryOperator, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parses a SQL WHERE clause expression string into a kernel [`Predicate`].
///
/// Returns an error if the SQL cannot be parsed or contains unsupported features
/// (e.g. `LIKE`, function calls, arithmetic expressions, typed literals).
///
/// # Example
/// ```ignore
/// let pred = parse_predicate("id < 500 AND value > 10").unwrap();
/// ```
pub fn parse_predicate(sql: &str) -> Result<Predicate, Box<dyn std::error::Error>> {
    let dialect = GenericDialect {};
    let expr = Parser::new(&dialect)
        .try_with_sql(sql)?
        .parse_expr()
        .map_err(|e| format!("Failed to parse predicate: {e}"))?;
    convert_expr_to_predicate(&expr)
}

/// Converts a sqlparser AST expression into a kernel [`Predicate`].
fn convert_expr_to_predicate(expr: &Expr) -> Result<Predicate, Box<dyn std::error::Error>> {
    match expr {
        Expr::BinaryOp { left, op, right } => convert_binary_op(left, op, right),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let inner = convert_expr_to_predicate(expr)?;
            Ok(Predicate::not(inner))
        }
        Expr::IsNull(expr) => {
            let inner = convert_expr_to_expression(expr)?;
            Ok(Predicate::is_null(inner))
        }
        Expr::IsNotNull(expr) => {
            let inner = convert_expr_to_expression(expr)?;
            Ok(Predicate::is_not_null(inner))
        }
        Expr::Nested(inner) => convert_expr_to_predicate(inner),
        Expr::Value(value) => match &value.value {
            Value::Boolean(b) => Ok(Predicate::literal(*b)),
            _ => Err(format!("Unsupported literal in predicate position: {value}").into()),
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let col_expr = convert_expr_to_expression(expr)?;
            Ok(Predicate::from_expr(col_expr))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => convert_in_list(expr, list, *negated),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => convert_between(expr, *negated, low, high),
        _ => Err(format!("Unsupported expression type: {expr}").into()),
    }
}

/// Converts an IN/NOT IN list into a kernel [`Predicate`].
fn convert_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    let col = convert_expr_to_expression(expr)?;

    // Convert all list elements to scalars and infer the element type from the first element
    let scalars: Vec<Scalar> = list
        .iter()
        .map(|e| {
            let expr = convert_expr_to_expression(e)?;
            match expr {
                Expression::Literal(s) => Ok(s),
                _ => Err(format!("IN list elements must be literals, got: {e}").into()),
            }
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;

    let element_type = scalars
        .first()
        .map(|s| s.data_type())
        .unwrap_or(DataType::LONG);

    let array_data = ArrayData::try_new(ArrayType::new(element_type, false), scalars)?;
    let array_expr = Expression::literal(Scalar::Array(array_data));

    let pred = Predicate::binary(BinaryPredicateOp::In, col, array_expr);
    if negated {
        Ok(Predicate::not(pred))
    } else {
        Ok(pred)
    }
}

/// Converts BETWEEN into `col >= low AND col <= high` (or NOT of that if negated).
fn convert_between(
    expr: &Expr,
    negated: bool,
    low: &Expr,
    high: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    let col = convert_expr_to_expression(expr)?;
    let low_expr = convert_expr_to_expression(low)?;
    let high_expr = convert_expr_to_expression(high)?;

    let pred = Predicate::and(
        Predicate::ge(col.clone(), low_expr),
        Predicate::le(col, high_expr),
    );
    if negated {
        Ok(Predicate::not(pred))
    } else {
        Ok(pred)
    }
}

/// Converts a binary operation into a kernel [`Predicate`].
fn convert_binary_op(
    left: &Expr,
    op: &ast::BinaryOperator,
    right: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    match op {
        ast::BinaryOperator::Eq => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::eq(l, r))
        }
        ast::BinaryOperator::NotEq => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::ne(l, r))
        }
        ast::BinaryOperator::Lt => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::lt(l, r))
        }
        ast::BinaryOperator::LtEq => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::le(l, r))
        }
        ast::BinaryOperator::Gt => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::gt(l, r))
        }
        ast::BinaryOperator::GtEq => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::ge(l, r))
        }
        ast::BinaryOperator::And => {
            let l = convert_expr_to_predicate(left)?;
            let r = convert_expr_to_predicate(right)?;
            Ok(Predicate::and(l, r))
        }
        ast::BinaryOperator::Or => {
            let l = convert_expr_to_predicate(left)?;
            let r = convert_expr_to_predicate(right)?;
            Ok(Predicate::or(l, r))
        }
        // <=> is null-safe equals, which is NOT DISTINCT in kernel
        ast::BinaryOperator::Spaceship => {
            let l = convert_expr_to_expression(left)?;
            let r = convert_expr_to_expression(right)?;
            Ok(Predicate::not(Predicate::distinct(l, r)))
        }
        _ => Err(format!("Unsupported binary operator: {op}").into()),
    }
}

/// Converts a sqlparser AST expression into a kernel [`Expression`] (for use as operands).
fn convert_expr_to_expression(expr: &Expr) -> Result<Expression, Box<dyn std::error::Error>> {
    match expr {
        Expr::Identifier(ident) => {
            let name = ColumnName::new([ident.value.clone()]);
            Ok(name.into())
        }
        Expr::CompoundIdentifier(parts) => {
            let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
            Ok(Expression::column(names))
        }
        Expr::Value(value) => convert_value(&value.value),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let Expr::Value(value) = expr.as_ref() {
                convert_negative_value(&value.value)
            } else {
                Err(format!("Unsupported unary minus on: {expr}").into())
            }
        }
        Expr::Nested(inner) => convert_expr_to_expression(inner),
        _ => Err(format!("Unsupported expression: {expr}").into()),
    }
}

/// Converts a sqlparser [`Value`] into a kernel [`Expression`].
fn convert_value(value: &Value) -> Result<Expression, Box<dyn std::error::Error>> {
    match value {
        Value::Number(n, _long) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Scalar::Long(i).into())
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Scalar::Double(f).into())
            } else {
                Err(format!("Cannot parse number: {n}").into())
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(Scalar::from(s.clone()).into())
        }
        Value::Boolean(b) => Ok(Scalar::Boolean(*b).into()),
        // SQL NULL has no inherent type; LONG is an arbitrary default since the kernel
        // requires a DataType for Scalar::Null and predicates don't carry type context.
        Value::Null => Ok(Scalar::Null(DataType::LONG).into()),
        _ => Err(format!("Unsupported value: {value}").into()),
    }
}

/// Converts a negated sqlparser [`Value`] into a kernel [`Expression`] by delegating
/// to [`convert_value`] and negating the resulting numeric scalar.
fn convert_negative_value(value: &Value) -> Result<Expression, Box<dyn std::error::Error>> {
    let expr = convert_value(value)?;
    match expr {
        Expression::Literal(Scalar::Long(n)) => Ok(Scalar::Long(-n).into()),
        Expression::Literal(Scalar::Double(n)) => Ok(Scalar::Double(-n).into()),
        _ => Err(format!("Unsupported negative value: {value}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::expressions::{column_name, BinaryPredicateOp};
    use rstest::rstest;

    // Helper to build an IN predicate: `col IN (scalars...)`
    fn in_list(col: ColumnName, scalars: Vec<Scalar>) -> Predicate {
        let element_type = scalars
            .first()
            .map(|s| s.data_type())
            .unwrap_or(DataType::LONG);
        let array = ArrayData::try_new(ArrayType::new(element_type, false), scalars).unwrap();
        Predicate::binary(
            BinaryPredicateOp::In,
            col,
            Expression::literal(Scalar::Array(array)),
        )
    }

    // -- Comparisons --
    #[rstest]
    #[case("id < 5", Predicate::lt(column_name!("id"), Scalar::Long(5)))]
    #[case("id = 999", Predicate::eq(column_name!("id"), Scalar::Long(999)))]
    #[case("id > 250", Predicate::gt(column_name!("id"), Scalar::Long(250)))]
    #[case("id <= 2", Predicate::le(column_name!("id"), Scalar::Long(2)))]
    #[case("id >= 100", Predicate::ge(column_name!("id"), Scalar::Long(100)))]
    #[case("a <> 1", Predicate::ne(column_name!("a"), Scalar::Long(1)))]
    #[case("a != 1", Predicate::ne(column_name!("a"), Scalar::Long(1)))]
    // Literal on the left
    #[case("1 < a", Predicate::lt(Scalar::Long(1), column_name!("a")))]
    #[case("1 = a", Predicate::eq(Scalar::Long(1), column_name!("a")))]
    #[case("1 != a", Predicate::ne(Scalar::Long(1), column_name!("a")))]
    fn comparison(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- Literal types --
    #[rstest]
    #[case("name = 'bob'", Predicate::eq(column_name!("name"), Scalar::from("bob".to_string())))]
    #[case("c3 < 1.5", Predicate::lt(column_name!("c3"), Scalar::Double(1.5)))]
    #[case("val < -2.5", Predicate::lt(column_name!("val"), Scalar::Double(-2.5)))]
    #[case("c4 > 5.0", Predicate::gt(column_name!("c4"), Scalar::Double(5.0)))]
    #[case("flag = true", Predicate::eq(column_name!("flag"), Scalar::Boolean(true)))]
    #[case("cc9 = false", Predicate::eq(column_name!("cc9"), Scalar::Boolean(false)))]
    #[case("a = NULL", Predicate::eq(column_name!("a"), Scalar::Null(DataType::LONG)))]
    #[case("id > -100", Predicate::gt(column_name!("id"), Scalar::Long(-100)))]
    #[case("value > 1.0E300", Predicate::gt(column_name!("value"), Scalar::Double(1.0E300)))]
    #[case("a > 2147483647", Predicate::gt(column_name!("a"), Scalar::Long(2147483647)))]
    #[case("long_val < 50000000000", Predicate::lt(column_name!("long_val"), Scalar::Long(50000000000)))]
    fn literal_types(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- Compound identifiers (nested columns) --
    #[rstest]
    #[case("a.b > 1", Predicate::gt(Expression::column(["a", "b"]), Scalar::Long(1)))]
    #[case("a.b.c = 2", Predicate::eq(Expression::column(["a", "b", "c"]), Scalar::Long(2)))]
    #[case("b.c.f.i < 0", Predicate::lt(Expression::column(["b", "c", "f", "i"]), Scalar::Long(0)))]
    #[case("data.value > 150", Predicate::gt(Expression::column(["data", "value"]), Scalar::Long(150)))]
    fn nested_columns(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- Boolean literals --
    #[rstest]
    #[case("TRUE", Predicate::literal(true))]
    #[case("FALSE", Predicate::literal(false))]
    fn boolean_literals(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- IS NULL / IS NOT NULL --
    #[rstest]
    #[case("a IS NULL", Predicate::is_null(column_name!("a")))]
    #[case("a IS NOT NULL", Predicate::is_not_null(column_name!("a")))]
    #[case("null_v_struct.v IS NULL", Predicate::is_null(Expression::column(["null_v_struct", "v"])))]
    fn is_null(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- NOT --
    #[rstest]
    #[case("NOT a = 1", Predicate::not(Predicate::eq(column_name!("a"), Scalar::Long(1))))]
    #[case("NOT a = NULL", Predicate::not(Predicate::eq(column_name!("a"), Scalar::Null(DataType::LONG))))]
    #[case(
        "NOT(a < 1 OR b > 20)",
        Predicate::not(Predicate::or(
            Predicate::lt(column_name!("a"), Scalar::Long(1)),
            Predicate::gt(column_name!("b"), Scalar::Long(20)),
        ))
    )]
    fn not_predicate(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- AND / OR --
    #[rstest]
    #[case(
        "id < 500 AND value > 10",
        Predicate::and(
            Predicate::lt(column_name!("id"), Scalar::Long(500)),
            Predicate::gt(column_name!("value"), Scalar::Long(10)),
        )
    )]
    #[case(
        "id = 1 OR id = 2",
        Predicate::or(
            Predicate::eq(column_name!("id"), Scalar::Long(1)),
            Predicate::eq(column_name!("id"), Scalar::Long(2)),
        )
    )]
    #[case(
        "a = 0 AND b = 0 AND c = 0",
        Predicate::and(
            Predicate::and(
                Predicate::eq(column_name!("a"), Scalar::Long(0)),
                Predicate::eq(column_name!("b"), Scalar::Long(0)),
            ),
            Predicate::eq(column_name!("c"), Scalar::Long(0)),
        )
    )]
    #[case(
        "(a < 3 AND b < 3) OR (a > 7 AND b > 7)",
        Predicate::or(
            Predicate::and(
                Predicate::lt(column_name!("a"), Scalar::Long(3)),
                Predicate::lt(column_name!("b"), Scalar::Long(3)),
            ),
            Predicate::and(
                Predicate::gt(column_name!("a"), Scalar::Long(7)),
                Predicate::gt(column_name!("b"), Scalar::Long(7)),
            ),
        )
    )]
    #[case(
        "(a = 5 OR a = 7) AND b < 5",
        Predicate::and(
            Predicate::or(
                Predicate::eq(column_name!("a"), Scalar::Long(5)),
                Predicate::eq(column_name!("a"), Scalar::Long(7)),
            ),
            Predicate::lt(column_name!("b"), Scalar::Long(5)),
        )
    )]
    fn and_or(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- Null-safe equals (<=>)  ->  NOT DISTINCT --
    #[rstest]
    #[case("a <=> 1", Predicate::not(Predicate::distinct(column_name!("a"), Scalar::Long(1))))]
    #[case("a <=> NULL", Predicate::not(Predicate::distinct(column_name!("a"), Scalar::Null(DataType::LONG))))]
    #[case("1 <=> a", Predicate::not(Predicate::distinct(Scalar::Long(1), column_name!("a"))))]
    #[case(
        "NOT a <=> 1",
        Predicate::not(Predicate::not(Predicate::distinct(column_name!("a"), Scalar::Long(1))))
    )]
    fn null_safe_equals(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- IN / NOT IN --
    #[rstest]
    #[case(
        "a in (1, 2, 3)",
        in_list(column_name!("a"), vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)])
    )]
    #[case(
        "a in (1)",
        in_list(column_name!("a"), vec![Scalar::Long(1)])
    )]
    #[case(
        "value in (300, 787, 239)",
        in_list(column_name!("value"), vec![Scalar::Long(300), Scalar::Long(787), Scalar::Long(239)])
    )]
    #[case(
        "name in ('alice', 'bob')",
        in_list(column_name!("name"), vec![Scalar::from("alice".to_string()), Scalar::from("bob".to_string())])
    )]
    fn in_predicate(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    #[rstest]
    #[case(
        "a NOT IN (1, 2)",
        Predicate::not(in_list(column_name!("a"), vec![Scalar::Long(1), Scalar::Long(2)]))
    )]
    #[case(
        "a NOT IN (10, 20, 30)",
        Predicate::not(in_list(column_name!("a"), vec![Scalar::Long(10), Scalar::Long(20), Scalar::Long(30)]))
    )]
    fn not_in_predicate(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- BETWEEN  ->  col >= low AND col <= high --
    #[rstest]
    #[case(
        "id BETWEEN 10 AND 20",
        Predicate::and(
            Predicate::ge(column_name!("id"), Scalar::Long(10)),
            Predicate::le(column_name!("id"), Scalar::Long(20)),
        )
    )]
    #[case(
        "id BETWEEN -10 AND -1",
        Predicate::and(
            Predicate::ge(column_name!("id"), Scalar::Long(-10)),
            Predicate::le(column_name!("id"), Scalar::Long(-1)),
        )
    )]
    #[case(
        "id NOT BETWEEN 10 AND 20",
        Predicate::not(Predicate::and(
            Predicate::ge(column_name!("id"), Scalar::Long(10)),
            Predicate::le(column_name!("id"), Scalar::Long(20)),
        ))
    )]
    fn between(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    // -- Complex predicates (multi-feature) --
    #[rstest]
    #[case(
        "id >= 0 AND id < 1000 AND version_tag = 'v0'",
        Predicate::and(
            Predicate::and(
                Predicate::ge(column_name!("id"), Scalar::Long(0)),
                Predicate::lt(column_name!("id"), Scalar::Long(1000)),
            ),
            Predicate::eq(column_name!("version_tag"), Scalar::from("v0".to_string())),
        )
    )]
    #[case(
        "int_col IS NOT NULL AND str_col IS NOT NULL",
        Predicate::and(
            Predicate::is_not_null(column_name!("int_col")),
            Predicate::is_not_null(column_name!("str_col")),
        )
    )]
    #[case(
        "NOT (a >= 5 AND NOT (b < 5))",
        Predicate::not(Predicate::and(
            Predicate::ge(column_name!("a"), Scalar::Long(5)),
            Predicate::not(Predicate::lt(column_name!("b"), Scalar::Long(5))),
        ))
    )]
    #[case(
        "partCol = 3 and id > 25",
        Predicate::and(
            Predicate::eq(column_name!("partCol"), Scalar::Long(3)),
            Predicate::gt(column_name!("id"), Scalar::Long(25)),
        )
    )]
    fn complex(#[case] sql: &str, #[case] expected: Predicate) {
        assert_eq!(parse_predicate(sql).unwrap(), expected);
    }

    #[rstest]
    // LIKE (no kernel support)
    #[case("a like 'C%'")]
    #[case("fruit like 'b%'")]
    #[case("a > 0 AND b like '2016-%'")]
    // Function calls
    #[case("cc8 = HEX('1111')")]
    #[case("size(items) > 2")]
    #[case("length(s) < 4")]
    // Arithmetic expressions
    #[case("a % 100 < 10 OR b > 20")]
    #[case("a < 10 AND b % 100 > 20")]
    // Typed literals (TIME keyword)
    #[case("time_col >= TIME '00:00:00'")]
    #[case("time_col < TIME '12:00:00'")]
    // IS NULL on non-column expressions
    #[case("(a > 0) IS NULL")]
    #[case("(a > 0 AND b > 1) IS NULL")]
    fn unsupported_predicates_fail_gracefully(#[case] sql: &str) {
        let result = parse_predicate(sql);
        assert!(
            result.is_err(),
            "Expected {sql:?} to fail, but it parsed as: {:?}",
            result.unwrap()
        );
    }
}
