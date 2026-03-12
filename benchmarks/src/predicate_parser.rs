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

use delta_kernel::expressions::{ArrayData, ColumnName, Expression, Predicate, Scalar};
use delta_kernel::schema::{ArrayType, DataType};

use sqlparser::ast::{self, Expr, UnaryOperator, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parses a SQL WHERE clause expression string into a kernel [`Predicate`].
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

    let pred = Predicate::binary(
        delta_kernel::expressions::BinaryPredicateOp::In,
        col,
        array_expr,
    );
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
        Value::Null => Ok(Scalar::Null(DataType::LONG).into()),
        _ => Err(format!("Unsupported value: {value}").into()),
    }
}

/// Converts a negated sqlparser [`Value`] into a kernel [`Expression`].
fn convert_negative_value(value: &Value) -> Result<Expression, Box<dyn std::error::Error>> {
    match value {
        Value::Number(n, _long) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Scalar::Long(-i).into())
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Scalar::Double(-f).into())
            } else {
                Err(format!("Cannot parse negative number: {n}").into())
            }
        }
        _ => Err(format!("Unsupported negative value: {value}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::expressions::column_name;

    #[test]
    fn parse_simple_comparison() {
        let pred = parse_predicate("id < 500").unwrap();
        let expected = Predicate::lt(column_name!("id"), Scalar::Long(500));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_and_predicate() {
        let pred = parse_predicate("id < 500 AND value > 10").unwrap();
        let expected = Predicate::and(
            Predicate::lt(column_name!("id"), Scalar::Long(500)),
            Predicate::gt(column_name!("value"), Scalar::Long(10)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_or_predicate() {
        let pred = parse_predicate("id = 1 OR id = 2").unwrap();
        let expected = Predicate::or(
            Predicate::eq(column_name!("id"), Scalar::Long(1)),
            Predicate::eq(column_name!("id"), Scalar::Long(2)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_string_comparison() {
        let pred = parse_predicate("version_tag = 'v0'").unwrap();
        let expected = Predicate::eq(column_name!("version_tag"), Scalar::from("v0".to_string()));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_not_predicate() {
        let pred = parse_predicate("NOT id = 500").unwrap();
        let expected = Predicate::not(Predicate::eq(column_name!("id"), Scalar::Long(500)));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_is_null() {
        let pred = parse_predicate("id IS NULL").unwrap();
        let expected = Predicate::is_null(column_name!("id"));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_is_not_null() {
        let pred = parse_predicate("id IS NOT NULL").unwrap();
        let expected = Predicate::is_not_null(column_name!("id"));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_nested_expression() {
        let pred = parse_predicate("(id < 500) AND (value > 10)").unwrap();
        let expected = Predicate::and(
            Predicate::lt(column_name!("id"), Scalar::Long(500)),
            Predicate::gt(column_name!("value"), Scalar::Long(10)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_negative_number() {
        let pred = parse_predicate("id > -100").unwrap();
        let expected = Predicate::gt(column_name!("id"), Scalar::Long(-100));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_complex_predicate() {
        let pred = parse_predicate("id >= 0 AND id < 1000 AND version_tag = 'v0'").unwrap();
        let expected = Predicate::and(
            Predicate::and(
                Predicate::ge(column_name!("id"), Scalar::Long(0)),
                Predicate::lt(column_name!("id"), Scalar::Long(1000)),
            ),
            Predicate::eq(column_name!("version_tag"), Scalar::from("v0".to_string())),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_in_list() {
        let pred = parse_predicate("a in (1, 2, 3)").unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)],
        )
        .unwrap();
        let expected = Predicate::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            column_name!("a"),
            Expression::literal(Scalar::Array(array)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_not_in_list() {
        let pred = parse_predicate("a NOT IN (1, 2)").unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2)],
        )
        .unwrap();
        let expected = Predicate::not(Predicate::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            column_name!("a"),
            Expression::literal(Scalar::Array(array)),
        ));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_between() {
        let pred = parse_predicate("id BETWEEN 10 AND 20").unwrap();
        let expected = Predicate::and(
            Predicate::ge(column_name!("id"), Scalar::Long(10)),
            Predicate::le(column_name!("id"), Scalar::Long(20)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_null_safe_equals() {
        let pred = parse_predicate("a <=> 1").unwrap();
        let expected = Predicate::not(Predicate::distinct(column_name!("a"), Scalar::Long(1)));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_null_safe_equals_null() {
        let pred = parse_predicate("a <=> NULL").unwrap();
        let expected = Predicate::not(Predicate::distinct(
            column_name!("a"),
            Scalar::Null(DataType::LONG),
        ));
        assert_eq!(pred, expected);
    }

    /// Bulk test: every predicate that the parser should handle successfully.
    /// We don't check the exact output -- just that parsing doesn't error.
    #[test]
    fn parse_all_supported_predicates() {
        let supported = [
            // Simple comparisons
            "id < 5",
            "full_name = 'John Doe'",
            "squared > 10",
            "id = 999",
            "a <=> 1",
            "a <> 1",
            "NOT a <=> NULL",
            "NOT a <=> 1",
            "FALSE",
            "a > 1",
            "a = NULL",
            "NOT a = 1",
            "a IS NULL",
            "a IS NOT NULL",
            "a = 1",
            "NOT a = NULL",
            "TRUE",
            "a < 1",
            // NOT IN
            "a NOT IN (1, 2)",
            "a NOT IN (10, 20, 30)",
            "a NOT IN (3)",
            "a NOT IN (1, 2, 3, 4, 5)",
            // Simple column comparisons
            "id = 1",
            "c7 = '2003-02-02'",
            "c1 = 1",
            "c3 < 1.5",
            "c10 > 1.5",
            "c6 >= '2001-01-01 01:00:00'",
            "c4 > 5.0",
            "c6 >= '2003-01-01 01:00:00'",
            "c4 > 1.0",
            "c10 > 2.5",
            "c3 < 0.5",
            "c5 >= '2003-01-01 01:00:00'",
            "c5 >= '2001-01-01 01:00:00'",
            "c1 = 10",
            "c7 = '2002-02-02'",
            "category = '0'",
            "date = '2024-01-01'",
            // AND predicates
            "a = 2 AND b >= '2017-08-30'",
            "a > 0 AND b = '2017-09-01'",
            "name IS NOT NULL",
            "id > 250",
            "distance = 5.0",
            "id < 100",
            "id >= 100",
            "doubled > 40",
            // Compound identifiers (nested columns)
            "a.b.c > 1",
            "a.b.c < 1",
            "a.b.c <= 2",
            "a.b.c = 1",
            "a.b.c <= 1",
            "a.b.c >= 0",
            "a.b.c = 2",
            "a.b.c >= 1",
            "id < 50",
            "part = 1",
            "part = 0",
            "a.b >= 0",
            "a.b = 2",
            "a.b >= 1",
            "a.b > 1",
            "a.b <= 1",
            "a.b <= 2",
            "a.b < 1",
            "a.b = 1",
            "id > 5",
            "status = 'inactive'",
            "count > 30",
            "status = 'active'",
            "a < 0",
            "b < 0",
            "c < 0",
            "d < 0",
            "m < 0",
            "b.l = 9",
            "b.c.f.i < 0",
            "b.l < 0",
            "b.c.d = 2",
            "b.c.f.i = 6",
            "value >= 100",
            "fruit = 'apple'",
            "fruit < 'cherry'",
            "fruit = 'fig'",
            "fruit > 'cherry'",
            "fruit = 'banana'",
            "part = 0",
            "amount > 1000",
            "a > 127",
            "a > 32767",
            "id > 2",
            "name = 'bob'",
            "key > 5",
            "id < 10",
            "id >= 20",
            "value = 1",
            "id >= 50 AND id < 80",
            "id >= 1000",
            "b.c.d < 0",
            "b.c.e < 0",
            "b.c.f.g < 0",
            "b.c.d = 2",
            "b.c.e = 3",
            "b.l < 0",
            "a < 0",
            "b.c.f.i < 0",
            "a = 1",
            "cc2 = '4'",
            "cc7 = '2002-02-02'",
            "cc10 > 1.5",
            "cc5 >= '2003-01-01 01:00:00'",
            "cc2 = '2'",
            "cc6 >= '2001-01-01 01:00:00'",
            "cc1 = 1",
            "cc3 < 1.5",
            "cc6 >= '2003-01-01 01:00:00'",
            "cc3 < 0.5",
            "cc5 >= '2001-01-01 01:00:00'",
            "cc1 = 10",
            "cc7 = '2003-02-02'",
            "cc4 > 1.0",
            "cc10 > 2.5",
            "cc9 = false",
            "cc9 = true",
            "cc4 > 5.0",
            "part = 1",
            "flag = true",
            "a <= 1 AND a > -1",
            "a < 0 AND a > -2",
            "a > 0 AND a < 3",
            "id > 3",
            "int_col IS NULL",
            "int_col IS NOT NULL AND str_col IS NOT NULL",
            "int_col IS NOT NULL",
            "str_col IS NULL",
            "id = 2",
            "date_col >= '2024-01-10' AND date_col < '2024-01-20'",
            "date_col = '2024-01-15'",
            "id >= 100",
            "id < 100",
            "col3 = 200",
            "col1 = 2",
            "col2 = 20",
            "value > 1.0E300",
            "id = 1",
            "NOT(a < 1 OR b > 20)",
            "NOT(a >= 1 OR b <= 20)",
            "category = 'A'",
            "code = 'ABCDE'",
            "id < 5",
            "a >= 'D'",
            "a > 'CD'",
            "a < 'AB'",
            "a > 'BA'",
            "a > 'CC'",
            "amount > 250.0",
            "amount <= 200.0",
            "id = 1",
            "data.count > 2147483647",
            "id < 10",
            // OR predicates
            "a < 0 or b = '2017-09-01'",
            "a = 2 or b < '2017-08-30'",
            "a < 2",
            "domain = 'example.com'",
            "id > 1",
            "id < 5",
            "category = 'B'",
            "category = 'C'",
            "category = 'A'",
            // NOT with AND/OR
            "NOT(a >= 10 AND b <= 20)",
            "NOT(a < 10 AND b > 20)",
            "NOT(a >= 10 AND b >= 10)",
            "id = 1",
            "info.age > 27",
            "id < 0",
            "value > 1.5",
            "start_date >= '2024-02-01'",
            "b.c.e < 0",
            "m < 0",
            "b.c.k < 0",
            "b.c.f.h < 0",
            "b.c.j < 0",
            "b.l < 0",
            "b.c.d < 0",
            "b.c.f.g < 0",
            "b.c.f.i < 0",
            "a < 0",
            "price > 100",
            "id = 1",
            "value > 150",
            "a > 100",
            "a <= 10",
            "a > 10 AND a <= 20",
            "a <= 15",
            "a > 20",
            "a > 15",
            "a < 0",
            "id <= 2",
            "double_col > 1400",
            "int_col = 50",
            "long_col >= 900 AND long_col < 950",
            "test_data < 50",
            "event_date = '2024-01-15'",
            "event_hour < 12",
            "a < 0",
            "b.c < 0",
            "b.d < 0",
            "name = 'ALICE'",
            "info.age > 27",
            "id < 5",
            "i < 0",
            "a.d < 0",
            "a.f.g < 0",
            "a.e < 0",
            "a.f.g < 10",
            "a.d > 6",
            // BETWEEN
            "id BETWEEN -10 AND -1",
            "id BETWEEN 100 AND 200",
            "id BETWEEN 20 AND 30",
            "id BETWEEN 0 AND 99",
            "id BETWEEN 50 AND 60",
            "date_part = '2024-01-01'",
            "id = 1",
            "id >= 15",
            "id = 1",
            "id = 2",
            "name IS NOT NULL",
            "b.c.j < 0",
            "b.c.f.i = 6",
            "b.c.k < 0",
            "b.c.f.i < 0",
            "b.l < 0",
            "a < 0",
            "b.l < 0",
            "a < 0",
            "m < 0",
            "b.c.d < 0",
            "b.c.f.i < 0",
            "a = 100",
            "a = 1000",
            "c = 100",
            "x > 3",
            "NOT (a <=> NULL)",
            "NOT (a <=> 1)",
            "a <=> 1",
            "b <=> NULL",
            "a <=> NULL",
            "id < 10",
            "b.c.k < 0",
            "b.c.f.i < 0",
            "b.c.d < 0",
            "b.c.f.g < 0",
            "m < 0",
            "b.l < 0",
            "b.c.f.h < 0",
            "b.c.e < 0",
            "a < 0",
            "b.c.j < 0",
            "value < 500",
            "x < 0",
            "doubled = 4",
            "null_v IS NOT NULL",
            "null_v_struct.v IS NULL",
            "null_v_struct.v IS NOT NULL",
            "v IS NOT NULL",
            "v_struct.v IS NULL",
            "null_v IS NULL",
            "v IS NULL",
            "v_struct.v IS NOT NULL",
            "a = 1",
            "NOT a <=> NULL",
            "a < 1",
            "a <=> NULL",
            "a <> 1",
            "NOT a = NULL",
            "a > 1",
            "a IS NOT NULL",
            "a IS NULL",
            "NOT a = 1",
            "NOT a <=> 1",
            "FALSE",
            "a = NULL",
            "TRUE",
            "a <=> 1",
            "name = 'two'",
            "id = 1",
            "a >= 2 or a < -1",
            "a > 5 or a < -2",
            "a > 0 or a < -3",
            "data.value > 150",
            "data.category = 5",
            "value = 'a'",
            "id > 2",
            "name = 'alice'",
            "c10 > 2.5",
            "c4 > 5.0",
            "c2 = '2'",
            "c4 > 1.0",
            "c6 >= '2003-01-01 01:00:00'",
            "c5 >= '2001-01-01 01:00:00'",
            "c6 >= '2001-01-01 01:00:00'",
            "c10 > 1.5",
            "c9 = false",
            "c1 = 1",
            "c1 = 10",
            "c7 = '2003-02-02'",
            "c5 >= '2003-01-01 01:00:00'",
            "c3 < 1.5",
            "c2 = '4'",
            "c3 < 0.5",
            "c7 = '2002-02-02'",
            "c9 = true",
            "cc9 = false",
            "cc9 = true",
            // Complex AND/OR
            "(a < 3 AND b < 3) OR (a > 7 AND b > 7)",
            "NOT (a >= 5 AND NOT (b < 5))",
            "a = 0 AND b = 0 AND c = 0",
            "(a = 5 OR a = 7) AND b < 5",
            "id <= 3",
            "part IS NULL",
            "part = '1'",
            "name >= 'c' AND name < 'e'",
            "name = 'cherry'",
            "price > 25.00",
            "v IS NOT NULL",
            "part = 0",
            "part = 3",
            "id < 5",
            "id2 < 100",
            "part = 5",
            "part = 0",
            "part >= 7",
            "not a < 0",
            "not a > 0",
            "flag = true",
            "part = 0",
            "part = 1",
            "part = 0 AND id < 10",
            "value < 100",
            "part = 99",
            "a < 10 OR b > 20",
            "part = 1",
            "num > 7",
            "value > 200",
            "id >= 1000 AND id <= 1200",
            "a > 'CD'",
            "a > 'BA'",
            "a < 'AA'",
            "a < 'AB'",
            "a = false",
            "false = a",
            "NOT a",
            "a <= false",
            "NOT a = false",
            "int_val = 25",
            "long_val < 50000000000",
            "double_val < 5.0",
            "int_val > 200",
            "int_val < 50",
            "long_val < 0",
            "value > 1500",
            "amount >= 50.00 AND amount < 75.00",
            "amount = 12.30",
            "category = 'A'",
            "category = 'B'",
            "category = 'C'",
            "dt >= '2024-01-15'",
            "dt < '2024-01-01'",
            "dt = '2024-01-15'",
            "dt < '2024-01-20'",
            "dt > '2024-12-31'",
            "a < 1 OR b < 10",
            "b < 10",
            "a < 1 OR (a > 10 AND b < 10)",
            "a < 1 AND b < 10",
            "a < 1 OR (a >= 1 AND b < 10)",
            "a <= 0",
            "1 != a",
            "2 <= a",
            "a < 1",
            "0 <= a",
            "a <=> 1",
            "1 < a",
            "2 >= a",
            "a >= 0",
            "a <=> 2",
            "1 = a",
            "NOT a <=> 2",
            "a > 1",
            "NOT a = 1",
            "2 <=> a",
            "0 >= a",
            "True",
            "a = 2",
            "a != 1",
            "a = 1",
            "a <= 2",
            "a >= 1",
            "1 >= a",
            "2 = a",
            "1 > a",
            "a >= 2",
            "NOT a <=> 1",
            "a <= 1",
            "1 <= a",
            "1 <=> a",
            "col1 = 2",
            // IN lists
            "a in (4, 5, 6)",
            "a in (1, 2)",
            "a in (1)",
            "a in (1, 2, 3)",
            "id >= 10",
            "col00 = 0",
            "col32 = -1",
            "col00 = 1",
            "col32 = 32",
            "amount > 50",
            "status = 'active'",
            "partCol = 3 and id > 25",
            "id > 25",
            "partCol = 3",
            "wrapper.label = 'first'",
            "s IS NOT NULL",
            "a > 2147483647",
            "value in (300, 787, 239)",
            "_metadata.default_row_commit_version > 1",
            "id >= 5",
            // Time-as-string predicates (parsed as plain string comparisons)
            "start_time < '10:00:00'",
            "event_time >= '12:00:00'",
            // Backtick-quoted identifiers (single-component column name containing dots)
            "`b.c` < 0",
        ];

        let mut failures = Vec::new();
        for sql in &supported {
            if let Err(e) = parse_predicate(sql) {
                failures.push(format!("  FAIL: {sql:?} -> {e}"));
            }
        }
        assert!(
            failures.is_empty(),
            "Expected all predicates to parse successfully, but {} failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    /// Predicates that use SQL features not representable in kernel expressions.
    /// These should fail gracefully with an error (not panic).
    #[test]
    fn unsupported_predicates_fail_gracefully() {
        let unsupported = [
            // LIKE (no kernel support)
            "a like 'C%'",
            "a like 'A%'",
            "a.b like '%'",
            "a.b like 'mic%'",
            "fruit like 'b%'",
            "fruit like 'a%'",
            "fruit like 'z%'",
            "fruit like '%'",
            "name LIKE 'b%'",
            "a > 0 AND b like '2016-%'",
            "a >= 2 AND b like '2017-08-%'",
            "a >= 2 or b like '2016-08-%'",
            "a < 2 or b like '2017-08-%'",
            "a < 0 or b like '2016-%'",
            // Function calls
            "cc8 = HEX('1111')",
            "cc8 = HEX('3333')",
            "c8 = HEX('3333')",
            "c8 = HEX('1111')",
            "size(items) > 2",
            "size(tags) > 2",
            "length(s) < 4",
            // Arithmetic expressions
            "a % 100 < 10 OR b > 20",
            "a % 100 < 10 AND b > 20",
            "a < 10 OR b % 100 > 20",
            "a % 100 < 10 AND b % 100 > 20",
            "a < 10 AND b % 100 > 20",
            // Typed literals (TIME keyword)
            "time_col >= TIME '00:00:00'",
            "time_col >= TIME '10:00:00' AND time_col < TIME '12:00:00'",
            "time_col > TIME '12:00:00'",
            "time_col < TIME '12:00:00'",
            // IS NULL on complex expressions (not a column)
            "(a < 0) IS NULL",
            "(a > 0) IS NULL",
            "(a > 1) IS NULL",
            "NOT ((a > 0) IS NULL)",
            "NOT ((a > 1) IS NULL)",
            "(a > 0 OR b > 1) IS NULL",
            "(a > 0 AND b > 1) IS NULL",
            "(a > 1 AND a > 0) IS NULL",
            "(b > 1 AND a > 0) IS NULL",
            "(b > 1 OR a < 0) IS NULL",
            "(a > 1 OR a < 0) IS NULL",
            "(b > 0) IS NULL",
            "(b < 0) IS NULL",
        ];

        for sql in &unsupported {
            let result = parse_predicate(sql);
            assert!(
                result.is_err(),
                "Expected {sql:?} to fail, but it parsed as: {:?}",
                result.unwrap()
            );
        }
    }
}
