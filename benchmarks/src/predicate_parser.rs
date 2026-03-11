//! Parses SQL WHERE clause expressions into kernel [`Predicate`] types.
//!
//! Supports a subset of SQL sufficient for benchmark predicates:
//! - Binary comparisons: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - `IS NULL`, `IS NOT NULL`
//! - Column references and literal values (integers, floats, strings, booleans)

use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};

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
        _ => Err(format!("Unsupported expression type: {expr}").into()),
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
        Value::Null => Ok(Scalar::Null(delta_kernel::schema::DataType::LONG).into()),
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
}
