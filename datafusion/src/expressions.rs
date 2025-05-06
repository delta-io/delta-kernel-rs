use std::sync::Arc;

use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{col, lit, utils::conjunction, BinaryExpr, Expr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryOperator, DecimalData, Expression, JunctionExpression,
    JunctionOperator, Scalar, UnaryExpression, UnaryOperator,
};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType};

use crate::error::to_df_err;

pub(crate) fn to_delta_predicate(filters: &[Expr]) -> DFResult<Arc<Expression>> {
    let Some(expr) = conjunction(filters.iter().cloned()) else {
        return Ok(Arc::new(Expression::Literal(Scalar::Boolean(true))));
    };
    to_delta_expression(&expr).map(Arc::new)
}

/// Convert a DataFusion expression to a Delta expression.
fn to_delta_expression(expr: &Expr) -> DFResult<Expression> {
    match expr {
        Expr::Column(column) => Ok(Expression::Column(
            column
                .name
                .parse()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        )),
        Expr::Literal(scalar) => Ok(Expression::Literal(datafusion_scalar_to_scalar(scalar)?)),
        Expr::BinaryExpr(BinaryExpr {
            op: op @ (Operator::And | Operator::Or),
            ..
        }) => {
            let exprs = flatten_junction_expr(expr, *op)?;
            Ok(Expression::Junction(JunctionExpression {
                op: to_junction_op(*op),
                exprs,
            }))
        }
        Expr::BinaryExpr(binary_expr) => Ok(Expression::Binary(BinaryExpression {
            left: Box::new(to_delta_expression(binary_expr.left.as_ref())?),
            op: to_binary_op(binary_expr.op)?,
            right: Box::new(to_delta_expression(binary_expr.right.as_ref())?),
        })),
        Expr::IsNull(expr) => Ok(Expression::Unary(UnaryExpression {
            op: UnaryOperator::IsNull,
            expr: Box::new(to_delta_expression(expr.as_ref())?),
        })),
        Expr::Not(expr) => Ok(Expression::Unary(UnaryExpression {
            op: UnaryOperator::Not,
            expr: Box::new(to_delta_expression(expr.as_ref())?),
        })),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn datafusion_scalar_to_scalar(scalar: &ScalarValue) -> DFResult<Scalar> {
    match scalar {
        ScalarValue::Boolean(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Boolean(*value)),
            None => Ok(Scalar::Null(DataType::BOOLEAN)),
        },
        ScalarValue::Utf8(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::String(value.clone())),
            None => Ok(Scalar::Null(DataType::STRING)),
        },
        ScalarValue::Int8(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Byte(*value)),
            None => Ok(Scalar::Null(DataType::BYTE)),
        },
        ScalarValue::Int16(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Short(*value)),
            None => Ok(Scalar::Null(DataType::SHORT)),
        },
        ScalarValue::Int32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Integer(*value)),
            None => Ok(Scalar::Null(DataType::INTEGER)),
        },
        ScalarValue::Int64(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Long(*value)),
            None => Ok(Scalar::Null(DataType::LONG)),
        },
        ScalarValue::Float32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Float(*value)),
            None => Ok(Scalar::Null(DataType::FLOAT)),
        },
        ScalarValue::Float64(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Double(*value)),
            None => Ok(Scalar::Null(DataType::DOUBLE)),
        },
        ScalarValue::TimestampMicrosecond(maybe_value, Some(_)) => match maybe_value {
            Some(value) => Ok(Scalar::Timestamp(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP)),
        },
        ScalarValue::TimestampMicrosecond(maybe_value, None) => match maybe_value {
            Some(value) => Ok(Scalar::TimestampNtz(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP_NTZ)),
        },
        ScalarValue::Date32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Date(*value)),
            None => Ok(Scalar::Null(DataType::DATE)),
        },
        ScalarValue::Binary(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Binary(value.clone())),
            None => Ok(Scalar::Null(DataType::BINARY)),
        },
        ScalarValue::Decimal128(maybe_value, precision, scale) => match maybe_value {
            Some(value) => Ok(Scalar::Decimal(
                DecimalData::try_new(
                    *value,
                    DecimalType::try_new(*precision, *scale as u8).map_err(to_df_err)?,
                )
                .map_err(to_df_err)?,
            )),
            None => Ok(Scalar::Null(DataType::Primitive(PrimitiveType::Decimal(
                DecimalType::try_new(*precision, *scale as u8).map_err(to_df_err)?,
            )))),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported scalar value: {:?}",
            scalar
        ))),
    }
}

fn to_binary_op(op: Operator) -> DFResult<BinaryOperator> {
    match op {
        Operator::Eq => Ok(BinaryOperator::Equal),
        Operator::NotEq => Ok(BinaryOperator::NotEqual),
        Operator::Lt => Ok(BinaryOperator::LessThan),
        Operator::LtEq => Ok(BinaryOperator::LessThanOrEqual),
        Operator::Gt => Ok(BinaryOperator::GreaterThan),
        Operator::GtEq => Ok(BinaryOperator::GreaterThanOrEqual),
        Operator::Plus => Ok(BinaryOperator::Plus),
        Operator::Minus => Ok(BinaryOperator::Minus),
        Operator::Multiply => Ok(BinaryOperator::Multiply),
        Operator::Divide => Ok(BinaryOperator::Divide),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {:?}",
            op
        ))),
    }
}

/// Helper function to flatten nested AND/OR expressions into a single junction expression
fn flatten_junction_expr(expr: &Expr, target_op: Operator) -> DFResult<Vec<Expression>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { op, left, right }) if *op == target_op => {
            let mut left_exprs = flatten_junction_expr(left.as_ref(), target_op)?;
            let mut right_exprs = flatten_junction_expr(right.as_ref(), target_op)?;
            left_exprs.append(&mut right_exprs);
            Ok(left_exprs)
        }
        _ => {
            let delta_expr = to_delta_expression(expr)?;
            Ok(vec![delta_expr])
        }
    }
}

fn to_junction_op(op: Operator) -> JunctionOperator {
    match op {
        Operator::And => JunctionOperator::And,
        Operator::Or => JunctionOperator::Or,
        _ => unimplemented!("Unsupported operator: {:?}", op),
    }
}

pub(crate) fn to_df_expr(expr: &Expression) -> DFResult<Expr> {
    match expr {
        Expression::Column(name) => Ok(col(name.to_string())),
        Expression::Literal(scalar) => scalar_to_df_scalar(scalar),
        Expression::Binary(BinaryExpression { left, op, right }) => {
            let left_expr = to_df_expr(left)?;
            let right_expr = to_df_expr(right)?;
            Ok(match op {
                BinaryOperator::Equal => left_expr.eq(right_expr),
                BinaryOperator::NotEqual => left_expr.not_eq(right_expr),
                BinaryOperator::LessThan => left_expr.lt(right_expr),
                BinaryOperator::LessThanOrEqual => left_expr.lt_eq(right_expr),
                BinaryOperator::GreaterThan => left_expr.gt(right_expr),
                BinaryOperator::GreaterThanOrEqual => left_expr.gt_eq(right_expr),
                BinaryOperator::Plus => left_expr + right_expr,
                BinaryOperator::Minus => left_expr - right_expr,
                BinaryOperator::Multiply => left_expr * right_expr,
                BinaryOperator::Divide => left_expr / right_expr,
                BinaryOperator::Distinct => Err(DataFusionError::NotImplemented(
                    "DISTINCT operator not supported".into(),
                ))?,
                BinaryOperator::In => Err(DataFusionError::NotImplemented(
                    "IN operator not supported".into(),
                ))?,
                BinaryOperator::NotIn => Err(DataFusionError::NotImplemented(
                    "NOT IN operator not supported".into(),
                ))?,
            })
        }
        Expression::Unary(UnaryExpression { op, expr }) => {
            let inner_expr = to_df_expr(expr)?;
            Ok(match op {
                UnaryOperator::IsNull => inner_expr.is_null(),
                UnaryOperator::Not => !inner_expr,
            })
        }
        Expression::Junction(JunctionExpression { op, exprs }) => {
            let df_exprs: DFResult<Vec<_>> = exprs.iter().map(to_df_expr).collect();
            let df_exprs = df_exprs?;

            match op {
                JunctionOperator::And => Ok(df_exprs
                    .into_iter()
                    .reduce(|a, b| a.and(b))
                    .unwrap_or(lit(true))),
                JunctionOperator::Or => Ok(df_exprs
                    .into_iter()
                    .reduce(|a, b| a.or(b))
                    .unwrap_or(lit(false))),
            }
        }
        Expression::Struct(fields) => {
            let df_exprs: DFResult<Vec<_>> = fields.iter().map(to_df_expr).collect();
            let df_exprs = df_exprs?;
            Err(DataFusionError::NotImplemented(format!(
                "Struct expressions not supported: {:?}",
                df_exprs
            )))
        }
    }
}

fn scalar_to_df_scalar(scalar: &Scalar) -> DFResult<Expr> {
    Ok(lit(match scalar {
        Scalar::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Scalar::String(value) => ScalarValue::Utf8(Some(value.clone())),
        Scalar::Byte(value) => ScalarValue::Int8(Some(*value)),
        Scalar::Short(value) => ScalarValue::Int16(Some(*value)),
        Scalar::Integer(value) => ScalarValue::Int32(Some(*value)),
        Scalar::Long(value) => ScalarValue::Int64(Some(*value)),
        Scalar::Float(value) => ScalarValue::Float32(Some(*value)),
        Scalar::Double(value) => ScalarValue::Float64(Some(*value)),
        Scalar::Timestamp(value) => {
            ScalarValue::TimestampMicrosecond(Some(*value), Some("UTC".into()))
        }
        Scalar::TimestampNtz(value) => ScalarValue::TimestampMicrosecond(Some(*value), None),
        Scalar::Date(value) => ScalarValue::Date32(Some(*value)),
        Scalar::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        Scalar::Decimal(data) => {
            let value = data.bits();
            let precision = data.precision();
            let scale = data.scale();
            ScalarValue::Decimal128(Some(value), precision, scale as i8)
        }
        Scalar::Null(data_type) => match data_type {
            &DataType::BOOLEAN => ScalarValue::Boolean(None),
            &DataType::STRING => ScalarValue::Utf8(None),
            &DataType::BYTE => ScalarValue::Int8(None),
            &DataType::SHORT => ScalarValue::Int16(None),
            &DataType::INTEGER => ScalarValue::Int32(None),
            &DataType::LONG => ScalarValue::Int64(None),
            &DataType::FLOAT => ScalarValue::Float32(None),
            &DataType::DOUBLE => ScalarValue::Float64(None),
            &DataType::TIMESTAMP => ScalarValue::TimestampMicrosecond(None, Some("UTC".into())),
            &DataType::TIMESTAMP_NTZ => ScalarValue::TimestampMicrosecond(None, None),
            &DataType::DATE => ScalarValue::Date32(None),
            &DataType::BINARY => ScalarValue::Binary(None),
            DataType::Primitive(PrimitiveType::Decimal(decimal_type)) => {
                let precision = decimal_type.precision();
                let scale = decimal_type.scale();
                ScalarValue::Decimal128(None, precision, scale as i8)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Null value with type {:?} not supported",
                    data_type
                )))
            }
        },
        Scalar::Struct(_) => {
            return Err(DataFusionError::NotImplemented(
                "Struct scalar values not supported".into(),
            ))
        }
        Scalar::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Array scalar values not supported".into(),
            ))
        }
        Scalar::Map(_) => {
            return Err(DataFusionError::NotImplemented(
                "Map scalar values not supported".into(),
            ))
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};
    use delta_kernel::expressions::{
        BinaryExpression, BinaryOperator, JunctionExpression, JunctionOperator, UnaryExpression,
        UnaryOperator,
    };

    fn assert_junction_expr(expr: &Expr, expected_op: JunctionOperator, expected_children: usize) {
        let delta_expr = to_delta_expression(expr).unwrap();
        match delta_expr {
            Expression::Junction(junction) => {
                assert_eq!(junction.op, expected_op);
                assert_eq!(junction.exprs.len(), expected_children);
            }
            _ => panic!("Expected Junction expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_simple_and() {
        let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        assert_junction_expr(&expr, JunctionOperator::And, 2);
    }

    #[test]
    fn test_simple_or() {
        let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));
        assert_junction_expr(&expr, JunctionOperator::Or, 2);
    }

    #[test]
    fn test_nested_and() {
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionOperator::And, 4);
    }

    #[test]
    fn test_nested_or() {
        let expr = col("a")
            .eq(lit(1))
            .or(col("b").eq(lit(2)))
            .or(col("c").eq(lit(3)))
            .or(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionOperator::Or, 4);
    }

    #[test]
    fn test_mixed_nested_and_or() {
        // (a AND b) OR (c AND d)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c").eq(lit(3)).and(col("d").eq(lit(4)));
        let expr = left.or(right);

        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Junction(junction) => {
                assert_eq!(junction.op, JunctionOperator::Or);
                assert_eq!(junction.exprs.len(), 2);

                // Check that both children are AND junctions
                for child in junction.exprs {
                    match child {
                        Expression::Junction(child_junction) => {
                            assert_eq!(child_junction.op, JunctionOperator::And);
                            assert_eq!(child_junction.exprs.len(), 2);
                        }
                        _ => panic!("Expected Junction expression in child"),
                    }
                }
            }
            _ => panic!("Expected Junction expression"),
        }
    }

    #[test]
    fn test_deeply_nested_and() {
        // (((a AND b) AND c) AND d)
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionOperator::And, 4);
    }

    #[test]
    fn test_complex_expression() {
        // (a AND b) OR ((c AND d) AND e)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c")
            .eq(lit(3))
            .and(col("d").eq(lit(4)))
            .and(col("e").eq(lit(5)));
        let expr = left.or(right);

        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Junction(junction) => {
                assert_eq!(junction.op, JunctionOperator::Or);
                assert_eq!(junction.exprs.len(), 2);

                // First child should be an AND with 2 expressions
                match &junction.exprs[0] {
                    Expression::Junction(child_junction) => {
                        assert_eq!(child_junction.op, JunctionOperator::And);
                        assert_eq!(child_junction.exprs.len(), 2);
                    }
                    _ => panic!("Expected Junction expression in first child"),
                }

                // Second child should be an AND with 3 expressions
                match &junction.exprs[1] {
                    Expression::Junction(child_junction) => {
                        assert_eq!(child_junction.op, JunctionOperator::And);
                        assert_eq!(child_junction.exprs.len(), 3);
                    }
                    _ => panic!("Expected Junction expression in second child"),
                }
            }
            _ => panic!("Expected Junction expression"),
        }
    }

    #[test]
    fn test_roundtrip_simple_and() {
        let df_expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_nested_and() {
        let df_expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_mixed_and_or() {
        let df_expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .or(col("c").eq(lit(3)).and(col("d").eq(lit(4))));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_unary() {
        let df_expr = !col("a").eq(lit(1));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_is_null() {
        let df_expr = col("a").is_null();
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_binary_ops() {
        let df_expr = col("a") + col("b") * col("c");
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_roundtrip_comparison_ops() {
        let df_expr = col("a").gt(col("b")).and(col("c").lt_eq(col("d")));
        let delta_expr = to_delta_expression(&df_expr).unwrap();
        let df_expr_roundtrip = to_df_expr(&delta_expr).unwrap();
        assert_eq!(df_expr, df_expr_roundtrip);
    }

    #[test]
    fn test_unsupported_operators() {
        // Test that unsupported operators return appropriate errors
        let delta_expr = Expression::Binary(BinaryExpression {
            op: BinaryOperator::Distinct,
            left: Box::new(Expression::Column("a".parse().unwrap())),
            right: Box::new(Expression::Column("b".parse().unwrap())),
        });
        assert!(to_df_expr(&delta_expr).is_err());

        let delta_expr = Expression::Binary(BinaryExpression {
            op: BinaryOperator::In,
            left: Box::new(Expression::Column("a".parse().unwrap())),
            right: Box::new(Expression::Column("b".parse().unwrap())),
        });
        assert!(to_df_expr(&delta_expr).is_err());

        let delta_expr = Expression::Binary(BinaryExpression {
            op: BinaryOperator::NotIn,
            left: Box::new(Expression::Column("a".parse().unwrap())),
            right: Box::new(Expression::Column("b".parse().unwrap())),
        });
        assert!(to_df_expr(&delta_expr).is_err());
    }

    #[test]
    fn test_unsupported_struct() {
        let delta_expr = Expression::Struct(vec![
            Expression::Column("a".parse().unwrap()),
            Expression::Column("b".parse().unwrap()),
        ]);
        assert!(to_df_expr(&delta_expr).is_err());
    }
}
