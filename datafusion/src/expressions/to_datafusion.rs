use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{col, lit, Expr};
use delta_kernel::expressions::{
    BinaryExpression, BinaryOperator, Expression, JunctionExpression, JunctionOperator, Scalar,
    UnaryExpression, UnaryOperator,
};
use delta_kernel::schema::{DataType, PrimitiveType};

pub(crate) fn to_datafusion_expr(expr: &Expression) -> DFResult<Vec<Expr>> {
    match expr {
        Expression::Column(name) => Ok(vec![col(name.to_string())]),
        Expression::Literal(scalar) => Ok(vec![scalar_to_df_scalar(scalar)?]),
        Expression::Binary(BinaryExpression { left, op, right }) => {
            let mut left_expr = to_datafusion_expr(left)?;
            let mut right_expr = to_datafusion_expr(right)?;
            if left_expr.len() != 1 || right_expr.len() != 1 {
                return Err(DataFusionError::Execution(
                    "Binary expressions must have exactly one child".into(),
                ));
            }
            let left_expr = left_expr.remove(0);
            let right_expr = right_expr.remove(0);
            Ok(match op {
                BinaryOperator::Equal => vec![left_expr.eq(right_expr)],
                BinaryOperator::NotEqual => vec![left_expr.not_eq(right_expr)],
                BinaryOperator::LessThan => vec![left_expr.lt(right_expr)],
                BinaryOperator::LessThanOrEqual => vec![left_expr.lt_eq(right_expr)],
                BinaryOperator::GreaterThan => vec![left_expr.gt(right_expr)],
                BinaryOperator::GreaterThanOrEqual => vec![left_expr.gt_eq(right_expr)],
                BinaryOperator::Plus => vec![left_expr + right_expr],
                BinaryOperator::Minus => vec![left_expr - right_expr],
                BinaryOperator::Multiply => vec![left_expr * right_expr],
                BinaryOperator::Divide => vec![left_expr / right_expr],
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
            let mut inner_expr = to_datafusion_expr(expr)?;
            if inner_expr.len() != 1 {
                return Err(DataFusionError::Execution(
                    "Unary expressions must have exactly one child".into(),
                ));
            }
            let inner_expr = inner_expr.remove(0);
            Ok(match op {
                UnaryOperator::IsNull => vec![inner_expr.is_null()],
                UnaryOperator::Not => vec![!inner_expr],
            })
        }
        Expression::Junction(JunctionExpression { op, exprs }) => {
            let df_exprs: DFResult<Vec<_>> = exprs.iter().map(to_datafusion_expr).collect();
            let df_exprs: Vec<_> = df_exprs?.into_iter().flatten().collect();

            match op {
                JunctionOperator::And => Ok(vec![df_exprs
                    .into_iter()
                    .reduce(|a, b| a.and(b))
                    .unwrap_or(lit(true))]),
                JunctionOperator::Or => Ok(vec![df_exprs
                    .into_iter()
                    .reduce(|a, b| a.or(b))
                    .unwrap_or(lit(false))]),
            }
        }
        Expression::Struct(fields) => {
            let df_exprs: DFResult<Vec<_>> = fields.iter().map(to_datafusion_expr).collect();
            Ok(df_exprs?.into_iter().flatten().collect())
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
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::expressions::{
        BinaryExpression, BinaryOperator, JunctionOperator, Scalar, StructData, UnaryExpression,
        UnaryOperator,
    };
    use delta_kernel::schema::{DataType, StructField};

    /// Test basic column reference: `test_col`
    #[test]
    fn test_column_expression() {
        let expr = Expression::Column(ColumnName::new(["test_col"]));
        let result = to_datafusion_expr(&expr).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], col("test_col"));
    }

    /// Test various literal values:
    /// - `true` (boolean)
    /// - `"test"` (string)
    /// - `42` (integer)
    /// - `42L` (long)
    /// - `42.0f` (float)
    /// - `42.0` (double)
    /// - `NULL` (null boolean)
    #[test]
    fn test_literal_expressions() {
        // Test various scalar types
        let test_cases = vec![
            (Expression::Literal(Scalar::Boolean(true)), lit(true)),
            (
                Expression::Literal(Scalar::String("test".to_string())),
                lit("test"),
            ),
            (Expression::Literal(Scalar::Integer(42)), lit(42)),
            (Expression::Literal(Scalar::Long(42)), lit(42i64)),
            (Expression::Literal(Scalar::Float(42.0)), lit(42.0f32)),
            (Expression::Literal(Scalar::Double(42.0)), lit(42.0)),
            (
                Expression::Literal(Scalar::Null(DataType::BOOLEAN)),
                lit(ScalarValue::Boolean(None)),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_expr(&input).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], expected);
        }
    }

    /// Test binary operations:
    /// - `a = 1` (equality)
    /// - `a + b` (addition)
    /// - `a * 2` (multiplication)
    #[test]
    fn test_binary_expressions() {
        let test_cases = vec![
            (
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(Scalar::Integer(1))),
                }),
                col("a").eq(lit(1)),
            ),
            (
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                }),
                col("a") + col("b"),
            ),
            (
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Literal(Scalar::Integer(2))),
                }),
                col("a") * lit(2),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_expr(&input).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], expected);
        }
    }

    /// Test unary operations:
    /// - `a IS NULL` (null check)
    /// - `NOT a` (logical negation)
    #[test]
    fn test_unary_expressions() {
        let test_cases = vec![
            (
                Expression::Unary(UnaryExpression {
                    op: UnaryOperator::IsNull,
                    expr: Box::new(Expression::Column(ColumnName::new(["a"]))),
                }),
                col("a").is_null(),
            ),
            (
                Expression::Unary(UnaryExpression {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expression::Column(ColumnName::new(["a"]))),
                }),
                !col("a"),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_expr(&input).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], expected);
        }
    }

    /// Test junction operations:
    /// - `a AND b` (logical AND)
    /// - `a OR b` (logical OR)
    #[test]
    fn test_junction_expressions() {
        let test_cases = vec![
            (
                Expression::Junction(JunctionExpression {
                    op: JunctionOperator::And,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["a"])),
                        Expression::Column(ColumnName::new(["b"])),
                    ],
                }),
                col("a").and(col("b")),
            ),
            (
                Expression::Junction(JunctionExpression {
                    op: JunctionOperator::Or,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["a"])),
                        Expression::Column(ColumnName::new(["b"])),
                    ],
                }),
                col("a").or(col("b")),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_expr(&input).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], expected);
        }
    }

    /// Test complex nested expression:
    /// `(a > 1 AND b < 2) OR (c = 3)`
    #[test]
    fn test_complex_nested_expressions() {
        // Test a complex expression: (a > 1 AND b < 2) OR (c = 3)
        let expr = Expression::Junction(JunctionExpression {
            op: JunctionOperator::Or,
            exprs: vec![
                Expression::Junction(JunctionExpression {
                    op: JunctionOperator::And,
                    exprs: vec![
                        Expression::Binary(BinaryExpression {
                            left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(Expression::Literal(Scalar::Integer(1))),
                        }),
                        Expression::Binary(BinaryExpression {
                            left: Box::new(Expression::Column(ColumnName::new(["b"]))),
                            op: BinaryOperator::LessThan,
                            right: Box::new(Expression::Literal(Scalar::Integer(2))),
                        }),
                    ],
                }),
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["c"]))),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(Scalar::Integer(3))),
                }),
            ],
        });

        let result = to_datafusion_expr(&expr).unwrap();
        assert_eq!(result.len(), 1);
        let expected = (col("a").gt(lit(1)).and(col("b").lt(lit(2)))).or(col("c").eq(lit(3)));
        assert_eq!(result[0], expected);
    }

    /// Test error cases:
    /// - `a DISTINCT b` (unsupported operator)
    /// - `STRUCT()` (unsupported scalar type)
    #[test]
    fn test_error_cases() {
        // Test unsupported binary operators
        let expr = Expression::Binary(BinaryExpression {
            left: Box::new(Expression::Column(ColumnName::new(["a"]))),
            op: BinaryOperator::Distinct,
            right: Box::new(Expression::Column(ColumnName::new(["b"]))),
        });
        assert!(to_datafusion_expr(&expr).is_err());

        // Test unsupported scalar types
        let expr = Expression::Literal(Scalar::Struct(
            StructData::try_new(
                vec![StructField::nullable("field", DataType::INTEGER)],
                vec![Scalar::Integer(42)],
            )
            .unwrap(),
        ));
        assert!(to_datafusion_expr(&expr).is_err());
    }
}
