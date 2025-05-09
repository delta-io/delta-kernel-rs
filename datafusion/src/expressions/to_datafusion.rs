use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{col, lit, Expr};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions::expr_fn::named_struct;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use delta_kernel::expressions::{
    BinaryExpression, BinaryOperator, Expression, JunctionExpression, JunctionOperator, Scalar,
    UnaryExpression, UnaryOperator,
};
use delta_kernel::schema::DataType;
use itertools::Itertools;

pub(crate) fn to_datafusion_expr(expr: &Expression, output_type: &DataType) -> DFResult<Expr> {
    match expr {
        Expression::Column(name) => {
            let mut name_iter = name.iter();
            let base_name = name_iter.next().ok_or_else(|| {
                DataFusionError::Internal("Expected at least one column name".into())
            })?;
            Ok(name_iter.fold(col(base_name), |acc, n| acc.field(n)))
        }
        Expression::Unary(UnaryExpression { op, expr }) => {
            let inner_expr = to_datafusion_expr(expr, output_type)?;
            Ok(match op {
                UnaryOperator::IsNull => inner_expr.is_null(),
                UnaryOperator::Not => !inner_expr,
            })
        }
        Expression::Literal(scalar) => scalar_to_df(scalar).map(lit),
        Expression::Binary(expr) => binary_to_df(expr, output_type),
        Expression::Junction(expr) => junction_to_df(expr, output_type),
        Expression::Struct(fields) => struct_to_df(fields, output_type),
    }
}

fn scalar_to_df(scalar: &Scalar) -> DFResult<ScalarValue> {
    Ok(match scalar {
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
            ScalarValue::Decimal128(Some(data.bits()), data.precision(), data.scale() as i8)
        }
        Scalar::Struct(data) => {
            let fields = data
                .fields()
                .iter()
                .map(ArrowField::try_from)
                .try_collect::<_, Vec<_>, _>()?;
            let values = data
                .values()
                .iter()
                .map(scalar_to_df)
                .try_collect::<_, Vec<_>, _>()?;
            fields
                .into_iter()
                .zip(values.into_iter())
                .fold(ScalarStructBuilder::new(), |builder, (field, value)| {
                    builder.with_scalar(field, value)
                })
                .build()?
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
        Scalar::Null(data_type) => ScalarValue::try_from(&ArrowDataType::try_from(data_type)?)?,
    })
}

fn binary_to_df(bin: &BinaryExpression, output_type: &DataType) -> DFResult<Expr> {
    let BinaryExpression { left, op, right } = bin;
    let left_expr = to_datafusion_expr(left, output_type)?;
    let right_expr = to_datafusion_expr(right, output_type)?;
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

fn junction_to_df(junction: &JunctionExpression, output_type: &DataType) -> DFResult<Expr> {
    let JunctionExpression { op, exprs } = junction;
    let df_exprs: Vec<_> = exprs
        .iter()
        .map(|e| to_datafusion_expr(e, output_type))
        .try_collect()?;
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

fn struct_to_df(fields: &[Expression], output_type: &DataType) -> DFResult<Expr> {
    let DataType::Struct(struct_type) = output_type else {
        return Err(DataFusionError::Execution(
            "expected struct output type".into(),
        ));
    };
    let df_exprs: Vec<_> = fields
        .iter()
        .zip(struct_type.fields())
        .map(|(expr, field)| {
            Ok(vec![
                lit(field.name().to_string()),
                to_datafusion_expr(expr, field.data_type())?,
            ])
        })
        .flatten_ok()
        .try_collect::<_, _, DataFusionError>()?;
    Ok(named_struct(df_exprs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::expressions::{
        BinaryExpression, BinaryOperator, JunctionOperator, Scalar, UnaryExpression, UnaryOperator,
    };
    use delta_kernel::schema::{DataType, StructField, StructType};

    /// Test basic column reference: `test_col`
    #[test]
    fn test_column_expression() {
        let expr = Expression::Column(ColumnName::new(["test_col"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, col("test_col"));

        let expr = Expression::Column(ColumnName::new(["test_col", "field_1", "field_2"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, col("test_col").field("field_1").field("field_2"));
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
            let result = to_datafusion_expr(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
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
            let result = to_datafusion_expr(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
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
            let result = to_datafusion_expr(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
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
            let result = to_datafusion_expr(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
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

        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        let expected = (col("a").gt(lit(1)).and(col("b").lt(lit(2)))).or(col("c").eq(lit(3)));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_struct_expression() {
        let expr = Expression::Struct(vec![
            Expression::Column(ColumnName::new(["a"])),
            Expression::Column(ColumnName::new(["b"])),
        ]);
        let result = to_datafusion_expr(
            &expr,
            &DataType::Struct(Box::new(StructType::new(vec![
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::INTEGER),
            ]))),
        )
        .unwrap();
        assert_eq!(
            result,
            named_struct(vec![lit("a"), col("a"), lit("b"), col("b")])
        );
    }

    /// Test error cases:
    /// - `a DISTINCT b` (unsupported operator)
    #[test]
    fn test_error_cases() {
        // Test unsupported binary operators
        let expr = Expression::Binary(BinaryExpression {
            left: Box::new(Expression::Column(ColumnName::new(["a"]))),
            op: BinaryOperator::Distinct,
            right: Box::new(Expression::Column(ColumnName::new(["b"]))),
        });
        assert!(to_datafusion_expr(&expr, &DataType::BOOLEAN).is_err());
    }
}
