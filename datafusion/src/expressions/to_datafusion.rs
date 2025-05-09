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
            let fields: Vec<ArrowField> =
                data.fields().iter().map(TryInto::try_into).try_collect()?;
            let values: Vec<_> = data.values().iter().map(scalar_to_df).try_collect()?;
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
                "Array scalar values not implemented".into(),
            ))
        }
        Scalar::Map(_) => {
            return Err(DataFusionError::NotImplemented(
                "Map scalar values not implemented".into(),
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
        ArrayData, BinaryExpression, BinaryOperator, JunctionOperator, MapData, Scalar, StructData,
        UnaryExpression, UnaryOperator,
    };
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

    /// Test conversion of primitive scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_primitives() {
        let test_cases = vec![
            (Scalar::Boolean(true), ScalarValue::Boolean(Some(true))),
            (
                Scalar::String("test".to_string()),
                ScalarValue::Utf8(Some("test".to_string())),
            ),
            (Scalar::Integer(42), ScalarValue::Int32(Some(42))),
            (Scalar::Long(42), ScalarValue::Int64(Some(42))),
            (Scalar::Float(42.0), ScalarValue::Float32(Some(42.0))),
            (Scalar::Double(42.0), ScalarValue::Float64(Some(42.0))),
            (Scalar::Byte(42), ScalarValue::Int8(Some(42))),
            (Scalar::Short(42), ScalarValue::Int16(Some(42))),
        ];

        for (input, expected) in test_cases {
            let result = scalar_to_df(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test conversion of temporal scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_temporal() {
        let test_cases = vec![
            (
                Scalar::Timestamp(1234567890),
                ScalarValue::TimestampMicrosecond(Some(1234567890), Some("UTC".into())),
            ),
            (
                Scalar::TimestampNtz(1234567890),
                ScalarValue::TimestampMicrosecond(Some(1234567890), None),
            ),
            (Scalar::Date(18262), ScalarValue::Date32(Some(18262))),
        ];

        for (input, expected) in test_cases {
            let result = scalar_to_df(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test conversion of binary and decimal scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_binary_decimal() {
        let binary_data = vec![1, 2, 3];
        let decimal_data = Scalar::decimal(123456789, 10, 2).unwrap();

        let test_cases = vec![
            (
                Scalar::Binary(binary_data.clone()),
                ScalarValue::Binary(Some(binary_data)),
            ),
            (
                decimal_data,
                ScalarValue::Decimal128(Some(123456789), 10, 2),
            ),
        ];

        for (input, expected) in test_cases {
            let result = scalar_to_df(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test conversion of struct scalar type to DataFusion scalar value
    #[test]
    fn test_scalar_to_df_struct() {
        let result = scalar_to_df(&Scalar::Struct(
            StructData::try_new(
                vec![
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ],
                vec![Scalar::Integer(42), Scalar::String("test".to_string())],
            )
            .unwrap(),
        ))
        .unwrap();

        // Create the expected struct value
        let expected = ScalarStructBuilder::new()
            .with_scalar(
                ArrowField::new("a", ArrowDataType::Int32, true),
                ScalarValue::Int32(Some(42)),
            )
            .with_scalar(
                ArrowField::new("b", ArrowDataType::Utf8, true),
                ScalarValue::Utf8(Some("test".to_string())),
            )
            .build()
            .unwrap();

        assert_eq!(result, expected);
    }

    /// Test conversion of null scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_null() {
        let test_cases = vec![
            (Scalar::Null(DataType::INTEGER), ScalarValue::Int32(None)),
            (Scalar::Null(DataType::STRING), ScalarValue::Utf8(None)),
            (Scalar::Null(DataType::BOOLEAN), ScalarValue::Boolean(None)),
            (Scalar::Null(DataType::DOUBLE), ScalarValue::Float64(None)),
        ];

        for (input, expected) in test_cases {
            let result = scalar_to_df(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test error cases for unsupported scalar types (Array and Map)
    #[test]
    fn test_scalar_to_df_errors() {
        let array_data = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, true),
            vec![Scalar::Integer(1), Scalar::Integer(2)],
        )
        .unwrap();

        let map_data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, true),
            vec![
                (Scalar::String("key1".to_string()), Scalar::Integer(1)),
                (Scalar::String("key2".to_string()), Scalar::Integer(2)),
            ],
        )
        .unwrap();

        let test_cases = vec![
            (
                Scalar::Array(array_data),
                "Array scalar values not supported",
            ),
            (Scalar::Map(map_data), "Map scalar values not supported"),
        ];

        for (input, expected_error) in test_cases {
            let result = scalar_to_df(&input);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected_error));
        }
    }

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

        // Test error case: empty column name
        let expr = Expression::Column(ColumnName::new::<&str>([]));
        assert!(to_datafusion_expr(&expr, &DataType::BOOLEAN).is_err());
    }

    /// Test binary expression conversions:
    /// - Equality: a = b
    /// - Inequality: a != b
    /// - Less than: a < b
    /// - Less than or equal: a <= b
    /// - Greater than: a > b
    /// - Greater than or equal: a >= b
    /// - Addition: a + b
    /// - Subtraction: a - b
    /// - Multiplication: a * b
    /// - Division: a / b
    #[test]
    fn test_binary_to_df() {
        let test_cases = vec![
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").eq(col("b")),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::NotEqual,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").not_eq(col("b")),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::LessThan,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").lt(col("b")),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::LessThanOrEqual,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").lt_eq(col("b")),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").gt(col("b")),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").gt_eq(col("b")),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") + col("b"),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Minus,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") - col("b"),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") * col("b"),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::Divide,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") / col("b"),
            ),
        ];

        for (input, expected) in test_cases {
            let result = binary_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test junction expression conversions:
    /// - Simple AND: a AND b
    /// - Simple OR: a OR b
    /// - Multiple AND: a AND b AND c
    /// - Multiple OR: a OR b OR c
    /// - Empty AND (should return true)
    /// - Empty OR (should return false)
    #[test]
    fn test_junction_to_df() {
        let test_cases = vec![
            // Simple AND
            (
                JunctionExpression {
                    op: JunctionOperator::And,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["a"])),
                        Expression::Column(ColumnName::new(["b"])),
                    ],
                },
                col("a").and(col("b")),
            ),
            // Simple OR
            (
                JunctionExpression {
                    op: JunctionOperator::Or,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["a"])),
                        Expression::Column(ColumnName::new(["b"])),
                    ],
                },
                col("a").or(col("b")),
            ),
            // Multiple AND
            (
                JunctionExpression {
                    op: JunctionOperator::And,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["a"])),
                        Expression::Column(ColumnName::new(["b"])),
                        Expression::Column(ColumnName::new(["c"])),
                    ],
                },
                col("a").and(col("b")).and(col("c")),
            ),
            // Multiple OR
            (
                JunctionExpression {
                    op: JunctionOperator::Or,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["a"])),
                        Expression::Column(ColumnName::new(["b"])),
                        Expression::Column(ColumnName::new(["c"])),
                    ],
                },
                col("a").or(col("b")).or(col("c")),
            ),
            // Empty AND (should return true)
            (
                JunctionExpression {
                    op: JunctionOperator::And,
                    exprs: vec![],
                },
                lit(true),
            ),
            // Empty OR (should return false)
            (
                JunctionExpression {
                    op: JunctionOperator::Or,
                    exprs: vec![],
                },
                lit(false),
            ),
        ];

        for (input, expected) in test_cases {
            let result = junction_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test to_datafusion_expr with various expression types and combinations:
    /// - Column expressions with nested fields
    /// - Complex unary expressions
    /// - Nested binary expressions
    /// - Mixed junction expressions
    /// - Struct expressions with nested fields
    /// - Complex combinations of all expression types
    #[test]
    fn test_to_datafusion_expr_comprehensive() {
        // Test column expressions with nested fields
        let expr = Expression::Column(ColumnName::new(["struct", "field", "nested"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, col("struct").field("field").field("nested"));

        // Test complex unary expressions
        let expr = Expression::Unary(UnaryExpression {
            op: UnaryOperator::Not,
            expr: Box::new(Expression::Unary(UnaryExpression {
                op: UnaryOperator::IsNull,
                expr: Box::new(Expression::Column(ColumnName::new(["a"]))),
            })),
        });
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, !col("a").is_null());

        // Test nested binary expressions
        let expr = Expression::Binary(BinaryExpression {
            left: Box::new(Expression::Binary(BinaryExpression {
                left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Column(ColumnName::new(["b"]))),
            })),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Column(ColumnName::new(["c"]))),
        });
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, (col("a") + col("b")) * col("c"));

        // Test mixed junction expressions
        let expr = Expression::Junction(JunctionExpression {
            op: JunctionOperator::And,
            exprs: vec![
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expression::Literal(Scalar::Integer(0))),
                }),
                Expression::Junction(JunctionExpression {
                    op: JunctionOperator::Or,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["b"])),
                        Expression::Column(ColumnName::new(["c"])),
                    ],
                }),
            ],
        });
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, col("a").gt(lit(0)).and(col("b").or(col("c"))));

        // Test struct expressions with nested fields
        let expr = Expression::Struct(vec![
            Expression::Column(ColumnName::new(["a"])),
            Expression::Binary(BinaryExpression {
                left: Box::new(Expression::Column(ColumnName::new(["b"]))),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Column(ColumnName::new(["c"]))),
            }),
        ]);
        let result = to_datafusion_expr(
            &expr,
            &DataType::Struct(Box::new(StructType::new(vec![
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("sum", DataType::INTEGER),
            ]))),
        )
        .unwrap();
        assert_eq!(
            result,
            named_struct(vec![lit("a"), col("a"), lit("sum"), col("b") + col("c")])
        );

        // Test complex combination of all expression types
        let expr = Expression::Junction(JunctionExpression {
            op: JunctionOperator::And,
            exprs: vec![
                Expression::Unary(UnaryExpression {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expression::Column(ColumnName::new(["a"]))),
                }),
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["b"]))),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(Scalar::Integer(42))),
                }),
                Expression::Junction(JunctionExpression {
                    op: JunctionOperator::Or,
                    exprs: vec![
                        Expression::Column(ColumnName::new(["c"])),
                        Expression::Column(ColumnName::new(["d"])),
                    ],
                }),
            ],
        });
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(
            result,
            (!col("a"))
                .and(col("b").eq(lit(42)))
                .and(col("c").or(col("d")))
        );

        // Test error case: empty column name
        let expr = Expression::Column(ColumnName::new::<&str>([]));
        assert!(to_datafusion_expr(&expr, &DataType::BOOLEAN).is_err());
    }
}
