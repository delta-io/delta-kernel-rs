use delta_kernel::expressions::{column_name, column_pred};
use delta_kernel::schema::DecimalType;
use delta_kernel::{
    expressions::{
        BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
        JunctionPredicate, JunctionPredicateOp, Scalar, UnaryPredicate, UnaryPredicateOp,
    },
    schema::{DataType, PrimitiveType},
    Expression, Predicate,
};
use itertools::Either;
use serde::{Deserialize, Deserializer};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::{ast::ValueWithSpan, dialect::GenericDialect, parser::Parser};
use url::Url;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum WorkloadSpec {
    #[serde(rename = "read_metadata")]
    ReadMetadata(ReadMetadataWorkload),
}

#[derive(Deserialize, Debug)]
pub struct ReadMetadataWorkload {
    pub table_root: Url,
    #[serde(rename = "snapshot_version")]
    pub version: Option<u64>,
    pub predicate: Option<WorkloadPredicate>,
    pub expected_scan_metadata: String,
}

#[derive(Debug)]
pub struct WorkloadPredicate(pub Expression);

impl<'de> Deserialize<'de> for WorkloadPredicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let sql_string = String::deserialize(deserializer)?;

        // Wrap in a dummy SELECT to make it valid SQL
        let full_sql = format!("SELECT * FROM dummy WHERE {}", sql_string);

        let dialect = DatabricksDialect {};
        let mut ast = Parser::parse_sql(&dialect, &full_sql)
            .map_err(|e| serde::de::Error::custom(format!("SQL parse error: {}", e)))?;

        if let Some(sqlparser::ast::Statement::Query(query)) = ast.pop() {
            if let Some(selection) = query.body.as_select() {
                if let Some(where_clause) = &selection.selection {
                    return Self::try_from(where_clause).map_err(|x| serde::de::Error::custom(x));
                }
            }
        }
        Err(serde::de::Error::custom("Failed to extract WHERE clause"))
    }
}
impl TryFrom<&sqlparser::ast::Expr> for WorkloadPredicate {
    type Error = String;

    fn try_from(value: &sqlparser::ast::Expr) -> Result<Self, Self::Error> {
        use sqlparser::ast::Expr::*;
        fn parser_expr_to_kernel(value: &sqlparser::ast::Expr) -> Expression {
            match value {
                Identifier(ident) => Expression::Column(ColumnName::new([ident.value.clone()])),
                CompoundIdentifier(compound) => Expression::Column(ColumnName::new(
                    compound.into_iter().cloned().map(|ident| ident.value),
                )),
                IsNull(inner) => {
                    Expression::Predicate(Box::new(Predicate::Unary(UnaryPredicate {
                        op: UnaryPredicateOp::IsNull,
                        expr: Box::new(parser_expr_to_kernel(inner.as_ref())),
                    })))
                }
                BinaryOp {
                    left: left_inner,
                    op,
                    right: right_inner,
                } => {
                    let left = Box::new(parser_expr_to_kernel(left_inner));
                    let right = Box::new(parser_expr_to_kernel(right_inner));
                    match op {
                        sqlparser::ast::BinaryOperator::Plus => {
                            Expression::Binary(BinaryExpression {
                                left,
                                op: BinaryExpressionOp::Plus,
                                right,
                            })
                        }
                        sqlparser::ast::BinaryOperator::Minus => {
                            Expression::Binary(BinaryExpression {
                                left,
                                op: BinaryExpressionOp::Minus,
                                right,
                            })
                        }
                        sqlparser::ast::BinaryOperator::Multiply => {
                            Expression::Binary(BinaryExpression {
                                left,
                                op: BinaryExpressionOp::Multiply,
                                right,
                            })
                        }
                        sqlparser::ast::BinaryOperator::Divide => {
                            Expression::Binary(BinaryExpression {
                                left,
                                op: BinaryExpressionOp::Divide,
                                right,
                            })
                        }
                        sqlparser::ast::BinaryOperator::Gt => {
                            Expression::Predicate(Box::new(Predicate::Binary(BinaryPredicate {
                                left,
                                op: BinaryPredicateOp::GreaterThan,
                                right,
                            })))
                        }
                        sqlparser::ast::BinaryOperator::Lt => {
                            Expression::Predicate(Box::new(Predicate::Binary(BinaryPredicate {
                                left,
                                op: BinaryPredicateOp::LessThan,
                                right,
                            })))
                        }
                        sqlparser::ast::BinaryOperator::GtEq => {
                            let lt = Box::new(Predicate::Binary(BinaryPredicate {
                                left,
                                op: BinaryPredicateOp::LessThan,
                                right,
                            }));
                            Expression::Predicate(Box::new(Predicate::Not(lt)))
                        }
                        sqlparser::ast::BinaryOperator::LtEq => {
                            let gt = Box::new(Predicate::Binary(BinaryPredicate {
                                left,
                                op: BinaryPredicateOp::GreaterThan,
                                right,
                            }));
                            Expression::Predicate(Box::new(Predicate::Not(gt)))
                        }
                        sqlparser::ast::BinaryOperator::Eq => {
                            Expression::Predicate(Box::new(Predicate::Binary(BinaryPredicate {
                                left,
                                op: BinaryPredicateOp::Equal,
                                right,
                            })))
                        }
                        sqlparser::ast::BinaryOperator::NotEq => {
                            let eq = Box::new(Predicate::Binary(BinaryPredicate {
                                left,
                                op: BinaryPredicateOp::Equal,
                                right,
                            }));
                            Expression::Predicate(Box::new(Predicate::Not(eq)))
                        }
                        sqlparser::ast::BinaryOperator::And => Expression::Predicate(Box::new(
                            Predicate::Junction(JunctionPredicate {
                                op: JunctionPredicateOp::And,
                                preds: vec![
                                    Predicate::BooleanExpression(*left),
                                    Predicate::BooleanExpression(*right),
                                ],
                            }),
                        )),
                        sqlparser::ast::BinaryOperator::Or => Expression::Predicate(Box::new(
                            Predicate::Junction(JunctionPredicate {
                                op: JunctionPredicateOp::Or,
                                preds: vec![
                                    Predicate::BooleanExpression(*left),
                                    Predicate::BooleanExpression(*right),
                                ],
                            }),
                        )),
                        _ => unimplemented!("Not implemented"),
                    }
                }
                Value(ValueWithSpan { value, span }) => {
                    let scalar = match value {
                        sqlparser::ast::Value::Number(num, _) => {
                            let fns = vec![
                                |x| PrimitiveType::Byte.parse_scalar(x),
                                |x| PrimitiveType::Short.parse_scalar(x),
                                |x| PrimitiveType::Integer.parse_scalar(x),
                                |x| PrimitiveType::Long.parse_scalar(x),
                                |x| PrimitiveType::Float.parse_scalar(x),
                                |x| PrimitiveType::Double.parse_scalar(x),
                                |x| PrimitiveType::Timestamp.parse_scalar(x),
                            ];

                            // Try each parser function in order until one succeeds
                            for parse_fn in fns {
                                if let Ok(scalar) = parse_fn(&num) {
                                    return Expression::Literal(scalar);
                                }
                            }
                            unimplemented!("Not implemented")
                        }
                        sqlparser::ast::Value::SingleQuotedString(string) => {
                            Scalar::String(string.clone())
                        }

                        sqlparser::ast::Value::Boolean(boolean) => Scalar::Boolean(*boolean),
                        _ => unimplemented!("Not implemented"),
                    };
                    Expression::Literal(scalar)
                }
                _ => unimplemented!("Not implemented"),
            }
        }
        Ok(Self(parser_expr_to_kernel(value)))
    }
}
