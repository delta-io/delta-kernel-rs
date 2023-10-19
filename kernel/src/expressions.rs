use arrow_array::{
    array::PrimitiveArray, types::Int32Type, BooleanArray, Int32Array, RecordBatch, StructArray,
};
use arrow_ord::cmp::lt;
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
};

use arrow_schema::ArrowError;

use self::scalars::Scalar;

pub mod scalars;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    // Logical
    And,
    Or,
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    // Comparison
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::LessThan => write!(f, "<"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThan => write!(f, ">"),
            Self::GreaterThanOrEqual => write!(f, ">="),
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "!="),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    IsNull,
}

/// A SQL expression.
///
/// These expressions do not track or validate data types, other than the type
/// of literals. It is up to the expression evaluator to validate the
/// expression against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A literal value.
    Literal(Scalar),
    /// A column reference by name.
    Column(String),
    BinaryOperation {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOperation {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{}", l),
            Self::Column(name) => write!(f, "Column({})", name),
            Self::BinaryOperation { op, left, right } => {
                match op {
                    // OR requires parentheses
                    BinaryOperator::Or => write!(f, "({} OR {})", left, right),
                    _ => write!(f, "{} {} {}", left, op, right),
                }
            }
            Self::UnaryOperation { op, expr } => match op {
                UnaryOperator::Not => write!(f, "NOT {}", expr),
                UnaryOperator::IsNull => write!(f, "{} IS NULL", expr),
            },
        }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&str> {
        let mut set = HashSet::new();

        for expr in self.walk() {
            if let Self::Column(name) = expr {
                set.insert(name.as_str());
            }
        }

        set
    }

    /// Create an new expression for a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    fn binary_op_impl(self, other: Self, op: BinaryOperator) -> Self {
        Self::BinaryOperation {
            op,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::Equal)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::NotEqual)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::LessThan)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::GreaterThan)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::GreaterThanOrEqual)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::LessThanOrEqual)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::And)
    }

    /// Create a new expression `self OR other`
    pub fn or(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::Or)
    }

    fn walk(&self) -> impl Iterator<Item = &Self> + '_ {
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::BinaryOperation { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::UnaryOperation { expr, .. } => {
                    stack.push(expr);
                }
            }
            Some(expr)
        })
    }

    /// Apply the predicate to a stats record batch, returning a boolean array
    ///
    /// The boolean array will represent a mask of the files that could match
    /// the predicate.
    ///
    /// For example, if the predicate is `x > 2` and the stats record batch has
    /// `maxValues.x = 1`, then the returned boolean array will have `false` at
    /// that index.
    pub(crate) fn construct_metadata_filters(
        &self,
        stats: RecordBatch,
    ) -> Result<BooleanArray, ArrowError> {
        match self {
            // col < value
            Expression::BinaryOperation { op, left, right } => {
                match op {
                    BinaryOperator::LessThan => {
                        match (left.as_ref(), right.as_ref()) {
                            (Expression::Column(name), Expression::Literal(l)) => {
                                let literal_value = match l {
                                    Scalar::Integer(v) => *v,
                                    _ => todo!(),
                                };
                                // column_min < value
                                lt(
                                    stats
                                        .column_by_name("minValues")
                                        .unwrap()
                                        .as_any()
                                        .downcast_ref::<StructArray>()
                                        .unwrap()
                                        .column_by_name(name)
                                        .unwrap()
                                        .as_any()
                                        .downcast_ref::<Int32Array>()
                                        .unwrap(),
                                    &PrimitiveArray::<Int32Type>::new_scalar(literal_value),
                                )
                            }
                            _ => todo!(),
                        }
                    }
                    _ => todo!(),
                }
            }

            _ => todo!(),
        }
    }
}

// impl Expression {
//     fn to_arrow(&self, stats: &StructArray) -> Result<BooleanArray, ArrowError> {
//         match self {
//             Expression::LessThan(left, right) => {
//                 lt_scalar(left.to_arrow(stats), right.to_arrow(stats))
//             }
//             Expression::Column(c) => todo!(),
//             Expression::Literal(l) => todo!(),
//         }
//     }
// }

// transform data predicate into metadata predicate
// WHERE x < 10 -> min(x) < 10
// fn construct_metadata_filters(e: Expression) -> Expression {
//     match e {
//         // col < value
//         Expression::LessThan(left, right) => {
//             match (*left, *right.clone()) {
//                 (Expression::Column(name), Expression::Literal(_)) => {
//                     // column_min < value
//                     Expression::LessThan(Box::new(min_stat_col(name)), right)
//                 }
//                 _ => todo!(),
//             }
//         }
//         _ => todo!(),
//     }
// }

// fn min_stat_col(col_name: Vec<String>) -> Expression {
//     stat_col("min", col_name)
// }
//
// fn stat_col(stat: &str, name: Vec<String>) -> Expression {
//     let mut v = vec![stat.to_owned()];
//     v.extend(name);
//     Expression::Column(v)
// }

impl std::ops::Add<Expression> for Expression {
    type Output = Self;

    fn add(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Plus)
    }
}

impl std::ops::Sub<Expression> for Expression {
    type Output = Self;

    fn sub(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Minus)
    }
}

impl std::ops::Mul<Expression> for Expression {
    type Output = Self;

    fn mul(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Multiply)
    }
}

impl std::ops::Div<Expression> for Expression {
    type Output = Self;

    fn div(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Divide)
    }
}

#[cfg(test)]
mod tests {
    use super::Expression as Expr;

    #[test]
    fn test_expression_format() {
        let col_ref = Expr::column("x");
        let cases = [
            (col_ref.clone(), "Column(x)"),
            (col_ref.clone().eq(Expr::literal(2)), "Column(x) = 2"),
            (
                col_ref
                    .clone()
                    .gt_eq(Expr::literal(2))
                    .and(col_ref.clone().lt_eq(Expr::literal(10))),
                "Column(x) >= 2 AND Column(x) <= 10",
            ),
            (
                col_ref
                    .clone()
                    .gt(Expr::literal(2))
                    .or(col_ref.clone().lt(Expr::literal(10))),
                "(Column(x) > 2 OR Column(x) < 10)",
            ),
            (
                (col_ref.clone() - Expr::literal(4)).lt(Expr::literal(10)),
                "Column(x) - 4 < 10",
            ),
            (
                (col_ref.clone() + Expr::literal(4)) / Expr::literal(10) * Expr::literal(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (col_ref.eq(Expr::literal("foo")), "Column(x) = 'foo'"),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }
}
