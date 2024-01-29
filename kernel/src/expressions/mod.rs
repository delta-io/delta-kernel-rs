use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

pub use self::scalars::Scalar;

mod scalars;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A binary operator.
pub enum BinaryOperator {
    /// Arithmetic Plus
    Plus,
    /// Arithmetic Minus
    Minus,
    /// Arithmetic Multiply
    Multiply,
    /// Arithmetic Divide
    Divide,
    /// Comparison Less Than
    LessThan,
    /// Comparison Less Than Or Equal
    LessThanOrEqual,
    /// Comparison Greater Than
    GreaterThan,
    /// Comparison Greater Than Or Equal
    GreaterThanOrEqual,
    /// Comparison Equal
    Equal,
    /// Comparison Not Equal
    NotEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariadicOperator {
    And,
    Or,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            // Self::And => write!(f, "AND"),
            // Self::Or => write!(f, "OR"),
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
/// A unary operator.
pub enum UnaryOperator {
    /// Unary Not
    Not,
    /// Unary Is Null
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
    /// A binary operation.
    BinaryOperation {
        /// The operator.
        op: BinaryOperator,
        /// The left-hand side of the operation.
        left: Box<Expression>,
        /// The right-hand side of the operation.
        right: Box<Expression>,
    },
    /// A unary operation.
    UnaryOperation {
        /// The operator.
        op: UnaryOperator,
        /// The expression.
        expr: Box<Expression>,
    },
    VariadicOperation {
        /// The operator.
        op: VariadicOperator,
        /// The expressions.
        exprs: Vec<Expression>,
    },
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

impl<T: Into<Scalar>> From<T> for Expression {
    fn from(value: T) -> Self {
        Self::literal(value)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{}", l),
            Self::Column(name) => write!(f, "Column({})", name),
            Self::BinaryOperation { op, left, right } => write!(f, "{} {} {}", left, op, right),
            Self::UnaryOperation { op, expr } => match op {
                UnaryOperator::Not => write!(f, "NOT {}", expr),
                UnaryOperator::IsNull => write!(f, "{} IS NULL", expr),
            },
            Self::VariadicOperation { op, exprs } => match op {
                VariadicOperator::And => {
                    write!(
                        f,
                        "AND({})",
                        &exprs.iter().map(|e| format!("{e}")).join(", ")
                    )
                }
                VariadicOperator::Or => {
                    write!(
                        f,
                        "OR({})",
                        &exprs.iter().map(|e| format!("{e}")).join(", ")
                    )
                }
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

    /// Creates a new unary expression OP expr
    pub fn unary(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        Self::UnaryOperation {
            op,
            expr: Box::new(expr.into()),
        }
    }

    /// Creates a new binary expression lhs OP rhs
    pub fn binary(
        op: BinaryOperator,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::BinaryOperation {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        }
    }

    /// Creates a new variadic expression OP(exprs...)
    pub fn variadic(op: VariadicOperator, exprs: impl IntoIterator<Item = Self>) -> Self {
        let exprs = exprs.into_iter().collect::<Vec<_>>();
        Self::VariadicOperation { op, exprs }
    }

    /// Creates a new expression AND(exprs...)
    pub fn and_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::variadic(VariadicOperator::And, exprs)
    }

    /// Creates a new expression OR(exprs...)
    pub fn or_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::variadic(VariadicOperator::Or, exprs)
    }

    /// Create a new expression `self IS NULL`
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOperator::IsNull, self)
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: Self) -> Self {
        Self::binary(BinaryOperator::Equal, self, other)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: Self) -> Self {
        Self::binary(BinaryOperator::NotEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn le(self, other: Self) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: Self) -> Self {
        Self::binary(BinaryOperator::LessThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn ge(self, other: Self) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: Self) -> Self {
        Self::binary(BinaryOperator::GreaterThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: Self) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: Self) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: Self) -> Self {
        Self::and_from([self, other])
    }

    /// Create a new expression `self OR other`
    pub fn or(self, other: Self) -> Self {
        Self::or_from([self, other])
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
                Self::VariadicOperation { exprs, .. } => {
                    stack.extend(exprs.iter());
                }
            }
            Some(expr)
        })
    }
}

impl std::ops::Not for Expression {
    type Output = Self;

    fn not(self) -> Self {
        Self::unary(UnaryOperator::Not, self)
    }
}

impl std::ops::Add<Expression> for Expression {
    type Output = Self;

    fn add(self, rhs: Expression) -> Self::Output {
        Self::binary(BinaryOperator::Plus, self, rhs)
    }
}

impl std::ops::Sub<Expression> for Expression {
    type Output = Self;

    fn sub(self, rhs: Expression) -> Self {
        Self::binary(BinaryOperator::Minus, self, rhs)
    }
}

impl std::ops::Mul<Expression> for Expression {
    type Output = Self;

    fn mul(self, rhs: Expression) -> Self {
        Self::binary(BinaryOperator::Multiply, self, rhs)
    }
}

impl std::ops::Div<Expression> for Expression {
    type Output = Self;

    fn div(self, rhs: Expression) -> Self {
        Self::binary(BinaryOperator::Divide, self, rhs)
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
                (col_ref.clone() - Expr::literal(4)).lt(Expr::literal(10)),
                "Column(x) - 4 < 10",
            ),
            (
                (col_ref.clone() + Expr::literal(4)) / Expr::literal(10) * Expr::literal(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (
                col_ref
                    .clone()
                    .gt_eq(Expr::literal(2))
                    .and(col_ref.clone().lt_eq(Expr::literal(10))),
                "AND(Column(x) >= 2, Column(x) <= 10)",
            ),
            (
                Expr::and_from([
                    col_ref.clone().gt_eq(Expr::literal(2)),
                    col_ref.clone().lt_eq(Expr::literal(10)),
                    col_ref.clone().lt_eq(Expr::literal(100)),
                ]),
                "AND(Column(x) >= 2, Column(x) <= 10, Column(x) <= 100)",
            ),
            (
                col_ref
                    .clone()
                    .gt(Expr::literal(2))
                    .or(col_ref.clone().lt(Expr::literal(10))),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (col_ref.eq(Expr::literal("foo")), "Column(x) = 'foo'"),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }
}
