//! Definitions and functions to create and manipulate kernel expressions

use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

pub use self::column_names::{
    column_expr, column_name, joined_column_expr, joined_column_name, ColumnName,
};
pub use self::scalars::{ArrayData, Scalar, StructData};
use crate::DataType;

mod column_names;
mod scalars;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// Distinct
    Distinct,
    /// IN
    In,
    /// NOT IN
    NotIn,
}

impl BinaryOperator {
    /// Returns `<op2>` (if any) such that `B <op2> A` is equivalent to `A <op> B`.
    pub(crate) fn commute(&self) -> Option<BinaryOperator> {
        use BinaryOperator::*;
        match self {
            GreaterThan => Some(LessThan),
            GreaterThanOrEqual => Some(LessThanOrEqual),
            LessThan => Some(GreaterThan),
            LessThanOrEqual => Some(GreaterThanOrEqual),
            Equal | NotEqual | Plus | Multiply => Some(*self),
            _ => None,
        }
    }

    /// invert an operator. Returns Some<InvertedOp> if the operator supports inversion, None if it
    /// cannot be inverted
    pub(crate) fn invert(&self) -> Option<BinaryOperator> {
        use BinaryOperator::*;
        match self {
            LessThan => Some(GreaterThanOrEqual),
            LessThanOrEqual => Some(GreaterThan),
            GreaterThan => Some(LessThanOrEqual),
            GreaterThanOrEqual => Some(LessThan),
            Equal => Some(NotEqual),
            NotEqual => Some(Equal),
            In => Some(NotIn),
            NotIn => Some(In),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VariadicOperator {
    And,
    Or,
}

impl VariadicOperator {
    pub(crate) fn invert(&self) -> VariadicOperator {
        use VariadicOperator::*;
        match self {
            And => Or,
            Or => And,
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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
            // TODO(roeap): AFAIK DISTINCT does not have a commonly used operator symbol
            // so ideally this would not be used as we use Display for rendering expressions
            // in our code we take care of this, but theirs might not ...
            Self::Distinct => write!(f, "DISTINCT"),
            Self::In => write!(f, "IN"),
            Self::NotIn => write!(f, "NOT IN"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// A unary operator.
pub enum UnaryOperator {
    /// Unary Not
    Not,
    /// Unary Is Null
    IsNull,
}

pub type ExpressionRef = std::sync::Arc<Expression>;

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
    Column(ColumnName),
    /// A struct computed from a Vec of expressions
    Struct(Vec<Expression>),
    /// A unary operation.
    UnaryOperation {
        /// The operator.
        op: UnaryOperator,
        /// The expression.
        expr: Box<Expression>,
    },
    /// A binary operation.
    BinaryOperation {
        /// The operator.
        op: BinaryOperator,
        /// The left-hand side of the operation.
        left: Box<Expression>,
        /// The right-hand side of the operation.
        right: Box<Expression>,
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

impl From<ColumnName> for Expression {
    fn from(value: ColumnName) -> Self {
        Self::Column(value)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{l}"),
            Self::Column(name) => write!(f, "Column({name})"),
            Self::Struct(exprs) => write!(
                f,
                "Struct({})",
                &exprs.iter().map(|e| format!("{e}")).join(", ")
            ),
            Self::BinaryOperation {
                op: BinaryOperator::Distinct,
                left,
                right,
            } => write!(f, "DISTINCT({left}, {right})"),
            Self::BinaryOperation { op, left, right } => write!(f, "{left} {op} {right}"),
            Self::UnaryOperation { op, expr } => match op {
                UnaryOperator::Not => write!(f, "NOT {expr}"),
                UnaryOperator::IsNull => write!(f, "{expr} IS NULL"),
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
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut set = HashSet::new();

        for expr in self.walk() {
            if let Self::Column(name) = expr {
                set.insert(name);
            }
        }

        set
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    pub fn null_literal(data_type: DataType) -> Self {
        Self::Literal(Scalar::Null(data_type))
    }

    /// Create a new struct expression
    pub fn struct_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::Struct(exprs.into_iter().collect())
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

    /// Create a new expression `self IS NOT NULL`
    pub fn is_not_null(self) -> Self {
        !Self::is_null(self)
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::Equal, self, other)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::NotEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn le(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn ge(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: impl Into<Self>) -> Self {
        Self::and_from([self, other.into()])
    }

    /// Create a new expression `self OR other`
    pub fn or(self, other: impl Into<Self>) -> Self {
        Self::or_from([self, other.into()])
    }

    /// Create a new expression `DISTINCT(self, other)`
    pub fn distinct(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::Distinct, self, other)
    }

    fn walk(&self) -> impl Iterator<Item = &Self> + '_ {
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::Struct(exprs) => {
                    stack.extend(exprs.iter());
                }
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

impl<R: Into<Expression>> std::ops::Add<R> for Expression {
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        Self::binary(BinaryOperator::Plus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Sub<R> for Expression {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        Self::binary(BinaryOperator::Minus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Mul<R> for Expression {
    type Output = Self;

    fn mul(self, rhs: R) -> Self {
        Self::binary(BinaryOperator::Multiply, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Div<R> for Expression {
    type Output = Self;

    fn div(self, rhs: R) -> Self {
        Self::binary(BinaryOperator::Divide, self, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::{column_expr, Expression as Expr};

    #[test]
    fn test_expression_format() {
        let col_ref = column_expr!("x");
        let cases = [
            (col_ref.clone(), "Column(x)"),
            (col_ref.clone().eq(2), "Column(x) = 2"),
            ((col_ref.clone() - 4).lt(10), "Column(x) - 4 < 10"),
            ((col_ref.clone() + 4) / 10 * 42, "Column(x) + 4 / 10 * 42"),
            (
                col_ref.clone().gt_eq(2).and(col_ref.clone().lt_eq(10)),
                "AND(Column(x) >= 2, Column(x) <= 10)",
            ),
            (
                Expr::and_from([
                    col_ref.clone().gt_eq(2),
                    col_ref.clone().lt_eq(10),
                    col_ref.clone().lt_eq(100),
                ]),
                "AND(Column(x) >= 2, Column(x) <= 10, Column(x) <= 100)",
            ),
            (
                col_ref.clone().gt(2).or(col_ref.clone().lt(10)),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (col_ref.eq("foo"), "Column(x) = 'foo'"),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }
}
