//! Definitions and functions to create and manipulate kernel expressions

use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

pub use self::column_names::{
    column_expr, column_name, column_pred, joined_column_expr, joined_column_name, ColumnName,
};
pub use self::scalars::{ArrayData, DecimalData, MapData, Scalar, StructData};
use self::transforms::GetColumnReferences;
pub use self::transforms::{ExpressionDepthChecker, ExpressionTransform};
use crate::DataType;

mod column_names;
pub(crate) mod literal_expression_transform;
mod scalars;
pub mod transforms;

pub type ExpressionRef = std::sync::Arc<Expression>;
pub type PredicateRef = std::sync::Arc<Predicate>;

////////////////////////////////////////////////////////////////////////
// Operators
////////////////////////////////////////////////////////////////////////

/// A unary operator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOperator {
    /// Unary Not
    Not,
    /// Unary Is Null
    IsNull,
}

/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
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
    /// Arithmetic Plus
    Plus,
    /// Arithmetic Minus
    Minus,
    /// Arithmetic Multiply
    Multiply,
    /// Arithmetic Divide
    Divide,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JunctionOperator {
    /// Conjunction
    And,
    /// Disjunction
    Or,
}

////////////////////////////////////////////////////////////////////////
// Expressions and predicates
////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub struct UnaryPredicate {
    /// The operator.
    pub op: UnaryOperator,
    /// The input expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryPredicate {
    /// The operator.
    pub op: BinaryOperator,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

// TODO: Actually split this out
pub type BinaryExpression = BinaryPredicate;

#[derive(Clone, Debug, PartialEq)]
pub struct JunctionPredicate {
    /// The operator.
    pub op: JunctionOperator,
    /// The input predicates.
    pub preds: Vec<Predicate>,
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
    Column(ColumnName),
    /// A struct computed from a Vec of expressions
    Struct(Vec<Expression>),
    /// A unary operation.
    Unary(UnaryPredicate),
    /// A binary operation.
    Binary(BinaryPredicate),
    /// A junction operation (AND/OR).
    Junction(JunctionPredicate),
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

/// TODO: Split this out as a proper enum
pub type Predicate = Expression;

////////////////////////////////////////////////////////////////////////
// Struct/Enum impls
////////////////////////////////////////////////////////////////////////

impl BinaryOperator {
    /// True if this is a comparison for which NULL input always produces NULL output
    pub(crate) fn is_null_intolerant_comparison(&self) -> bool {
        use BinaryOperator::*;
        match self {
            Plus | Minus | Multiply | Divide => false, // not a comparison
            LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => true,
            Equal | NotEqual => true,
            Distinct | In | NotIn => false, // tolerates NULL input
        }
    }

    /// Returns `<op2>` (if any) such that `B <op2> A` is equivalent to `A <op> B`.
    pub(crate) fn commute(&self) -> Option<BinaryOperator> {
        use BinaryOperator::*;
        match self {
            GreaterThan => Some(LessThan),
            GreaterThanOrEqual => Some(LessThanOrEqual),
            LessThan => Some(GreaterThan),
            LessThanOrEqual => Some(GreaterThanOrEqual),
            Equal | NotEqual | Distinct | Plus | Multiply => Some(*self),
            In | NotIn | Minus | Divide => None, // not commutative
        }
    }
}

impl JunctionOperator {
    pub(crate) fn invert(&self) -> JunctionOperator {
        use JunctionOperator::*;
        match self {
            And => Or,
            Or => And,
        }
    }
}

impl UnaryPredicate {
    fn new(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl BinaryPredicate {
    fn new(op: BinaryOperator, left: impl Into<Expression>, right: impl Into<Expression>) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

impl JunctionPredicate {
    fn new(op: JunctionOperator, preds: Vec<Predicate>) -> Self {
        Self { op, preds }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        let _ = references.transform_expr(self);
        references.into_inner()
    }

    /// Create a new column name expression from input satisfying `FromIterator for ColumnName`.
    pub fn column<A>(field_names: impl IntoIterator<Item = A>) -> Expression
    where
        ColumnName: FromIterator<A>,
    {
        ColumnName::new(field_names).into()
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    /// Creates a NULL literal expression
    pub const fn null_literal(data_type: DataType) -> Self {
        Self::Literal(Scalar::Null(data_type))
    }

    /// Create a new struct expression
    pub fn struct_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::Struct(exprs.into_iter().collect())
    }

    /// Logical NOT (boolean inversion)
    pub fn not(pred: impl Into<Self>) -> Self {
        Self::unary(UnaryOperator::Not, pred.into())
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOperator::IsNull, self)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(self) -> Predicate {
        Self::not(Self::is_null(self))
    }

    /// Create a new predicate `self == other`
    pub fn eq(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::Equal, self, other)
    }

    /// Create a new predicate `self != other`
    pub fn ne(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::NotEqual, self, other)
    }

    /// Create a new predicate `self <= other`
    pub fn le(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new predicate `self < other`
    pub fn lt(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::LessThan, self, other)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new predicate `self > other`
    pub fn gt(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::GreaterThan, self, other)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(self, other: impl Into<Self>) -> Self {
        Self::binary(BinaryOperator::Distinct, self, other)
    }

    /// Create a new predicate `self AND other`
    pub fn and(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::and_from([a.into(), b.into()])
    }

    /// Create a new predicate `self OR other`
    pub fn or(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::or_from([a.into(), b.into()])
    }

    /// Creates a new predicate AND(preds...)
    pub fn and_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionOperator::And, preds)
    }

    /// Creates a new predicate OR(preds...)
    pub fn or_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionOperator::Or, preds)
    }

    /// Creates a new unary predicate OP expr
    pub fn unary(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self::Unary(UnaryPredicate { op, expr })
    }

    /// Creates a new binary predicate lhs OP rhs
    pub fn binary(
        op: BinaryOperator,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryPredicate {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        })
    }

    /// Creates a new junction predicate OP(preds...)
    pub fn junction(op: JunctionOperator, preds: impl IntoIterator<Item = Self>) -> Self {
        let preds = preds.into_iter().collect();
        Self::Junction(JunctionPredicate { op, preds })
    }
}

////////////////////////////////////////////////////////////////////////
// Trait impls
////////////////////////////////////////////////////////////////////////

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryOperator::*;
        match self {
            Plus => write!(f, "+"),
            Minus => write!(f, "-"),
            Multiply => write!(f, "*"),
            Divide => write!(f, "/"),
            LessThan => write!(f, "<"),
            LessThanOrEqual => write!(f, "<="),
            GreaterThan => write!(f, ">"),
            GreaterThanOrEqual => write!(f, ">="),
            Equal => write!(f, "="),
            NotEqual => write!(f, "!="),
            // TODO(roeap): AFAIK DISTINCT does not have a commonly used operator symbol
            // so ideally this would not be used as we use Display for rendering expressions
            // in our code we take care of this, but theirs might not ...
            Distinct => write!(f, "DISTINCT"),
            In => write!(f, "IN"),
            NotIn => write!(f, "NOT IN"),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        match self {
            Literal(l) => write!(f, "{l}"),
            Column(name) => write!(f, "Column({name})"),
            Struct(exprs) => write!(
                f,
                "Struct({})",
                &exprs.iter().map(|e| format!("{e}")).join(", ")
            ),
            Binary(BinaryPredicate {
                op: BinaryOperator::Distinct,
                left,
                right,
            }) => write!(f, "DISTINCT({left}, {right})"),
            Binary(BinaryPredicate { op, left, right }) => write!(f, "{left} {op} {right}"),
            Unary(UnaryPredicate { op, expr }) => match op {
                UnaryOperator::Not => write!(f, "NOT {expr}"),
                UnaryOperator::IsNull => write!(f, "{expr} IS NULL"),
            },
            Junction(JunctionPredicate { op, preds }) => {
                let preds = &preds.iter().map(|p| format!("{p}")).join(", ");
                let op = match op {
                    JunctionOperator::And => "AND",
                    JunctionOperator::Or => "OR",
                };
                write!(f, "{op}({preds})")
            }
        }
    }
}

impl From<Scalar> for Expression {
    fn from(value: Scalar) -> Self {
        Self::literal(value)
    }
}

impl From<ColumnName> for Expression {
    fn from(value: ColumnName) -> Self {
        Self::Column(value)
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
    use super::{column_expr, column_pred, Expression as Expr, Predicate as Pred};

    #[test]
    fn test_expression_format() {
        let cases = [
            (column_expr!("x"), "Column(x)"),
            (
                (column_expr!("x") + Expr::literal(4)) / Expr::literal(10) * Expr::literal(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (
                Expr::struct_from([column_expr!("x"), Expr::literal(2), Expr::literal(10)]),
                "Struct(Column(x), 2, 10)",
            ),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_predicate_format() {
        let cases = [
            (column_pred!("x"), "Column(x)"),
            (column_expr!("x").eq(Expr::literal(2)), "Column(x) = 2"),
            (
                (column_expr!("x") - Expr::literal(4)).lt(Expr::literal(10)),
                "Column(x) - 4 < 10",
            ),
            (
                Pred::and(
                    column_expr!("x").ge(Expr::literal(2)),
                    column_expr!("x").le(Expr::literal(10)),
                ),
                "AND(Column(x) >= 2, Column(x) <= 10)",
            ),
            (
                Pred::and_from([
                    column_expr!("x").ge(Expr::literal(2)),
                    column_expr!("x").le(Expr::literal(10)),
                    column_expr!("x").le(Expr::literal(100)),
                ]),
                "AND(Column(x) >= 2, Column(x) <= 10, Column(x) <= 100)",
            ),
            (
                Pred::or(
                    column_expr!("x").gt(Expr::literal(2)),
                    column_expr!("x").lt(Expr::literal(10)),
                ),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (
                column_expr!("x").eq(Expr::literal("foo")),
                "Column(x) = 'foo'",
            ),
        ];

        for (pred, expected) in cases {
            let result = format!("{}", pred);
            assert_eq!(result, expected);
        }
    }
}
