//! Definitions and functions to create and manipulate kernel expressions

use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

#[cfg(test)]
pub use self::column_names::column_pred;
pub use self::column_names::{
    column_expr, column_name, joined_column_expr, joined_column_name, ColumnName,
};
pub use self::scalars::{ArrayData, Scalar, StructData};
use self::transforms::{ExpressionTransform as _, GetColumnReferences};
use crate::DataType;

mod column_names;
mod scalars;
pub mod transforms;

pub type ExpressionRef = std::sync::Arc<Expression>;
pub type PredicateRef = std::sync::Arc<Predicate>;

////////////////////////////////////////////////////////////////////////
// Operators
////////////////////////////////////////////////////////////////////////

/// A unary predicate operator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryPredicateOp {
    /// Unary Is Null
    IsNull,
}

/// A unary expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(unused)] // No unary expressions yet
pub enum UnaryExpressionOp {
    // TODO: Integer negation? Casting?
}

/// A binary predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryPredicateOp {
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

/// A binary expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryExpressionOp {
    /// Arithmetic Plus
    Plus,
    /// Arithmetic Minus
    Minus,
    /// Arithmetic Multiply
    Multiply,
    /// Arithmetic Divide
    Divide,
}

/// A junction (AND/OR) predicate operator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JunctionPredicateOp {
    /// Conjunction
    And,
    /// Disjunction
    Or,
}

/// A variadic expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(unused)] // No variadic expressions yet
pub enum VariadicExpressionOp {
    // TODO: COALESCE?
}

////////////////////////////////////////////////////////////////////////
// Expressions
////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
#[allow(unused)] // No unary expressions yet
pub struct UnaryExpression {
    /// The operator.
    pub op: UnaryExpressionOp,
    /// The expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnaryPredicate {
    /// The operator.
    pub op: UnaryPredicateOp,
    /// The expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryPredicate {
    /// The operator.
    pub op: BinaryPredicateOp,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryExpression {
    /// The operator.
    pub op: BinaryExpressionOp,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(unused)] // No variadic expressions yet
pub struct VariadicExpression {
    /// The operator.
    pub op: VariadicExpressionOp,
    /// The input expressions.
    pub exprs: Vec<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JunctionPredicate {
    /// The operator.
    pub op: JunctionPredicateOp,
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
    /// A predicate treated as a boolean expression
    Predicate(Box<Predicate>),
    /// A struct computed from a Vec of expressions
    Struct(Vec<Expression>),
    // TODO: An expression that takes one expression as input.
    // Unary(UnaryExpression),
    /// An expression that takes two expressions as input.
    Binary(BinaryExpression),
    // TODO: An expression that takes a variable number of expressions as input.
    // Variadic(VariadicExpression),
}

/// A SQL predicate.
///
/// These predicates do not track or validate data types, other than the type
/// of literals. It is up to the predicate evaluator to validate the
/// predicate against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// A boolean-valued expression, useful for e.g. `AND(<boolean_col1>, <boolean_col2>)`.
    BooleanExpression(Box<Expression>),
    /// Boolean inversion (true <-> false)
    ///
    /// NOTE: NOT is not a normal unary predicate, because it requires a predicate as input (not an
    /// expression), and is never directly evaluated. Instead, observing that all predicates are
    /// invertible, NOT is always pushed down into its child predicate, inverting it. For example,
    /// `NOT (a < b)` pushes down and inverts `<` to `>=`, producing `a >= b`.
    Not(Box<Predicate>),
    /// A predicate that takes one expression as input.
    Unary(UnaryPredicate),
    /// A predicate that takes two expressions as input.
    Binary(BinaryPredicate),
    /// A variadic junction predicate -- conjunction or disjunction.
    Junction(JunctionPredicate),
}

////////////////////////////////////////////////////////////////////////
// Struct/Enum impls
////////////////////////////////////////////////////////////////////////

impl BinaryPredicateOp {
    /// True if this is a comparison for which NULL input always produces NULL output
    pub(crate) fn is_null_intolerant(&self) -> bool {
        use BinaryPredicateOp::*;
        match self {
            LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => true,
            Equal | NotEqual => true,
            Distinct | In | NotIn => false, // tolerates NULL input
        }
    }

    /// Returns `<op2>` (if any) such that `B <op2> A` is equivalent to `A <op> B`.
    pub(crate) fn commute(&self) -> Option<BinaryPredicateOp> {
        use BinaryPredicateOp::*;
        match self {
            GreaterThan => Some(LessThan),
            GreaterThanOrEqual => Some(LessThanOrEqual),
            LessThan => Some(GreaterThan),
            LessThanOrEqual => Some(GreaterThanOrEqual),
            Equal | NotEqual | Distinct => Some(*self),
            In | NotIn => None, // not commutative
        }
    }
}

impl JunctionPredicateOp {
    pub(crate) fn invert(&self) -> JunctionPredicateOp {
        use JunctionPredicateOp::*;
        match self {
            And => Or,
            Or => And,
        }
    }
}

#[allow(unused)] // No unary expressions yet
impl UnaryExpression {
    fn new(op: UnaryExpressionOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl UnaryPredicate {
    fn new(op: UnaryPredicateOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl BinaryExpression {
    fn new(
        op: BinaryExpressionOp,
        left: impl Into<Expression>,
        right: impl Into<Expression>,
    ) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

impl BinaryPredicate {
    fn new(
        op: BinaryPredicateOp,
        left: impl Into<Expression>,
        right: impl Into<Expression>,
    ) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

#[allow(unused)] // No variadic expressions yet
impl VariadicExpression {
    fn new(op: VariadicExpressionOp, exprs: Vec<Expression>) -> Self {
        Self { op, exprs }
    }
}

impl JunctionPredicate {
    fn new(op: JunctionPredicateOp, preds: Vec<Predicate>) -> Self {
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

    /// Wraps a predicate as a boolean-valued expression
    pub fn predicate(value: Predicate) -> Self {
        match value {
            Predicate::BooleanExpression(expr) => *expr,
            _ => Self::Predicate(value.into()),
        }
    }

    /// Create a new struct expression
    pub fn struct_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::Struct(exprs.into_iter().collect())
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(self) -> Predicate {
        Predicate::is_null(self)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(self) -> Predicate {
        Predicate::is_not_null(self)
    }

    /// Create a new predicate `self == other`
    pub fn eq(self, other: impl Into<Self>) -> Predicate {
        Predicate::eq(self, other)
    }

    /// Create a new predicate `self != other`
    pub fn ne(self, other: impl Into<Self>) -> Predicate {
        Predicate::ne(self, other)
    }

    /// Create a new predicate `self <= other`
    pub fn le(self, other: impl Into<Self>) -> Predicate {
        Predicate::le(self, other)
    }

    /// Create a new predicate `self < other`
    pub fn lt(self, other: impl Into<Self>) -> Predicate {
        Predicate::lt(self, other)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(self, other: impl Into<Self>) -> Predicate {
        Predicate::ge(self, other)
    }

    /// Create a new predicate `self > other`
    pub fn gt(self, other: impl Into<Self>) -> Predicate {
        Predicate::gt(self, other)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(self, other: impl Into<Self>) -> Predicate {
        Predicate::distinct(self, other)
    }

    /// Creates a new binary expression lhs OP rhs
    pub fn binary(
        op: BinaryExpressionOp,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryExpression {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        })
    }
}

impl Predicate {
    /// Returns a set of columns referenced by this predicate.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        let _ = references.transform_pred(self);
        references.into_inner()
    }

    /// Wraps a boolean-valued expression as a predicate
    pub fn expression(expr: impl Into<Expression>) -> Self {
        match expr.into() {
            Expression::Predicate(p) => *p,
            expr => Predicate::BooleanExpression(expr.into()),
        }
    }

    /// Logical NOT (boolean inversion)
    pub fn not(pred: impl Into<Self>) -> Self {
        Self::Not(pred.into().into())
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(expr: impl Into<Expression>) -> Predicate {
        Self::unary(UnaryPredicateOp::IsNull, expr)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(expr: impl Into<Expression>) -> Predicate {
        Self::not(Self::is_null(expr))
    }

    /// Create a new predicate `self == other`
    pub fn eq(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::Equal, a, b)
    }

    /// Create a new predicate `self != other`
    pub fn ne(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::NotEqual, a, b)
    }

    /// Create a new predicate `self <= other`
    pub fn le(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::LessThanOrEqual, a, b)
    }

    /// Create a new predicate `self < other`
    pub fn lt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::LessThan, a, b)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::GreaterThanOrEqual, a, b)
    }

    /// Create a new predicate `self > other`
    pub fn gt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::GreaterThan, a, b)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::Distinct, a, b)
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
        Self::junction(JunctionPredicateOp::And, preds)
    }

    /// Creates a new predicate OR(preds...)
    pub fn or_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionPredicateOp::Or, preds)
    }

    /// Creates a new unary predicate OP expr
    pub fn unary(op: UnaryPredicateOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self::Unary(UnaryPredicate { op, expr })
    }

    /// Creates a new binary predicate lhs OP rhs
    pub fn binary(
        op: BinaryPredicateOp,
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
    pub fn junction(op: JunctionPredicateOp, preds: impl IntoIterator<Item = Self>) -> Self {
        let preds = preds.into_iter().collect();
        Self::Junction(JunctionPredicate { op, preds })
    }
}

////////////////////////////////////////////////////////////////////////
// Trait impls
////////////////////////////////////////////////////////////////////////

impl Display for BinaryExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryExpressionOp::*;
        match self {
            Plus => write!(f, "+"),
            Minus => write!(f, "-"),
            Multiply => write!(f, "*"),
            Divide => write!(f, "/"),
        }
    }
}

impl Display for BinaryPredicateOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryPredicateOp::*;
        match self {
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
            Predicate(p) => write!(f, "{p}"),
            Struct(exprs) => write!(
                f,
                "Struct({})",
                &exprs.iter().map(|e| format!("{e}")).join(", ")
            ),
            Binary(BinaryExpression { op, left, right }) => write!(f, "{left} {op} {right}"),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Predicate::*;
        match self {
            BooleanExpression(expr) => write!(f, "{expr}"),
            Not(pred) => write!(f, "NOT({pred})"),
            Binary(BinaryPredicate {
                op: BinaryPredicateOp::Distinct,
                left,
                right,
            }) => write!(f, "DISTINCT({left}, {right})"),
            Binary(BinaryPredicate { op, left, right }) => write!(f, "{left} {op} {right}"),
            Unary(UnaryPredicate { op, expr }) => match op {
                UnaryPredicateOp::IsNull => write!(f, "{expr} IS NULL"),
            },
            Junction(JunctionPredicate { op, preds }) => {
                let preds = &preds.iter().map(|p| format!("{p}")).join(", ");
                let op = match op {
                    JunctionPredicateOp::And => "AND",
                    JunctionPredicateOp::Or => "OR",
                };
                write!(f, "{op}({preds})")
            }
        }
    }
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

impl From<Predicate> for Expression {
    fn from(value: Predicate) -> Self {
        Self::predicate(value)
    }
}

impl From<Expression> for Predicate {
    fn from(value: Expression) -> Self {
        Self::expression(value)
    }
}

/// Shortcut for wrapping a boolean [`Expression::Literal`] inside [`Predicate::BooleanExpression`].
//#[cfg(test)]
impl From<bool> for Predicate {
    fn from(value: bool) -> Self {
        Self::expression(value)
    }
}

/// Shortcut for wrapping a boolean [`Expression::Column`] inside [`Predicate::BooleanExpression`].
//#[cfg(test)]
impl From<ColumnName> for Predicate {
    fn from(value: ColumnName) -> Self {
        Self::expression(value)
    }
}

#[cfg(test)]
impl std::ops::Not for Predicate {
    type Output = Self;

    fn not(self) -> Self {
        Self::not(self)
    }
}

impl<R: Into<Expression>> std::ops::Add<R> for Expression {
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        Self::binary(BinaryExpressionOp::Plus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Sub<R> for Expression {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Minus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Mul<R> for Expression {
    type Output = Self;

    fn mul(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Multiply, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Div<R> for Expression {
    type Output = Self;

    fn div(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Divide, self, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::{column_expr, Predicate};

    #[test]
    fn test_expression_format() {
        let col_ref = column_expr!("x");
        let cases = [
            (Predicate::from(col_ref.clone()), "Column(x)"),
            (col_ref.clone().eq(2), "Column(x) = 2"),
            ((col_ref.clone() - 4).lt(10), "Column(x) - 4 < 10"),
            (
                Predicate::from((col_ref.clone() + 4) / 10 * 42),
                "Column(x) + 4 / 10 * 42",
            ),
            (
                Predicate::and(col_ref.clone().ge(2), col_ref.clone().le(10)),
                "AND(Column(x) >= 2, Column(x) <= 10)",
            ),
            (
                Predicate::and_from([
                    col_ref.clone().ge(2),
                    col_ref.clone().le(10),
                    col_ref.clone().le(100),
                ]),
                "AND(Column(x) >= 2, Column(x) <= 10, Column(x) <= 100)",
            ),
            (
                Predicate::or(col_ref.clone().gt(2), col_ref.clone().lt(10)),
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
