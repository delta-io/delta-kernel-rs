//! Definitions and functions to create and manipulate kernel expressions

use std::borrow::Cow;
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

#[derive(Clone, Debug, PartialEq)]
pub struct UnaryExpression {
    /// The operator.
    pub op: UnaryOperator,
    /// The expression.
    pub expr: Box<Expression>,
}
impl UnaryExpression {
    fn new(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryExpression {
    /// The operator.
    pub op: BinaryOperator,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}
impl BinaryExpression {
    fn new(op: BinaryOperator, left: impl Into<Expression>, right: impl Into<Expression>) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariadicExpression {
    /// The operator.
    pub op: VariadicOperator,
    /// The expressions.
    pub exprs: Vec<Expression>,
}
impl VariadicExpression {
    fn new(op: VariadicOperator, exprs: Vec<Expression>) -> Self {
        Self { op, exprs }
    }
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
    Unary(UnaryExpression),
    /// A binary operation.
    Binary(BinaryExpression),
    /// A variadic operation.
    Variadic(VariadicExpression),
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
            Self::Binary(BinaryExpression {
                op: BinaryOperator::Distinct,
                left,
                right,
            }) => write!(f, "DISTINCT({left}, {right})"),
            Self::Binary(BinaryExpression { op, left, right }) => write!(f, "{left} {op} {right}"),
            Self::Unary(UnaryExpression { op, expr }) => match op {
                UnaryOperator::Not => write!(f, "NOT {expr}"),
                UnaryOperator::IsNull => write!(f, "{expr} IS NULL"),
            },
            Self::Variadic(VariadicExpression { op, exprs }) => {
                let exprs = &exprs.iter().map(|e| format!("{e}")).join(", ");
                let op = match op {
                    VariadicOperator::And => "AND",
                    VariadicOperator::Or => "OR",
                };
                write!(f, "{op}({exprs})")
            }
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

    pub const fn null_literal(data_type: DataType) -> Self {
        Self::Literal(Scalar::Null(data_type))
    }

    /// Create a new struct expression
    pub fn struct_from(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::Struct(exprs.into_iter().collect())
    }

    /// Creates a new unary expression OP expr
    pub fn unary(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        Self::Unary(UnaryExpression {
            op,
            expr: Box::new(expr.into()),
        })
    }

    /// Creates a new binary expression lhs OP rhs
    pub fn binary(
        op: BinaryOperator,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryExpression {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        })
    }

    /// Creates a new variadic expression OP(exprs...)
    pub fn variadic(op: VariadicOperator, exprs: impl IntoIterator<Item = Self>) -> Self {
        let exprs = exprs.into_iter().collect::<Vec<_>>();
        Self::Variadic(VariadicExpression { op, exprs })
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
        use Expression::*;
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Literal(_) => {}
                Column { .. } => {}
                Struct(exprs) => stack.extend(exprs),
                Unary(UnaryExpression { expr, .. }) => stack.push(expr),
                Binary(BinaryExpression { left, right, .. }) => {
                    stack.push(left);
                    stack.push(right);
                }
                Variadic(VariadicExpression { exprs, .. }) => stack.extend(exprs),
            }
            Some(expr)
        })
    }
}

/// Generic framework for recursive bottom-up expression transforms. Transformations return
/// `Option<Cow>` with the following semantics:
///
/// * `Some(Cow::Owned)` -- The input was transformed and the parent should be updated with it.
/// * `Some(Cow::Borrowed)` -- The input was not transformed.
/// * `None` -- The input was filtered out and the parent should be updated to not reference it.
///
/// The transform can start from the generic [`Self::transform`], or directly from a specific
/// expression variant (e.g. [`Self::transform_binary`] to start with [`BinaryExpression`]).
///
/// The provided `transform_xxx` methods all default to no-op (returning their input as
/// `Some(Cow::Borrowed)`), and implementations should selectively override specific `transform_xxx`
/// methods as needed for the task at hand.
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// children of each expression variant. Implementations can call these as needed but will generally
/// not need to override them.
pub trait ExpressionTransform<'a> {
    /// Called for each literal encountered during the expression traversal.
    fn transform_literal(&mut self, value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
        Some(Cow::Borrowed(value))
    }

    /// Called for each column reference encountered during the expression traversal.
    fn transform_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        Some(Cow::Borrowed(name))
    }

    /// Called for the expression list of each [`Expression::Struct`] encountered during the
    /// traversal. Implementations can call [`Self::recurse_into_struct`] if they wish to
    /// recursively transform child expressions.
    fn transform_struct(
        &mut self,
        fields: &'a Vec<Expression>,
    ) -> Option<Cow<'a, Vec<Expression>>> {
        self.recurse_into_struct(fields)
    }

    /// Called for each [`UnaryExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_unary`] if they wish to recursively transform the child.
    fn transform_unary(&mut self, expr: &'a UnaryExpression) -> Option<Cow<'a, UnaryExpression>> {
        self.recurse_into_unary(expr)
    }

    /// Called for each [`BinaryExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_binary`] if they wish to recursively transform the children.
    fn transform_binary(
        &mut self,
        expr: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        self.recurse_into_binary(expr)
    }

    /// Called for each [`VariadicExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_variadic`] if they wish to recursively transform the children.
    fn transform_variadic(
        &mut self,
        expr: &'a VariadicExpression,
    ) -> Option<Cow<'a, VariadicExpression>> {
        self.recurse_into_variadic(expr)
    }

    /// General entry point for transforming an expression. This method will dispatch to the
    /// specific transform for each expression variant. Also invoked internally in order to recurse
    /// on the child(ren) of non-leaf variants.
    fn transform(&mut self, expr: &'a Expression) -> Option<Cow<'a, Expression>> {
        use Cow::*;
        let expr = match expr {
            Expression::Literal(s) => match self.transform_literal(s)? {
                Owned(s) => Owned(Expression::Literal(s)),
                Borrowed(_) => Borrowed(expr),
            },
            Expression::Column(c) => match self.transform_column(c)? {
                Owned(c) => Owned(Expression::Column(c)),
                Borrowed(_) => Borrowed(expr),
            },
            Expression::Struct(s) => match self.transform_struct(s)? {
                Owned(s) => Owned(Expression::Struct(s)),
                Borrowed(_) => Borrowed(expr),
            },
            Expression::Unary(u) => match self.transform_unary(u)? {
                Owned(u) => Owned(Expression::Unary(u)),
                Borrowed(_) => Borrowed(expr),
            },
            Expression::Binary(b) => match self.transform_binary(b)? {
                Owned(b) => Owned(Expression::Binary(b)),
                Borrowed(_) => Borrowed(expr),
            },
            Expression::Variadic(v) => match self.transform_variadic(v)? {
                Owned(v) => Owned(Expression::Variadic(v)),
                Borrowed(_) => Borrowed(expr),
            },
        };
        Some(expr)
    }

    /// Recursively transforms a struct's child expressions. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_struct(
        &mut self,
        fields: &'a Vec<Expression>,
    ) -> Option<Cow<'a, Vec<Expression>>> {
        let mut num_borrowed = 0;
        let new_fields: Vec<_> = fields
            .iter()
            .filter_map(|f| self.transform(f))
            .inspect(|f| {
                if matches!(f, Cow::Borrowed(_)) {
                    num_borrowed += 1;
                }
            })
            .collect();

        if new_fields.is_empty() {
            None // all fields filtered out
        } else if num_borrowed < fields.len() {
            // At least one field was changed or filtered out, so make a new field list
            let fields = new_fields.into_iter().map(|f| f.into_owned()).collect();
            Some(Cow::Owned(fields))
        } else {
            Some(Cow::Borrowed(fields))
        }
    }

    /// Recursively transforms a unary expression's child. Returns `None` if the child was removed,
    /// `Some(Cow::Owned)` if the child was changed, and `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_unary(&mut self, u: &'a UnaryExpression) -> Option<Cow<'a, UnaryExpression>> {
        use Cow::*;
        let u = match self.transform(&u.expr)? {
            Owned(expr) => Owned(UnaryExpression::new(u.op, expr)),
            Borrowed(_) => Borrowed(u),
        };
        Some(u)
    }

    /// Recursively transforms a binary expression's children. Returns `None` if at least one child
    /// was removed, `Some(Cow::Owned)` if at least one child changed, and `Some(Cow::Borrowed)`
    /// otherwise.
    fn recurse_into_binary(
        &mut self,
        b: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        use Cow::*;
        let left = self.transform(&b.left)?;
        let right = self.transform(&b.right)?;
        let b = match (&left, &right) {
            (Borrowed(_), Borrowed(_)) => Borrowed(b),
            _ => Owned(BinaryExpression::new(
                b.op,
                left.into_owned(),
                right.into_owned(),
            )),
        };
        Some(b)
    }

    /// Recursively transforms a variadic expression's children. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_variadic(
        &mut self,
        v: &'a VariadicExpression,
    ) -> Option<Cow<'a, VariadicExpression>> {
        use Cow::*;
        let v = match self.recurse_into_struct(&v.exprs)? {
            Owned(exprs) => Owned(VariadicExpression::new(v.op, exprs)),
            Borrowed(_) => Borrowed(v),
        };
        Some(v)
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

/// An expression "transform" that doesn't actually change the expression at all. Instead, it
/// measures the maximum depth of a expression, with a depth limit to prevent stack overflow. Useful
/// for verifying that a expression has reasonable depth before attempting to work with it.
pub struct ExpressionDepthChecker {
    depth_limit: usize,
    max_depth_seen: usize,
    current_depth: usize,
    call_count: usize,
}
impl ExpressionDepthChecker {
    /// Depth-checks the given expression against a given depth limit. The return value is the
    /// largest depth seen, which is capped at one more than the depth limit (indicating the
    /// recursion was terminated).
    pub fn check(expr: &Expression, depth_limit: usize) -> usize {
        Self::check_with_call_count(expr, depth_limit).0
    }

    // Exposed for testing
    fn check_with_call_count(expr: &Expression, depth_limit: usize) -> (usize, usize) {
        let mut checker = Self {
            depth_limit,
            max_depth_seen: 0,
            current_depth: 0,
            call_count: 0,
        };
        checker.transform(expr);
        (checker.max_depth_seen, checker.call_count)
    }

    // Triggers the requested recursion only doing so would not exceed the depth limit.
    fn depth_limited<'a, T: Clone + std::fmt::Debug>(
        &mut self,
        recurse: impl FnOnce(&mut Self, &'a T) -> Option<Cow<'a, T>>,
        arg: &'a T,
    ) -> Option<Cow<'a, T>> {
        self.call_count += 1;
        if self.max_depth_seen < self.current_depth {
            self.max_depth_seen = self.current_depth;
            if self.depth_limit < self.current_depth {
                tracing::warn!(
                    "Max expression depth {} exceeded by {arg:?}",
                    self.depth_limit
                );
            }
        }
        if self.max_depth_seen <= self.depth_limit {
            self.current_depth += 1;
            let _ = recurse(self, arg);
            self.current_depth -= 1;
        }
        None
    }
}
impl<'a> ExpressionTransform<'a> for ExpressionDepthChecker {
    fn transform_struct(
        &mut self,
        fields: &'a Vec<Expression>,
    ) -> Option<Cow<'a, Vec<Expression>>> {
        self.depth_limited(Self::recurse_into_struct, fields)
    }

    fn transform_unary(&mut self, expr: &'a UnaryExpression) -> Option<Cow<'a, UnaryExpression>> {
        self.depth_limited(Self::recurse_into_unary, expr)
    }

    fn transform_binary(
        &mut self,
        expr: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        self.depth_limited(Self::recurse_into_binary, expr)
    }

    fn transform_variadic(
        &mut self,
        expr: &'a VariadicExpression,
    ) -> Option<Cow<'a, VariadicExpression>> {
        self.depth_limited(Self::recurse_into_variadic, expr)
    }
}

#[cfg(test)]
mod tests {
    use super::{column_expr, Expression as Expr, ExpressionDepthChecker};
    use std::ops::Not;

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

    #[test]
    fn test_depth_checker() {
        let expr = Expr::and_from([
            Expr::struct_from([
                Expr::and_from([
                    Expr::lt(Expr::literal(10), column_expr!("x")),
                    Expr::or_from([Expr::literal(true), column_expr!("b")]),
                ]),
                Expr::literal(true),
                Expr::not(Expr::literal(true)),
            ]),
            Expr::and_from([
                Expr::not(column_expr!("b")),
                Expr::gt(Expr::literal(10), column_expr!("x")),
                Expr::or_from([
                    Expr::and_from([Expr::not(Expr::literal(true)), Expr::literal(10)]),
                    Expr::literal(10),
                ]),
                Expr::literal(true),
            ]),
            Expr::ne(
                Expr::literal(true),
                Expr::and_from([Expr::literal(true), column_expr!("b")]),
            ),
        ]);

        // Similar to ExpressionDepthChecker::check, but also returns call count
        let check_with_call_count =
            |depth_limit| ExpressionDepthChecker::check_with_call_count(&expr, depth_limit);

        // NOTE: The checker ignores leaf nodes!

        // AND
        //  * STRUCT
        //    * AND     >LIMIT<
        //    * NOT
        //  * AND
        //  * NE
        assert_eq!(check_with_call_count(1), (2, 6));

        // AND
        //  * STRUCT
        //    * AND
        //      * LT     >LIMIT<
        //      * OR
        //    * NOT
        //  * AND
        //  * NE
        assert_eq!(check_with_call_count(2), (3, 8));

        // AND
        //  * STRUCT
        //    * AND
        //      * LT
        //      * OR
        //    * NOT
        //  * AND
        //    * NOT
        //    * GT
        //    * OR
        //      * AND
        //        * NOT     >LIMIT<
        //  * NE
        assert_eq!(check_with_call_count(3), (4, 13));

        // Depth limit not hit (full traversal required)

        // AND
        //  * STRUCT
        //    * AND
        //      * LT
        //      * OR
        //    * NOT
        //  * AND
        //    * NOT
        //    * GT
        //    * OR
        //      * AND
        //        * NOT
        //  * NE
        //    * AND
        assert_eq!(check_with_call_count(4), (4, 14));
        assert_eq!(check_with_call_count(5), (4, 14));
    }
}
