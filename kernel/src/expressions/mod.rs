//! Definitions and functions to create and manipulate kernel expressions

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use itertools::Itertools;

pub use self::column_names::{
    column_expr, column_name, column_pred, joined_column_expr, joined_column_name, ColumnName,
};
pub use self::scalars::{ArrayData, DecimalData, MapData, Scalar, StructData};
use self::transforms::{ExpressionTransform as _, GetColumnReferences};
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::{DataType, DeltaResult, DynPartialEq};

mod column_names;
pub(crate) mod literal_expression_transform;
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

/// A binary predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryPredicateOp {
    /// Comparison Less Than
    LessThan,
    /// Comparison Greater Than
    GreaterThan,
    /// Comparison Equal
    Equal,
    /// Distinct
    Distinct,
    /// IN
    In,
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

/// A kernel-supplied scalar expression evaluator which in particular can convert column references
/// (i.e. [`Expression::Column`]) to [`Scalar`] values. [`OpaqueExpressionOp::eval_expr_scalar`] and
/// [`OpaquePredicateOp::eval_pred_scalar`] rely on this evaluator.
///
/// If the evaluator produces `None`, it means kernel was unable to evaluate
/// the input expression. Otherwise, `Some(Scalar)` is the result of that evaluation (possibly
/// `Scalar::Null` if the output was NULL).
pub type ScalarExpressionEvaluator<'a> = dyn Fn(&Expression) -> Option<Scalar> + 'a;

/// An opaque expression operation (ie defined and implemented by the engine).
pub trait OpaqueExpressionOp: DynPartialEq + std::fmt::Debug {
    /// Succinctly identifies this op
    fn name(&self) -> &str;

    /// Attempts scalar evaluation of this opaque expression, e.g. for partition pruning.
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by
    /// calling back to the provided [`ScalarExpressionEvaluator`],
    ///
    /// An output of `Err` indicates that this operation does not support scalar evaluation, or was
    /// invoked incorrectly (e.g. with the wrong number and/or types of arguments, None input,
    /// etc); the operation is disqualified from participating in partition pruning.
    ///
    /// `Ok(Scalar::Null)` means the operation actually produced a legitimately NULL result.
    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar>;
}

/// An opaque predicate operation (ie defined and implemented by the engine).
pub trait OpaquePredicateOp: DynPartialEq + std::fmt::Debug {
    /// Succinctly identifies this op
    fn name(&self) -> &str;

    /// Attempts scalar evaluation of this (possibly inverted) opaque predicate on behalf of a
    /// [`DirectPredicateEvaluator`], e.g. for partition pruning or to evaluate an opaque data
    /// skipping predicate produced previously by an [`IndirectDataSkippingPredicateEvaluator`].
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by calling
    /// back to the provided [`ScalarExpressionEvaluator`] and/or [`DirectPredicateEvaluator`].
    ///
    /// An output of `Err` indicates that this operation does not support scalar evaluation, or was
    /// invoked incorrectly (e.g. wrong number and/or types of arguments, None input, etc); the
    /// operation is disqualified from participating in partition pruning and/or data skipping.
    ///
    /// `Ok(None)` means the operation actually produced a legitimately NULL output.
    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>>;

    /// Evaluates this (possibly inverted) opaque predicate for data skipping on behalf of a
    /// [`DirectDataSkippingPredicateEvaluator`], e.g. for parquet row group skipping.
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by
    /// calling back to the provided [`DirectDataSkippingPredicateEvaluator`].
    ///
    /// An output of `None` indicates that this operation does not support evaluation as a data
    /// skipping predicate, or was invoked incorrectly (e.g. wrong number and/or types of arguments,
    /// None input, etc.); the operation is disqualified from participating in row group skipping.
    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool>;

    /// Converts this (possibly inverted) opaque predicate to a data skipping predicate on behalf of
    /// an [`IndirectDataSkippingPredicateEvaluator`], e.g. for stats-based file pruning.
    ///
    /// Implementations can transform the predicate and its child expressions however they see fit,
    /// possibly by calling back to the owning [`IndirectDataSkippingPredicateEvaluator`].
    ///
    /// An output of `None` indicates that this operation does not support conversion to a data
    /// skipping predicate, or was invoked incorrectly (e.g. wrong number and/or types of arguments,
    /// None input, etc.); the operation is disqualified from participating in file pruning.
    //
    // NOTE: It would be nicer if this method could accept an `Arc<Self>`, in case the data skipping
    // predicate rewrite can reuse the same operation. But sadly, that would not be dyn-compatible.
    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate>;
}

/// A shared reference to an [`OpaqueExpressionOp`] instance.
pub type OpaqueExpressionOpRef = Arc<dyn OpaqueExpressionOp>;

/// A shared reference to an [`OpaquePredicateOp`] instance.
pub type OpaquePredicateOpRef = Arc<dyn OpaquePredicateOp>;

////////////////////////////////////////////////////////////////////////
// Expressions and predicates
////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub struct UnaryPredicate {
    /// The operator.
    pub op: UnaryPredicateOp,
    /// The input expression.
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
pub struct JunctionPredicate {
    /// The operator.
    pub op: JunctionPredicateOp,
    /// The input predicates.
    pub preds: Vec<Predicate>,
}

// NOTE: We have to use `Arc<dyn OpaquePredicateOp>` instead of `Box<dyn OpaquePredicateOp>` because
// we cannot require `OpaquePredicateOp: Clone` (not a dyn-compatible trait). Instead, we must rely
// on cheap `Arc` clone, which does not duplicate the inner object.
#[derive(Clone, Debug)]
pub struct OpaquePredicate {
    pub op: OpaquePredicateOpRef,
    pub exprs: Vec<Expression>,
}

impl OpaquePredicate {
    fn new(op: OpaquePredicateOpRef, exprs: impl IntoIterator<Item = Expression>) -> Self {
        let exprs = exprs.into_iter().collect();
        Self { op, exprs }
    }
}

// NOTE: We have to use `Arc<dyn OpaqueExpressionOp>` instead of `Box<dyn OpaqueExpressionOp>`
// because we cannot require `OpaqueExpressionOp: Clone` (not a dyn-compatible trait). Instead, we
// must rely on cheap `Arc` clone, which does not duplicate the inner object.
#[derive(Clone, Debug)]
pub struct OpaqueExpression {
    pub op: OpaqueExpressionOpRef,
    pub exprs: Vec<Expression>,
}

impl OpaqueExpression {
    fn new(op: OpaqueExpressionOpRef, exprs: impl IntoIterator<Item = Expression>) -> Self {
        let exprs = exprs.into_iter().collect();
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
    /// A predicate treated as a boolean expression
    Predicate(Box<Predicate>),
    /// A struct computed from a Vec of expressions
    Struct(Vec<Expression>),
    /// An expression that takes two expressions as input.
    Binary(BinaryExpression),
    /// An expression that the engine defines and implements. Kernel interacts with the expression
    /// only through methods provided by the [`OpaqueExpressionOp`] trait.
    Opaque(OpaqueExpression),
    /// An unknown expression (i.e. one that neither kernel nor engine attempts to evaluate). For
    /// data skipping purposes, kernel treats unknown expressions as if they were literal NULL
    /// values (which may disable skipping if it "poisons" the predicate), but engines MUST NOT
    /// attempt to interpret them as NULL when evaluating query filters because it could produce
    /// incorrect results. For example, converting `WHERE <fancy-udf-invocation> IS NULL` to `WHERE
    /// <unknown> IS NULL` to `WHERE NULL IS NULL` is equivalent to `WHERE TRUE` and would include
    /// all rows -- almost certainly NOT what the query author intended. Use `Expression::Opaque`
    /// for expressions kernel doesn't understand but which engine can still evaluate.
    Unknown(String),
}

/// A SQL predicate.
///
/// These predicates do not track or validate data types, other than the type
/// of literals. It is up to the predicate evaluator to validate the
/// predicate against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// A boolean-valued expression, useful for e.g. `AND(<boolean_col1>, <boolean_col2>)`.
    BooleanExpression(Expression),
    /// Boolean inversion (true <-> false)
    ///
    /// NOTE: NOT is not a normal unary predicate, because it requires a predicate as input (not an
    /// expression), and is never directly evaluated. Instead, observing that all predicates are
    /// invertible, NOT is always pushed down into its child predicate, inverting it. For example,
    /// `NOT (a < b)` pushes down and inverts `<` to `>=`, producing `a >= b`.
    Not(Box<Predicate>),
    /// A unary operation.
    Unary(UnaryPredicate),
    /// A binary operation.
    Binary(BinaryPredicate),
    /// A junction operation (AND/OR).
    Junction(JunctionPredicate),
    /// A predicate that the engine defines and implements. Kernel interacts with the predicate
    /// only through methods provided by the [`OpaquePredicateOp`] trait.
    Opaque(OpaquePredicate),
    /// An unknown predicate (i.e. one that neither kernel nor engine attempts to evaluate). For
    /// data skipping purposes, kernel treats unknown predicates as if they were literal NULL values
    /// (which may disable skipping if it "poisons" the predicate), but engines MUST NOT attempt to
    /// interpret them as NULL when evaluating query filters because it could produce incorrect
    /// results. For example, converting `WHERE <fancy-udf-invocation>` to `WHERE NULL` is
    /// equivalent to `WHERE FALSE` and would filter out all rows -- almost certainly NOT what the
    /// query author intended. Use `Predicate::Opaque` for predicates kernel doesn't understand
    /// but which engine can still evaluate.
    Unknown(String),
}

////////////////////////////////////////////////////////////////////////
// Struct/Enum impls
////////////////////////////////////////////////////////////////////////

impl BinaryPredicateOp {
    /// True if this is a comparison for which NULL input always produces NULL output
    pub(crate) fn is_null_intolerant(&self) -> bool {
        use BinaryPredicateOp::*;
        match self {
            LessThan | GreaterThan | Equal => true,
            Distinct | In => false, // tolerates NULL input
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
    pub fn from_pred(value: Predicate) -> Self {
        match value {
            Predicate::BooleanExpression(expr) => expr,
            _ => Self::Predicate(Box::new(value)),
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

    /// Creates a new opaque expression
    pub fn opaque(
        op: impl OpaqueExpressionOp,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        Self::Opaque(OpaqueExpression::new(Arc::new(op), exprs))
    }

    /// Creates a new unknown expression
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::Unknown(name.into())
    }
}

impl Predicate {
    /// Returns a set of columns referenced by this predicate.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        let _ = references.transform_pred(self);
        references.into_inner()
    }

    /// Creates a new boolean column reference. See also [`Expression::column`].
    pub fn column<A>(field_names: impl IntoIterator<Item = A>) -> Predicate
    where
        ColumnName: FromIterator<A>,
    {
        Self::from_expr(ColumnName::new(field_names))
    }

    /// Create a new literal boolean value
    pub const fn literal(value: bool) -> Self {
        Self::BooleanExpression(Expression::Literal(Scalar::Boolean(value)))
    }

    /// Creates a NULL literal boolean value
    pub const fn null_literal() -> Self {
        Self::BooleanExpression(Expression::Literal(Scalar::Null(DataType::BOOLEAN)))
    }

    /// Converts a boolean-valued expression into a predicate
    pub fn from_expr(expr: impl Into<Expression>) -> Self {
        match expr.into() {
            Expression::Predicate(p) => *p,
            expr => Predicate::BooleanExpression(expr),
        }
    }

    /// Logical NOT (boolean inversion)
    pub fn not(pred: impl Into<Self>) -> Self {
        Self::Not(Box::new(pred.into()))
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
        Self::not(Self::binary(BinaryPredicateOp::Equal, a, b))
    }

    /// Create a new predicate `self <= other`
    pub fn le(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::GreaterThan, a, b))
    }

    /// Create a new predicate `self < other`
    pub fn lt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::LessThan, a, b)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::LessThan, a, b))
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

    /// Creates a new opaque predicate
    pub fn opaque(op: impl OpaquePredicateOp, exprs: impl IntoIterator<Item = Expression>) -> Self {
        Self::Opaque(OpaquePredicate::new(Arc::new(op), exprs))
    }

    /// Creates a new unknown predicate
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::Unknown(name.into())
    }
}

////////////////////////////////////////////////////////////////////////
// Trait impls
////////////////////////////////////////////////////////////////////////

impl PartialEq for OpaquePredicate {
    fn eq(&self, other: &Self) -> bool {
        self.op.dyn_eq(other.op.any_ref()) && self.exprs == other.exprs
    }
}

impl PartialEq for OpaqueExpression {
    fn eq(&self, other: &Self) -> bool {
        self.op.dyn_eq(other.op.any_ref()) && self.exprs == other.exprs
    }
}

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
            GreaterThan => write!(f, ">"),
            Equal => write!(f, "="),
            // TODO(roeap): AFAIK DISTINCT does not have a commonly used operator symbol
            // so ideally this would not be used as we use Display for rendering expressions
            // in our code we take care of this, but theirs might not ...
            Distinct => write!(f, "DISTINCT"),
            In => write!(f, "IN"),
        }
    }
}

// Helper for displaying the children of variadic expressions and predicates
fn format_child_list<T: Display>(children: &[T]) -> String {
    children.iter().map(|c| format!("{c}")).join(", ")
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        match self {
            Literal(l) => write!(f, "{l}"),
            Column(name) => write!(f, "Column({name})"),
            Predicate(p) => write!(f, "{p}"),
            Struct(exprs) => write!(f, "Struct({})", format_child_list(exprs)),
            Binary(BinaryExpression { op, left, right }) => write!(f, "{left} {op} {right}"),
            Opaque(OpaqueExpression { op, exprs }) => {
                write!(f, "{op:?}({})", format_child_list(exprs))
            }
            Unknown(name) => write!(f, "<unknown: {name}>"),
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
                let op = match op {
                    JunctionPredicateOp::And => "AND",
                    JunctionPredicateOp::Or => "OR",
                };
                write!(f, "{op}({})", format_child_list(preds))
            }
            Opaque(OpaquePredicate { op, exprs }) => {
                write!(f, "{op:?}({})", format_child_list(exprs))
            }
            Unknown(name) => write!(f, "<unknown: {name}>"),
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

impl From<Predicate> for Expression {
    fn from(value: Predicate) -> Self {
        Self::from_pred(value)
    }
}

impl From<ColumnName> for Predicate {
    fn from(value: ColumnName) -> Self {
        Self::from_expr(value)
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
            let result = format!("{expr}");
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
                "AND(NOT(Column(x) < 2), NOT(Column(x) > 10))",
            ),
            (
                Pred::and_from([
                    column_expr!("x").ge(Expr::literal(2)),
                    column_expr!("x").le(Expr::literal(10)),
                    column_expr!("x").le(Expr::literal(100)),
                ]),
                "AND(NOT(Column(x) < 2), NOT(Column(x) > 10), NOT(Column(x) > 100))",
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
            let result = format!("{pred}");
            assert_eq!(result, expected);
        }
    }
}
