use crate::arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use crate::expressions::{
    Expression, OpaqueExpressionOp, OpaquePredicateOp, Predicate, Scalar, ScalarExpressionEvaluator,
};
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::schema::DataType;
use crate::{DeltaResult, DynPartialEq};

/// An arrow-enhanced opaque expression op that supports full expression evaluation over arrow data.
///
/// NOTE: This trait includes all methods of [`OpaqueExpressionOp`], but intentionally does not
/// implement it. This is to prevent accidentally creating an [`Expression`] directly from an object
/// that implements this trait. Doing so would "clip" it [`&dyn OpaqueExpressionOp`], and we would
/// not be able to recover a [`&dyn ArrowOpaqueExpressionOp`] from it later. Instead, we use
/// [`ArrowOpaqueExpression::arrow_opaque`] to safely convert it to an [`OpaqueExpressionOp`].
pub trait ArrowOpaqueExpressionOp: DynPartialEq + std::fmt::Debug {
    /// Evaluates this expression over the provided input expressions, returning `Err` in case of an
    /// incorrect or unsupported invocation (e.g. wrong number and/or types of arguments.
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef>;

    /// See [`OpaqueExpressionOp::name`].
    fn name(&self) -> &str;

    /// See [`OpaqueExpressionOp::eval_expr_scalar`].
    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar>;
}

/// An arrow-enhanced opaque predicate op that supports full predicate evaluation over arrow data.
///
/// NOTE: This trait includes all methods of [`OpaquePredicateOp`], but intentionally does not
/// implement it. This is to prevent accidentally creating an [`Predicate`] directly from an object
/// that implements this trait. Doing so would "clip" it [`&dyn OpaquePredicateOp`], and we would
/// not be able to recover a [`&dyn ArrowOpaquePredicateOp`] from it later. Instead, we use
/// [`ArrowOpaquePredicate::arrow_opaque`] to safely convert it to an [`OpaquePredicateOp`].
pub trait ArrowOpaquePredicateOp: DynPartialEq + std::fmt::Debug {
    /// Evaluates this (possibly inverted) predicate over the provided input args, returning `Err`
    /// in case of an incorrect or unsupported invocation (e.g. wrong number and/or types of
    /// arguments.
    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray>;

    /// See [`OpaquePredicateOp::name`].
    fn name(&self) -> &str;

    /// See [`OpaquePredicateOp::eval_pred_scalar`].
    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>>;

    /// See [`OpaquePredicateOp::eval_as_data_skipping_predicate`].
    fn eval_as_data_skipping_predicate(
        &self,
        predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool>;

    /// See [`OpaquePredicateOp::as_data_skipping_predicate`].
    fn as_data_skipping_predicate(
        &self,
        predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate>;
}

/// Extension trait for turning [`ArrowOpaqueExpressionOp`] into an [`Expression`].
pub trait ArrowOpaqueExpression {
    /// Creates a new opaque expression. See also [`Expression::opaque`].
    fn arrow_opaque(
        op: impl ArrowOpaqueExpressionOp,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Expression;
}

impl ArrowOpaqueExpression for Expression {
    fn arrow_opaque(
        op: impl ArrowOpaqueExpressionOp,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Expression {
        Expression::opaque(ArrowOpaqueExpressionOpAdaptor(Box::new(op)), exprs)
    }
}

/// Extension trait for safely turning [`ArrowOpaquePredicateOp`] into an opaque [`Predicate`].
pub trait ArrowOpaquePredicate {
    /// Creates a new opaque predicate. See also [`Predicate::opaque`].
    fn arrow_opaque<T: ArrowOpaquePredicateOp>(
        op: T,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Predicate;
}

impl ArrowOpaquePredicate for Predicate {
    fn arrow_opaque<T: ArrowOpaquePredicateOp>(
        op: T,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Predicate {
        Predicate::opaque(ArrowOpaquePredicateOpAdaptor(Box::new(op)), exprs)
    }
}

/// Adaptor that implements [`OpaquePredicateOp`] on behalf of all [`ArrowOpaqueExpressionOp`], and
/// also acts as an intermediary for downcasting so that arrow evaluation can actually use it.
///
/// This extra layer of indirection is needed because Rust does not support trait downcasting: If we
/// allowed `ArrowOpaqueExpressionOp: OpaqueExpressionOp`, to decay to `&dyn OpaqueExpressionOp`,
/// there would be no way to recover a &dyn ArrowOpaqueExpressionOp` from it. Instead, we must
/// downcast from `&dyn OpaqueExpressionOp` to the concrete `ArrowOpaqueExpressionOpAdaptor` type
/// that implements both traits, and extract its inner `dyn ArrowOpaqueExpressionOp`.
#[derive(Debug)]
pub(crate) struct ArrowOpaqueExpressionOpAdaptor(Box<dyn ArrowOpaqueExpressionOp>);

impl std::ops::Deref for ArrowOpaqueExpressionOpAdaptor {
    type Target = dyn ArrowOpaqueExpressionOp;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl PartialEq for ArrowOpaqueExpressionOpAdaptor {
    fn eq(&self, other: &Self) -> bool {
        (**self).dyn_eq(other.any_ref())
    }
}

// Forward the op's trait methods so kernel can use them
impl OpaqueExpressionOp for ArrowOpaqueExpressionOpAdaptor {
    fn name(&self) -> &str {
        (**self).name()
    }

    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        (**self).eval_expr_scalar(eval_expr, exprs)
    }
}

/// Adaptor that implements [`OpaquePredicateOp`] on behalf of all [`ArrowOpaquePredicateOp`], and
/// also acts as an intermediary for downcasting so that arrow evaluation can actually use it.
///
/// This extra layer of indirection is needed because Rust does not support trait downcasting: If we
/// allowed `ArrowOpaquePredicateOp: OpaquePredicateOp`, to decay to `&dyn OpaquePredicateOp`, there
/// would be no way to recover a &dyn ArrowOpaquePredicateOp` from it. Instead, we must downcast
/// from `&dyn OpaquePredicateOp` to the concrete `ArrowOpaquePredicateOpAdaptor` type that
/// implements both traits, and extract its inner `dyn ArrowOpaquePredicateOp`.
#[derive(Debug)]
pub(crate) struct ArrowOpaquePredicateOpAdaptor(Box<dyn ArrowOpaquePredicateOp>);

impl std::ops::Deref for ArrowOpaquePredicateOpAdaptor {
    type Target = dyn ArrowOpaquePredicateOp;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl PartialEq for ArrowOpaquePredicateOpAdaptor {
    fn eq(&self, other: &Self) -> bool {
        (**self).dyn_eq(other.any_ref())
    }
}

// Forward the op's trait methods so kernel can use them
impl OpaquePredicateOp for ArrowOpaquePredicateOpAdaptor {
    fn name(&self) -> &str {
        (**self).name()
    }

    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        (**self).eval_pred_scalar(eval_expr, eval_pred, exprs, inverted)
    }

    fn eval_as_data_skipping_predicate(
        &self,
        predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        (**self).eval_as_data_skipping_predicate(predicate_evaluator, exprs, inverted)
    }

    fn as_data_skipping_predicate(
        &self,
        predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        (**self).as_data_skipping_predicate(predicate_evaluator, exprs, inverted)
    }
}
