//! Support for kernel-driven predicate evaluation via the [`KernelPredicateEvaluator`]
//! trait. Various trait implementations are used for partition pruning, stats-based data skipping,
//! and parquet row group filtering. The evaluation is normally performed over [`Scalar`] values,
//! but data skipping "evaluation" actually produces a transformed predicate that replaces column
//! references with stats column references, which log replay will instruct the engine to evaluate.
use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
    Expression as Expr, JunctionPredicate, JunctionPredicateOp, OpaqueExpression,
    OpaqueExpressionOpRef, OpaquePredicate, OpaquePredicateOpRef, Predicate as Pred, Scalar,
    UnaryPredicate, UnaryPredicateOp,
};
use crate::schema::DataType;

use std::cmp::Ordering;
use tracing::{debug, warn};

pub(crate) mod parquet_stats_skipping;

#[cfg(test)]
mod tests;

// NOTE: When creating `&dyn Foo` for some `impl<'a> Bar<'a>`, the compiler infers `&'r dyn Foo +
// 'a` (and then elides the lifetimes because `'a: 'r`). Creating a type alias for `dyn Foo` causes
// the compiler to infer `dyn Foo + 'static` (the lifetime of the alias). Which in turn requires
// `impl Bar<'static>`, which is almost always an impossible constraint. Defining the type aliases
// below with generic lifetimes allows `&'r DynFoo<'a>` (again with `'a: 'r`). Unfortunately,
// generic lifetimes cannot be hidden, so we end up with `&DynFoo<'_>` at every use site.

/// A predicate evaluator that directly evaluates predicates, resolving column references to scalar values.
pub type DirectPredicateEvaluator<'a> = dyn KernelPredicateEvaluator<Output = bool> + 'a;

/// A data skipping predicate evaluator that directly applies data skipping, resolving column
/// references to scalar stats values such as those provided by parquet footer stats.
pub type DirectDataSkippingPredicateEvaluator<'a> =
    dyn DataSkippingPredicateEvaluator<Output = bool, ColumnStat = Scalar> + 'a;

/// A data skipping predicate evaluator that rewrites the input to a predicate that performs data
/// skipping over column stats for all referenced columns. The resulting predicate can be evaluated
/// against batches of column stats at some future point.
pub type IndirectDataSkippingPredicateEvaluator<'a> =
    dyn DataSkippingPredicateEvaluator<Output = Pred, ColumnStat = Expr> + 'a;

/// Uses kernel (not engine) logic to evaluate a predicate tree against column names that resolve as
/// scalars. Useful for testing/debugging but also serves as a reference implementation that
/// documents the expression semantics that kernel relies on for data skipping.
///
/// # Inverted expression semantics
///
/// Because inversion (`NOT` operator) has special semantics and can often be optimized away by
/// pushing it down, most methods take an `inverted` flag. That allows operations like [`Pred::Not`]
/// to simply evaluate their operand with a flipped `inverted` flag, and greatly simplifies the
/// implementations of most operators (other than those which have to directly implement NOT
/// semantics, which are unavoidably complex in that regard).
///
/// # Parameterized output type
///
/// The types involved in predicate evaluation are parameterized and implementation-specific. For
/// example, a [`DirectDataSkippingPredicateEvaluator`] directly evaluates the predicate (e.g. using
/// parquet footer stats) and returns boolean results, while
/// [`IndirectDataSkippingPredicateEvaluator`] instead transforms the input predicate to a data
/// skipping predicate that the engine can evaluated directly against Delta data skipping stats
/// during log replay. Although this approach is harder to read and reason about at first, the
/// majority of predicates can be implemented generically, which greatly reduces redundancy and
/// ensures that all flavors of predicate evaluation have the same semantics.
///
/// # NULL and error semantics
///
/// Literal NULL values almost always produce cascading changes in the predicate's structure, so we
/// represent them by `Option::None` rather than `Scalar::Null`. This allows e.g. `A < NULL` to be
/// rewritten as `NULL`, or `AND(NULL, FALSE)` to be rewritten as `FALSE`.
///
/// Almost all operations produce NULL output if any input is `NULL`. Any resolution failures also
/// produce NULL (such as missing columns or type mismatch between a column and the scalar it is
/// compared against). NULL-checking operations like `IS [NOT] NULL` and `DISTINCT` are special, and
/// rely on nullcount stats for their work (NULL/missing nullcount stats makes them output NULL).
///
/// For safety reasons, NULL-checking operations only accept literal and column inputs where
/// stats-based skipping is well-defined. If an arbitrary data skipping predicate evaluates to
/// NULL, there is no way to tell whether the original predicate really evaluated to NULL (safe to
/// use), or the data skipping version evaluated to NULL due to missing stats (very unsafe to use).
///
/// NOTE: The error-handling semantics of this trait's scalar-based predicate evaluation may differ
/// from those of the engine's predicate evaluation, because kernel predicates don't include the
/// necessary type information to reliably detect all type errors.
pub trait KernelPredicateEvaluator {
    type Output;

    /// A (possibly inverted) boolean scalar value, e.g. `[NOT] <value>`.
    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) scalar NULL test, e.g. `<value> IS [NOT] NULL`.
    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) NULL check, e.g. `<expr> IS [NOT] NULL`.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) less-than comparison, e.g. `<col> < <value>`.
    fn eval_pred_lt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) greater-than comparison, e.g. `<col> > <value>`
    fn eval_pred_gt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) equality comparison, e.g. `<col> = <value>` or `<col> != <value>`.
    ///
    /// NOTE: Caller is responsible to commute the operation if needed, e.g. `<value> != <col>`
    /// becomes `<col> != <value>`.
    fn eval_pred_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) comparison between two scalars, e.g. `<valueA> != <valueB>`.
    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// A (possibly inverted) comparison between two columns, e.g. `<colA> != <colB>`.
    fn eval_pred_binary_columns(
        &self,
        op: BinaryPredicateOp,
        a: &ColumnName,
        b: &ColumnName,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Dispatches an opaque predicate.
    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Dispatches an opaque expression used as a predicate
    fn eval_pred_expr_opaque(
        &self,
        op: &OpaqueExpressionOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Completes evaluation of a (possibly inverted) junction predicate.
    ///
    /// AND and OR are implemented by first evaluating its (possibly inverted) inputs. This part is
    /// always the same, provided by [`Self::eval_pred_junction`]). The results are then combined to
    /// become the predicate's output in some implementation-defined way (this method).
    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output>;

    // ==================== PROVIDED METHODS ====================

    /// A (possibly inverted) boolean column access, e.g. `[NOT] <col>`.
    fn eval_pred_column(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output> {
        // The expression <col> is equivalent to <col> != FALSE, and the expression NOT <col> is
        // equivalent to <col> != TRUE.
        self.eval_pred_eq(col, &Scalar::from(inverted), true)
    }

    /// Dispatches a (possibly inverted) NOT predicate
    fn eval_pred_not(&self, pred: &Pred, inverted: bool) -> Option<Self::Output> {
        self.eval_pred(pred, !inverted)
    }

    /// Dispatches a (possibly inverted) boolean expression used as a predicate
    fn eval_pred_expr(&self, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        // Directly evaluate literals and and predicates used as expressions. Evaluate columns as
        // `<col> == TRUE`. All other expressions unsupported.
        match expr {
            Expr::Literal(val) => self.eval_pred_scalar(val, inverted),
            Expr::Column(col) => self.eval_pred_column(col, inverted),
            Expr::Predicate(pred) => self.eval_pred(pred, inverted),
            Expr::Opaque(OpaqueExpression { op, exprs }) => {
                self.eval_pred_expr_opaque(op, exprs, inverted)
            }
            Expr::Struct(_)
            | Expr::Transform(_)
            | Expr::Unary(_)
            | Expr::Binary(_)
            | Expr::Variadic(_)
            | Expr::Unknown(_) => None,
        }
    }

    /// Dispatches a (possibly inverted) unary expression to each operator's specific implementation.
    fn eval_pred_unary(
        &self,
        op: UnaryPredicateOp,
        expr: &Expr,
        inverted: bool,
    ) -> Option<Self::Output> {
        match op {
            UnaryPredicateOp::IsNull => match expr {
                // WARNING: Only literals and columns can be safely null-checked. Attempting to
                // null-check an expressions such as `a < 10` could wrongly produce FALSE in case
                // `a` is just plain missing (rather than known to be NULL. A missing-value can
                // arise e.g. if data skipping encounters a column with missing stats, or if
                // partition pruning encounters a non-partition column.
                Expr::Literal(val) => self.eval_pred_scalar_is_null(val, inverted),
                Expr::Column(col) => self.eval_pred_is_null(col, inverted),
                Expr::Predicate(_)
                | Expr::Struct(_)
                | Expr::Transform(_)
                | Expr::Unary(_)
                | Expr::Binary(_)
                | Expr::Variadic(_)
                | Expr::Opaque(_)
                | Expr::Unknown(_) => {
                    debug!("Unsupported operand: IS [NOT] NULL: {expr:?}");
                    None
                }
            },
        }
    }

    /// A (possibly inverted) DISTINCT test, e.g. `[NOT] DISTINCT(<col>, false)`. DISTINCT can be
    /// seen as one of two operations, depending on the input:
    ///
    /// 1. `DISTINCT(<col>, NULL)` is equivalent to `<col> IS NOT NULL`
    /// 2. `DISTINCT(<col>, <value>)` is equivalent to `OR(<col> IS NULL, <col> != <value>)`
    fn eval_pred_distinct(
        &self,
        col: &ColumnName,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        if let Scalar::Null(_) = val {
            self.eval_pred_is_null(col, !inverted)
        } else {
            let mut args = [
                self.eval_pred_is_null(col, inverted),
                self.eval_pred_eq(col, val, !inverted),
            ]
            .into_iter();
            self.finish_eval_pred_junction(JunctionPredicateOp::Or, &mut args, inverted)
        }
    }

    /// A (possibly inverted) IN-list check, e.g. `<col> [NOT] IN <array-value>`.
    ///
    /// Unsupported by default, but implementations can override it if they wish.
    fn eval_pred_in(
        &self,
        _col: &ColumnName,
        _val: &Scalar,
        _inverted: bool,
    ) -> Option<Self::Output> {
        None // TODO?
    }

    /// Dispatches a (possibly inverted) binary expression to each operator's specific implementation.
    ///
    /// NOTE: Only binary operators that produce boolean outputs are supported.
    fn eval_pred_binary(
        &self,
        op: BinaryPredicateOp,
        left: &Expr,
        right: &Expr,
        inverted: bool,
    ) -> Option<Self::Output> {
        use BinaryPredicateOp::*;
        use Expr::{Column, Literal};

        match (left, right) {
            (Column(a), Column(b)) => self.eval_pred_binary_columns(op, a, b, inverted),
            (Literal(a), Literal(b)) => self.eval_pred_binary_scalars(op, a, b, inverted),
            (Column(col), Literal(val)) => match op {
                LessThan => self.eval_pred_lt(col, val, inverted),
                GreaterThan => self.eval_pred_gt(col, val, inverted),
                Equal => self.eval_pred_eq(col, val, inverted),
                Distinct => self.eval_pred_distinct(col, val, inverted),
                In => self.eval_pred_in(col, val, inverted),
            },
            (Literal(val), Column(col)) => match op {
                // NOTE: The column has to be on the left, so e.g. `10 < x` becomes `x > 10`
                LessThan => self.eval_pred_gt(col, val, inverted),
                GreaterThan => self.eval_pred_lt(col, val, inverted),
                Equal => self.eval_pred_eq(col, val, inverted),
                Distinct => self.eval_pred_distinct(col, val, inverted),
                In => None, // arg order is semantically important
            },
            _ => {
                debug!("Unsupported binary operand(s): {left:?} {op:?} {right:?}");
                None
            }
        }
    }

    /// Dispatches a predicate junction operation (AND or OR), leveraging each implementation's
    /// [`Self::finish_eval_pred_junction`].
    fn eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &[Pred],
        inverted: bool,
    ) -> Option<Self::Output> {
        let mut preds = preds.iter().map(|pred| self.eval_pred(pred, inverted));
        self.finish_eval_pred_junction(op, &mut preds, inverted)
    }

    /// Dispatches a predicate to the specific implementation for each predicate variant.
    fn eval_pred(&self, pred: &Pred, inverted: bool) -> Option<Self::Output> {
        use Pred::*;
        match pred {
            BooleanExpression(expr) => self.eval_pred_expr(expr, inverted),
            Not(pred) => self.eval_pred_not(pred, inverted),
            Unary(UnaryPredicate { op, expr }) => self.eval_pred_unary(*op, expr, inverted),
            Binary(BinaryPredicate { op, left, right }) => {
                self.eval_pred_binary(*op, left, right, inverted)
            }
            Junction(JunctionPredicate { op, preds }) => {
                self.eval_pred_junction(*op, preds, inverted)
            }
            Opaque(OpaquePredicate { op, exprs }) => self.eval_pred_opaque(op, exprs, inverted),
            Unknown(_) => None, // not supported by definition
        }
    }

    /// Evaluates a (possibly inverted) predicate with SQL WHERE semantics.
    ///
    /// By default, [`Self::eval_pred`] behaves badly for comparisons involving NULL columns
    /// (e.g. `a < 10` when `a` is NULL), because the comparison correctly evaluates to NULL, but
    /// NULL values are interpreted as "stats missing" (= cannot skip). This ambiguity can "poison"
    /// the entire predicate, causing it to return NULL instead of FALSE that would allow skipping:
    ///
    /// ```text
    /// WHERE a < 10 -- NULL (can't skip file)
    /// WHERE a < 10 AND TRUE -- NULL (can't skip file)
    /// WHERE a < 10 OR FALSE -- NULL (can't skip file)
    /// ```
    ///
    /// Meanwhile, SQL WHERE semantics only keeps rows for which the filter evaluates to
    /// TRUE (discarding rows that evaluate to FALSE or NULL):
    ///
    /// ```text
    /// WHERE a < 10 -- NULL (discard row)
    /// WHERE a < 10 AND TRUE -- NULL (discard row)
    /// WHERE a < 10 OR FALSE -- NULL (discard row)
    /// ```
    ///
    /// Conceptually, the behavior difference between data skipping and SQL WHERE semantics can be
    /// addressed by evaluating with null-safe semantics, as if by `<expr> IS NOT NULL AND <expr>`:
    ///
    /// ```text
    /// WHERE (a < 10) IS NOT NULL AND (a < 10) -- FALSE (skip file)
    /// WHERE (a < 10 AND TRUE) IS NOT NULL AND (a < 10 AND TRUE) -- FALSE (skip file)
    /// WHERE (a < 10 OR FALSE) IS NOT NULL AND (a < 10 OR FALSE) -- FALSE (skip file)
    /// ```
    ///
    /// HOWEVER, we cannot safely NULL-check the result of an arbitrary data skipping predicate
    /// because a predicate will also produce NULL if the value is just plain missing (e.g. data
    /// skipping over a column that lacks stats), and if that NULL should propagate all the way to
    /// top-level, it would be wrongly interpreted as FALSE (= skippable).
    ///
    /// To prevent wrong data skipping, the predicate evaluator always returns NULL for a NULL check
    /// over anything except for literals and columns with known values. So we must push the NULL
    /// check down through supported operations (AND as well as null-intolerant comparisons like
    /// `<`, `!=`, etc) until it reaches columns and literals where it can do some good, e.g.:
    ///
    /// ```text
    /// WHERE a < 10 AND (b < 20 OR c < 30)
    /// ```
    ///
    /// would conceptually be interpreted as
    ///
    /// ```text
    /// WHERE
    ///   (a < 10 AND (b < 20 OR c < 30)) IS NOT NULL AND
    ///   (a < 10 AND (b < 20 OR c < 30))
    /// ```
    ///
    /// We then push the NULL check down through the top-level AND:
    ///
    /// ```text
    /// WHERE
    ///   (a < 10 IS NOT NULL AND a < 10) AND
    ///   ((b < 20 OR c < 30) IS NOT NULL AND (b < 20 OR c < 30))
    /// ```
    ///
    /// and attempt to push it further into the `a < 10` and `OR` clauses:
    ///
    /// ```text
    /// WHERE
    ///   (a IS NOT NULL AND 10 IS NOT NULL AND a < 10) AND
    ///   (b < 20 OR c < 30)
    /// ```
    ///
    /// Any time the push-down reaches an operator that does not support push-down (such as OR), we
    /// simply drop the NULL check. This way, the top-level NULL check only applies to
    /// sub-predicates that can safely implement it, while ignoring other sub-predicates. The
    /// unsupported sub-predicates could produce nulls at runtime that prevent skipping, but false
    /// positives are OK -- the query will still correctly filter out the unwanted rows that result.
    ///
    /// At predicate evaluation time, a NULL value of `a` (from our example) would evaluate as:
    ///
    /// ```text
    /// AND(..., AND(a IS NOT NULL, 10 IS NOT NULL, a < 10), ...)
    /// AND(..., AND(FALSE, TRUE, NULL), ...)
    /// AND(..., FALSE, ...)
    /// FALSE
    /// ```
    ///
    /// While a non-NULL value of `a` would instead evaluate as:
    ///
    /// ```text
    /// AND(..., AND(a IS NOT NULL, 10 IS NOT NULL, a < 10), ...)
    /// AND(..., AND(TRUE, TRUE, <result>), ...)
    /// AND(..., <result>, ...)
    /// ```
    ///
    /// And a missing value for `a` would safely disable the clause:
    ///
    /// ```text
    /// AND(..., AND(a IS NOT NULL, 10 IS NOT NULL, a < 10), ...)
    /// AND(..., AND(NULL, TRUE, NULL), ...)
    /// AND(..., NULL, ...)
    /// ```
    ///
    /// WARNING: Not an idempotent transform. If data skipping eval produces a sql predicate,
    /// evaluating the result with sql semantics has undefined behavior.
    fn eval_pred_sql_where(&self, pred: &Pred, inverted: bool) -> Option<Self::Output> {
        use Pred::*;
        match pred {
            Junction(JunctionPredicate { op, preds }) => {
                // Recursively invoke `eval_pred_sql_where` instead of the usual `eval_pred` for AND/OR.
                let mut preds = preds
                    .iter()
                    .map(|pred| self.eval_pred_sql_where(pred, inverted));
                self.finish_eval_pred_junction(*op, &mut preds, inverted)
            }
            Binary(BinaryPredicate { op, left, right }) if op.is_null_intolerant() => {
                // Perform a nullsafe comparison instead of the usual `eval_pred_binary`
                let mut preds = [
                    self.eval_pred_unary(UnaryPredicateOp::IsNull, left, true),
                    self.eval_pred_unary(UnaryPredicateOp::IsNull, right, true),
                    self.eval_pred_binary(*op, left, right, inverted),
                ]
                .into_iter();
                self.finish_eval_pred_junction(JunctionPredicateOp::And, &mut preds, false)
            }
            Not(pred) => self.eval_pred_sql_where(pred, !inverted),
            BooleanExpression(Expr::Column(col)) => {
                // Perform a nullsafe comparison instead of the usual `eval_pred_column`
                let mut preds = [
                    self.eval_pred_is_null(col, true),
                    self.eval_pred_column(col, inverted),
                ]
                .into_iter();
                self.finish_eval_pred_junction(JunctionPredicateOp::And, &mut preds, false)
            }
            BooleanExpression(Expr::Literal(val)) if val.is_null() => {
                // AND(NULL IS NOT NULL, NULL) = AND(FALSE, NULL) = FALSE
                self.eval_pred_scalar(&Scalar::from(false), false)
            }
            BooleanExpression(Expr::Predicate(pred)) => self.eval_pred_sql_where(pred, inverted),
            // Process all remaining predicates normally, because they are not proven safe. Indeed,
            // predicates like DISTINCT and IS [NOT] NULL are known-unsafe under SQL semantics:
            //
            // ```
            // x IS NULL    # when x really is NULL
            // = AND(x IS NOT NULL, x IS NULL)
            // = AND(FALSE, TRUE)
            // = FALSE
            //
            // DISTINCT(x, 10)  # when x is NULL
            // = AND(x IS NOT NULL, 10 IS NOT NULL, DISTINCT(x, 10))
            // = AND(FALSE, TRUE, TRUE)
            // = FALSE
            //
            // DISTINCT(x, NULL) # when x is not NULL
            // = AND(x IS NOT NULL, NULL IS NOT NULL, DISTINCT(x, NULL))
            // = AND(TRUE, FALSE, TRUE)
            // = FALSE
            // ```
            //
            _ => self.eval_pred(pred, inverted),
        }
    }

    /// A convenient non-inverted wrapper for [`Self::eval_pred`]
    #[allow(unused)]
    fn eval(&self, pred: &Pred) -> Option<Self::Output> {
        self.eval_pred(pred, false)
    }

    /// A convenient non-inverted wrapper for [`Self::eval_pred_sql_where`].
    fn eval_sql_where(&self, pred: &Pred) -> Option<Self::Output> {
        self.eval_pred_sql_where(pred, false)
    }
}

/// A collection of provided methods from the [`KernelPredicateEvaluator`] trait, factored out to allow
/// reuse by multiple bool-output predicate evaluator implementations.
pub struct KernelPredicateEvaluatorDefaults;
impl KernelPredicateEvaluatorDefaults {
    /// Directly evaluates a boolean scalar. See [`KernelPredicateEvaluator::eval_pred_scalar`].
    pub fn eval_pred_scalar(val: &Scalar, inverted: bool) -> Option<bool> {
        match val {
            Scalar::Boolean(val) => Some(*val != inverted),
            _ => None,
        }
    }

    /// Directly null-tests a scalar. See [`KernelPredicateEvaluator::eval_pred_scalar_is_null`].
    pub fn eval_pred_scalar_is_null(val: &Scalar, inverted: bool) -> Option<bool> {
        Some(val.is_null() != inverted)
    }

    /// A (possibly inverted) partial comparison of two scalars, leveraging the [`PartialOrd`]
    /// trait.
    pub fn partial_cmp_scalars(
        ord: Ordering,
        a: &Scalar,
        b: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        let cmp = a.partial_cmp(b)?;
        let matched = cmp == ord;
        Some(matched != inverted)
    }

    /// Directly evaluates a boolean comparison. See [`KernelPredicateEvaluator::eval_pred_binary_scalars`].
    pub fn eval_pred_binary_scalars(
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryPredicateOp::*;
        match op {
            Equal => Self::partial_cmp_scalars(Ordering::Equal, left, right, inverted),
            LessThan => Self::partial_cmp_scalars(Ordering::Less, left, right, inverted),
            GreaterThan => Self::partial_cmp_scalars(Ordering::Greater, left, right, inverted),
            Distinct | In => {
                debug!("Unsupported binary operator: {left:?} {op:?} {right:?}");
                None
            }
        }
    }

    /// Finishes evaluating a (possibly inverted) junction operation. See
    /// [`KernelPredicateEvaluator::finish_eval_pred_junction`].
    ///
    /// The inputs were already inverted by the caller, if needed.
    ///
    /// With AND (OR), any FALSE (TRUE) input dominates, forcing a FALSE (TRUE) output.  If there
    /// was no dominating input, then any NULL input forces NULL output.  Otherwise, return the
    /// non-dominant value. Inverting the operation also inverts the dominant value.
    pub fn finish_eval_pred_junction(
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        let dominator = match op {
            JunctionPredicateOp::And => inverted,
            JunctionPredicateOp::Or => !inverted,
        };
        let mut found_null = false;
        for val in preds {
            match val {
                Some(val) if val == dominator => return Some(dominator), // short circuit!
                None => found_null = true,
                Some(_) => (), // ignore non-dominant values
            }
        }
        (!found_null).then_some(!dominator)
    }
}

/// Resolves columns as scalars, as a building block for [`DefaultKernelPredicateEvaluator`].
pub(crate) trait ResolveColumnAsScalar {
    fn resolve_column(&self, col: &ColumnName) -> Option<Scalar>;
}

// Some tests do not actually require column resolution
#[cfg(test)]
pub(crate) struct UnimplementedColumnResolver;
#[cfg(test)]
impl ResolveColumnAsScalar for UnimplementedColumnResolver {
    fn resolve_column(&self, _col: &ColumnName) -> Option<Scalar> {
        unimplemented!()
    }
}

// Used internally and by some tests
pub(crate) struct EmptyColumnResolver;
impl ResolveColumnAsScalar for EmptyColumnResolver {
    fn resolve_column(&self, _col: &ColumnName) -> Option<Scalar> {
        None
    }
}

impl ResolveColumnAsScalar for std::collections::HashMap<ColumnName, Scalar> {
    fn resolve_column(&self, col: &ColumnName) -> Option<Scalar> {
        self.get(col).cloned()
    }
}

/// A predicate evaluator that directly evaluates the predicate to produce an `Option<bool>`
/// result. Column resolution is handled by an embedded [`ResolveColumnAsScalar`] instance.
pub(crate) struct DefaultKernelPredicateEvaluator<R: ResolveColumnAsScalar> {
    resolver: R,
}
impl<R: ResolveColumnAsScalar> DefaultKernelPredicateEvaluator<R> {
    // Convenient thin wrapper
    fn resolve_column(&self, col: &ColumnName) -> Option<Scalar> {
        self.resolver.resolve_column(col)
    }

    pub(crate) fn eval_expr(&self, expr: &Expr) -> Option<Scalar> {
        match expr {
            Expr::Literal(value) => Some(value.clone()),
            Expr::Column(name) => self.resolve_column(name),
            Expr::Predicate(pred) => self.eval_pred(pred, false).map(Scalar::from),
            Expr::Struct(_) | Expr::Transform(_) | Expr::Unary(_) => None, // TODO?
            Expr::Binary(BinaryExpression { op, left, right }) => {
                let op_fn = match op {
                    BinaryExpressionOp::Plus => Scalar::try_add,
                    BinaryExpressionOp::Minus => Scalar::try_sub,
                    BinaryExpressionOp::Multiply => Scalar::try_mul,
                    BinaryExpressionOp::Divide => Scalar::try_div,
                };
                op_fn(&self.eval_expr(left)?, &self.eval_expr(right)?)
            }
            Expr::Variadic(_) => None, // TODO
            Expr::Opaque(OpaqueExpression { op, exprs }) => op
                .eval_expr_scalar(&|expr| self.eval_expr(expr), exprs)
                .inspect_err(|err| {
                    warn!("Failed to evaluate {:?}: {err:?}", op.as_ref());
                })
                .ok(),
            Expr::Unknown(_) => None,
        }
    }
}

impl<R: ResolveColumnAsScalar + 'static> From<R> for DefaultKernelPredicateEvaluator<R> {
    fn from(resolver: R) -> Self {
        Self { resolver }
    }
}

/// A "normal" predicate evaluator. It takes expressions as input, uses a [`ResolveColumnAsScalar`]
/// to convert column references to scalars, and evaluates the resulting constant expression to
/// produce a boolean output.
impl<R: ResolveColumnAsScalar> KernelPredicateEvaluator for DefaultKernelPredicateEvaluator<R> {
    type Output = bool;

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted)
    }

    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_pred_scalar_is_null(&col, inverted)
    }

    fn eval_pred_lt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_pred_binary_scalars(BinaryPredicateOp::LessThan, &col, val, inverted)
    }

    fn eval_pred_gt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_pred_binary_scalars(BinaryPredicateOp::GreaterThan, &col, val, inverted)
    }

    fn eval_pred_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_pred_binary_scalars(BinaryPredicateOp::Equal, &col, val, inverted)
    }

    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
    }

    fn eval_pred_binary_columns(
        &self,
        op: BinaryPredicateOp,
        left: &ColumnName,
        right: &ColumnName,
        inverted: bool,
    ) -> Option<bool> {
        let left = self.resolve_column(left)?;
        let right = self.resolve_column(right)?;
        self.eval_pred_binary_scalars(op, &left, &right, inverted)
    }

    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<bool> {
        op.eval_pred_scalar(&|expr| self.eval_expr(expr), self, exprs, inverted)
            .unwrap_or_else(|err| {
                warn!("Unable to evaluate {:?}: {err:?}", op.as_ref());
                None
            })
    }

    fn eval_pred_expr_opaque(
        &self,
        op: &OpaqueExpressionOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<bool> {
        match op.eval_expr_scalar(&|expr| self.eval_expr(expr), exprs) {
            Ok(Scalar::Boolean(val)) => Some(val != inverted),
            Ok(Scalar::Null(DataType::BOOLEAN)) => None,
            Ok(other) => {
                warn!(
                    "Expected {:?} to produce a boolean value, but got {:?}",
                    op.as_ref(),
                    other.data_type()
                );
                None
            }
            Err(err) => {
                warn!("Unable to evaluate {:?}: {err:?}", op.as_ref());
                None
            }
        }
    }

    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::finish_eval_pred_junction(op, preds, inverted)
    }
}

/// A predicate evaluator that implements data skipping semantics over various column stats. For
/// example, comparisons involving a column are converted into comparisons over that column's
/// min/max stats, and NULL checks are converted into comparisons involving the column's nullcount
/// and rowcount stats.
pub trait DataSkippingPredicateEvaluator {
    /// The output type produced by this predicate evaluator
    type Output;
    /// The type for column stats consumed by this predicate evaluator
    type ColumnStat;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Self::ColumnStat>;

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Self::ColumnStat>;

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Self::ColumnStat>;

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat(&self) -> Option<Self::ColumnStat>;

    /// See [`KernelPredicateEvaluator::eval_pred_scalar`]
    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// See [`KernelPredicateEvaluator::eval_pred_scalar_is_null`]
    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// For IS NULL (IS NOT NULL), we can only skip the file if all-null (no-null). Any other
    /// nullcount always forces us to keep the file.
    ///
    /// NOTE: When deletion vectors are enabled, they could produce a file that is logically
    /// all-null or logically no-null, even tho the physical stats indicate a mix of null and
    /// non-null values. They cannot invalidate a file's physical all-null or non-null status,
    /// however, so the worst that can happen is we fail to skip an unnecessary file.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output>;

    /// See [`KernelPredicateEvaluator::eval_pred_binary_scalars`]
    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// See [`KernelPredicateEvaluator::eval_pred_opaque`].
    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Self::Output>;

    /// See [`KernelPredicateEvaluator::finish_eval_pred_junction`]
    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Helper method that performs a (possibly inverted) partial comparison between a typed column
    /// stat and a scalar.
    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Self::ColumnStat,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Performs a partial comparison against a column min-stat. See
    /// [`KernelPredicateEvaluatorDefaults::partial_cmp_scalars`] for details of the comparison semantics.
    fn partial_cmp_min_stat(
        &self,
        col: &ColumnName,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Self::Output> {
        let min = self.get_min_stat(col, &val.data_type())?;
        self.eval_partial_cmp(ord, min, val, inverted)
    }

    /// Performs a partial comparison against a column max-stat. See
    /// [`KernelPredicateEvaluatorDefaults::partial_cmp_scalars`] for details of the comparison semantics.
    fn partial_cmp_max_stat(
        &self,
        col: &ColumnName,
        val: &Scalar,
        ord: Ordering,
        inverted: bool,
    ) -> Option<Self::Output> {
        let max = self.get_max_stat(col, &val.data_type())?;
        self.eval_partial_cmp(ord, max, val, inverted)
    }

    /// See [`KernelPredicateEvaluator::eval_pred_lt`]
    fn eval_pred_lt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        if inverted {
            // Given `col >= val`:
            // Skip if `val is greater than _every_ value in [min, max], implies
            // Skip if `val > min AND val > max` implies
            // Skip if `val > max` implies
            // Keep if `NOT(val > max)` implies
            // Keep if `NOT(max < val)`
            self.partial_cmp_max_stat(col, val, Ordering::Less, true)
        } else {
            // Given `col < val`:
            // Skip if `val` is not greater than _all_ values in [min, max], implies
            // Skip if `val <= min AND val <= max` implies
            // Skip if `val <= min` implies
            // Keep if `NOT(val <= min)` implies
            // Keep if `val > min` implies
            // Keep if `min < val`
            self.partial_cmp_min_stat(col, val, Ordering::Less, false)
        }
    }

    /// See [`KernelPredicateEvaluator::eval_pred_gt`]
    fn eval_pred_gt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        if inverted {
            // Given `col <= val`:
            // Skip if `val` is less than _all_ values in [min, max], implies
            // Skip if `val < min AND val < max` implies
            // Skip if `val < min` implies
            // Keep if `NOT(val < min)` implies
            // Keep if `NOT(min > val)`
            self.partial_cmp_min_stat(col, val, Ordering::Greater, true)
        } else {
            // Given `col > val`:
            // Skip if `val` is not less than _all_ values in [min, max], implies
            // Skip if `val >= min AND val >= max` implies
            // Skip if `val >= max` implies
            // Keep if `NOT(val >= max)` implies
            // Keep if `NOT(max <= val)` implies
            // Keep if `max > val`
            self.partial_cmp_max_stat(col, val, Ordering::Greater, false)
        }
    }

    /// See [`KernelPredicateEvaluator::eval_pred_eq`]
    fn eval_pred_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        let (op, preds) = if inverted {
            // Column could compare not-equal if min or max value differs from the literal.
            let preds = [
                self.partial_cmp_min_stat(col, val, Ordering::Equal, true),
                self.partial_cmp_max_stat(col, val, Ordering::Equal, true),
            ];
            (JunctionPredicateOp::Or, preds)
        } else {
            // Column could compare equal if its min/max values bracket the literal.
            let preds = [
                self.partial_cmp_min_stat(col, val, Ordering::Greater, true),
                self.partial_cmp_max_stat(col, val, Ordering::Less, true),
            ];
            (JunctionPredicateOp::And, preds)
        };
        self.finish_eval_pred_junction(op, &mut preds.into_iter(), false)
    }
}

impl<T: DataSkippingPredicateEvaluator + ?Sized> KernelPredicateEvaluator for T {
    type Output = T::Output;

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_pred_scalar(val, inverted)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_pred_scalar_is_null(val, inverted)
    }

    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output> {
        self.eval_pred_is_null(col, inverted)
    }

    fn eval_pred_lt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_pred_lt(col, val, inverted)
    }

    fn eval_pred_gt(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_pred_gt(col, val, inverted)
    }

    fn eval_pred_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_pred_eq(col, val, inverted)
    }

    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        self.eval_pred_binary_scalars(op, left, right, inverted)
    }

    // NOTE: We rely on the literal values to provide logical type hints. That means we cannot
    // perform column-column comparisons, because we cannot infer the logical type to use.
    fn eval_pred_binary_columns(
        &self,
        _op: BinaryPredicateOp,
        _a: &ColumnName,
        _b: &ColumnName,
        _inverted: bool,
    ) -> Option<Self::Output> {
        None
    }

    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Self::Output> {
        self.eval_pred_opaque(op, exprs, inverted)
    }

    fn eval_pred_expr_opaque(
        &self,
        _op: &OpaqueExpressionOpRef,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<Self::Output> {
        None // Unsupported
    }

    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output> {
        self.finish_eval_pred_junction(op, preds, inverted)
    }
}
