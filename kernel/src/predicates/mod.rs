use crate::expressions::{
    BinaryExpression, BinaryOperator, ColumnName, Expression as Expr, Scalar, UnaryExpression,
    UnaryOperator, VariadicExpression, VariadicOperator,
};
use crate::schema::DataType;

use std::cmp::Ordering;
use tracing::debug;

#[cfg(test)]
mod tests;

/// Evaluates a predicate expression tree against column names that resolve as scalars. Useful for
/// testing/debugging but also serves as a reference implementation that documents the expression
/// semantics that kernel relies on for data skipping.
///
/// # Inverted expression semantics
///
/// Because inversion (`NOT` operator) has special semantics and can often be optimized away by
/// pushing it down, most methods take an `inverted` flag. That allows operations like
/// [`UnaryOperator::Not`] to simply evaluate their operand with a flipped `inverted` flag,
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
/// stats-based skipping is well-defined. If an arbitrary data skipping expression evaluates to
/// NULL, there is no way to tell whether the original expression really evaluated to NULL (safe to
/// use), or the data skipping version evaluated to NULL due to missing stats (very unsafe to use).
///
/// NOTE: The error-handling semantics of this trait's scalar-based predicate evaluation may differ
/// from those of the engine's expression evaluation, because kernel expressions don't include the
/// necessary type information to reliably detect all type errors.
pub(crate) trait PredicateEvaluator {
    type Output;

    /// A (possibly inverted) boolean scalar value, e.g. `[NOT] <value>`.
    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) NULL check, e.g. `<expr> IS [NOT] NULL`.
    fn eval_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output>;

    /// A less-than comparison, e.g. `<col> < <value>`.
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> < <col>)` becomes `<col> <= <value>`.
    fn eval_lt(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output>;

    /// A less-than-or-equal comparison, e.g. `<col> <= <value>`
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> <= <col>)` becomes `<col> < <value>`.
    fn eval_le(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output>;

    /// A greater-than comparison, e.g. `<col> > <value>`
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> > <col>)` becomes `<col> >= <value>`.
    fn eval_gt(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output>;

    /// A greater-than-or-equal comparison, e.g. `<col> >= <value>`
    ///
    /// NOTE: Caller is responsible to commute and/or invert the operation if needed,
    /// e.g. `NOT(<value> >= <col>)` becomes `<col> > <value>`.
    fn eval_ge(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output>;

    /// A (possibly inverted) equality comparison, e.g. `<col> = <value>` or `<col> != <value>`.
    ///
    /// NOTE: Caller is responsible to commute the operation if needed, e.g. `<value> != <col>`
    /// becomes `<col> != <value>`.
    fn eval_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// A (possibly inverted) comparison between two scalars, e.g. `<valueA> != <valueB>`.
    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// A (possibly inverted) comparison between two columns, e.g. `<colA> != <colB>`.
    fn eval_binary_columns(
        &self,
        op: BinaryOperator,
        a: &ColumnName,
        b: &ColumnName,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Completes evaluation of a (possibly inverted) variadic expression.
    ///
    /// AND and OR are implemented by first evaluating its (possibly inverted) inputs. This part is
    /// always the same, provided by [`eval_variadic`]). The results are then combined to become the
    /// expression's output in some implementation-defined way (this method).
    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output>;

    // ==================== PROVIDED METHODS ====================

    /// A (possibly inverted) boolean column access, e.g. `[NOT] <col>`.
    fn eval_column(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output> {
        // The expression <col> is equivalent to <col> != FALSE, and the expression NOT <col> is
        // equivalent to <col> != TRUE.
        self.eval_eq(col, &Scalar::from(inverted), true)
    }

    /// Dispatches a (possibly inverted) unary expression to each operator's specific implementation.
    fn eval_unary(&self, op: UnaryOperator, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        match op {
            UnaryOperator::Not => self.eval_expr(expr, !inverted),
            UnaryOperator::IsNull => {
                // Data skipping only supports IS [NOT] NULL over columns (not expressions)
                let Expr::Column(col) = expr else {
                    debug!("Unsupported operand: IS [NOT] NULL: {expr:?}");
                    return None;
                };
                self.eval_is_null(col, inverted)
            }
        }
    }

    /// A (possibly inverted) DISTINCT test, e.g. `[NOT] DISTINCT(<col>, false)`. DISTINCT can be
    /// seen as one of two operations, depending on the input:
    ///
    /// 1. DISTINCT(<col>, NULL) is equivalent to `<col> IS NOT NULL`
    /// 2. DISTINCT(<col>, <value>) is equivalent to `OR(<col> IS NULL, <col> != <value>)`
    fn eval_distinct(
        &self,
        col: &ColumnName,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        if let Scalar::Null(_) = val {
            self.eval_is_null(col, !inverted)
        } else {
            let args = [
                self.eval_is_null(col, inverted),
                self.eval_eq(col, val, !inverted),
            ];
            self.finish_eval_variadic(VariadicOperator::Or, args, inverted)
        }
    }

    /// A (possibly inverted) IN-list check, e.g. `<col> [NOT] IN <array-value>`.
    ///
    /// Unsupported by default, but implementations can override it if they wish.
    fn eval_in(&self, _col: &ColumnName, _val: &Scalar, _inverted: bool) -> Option<Self::Output> {
        None // TODO?
    }

    /// Dispatches a (possibly inverted) binary expression to each operator's specific implementation.
    ///
    /// NOTE: Only binary operators that produce boolean outputs are supported.
    fn eval_binary(
        &self,
        op: BinaryOperator,
        left: &Expr,
        right: &Expr,
        inverted: bool,
    ) -> Option<Self::Output> {
        use BinaryOperator::*;
        use Expr::{Column, Literal};

        // NOTE: We rely on the literal values to provide logical type hints. That means we cannot
        // perform column-column comparisons, because we cannot infer the logical type to use.
        let (op, col, val) = match (left, right) {
            (Column(a), Column(b)) => return self.eval_binary_columns(op, a, b, inverted),
            (Literal(a), Literal(b)) => return self.eval_binary_scalars(op, a, b, inverted),
            (Literal(val), Column(col)) => (op.commute()?, col, val),
            (Column(col), Literal(val)) => (op, col, val),
            _ => {
                debug!("Unsupported binary operand(s): {left:?} {op:?} {right:?}");
                return None;
            }
        };
        match (op, inverted) {
            (Plus | Minus | Multiply | Divide, _) => None, // Unsupported - not boolean output
            (LessThan, false) | (GreaterThanOrEqual, true) => self.eval_lt(col, val),
            (LessThanOrEqual, false) | (GreaterThan, true) => self.eval_le(col, val),
            (GreaterThan, false) | (LessThanOrEqual, true) => self.eval_gt(col, val),
            (GreaterThanOrEqual, false) | (LessThan, true) => self.eval_ge(col, val),
            (Equal, _) => self.eval_eq(col, val, inverted),
            (NotEqual, _) => self.eval_eq(col, val, !inverted),
            (Distinct, _) => self.eval_distinct(col, val, inverted),
            (In, _) => self.eval_in(col, val, inverted),
            (NotIn, _) => self.eval_in(col, val, !inverted),
        }
    }

    /// Dispatches a variadic operation, leveraging each implementation's [`finish_eval_variadic`].
    fn eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Self::Output> {
        let exprs = exprs.iter().map(|expr| self.eval_expr(expr, inverted));
        self.finish_eval_variadic(op, exprs, inverted)
    }

    /// Dispatches an expression to the specific implementation for each expression variant.
    ///
    /// NOTE: [`Expression::Struct`] is not supported and always evaluates to `None`.
    fn eval_expr(&self, expr: &Expr, inverted: bool) -> Option<Self::Output> {
        use Expr::*;
        match expr {
            Literal(val) => self.eval_scalar(val, inverted),
            Column(col) => self.eval_column(col, inverted),
            Struct(_) => None, // not supported
            Unary(UnaryExpression { op, expr }) => self.eval_unary(*op, expr, inverted),
            Binary(BinaryExpression { op, left, right }) => {
                self.eval_binary(*op, left, right, inverted)
            }
            Variadic(VariadicExpression { op, exprs }) => self.eval_variadic(*op, exprs, inverted),
        }
    }
}

/// A collection of provided methods from the [`PredicateEvaluator`] trait, factored out to allow
/// reuse by the different predicate evaluator implementations.
pub(crate) struct PredicateEvaluatorDefaults;
impl PredicateEvaluatorDefaults {
    /// Directly evaluates a boolean scalar. See [`PredicateEvaluator::eval_scalar`].
    pub(crate) fn eval_scalar(val: &Scalar, inverted: bool) -> Option<bool> {
        match val {
            Scalar::Boolean(val) => Some(*val != inverted),
            _ => None,
        }
    }

    /// A (possibly inverted) partial comparison of two scalars, leveraging the [`PartialOrd`]
    /// trait.
    pub(crate) fn partial_cmp_scalars(
        ord: Ordering,
        a: &Scalar,
        b: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        let cmp = a.partial_cmp(b)?;
        let matched = cmp == ord;
        Some(matched != inverted)
    }

    /// Directly evaluates a boolean comparison. See [`PredicateEvaluator::eval_binary_scalars`].
    pub(crate) fn eval_binary_scalars(
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        use BinaryOperator::*;
        match op {
            Equal => Self::partial_cmp_scalars(Ordering::Equal, left, right, inverted),
            NotEqual => Self::partial_cmp_scalars(Ordering::Equal, left, right, !inverted),
            LessThan => Self::partial_cmp_scalars(Ordering::Less, left, right, inverted),
            LessThanOrEqual => Self::partial_cmp_scalars(Ordering::Greater, left, right, !inverted),
            GreaterThan => Self::partial_cmp_scalars(Ordering::Greater, left, right, inverted),
            GreaterThanOrEqual => Self::partial_cmp_scalars(Ordering::Less, left, right, !inverted),
            _ => {
                debug!("Unsupported binary operator: {left:?} {op:?} {right:?}");
                None
            }
        }
    }

    /// Finishes evaluating a (possibly inverted) variadic operation. See
    /// [`PredicateEvaluator::finish_eval_variadic`].
    ///
    /// The inputs were already inverted by the caller, if needed.
    ///
    /// With AND (OR), any FALSE (TRUE) input dominates, forcing a FALSE (TRUE) output.  If there
    /// was no dominating input, then any NULL input forces NULL output.  Otherwise, return the
    /// non-dominant value. Inverting the operation also inverts the dominant value.
    pub(crate) fn finish_eval_variadic(
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        let dominator = match op {
            VariadicOperator::And => inverted,
            VariadicOperator::Or => !inverted,
        };
        let result = exprs.into_iter().try_fold(false, |found_null, val| {
            match val {
                Some(val) if val == dominator => None, // (1) short circuit, dominant found
                Some(_) => Some(found_null),
                None => Some(true), // (2) null found (but keep looking for a dominant value)
            }
        });

        match result {
            None => Some(dominator), // (1) short circuit, dominant found
            Some(false) => Some(!dominator),
            Some(true) => None, // (2) null found, dominant not found
        }
    }
}

/// Resolves columns as scalars, as a building block for [`DefaultPredicateEvaluator`].
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

// In testing, it is convenient to just build a hashmap of scalar values.
#[cfg(test)]
impl ResolveColumnAsScalar for std::collections::HashMap<ColumnName, Scalar> {
    fn resolve_column(&self, col: &ColumnName) -> Option<Scalar> {
        self.get(col).cloned()
    }
}

/// A predicate evaluator that directly evaluates the predicate to produce an `Option<bool>`
/// result. Column resolution is handled by an embedded [`ResolveColumnAsScalar`] instance.
pub(crate) struct DefaultPredicateEvaluator<R: ResolveColumnAsScalar> {
    resolver: R,
}
impl<R: ResolveColumnAsScalar> DefaultPredicateEvaluator<R> {
    // Convenient thin wrapper
    fn resolve_column(&self, col: &ColumnName) -> Option<Scalar> {
        self.resolver.resolve_column(col)
    }
}

impl<R: ResolveColumnAsScalar + 'static> From<R> for DefaultPredicateEvaluator<R> {
    fn from(resolver: R) -> Self {
        Self { resolver }
    }
}

/// A "normal" predicate evaluator. It takes expressions as input, uses a [`ResolveColumnAsScalar`]
/// to convert column references to scalars, and evaluates the resulting constant expression to
/// produce a boolean output.
impl<R: ResolveColumnAsScalar> PredicateEvaluator for DefaultPredicateEvaluator<R> {
    type Output = bool;

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        PredicateEvaluatorDefaults::eval_scalar(val, inverted)
    }

    fn eval_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        Some(matches!(col, Scalar::Null(_)) != inverted)
    }

    fn eval_lt(&self, col: &ColumnName, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::LessThan, &col, val, false)
    }

    fn eval_le(&self, col: &ColumnName, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::LessThanOrEqual, &col, val, false)
    }

    fn eval_gt(&self, col: &ColumnName, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::GreaterThan, &col, val, false)
    }

    fn eval_ge(&self, col: &ColumnName, val: &Scalar) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::GreaterThanOrEqual, &col, val, false)
    }

    fn eval_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<bool> {
        let col = self.resolve_column(col)?;
        self.eval_binary_scalars(BinaryOperator::Equal, &col, val, inverted)
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        PredicateEvaluatorDefaults::eval_binary_scalars(op, left, right, inverted)
    }

    fn eval_binary_columns(
        &self,
        op: BinaryOperator,
        left: &ColumnName,
        right: &ColumnName,
        inverted: bool,
    ) -> Option<Self::Output> {
        let left = self.resolve_column(left)?;
        let right = self.resolve_column(right)?;
        self.eval_binary_scalars(op, &left, &right, inverted)
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        PredicateEvaluatorDefaults::finish_eval_variadic(op, exprs, inverted)
    }
}

/// A predicate evaluator that implements data skipping semantics over various column stats. For
/// example, comparisons involving a column are converted into comparisons over that column's
/// min/max stats, and NULL checks are converted into comparisons involving the column's nullcount
/// and rowcount stats.
///
/// The types involved in these operations are parameterized and implementation-specific. For
/// example, [`crate::engine::parquet_stats_skipping::ParquetStatsProvider`] directly evaluates data
/// skipping expressions and returnss boolean results, while
/// [`crate::scan::data_skipping::DataSkippingPredicateCreator`] instead converts the input
/// predicate to a data skipping predicate that can be evaluated directly later.
pub(crate) trait DataSkippingPredicateEvaluator {
    /// The output type produced by this expression evaluator
    type Output;
    /// The type of min and max column stats
    type TypedStat;
    /// The type of nullcount and rowcount column stats
    type IntStat;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Self::TypedStat>;

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Self::TypedStat>;

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Self::IntStat>;

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat(&self) -> Option<Self::IntStat>;

    /// See [`PredicateEvaluator::eval_scalar`]
    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output>;

    /// For IS NULL (IS NOT NULL), we can only skip the file if all-null (no-null). Any other
    /// nullcount always forces us to keep the file.
    ///
    /// NOTE: When deletion vectors are enabled, they could produce a file that is logically
    /// all-null or logically no-null, even tho the physical stats indicate a mix of null and
    /// non-null values. They cannot invalidate a file's physical all-null or non-null status,
    /// however, so the worst that can happen is we fail to skip an unnecessary file.
    fn eval_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output>;

    /// See [`PredicateEvaluator::eval_binary_scalars`]
    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// See [`PredicateEvaluator::finish_eval_variadic`]
    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Helper method that performs a (possibly inverted) partial comparison between a typed column
    /// stat and a scalar.
    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Self::TypedStat,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output>;

    /// Performs a partial comparison against a column min-stat. See
    /// [`PredicateEvaluatorDefaults::partial_cmp_scalars`] for details of the comparison semantics.
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
    /// [`PredicateEvaluatorDefaults::partial_cmp_scalars`] for details of the comparison semantics.
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

    /// See [`PredicateEvaluator::eval_lt`]
    fn eval_lt(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        // Given `col < val`:
        // Skip if `val` is not greater than _all_ values in [min, max], implies
        // Skip if `val <= min AND val <= max` implies
        // Skip if `val <= min` implies
        // Keep if `NOT(val <= min)` implies
        // Keep if `val > min` implies
        // Keep if `min < val`
        self.partial_cmp_min_stat(col, val, Ordering::Less, false)
    }

    /// See [`PredicateEvaluator::eval_le`]
    fn eval_le(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        // Given `col <= val`:
        // Skip if `val` is less than _all_ values in [min, max], implies
        // Skip if `val < min AND val < max` implies
        // Skip if `val < min` implies
        // Keep if `NOT(val < min)` implies
        // Keep if `NOT(min > val)`
        self.partial_cmp_min_stat(col, val, Ordering::Greater, true)
    }

    /// See [`PredicateEvaluator::eval_gt`]
    fn eval_gt(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        // Given `col > val`:
        // Skip if `val` is not less than _all_ values in [min, max], implies
        // Skip if `val >= min AND val >= max` implies
        // Skip if `val >= max` implies
        // Keep if `NOT(val >= max)` implies
        // Keep if `NOT(max <= val)` implies
        // Keep if `max > val`
        self.partial_cmp_max_stat(col, val, Ordering::Greater, false)
    }

    /// See [`PredicateEvaluator::eval_ge`]
    fn eval_ge(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        // Given `col >= val`:
        // Skip if `val is greater than _every_ value in [min, max], implies
        // Skip if `val > min AND val > max` implies
        // Skip if `val > max` implies
        // Keep if `NOT(val > max)` implies
        // Keep if `NOT(max < val)`
        self.partial_cmp_max_stat(col, val, Ordering::Less, true)
    }

    /// See [`PredicateEvaluator::eval_ge`]
    fn eval_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        let (op, exprs) = if inverted {
            // Column could compare not-equal if min or max value differs from the literal.
            let exprs = [
                self.partial_cmp_min_stat(col, val, Ordering::Equal, true),
                self.partial_cmp_max_stat(col, val, Ordering::Equal, true),
            ];
            (VariadicOperator::Or, exprs)
        } else {
            // Column could compare equal if its min/max values bracket the literal.
            let exprs = [
                self.partial_cmp_min_stat(col, val, Ordering::Greater, true),
                self.partial_cmp_max_stat(col, val, Ordering::Less, true),
            ];
            (VariadicOperator::And, exprs)
        };
        self.finish_eval_variadic(op, exprs, false)
    }
}

impl<T: DataSkippingPredicateEvaluator> PredicateEvaluator for T {
    type Output = T::Output;

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_scalar(val, inverted)
    }

    fn eval_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Self::Output> {
        self.eval_is_null(col, inverted)
    }

    fn eval_lt(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        self.eval_lt(col, val)
    }

    fn eval_le(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        self.eval_le(col, val)
    }

    fn eval_gt(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        self.eval_gt(col, val)
    }

    fn eval_ge(&self, col: &ColumnName, val: &Scalar) -> Option<Self::Output> {
        self.eval_ge(col, val)
    }

    fn eval_eq(&self, col: &ColumnName, val: &Scalar, inverted: bool) -> Option<Self::Output> {
        self.eval_eq(col, val, inverted)
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Self::Output> {
        self.eval_binary_scalars(op, left, right, inverted)
    }

    fn eval_binary_columns(
        &self,
        _op: BinaryOperator,
        _a: &ColumnName,
        _b: &ColumnName,
        _inverted: bool,
    ) -> Option<Self::Output> {
        None // Unsupported
    }

    fn finish_eval_variadic(
        &self,
        op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Self::Output>>,
        inverted: bool,
    ) -> Option<Self::Output> {
        self.finish_eval_variadic(op, exprs, inverted)
    }
}
