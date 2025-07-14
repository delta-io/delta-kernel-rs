use std::borrow::{Cow, ToOwned};
use std::collections::HashSet;

use crate::expressions::{
    BinaryExpression, BinaryPredicate, ColumnName, Expression, JunctionPredicate, OpaqueExpression,
    OpaquePredicate, Predicate, Scalar, UnaryPredicate,
};
use crate::utils::CowExt as _;

/// Generic framework for recursive bottom-up transforms of expressions and
/// predicates. Transformations return `Option<Cow>` with the following semantics:
///
/// * `Some(Cow::Owned)` -- The input was transformed and the parent should be updated with it.
/// * `Some(Cow::Borrowed)` -- The input was not transformed.
/// * `None` -- The input was filtered out and the parent should be updated to not reference it.
///
/// The transform can start from the generic [`Self::transform_expr`] or [`Self::transform_pred`'],
/// or directly from a specific expression/predicate variant (e.g. [`Self::transform_expr_column`]
/// for [`ColumnName`], [`Self::transform_pred_unary`] for [`UnaryPredicate`]).
///
/// The provided `transform_xxx` methods all default to no-op (returning their input as
/// `Some(Cow::Borrowed)`), and implementations should selectively override specific `transform_xxx`
/// methods as needed for the task at hand.
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// children of each expression or predicate variant. Implementations can call these as needed but
/// will generally not need to override them.
pub trait ExpressionTransform<'a> {
    /// Called for each literal encountered during the expression traversal.
    fn transform_expr_literal(&mut self, value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
        Some(Cow::Borrowed(value))
    }

    /// Called for each column reference encountered during the expression traversal.
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        Some(Cow::Borrowed(name))
    }

    /// Called for the expression list of each [`Expression::Struct`] encountered during the
    /// traversal. Implementations can call [`Self::recurse_into_expr_struct`] if they wish to
    /// recursively transform the child expressions.
    fn transform_expr_struct(&mut self, fields: &'a [Expression]) -> Option<Cow<'a, [Expression]>> {
        self.recurse_into_expr_struct(fields)
    }

    /// Called for each [`OpaqueExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_expr_opaque`] if they wish to recursively transform the children.
    fn transform_expr_opaque(
        &mut self,
        expr: &'a OpaqueExpression,
    ) -> Option<Cow<'a, OpaqueExpression>> {
        self.recurse_into_expr_opaque(expr)
    }

    /// Called for each [`Expression::Unknown`] encountered during the traversal.
    fn transform_expr_unknown(&mut self, name: &'a String) -> Option<Cow<'a, String>> {
        Some(Cow::Borrowed(name))
    }

    /// Called for the child predicate of each [`Expression::Predicate`] encountered during the
    /// traversal. Implementations can call [`Self::recurse_into_expr_pred`] if they wish to
    /// recursively transform the child predicate.
    fn transform_expr_pred(&mut self, pred: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        self.recurse_into_expr_pred(pred)
    }

    /// Called for the child predicate of each [`Predicate::Not`] encountered during the
    /// traversal. Implementations can call [`Self::recurse_into_pred_not`] if they wish to
    /// recursively transform the child expression.
    fn transform_pred_not(&mut self, pred: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        self.recurse_into_pred_not(pred)
    }

    /// Called for each [`UnaryPredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_pred_unary`] if they wish to recursively transform the child.
    fn transform_pred_unary(
        &mut self,
        pred: &'a UnaryPredicate,
    ) -> Option<Cow<'a, UnaryPredicate>> {
        self.recurse_into_pred_unary(pred)
    }

    /// Called for each [`BinaryExpression`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_expr_binary`] if they wish to recursively transform the children.
    fn transform_expr_binary(
        &mut self,
        expr: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        self.recurse_into_expr_binary(expr)
    }

    /// Called for each [`BinaryPredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_pred_binary`] if they wish to recursively transform the children.
    fn transform_pred_binary(
        &mut self,
        pred: &'a BinaryPredicate,
    ) -> Option<Cow<'a, BinaryPredicate>> {
        self.recurse_into_pred_binary(pred)
    }

    /// Called for each [`JunctionPredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_pred_junction`] if they wish to recursively transform the children.
    fn transform_pred_junction(
        &mut self,
        pred: &'a JunctionPredicate,
    ) -> Option<Cow<'a, JunctionPredicate>> {
        self.recurse_into_pred_junction(pred)
    }

    /// Called for each [`OpaquePredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_pred_opaque`] if they wish to recursively transform the children.
    fn transform_pred_opaque(
        &mut self,
        pred: &'a OpaquePredicate,
    ) -> Option<Cow<'a, OpaquePredicate>> {
        self.recurse_into_pred_opaque(pred)
    }

    /// Called for each [`Predicate::Unknown`] encountered during the traversal.
    fn transform_pred_unknown(&mut self, name: &'a String) -> Option<Cow<'a, String>> {
        Some(Cow::Borrowed(name))
    }

    /// General entry point for transforming an expression. This method will dispatch to the
    /// specific transform for each expression variant. Also invoked internally in order to recurse
    /// on the child(ren) of non-leaf variants.
    fn transform_expr(&mut self, expr: &'a Expression) -> Option<Cow<'a, Expression>> {
        let expr = match expr {
            Expression::Literal(s) => self
                .transform_expr_literal(s)?
                .map_owned_or_else(expr, Expression::Literal),
            Expression::Column(c) => self
                .transform_expr_column(c)?
                .map_owned_or_else(expr, Expression::Column),
            Expression::Predicate(p) => self
                .transform_expr_pred(p)?
                .map_owned_or_else(expr, Expression::from),
            Expression::Struct(s) => self
                .transform_expr_struct(s)?
                .map_owned_or_else(expr, Expression::Struct),
            Expression::Binary(b) => self
                .transform_expr_binary(b)?
                .map_owned_or_else(expr, Expression::Binary),
            Expression::Opaque(o) => self
                .transform_expr_opaque(o)?
                .map_owned_or_else(expr, Expression::Opaque),
            Expression::Unknown(u) => self
                .transform_expr_unknown(u)?
                .map_owned_or_else(expr, Expression::Unknown),
        };
        Some(expr)
    }

    /// General entry point for transforming a predicate. This method will dispatch to the specific
    /// transform for each predicate variant. Also invoked internally in order to recurse on the
    /// child(ren) of non-leaf variants.
    fn transform_pred(&mut self, pred: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        let pred = match pred {
            Predicate::BooleanExpression(e) => self
                .transform_expr(e)?
                .map_owned_or_else(pred, Predicate::BooleanExpression),
            Predicate::Not(p) => self.transform_pred_not(p)?.map_owned_or_else(pred, |p| p),
            Predicate::Unary(u) => self
                .transform_pred_unary(u)?
                .map_owned_or_else(pred, Predicate::Unary),
            Predicate::Binary(b) => self
                .transform_pred_binary(b)?
                .map_owned_or_else(pred, Predicate::Binary),
            Predicate::Junction(j) => self
                .transform_pred_junction(j)?
                .map_owned_or_else(pred, Predicate::Junction),
            Predicate::Opaque(o) => self
                .transform_pred_opaque(o)?
                .map_owned_or_else(pred, Predicate::Opaque),
            Predicate::Unknown(u) => self
                .transform_pred_unknown(u)?
                .map_owned_or_else(pred, Predicate::Unknown),
        };
        Some(pred)
    }

    /// Recursively transforms a struct's child expressions. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_expr_struct(
        &mut self,
        fields: &'a [Expression],
    ) -> Option<Cow<'a, [Expression]>> {
        recurse_into_children(fields, |f| self.transform_expr(f))
    }

    /// Recursively transforms the children of an [`OpaqueExpression`]. Returns `None` if all
    /// children were removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_expr_opaque(
        &mut self,
        o: &'a OpaqueExpression,
    ) -> Option<Cow<'a, OpaqueExpression>> {
        let nested_result = recurse_into_children(&o.exprs, |e| self.transform_expr(e))?;
        Some(nested_result.map_owned_or_else(o, |exprs| OpaqueExpression::new(o.op.clone(), exprs)))
    }

    /// Recursively transforms the child of an [`Expression::Predicate`]. Returns `None` if all
    /// children were removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_expr_pred(&mut self, pred: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        self.transform_pred(pred)
    }

    /// Recursively transforms the child of a [`Predicate::Not`] expression. Returns `None` if the
    /// child was removed, `Some(Cow::Owned)` if the child was changed, and `Some(Cow::Borrowed)`
    /// otherwise.
    fn recurse_into_pred_not(&mut self, p: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        Some(self.transform_pred(p)?.map_owned_or_else(p, Predicate::not))
    }

    /// Recursively transforms a unary predicate's child. Returns `None` if the child was removed,
    /// `Some(Cow::Owned)` if the child was changed, and `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_pred_unary(
        &mut self,
        u: &'a UnaryPredicate,
    ) -> Option<Cow<'a, UnaryPredicate>> {
        let nested_result = self.transform_expr(&u.expr)?;
        Some(nested_result.map_owned_or_else(u, |expr| UnaryPredicate::new(u.op, expr)))
    }

    /// Recursively transforms a binary predicate's children. Returns `None` if at least one child
    /// was removed, `Some(Cow::Owned)` if at least one child changed, and `Some(Cow::Borrowed)`
    /// otherwise.
    fn recurse_into_pred_binary(
        &mut self,
        b: &'a BinaryPredicate,
    ) -> Option<Cow<'a, BinaryPredicate>> {
        let left = self.transform_expr(&b.left)?;
        let right = self.transform_expr(&b.right)?;
        let f = |(left, right)| BinaryPredicate::new(b.op, left, right);
        Some((left, right).map_owned_or_else(b, f))
    }

    /// Recursively transforms a binary expression's children. Returns `None` if at least one child
    /// was removed, `Some(Cow::Owned)` if at least one child changed, and `Some(Cow::Borrowed)`
    /// otherwise.
    fn recurse_into_expr_binary(
        &mut self,
        b: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        let left = self.transform_expr(&b.left)?;
        let right = self.transform_expr(&b.right)?;
        let f = |(left, right)| BinaryExpression::new(b.op, left, right);
        Some((left, right).map_owned_or_else(b, f))
    }

    /// Recursively transforms a junction predicate's children. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_pred_junction(
        &mut self,
        j: &'a JunctionPredicate,
    ) -> Option<Cow<'a, JunctionPredicate>> {
        let nested_result = recurse_into_children(&j.preds, |p| self.transform_pred(p))?;
        Some(nested_result.map_owned_or_else(j, |preds| JunctionPredicate::new(j.op, preds)))
    }

    /// Recursively transforms the children of an [`OpaquePredicate`]. Returns `None` if all
    /// children were removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_pred_opaque(
        &mut self,
        o: &'a OpaquePredicate,
    ) -> Option<Cow<'a, OpaquePredicate>> {
        let nested_result = recurse_into_children(&o.exprs, |e| self.transform_expr(e))?;
        Some(nested_result.map_owned_or_else(o, |exprs| OpaquePredicate::new(o.op.clone(), exprs)))
    }
}

/// Used to recurse into the children of an `Expression::Struct` or `Predicate::Junction`.
fn recurse_into_children<'a, T: Clone>(
    children: &'a [T],
    recurse_fn: impl FnMut(&'a T) -> Option<Cow<'a, T>>,
) -> Option<Cow<'a, [T]>> {
    let mut num_borrowed = 0;
    let new_children: Vec<_> = children
        .iter()
        .filter_map(recurse_fn)
        .inspect(|f| {
            if matches!(f, Cow::Borrowed(_)) {
                num_borrowed += 1;
            }
        })
        .collect();

    if new_children.is_empty() {
        None // all children filtered out
    } else if num_borrowed < children.len() {
        // At least one child was changed or removed, so make a new child list
        let children = new_children.into_iter().map(Cow::into_owned).collect();
        Some(Cow::Owned(children))
    } else {
        Some(Cow::Borrowed(children))
    }
}

/// Retrieves the set of column names referenced by an expression.
#[derive(Default)]
pub(crate) struct GetColumnReferences<'a> {
    references: HashSet<&'a ColumnName>,
}

impl<'a> GetColumnReferences<'a> {
    pub(crate) fn into_inner(self) -> HashSet<&'a ColumnName> {
        self.references
    }
}

impl<'a> ExpressionTransform<'a> for GetColumnReferences<'a> {
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
        self.references.insert(name);
        Some(Cow::Borrowed(name))
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
    pub fn check_expr(expr: &Expression, depth_limit: usize) -> usize {
        Self::check_expr_with_call_count(expr, depth_limit).0
    }

    /// Depth-checks the given predicate against a given depth limit. The return value is the
    /// largest depth seen, which is capped at one more than the depth limit (indicating the
    /// recursion was terminated).
    pub fn check_pred(pred: &Predicate, depth_limit: usize) -> usize {
        Self::check_pred_with_call_count(pred, depth_limit).0
    }

    // Exposed for testing
    fn check_expr_with_call_count(expr: &Expression, depth_limit: usize) -> (usize, usize) {
        let mut checker = Self::new(depth_limit);
        checker.transform_expr(expr);
        (checker.max_depth_seen, checker.call_count)
    }

    // Exposed for testing
    fn check_pred_with_call_count(pred: &Predicate, depth_limit: usize) -> (usize, usize) {
        let mut checker = Self::new(depth_limit);
        checker.transform_pred(pred);
        (checker.max_depth_seen, checker.call_count)
    }

    fn new(depth_limit: usize) -> Self {
        Self {
            depth_limit,
            max_depth_seen: 0,
            current_depth: 0,
            call_count: 0,
        }
    }

    // Triggers the requested recursion only doing so would not exceed the depth limit.
    fn depth_limited<'a, T: std::fmt::Debug + ToOwned + ?Sized>(
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
    fn transform_expr_struct(&mut self, fields: &'a [Expression]) -> Option<Cow<'a, [Expression]>> {
        self.depth_limited(Self::recurse_into_expr_struct, fields)
    }

    fn transform_expr_pred(&mut self, pred: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        self.depth_limited(Self::recurse_into_expr_pred, pred)
    }

    fn transform_pred_not(&mut self, pred: &'a Predicate) -> Option<Cow<'a, Predicate>> {
        self.depth_limited(Self::recurse_into_pred_not, pred)
    }

    fn transform_pred_unary(
        &mut self,
        pred: &'a UnaryPredicate,
    ) -> Option<Cow<'a, UnaryPredicate>> {
        self.depth_limited(Self::recurse_into_pred_unary, pred)
    }

    fn transform_expr_binary(
        &mut self,
        expr: &'a BinaryExpression,
    ) -> Option<Cow<'a, BinaryExpression>> {
        self.depth_limited(Self::recurse_into_expr_binary, expr)
    }

    fn transform_pred_binary(
        &mut self,
        pred: &'a BinaryPredicate,
    ) -> Option<Cow<'a, BinaryPredicate>> {
        self.depth_limited(Self::recurse_into_pred_binary, pred)
    }

    fn transform_pred_junction(
        &mut self,
        pred: &'a JunctionPredicate,
    ) -> Option<Cow<'a, JunctionPredicate>> {
        self.depth_limited(Self::recurse_into_pred_junction, pred)
    }

    fn transform_pred_opaque(
        &mut self,
        pred: &'a OpaquePredicate,
    ) -> Option<Cow<'a, OpaquePredicate>> {
        self.depth_limited(Self::recurse_into_pred_opaque, pred)
    }

    fn transform_expr_opaque(
        &mut self,
        expr: &'a OpaqueExpression,
    ) -> Option<Cow<'a, OpaqueExpression>> {
        self.depth_limited(Self::recurse_into_expr_opaque, expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{
        column_expr, column_pred, Expression as Expr, OpaqueExpressionOp, OpaquePredicateOp,
        Predicate as Pred, ScalarExpressionEvaluator,
    };
    use crate::kernel_predicates::{
        DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
        IndirectDataSkippingPredicateEvaluator,
    };
    use crate::DeltaResult;

    #[derive(Debug, PartialEq)]
    struct OpaqueTestOp(String);

    impl OpaqueExpressionOp for OpaqueTestOp {
        fn name(&self) -> &str {
            &self.0
        }
        fn eval_expr_scalar(
            &self,
            _eval_expr: &ScalarExpressionEvaluator<'_>,
            _exprs: &[Expression],
        ) -> DeltaResult<Scalar> {
            unimplemented!()
        }
    }

    impl OpaquePredicateOp for OpaqueTestOp {
        fn name(&self) -> &str {
            &self.0
        }

        fn eval_pred_scalar(
            &self,
            _eval_expr: &ScalarExpressionEvaluator<'_>,
            _evaluator: &DirectPredicateEvaluator<'_>,
            _exprs: &[Expr],
            _inverted: bool,
        ) -> DeltaResult<Option<bool>> {
            unimplemented!()
        }

        fn eval_as_data_skipping_predicate(
            &self,
            _predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
            _exprs: &[Expr],
            _inverted: bool,
        ) -> Option<bool> {
            unimplemented!()
        }

        fn as_data_skipping_predicate(
            &self,
            _predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
            _exprs: &[Expr],
            _inverted: bool,
        ) -> Option<Pred> {
            unimplemented!()
        }
    }

    #[test]
    fn test_depth_checker() {
        let pred = Pred::or_from([
            Pred::and_from([
                Pred::opaque(
                    OpaqueTestOp("opaque".to_string()),
                    vec![
                        Expr::literal(10) + column_expr!("x"),
                        Expr::unknown("unknown") - column_expr!("b"),
                    ],
                ),
                Pred::literal(true),
                Pred::not(Pred::literal(true)),
            ]),
            Pred::and_from([
                Pred::is_null(column_expr!("b")),
                Pred::gt(Expr::literal(10), column_expr!("x")),
                Pred::or(
                    Pred::gt(
                        Expr::literal(5)
                            + Expr::opaque(
                                OpaqueTestOp("inscrutable".to_string()),
                                vec![Expr::literal(10)],
                            ),
                        Expr::literal(20),
                    ),
                    column_pred!("y"),
                ),
                Pred::unknown("mystery"),
            ]),
            Pred::eq(
                Expr::literal(42),
                Expr::struct_from([Expr::literal(10), column_expr!("b")]),
            ),
        ]);

        // Verify the default/no-op transform, since we have this nice complex expression handy.
        struct Noop;
        impl super::ExpressionTransform<'_> for Noop {}
        assert!(matches!(
            Noop.transform_pred(&pred),
            Some(std::borrow::Cow::Borrowed(_))
        ));

        // Similar to ExpressionDepthChecker::check_pred, but also returns call count
        let check_with_call_count =
            |depth_limit| ExpressionDepthChecker::check_pred_with_call_count(&pred, depth_limit);

        // NOTE: The checker ignores leaf nodes!

        // OR
        //  * AND
        //    * OPAQUE   >LIMIT<
        //    * NOT
        //  * AND
        //  * EQ
        assert_eq!(check_with_call_count(1), (2, 6));

        // OR
        //  * AND
        //    * OPAQUE
        //      * PLUS      >LIMIT<
        //      * MINUS
        //    * NOT
        //  * AND
        //  * EQ
        assert_eq!(check_with_call_count(2), (3, 8));

        // OR
        //  * AND
        //    * OPAQUE
        //      * PLUS
        //      * MINUS
        //    * NOT
        //  * AND
        //    * IS NULL
        //    * GT
        //    * OR
        //      * GT
        //        * PLUS     >LIMIT<
        //  * EQ
        assert_eq!(check_with_call_count(3), (4, 13));

        // OR
        //  * AND
        //    * OPAQUE
        //      * PLUS
        //      * MINUS
        //    * NOT
        //  * AND
        //    * IS_NULL
        //    * GT
        //    * OR
        //      * GT
        //        * PLUS
        //          * OPAQUE    >LIMIT<
        //  * EQ
        assert_eq!(check_with_call_count(4), (5, 14));

        // Depth limit not hit (full traversal required)
        //
        // OR
        //  * AND
        //    * OPAQUE
        //      * PLUS
        //      * MINUS
        //    * NOT
        //  * AND
        //    * IS_NULL
        //    * GT
        //    * OR
        //      * GT
        //        * PLUS
        //          * OPAQUE
        //  * EQ
        //    * STRUCT
        assert_eq!(check_with_call_count(5), (5, 15));
        assert_eq!(check_with_call_count(6), (5, 15));

        // Check expressions as well
        let expr = Expr::from(pred);
        let check_with_call_count =
            |depth_limit| ExpressionDepthChecker::check_expr_with_call_count(&expr, depth_limit);

        // Adding an `Expression::Predicate` root makes the expression tree exactly one node taller,
        // which makes the recursion terminate sooner than previously:
        //
        // PRED
        //  * OR
        //    * AND              > LIMIT 1 <
        //      * OPAQUE         > LIMIT 2 <
        //        * PLUS         > LIMIT 3 <
        //        * MINUS
        //      * NOT
        //    * AND
        //      * IS_NULL
        //      * GT
        //      * OR
        //        * GT
        //          * PLUS       > LIMIT 4 <
        //            * OPAQUE   > LIMIT 5 <
        //    * EQ
        //      * STRUCT
        assert_eq!(check_with_call_count(1), (2, 5));
        assert_eq!(check_with_call_count(2), (3, 7));
        assert_eq!(check_with_call_count(3), (4, 9));
        assert_eq!(check_with_call_count(4), (5, 14));
        assert_eq!(check_with_call_count(5), (6, 15));
        assert_eq!(check_with_call_count(6), (6, 16));
        assert_eq!(check_with_call_count(7), (6, 16));
    }
}
