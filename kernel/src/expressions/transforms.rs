use crate::expressions::{
    BinaryPredicate, ColumnName, Expression, JunctionPredicate, Predicate, Scalar, UnaryPredicate,
};
use std::borrow::Cow;
use std::collections::HashSet;

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

    /// Called for each [`UnaryPredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_unary`] if they wish to recursively transform the child.
    fn transform_unary(&mut self, pred: &'a UnaryPredicate) -> Option<Cow<'a, UnaryPredicate>> {
        self.recurse_into_unary(pred)
    }

    /// Called for each [`BinaryPredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_binary`] if they wish to recursively transform the children.
    fn transform_binary(&mut self, pred: &'a BinaryPredicate) -> Option<Cow<'a, BinaryPredicate>> {
        self.recurse_into_binary(pred)
    }

    /// Called for each [`JunctionPredicate`] encountered during the traversal. Implementations can
    /// call [`Self::recurse_into_junction`] if they wish to recursively transform the children.
    fn transform_junction(
        &mut self,
        pred: &'a JunctionPredicate,
    ) -> Option<Cow<'a, JunctionPredicate>> {
        self.recurse_into_junction(pred)
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
            Predicate::Unary(u) => match self.transform_unary(u)? {
                Owned(u) => Owned(Predicate::Unary(u)),
                Borrowed(_) => Borrowed(expr),
            },
            Predicate::Binary(b) => match self.transform_binary(b)? {
                Owned(b) => Owned(Predicate::Binary(b)),
                Borrowed(_) => Borrowed(expr),
            },
            Predicate::Junction(j) => match self.transform_junction(j)? {
                Owned(j) => Owned(Predicate::Junction(j)),
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
        recurse_into_children(fields, |f| self.transform(f))
    }

    /// Recursively transforms a unary predicate's child. Returns `None` if the child was removed,
    /// `Some(Cow::Owned)` if the child was changed, and `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_unary(&mut self, u: &'a UnaryPredicate) -> Option<Cow<'a, UnaryPredicate>> {
        use Cow::*;
        let u = match self.transform(&u.expr)? {
            Owned(expr) => Owned(UnaryPredicate::new(u.op, expr)),
            Borrowed(_) => Borrowed(u),
        };
        Some(u)
    }

    /// Recursively transforms a binary predicate's children. Returns `None` if at least one child
    /// was removed, `Some(Cow::Owned)` if at least one child changed, and `Some(Cow::Borrowed)`
    /// otherwise.
    fn recurse_into_binary(&mut self, b: &'a BinaryPredicate) -> Option<Cow<'a, BinaryPredicate>> {
        use Cow::*;
        let left = self.transform(&b.left)?;
        let right = self.transform(&b.right)?;
        let b = match (&left, &right) {
            (Borrowed(_), Borrowed(_)) => Borrowed(b),
            _ => Owned(BinaryPredicate::new(
                b.op,
                left.into_owned(),
                right.into_owned(),
            )),
        };
        Some(b)
    }

    /// Recursively transforms a junction predicate's children. Returns `None` if all children were
    /// removed, `Some(Cow::Owned)` if at least one child was changed or removed, and
    /// `Some(Cow::Borrowed)` otherwise.
    fn recurse_into_junction(
        &mut self,
        j: &'a JunctionPredicate,
    ) -> Option<Cow<'a, JunctionPredicate>> {
        use Cow::*;
        let j = match recurse_into_children(&j.preds, |p| self.transform(p))? {
            Owned(preds) => Owned(JunctionPredicate::new(j.op, preds)),
            Borrowed(_) => Borrowed(j),
        };
        Some(j)
    }
}

/// Used to recurse into the children of an `Expression::Struct` or `Predicate::Junction`.
fn recurse_into_children<'a, T: Clone>(
    children: &'a Vec<T>,
    recurse_fn: impl FnMut(&'a T) -> Option<Cow<'a, T>>,
) -> Option<Cow<'a, Vec<T>>> {
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
        None // all fields filtered out
    } else if num_borrowed < children.len() {
        // At least one field was changed or removed, so make a new field list
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
    fn transform_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
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

    fn transform_unary(&mut self, pred: &'a UnaryPredicate) -> Option<Cow<'a, UnaryPredicate>> {
        self.depth_limited(Self::recurse_into_unary, pred)
    }

    fn transform_binary(&mut self, pred: &'a BinaryPredicate) -> Option<Cow<'a, BinaryPredicate>> {
        self.depth_limited(Self::recurse_into_binary, pred)
    }

    fn transform_junction(
        &mut self,
        pred: &'a JunctionPredicate,
    ) -> Option<Cow<'a, JunctionPredicate>> {
        self.depth_limited(Self::recurse_into_junction, pred)
    }
}

#[cfg(test)]
mod tests {
    use crate::expressions::{
        column_expr, column_pred, Expression as Expr, ExpressionDepthChecker, Predicate as Pred,
    };

    #[test]
    fn test_depth_checker() {
        let pred = Pred::or_from([
            Pred::and_from([
                Pred::or(
                    Pred::lt(Expr::literal(10), column_expr!("x")),
                    Pred::gt(Expr::literal(20), column_expr!("b")),
                ),
                Pred::literal(true),
                Pred::not(Pred::literal(true)),
            ]),
            Pred::and_from([
                Pred::is_null(column_expr!("b")),
                Pred::gt(Expr::literal(10), column_expr!("x")),
                Pred::or(
                    Pred::gt(Expr::literal(5) + Expr::literal(10), Expr::literal(20)),
                    column_pred!("y"),
                ),
                Pred::literal(true),
            ]),
            Pred::ne(
                Expr::literal(42),
                Expr::struct_from([Expr::literal(10), column_expr!("b")]),
            ),
        ]);

        // Similar to ExpressionDepthChecker::check, but also returns call count
        let check_with_call_count =
            |depth_limit| ExpressionDepthChecker::check_with_call_count(&pred, depth_limit);

        // NOTE: The checker ignores leaf nodes!

        // OR
        //  * AND
        //    * OR     >LIMIT<
        //    * NOT
        //  * AND
        //  * NE
        assert_eq!(check_with_call_count(1), (2, 6));

        // OR
        //  * AND
        //    * OR
        //      * LT     >LIMIT<
        //      * GT
        //    * NOT
        //  * AND
        //  * NE
        assert_eq!(check_with_call_count(2), (3, 8));

        // OR
        //  * AND
        //    * OR
        //      * LT
        //      * GT
        //    * NOT
        //  * AND
        //    * IS NULL
        //    * GT
        //    * OR
        //      * GT
        //        * PLUS     >LIMIT<
        //  * NE
        assert_eq!(check_with_call_count(3), (4, 13));

        // Depth limit not hit (full traversal required)
        //
        // OR
        //  * AND
        //    * OR
        //      * LT
        //      * GT
        //    * NOT
        //  * AND
        //    * IS_NULL
        //    * GT
        //    * OR
        //      * GT
        //        * PLUS
        //  * NE
        //    * STRUCT
        assert_eq!(check_with_call_count(4), (4, 14));
        assert_eq!(check_with_call_count(5), (4, 14));
    }
}
