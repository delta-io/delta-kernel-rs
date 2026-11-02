use std::borrow::{Cow, ToOwned};
use std::sync::Arc;

use crate::expressions::{
    BinaryExpression, BinaryPredicate, CaseWhenExpression, CastExpression, ColumnName, Expression,
    ExpressionRef, ExpressionStructPatch, JunctionPredicate, MapToStructExpression,
    OpaqueExpression, OpaquePredicate, ParseJsonExpression, Predicate, Scalar, UnaryExpression,
    UnaryPredicate, VariadicExpression,
};
use crate::transforms::{
    Carrier, carrier_into_inner_opt, carrier_try_none, map_owned_children_or_else,
    map_owned_or_else, map_owned_pair_or_else, transform_output_type,
};
use crate::{KernelError, KernelResult};

/// Generic framework for recursive bottom-up transforms of expressions and predicates.
///
/// The transform entry point is generally [`Self::transform_expr`] or [`Self::transform_pred`] (for
/// expressions or predicates, respectively), but callers can also directly invoke the transform
/// for a specific expression/predicate variant (e.g. [`Self::transform_expr_column`] for
/// [`ColumnName`] or [`Self::transform_pred_unary`] for [`UnaryPredicate`]).
///
/// The provided `transform_xxx` methods all default to no-op (usually by invoking the corresponding
/// recursive helper method), and implementations should selectively override specific
/// `transform_xxx` methods as needed for the task at hand.
///
/// # Recursive helper methods
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// child expression of each expression type. Except as specifically noted otherwise, these
/// recursive helpers all behave uniformly, based on the number of children the parent has:
///
/// * Leaf (no children) - Leaf `transform_xxx` methods simply return their argument unchanged, and
///   no corresponding `recurse_into_xxx` method is provided.
///
/// * Unary (single child) - If the child was filtered out, filter out the parent. If the child
///   changed, build a new parent around it. Otherwise, return the parent unchanged.
///
/// * Binary (two children) - If either child was filtered out, filter out the parent. If at least
///   one child changed, build a new parent around them. Otherwise, return the parent unchanged.
///
/// * Variadic (0+ children) - If no children remain (all filtered out), filter out the parent.
///   Otherwise, if at least one child changed or was filtered out, build a new parent around the
///   children. Otherwise, return the parent unchanged.
///
/// * CASE WHEN - If any condition, result, or ELSE was filtered out, filter out the parent.
///   Otherwise, rebuild only if a child changed.
///
/// Implementations can call these as needed but will generally not need to override them.
///
/// # Transform carrier selection
///
/// Implementations choose an output [`Carrier`] instance based on the operation to be
/// performed. That carrier determines the return type of each transform method.
///
/// For example, a simple read-only visitor would use `()` as a carrier, while a validity checker
/// could use `Result<()>` instead. A mutating transform uses `Cow<_>`, returning `Cow::Owned`
/// for changed/replaced nodes, and a filtering transform uses `Option<Cow<_>>`, where `None`
/// indicates the node should be dropped rather than replaced. `Result<Cow<_>>` and
/// `Result<Option<Cow<_>>, E>` round out the set as fallible mutating and fitering transforms that
/// short circuit immediately upon `Err`.
pub trait ExpressionTransform<'a> {
    /// [`Carrier`] output type for transformed nodes.
    ///
    /// Implementations can use [`crate::transforms::transform_output_type`] to define `Output`
    /// and `Residual` together.
    type Output<T: ToOwned + ?Sized + 'a>: Carrier<'a, T, Residual = Self::Residual>;
    /// Residual type propagated by this transform's output [`Carrier`].
    ///
    /// Implementations can use [`crate::transforms::transform_output_type`] to define `Output`
    /// and `Residual` together. Or, define it manually like this:
    /// ```rust,no_run
    /// # use std::borrow::Cow;
    /// # use delta_kernel::transforms::{Carrier, ExpressionTransform};
    /// # struct X;
    /// # impl<'a> ExpressionTransform<'a> for X {
    /// #     type Output<T: std::borrow::ToOwned + ?Sized + 'a> = Cow<'a, T>;
    /// type Residual = <Self::Output<()> as Carrier<'a, ()>>::Residual;
    /// # }
    /// ```
    /// (required because associated type defaults are not stable rust yet)
    type Residual;

    /// Called for each literal encountered during the traversal (leaf).
    fn transform_expr_literal(&mut self, value: &'a Scalar) -> Self::Output<Scalar> {
        Carrier::from_inner(Cow::Borrowed(value))
    }

    /// Called for each column reference encountered during the traversal (leaf).
    fn transform_expr_column(&mut self, name: &'a ColumnName) -> Self::Output<ColumnName> {
        Carrier::from_inner(Cow::Borrowed(name))
    }

    /// Called for the expression list of each struct expression encountered during the
    /// traversal. The provided implementation just forwards to [`Self::recurse_into_expr_struct`].
    fn transform_expr_struct(
        &mut self,
        fields: &'a [ExpressionRef],
    ) -> Self::Output<[ExpressionRef]> {
        self.recurse_into_expr_struct(fields)
    }

    /// Called for each opaque expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_opaque`].
    fn transform_expr_opaque(
        &mut self,
        expr: &'a OpaqueExpression,
    ) -> Self::Output<OpaqueExpression> {
        self.recurse_into_expr_opaque(expr)
    }

    /// Called for each unknown expression encountered during the traversal (leaf).
    fn transform_expr_unknown(&mut self, name: &'a String) -> Self::Output<String> {
        Carrier::from_inner(Cow::Borrowed(name))
    }

    /// Called for each struct patch expression encountered during the traversal (leaf).
    ///
    /// The provided implementation does _NOT_ recurse into its children.
    fn transform_expr_struct_patch(
        &mut self,
        patch: &'a ExpressionStructPatch,
    ) -> Self::Output<ExpressionStructPatch> {
        Carrier::from_inner(Cow::Borrowed(patch))
    }

    /// Called for each parse-json expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_parse_json`].
    fn transform_expr_parse_json(
        &mut self,
        expr: &'a ParseJsonExpression,
    ) -> Self::Output<ParseJsonExpression> {
        self.recurse_into_expr_parse_json(expr)
    }

    /// Called for each map-to-struct expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_map_to_struct`].
    fn transform_expr_map_to_struct(
        &mut self,
        expr: &'a MapToStructExpression,
    ) -> Self::Output<MapToStructExpression> {
        self.recurse_into_expr_map_to_struct(expr)
    }

    /// Called for each cast expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_cast`].
    fn transform_expr_cast(&mut self, expr: &'a CastExpression) -> Self::Output<CastExpression> {
        self.recurse_into_expr_cast(expr)
    }

    /// Called for the child of each predicate expression encountered during the
    /// traversal. The provided implementation just forwards to [`Self::recurse_into_expr_pred`].
    fn transform_expr_pred(&mut self, pred: &'a Predicate) -> Self::Output<Predicate> {
        self.recurse_into_expr_pred(pred)
    }

    /// Called for the child of each NOT predicate encountered during the
    /// traversal. The provided implementation just forwards to [`Self::recurse_into_pred_not`].
    fn transform_pred_not(&mut self, pred: &'a Predicate) -> Self::Output<Predicate> {
        self.recurse_into_pred_not(pred)
    }

    /// Called for each unary expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_unary`].
    fn transform_expr_unary(&mut self, expr: &'a UnaryExpression) -> Self::Output<UnaryExpression> {
        self.recurse_into_expr_unary(expr)
    }

    /// Called for each unary predicate encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_pred_unary`].
    fn transform_pred_unary(&mut self, pred: &'a UnaryPredicate) -> Self::Output<UnaryPredicate> {
        self.recurse_into_pred_unary(pred)
    }

    /// Called for each binary expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_binary`].
    fn transform_expr_binary(
        &mut self,
        expr: &'a BinaryExpression,
    ) -> Self::Output<BinaryExpression> {
        self.recurse_into_expr_binary(expr)
    }

    /// Called for each binary predicate encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_pred_binary`].
    fn transform_pred_binary(
        &mut self,
        pred: &'a BinaryPredicate,
    ) -> Self::Output<BinaryPredicate> {
        self.recurse_into_pred_binary(pred)
    }

    /// Called for each variadic expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_variadic`].
    fn transform_expr_variadic(
        &mut self,
        expr: &'a VariadicExpression,
    ) -> Self::Output<VariadicExpression> {
        self.recurse_into_expr_variadic(expr)
    }

    /// Called for each CASE WHEN expression encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_expr_case_when`].
    fn transform_expr_case_when(
        &mut self,
        expr: &'a CaseWhenExpression,
    ) -> Self::Output<CaseWhenExpression> {
        self.recurse_into_expr_case_when(expr)
    }

    /// Called for each junction predicate encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_pred_junction`].
    fn transform_pred_junction(
        &mut self,
        pred: &'a JunctionPredicate,
    ) -> Self::Output<JunctionPredicate> {
        self.recurse_into_pred_junction(pred)
    }

    /// Called for each opaque predicate encountered during the traversal. The provided
    /// implementation just forwards to [`Self::recurse_into_pred_opaque`].
    fn transform_pred_opaque(
        &mut self,
        pred: &'a OpaquePredicate,
    ) -> Self::Output<OpaquePredicate> {
        self.recurse_into_pred_opaque(pred)
    }

    /// Called for each unknown predicate encountered during the traversal (leaf).
    fn transform_pred_unknown(&mut self, name: &'a String) -> Self::Output<String> {
        Carrier::from_inner(Cow::Borrowed(name))
    }

    /// General entry point for transforming an expression. This method will dispatch to the
    /// specific transform for each expression variant. Also invoked internally in order to recurse
    /// on the child(ren) of non-leaf expressions.
    fn transform_expr(&mut self, expr: &'a Expression) -> Self::Output<Expression> {
        match expr {
            Expression::Literal(s) => {
                let child = self.transform_expr_literal(s);
                map_owned_or_else(expr, child, Expression::Literal)
            }
            Expression::Column(c) => {
                let child = self.transform_expr_column(c);
                map_owned_or_else(expr, child, Expression::Column)
            }
            Expression::Predicate(p) => {
                let child = self.transform_expr_pred(p);
                map_owned_or_else(expr, child, Expression::from)
            }
            Expression::Struct(s, nullability) => {
                let map_owned = |exprs| Expression::Struct(exprs, nullability.clone());
                map_owned_or_else(expr, self.transform_expr_struct(s), map_owned)
            }
            Expression::StructPatch(t) => {
                let child = self.transform_expr_struct_patch(t);
                map_owned_or_else(expr, child, Expression::StructPatch)
            }
            Expression::Unary(u) => {
                let child = self.transform_expr_unary(u);
                map_owned_or_else(expr, child, Expression::Unary)
            }
            Expression::Binary(b) => {
                let child = self.transform_expr_binary(b);
                map_owned_or_else(expr, child, Expression::Binary)
            }
            Expression::Variadic(v) => {
                let child = self.transform_expr_variadic(v);
                map_owned_or_else(expr, child, Expression::Variadic)
            }
            Expression::CaseWhen(c) => {
                let child = self.transform_expr_case_when(c);
                map_owned_or_else(expr, child, Expression::CaseWhen)
            }
            Expression::Opaque(o) => {
                let child = self.transform_expr_opaque(o);
                map_owned_or_else(expr, child, Expression::Opaque)
            }
            Expression::ParseJson(p) => {
                let child = self.transform_expr_parse_json(p);
                map_owned_or_else(expr, child, Expression::ParseJson)
            }
            Expression::MapToStruct(m) => {
                let child = self.transform_expr_map_to_struct(m);
                map_owned_or_else(expr, child, Expression::MapToStruct)
            }
            Expression::Cast(c) => {
                let child = self.transform_expr_cast(c);
                map_owned_or_else(expr, child, Expression::Cast)
            }
            Expression::Unknown(u) => {
                let child = self.transform_expr_unknown(u);
                map_owned_or_else(expr, child, Expression::Unknown)
            }
        }
    }

    /// General entry point for transforming a predicate. This method will dispatch to the specific
    /// transform for each predicate variant. Also invoked internally in order to recurse on the
    /// child(ren) of non-leaf variants.
    fn transform_pred(&mut self, pred: &'a Predicate) -> Self::Output<Predicate> {
        match pred {
            Predicate::BooleanExpression(e) => {
                let child = self.transform_expr(e);
                map_owned_or_else(pred, child, Predicate::BooleanExpression)
            }
            Predicate::Not(p) => {
                let child = self.transform_pred_not(p);
                map_owned_or_else(pred, child, |p| p)
            }
            Predicate::Unary(u) => {
                let child = self.transform_pred_unary(u);
                map_owned_or_else(pred, child, Predicate::Unary)
            }
            Predicate::Binary(b) => {
                let child = self.transform_pred_binary(b);
                map_owned_or_else(pred, child, Predicate::Binary)
            }
            // Route through the constructor to normalize in case the transform removed children.
            // When `transform_pred` returns `None` for a child, it is filtered out, which may
            // reduce the junction to one or zero elements. The constructor normalizes these.
            Predicate::Junction(j) => {
                let child = self.transform_pred_junction(j);
                map_owned_or_else(pred, child, |j| Predicate::junction(j.op, j.preds))
            }
            Predicate::Opaque(o) => {
                let child = self.transform_pred_opaque(o);
                map_owned_or_else(pred, child, Predicate::Opaque)
            }
            Predicate::Unknown(u) => {
                let child = self.transform_pred_unknown(u);
                map_owned_or_else(pred, child, Predicate::Unknown)
            }
        }
    }

    /// Recursively transforms a struct's child expressions (variadic).
    fn recurse_into_expr_struct(
        &mut self,
        fields: &'a [ExpressionRef],
    ) -> Self::Output<[ExpressionRef]> {
        let children = fields.iter().map(|f| -> Self::Output<ExpressionRef> {
            map_owned_or_else(f, self.transform_expr(f), Arc::new)
        });
        map_owned_children_or_else(fields, children, |fields| fields)
    }

    /// Recursively transforms the child expression of a parse-json expression (unary).
    fn recurse_into_expr_parse_json(
        &mut self,
        expr: &'a ParseJsonExpression,
    ) -> Self::Output<ParseJsonExpression> {
        let f = |json_expr| ParseJsonExpression::new(json_expr, expr.output_schema.clone());
        map_owned_or_else(expr, self.transform_expr(&expr.json_expr), f)
    }

    /// Recursively transforms the child expression of a map-to-struct expression (unary).
    fn recurse_into_expr_map_to_struct(
        &mut self,
        expr: &'a MapToStructExpression,
    ) -> Self::Output<MapToStructExpression> {
        let nested = self.transform_expr(&expr.map_expr);
        let rebuild = |map_expr| MapToStructExpression::new(map_expr, expr.options.clone());
        map_owned_or_else(expr, nested, rebuild)
    }

    /// Recursively transforms the child expression of a cast expression (unary).
    fn recurse_into_expr_cast(&mut self, expr: &'a CastExpression) -> Self::Output<CastExpression> {
        let f = |child| CastExpression::new(child, expr.target.clone());
        map_owned_or_else(expr, self.transform_expr(&expr.expr), f)
    }

    /// Recursively transforms the children of an opaque expression (variadic).
    fn recurse_into_expr_opaque(
        &mut self,
        o: &'a OpaqueExpression,
    ) -> Self::Output<OpaqueExpression> {
        let transformed_children = o.exprs.iter().map(|e| self.transform_expr(e));
        let map_owned = |exprs| OpaqueExpression::new(o.op.clone(), exprs);
        map_owned_children_or_else(o, transformed_children, map_owned)
    }

    /// Recursively transforms the child of a predicate expression (unary).
    fn recurse_into_expr_pred(&mut self, pred: &'a Predicate) -> Self::Output<Predicate> {
        self.transform_pred(pred)
    }

    /// Recursively transforms the child of a not predicate expression (unary).
    fn recurse_into_pred_not(&mut self, p: &'a Predicate) -> Self::Output<Predicate> {
        map_owned_or_else(p, self.transform_pred(p), Predicate::not)
    }

    /// Recursively transforms a unary predicate's child (unary).
    fn recurse_into_pred_unary(&mut self, u: &'a UnaryPredicate) -> Self::Output<UnaryPredicate> {
        let nested = self.transform_expr(&u.expr);
        map_owned_or_else(u, nested, |expr| UnaryPredicate::new(u.op, expr))
    }

    /// Recursively transforms a binary predicate's children (binary).
    fn recurse_into_pred_binary(
        &mut self,
        b: &'a BinaryPredicate,
    ) -> Self::Output<BinaryPredicate> {
        let left = self.transform_expr(&b.left);
        let right = self.transform_expr(&b.right);
        let f = |(left, right)| BinaryPredicate::new(b.op, left, right);
        map_owned_pair_or_else(b, left, right, f)
    }

    /// Recursively transforms a unary expression's child (unary).
    fn recurse_into_expr_unary(&mut self, u: &'a UnaryExpression) -> Self::Output<UnaryExpression> {
        let nested = self.transform_expr(&u.expr);
        map_owned_or_else(u, nested, |expr| UnaryExpression::new(u.op, expr))
    }

    /// Recursively transforms a binary expression's children (binary).
    fn recurse_into_expr_binary(
        &mut self,
        b: &'a BinaryExpression,
    ) -> Self::Output<BinaryExpression> {
        let left = self.transform_expr(&b.left);
        let right = self.transform_expr(&b.right);
        let f = |(left, right)| BinaryExpression::new(b.op, left, right);
        map_owned_pair_or_else(b, left, right, f)
    }

    /// Recursively transforms a variadic expression's children (variadic).
    fn recurse_into_expr_variadic(
        &mut self,
        v: &'a VariadicExpression,
    ) -> Self::Output<VariadicExpression> {
        let children = v.exprs.iter().map(|e| self.transform_expr(e));
        map_owned_children_or_else(v, children, |exprs| VariadicExpression::new(v.op, exprs))
    }

    /// Recursively transforms CASE WHEN branches and ELSE. Filtering any child filters out the
    /// entire CASE.
    fn recurse_into_expr_case_when(
        &mut self,
        c: &'a CaseWhenExpression,
    ) -> Self::Output<CaseWhenExpression> {
        let mut changed = false;
        let mut branches = Vec::new();
        for (condition, result) in &c.branches {
            let condition = carrier_into_inner_opt!(self.transform_pred(condition));
            let result = carrier_into_inner_opt!(self.transform_expr(result));
            if let (Some(condition), Some(result)) = (condition, result) {
                changed |= matches!(condition, Cow::Owned(_)) || matches!(result, Cow::Owned(_));
                branches.push((condition, result));
            }
        }
        let else_expr = carrier_into_inner_opt!(self.transform_expr(&c.else_expr));
        // Visit all children before propagating filtering: visitor carriers also produce None.
        if branches.len() != c.branches.len() || else_expr.is_none() {
            carrier_try_none!();
        }
        changed |= matches!(else_expr, Some(Cow::Owned(_)));
        let result = match else_expr {
            Some(else_expr) if changed => Cow::Owned(CaseWhenExpression::new(
                branches
                    .into_iter()
                    .map(|(condition, result)| (condition.into_owned(), result.into_owned())),
                else_expr.into_owned(),
            )),
            _ => Cow::Borrowed(c),
        };
        Carrier::from_inner(result)
    }

    /// Recursively transforms a junction predicate's children (variadic).
    fn recurse_into_pred_junction(
        &mut self,
        j: &'a JunctionPredicate,
    ) -> Self::Output<JunctionPredicate> {
        let children = j.preds.iter().map(|p| self.transform_pred(p));
        map_owned_children_or_else(j, children, |preds| JunctionPredicate::new(j.op, preds))
    }

    /// Recursively transforms an opaque predicate's children (variadic).
    fn recurse_into_pred_opaque(
        &mut self,
        o: &'a OpaquePredicate,
    ) -> Self::Output<OpaquePredicate> {
        let children = o.exprs.iter().map(|e| self.transform_expr(e));
        let map_owned = |exprs| OpaquePredicate::new(o.op.clone(), exprs);
        map_owned_children_or_else(o, children, map_owned)
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
        let _ = checker.transform_expr(expr);
        (checker.max_depth_seen, checker.call_count)
    }

    // Exposed for testing
    fn check_pred_with_call_count(pred: &Predicate, depth_limit: usize) -> (usize, usize) {
        let mut checker = Self::new(depth_limit);
        let _ = checker.transform_pred(pred);
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
        recurse: impl FnOnce(&mut Self, &'a T) -> KernelResult<()>,
        arg: &'a T,
    ) -> KernelResult<()> {
        self.call_count += 1;
        if self.current_depth > self.max_depth_seen {
            self.max_depth_seen = self.current_depth;
            if self.current_depth > self.depth_limit {
                return Err(KernelError::schema(format!(
                    "Max expression depth {} exceeded by {arg:?}",
                    self.depth_limit
                )));
            }
        }
        self.current_depth += 1;
        let result = recurse(self, arg);
        self.current_depth -= 1;
        result
    }
}

impl<'a> ExpressionTransform<'a> for ExpressionDepthChecker {
    transform_output_type!(|'a, T| KernelResult<()>);

    fn transform_expr_cast(&mut self, expr: &'a CastExpression) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_cast, expr)
    }

    fn transform_expr_struct(&mut self, fields: &'a [ExpressionRef]) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_struct, fields)
    }

    fn transform_expr_pred(&mut self, pred: &'a Predicate) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_pred, pred)
    }

    fn transform_pred_not(&mut self, pred: &'a Predicate) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_pred_not, pred)
    }

    fn transform_pred_unary(&mut self, pred: &'a UnaryPredicate) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_pred_unary, pred)
    }

    fn transform_expr_binary(&mut self, expr: &'a BinaryExpression) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_binary, expr)
    }

    fn transform_expr_case_when(&mut self, expr: &'a CaseWhenExpression) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_case_when, expr)
    }

    fn transform_pred_binary(&mut self, pred: &'a BinaryPredicate) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_pred_binary, pred)
    }

    fn transform_pred_junction(&mut self, pred: &'a JunctionPredicate) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_pred_junction, pred)
    }

    fn transform_pred_opaque(&mut self, pred: &'a OpaquePredicate) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_pred_opaque, pred)
    }

    fn transform_expr_opaque(&mut self, expr: &'a OpaqueExpression) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_opaque, expr)
    }

    fn transform_expr_map_to_struct(
        &mut self,
        expr: &'a MapToStructExpression,
    ) -> KernelResult<()> {
        self.depth_limited(Self::recurse_into_expr_map_to_struct, expr)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use super::*;
    use crate::expressions::VariadicExpressionOp::Coalesce;
    use crate::expressions::{
        Expression, Expression as Expr, MapToStructOptions, OpaqueExpressionOp, OpaquePredicateOp,
        ParseJsonExpression, Predicate as Pred, Scalar, ScalarExpressionEvaluator,
        VariadicExpression, col, column_name, column_pred, lit,
    };
    use crate::kernel_predicates::{
        DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
        IndirectDataSkippingPredicateEvaluator,
    };
    use crate::schema::{DataType, StructType, schema_ref};

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
        ) -> KernelResult<Scalar> {
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
        ) -> KernelResult<Option<bool>> {
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

    struct NoopTransform;
    impl<'a> ExpressionTransform<'a> for NoopTransform {
        transform_output_type!(|'a, T| Cow<'a, T>);
    }

    struct ColumnReplacer(Option<&'static str>);
    impl<'a> ExpressionTransform<'a> for ColumnReplacer {
        transform_output_type!(|'a, T| Option<Cow<'a, T>>);

        fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
            if name.len() == 1 && name[0] == "old_col" {
                self.0
                    .map(|replacement| Cow::Owned(ColumnName::new([replacement])))
            } else {
                Some(Cow::Borrowed(name))
            }
        }
    }

    #[test]
    fn test_transform_expr_variadic_noop() {
        // Test default no-op behavior - should return Cow::Borrowed
        let variadic_expr = VariadicExpression::new(Coalesce, vec![lit(1), col!("x"), lit("test")]);

        let mut transform = NoopTransform;
        let result = transform.transform_expr_variadic(&variadic_expr);

        assert!(matches!(result, Cow::Borrowed(_)));
        if let Cow::Borrowed(result_expr) = result {
            assert_eq!(result_expr, &variadic_expr);
        }
    }

    fn case_when_columns(names: [&str; 5]) -> Expr {
        let [c1, r1, c2, r2, fallback] = names.map(|name| Expr::column([name]));
        Expr::case_when(
            [
                (Pred::BooleanExpression(c1), r1),
                (Pred::BooleanExpression(c2), r2),
            ],
            fallback,
        )
    }

    #[test]
    fn test_transform_expr_case_when_visitor_visits_all_children() {
        struct ColumnCollector(Vec<ColumnName>);
        impl<'a> ExpressionTransform<'a> for ColumnCollector {
            transform_output_type!(|'a, T| ());
            fn transform_expr_column(&mut self, name: &'a ColumnName) {
                self.0.push(name.clone());
            }
        }
        let names = ["c1", "r1", "c2", "r2", "fallback"];
        let expr = case_when_columns(names);
        let mut collector = ColumnCollector(Vec::new());
        collector.transform_expr(&expr);
        assert_eq!(collector.0, names.map(|name| ColumnName::new([name])));
    }

    #[test]
    fn test_transform_expr_variadic_empty_input() {
        // Test edge case with empty children list
        let variadic_expr = VariadicExpression::new(Coalesce, Vec::<Expr>::new());

        let mut transform = NoopTransform;
        let result = transform.transform_expr_variadic(&variadic_expr);

        // Empty variadic lists remain present for non-filtering carriers.
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[rstest::rstest]
    #[case::case_when_first_condition(|name: Option<&str>| name.map(|n| {
        case_when_columns([n, "x", "x", "x", "x"])
    }))]
    #[case::case_when_first_result(
        |name: Option<&str>| name.map(|n| case_when_columns(["x", n, "x", "x", "x"])),
    )]
    #[case::case_when_first_branch(
        |name: Option<&str>| name.map(|n| case_when_columns([n, n, "x", "x", "x"])),
    )]
    #[case::case_when_second_condition(|name: Option<&str>| name.map(|n| {
        case_when_columns(["x", "x", n, "x", "x"])
    }))]
    #[case::case_when_second_result(|name: Option<&str>| name.map(|n| {
        case_when_columns(["x", "x", "x", n, "x"])
    }))]
    #[case::case_when_else(
        |name: Option<&str>| name.map(|n| case_when_columns(["x", "x", "x", "x", n])),
    )]
    #[case::case_when_all_children(|name: Option<&str>| name.map(|n| case_when_columns([n; 5])))]
    #[case::case_when_no_match(|_| Some(case_when_columns(["x"; 5])))]
    #[case::case_when_empty(|name: Option<&str>| name.map(|n| {
        Expr::CaseWhen(CaseWhenExpression::new([], Expr::column([n])))
    }))]
    #[case::case_when_empty_no_match(|_| Some(Expr::CaseWhen(
        CaseWhenExpression::new([], col!("x")),
    )))]
    #[case::parse_json(|name: Option<&str>| name.map(|n| {
        Expr::ParseJson(ParseJsonExpression::new(Expr::column([n]), test_output_schema()))
    }))]
    #[case::parse_json_no_match(|_| Some(Expr::ParseJson(
        ParseJsonExpression::new(col!("unchanged_col"), test_output_schema()),
    )))]
    #[case::variadic(|name: Option<&str>| Some(Expr::coalesce([
        Some(lit(1)),
        name.map(|n| Expr::column([n])),
        Some(col!("unchanged_col")),
        Some(lit("test")),
    ].into_iter().flatten())))]
    fn test_transform_expr_child_transformation(
        #[case] make_expr: fn(Option<&'static str>) -> Option<Expr>,
        #[values(Some("new_col"), None)] replacement: Option<&'static str>,
    ) {
        let input = make_expr(Some("old_col")).unwrap();
        let expected = make_expr(replacement);
        let result = ColumnReplacer(replacement).transform_expr(&input);
        assert_eq!(result.as_deref(), expected.as_ref());
        assert_eq!(
            matches!(result, Some(Cow::Borrowed(_))),
            expected.as_ref() == Some(&input)
        );
    }

    #[test]
    fn test_transform_expr_variadic_child_removal() {
        // Test removal of child expressions - should return Cow::Owned with fewer children
        struct LiteralRemover;
        impl<'a> ExpressionTransform<'a> for LiteralRemover {
            transform_output_type!(|'a, T| Option<Cow<'a, T>>);

            fn transform_expr_literal(&mut self, _value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
                None // Remove all literals
            }
        }

        let variadic_expr =
            VariadicExpression::new(Coalesce, vec![lit(1), col!("x"), lit("test"), col!("y")]);

        let mut transform = LiteralRemover;
        let result = transform.transform_expr_variadic(&variadic_expr);

        assert!(matches!(result, Some(Cow::Owned(_))));
        if let Some(Cow::Owned(result_expr)) = result {
            assert_eq!(result_expr.op, Coalesce);
            assert_eq!(result_expr.exprs.len(), 2); // Only columns should remain

            // Check that only columns remain
            if let Expr::Column(col) = &result_expr.exprs[0] {
                assert_eq!(col.len(), 1);
                assert_eq!(col[0], "x");
            } else {
                panic!("Expected column expression");
            }
            if let Expr::Column(col) = &result_expr.exprs[1] {
                assert_eq!(col.len(), 1);
                assert_eq!(col[0], "y");
            } else {
                panic!("Expected column expression");
            }
        }
    }

    #[test]
    fn test_transform_expr_variadic_all_children_removed() {
        // Test edge case where all children are removed - should return None
        struct RemoveAll;
        impl<'a> ExpressionTransform<'a> for RemoveAll {
            transform_output_type!(|'a, T| Option<Cow<'a, T>>);

            fn transform_expr_literal(&mut self, _value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
                None
            }
            fn transform_expr_column(
                &mut self,
                _name: &'a ColumnName,
            ) -> Option<Cow<'a, ColumnName>> {
                None
            }
        }

        let variadic_expr = VariadicExpression::new(Coalesce, vec![lit(1), col!("x"), lit("test")]);

        let mut transform = RemoveAll;
        let result = transform.transform_expr_variadic(&variadic_expr);

        assert!(result.is_none());
    }

    #[test]
    fn test_transform_expr_variadic_mixed_transformations() {
        // Test mixed scenario: some children transformed, some removed, some unchanged
        struct MixedTransform;
        impl<'a> ExpressionTransform<'a> for MixedTransform {
            transform_output_type!(|'a, T| Option<Cow<'a, T>>);

            fn transform_expr_literal(&mut self, value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
                match value {
                    Scalar::Integer(1) => None,                 // Remove literal 1
                    Scalar::String(s) if s == "remove" => None, // Remove "remove" string
                    Scalar::Integer(n) => Some(Cow::Owned(Scalar::Integer(n * 2))), /* Double other integers */
                    _ => Some(Cow::Borrowed(value)), // Keep others unchanged
                }
            }
            fn transform_expr_column(
                &mut self,
                name: &'a ColumnName,
            ) -> Option<Cow<'a, ColumnName>> {
                if name.len() == 1 && name[0] == "transform_me" {
                    Some(Cow::Owned(column_name!("transformed")))
                } else {
                    Some(Cow::Borrowed(name))
                }
            }
        }

        let variadic_expr = VariadicExpression::new(
            Coalesce,
            vec![
                lit(1),               // Will be removed
                col!("unchanged"),    // Will stay unchanged
                lit(5),               // Will be transformed to 10
                lit("remove"),        // Will be removed
                col!("transform_me"), // Will be transformed
                lit("keep"),          // Will stay unchanged
            ],
        );

        let mut transform = MixedTransform;
        let result = transform.transform_expr_variadic(&variadic_expr);

        assert!(matches!(result, Some(Cow::Owned(_))));
        if let Some(Cow::Owned(result_expr)) = result {
            assert_eq!(result_expr.op, Coalesce);
            assert_eq!(result_expr.exprs.len(), 4); // 2 removed, 4 remaining

            // Check remaining expressions in order
            if let Expr::Column(col) = &result_expr.exprs[0] {
                assert_eq!(col.len(), 1);
                assert_eq!(col[0], "unchanged");
            } else {
                panic!("Expected unchanged column");
            }

            assert_eq!(result_expr.exprs[1], lit(10)); // 5 * 2

            if let Expr::Column(col) = &result_expr.exprs[2] {
                assert_eq!(col.len(), 1);
                assert_eq!(col[0], "transformed");
            } else {
                panic!("Expected transformed column");
            }

            assert_eq!(result_expr.exprs[3], lit("keep"));
        }
    }

    fn test_output_schema() -> Arc<StructType> {
        schema_ref! {
            nullable "a": LONG,
            nullable "b": STRING,
        }
    }

    #[test]
    fn test_transform_expr_parse_json_noop() {
        // Test default no-op behavior - should return Cow::Borrowed
        let parse_json_expr = ParseJsonExpression::new(col!("json_col"), test_output_schema());

        let mut transform = NoopTransform;
        let result = transform.transform_expr_parse_json(&parse_json_expr);

        assert!(matches!(result, Cow::Borrowed(_)));
        if let Cow::Borrowed(result_expr) = result {
            assert_eq!(result_expr, &parse_json_expr);
        }
    }

    #[test]
    fn test_map_to_struct_options_survive_child_transform() {
        let expr = Expr::map_to_struct(
            col!("old_col"),
            MapToStructOptions::default().with_timestamp_timezone("Europe/Berlin"),
        );
        let Expr::MapToStruct(transformed) = ColumnReplacer(Some("new_col"))
            .transform_expr(&expr)
            .unwrap()
            .into_owned()
        else {
            panic!("expected map-to-struct expression");
        };
        assert_eq!(transformed.map_expr.as_ref(), &col!("new_col"));
        assert_eq!(
            transformed.options.timestamp_timezone(),
            Some("Europe/Berlin")
        );
    }

    #[test]
    fn test_transform_expr_parse_json_nested_child() {
        // Test with a more complex nested child expression
        struct LiteralDoubler;
        impl<'a> ExpressionTransform<'a> for LiteralDoubler {
            transform_output_type!(|'a, T| Cow<'a, T>);

            fn transform_expr_literal(&mut self, value: &'a Scalar) -> Cow<'a, Scalar> {
                if let Scalar::Integer(n) = value {
                    Cow::Owned(Scalar::Integer(n * 2))
                } else {
                    Cow::Borrowed(value)
                }
            }
        }

        // ParseJson with a binary expression as child: column + 5
        let child_expr = col!("x") + lit(5);
        let parse_json_expr = ParseJsonExpression::new(child_expr, test_output_schema());

        let mut transform = LiteralDoubler;
        let result = transform.transform_expr_parse_json(&parse_json_expr);

        assert!(matches!(result, Cow::Owned(_)));
        if let Cow::Owned(result_expr) = result {
            // The literal 5 should have been doubled to 10
            if let Expr::Binary(binary) = result_expr.json_expr.as_ref() {
                if let Expr::Literal(Scalar::Integer(n)) = &*binary.right {
                    assert_eq!(*n, 10);
                } else {
                    panic!("Expected integer literal");
                }
            } else {
                panic!("Expected binary expression");
            }
        }
    }

    #[test]
    fn test_case_when_depth_checker_counts_nested_cases() {
        let mut expr = lit(1);
        for _ in 0..8 {
            expr = Expr::case_when([(Pred::TRUE, expr)], lit(0));
        }
        assert_eq!(ExpressionDepthChecker::check_expr(&expr, 10), 7);
        assert_eq!(ExpressionDepthChecker::check_expr(&expr, 3), 4);
    }

    #[test]
    fn test_depth_checker() {
        let pred = Pred::or_from([
            Pred::and_from([
                Pred::opaque(
                    OpaqueTestOp("opaque".to_string()),
                    vec![lit(10) + col!("x"), Expr::unknown("unknown") - col!("b")],
                ),
                Pred::TRUE,
                Pred::not(Pred::TRUE),
            ]),
            Pred::and_from([
                Pred::is_null(col!("b")),
                Pred::gt(lit(10), col!("x")),
                Pred::or(
                    Pred::gt(
                        lit(5)
                            + Expr::opaque(OpaqueTestOp("inscrutable".to_string()), vec![lit(10)]),
                        lit(20),
                    ),
                    column_pred!("y"),
                ),
                Pred::unknown("mystery"),
            ]),
            Pred::eq(lit(42), Expr::struct_from([lit(10), col!("b")])),
        ]);

        // Verify the default/no-op transform, since we have this nice complex expression handy.
        assert!(matches!(
            NoopTransform.transform_pred(&pred),
            std::borrow::Cow::Borrowed(_)
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
        assert_eq!(check_with_call_count(1), (2, 3));

        // OR
        //  * AND
        //    * OPAQUE
        //      * PLUS      >LIMIT<
        //      * MINUS
        //    * NOT
        //  * AND
        //  * EQ
        assert_eq!(check_with_call_count(2), (3, 4));

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
        assert_eq!(check_with_call_count(3), (4, 12));

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
        assert_eq!(check_with_call_count(4), (5, 13));

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
        assert_eq!(check_with_call_count(1), (2, 3));
        assert_eq!(check_with_call_count(2), (3, 4));
        assert_eq!(check_with_call_count(3), (4, 5));
        assert_eq!(check_with_call_count(4), (5, 13));
        assert_eq!(check_with_call_count(5), (6, 14));
        assert_eq!(check_with_call_count(6), (6, 16));
        assert_eq!(check_with_call_count(7), (6, 16));
    }

    #[test]
    fn test_depth_checker_counts_cast_expressions() {
        let expr = Expr::cast(Expr::cast(col!("x"), DataType::INTEGER), DataType::LONG);

        assert_eq!(
            ExpressionDepthChecker::check_expr_with_call_count(&expr, 0),
            (1, 2)
        );
    }

    #[test]
    fn transform_junction_to_single_child_unwraps() {
        // A transform that removes one child from AND(a, b) should produce the surviving
        // predicate directly, not a degenerate single-element junction AND(a).
        struct LiteralRemover;
        impl<'a> ExpressionTransform<'a> for LiteralRemover {
            transform_output_type!(|'a, T| Option<Cow<'a, T>>);

            fn transform_expr_literal(&mut self, _value: &'a Scalar) -> Option<Cow<'a, Scalar>> {
                None
            }
        }

        let pred = Pred::and(column_pred!("x"), Pred::TRUE);
        let mut transform = LiteralRemover;
        let result = transform.transform_pred(&pred);
        let result = result.map(Cow::into_owned);
        assert_eq!(result.as_ref(), Some(&column_pred!("x")));
        assert!(!matches!(result, Some(Pred::Junction(_))));
    }

    #[test]
    fn transform_junction_removing_all_children_returns_none() {
        // Removing all children propagates None (the junction is dropped entirely),
        // rather than producing an empty junction or identity literal.
        struct ColumnRemover;
        impl<'a> ExpressionTransform<'a> for ColumnRemover {
            transform_output_type!(|'a, T| Option<Cow<'a, T>>);

            fn transform_expr_column(
                &mut self,
                _name: &'a ColumnName,
            ) -> Option<Cow<'a, ColumnName>> {
                None
            }
        }

        let pred = Pred::and(column_pred!("x"), column_pred!("y"));
        let mut transform = ColumnRemover;
        assert!(transform.transform_pred(&pred).is_none());
    }
}
