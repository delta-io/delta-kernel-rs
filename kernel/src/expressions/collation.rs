//! Collation-aware string comparisons.
//!
//! Kernel compares strings by their UTF-8 bytes and has no collation library. A comparison under
//! another collation is therefore plain data: [`collated_comparison`] names the collation, and the
//! engine evaluates the comparison with its own implementation of it. Kernel skips files for the
//! comparison using the bounds that writers recorded under that exact collation.

use std::cmp::Ordering;

use super::{
    BinaryPredicate, BinaryPredicateOp, Expression, OpaquePredicate, OpaquePredicateOp, Predicate,
    Scalar, ScalarExpressionEvaluator,
};
use crate::KernelResult;
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};

/// Creates a predicate that is `comparison` evaluated under `collation` instead of by the bytes
/// of its strings. `comparison` compares a value to a STRING literal, in either order, with
/// [`Predicate::eq`], `ne`, `lt`, `le`, `gt`, or `ge`. Kernel keeps every file for any other
/// predicate, and evaluating it fails.
///
/// `collation` is the exact `provider.name.version` identifier that writers record bounds under,
/// such as `icu.UNICODE_CI.75.1`. A scan can skip a file only when the value is a column whose
/// bounds the file records under that identifier, and it trusts those bounds to enclose the
/// file's values under the collation. It keeps every other file: Kernel never substitutes binary
/// bounds or the bounds of another version. Only
/// [`Scan::scan_metadata`](crate::scan::Scan::scan_metadata) and the scans built on it skip
/// files for the comparison; every other scan keeps every file.
///
/// Kernel cannot compare strings under a collation, so the engine's
/// [`EvaluationHandler`](crate::EvaluationHandler) evaluates the comparisons that
/// [`CollatedComparison::of`] recognizes. A handler that does not implement the collation must
/// fail, or evaluate the comparison to NULL. TRUE and FALSE are both unsafe, because a scan also
/// evaluates the negated comparison over a file's bounds.
///
/// ```
/// # use delta_kernel::expressions::{col, collated_comparison, lit};
/// // `name = 'alice'` under a case-insensitive collation.
/// let predicate = collated_comparison("icu.UNICODE_CI.75.1", col!("name").eq(lit("alice")));
/// ```
pub fn collated_comparison(collation: impl Into<String>, comparison: Predicate) -> Predicate {
    let collated = CollatedComparison {
        collation: collation.into(),
    };
    Predicate::opaque(collated, [Expression::from(comparison)])
}

/// The operation of a [`collated_comparison`]: its comparison holds under [`Self::collation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollatedComparison {
    collation: String,
}

impl CollatedComparison {
    /// Returns the collation and wrapped comparison of a [`collated_comparison`], or `None` if
    /// `predicate` is not one.
    pub fn of(predicate: &Predicate) -> Option<(&Self, &Predicate)> {
        let Predicate::Opaque(opaque) = predicate else {
            return None;
        };
        let (collated, compared) = Self::from_opaque(opaque)?;
        Some((collated, compared.comparison))
    }

    /// The exact `provider.name.version` identifier of the collation.
    pub fn collation(&self) -> &str {
        &self.collation
    }

    /// Like [`Self::of`], for the opaque predicate that carries a collated comparison.
    pub(crate) fn from_opaque(predicate: &OpaquePredicate) -> Option<(&Self, Compared<'_>)> {
        let collated = predicate.op.any_ref().downcast_ref::<Self>()?;
        Some((collated, Self::compared(&predicate.exprs)?))
    }

    /// Reads the arguments of a collated comparison. Returns `None` unless they are the one
    /// comparison that [`collated_comparison`] documents, so that every user of a collated
    /// comparison agrees on what one is.
    pub(crate) fn compared(exprs: &[Expression]) -> Option<Compared<'_>> {
        let [Expression::Predicate(comparison)] = exprs else {
            return None;
        };
        let mut unnegated = comparison.as_ref();
        let mut negated = false;
        while let Predicate::Not(inner) = unnegated {
            unnegated = inner;
            negated = !negated;
        }
        let Predicate::Binary(BinaryPredicate { op, left, right }) = unnegated else {
            return None;
        };
        let ordering = match op {
            BinaryPredicateOp::LessThan => Ordering::Less,
            BinaryPredicateOp::Equal => Ordering::Equal,
            BinaryPredicateOp::GreaterThan => Ordering::Greater,
            _ => return None,
        };
        let (ordering, value, literal) = match (left.as_ref(), right.as_ref()) {
            (value, Expression::Literal(Scalar::String(literal))) => (ordering, value, literal),
            (Expression::Literal(Scalar::String(literal)), value) => {
                (ordering.reverse(), value, literal)
            }
            _ => return None,
        };
        Some(Compared {
            comparison,
            ordering,
            value,
            literal,
            negated,
        })
    }
}

/// The comparison a [`collated_comparison`] wraps, read as `value <ordering> literal` and negated
/// if `negated`.
pub(crate) struct Compared<'a> {
    /// The comparison as it was written.
    pub(crate) comparison: &'a Predicate,
    pub(crate) ordering: Ordering,
    pub(crate) value: &'a Expression,
    pub(crate) literal: &'a str,
    pub(crate) negated: bool,
}

impl OpaquePredicateOp for CollatedComparison {
    fn name(&self) -> &str {
        "collated_comparison"
    }

    /// Kernel cannot compare strings under a collation, so the comparison is undecided.
    // An error would log a warning for every scan that statically checks its predicate.
    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> KernelResult<Option<bool>> {
        Ok(None)
    }

    /// Parquet footer statistics are ordered by UTF-8 bytes, so they cannot bound a collation.
    fn eval_as_data_skipping_predicate(
        &self,
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        None
    }

    /// A scan rewrites the comparison itself, over collation bounds this hook cannot reach.
    fn as_data_skipping_predicate(
        &self,
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<Predicate> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use crate::kernel_predicates::{
        DefaultKernelPredicateEvaluator, EmptyColumnResolver, KernelPredicateEvaluator as _,
    };

    const COLLATION: &str = "test.ASCII_CI.1";

    #[test]
    fn collated_comparison_exposes_its_collation_and_comparison() {
        let comparison = col!("name").lt(lit("bob"));
        let predicate = collated_comparison(COLLATION, comparison.clone());
        let (collated, compared) = CollatedComparison::of(&predicate).unwrap();
        assert_eq!(collated.collation(), COLLATION);
        assert_eq!(compared, &comparison);
        assert_eq!(CollatedComparison::of(&comparison), None);
        let not_a_comparison = collated_comparison(COLLATION, Predicate::is_null(col!("name")));
        assert_eq!(CollatedComparison::of(&not_a_comparison), None);
    }

    #[test]
    fn collated_comparisons_are_equal_when_collation_and_comparison_match() {
        let equal = collated_comparison(COLLATION, col!("name").eq(lit("bob")));
        assert_eq!(
            equal,
            collated_comparison(COLLATION, col!("name").eq(lit("bob")))
        );
        assert_ne!(
            equal,
            collated_comparison(COLLATION, col!("name").lt(lit("bob")))
        );
        assert_ne!(
            equal,
            collated_comparison("test.ASCII_CI.2", col!("name").eq(lit("bob")))
        );
    }

    #[test]
    fn collated_comparison_is_undecided_without_a_collation_implementation() {
        let evaluator = DefaultKernelPredicateEvaluator::from(EmptyColumnResolver);
        let predicate = collated_comparison(COLLATION, lit("bob").eq(lit("bob")));
        assert_eq!(evaluator.eval_pred(&predicate, false), None);
        assert_eq!(evaluator.eval_pred(&predicate, true), None);
    }
}
