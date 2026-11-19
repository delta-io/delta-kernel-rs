//! Collation-aware string comparisons.
//!
//! Kernel has no collation library. A [`collated_comparison`] names its collation, the engine
//! evaluates the comparison, and Kernel skips files using the bounds recorded under it.

use std::cmp::Ordering;

use super::{
    BinaryPredicate, BinaryPredicateOp, Expression, OpaquePredicateOp, Predicate, Scalar,
    ScalarExpressionEvaluator,
};
use crate::KernelResult;
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};

/// Creates a predicate that evaluates `comparison` under `collation` instead of by UTF-8 bytes.
///
/// `comparison` must compare a value to a STRING literal, in either order, with
/// [`Predicate::eq`], `ne`, `lt`, `le`, `gt`, or `ge`, or be the [`Predicate::not`] of one. Any
/// other predicate skips no files, and evaluating it fails. A valid comparison skips files only
/// when its value is a direct reference to a non-partition STRING column. On a partition column
/// or any other expression it skips no files, but can still be evaluated.
///
/// `collation` is the exact `provider.name.version` identifier that writers record bounds under,
/// such as `icu.UNICODE_CI.75.1`. [`Scan::scan_metadata`](crate::scan::Scan::scan_metadata),
/// [`Scan::parallel_scan_metadata`](crate::scan::Scan::parallel_scan_metadata), and the scans
/// built on them skip a file when its bounds for the compared column under that identifier
/// exclude a match, trusting those bounds to enclose the file's values, or when its null count
/// shows that the column holds only nulls. Kernel never substitutes binary bounds or another
/// version's, and every other scan keeps every file.
///
/// An [`EvaluationHandler`](crate::EvaluationHandler) evaluates the comparison by reading its
/// [`CollatedComparison`]. `engine::arrow_expression::collation::CollationEvaluationHandler`
/// does so over Arrow data.
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

/// The [`OpaquePredicateOp`] of a [`collated_comparison`], holding its collation identifier.
///
/// An engine that evaluates the predicate itself downcasts the op of an
/// [`OpaquePredicate`](super::OpaquePredicate) to this type and reads the comparison from the
/// predicate's arguments:
///
/// ```
/// # use std::cmp::Ordering;
/// # use delta_kernel::expressions::{
/// #     CollatedComparison, Predicate, col, collated_comparison, lit,
/// # };
/// let predicate = collated_comparison("icu.UNICODE_CI.75.1", lit("alice").ge(col!("name")));
/// let Predicate::Opaque(opaque) = &predicate else {
///     panic!("a collated comparison is an opaque predicate");
/// };
/// let collated = opaque.op.as_ref().any_ref().downcast_ref::<CollatedComparison>().unwrap();
/// assert_eq!(collated.collation(), "icu.UNICODE_CI.75.1");
///
/// // `'alice' >= name` reads as `NOT (name > 'alice')`.
/// let compared = CollatedComparison::compared(&opaque.exprs).unwrap();
/// assert_eq!(compared.value(), &col!("name"));
/// assert_eq!(compared.ordering(), Ordering::Greater);
/// assert_eq!(compared.literal(), "alice");
/// assert!(compared.negated());
/// ```
#[derive(Debug, PartialEq)]
pub struct CollatedComparison {
    collation: String,
}

impl CollatedComparison {
    /// The exact `provider.name.version` identifier of the collation.
    pub fn collation(&self) -> &str {
        &self.collation
    }

    /// Reads the arguments of a collated comparison. Returns `None` unless they are the one
    /// comparison that [`collated_comparison`] accepts.
    pub fn compared(exprs: &[Expression]) -> Option<Compared<'_>> {
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
            ordering,
            value,
            literal,
            negated,
        })
    }
}

/// The comparison a [`collated_comparison`] wraps, read as `value <ordering> literal` and negated
/// if [`negated`](Self::negated).
#[derive(Clone, Copy, Debug)]
pub struct Compared<'a> {
    ordering: Ordering,
    value: &'a Expression,
    literal: &'a str,
    negated: bool,
}

impl<'a> Compared<'a> {
    /// How the value compares to the literal: [`Ordering::Less`] reads `value < literal`.
    pub fn ordering(&self) -> Ordering {
        self.ordering
    }

    /// The value compared to the literal.
    pub fn value(&self) -> &'a Expression {
        self.value
    }

    /// The STRING literal the value is compared to.
    pub fn literal(&self) -> &'a str {
        self.literal
    }

    /// Whether the comparison is negated: `value >= literal` reads as a negated
    /// [`Ordering::Less`].
    pub fn negated(&self) -> bool {
        self.negated
    }
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
    use crate::expressions::lit;
    use crate::kernel_predicates::{
        DefaultKernelPredicateEvaluator, EmptyColumnResolver, KernelPredicateEvaluator as _,
    };

    #[test]
    fn collated_comparison_is_undecided_without_a_collation_implementation() {
        let evaluator = DefaultKernelPredicateEvaluator::from(EmptyColumnResolver);
        let predicate = collated_comparison("test.ASCII_CI.1", lit("bob").eq(lit("bob")));
        assert_eq!(evaluator.eval_pred(&predicate, false), None);
        assert_eq!(evaluator.eval_pred(&predicate, true), None);
    }
}
