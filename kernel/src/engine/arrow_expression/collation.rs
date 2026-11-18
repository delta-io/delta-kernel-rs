//! Evaluates collated comparisons with the collations an engine implements.
//!
//! A [`collated_comparison`](crate::expressions::collated_comparison) only names its collation.
//! An engine implements [`ArrowCollator`] over its collation library and returns a
//! [`CollationEvaluationHandler`] for it from
//! [`Engine::evaluation_handler`](crate::Engine::evaluation_handler).
//!
//! ```
//! # use std::cmp::Ordering;
//! # use std::sync::Arc;
//! # use delta_kernel::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
//! # use delta_kernel::engine::arrow_data::ArrowEngineData;
//! # use delta_kernel::engine::arrow_expression::collation::{
//! #     ArrowCollator, CollationEvaluationHandler,
//! # };
//! # use delta_kernel::expressions::{col, collated_comparison, lit};
//! # use delta_kernel::schema::schema_ref;
//! # use delta_kernel::{EvaluationHandler, KernelError, KernelResult};
//! #[derive(Debug)]
//! struct AsciiCaseInsensitive;
//!
//! impl ArrowCollator for AsciiCaseInsensitive {
//!     fn compare(
//!         &self,
//!         collation: &str,
//!         ordering: Ordering,
//!         values: &StringArray,
//!         literal: &str,
//!     ) -> KernelResult<BooleanArray> {
//!         if collation != "example.ASCII_CI.1" {
//!             return Err(KernelError::unsupported(format!("Unknown collation {collation}")));
//!         }
//!         let literal = literal.to_ascii_lowercase();
//!         let matches = |value: &str| value.to_ascii_lowercase().cmp(&literal) == ordering;
//!         Ok(values.iter().map(|value| value.map(matches)).collect())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let handler = CollationEvaluationHandler::new(Arc::new(AsciiCaseInsensitive));
//!
//! // `name = 'alice'`, ignoring ASCII case.
//! let predicate = collated_comparison("example.ASCII_CI.1", col!("name").eq(lit("alice")));
//! let evaluator = handler
//!     .new_predicate_evaluator(schema_ref! { nullable "name": STRING }, Arc::new(predicate))?;
//! let names: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
//! let batch = ArrowEngineData::new(RecordBatch::try_from_iter([("name", names)])?);
//! let result = ArrowEngineData::try_from_engine_data(evaluator.evaluate(&batch)?)?;
//! assert_eq!(
//!     result.record_batch().column(0).as_ref(),
//!     &BooleanArray::from(vec![true, false])
//! );
//! # Ok(())
//! # }
//! ```

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use super::ArrowEvaluationHandler;
use super::evaluate_expression::evaluate_expression;
use super::opaque::{ArrowOpaquePredicateOp, ArrowOpaquePredicateOpAdaptor};
use crate::arrow::array::{Array as _, AsArray as _, BooleanArray, RecordBatch, StringArray};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::compute::{cast, not};
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::expressions::collation::{CollatedComparison, Compared};
use crate::expressions::{
    Expression, ExpressionRef, OpaquePredicate, Predicate, PredicateRef, Scalar,
    ScalarExpressionEvaluator,
};
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::schema::{DataType, SchemaRef};
use crate::transforms::{ExpressionTransform, transform_output_type};
use crate::utils::require;
use crate::{
    EngineData, EvaluationHandler, ExpressionEvaluator, KernelError, KernelResult,
    PredicateEvaluator,
};

/// Compares strings under the collations an engine implements.
///
/// Implement each collation exactly, version included. Kernel skips files using bounds that
/// writers computed under it, so any other ordering can skip files that hold matching rows.
pub trait ArrowCollator: Debug + Send + Sync {
    /// Returns, for each of `values`, whether it compares to `literal` as `ordering` under
    /// `collation`: [`Ordering::Less`] tests `value < literal`.
    ///
    /// `collation` is an exact `provider.name.version` identifier such as `icu.UNICODE_CI.75.1`.
    /// Kernel calls this per batch, so cache any state built to resolve `collation`. A null entry
    /// leaves its row undecided, and Kernel nulls the entries of null values itself. For a
    /// collation the engine does not implement, return an error, which fails the scan, or all
    /// nulls, which keep every file but no row of a row filter. Never guess TRUE or FALSE: a scan
    /// also evaluates the negated comparison over file bounds.
    fn compare(
        &self,
        collation: &str,
        ordering: Ordering,
        values: &StringArray,
        literal: &str,
    ) -> KernelResult<BooleanArray>;
}

/// An [`EvaluationHandler`] that evaluates collated comparisons with an [`ArrowCollator`] and
/// everything else as [`ArrowEvaluationHandler`] does.
///
/// Evaluation fails for a collated comparison nested in a struct patch or in a struct's
/// nullability predicate. To customize evaluation further, wrap this handler.
pub struct CollationEvaluationHandler {
    collator: Arc<dyn ArrowCollator>,
}

impl CollationEvaluationHandler {
    /// Creates a handler that evaluates collated comparisons with `collator`.
    pub fn new(collator: Arc<dyn ArrowCollator>) -> Self {
        Self { collator }
    }
}

impl EvaluationHandler for CollationEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: ExpressionRef,
        output_type: DataType,
    ) -> KernelResult<Arc<dyn ExpressionEvaluator>> {
        let expression = match BindCollator(&self.collator).transform_expr(&expression) {
            Cow::Owned(bound) => Arc::new(bound),
            Cow::Borrowed(_) => expression,
        };
        ArrowEvaluationHandler.new_expression_evaluator(input_schema, expression, output_type)
    }

    fn new_predicate_evaluator(
        &self,
        input_schema: SchemaRef,
        predicate: PredicateRef,
    ) -> KernelResult<Arc<dyn PredicateEvaluator>> {
        let predicate = match BindCollator(&self.collator).transform_pred(&predicate) {
            Cow::Owned(bound) => Arc::new(bound),
            Cow::Borrowed(_) => predicate,
        };
        ArrowEvaluationHandler.new_predicate_evaluator(input_schema, predicate)
    }

    fn create_many(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<Scalar>>,
    ) -> KernelResult<Box<dyn EngineData>> {
        ArrowEvaluationHandler.create_many(schema, rows)
    }
}

/// Binds each collated comparison to the collator that evaluates it.
pub(crate) struct BindCollator<'c>(pub(crate) &'c Arc<dyn ArrowCollator>);

impl<'a> ExpressionTransform<'a> for BindCollator<'_> {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_pred_opaque(
        &mut self,
        predicate: &'a OpaquePredicate,
    ) -> Cow<'a, OpaquePredicate> {
        let Some(collated) = predicate.op.any_ref().downcast_ref::<CollatedComparison>() else {
            return self.recurse_into_pred_opaque(predicate);
        };
        let op = ArrowOpaquePredicateOpAdaptor(Box::new(BoundCollatedComparison {
            collation: collated.collation().to_owned(),
            collator: Arc::clone(self.0),
        }));
        // Also bind collated comparisons nested in the compared value.
        let exprs = self.recurse_into_pred_opaque(predicate).into_owned().exprs;
        Cow::Owned(OpaquePredicate::new(Arc::new(op), exprs))
    }
}

/// A collated comparison bound to the collator that evaluates it.
#[derive(Debug)]
struct BoundCollatedComparison {
    collation: String,
    collator: Arc<dyn ArrowCollator>,
}

impl BoundCollatedComparison {
    /// Compares `values` to `literal`. Null where the value or the collator's result is null.
    fn compare(
        &self,
        ordering: Ordering,
        values: &StringArray,
        literal: &str,
    ) -> KernelResult<BooleanArray> {
        let result = self
            .collator
            .compare(&self.collation, ordering, values, literal)?;
        require!(
            result.len() == values.len(),
            KernelError::internal_error(format!(
                "Collator returned {} results for {} values under collation '{}'",
                result.len(),
                values.len(),
                self.collation
            ))
        );
        let nulls = NullBuffer::union(result.nulls(), values.nulls());
        Ok(BooleanArray::new(result.values().clone(), nulls))
    }
}

impl PartialEq for BoundCollatedComparison {
    // A handler binds every comparison to its one collator, so the collator is not compared.
    fn eq(&self, other: &Self) -> bool {
        self.collation == other.collation
    }
}

impl ArrowOpaquePredicateOp for BoundCollatedComparison {
    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> KernelResult<BooleanArray> {
        let compared = compared(args)?;
        let values = evaluate_expression(compared.value, batch, Some(&DataType::STRING))?;
        let values = cast(&values, &ArrowDataType::Utf8)?;
        let values = values.as_string_opt().ok_or_else(|| {
            KernelError::internal_error("Casting to Utf8 did not produce a string array")
        })?;
        let result = self.compare(compared.ordering, values, compared.literal)?;
        Ok(if inverted != compared.negated {
            not(&result)?
        } else {
            result
        })
    }

    fn name(&self) -> &str {
        "collated_comparison"
    }

    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> KernelResult<Option<bool>> {
        let compared = compared(exprs)?;
        let Some(Scalar::String(value)) = eval_expr(compared.value) else {
            return Ok(None);
        };
        let values = StringArray::from(vec![value]);
        let result = self.compare(compared.ordering, &values, compared.literal)?;
        Ok(result
            .is_valid(0)
            .then(|| result.value(0) != (inverted != compared.negated)))
    }

    // A bound comparison exists only inside an evaluator, after any data skipping rewrite, so
    // neither skipping hook applies.
    fn eval_as_data_skipping_predicate(
        &self,
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        None
    }

    fn as_data_skipping_predicate(
        &self,
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<Predicate> {
        None
    }
}

fn compared(args: &[Expression]) -> KernelResult<Compared<'_>> {
    CollatedComparison::compared(args).ok_or_else(|| {
        KernelError::unsupported(format!(
            "A collated comparison must compare a value to a STRING literal with =, < or >, \
             possibly negated: {args:?}"
        ))
    })
}

/// Test collator: ASCII case-insensitive under the one collation it names, an error for any
/// other. Returns FALSE for null values, like a selection mask.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct AsciiCaseInsensitive(pub &'static str);

#[cfg(any(test, feature = "test-utils"))]
impl ArrowCollator for AsciiCaseInsensitive {
    fn compare(
        &self,
        collation: &str,
        ordering: Ordering,
        values: &StringArray,
        literal: &str,
    ) -> KernelResult<BooleanArray> {
        require!(
            collation == self.0,
            KernelError::unsupported(format!("Unknown collation '{collation}'"))
        );
        let literal = literal.to_ascii_lowercase();
        let matches = |value: &str| value.to_ascii_lowercase().cmp(&literal) == ordering;
        Ok(values
            .iter()
            .map(|value| Some(value.is_some_and(matches)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{ArrayRef, StringViewArray};
    use crate::engine::arrow_expression::evaluate_expression::evaluate_predicate;
    use crate::expressions::{col, collated_comparison, lit};

    const COLLATION: &str = "test.ASCII_CI.1";

    /// Returns the same result for every comparison.
    #[derive(Debug)]
    struct Fixed(BooleanArray);

    impl ArrowCollator for Fixed {
        fn compare(
            &self,
            _collation: &str,
            _ordering: Ordering,
            _values: &StringArray,
            _literal: &str,
        ) -> KernelResult<BooleanArray> {
            Ok(self.0.clone())
        }
    }

    fn evaluate(
        comparison: Predicate,
        names: ArrayRef,
        inverted: bool,
        collator: impl ArrowCollator + 'static,
    ) -> KernelResult<BooleanArray> {
        let predicate = collated_comparison(COLLATION, comparison);
        let collator: Arc<dyn ArrowCollator> = Arc::new(collator);
        evaluate_predicate(
            &BindCollator(&collator).transform_pred(&predicate),
            &RecordBatch::try_from_iter([("name", names)]).unwrap(),
            inverted,
        )
    }

    #[rstest]
    #[case::less(col!("name").lt(lit("bob")), [Some(true), None, Some(false), Some(false)])]
    #[case::equal(col!("name").eq(lit("bob")), [Some(false), None, Some(true), Some(false)])]
    #[case::greater(col!("name").gt(lit("bob")), [Some(false), None, Some(false), Some(true)])]
    #[case::not_less(col!("name").ge(lit("bob")), [Some(false), None, Some(true), Some(true)])]
    #[case::literal_first(lit("bob").gt(col!("name")), [Some(true), None, Some(false), Some(false)])]
    #[case::negated_twice(
        Predicate::not(Predicate::not(col!("name").gt(lit("bob")))),
        [Some(false), None, Some(false), Some(true)]
    )]
    fn collated_comparison_follows_the_collation_and_is_null_for_null_values(
        #[case] comparison: Predicate,
        #[case] expected: [Option<bool>; 4],
        #[values(false, true)] inverted: bool,
        #[values(false, true)] string_view: bool,
    ) {
        let names = [Some("Alice"), None, Some("BOB"), Some("carol")];
        let names: ArrayRef = if string_view {
            Arc::new(StringViewArray::from_iter(names))
        } else {
            Arc::new(StringArray::from_iter(names))
        };
        let result =
            evaluate(comparison, names, inverted, AsciiCaseInsensitive(COLLATION)).unwrap();
        let expected = expected.map(|verdict| verdict.map(|verdict| verdict != inverted));
        assert_eq!(result, BooleanArray::from(expected.to_vec()));
    }

    #[test]
    fn collated_comparison_nested_in_the_value_of_another_is_evaluated() {
        let names = Arc::new(StringArray::from(vec!["Alice", "BOB"]));
        let nested = collated_comparison(COLLATION, col!("name").eq(lit("bob")));
        let outer = Expression::case_when([(nested, lit("x"))], lit("y")).eq(lit("X"));
        let result = evaluate(outer, names, false, AsciiCaseInsensitive(COLLATION)).unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, true]));
    }

    #[rstest]
    fn collated_comparison_is_null_where_the_collator_reports_null(
        #[values(false, true)] inverted: bool,
    ) {
        let names = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let undecided = Fixed(BooleanArray::new_null(2));
        let comparison = col!("name").eq(lit("bob"));
        let result = evaluate(comparison, names, inverted, undecided).unwrap();
        assert_eq!(result, BooleanArray::new_null(2));
    }

    #[test]
    fn collated_comparison_rejects_a_result_with_the_wrong_length() {
        let names = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let misaligned = Fixed(BooleanArray::from(vec![true]));
        let comparison = col!("name").eq(lit("bob"));
        let error = evaluate(comparison, names, false, misaligned).unwrap_err();
        assert!(matches!(
            error.without_backtrace(),
            KernelError::InternalError(_)
        ));
    }

    #[rstest]
    #[case::two_columns(col!("name").eq(col!("name")))]
    #[case::not_a_comparison(Predicate::is_null(col!("name")))]
    #[case::distinct(Predicate::distinct(col!("name"), lit("bob")))]
    fn collated_predicate_that_is_not_a_comparison_fails_evaluation(#[case] wrapped: Predicate) {
        let names = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let error = evaluate(wrapped, names, false, AsciiCaseInsensitive(COLLATION)).unwrap_err();
        assert!(matches!(
            error.without_backtrace(),
            KernelError::Unsupported(_)
        ));
    }
}
