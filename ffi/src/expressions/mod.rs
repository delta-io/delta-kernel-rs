//! This module holds functionality for moving expressions across the FFI boundary, both from
//! engine to kernel, and from kernel to engine.
use std::ffi::c_void;
use std::sync::Arc;

use delta_kernel::expressions::{OpaqueExpressionOp, OpaquePredicateOp, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::{DeltaResult, Expression, Predicate};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice};

pub mod engine_visitor;
pub mod kernel_visitor;

#[cfg(feature = "default-engine-base")]
pub mod opaque_eval;

#[cfg(feature = "default-engine-base")]
mod arrow_eval;

#[cfg(feature = "default-engine-base")]
use opaque_eval::OpaqueEvalCallbacks;

#[handle_descriptor(target=Expression, mutable=false, sized=true)]
pub struct SharedExpression;

#[handle_descriptor(target=Predicate, mutable=false, sized=true)]
pub struct SharedPredicate;

#[handle_descriptor(target=dyn OpaquePredicateOp, mutable=false, sized=false)]
pub struct SharedOpaquePredicateOp;

#[handle_descriptor(target=dyn OpaqueExpressionOp, mutable=false, sized=false)]
pub struct SharedOpaqueExpressionOp;

/// Free the memory the passed SharedExpression
///
/// # Safety
/// Engine is responsible for passing a valid SharedExpression
#[no_mangle]
pub unsafe extern "C" fn free_kernel_expression(data: Handle<SharedExpression>) {
    data.drop_handle();
}

/// Free the memory the passed SharedPredicate
///
/// # Safety
/// Engine is responsible for passing a valid SharedPredicate
#[no_mangle]
pub unsafe extern "C" fn free_kernel_predicate(data: Handle<SharedPredicate>) {
    data.drop_handle();
}

/// Free the passed SharedOpaqueExpressionOp
///
/// # Safety
/// Engine is responsible for passing a valid SharedOpaqueExpressionOp
#[no_mangle]
pub unsafe extern "C" fn free_kernel_opaque_expression_op(data: Handle<SharedOpaqueExpressionOp>) {
    data.drop_handle();
}

/// Free the passed SharedOpaquePredicateOp
///
/// # Safety
/// Engine is responsible for passing a valid SharedOpaquePredicateOp
#[no_mangle]
pub unsafe extern "C" fn free_kernel_opaque_predicate_op(data: Handle<SharedOpaquePredicateOp>) {
    data.drop_handle();
}

/// Visits the name of a SharedOpaqueExpressionOp
///
/// # Safety
/// Engine is responsible for passing a valid SharedOpaqueExpressionOp
#[no_mangle]
pub unsafe extern "C" fn visit_kernel_opaque_expression_op_name(
    op: Handle<SharedOpaqueExpressionOp>,
    data: *mut c_void,
    visit: extern "C" fn(data: *mut c_void, name: KernelStringSlice),
) {
    let op = unsafe { op.as_ref() };
    let name = op.name();
    visit(data, kernel_string_slice!(name));
}

/// Visits the name of a SharedOpaquePredicateOp
///
/// # Safety
/// Engine is responsible for passing a valid SharedOpaquePredicateOp
#[no_mangle]
pub unsafe extern "C" fn visit_kernel_opaque_predicate_op_name(
    op: Handle<SharedOpaquePredicateOp>,
    data: *mut c_void,
    visit: extern "C" fn(data: *mut c_void, name: KernelStringSlice),
) {
    let op = unsafe { op.as_ref() };
    let name = op.name();
    visit(data, kernel_string_slice!(name));
}

// === NamedOpaquePredicateOp ===================================================

/// Engine-defined opaque predicate identified by a name (e.g. `STARTS_WITH`,
/// `LIKE`). Two independent optional facets:
///
/// - `callbacks`: an [`OpaqueEvalCallbacks`] reference. When attached, kernel routes row-time
///   evaluation through the engine's `eval_pred` callback (engine receives pre-evaluated args as
///   Arrow arrays). Available under `default-engine-base` only.
/// - `skipping_decomp`: a kernel-native [`Predicate`] using LOGICAL column refs that's structurally
///   equivalent (or a coarse over-approximation) to this op for data-skipping purposes. When
///   attached, `as_data_skipping_predicate` runs it through the
///   `IndirectDataSkippingPredicateEvaluator` -- kernel rewrites `col` ->
///   `stats_parsed.maxValues.col` / `minValues.col` automatically.
///
/// Both facets are independent. Engines can attach either or both:
/// - eval only: row-time eval works, file pruning abstains
/// - skipping only: kernel-side file pruning works, row-time eval errors
/// - both: full coverage (row-time + file pruning)
/// - neither: opaque op is opaque end-to-end; kernel keeps every file and the engine must filter on
///   its side
///
/// The scalar/partition-pruning paths always abstain (`Ok(None)`).
///
/// [`OpaqueEvalCallbacks`]: opaque_eval::OpaqueEvalCallbacks
#[derive(Debug, Clone)]
pub struct NamedOpaquePredicateOp {
    name: String,
    #[cfg(feature = "default-engine-base")]
    #[allow(dead_code)] // read via callbacks_clone() under default-engine-base
    callbacks: Option<Arc<OpaqueEvalCallbacks>>,
    /// Optional kernel-native decomposition used for data-skipping. Uses
    /// LOGICAL column refs; kernel's data-skipping evaluator rewrites them
    /// to `stats_parsed.minValues.*` / `maxValues.*` at evaluation time.
    skipping_decomp: Option<Predicate>,
}

impl NamedOpaquePredicateOp {
    /// Build an op identified by `name`, without any engine integrations.
    /// Kernel keeps every file and row.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            #[cfg(feature = "default-engine-base")]
            callbacks: None,
            skipping_decomp: None,
        }
    }

    /// Build an op that consults `callbacks` during row-time predicate
    /// evaluation through the Arrow batch evaluator.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_callbacks(
        name: impl Into<String>,
        callbacks: Arc<OpaqueEvalCallbacks>,
    ) -> Self {
        Self {
            name: name.into(),
            callbacks: Some(callbacks),
            skipping_decomp: None,
        }
    }

    /// Build an op with a data-skipping decomposition only (no row-time eval).
    pub(crate) fn with_skipping_decomp(name: impl Into<String>, decomp: Predicate) -> Self {
        Self {
            name: name.into(),
            #[cfg(feature = "default-engine-base")]
            callbacks: None,
            skipping_decomp: Some(decomp),
        }
    }

    /// Build an op with both row-time eval callbacks AND a data-skipping
    /// decomposition.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_callbacks_and_skipping(
        name: impl Into<String>,
        callbacks: Arc<OpaqueEvalCallbacks>,
        decomp: Predicate,
    ) -> Self {
        Self {
            name: name.into(),
            callbacks: Some(callbacks),
            skipping_decomp: Some(decomp),
        }
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn op_name(&self) -> &str {
        &self.name
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn callbacks_clone(&self) -> Option<Arc<OpaqueEvalCallbacks>> {
        self.callbacks.clone()
    }
}

impl PartialEq for NamedOpaquePredicateOp {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for NamedOpaquePredicateOp {}

impl OpaquePredicateOp for NamedOpaquePredicateOp {
    fn name(&self) -> &str {
        &self.name
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        Ok(None)
    }

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
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        // Engines that want file pruning provide a kernel-native
        // decomposition using LOGICAL column refs (e.g. `col >= "foo"`).
        // Kernel's evaluator rewrites those to `stats_parsed.maxValues.col`
        // automatically while walking the decomposition.
        use delta_kernel::kernel_predicates::KernelPredicateEvaluator;
        evaluator.eval_pred(self.skipping_decomp.as_ref()?, inverted)
    }
}

// === NamedOpaqueExpressionOp ===================================================

/// Engine-defined opaque expression identified by a name (e.g. `LOWER`,
/// `UPPER`). Optionally carries an [`OpaqueEvalCallbacks`] reference so the
/// engine can implement row-time evaluation via the `default-engine-base`
/// Arrow batch evaluator.
///
/// Scalar evaluation abstains via `Err`, the spec-defined "unsupported"
/// shape; this disqualifies the op from partition-level scalar pruning but
/// kernel swallows the error with a `warn!`, so it doesn't propagate as a
/// query failure.
///
/// [`OpaqueEvalCallbacks`]: opaque_eval::OpaqueEvalCallbacks
#[derive(Debug, Clone)]
pub struct NamedOpaqueExpressionOp {
    name: String,
    #[cfg(feature = "default-engine-base")]
    #[allow(dead_code)] // read via callbacks_clone() under default-engine-base
    callbacks: Option<Arc<OpaqueEvalCallbacks>>,
}

impl NamedOpaqueExpressionOp {
    /// Build an op identified by `name`, without engine callbacks.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            #[cfg(feature = "default-engine-base")]
            callbacks: None,
        }
    }

    /// Build an op that consults `callbacks` during row-time expression
    /// evaluation through the Arrow batch evaluator.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_callbacks(
        name: impl Into<String>,
        callbacks: Arc<OpaqueEvalCallbacks>,
    ) -> Self {
        Self {
            name: name.into(),
            callbacks: Some(callbacks),
        }
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn op_name(&self) -> &str {
        &self.name
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn callbacks_clone(&self) -> Option<Arc<OpaqueEvalCallbacks>> {
        self.callbacks.clone()
    }
}

impl PartialEq for NamedOpaqueExpressionOp {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for NamedOpaqueExpressionOp {}

impl OpaqueExpressionOp for NamedOpaqueExpressionOp {
    fn name(&self) -> &str {
        &self.name
    }

    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expression],
    ) -> DeltaResult<delta_kernel::expressions::Scalar> {
        // Spec-defined "unsupported"; kernel callers swallow with warn!().
        Err(delta_kernel::Error::generic(format!(
            "opaque FFI expression `{}` does not support scalar evaluation",
            self.name,
        )))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::cmp::Ordering;

    use delta_kernel::expressions::{
        BinaryPredicateOp, ColumnName, Expression as Expr, JunctionPredicateOp,
        OpaquePredicateOpRef,
    };
    use delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator;
    use delta_kernel::schema::DataType as SchemaDataType;
    use delta_kernel::Predicate as Pred;

    use super::*;

    #[test]
    fn named_opaque_predicate_op_carries_name() {
        let op = NamedOpaquePredicateOp::new("STARTS_WITH");
        assert_eq!(op.name(), "STARTS_WITH");
    }

    #[test]
    fn named_opaque_predicate_op_equality_by_name_only() {
        let a = NamedOpaquePredicateOp::new("LIKE");
        let b = NamedOpaquePredicateOp::new("LIKE");
        let c = NamedOpaquePredicateOp::new("OTHER");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn named_opaque_expression_op_carries_name() {
        let op = NamedOpaqueExpressionOp::new("LOWER");
        assert_eq!(op.name(), "LOWER");
    }

    #[test]
    fn named_opaque_expression_op_equality_by_name_only() {
        let a = NamedOpaqueExpressionOp::new("LOWER");
        let b = NamedOpaqueExpressionOp::new("LOWER");
        let c = NamedOpaqueExpressionOp::new("UPPER");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ============================================================================
    // Data-skipping decomposition: verifies that the stored decomp is run through
    // the evaluator and that column refs get rewritten to stats refs.
    // ============================================================================

    /// Minimal `DataSkippingPredicateEvaluator` impl that rewrites column refs
    /// to a stats-shaped column path, similar to the real kernel
    /// `DataSkippingPredicateCreator`. Used only by tests in this module to
    /// observe the rewrite that kernel performs when our op hands back its
    /// decomposition.
    struct StatsRewriter;

    fn stats_col(prefix: &str, col: &ColumnName) -> Expr {
        let mut name = ColumnName::new([prefix]);
        name = name.join(col);
        Expr::from(name)
    }

    impl DataSkippingPredicateEvaluator for StatsRewriter {
        type Output = Pred;
        type ColumnStat = Expr;

        fn get_min_stat(&self, col: &ColumnName, _: &SchemaDataType) -> Option<Expr> {
            Some(stats_col("minValues", col))
        }
        fn get_max_stat(&self, col: &ColumnName, _: &SchemaDataType) -> Option<Expr> {
            Some(stats_col("maxValues", col))
        }
        fn get_nullcount_stat(&self, _: &ColumnName) -> Option<Expr> {
            None
        }
        fn get_rowcount_stat(&self) -> Option<Expr> {
            None
        }

        fn eval_pred_scalar(
            &self,
            val: &delta_kernel::expressions::Scalar,
            inverted: bool,
        ) -> Option<Pred> {
            use delta_kernel::expressions::Scalar;
            match val {
                Scalar::Boolean(b) => Some(Pred::literal(*b != inverted)),
                _ => None,
            }
        }
        fn eval_pred_scalar_is_null(
            &self,
            _: &delta_kernel::expressions::Scalar,
            _: bool,
        ) -> Option<Pred> {
            None
        }
        fn eval_pred_is_null(&self, _: &ColumnName, _: bool) -> Option<Pred> {
            None
        }
        fn eval_pred_binary_scalars(
            &self,
            _: BinaryPredicateOp,
            _: &delta_kernel::expressions::Scalar,
            _: &delta_kernel::expressions::Scalar,
            _: bool,
        ) -> Option<Pred> {
            None
        }
        fn eval_pred_opaque(
            &self,
            _: &OpaquePredicateOpRef,
            _: &[Expression],
            _: bool,
        ) -> Option<Pred> {
            None
        }
        fn finish_eval_pred_junction(
            &self,
            op: JunctionPredicateOp,
            preds: &mut dyn Iterator<Item = Option<Pred>>,
            inverted: bool,
        ) -> Option<Pred> {
            let collected: Vec<Pred> = preds.collect::<Option<Vec<_>>>()?;
            let pred = Pred::junction(op, collected);
            Some(if inverted { Pred::not(pred) } else { pred })
        }
        fn eval_partial_cmp(
            &self,
            ord: Ordering,
            col: Expr,
            val: &delta_kernel::expressions::Scalar,
            inverted: bool,
        ) -> Option<Pred> {
            let base_op = match ord {
                Ordering::Less => BinaryPredicateOp::LessThan,
                Ordering::Greater => BinaryPredicateOp::GreaterThan,
                Ordering::Equal => BinaryPredicateOp::Equal,
            };
            let pred = Pred::binary(base_op, col, Expr::literal(val.clone()));
            Some(if inverted { Pred::not(pred) } else { pred })
        }
    }

    #[test]
    fn skipping_decomp_field_is_stored() {
        let decomp = Pred::literal(true);
        let op = NamedOpaquePredicateOp::with_skipping_decomp("STARTS_WITH", decomp.clone());
        assert_eq!(op.skipping_decomp.as_ref(), Some(&decomp));
    }

    #[test]
    fn as_data_skipping_predicate_returns_none_when_no_decomp() {
        use delta_kernel::expressions::OpaquePredicateOp;
        let op = NamedOpaquePredicateOp::new("ANY");
        let rewriter = StatsRewriter;
        let result = op.as_data_skipping_predicate(
            &rewriter as &dyn DataSkippingPredicateEvaluator<Output = Pred, ColumnStat = Expr>,
            &[],
            false,
        );
        assert!(result.is_none());
    }

    #[test]
    fn decomposition_is_rewritten_to_stats_column_refs() {
        use delta_kernel::expressions::{column_expr, OpaquePredicateOp};

        // Engine-side decomposition for STARTS_WITH(col, "foo"):
        //   col >= "foo" AND col < "fop"
        // (built as: NOT(col < "foo") AND col < "fop")
        let lo = Pred::not(Pred::lt(column_expr!("col"), Expr::literal("foo")));
        let hi = Pred::lt(column_expr!("col"), Expr::literal("fop"));
        let decomp = Pred::and_from([lo, hi]);

        let op = NamedOpaquePredicateOp::with_skipping_decomp("STARTS_WITH", decomp);

        // Kernel's evaluator walks the decomp and rewrites column refs.
        let rewriter = StatsRewriter;
        let stats_pred = op
            .as_data_skipping_predicate(
                &rewriter as &dyn DataSkippingPredicateEvaluator<Output = Pred, ColumnStat = Expr>,
                &[],
                false,
            )
            .expect("decomp should rewrite to a non-empty stats predicate");

        // Verify the rewrite touched stats columns. After the walk, the
        // predicate should mention "maxValues" and "minValues" instead of
        // raw "col".
        let serialized = format!("{stats_pred:?}");
        assert!(
            serialized.contains("maxValues"),
            "expected maxValues in rewritten predicate, got: {serialized}"
        );
        assert!(
            serialized.contains("minValues"),
            "expected minValues in rewritten predicate, got: {serialized}"
        );
        assert!(
            !serialized.contains("Column(ColumnName { path: [\"col\"] })"),
            "raw `col` ref should have been rewritten, got: {serialized}"
        );
    }
}
