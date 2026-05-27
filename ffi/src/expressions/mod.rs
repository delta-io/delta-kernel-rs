//! This module holds functionality for moving expressions across the FFI boundary, both from
//! engine to kernel, and from kernel to engine.
use std::ffi::c_void;
#[cfg(feature = "default-engine-base")]
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
use opaque_eval::{EvalMode, OpaqueEvalCallbacks};

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

/// Engine-defined opaque predicate identified by a name (e.g. `STARTS_WITH`, `LIKE`). Optionally
/// carries an [`OpaqueEvalCallbacks`] reference; when attached, kernel routes row-time evaluation
/// through the engine's `eval_pred` callback (engine receives pre-evaluated args as Arrow arrays).
///
/// # Data skipping
///
/// `as_data_skipping_predicate` rewrites each `Column` arg into a `Struct[min_ref, max_ref]` and
/// tags the op with `EvalMode::StatsMode`. Literals pass through; `Predicate` children recurse.
/// See [`EngineEvalPredFn`] for the per-mode arg shapes the engine callback must handle.
///
/// Bare ops (`Predicate::opaque(NamedOpaquePredicateOp::new(...))`) have no callback attached
/// and so abstain from file pruning. Scalar / partition-pruning paths always abstain.
///
/// Inverted ops (`inverted == true`) abstain -- per-op negation isn't expressible. Expression
/// kinds other than `Column`, `Literal`, and `Predicate` also abstain.
///
/// [`OpaqueEvalCallbacks`]: opaque_eval::OpaqueEvalCallbacks
/// [`EngineEvalPredFn`]: opaque_eval::EngineEvalPredFn
#[derive(Debug, Clone)]
pub struct NamedOpaquePredicateOp {
    name: String,
    #[cfg(feature = "default-engine-base")]
    #[allow(dead_code)] // read via callbacks_clone() under default-engine-base
    callbacks: Option<Arc<OpaqueEvalCallbacks>>,
    /// Mode forwarded to the engine callback during eval. Defaults to `RowMode`.
    #[cfg(feature = "default-engine-base")]
    mode: EvalMode,
}

impl NamedOpaquePredicateOp {
    /// Build an op identified by `name`, without any engine integrations.
    /// Kernel keeps every file and row.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            #[cfg(feature = "default-engine-base")]
            callbacks: None,
            #[cfg(feature = "default-engine-base")]
            mode: EvalMode::RowMode,
        }
    }

    /// Build an op that consults `callbacks` during eval. Defaults to `RowMode`.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_callbacks(
        name: impl Into<String>,
        callbacks: Arc<OpaqueEvalCallbacks>,
    ) -> Self {
        Self {
            name: name.into(),
            callbacks: Some(callbacks),
            mode: EvalMode::RowMode,
        }
    }

    /// Consume `self` and return it with `mode` overridden.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_mode(mut self, mode: EvalMode) -> Self {
        self.mode = mode;
        self
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn op_name(&self) -> &str {
        &self.name
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn callbacks_clone(&self) -> Option<Arc<OpaqueEvalCallbacks>> {
        self.callbacks.clone()
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn mode(&self) -> EvalMode {
        self.mode
    }
}

// Equality by name only. Safe today because kernel has no predicate CSE path; if it gains one,
// extend this to include `mode` and `callbacks` pointer identity.
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
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<Predicate> {
        // The real rewrite lives on the `ArrowOpaquePredicateOp` impl in `arrow_eval.rs` -- it
        // wraps the output via `Predicate::arrow_opaque` so runtime dispatch can reach the
        // engine callback. Ops built via `Predicate::opaque(NamedOpaquePredicateOp::new(...))`
        // have no callback to dispatch to, so file pruning is impossible regardless: abstain.
        None
    }
}

// === Note on opaque expressions ===============================================
//
// This FFI deliberately exposes only opaque PREDICATES, not opaque expressions. Engine-defined
// expression-level functions (e.g. `LOWER`, `UPPER`) can always be represented by folding them
// into a composite opaque predicate. For `STARTS_WITH(LOWER(col), "foo")`, the engine names a
// composite op and builds:
//
//     Predicate::Opaque("STARTS_WITH_LOWER", [Column("col"), Literal("foo")])
//
// Kernel pre-evaluates each arg natively (col -> column data, literal -> broadcast column) and
// hands them to the engine's `eval_pred` callback, which applies LOWER then STARTS_WITH
// internally. The combinator only fails for engine-defined expressions appearing inside
// kernel-native predicates (e.g. `Eq(LOWER(col), "FOO")`); engines can always re-shape such
// queries as a single opaque predicate.

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use delta_kernel::expressions::OpaquePredicateOp as _;

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

    /// The kernel-trait impl always abstains -- the real rewrite lives on the
    /// `ArrowOpaquePredicateOp` impl in `arrow_eval.rs`.
    #[test]
    fn kernel_trait_as_data_skipping_predicate_always_returns_none() {
        use std::cmp::Ordering;

        use delta_kernel::expressions::{
            BinaryPredicateOp, ColumnName, JunctionPredicateOp, OpaquePredicateOpRef, Scalar,
        };
        use delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator;
        use delta_kernel::schema::DataType as SchemaDataType;

        // Minimal evaluator stub -- the trait impl ignores its evaluator argument, so the body
        // is never exercised.
        struct NeverCalled;
        impl DataSkippingPredicateEvaluator for NeverCalled {
            type Output = Predicate;
            type ColumnStat = Expression;
            fn get_min_stat(&self, _: &ColumnName, _: &SchemaDataType) -> Option<Expression> {
                None
            }
            fn get_max_stat(&self, _: &ColumnName, _: &SchemaDataType) -> Option<Expression> {
                None
            }
            fn get_nullcount_stat(&self, _: &ColumnName) -> Option<Expression> {
                None
            }
            fn get_rowcount_stat(&self) -> Option<Expression> {
                None
            }
            fn eval_pred_scalar(&self, _: &Scalar, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_scalar_is_null(&self, _: &Scalar, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_is_null(&self, _: &ColumnName, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_binary_scalars(
                &self,
                _: BinaryPredicateOp,
                _: &Scalar,
                _: &Scalar,
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn eval_pred_opaque(
                &self,
                _: &OpaquePredicateOpRef,
                _: &[Expression],
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn finish_eval_pred_junction(
                &self,
                _: JunctionPredicateOp,
                _: &mut dyn Iterator<Item = Option<Predicate>>,
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn eval_partial_cmp(
                &self,
                _: Ordering,
                _: Expression,
                _: &Scalar,
                _: bool,
            ) -> Option<Predicate> {
                None
            }
        }

        let op = NamedOpaquePredicateOp::new("ANY");
        let evaluator = NeverCalled;
        let result = op.as_data_skipping_predicate(
            &evaluator
                as &dyn DataSkippingPredicateEvaluator<Output = Predicate, ColumnStat = Expression>,
            &[],
            false,
        );
        assert!(result.is_none());
    }
}
