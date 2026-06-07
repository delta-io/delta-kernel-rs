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
use opaque_eval::{COpaqueEvalCallbacks, EvalMode};

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

// === FfiOpaquePredicateOp ===================================================

/// Engine-defined opaque predicate identified by a name (e.g. `STARTS_WITH`, `LIKE`). Optionally
/// carries an [`COpaqueEvalCallbacks`] reference; when attached, kernel routes row-time evaluation
/// through the engine's `eval_pred` callback (engine receives pre-evaluated args as Arrow arrays).
///
/// # Data skipping
///
/// `as_data_skipping_predicate` (StatsMode) rewrites each `Column` arg into a
/// `Struct[min, max, nullcount, rowcount]` and tags the op with `EvalMode::StatsMode`. Literals
/// pass through; `Predicate` children recurse. This is the only mode that prunes files -- RowMode
/// evaluates per data row and never drops files. See [`EngineEvalPredFn`] for the per-mode arg
/// shapes the engine callback must handle.
///
/// Bare ops (`Predicate::opaque(FfiOpaquePredicateOp::new(...))`) have no callback attached
/// and so abstain from file pruning. Scalar / partition-pruning paths always abstain.
///
/// Inverted predicates (`NOT op`) are rewritten too: the inversion is recorded on the op and
/// forwarded to the engine callback's `inverted` flag, which must reason about the negated op (see
/// [`EngineEvalPredFn`]). The StatsMode rewrite abstains (keeps all files) when a `Column` arg has
/// no sibling literal to infer its type from, or an arg is some kind other than `Column`,
/// `Literal`, or `Predicate`.
///
/// [`COpaqueEvalCallbacks`]: opaque_eval::COpaqueEvalCallbacks
/// [`EngineEvalPredFn`]: opaque_eval::EngineEvalPredFn
#[derive(Debug, Clone)]
pub(crate) struct FfiOpaquePredicateOp {
    name: String,
    #[cfg(feature = "default-engine-base")]
    callbacks: Option<Arc<COpaqueEvalCallbacks>>,
    /// Mode forwarded to the engine callback during eval. Defaults to `RowMode`.
    #[cfg(feature = "default-engine-base")]
    mode: EvalMode,
    /// Whether the op is negated. Set by the StatsMode rewrite so the eval callback can reason
    /// about the negated op (see [`EngineEvalPredFn`]); XORed with the eval-time `inverted`.
    #[cfg(feature = "default-engine-base")]
    inverted: bool,
}

impl FfiOpaquePredicateOp {
    /// Build an op identified by `name`, without any engine integrations.
    /// Kernel keeps every file and row.
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            #[cfg(feature = "default-engine-base")]
            callbacks: None,
            #[cfg(feature = "default-engine-base")]
            mode: EvalMode::RowMode,
            #[cfg(feature = "default-engine-base")]
            inverted: false,
        }
    }

    /// Consume `self` and return it with `callbacks` attached.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_callbacks(mut self, callbacks: Arc<COpaqueEvalCallbacks>) -> Self {
        self.callbacks = Some(callbacks);
        self
    }

    /// Consume `self` and return it with `mode` overridden.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_mode(mut self, mode: EvalMode) -> Self {
        self.mode = mode;
        self
    }

    /// Consume `self` and return it with `inverted` overridden.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_inverted(mut self, inverted: bool) -> Self {
        self.inverted = inverted;
        self
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn op_name(&self) -> &str {
        &self.name
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn callbacks_clone(&self) -> Option<Arc<COpaqueEvalCallbacks>> {
        self.callbacks.clone()
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn mode(&self) -> EvalMode {
        self.mode
    }

    #[cfg(feature = "default-engine-base")]
    pub(crate) fn inverted(&self) -> bool {
        self.inverted
    }
}

impl PartialEq for FfiOpaquePredicateOp {
    fn eq(&self, other: &Self) -> bool {
        // The name identifies the op. Under `default-engine-base` the op also carries an eval mode,
        // an inversion flag, and a callback bundle, all of which change how it evaluates, so
        // equality must include them: same mode, same inversion, and the same callbacks by `Arc`
        // pointer identity (callbacks aren't otherwise comparable).
        let names_eq = self.name == other.name;
        #[cfg(feature = "default-engine-base")]
        let extras_eq = self.mode == other.mode
            && self.inverted == other.inverted
            && match (&self.callbacks, &other.callbacks) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
        #[cfg(not(feature = "default-engine-base"))]
        let extras_eq = true;
        names_eq && extras_eq
    }
}

impl Eq for FfiOpaquePredicateOp {}

impl OpaquePredicateOp for FfiOpaquePredicateOp {
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
        // engine callback. Ops built via `Predicate::opaque(FfiOpaquePredicateOp::new(...))`
        // have no callback to dispatch to, so file pruning is impossible regardless: abstain.
        None
    }
}

// This FFI exposes only opaque predicates, not opaque expressions; see the `opaque_eval` module
// docs for why (expression-level functions fold into composite opaque predicates).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::cmp::Ordering;

    use delta_kernel::expressions::{
        BinaryPredicateOp, ColumnName, JunctionPredicateOp, OpaquePredicateOp as _,
        OpaquePredicateOpRef, Scalar,
    };
    use delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator;
    use delta_kernel::schema::DataType as SchemaDataType;

    use super::*;

    #[test]
    fn named_opaque_predicate_op_carries_name() {
        let op = FfiOpaquePredicateOp::new("STARTS_WITH");
        assert_eq!(op.name(), "STARTS_WITH");
    }

    #[test]
    fn named_opaque_predicate_op_equality() {
        // Name is the baseline identity.
        assert_eq!(
            FfiOpaquePredicateOp::new("LIKE"),
            FfiOpaquePredicateOp::new("LIKE")
        );
        assert_ne!(
            FfiOpaquePredicateOp::new("LIKE"),
            FfiOpaquePredicateOp::new("OTHER")
        );

        // Under default-engine-base, equality also distinguishes eval mode and callback identity,
        // since both change how the op evaluates.
        #[cfg(feature = "default-engine-base")]
        {
            use std::ffi::c_void;
            use std::sync::Arc;

            use super::opaque_eval::{COpaqueEvalCallbacks, EvalMode};
            use crate::engine_data::ArrowFFIData;
            use crate::{KernelStringSlice, OptionalValue};

            unsafe extern "C" fn stub_eval(
                _: *mut c_void,
                _: KernelStringSlice,
                _: *mut ArrowFFIData,
                _: EvalMode,
                _: bool,
            ) -> OptionalValue<*mut ArrowFFIData> {
                OptionalValue::None
            }
            unsafe extern "C" fn stub_free(_: *mut c_void) {}
            let make_cb = || {
                Arc::new(COpaqueEvalCallbacks {
                    engine_state: std::ptr::null_mut(),
                    eval_pred: stub_eval,
                    free_state: stub_free,
                })
            };

            // Same name, different mode -> not equal.
            assert_ne!(
                FfiOpaquePredicateOp::new("OP"),
                FfiOpaquePredicateOp::new("OP").with_mode(EvalMode::StatsMode)
            );

            // Same name, different inversion -> not equal.
            assert_ne!(
                FfiOpaquePredicateOp::new("OP"),
                FfiOpaquePredicateOp::new("OP").with_inverted(true)
            );

            // Same callbacks Arc (cloned) -> equal; distinct Arcs -> not equal (pointer identity).
            let cb = make_cb();
            let shared = FfiOpaquePredicateOp::new("OP").with_callbacks(cb.clone());
            assert_eq!(shared, FfiOpaquePredicateOp::new("OP").with_callbacks(cb));
            assert_ne!(
                shared,
                FfiOpaquePredicateOp::new("OP").with_callbacks(make_cb())
            );

            // Callbacks present vs absent -> not equal.
            assert_ne!(shared, FfiOpaquePredicateOp::new("OP"));
        }
    }

    /// The kernel-trait impl always abstains -- the real rewrite lives on the
    /// `ArrowOpaquePredicateOp` impl in `arrow_eval.rs`.
    #[test]
    fn kernel_trait_as_data_skipping_predicate_always_returns_none() {
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

        let op = FfiOpaquePredicateOp::new("ANY");
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
