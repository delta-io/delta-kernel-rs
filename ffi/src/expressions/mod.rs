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
pub mod pruning;

#[cfg(feature = "default-engine-base")]
mod arrow_pruning;

use pruning::OpaquePruningCallbacks;

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

// === NamedOpaquePredicateOp ====================================================

/// An [`OpaquePredicateOp`] that carries an engine-defined name and an
/// optional reference to the engine's [`OpaquePruningCallbacks`].
///
/// Engine-side row-level evaluation is always required: kernel has no way
/// to evaluate the op itself. With callbacks registered, kernel invokes
/// `eval_against_stats` once per stats metadata batch during file pruning;
/// without callbacks, the engine handles every row.
///
/// Partition pruning and parquet row-group skipping abstain for opaque ops
/// -- kernel keeps every partition / row group.
///
/// [`OpaquePruningCallbacks`]: pruning::OpaquePruningCallbacks
#[derive(Debug, Clone)]
pub struct NamedOpaquePredicateOp {
    name: String,
    #[allow(dead_code)] // used under default-engine-base
    callbacks: Option<Arc<OpaquePruningCallbacks>>,
}

impl NamedOpaquePredicateOp {
    /// Build an op without callbacks. The engine is responsible for
    /// row-time evaluation.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            callbacks: None,
        }
    }

    /// Build an op that consults `callbacks` during file pruning.
    pub(crate) fn with_callbacks(
        name: impl Into<String>,
        callbacks: Arc<OpaquePruningCallbacks>,
    ) -> Self {
        Self {
            name: name.into(),
            callbacks: Some(callbacks),
        }
    }

    /// Op name as a `&str`.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn op_name(&self) -> &str {
        &self.name
    }

    /// `true` if engine callbacks are registered.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn has_callbacks(&self) -> bool {
        self.callbacks.is_some()
    }

    /// Clone the callback `Arc`, if any.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn callbacks_clone(&self) -> Option<Arc<OpaquePruningCallbacks>> {
        self.callbacks.clone()
    }
}

impl PartialEq for NamedOpaquePredicateOp {
    fn eq(&self, other: &Self) -> bool {
        // Identity is the op name; callbacks are engine-side bookkeeping.
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
        // File pruning dispatch lives in arrow_pruning.rs (default-engine-base).
        None
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::cell::RefCell;
    use std::ffi::c_void;

    use super::*;
    use crate::TryFromStringSlice;

    #[test]
    fn named_opaque_op_carries_name() {
        let op = NamedOpaquePredicateOp::new("STARTS_WITH");
        assert_eq!(op.name(), "STARTS_WITH");
    }

    #[test]
    fn named_opaque_op_equality() {
        let a = NamedOpaquePredicateOp::new("LIKE");
        let b = NamedOpaquePredicateOp::new("LIKE");
        let c = NamedOpaquePredicateOp::new("OTHER");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn named_opaque_op_predicate_eq_compares_op_and_exprs() {
        let pred_a = Predicate::opaque(
            NamedOpaquePredicateOp::new("STARTS_WITH"),
            [Expression::literal("a"), Expression::literal("b")],
        );
        let pred_b = Predicate::opaque(
            NamedOpaquePredicateOp::new("STARTS_WITH"),
            [Expression::literal("a"), Expression::literal("b")],
        );
        let pred_c = Predicate::opaque(
            NamedOpaquePredicateOp::new("STARTS_WITH"),
            [Expression::literal("a")],
        );
        let pred_d = Predicate::opaque(
            NamedOpaquePredicateOp::new("LIKE"),
            [Expression::literal("a"), Expression::literal("b")],
        );
        assert_eq!(pred_a, pred_b);
        assert_ne!(pred_a, pred_c);
        assert_ne!(pred_a, pred_d);
    }

    #[test]
    fn named_opaque_op_round_trips_through_shared_handle() {
        let pred = Predicate::opaque(
            NamedOpaquePredicateOp::new("STARTS_WITH"),
            [Expression::literal("test")],
        );
        let op_ref = match pred {
            Predicate::Opaque(opaque) => opaque.op.clone(),
            other => panic!("expected Predicate::Opaque, got {other:?}"),
        };
        let handle: Handle<SharedOpaquePredicateOp> = op_ref.into();

        thread_local! {
            static CAPTURED: RefCell<Option<String>> = const { RefCell::new(None) };
        }
        extern "C" fn capture(_data: *mut c_void, name: KernelStringSlice) {
            // SAFETY: kernel guarantees `name` is valid for the duration of this call.
            let s = unsafe { String::try_from_slice(&name) }.unwrap();
            CAPTURED.with(|c| *c.borrow_mut() = Some(s));
        }

        // SAFETY: the handle is a valid SharedOpaquePredicateOp built above.
        // `clone_handle` bumps the Arc so the visitor can consume one handle
        // while we retain another to free.
        unsafe {
            let visit_copy = handle.clone_handle();
            visit_kernel_opaque_predicate_op_name(visit_copy, std::ptr::null_mut(), capture);
            free_kernel_opaque_predicate_op(handle);
        }

        let captured = CAPTURED.with(|c| c.borrow().clone());
        assert_eq!(captured.as_deref(), Some("STARTS_WITH"));
    }
}
