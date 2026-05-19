//! This module holds functionality for moving expressions across the FFI boundary, both from
//! engine to kernel, and from kernel to engine.
use std::ffi::c_void;

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
mod skipping;

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

/// An [`OpaquePredicateOp`] that carries only a name, intended for FFI consumers that need
/// to push an engine-defined predicate through the kernel without registering a Rust-side
/// evaluator.
///
/// Engine-side row-level evaluation is always required: kernel has no way to evaluate the op
/// itself. For kernel-side pruning, the internal `skipping` shim recognizes a curated list
/// of op names (`STARTS_WITH`) and supplies the appropriate rewrites and scalar evaluators.
/// Skipping for recognized ops assumes Delta's default binary string collation; engines
/// using non-default collations must not push the affected predicates through this op.
/// Ops outside the recognized list opt out of every pruning pass; the engine is
/// responsible for filtering them at row time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedOpaquePredicateOp {
    name: String,
}

impl NamedOpaquePredicateOp {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl OpaquePredicateOp for NamedOpaquePredicateOp {
    fn name(&self) -> &str {
        &self.name
    }

    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        if let Some(result) =
            skipping::evaluate_on_partition_value(&self.name, eval_expr, exprs, inverted)
        {
            return result;
        }
        // Unrecognized op -- emit the documented "no scalar eval" signal.
        // See `OpaquePredicateOp::eval_pred_scalar`: returning `Err` opts the
        // op out of partition pruning.
        Err(delta_kernel::Error::generic(format!(
            "NamedOpaquePredicateOp({}) does not support scalar evaluation",
            self.name
        )))
    }

    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        skipping::evaluate_for_row_group_skipping(&self.name, evaluator, exprs, inverted)
    }

    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        skipping::rewrite_for_file_pruning(&self.name, evaluator, exprs, inverted)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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
        assert_ne!(
            pred_a, pred_c,
            "different child arity must not compare equal"
        );
        assert_ne!(pred_a, pred_d, "different op name must not compare equal");
    }

    #[test]
    fn named_opaque_op_predicate_carries_op_through_construction() {
        let pred = Predicate::opaque(
            NamedOpaquePredicateOp::new("STARTS_WITH"),
            [Expression::literal("a")],
        );
        match pred {
            Predicate::Opaque(opaque) => assert_eq!(opaque.op.name(), "STARTS_WITH"),
            other => panic!("expected Predicate::Opaque, got {other:?}"),
        }
    }

    #[test]
    fn named_opaque_op_round_trips_through_shared_handle() {
        // Verifies the engine-side path: an opaque predicate built from NamedOpaquePredicateOp
        // can be wrapped into a SharedOpaquePredicateOp handle and the name read back via
        // visit_kernel_opaque_predicate_op_name -- the same path engine_visitor uses when it
        // hands the op back to the engine.
        use std::cell::RefCell;
        use std::ffi::c_void;

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
        unsafe {
            visit_kernel_opaque_predicate_op_name(handle, std::ptr::null_mut(), capture);
        }

        let captured = CAPTURED.with(|c| c.borrow().clone());
        assert_eq!(captured.as_deref(), Some("STARTS_WITH"));
    }
}
