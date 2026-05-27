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
/// `LIKE`). Optionally carries an [`OpaqueEvalCallbacks`] reference so the
/// engine can implement row-time evaluation via the `default-engine-base`
/// Arrow batch evaluator.
///
/// The scalar/partition-pruning paths abstain (`Ok(None)`). Data-skipping
/// decomposition (`as_data_skipping_predicate`) also abstains -- opaque
/// predicates currently don't participate in stats-based file pruning in
/// this design; engines can pre-filter on their side, or a future
/// extension can expose a kernel-native decomposition callback.
///
/// [`OpaqueEvalCallbacks`]: opaque_eval::OpaqueEvalCallbacks
#[derive(Debug, Clone)]
pub struct NamedOpaquePredicateOp {
    name: String,
    #[cfg(feature = "default-engine-base")]
    #[allow(dead_code)] // read via callbacks_clone() under default-engine-base
    callbacks: Option<Arc<OpaqueEvalCallbacks>>,
}

impl NamedOpaquePredicateOp {
    /// Build an op identified by `name`, without engine callbacks. Kernel
    /// has no way to evaluate the op; engines are responsible for any
    /// row-time filtering on their side.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            #[cfg(feature = "default-engine-base")]
            callbacks: None,
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
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<Predicate> {
        // Row-time eval dispatch lives in arrow_eval.rs (default-engine-base).
        // Stats-based decomposition is not exposed through this FFI; engines
        // that want file-skipping must pre-filter on their side.
        None
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
}
