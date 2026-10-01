//! This module holds functionality for moving expressions across the FFI boundary, both from
//! engine to kernel, and from kernel to engine.
//!
//! This FFI exposes only opaque predicates, not opaque expressions; see the `opaque_eval` module
//! docs for why (expression-level functions fold into composite opaque predicates).
use std::ffi::c_void;

use delta_kernel::expressions::{MapToStructOptions, OpaqueExpressionOp, OpaquePredicateOp};
use delta_kernel::{DeltaResult, Expression, Predicate};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice, OptionalValue};

pub mod engine_visitor;
pub mod kernel_visitor;

#[cfg(feature = "default-engine-base")]
pub mod opaque_eval;

#[cfg(feature = "default-engine-base")]
mod arrow_eval;

#[cfg(feature = "default-engine-base")]
pub(crate) use arrow_eval::FfiOpaquePredicateOp;

#[handle_descriptor(target=Expression, mutable=false, sized=true)]
pub struct SharedExpression;

#[handle_descriptor(target=Predicate, mutable=false, sized=true)]
pub struct SharedPredicate;

/// Borrowed FFI representation of map-to-struct options.
///
/// Any string slice is valid only for the duration of the call or callback receiving this value.
#[repr(C)]
pub struct FfiMapToStructOptions {
    /// Reader timezone for offset-less timestamps, or `None` for UTC.
    pub timestamp_timezone: OptionalValue<KernelStringSlice>,
}

impl FfiMapToStructOptions {
    pub(crate) fn from_kernel(options: &MapToStructOptions) -> Self {
        let timestamp_timezone = options.timestamp_timezone();
        Self {
            timestamp_timezone: timestamp_timezone
                .map(|timezone| kernel_string_slice!(timezone))
                .into(),
        }
    }

    unsafe fn try_to_kernel(&self) -> DeltaResult<MapToStructOptions> {
        let timestamp_timezone = Option::<&KernelStringSlice>::from(&self.timestamp_timezone)
            .map(|timezone| unsafe { timezone.try_to_string() })
            .transpose()?;
        Ok(
            timestamp_timezone.map_or_else(MapToStructOptions::default, |timezone| {
                MapToStructOptions::default().with_timestamp_timezone(timezone)
            }),
        )
    }
}

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
