//! This module holds functionality for moving expressions across the FFI boundary, both from
//! engine to kernel, and from kernel to engine.
use delta_kernel::{Expression, Predicate};
use delta_kernel_ffi_macros::handle_descriptor;

pub mod engine;
pub mod kernel;

#[handle_descriptor(target=Expression, mutable=false, sized=true)]
pub struct SharedExpression;

#[handle_descriptor(target=Predicate, mutable=false, sized=true)]
pub struct SharedPredicate;
