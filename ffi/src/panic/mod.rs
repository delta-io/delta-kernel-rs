//! Contains unwinding panics at fallible C entry points.
//!
//! Caught panics use the existing Kernel error and tracing paths.

use std::panic::{AssertUnwindSafe, Location, catch_unwind};

use delta_kernel::KernelError;

/// Converts an unwinding panic from `f` into an internal error.
/// Panic diagnostics identify the FFI call site.
///
/// Keep all work that can panic inside `f`, including parsing and serialization. Consumed handles
/// must become Rust owners before work that can panic, so unwinding drops them. Partially mutated
/// objects must not be reused; catching a panic does not roll back state or side effects.
/// Each nested C entry returning an error needs its own boundary.
#[track_caller]
pub(crate) fn catch_panic<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<T, E>
where
    E: From<KernelError>,
{
    let operation = Location::caller();
    // Each call site owns its in-progress work and discards it on failure. Requiring UnwindSafe
    // would exclude engine trait objects without proving the necessary ownership invariants.
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            let error = KernelError::InternalError(format!("Rust panic in {operation}: {message}"));
            // A custom panic payload can panic again when dropped.
            if let Err(nested) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(nested);
            }
            tracing::error!("{error}");
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests;
