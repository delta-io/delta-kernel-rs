//! Contains unwinding panics at fallible C entry points.
//!
//! Caught panics use the existing Kernel error and tracing paths.

use std::fmt::Display;
use std::panic::{AssertUnwindSafe, Location, catch_unwind};

use delta_kernel::KernelError;

use crate::error::{AllocateError, ExternResult, IntoExternResult};

/// Catches unwinding panics and converts the operation's result through `allocator`.
/// Panic diagnostics identify the FFI call site.
///
/// # Safety
///
/// The allocator's callbacks and context must remain valid through this call.
#[track_caller]
pub(crate) unsafe fn catch_panic_into_result<T, E>(
    allocator: &dyn AllocateError,
    f: impl FnOnce() -> Result<T, E>,
) -> ExternResult<T>
where
    E: From<KernelError>,
    Result<T, E>: IntoExternResult<T>,
{
    let result = catch(Location::caller(), f);
    unsafe { result.into_extern_result(allocator) }
}

/// Converts an unwinding panic from `f` into an internal error.
///
/// Keep all fallible work inside `f`, including parsing and serialization. Consumed handles must
/// become Rust owners before work that can panic, so unwinding drops them. Partially mutated
/// objects must not be reused; catching a panic does not roll back state or side effects.
/// Each nested C entry returning an error needs its own boundary.
pub(crate) fn catch<T, E>(operation: impl Display, f: impl FnOnce() -> Result<T, E>) -> Result<T, E>
where
    E: From<KernelError>,
{
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
            drop(payload);
            tracing::error!("{error}");
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests;
