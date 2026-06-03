//! FFI types for mimicking the kernel's `PlanResult` enum.

use delta_kernel::Error;

use super::iter::{CBytesIterator, CEngineDataIterator, CFileMetaIterator};
use crate::error::{EngineError, ExternResult};
use crate::scan::EngineSchema;
use crate::NullableCvoid;

/// Helper function for converting an engine-allocated [`EngineError`] (handed to kernel as `*mut
/// EngineError`) into a [`delta_kernel::Error`].
///
/// TODO: EngineError does not communicate the error message, so for now we just log a generic error
/// with the error code.
pub(crate) fn engine_error_to_kernel(err: *mut EngineError) -> Error {
    Error::generic(format!(
        "Engine error during plan execution: {:?}",
        unsafe { &*err }.etype
    ))
}

/// C-compatible equivalent of the kernel's `ParquetFooter` struct.
///
/// The schema is delivered as an [`EngineSchema`] - an opaque engine-owned schema which will be
/// materialized on the kernel-side via vistor pattern.
#[repr(C)]
pub struct CParquetFooter {
    pub schema: EngineSchema,
}

/// C-compatible equivalent of the kernel's `PlanResult` enum.
#[repr(C)]
pub enum CPlanResult {
    Unit,
    Data(CEngineDataIterator),
    FileMeta(CFileMetaIterator),
    Bytes(CBytesIterator),
    ParquetFooter(CParquetFooter),
}

/// An engine-allocated wrapper around an [`ExternResult<CPlanResult>`].
///
/// This wrapper allows the engine to attach an opaque `state` pointer and a `free` callback,
/// letting the kernel free all associated engine resources (i.e nested iterators and ExternResults)
/// when the result is no longer needed. `state` is opaque to kernel and is passed verbatim
/// back to `free`. Using a single wrapper + free callback allows the engine to conveniently
/// maintain a single arena per plan execution, rather than tracking resources separately for each
/// inner item of the PlanResult.
///
/// This wrapper additionally attaches an opaque `state` pointer and a `free` callback so kernel
/// can release all engine-side resources (nested iterator state, error allocations, etc.) when the
/// result is no longer needed. `state` is opaque to kernel and is passed verbatim to `free`.
///
/// A single wrapper + `free` callback lets the engine maintain one arena per plan execution rather
/// than tracking resources separately for each inner item of the `CPlanResult`. On the Rust-side,
/// the `state` + `free` pair should be converted into a `PlanResultCleanup` guard to ensure the
/// free callback is invoked properly.
///
/// # Safety
/// The engine must ensure that the `state` and `free` pointers are safe to send between threads.
#[repr(C)]
pub struct CPlanResultWrapper {
    pub result: ExternResult<CPlanResult>,
    pub state: NullableCvoid,
    pub free: extern "C" fn(state: NullableCvoid),
}

/// RAII guard for invoking [`CPlanResultWrapper::free`] exactly once on drop.
pub(crate) struct PlanResultCleanup {
    state: NullableCvoid,
    free: extern "C" fn(state: NullableCvoid),
}

impl PlanResultCleanup {
    pub(crate) fn new(state: NullableCvoid, free: extern "C" fn(state: NullableCvoid)) -> Self {
        Self { state, free }
    }
}

impl Drop for PlanResultCleanup {
    fn drop(&mut self) {
        (self.free)(self.state);
    }
}

// # Safety
// The engine must ensure that all raw pointers sent are Send-safe
unsafe impl Send for PlanResultCleanup {}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::KernelError;

    #[test]
    fn engine_error_to_kernel_preserves_etype() {
        let err = EngineError {
            etype: KernelError::FileNotFoundError,
        };
        let kernel_err = engine_error_to_kernel(&err as *const _ as *mut _);
        let Error::Generic(msg) = kernel_err else {
            panic!("expected Error::Generic");
        };
        assert!(
            msg.contains("FileNotFoundError"),
            "etype should appear in the message, got {msg:?}",
        );
    }

    #[test]
    fn plan_result_cleanup_invokes_free_with_state_on_drop() {
        extern "C" fn count_free_calls(state: NullableCvoid) {
            // SAFETY: this test passes a pointer to a stack-allocated `AtomicUsize` that
            // outlives the `PlanResultCleanup` drop.
            let counter = unsafe { &*(state.unwrap().as_ptr() as *const AtomicUsize) };
            counter.fetch_add(1, Ordering::SeqCst);
        }

        let counter = AtomicUsize::new(0);
        let cleanup = PlanResultCleanup::new(
            NonNull::new(&counter as *const AtomicUsize as *mut c_void),
            count_free_calls,
        );

        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "free must not run before drop"
        );
        drop(cleanup);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "free must run exactly once on drop"
        );
    }
}
