//! Engine callback framework for opaque-predicate evaluation.
//!
//! Engines pass a [`COpaqueEvalCallbacks`] struct by value to
//! [`crate::expressions::kernel_visitor::visit_predicate_opaque_with_eval`]. Kernel does all
//! recursive evaluation natively through its standard `evaluate_expression` / `evaluate_predicate`
//! paths; the engine is called only for the opaque-predicate node itself, and receives args as
//! pre-computed Arrow arrays.
//!
//! Available only under `default-engine-base`: args and results cross as Arrow C Data Interface
//! batches, and kernel pre-evaluates the args with its Arrow expression evaluator.
//! TODO: support a non-arrow opaque-eval path so engines without the Arrow evaluator can use this.
//!
//! Mirrors how kernel-native opaque ops work (see `OpaqueLessThanOp` in
//! `kernel/src/engine/arrow_expression/tests.rs`): the op gets the raw args, calls
//! `evaluate_expression` on each to produce ArrayRefs, then combines them with op-specific logic.

use std::ffi::c_void;

use crate::engine_data::ArrowFFIData;
use crate::error::EngineExecResult;
use crate::KernelStringSlice;

/// Engine callback for **row-time** evaluation of an opaque predicate.
///
/// Kernel pre-evaluates the predicate's args into a `RecordBatch` (one field per arg) and exports
/// it across the Arrow C Data Interface; ownership of the inner `FFI_ArrowArray`/`FFI_ArrowSchema`
/// transfers to the engine, which imports them and invokes their release callbacks. The batch's
/// columns line up with the predicate's args by position:
/// - a `Column` arg arrives as its evaluated values
/// - a `Literal` arg as the constant repeated for every row
/// - a `Predicate` arg as a `BooleanArray` of its verdicts
///
/// The engine returns one bool per row.
///
/// The result uses the out-pointer convention: kernel pre-initializes `*out` to
/// `EngineExecResult::Uninit`. On success the engine writes `EngineExecResult::Success` holding the
/// result `BooleanArray` as Arrow C Data Interface structs, transferring their ownership to kernel.
/// On failure it writes `EngineExecResult::Failure` carrying a `KernelError` code and a message
/// handle (built via `allocate_kernel_string`); leaving `*out` as `Uninit` is also treated as an
/// error. On failure the engine may leave `args_in` unconsumed -- kernel releases whatever it did
/// not import. When `inverted`, evaluate `NOT op`.
///
/// # Safety
/// `out` is valid only for the duration of the call; the engine must not retain it. The callback
/// must not panic or unwind across the FFI boundary.
pub type EngineEvalRowsFn = unsafe extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    args_in: *mut ArrowFFIData,
    inverted: bool,
    out: *mut EngineExecResult<ArrowFFIData>,
);

/// Engine callback for **stats-based** evaluation of an opaque predicate, for file data skipping.
///
/// Like [`EngineEvalRowsFn`], but each `Column` arg arrives as a per-file 4-field struct indexed by
/// position: `0`=min, `1`=max (column type; null if not collected), `2`=nullcount, `3`=rowcount
/// (`Int64`; null if not collected). `Literal`/`Predicate` args are unchanged. The engine returns
/// one bool per file: `false` = skip, `true`/`null` = keep.
///
/// Stats are *conservative*: `min`/`max` only bound the values and `nullcount`/`rowcount` may
/// overcount, so skip a file only when the predicate *cannot* hold for any value in `[min, max]`.
/// Keep (`true`/`null`) whenever the stats can't prove the predicate impossible -- in particular
/// on null/absent bounds, where skipping silently drops live files from the scan. Log-replay
/// soundness does not rest on the engine, though: kernel wraps the rewritten predicate with an
/// `OR(NOT is_add, ...)` guard, so Remove rows (which carry null stats) are kept regardless of
/// the verdict and a misbehaving callback cannot resurrect deleted files.
/// When `inverted`, evaluate `NOT op` -- not `!verdict`; if you can't reason soundly about the
/// negated op, keep every file. Result/out-pointer/panic contract matches [`EngineEvalRowsFn`].
pub type EngineEvalStatsFn = unsafe extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    args_in: *mut ArrowFFIData,
    inverted: bool,
    out: *mut EngineExecResult<ArrowFFIData>,
);

/// Destructor for the engine's state pointer.
///
/// Must not panic or unwind across the FFI boundary.
pub type EngineFreeStateFn = unsafe extern "C" fn(engine_state: *mut c_void);

/// Bundle of engine callbacks for opaque-predicate evaluation, passed by value to
/// [`visit_predicate_opaque_with_eval`]. Ownership of `engine_state` transfers to kernel with the
/// call: `free_state` is invoked exactly once when the predicate built from it (including any
/// data-skipping clones kernel derives) is dropped. Engines attaching the same logical state to
/// multiple opaque ops must pass independently freeable state per call.
///
/// # Thread safety
///
/// Kernel may invoke the eval callbacks -- and ultimately `free_state` -- from any thread,
/// potentially concurrently: the predicate is shared across whatever threads drive the scan, and
/// the last reference may drop on any of them. The engine must hand over only an `engine_state`
/// and callbacks that tolerate this; any synchronization is the engine's responsibility.
///
/// [`visit_predicate_opaque_with_eval`]: crate::expressions::kernel_visitor::visit_predicate_opaque_with_eval
#[repr(C)]
pub struct COpaqueEvalCallbacks {
    /// Opaque engine state; passed back as the first argument to each
    /// callback.
    pub engine_state: *mut c_void,
    /// Row-time evaluation: one verdict per data row.
    pub eval_pred_rows: EngineEvalRowsFn,
    /// Stats-based evaluation for file data skipping: one verdict per file.
    pub eval_pred_stats: EngineEvalStatsFn,
    /// Destructor for `engine_state`. Called exactly once; may run on any
    /// kernel thread.
    pub free_state: EngineFreeStateFn,
}

impl std::fmt::Debug for COpaqueEvalCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("COpaqueEvalCallbacks")
            .field("engine_state", &self.engine_state)
            .finish_non_exhaustive()
    }
}

/// Rust-side adapter owning a [`COpaqueEvalCallbacks`]: invokes the engine's `free_state` exactly
/// once on drop. Shared across an op and its data-skipping clones via `Arc`.
#[derive(Debug)]
pub(crate) struct FfiOpaqueEvalCallbacks {
    pub(crate) inner: COpaqueEvalCallbacks,
}

impl FfiOpaqueEvalCallbacks {
    pub(crate) fn new(callbacks: COpaqueEvalCallbacks) -> Self {
        Self { inner: callbacks }
    }
}

// SAFETY: values of this type exist only via the engine handover in
// `visit_predicate_opaque_with_eval`, whose thread-safety contract (see `COpaqueEvalCallbacks`)
// obligates the engine to pass only state and callbacks that are safe to use from any thread,
// concurrently. Kernel adds no thread-affine state of its own, so sharing the adapter across
// threads (behind the op's `Arc`) is sound.
unsafe impl Send for FfiOpaqueEvalCallbacks {}
unsafe impl Sync for FfiOpaqueEvalCallbacks {}

impl Drop for FfiOpaqueEvalCallbacks {
    fn drop(&mut self) {
        // SAFETY: engine_state was handed over by value alongside the free_state callback; we
        // promise to call it exactly once when the last Arc reference drops. free_state must not
        // panic: drop can run while kernel is already unwinding, and unwinding into kernel aborts.
        unsafe { (self.inner.free_state)(self.inner.engine_state) };
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    pub(crate) static TEST_FREES: AtomicUsize = AtomicUsize::new(0);

    pub(crate) unsafe extern "C" fn noop_eval_pred(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        _args_in: *mut ArrowFFIData,
        _inverted: bool,
        _out: *mut EngineExecResult<ArrowFFIData>,
    ) {
    }

    pub(crate) unsafe extern "C" fn counting_free_state(_state: *mut c_void) {
        TEST_FREES.fetch_add(1, Ordering::SeqCst);
    }

    fn counting_callbacks() -> COpaqueEvalCallbacks {
        COpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred_rows: noop_eval_pred,
            eval_pred_stats: noop_eval_pred,
            free_state: counting_free_state,
        }
    }

    #[test]
    fn drop_invokes_free_state_once() {
        TEST_FREES.store(0, Ordering::SeqCst);
        let wrapper = FfiOpaqueEvalCallbacks::new(counting_callbacks());
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 0);
        drop(wrapper);
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_via_arc_clone_fires_free_only_once() {
        TEST_FREES.store(0, Ordering::SeqCst);
        let arc1 = Arc::new(FfiOpaqueEvalCallbacks::new(counting_callbacks()));
        let arc2 = arc1.clone();
        drop(arc1);
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 0);
        drop(arc2);
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 1);
    }
}
