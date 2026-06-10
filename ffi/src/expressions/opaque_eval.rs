//! Engine callback framework for opaque-predicate evaluation.
//!
//! Engines register a [`COpaqueEvalCallbacks`] struct, then attach it to an opaque predicate via
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
#[cfg(test)]
use std::sync::Arc;

use delta_kernel_ffi_macros::handle_descriptor;

use crate::engine_data::ArrowFFIData;
use crate::handle::Handle;
use crate::{KernelStringSlice, OptionalValue};

/// Engine callback for **row-time** evaluation of an opaque predicate.
///
/// Kernel pre-evaluates the predicate's args into a `RecordBatch` (one field per arg) and exports
/// it across the Arrow C Data Interface; ownership of the inner `FFI_ArrowArray`/`FFI_ArrowSchema`
/// transfers to the engine, which imports them and invokes their release callbacks. By position: a
/// `Column` arg holds its values, a `Literal` the constant repeated per row, a `Predicate` a
/// `BooleanArray`. The engine returns one bool per row.
///
/// On success return `OptionalValue::Some(ptr)` holding the result `BooleanArray`;
/// `OptionalValue::None` signals a (non-fatal) failure. When `inverted`, evaluate `NOT op`.
///
/// # Safety
/// `ptr` MUST be allocated via [`arrow_ffi_data_new`](crate::engine_data::arrow_ffi_data_new):
/// kernel reclaims it with `Box::from_raw`, so a pointer from any other allocator is undefined
/// behavior. The callback must not panic or unwind across the FFI boundary.
pub type EngineEvalRowsFn = unsafe extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    args_in: *mut ArrowFFIData,
    inverted: bool,
) -> OptionalValue<*mut ArrowFFIData>;

/// Engine callback for **stats-based** evaluation of an opaque predicate, for file data skipping.
///
/// Like [`EngineEvalRowsFn`], but each `Column` arg arrives as a per-file 4-field struct indexed by
/// position: `0`=min, `1`=max (column type; null if not collected), `2`=nullcount, `3`=rowcount
/// (`Int64`; null if not collected). `Literal`/`Predicate` args are unchanged. The engine returns
/// one bool per file: `false` = skip, `true`/`null` = keep.
///
/// Stats are *conservative*: `min`/`max` only bound the values and `nullcount`/`rowcount` may
/// overcount, so skip a file only when the predicate *cannot* hold for any value in `[min, max]`.
/// Returning `false` (skip) on null/absent bounds is unsound: during checkpoint replay the skipping
/// batch also carries Remove rows (which have null stats), and dropping one resurrects a deleted
/// file -- corrupting Add/Remove reconciliation, not just pruning accuracy. Keep (`true`/`null`)
/// whenever the stats can't prove the predicate impossible.
/// When `inverted`, evaluate `NOT op` -- not `!verdict`; if you can't reason soundly about the
/// negated op, keep every file. Result/allocation/panic contract matches [`EngineEvalRowsFn`].
pub type EngineEvalStatsFn = unsafe extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    args_in: *mut ArrowFFIData,
    inverted: bool,
) -> OptionalValue<*mut ArrowFFIData>;

/// Destructor for the engine's state pointer.
///
/// Must not panic or unwind across the FFI boundary.
pub type EngineFreeStateFn = unsafe extern "C" fn(engine_state: *mut c_void);

/// Bundle of engine callbacks for opaque-predicate evaluation.
#[repr(C)]
pub struct COpaqueEvalCallbacks {
    /// Opaque engine state; passed back as the first argument to each
    /// callback.
    pub engine_state: *mut c_void,
    /// Row-time evaluation: one verdict per data row.
    pub eval_pred_rows: EngineEvalRowsFn,
    /// Stats-based evaluation for file data skipping: one verdict per file.
    pub eval_pred_stats: EngineEvalStatsFn,
    /// Destructor for `engine_state`. Called once when the last reference
    /// to the eval context is dropped; may run on any kernel thread.
    pub free_state: EngineFreeStateFn,
}

// SAFETY: `engine_state` and the function pointers may be touched from
// any kernel thread. The struct lives behind `Arc<...>` via
// `SharedOpaqueEvalContext`, and `SharedHandle` requires `T: Sync`.
unsafe impl Send for COpaqueEvalCallbacks {}
unsafe impl Sync for COpaqueEvalCallbacks {}

impl std::fmt::Debug for COpaqueEvalCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("COpaqueEvalCallbacks")
            .field("engine_state", &self.engine_state)
            .finish_non_exhaustive()
    }
}

impl Drop for COpaqueEvalCallbacks {
    fn drop(&mut self) {
        // SAFETY: engine_state was provided by the engine alongside the free_state callback; we
        // promise to call it exactly once when the last Arc reference drops. free_state must not
        // panic: drop can run while kernel is already unwinding, and unwinding into kernel aborts.
        unsafe { (self.free_state)(self.engine_state) };
    }
}

#[handle_descriptor(target=COpaqueEvalCallbacks, mutable=false, sized=true)]
pub struct SharedOpaqueEvalContext;

/// Create an opaque-evaluation context. Returns a [`SharedOpaqueEvalContext`] handle the engine
/// attaches to opaque-predicate builders via `visit_predicate_opaque_with_eval`.
///
/// # Safety
/// All function pointers in `callbacks` must outlive the returned context and any opaque ops built
/// from it.
#[no_mangle]
pub unsafe extern "C" fn create_opaque_eval_context(
    callbacks: COpaqueEvalCallbacks,
) -> Handle<SharedOpaqueEvalContext> {
    std::sync::Arc::new(callbacks).into()
}

/// Free an opaque-evaluation context obtained from [`create_opaque_eval_context`].
///
/// # Safety
/// `ctx` must be a valid handle obtained from [`create_opaque_eval_context`] and not previously
/// freed.
#[no_mangle]
pub unsafe extern "C" fn free_opaque_eval_context(ctx: Handle<SharedOpaqueEvalContext>) {
    ctx.drop_handle();
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    pub(crate) static TEST_FREES: AtomicUsize = AtomicUsize::new(0);

    pub(crate) unsafe extern "C" fn noop_eval_pred(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        _args_in: *mut ArrowFFIData,
        _inverted: bool,
    ) -> OptionalValue<*mut ArrowFFIData> {
        OptionalValue::None
    }

    pub(crate) unsafe extern "C" fn counting_free_state(_state: *mut c_void) {
        TEST_FREES.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn create_then_free_invokes_free_state_once() {
        TEST_FREES.store(0, Ordering::SeqCst);
        let cb = COpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred_rows: noop_eval_pred,
            eval_pred_stats: noop_eval_pred,
            free_state: counting_free_state,
        };
        let ctx = unsafe { create_opaque_eval_context(cb) };
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 0);
        unsafe { free_opaque_eval_context(ctx) };
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_via_arc_clone_fires_free_only_once() {
        TEST_FREES.store(0, Ordering::SeqCst);
        let cb = COpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred_rows: noop_eval_pred,
            eval_pred_stats: noop_eval_pred,
            free_state: counting_free_state,
        };
        let arc1 = Arc::new(cb);
        let arc2 = arc1.clone();
        drop(arc1);
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 0);
        drop(arc2);
        assert_eq!(TEST_FREES.load(Ordering::SeqCst), 1);
    }
}
