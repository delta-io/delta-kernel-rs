//! Engine callback framework for opaque-predicate evaluation.
//!
//! Engines register an [`COpaqueEvalCallbacks`] struct, then attach it to an opaque predicate via
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

/// Tells the engine callback how to interpret each arg slot (see [`EngineEvalPredFn`]).
///
/// ABI: `#[repr(u8)]`. New variants must append, never reorder.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalMode {
    /// Per-row evaluation against a data batch. Each arg slot carries one value per row.
    RowMode = 0,
    /// Per-file evaluation against a stats batch. `Column` args arrive as
    /// `Struct[min, max, nullcount, rowcount]`; literals pass through.
    StatsMode = 1,
}

/// Engine callback for evaluating an opaque predicate.
///
/// # Arguments (`args_in`)
///
/// Args are pre-evaluated by kernel and handed over as a single `RecordBatch` (a struct array with
/// one field per argument) exported across the Arrow C Data Interface. Ownership of the inner
/// `FFI_ArrowArray` / `FFI_ArrowSchema` transfers to the engine, which imports them and invokes
/// their release callbacks when done.
///
/// # Result
///
/// On success, return `OptionalValue::Some(ptr)` where `ptr` owns an [`ArrowFFIData`] holding the
/// result `BooleanArray`. `ptr` MUST be obtained from
/// [`arrow_ffi_data_new`](crate::engine_data::arrow_ffi_data_new).
///
/// Returns `OptionalValue::None` to signal a (non-fatal) evaluation failure; kernel surfaces an
/// `Err` upstream.
///
/// # Inversion (`inverted`)
///
/// When `inverted` is true, evaluate the *negated* op (`NOT op`). In StatsMode this is not
/// `!verdict`; if you can't reason soundly about the negated op, keep every file.
///
/// # Panics
///
/// The callback must not panic or otherwise unwind across the FFI boundary. Engines must trap their
/// own errors and report them via `OptionalValue::None`.
///
/// # Arg shapes per [`EvalMode`]
///
/// Each arg slot maps by position to one argument of the predicate; what a slot *holds* depends
/// on the mode. Running example: `STARTS_WITH(col, "foo")` -- arg 0 is the column `col`, arg 1 is
/// the literal `"foo"`.
///
/// ## RowMode (one entry per data row)
///
/// Each slot holds one Arrow value per row: a `Column` -> its values, a `Literal` -> the constant
/// repeated for every row, a `Predicate` -> a `BooleanArray` of per-row results. For the example,
/// slot 0 is `col`'s values and slot 1 is `"foo"` repeated; the engine returns one bool per row.
///
/// ## StatsMode (one entry per file, used for data skipping)
///
/// Each slot holds per-file statistics so the engine can decide whether a file *might* contain a
/// match. A `Column` arrives as a 4-field struct, indexed by position (inner field names are not
/// part of the contract):
///
/// - 0 = `min` (column type; null if not collected)
/// - 1 = `max` (column type; null if not collected)
/// - 2 = `nullcount` (`Int64`; null if not collected)
/// - 3 = `rowcount` (`Int64`; null if the batch has no rowcount stat)
///
/// `Literal` and `Predicate` slots are the same as RowMode. The engine returns one bool per file:
/// `false` = skip the file, `true`/`null` = keep it. For the example, slot 0 is `col`'s
/// `{min, max, nullcount, rowcount}` and slot 1 is `"foo"`; keep the file if any value in
/// `[min, max]` could start with `"foo"`.
///
/// The stats are *conservative*, never exact: `min`/`max` only bound the values, and
/// `nullcount`/`rowcount` may overcount. So skip a file only when the predicate *cannot* hold for
/// any value in `[min, max]`. The per-file `tightBounds` flag is not forwarded -- always treat
/// stats as wide, matching kernel-native skipping.
///
/// ## Notes
///
/// - Some types have no min/max (map, array, boolean, binary). Without a sibling literal the
///   predicate abstains before the engine sees it; with one, min/max are null and the op keeps
///   every file rather than erroring.
/// - Partition columns: `min` and `max` are both the exact partition value; `nullcount`/`rowcount`
///   are null (Delta doesn't track them per partition).
/// - Struct columns use the same 4-field shape, with min/max following the nested schema.
/// - Kernel reads a column's stats at the type of a sibling literal (like `col < val` reads stats
///   at `val`'s type), so keep column and literal types compatible.
pub type EngineEvalPredFn = unsafe extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    args_in: *mut ArrowFFIData,
    mode: EvalMode,
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
    /// Predicate-evaluation callback.
    pub eval_pred: EngineEvalPredFn,
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
        _mode: EvalMode,
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
            eval_pred: noop_eval_pred,
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
            eval_pred: noop_eval_pred,
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
