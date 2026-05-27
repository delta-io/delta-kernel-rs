//! Engine callback framework for opaque-predicate evaluation.
//!
//! Engines register an [`OpaqueEvalCallbacks`] struct, then attach it to an opaque predicate via
//! [`crate::expressions::kernel_visitor::visit_predicate_opaque_with_eval`]. Kernel does all
//! recursive evaluation natively through its standard `evaluate_expression` / `evaluate_predicate`
//! paths; the engine is called only for the opaque-predicate node itself, and receives args as
//! pre-computed Arrow arrays.
//!
//! Mirrors how kernel-native opaque ops work (see `OpaqueLessThanOp` in
//! `kernel/src/engine/arrow_expression/tests.rs`): the op gets the raw args, calls
//! `evaluate_expression` on each to produce ArrayRefs, then combines them with op-specific logic.
//!
//! # Why only predicates, not opaque expressions
//!
//! Any engine-defined expression-level function (e.g. `LOWER`, `UPPER`) can be folded into a
//! composite opaque PREDICATE rather than expressed as a separate `Expression::Opaque` child. For
//! `STARTS_WITH(LOWER(col), "foo")`, the engine names a composite op like `STARTS_WITH_LOWER` and
//! builds:
//!
//! ```text
//! Predicate::Opaque("STARTS_WITH_LOWER", [Column("col"), Literal("foo")])
//! ```
//!
//! Kernel pre-evaluates `col` -> column data and the literal -> broadcast column; the engine's
//! callback receives both pre-computed columns and internally applies LOWER then STARTS_WITH. No
//! `Expression::Opaque` ever appears in the tree. The only case the FFI loses is engine-defined
//! expressions appearing INSIDE kernel-native predicates (e.g. `Eq(LOWER(col), "FOO")`), which
//! engines can always re-shape as a single opaque predicate.

use std::ffi::c_void;
#[cfg(test)]
use std::sync::Arc;

use delta_kernel_ffi_macros::handle_descriptor;

use crate::engine_data::ArrowFFIData;
use crate::handle::Handle;
use crate::KernelStringSlice;

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
/// Args are pre-evaluated by kernel: `args_in` is a RecordBatch (Struct array with one field per
/// argument) exported via Arrow C Data Interface. The engine takes ownership of the FFI handles by
/// moving them out of `*args_in` (typically via `std::mem::replace` or its C equivalent), then
/// imports via `from_ffi`. Kernel does not free the handles after the call returns; it sees the
/// emptied struct's Drop as a no-op.
///
/// The engine writes its result (a top-level `BooleanArray`) into `result_out` by populating the
/// `array` and `schema` fields. Kernel takes ownership of those handles via `from_ffi` after the
/// call. Returning `true` without populating `result_out` is a contract violation; kernel detects
/// the empty result and surfaces an error.
///
/// Returns `true` on success. On `false`, kernel treats the call as a non-fatal evaluation error
/// and surfaces an `Err` upstream.
///
/// # Arg shapes per [`EvalMode`]
///
/// In `RowMode`, each arg slot carries one Arrow value per data row matching the original
/// expression: `Column` -> column array, `Literal` -> broadcast array, `Predicate` ->
/// `BooleanArray`. Engine returns one bool per row.
///
/// In `StatsMode`, each arg slot carries one value per file, shaped by the original expression:
///
/// - `Column` (primitive): `StructArray[min, max, nullcount, rowcount]`. Crack by index:
///   - 0 = min, 1 = max (column's eligible type)
///   - 2 = nullcount (`Int64`; all-null if the column has no nullcount stats)
///   - 3 = rowcount (`Int64`; all-null only in checkpoint-only batches that lack it)
///
///   Inner field names are not part of the contract.
/// - `Column` (struct): same 4-field shape; min/max mirror the column's nested schema. Delta tracks
///   min/max per leaf only.
/// - `Column` (map/array): not stats-eligible; the rewrite abstains and the engine never sees a
///   wrapper for this slot (the entire predicate is dropped from the stats predicate).
/// - `Column` (partition): same 4-field shape; min and max are the partition value (exact),
///   nullcount is NULL (Delta doesn't carry nullcount for partition columns), rowcount is NULL
///   because partition-only stats batches omit `stats_parsed`.
/// - `Literal`: broadcast array, same as row mode.
/// - `Predicate`: `BooleanArray` of per-file verdicts (nulls = keep).
///
/// Engine returns one bool per file: `false` = prune, `true` or `null` = keep.
///
/// # Engine-side ownership pattern
/// ```ignore
/// // Take ownership of the args FFI handles (leaves kernel's slot empty).
/// let array = std::mem::replace(&mut (*args_in).array, FFI_ArrowArray::empty());
/// let schema = std::mem::replace(&mut (*args_in).schema, FFI_ArrowSchema::empty());
/// let data = from_ffi(array, &schema)?;
/// ```
pub type EngineEvalPredFn = unsafe extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    args_in: *mut ArrowFFIData,
    mode: EvalMode,
    inverted: bool,
    result_out: *mut ArrowFFIData,
) -> bool;

/// Destructor for the engine's state pointer.
pub type EngineFreeStateFn = unsafe extern "C" fn(engine_state: *mut c_void);

/// Bundle of engine callbacks for opaque-predicate evaluation.
#[repr(C)]
pub struct OpaqueEvalCallbacks {
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
unsafe impl Send for OpaqueEvalCallbacks {}
unsafe impl Sync for OpaqueEvalCallbacks {}

impl std::fmt::Debug for OpaqueEvalCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpaqueEvalCallbacks")
            .field("engine_state", &self.engine_state)
            .finish_non_exhaustive()
    }
}

impl Drop for OpaqueEvalCallbacks {
    fn drop(&mut self) {
        // SAFETY: engine_state was provided by the engine alongside the free_state callback; we
        // promise to call it exactly once when the last Arc reference drops.
        unsafe { (self.free_state)(self.engine_state) };
    }
}

#[handle_descriptor(target=OpaqueEvalCallbacks, mutable=false, sized=true)]
pub struct SharedOpaqueEvalContext;

/// Create an opaque-evaluation context. Returns a [`SharedOpaqueEvalContext`] handle the engine
/// attaches to opaque-predicate builders via `visit_predicate_opaque_with_eval`.
///
/// # Safety
/// All function pointers in `callbacks` must remain valid for the lifetime of the returned context
/// (i.e., until `free_opaque_eval_context` is called AND any opaque ops built with this context
/// have been dropped). Kernel keeps a reference internally per opaque op until that op's predicate
/// is dropped.
#[no_mangle]
pub unsafe extern "C" fn create_opaque_eval_context(
    callbacks: OpaqueEvalCallbacks,
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
        _result_out: *mut ArrowFFIData,
    ) -> bool {
        true
    }

    pub(crate) unsafe extern "C" fn counting_free_state(_state: *mut c_void) {
        TEST_FREES.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn create_then_free_invokes_free_state_once() {
        TEST_FREES.store(0, Ordering::SeqCst);
        let cb = OpaqueEvalCallbacks {
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
        let cb = OpaqueEvalCallbacks {
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
