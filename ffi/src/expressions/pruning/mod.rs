//! Engine callback framework for opaque-predicate data skipping.
//!
//! Engines fill [`OpaquePruningCallbacks`] with an
//! `eval_against_stats` callback, register them through
//! [`create_opaque_pruning_context`], and attach the context to an opaque
//! predicate via [`visit_predicate_opaque_with_pruning`]. Kernel invokes
//! the callback once per stats metadata batch during file pruning; the
//! engine writes one verdict per row.
//!
//! ## Contract
//!
//! - **Column names are physical.** Engines that build predicates from a logical (column-mapped)
//!   schema must resolve to physical names before constructing the children, otherwise lookups
//!   silently miss.
//! - **Single-segment columns only.** [`ChildAccessor`] reports nested column refs (`a.b.c`) as
//!   [`ExpressionKind::Unsupported`].
//!
//! [`visit_predicate_opaque_with_pruning`]: crate::expressions::kernel_visitor::visit_predicate_opaque_with_pruning

use std::ffi::c_void;
use std::sync::Arc;

use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice};

mod child;
mod expression;
mod stats;

pub use child::*;
pub use expression::*;
pub use stats::*;

// === Verdict =================================================================

/// Pruning verdict written by the engine.
///
/// `Unknown` is the zero value, so a default-initialized verdicts buffer is
/// already "I don't know" -- engines that cannot evaluate the op leave the
/// slot alone and kernel keeps the file conservatively.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpaquePruneVerdict {
    /// Engine could not determine; keep conservatively.
    Unknown = 0,
    /// Predicate evaluates true; keep the file.
    Keep = 1,
    /// Predicate evaluates false; skip.
    Skip = 2,
}

// === Callback ================================================================

/// Per-batch file-pruning callback. Engine writes one verdict per row into
/// `verdicts[0..n_rows]`. Slots are pre-initialized to
/// [`OpaquePruneVerdict::Unknown`].
pub type EvalAgainstStatsFn = extern "C" fn(
    engine_state: *mut c_void,
    op_name: KernelStringSlice,
    children: *const ChildAccessor<'_>,
    stats: *const BatchStatsAccessor<'_>,
    n_rows: usize,
    inverted: bool,
    verdicts: *mut OpaquePruneVerdict,
);

/// Bundle of engine callbacks for opaque-predicate pruning.
///
/// If the same `engine_state` is shared across multiple
/// [`create_opaque_pruning_context`] calls, the engine is responsible for
/// ref-counting it so `free_state` only releases the underlying resource
/// when the last context drops.
#[repr(C)]
#[derive(Debug)]
pub struct OpaquePruningCallbacks {
    /// Opaque engine state; passed back as the first argument to the
    /// callback.
    pub engine_state: *mut c_void,
    /// File-pruning callback.
    pub eval_against_stats: EvalAgainstStatsFn,
    /// Destructor for `engine_state`. Called once when the last reference
    /// to the pruning context is dropped; may run on any kernel thread.
    pub free_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: `engine_state` and the function pointers may be touched from any
// kernel thread. The struct lives behind `Arc<...>` via
// `SharedOpaquePruningContext`, and `SharedHandle` requires `T: Sync`.
unsafe impl Send for OpaquePruningCallbacks {}
unsafe impl Sync for OpaquePruningCallbacks {}

impl Drop for OpaquePruningCallbacks {
    fn drop(&mut self) {
        (self.free_state)(self.engine_state);
    }
}

// === Shared context handle ===================================================

#[handle_descriptor(target=OpaquePruningCallbacks, mutable=false, sized=true)]
pub struct SharedOpaquePruningContext;

/// Create a shared pruning context from a callback bundle.
///
/// # Safety
/// All function pointers in `callbacks` must be valid. `engine_state` must
/// be valid until `free_state` is invoked.
#[no_mangle]
pub unsafe extern "C" fn create_opaque_pruning_context(
    callbacks: OpaquePruningCallbacks,
) -> Handle<SharedOpaquePruningContext> {
    Arc::new(callbacks).into()
}

/// Drop a pruning context handle. The engine's `free_state` fires when the
/// last reference is released.
///
/// # Safety
/// `ctx` must be a valid handle produced by `create_opaque_pruning_context`.
#[no_mangle]
pub unsafe extern "C" fn free_opaque_pruning_context(ctx: Handle<SharedOpaquePruningContext>) {
    ctx.drop_handle();
}

// === Used by NamedOpaquePredicateOp ==========================================

/// Invoke the engine's batched file-pruning callback once for an entire
/// metadata batch.
#[allow(dead_code)] // used under default-engine-base
pub(crate) fn invoke_eval_against_stats(
    cb: &OpaquePruningCallbacks,
    op_name: &str,
    children: &ChildAccessor<'_>,
    stats: &BatchStatsAccessor<'_>,
    inverted: bool,
    verdicts: &mut [OpaquePruneVerdict],
) {
    (cb.eval_against_stats)(
        cb.engine_state,
        kernel_string_slice!(op_name),
        children,
        stats,
        verdicts.len(),
        inverted,
        verdicts.as_mut_ptr(),
    );
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use delta_kernel::expressions::{Expression, Scalar};
    use delta_kernel::schema::DataType;

    use super::*;
    use crate::TryFromStringSlice;

    thread_local! {
        pub(crate) static CALLBACK_LOG: Cell<Option<(String, bool)>> = const { Cell::new(None) };
        pub(crate) static CALLBACK_VERDICT: Cell<OpaquePruneVerdict> = const { Cell::new(OpaquePruneVerdict::Unknown) };
    }

    pub(crate) extern "C" fn test_eval_against_stats(
        _state: *mut c_void,
        op_name: KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const BatchStatsAccessor<'_>,
        n_rows: usize,
        inverted: bool,
        verdicts: *mut OpaquePruneVerdict,
    ) {
        let name = unsafe { String::try_from_slice(&op_name) }.unwrap();
        CALLBACK_LOG.with(|c| c.set(Some((name, inverted))));
        let verdict = CALLBACK_VERDICT.with(Cell::get);
        let slots = unsafe { std::slice::from_raw_parts_mut(verdicts, n_rows) };
        for slot in slots.iter_mut() {
            *slot = verdict;
        }
    }

    pub(crate) extern "C" fn test_free_state(_state: *mut c_void) {}

    pub(crate) fn build_test_callbacks() -> OpaquePruningCallbacks {
        OpaquePruningCallbacks {
            engine_state: ptr::null_mut(),
            eval_against_stats: test_eval_against_stats,
            free_state: test_free_state,
        }
    }

    pub(crate) struct FixedBatchStats {
        pub(crate) min: Option<Scalar>,
        pub(crate) max: Option<Scalar>,
        pub(crate) nulls: Option<i64>,
        pub(crate) rows: Option<i64>,
    }

    impl BatchStatsProvider for FixedBatchStats {
        fn min(&self, _row: usize, _col: &str, _dtype: &DataType) -> Option<Scalar> {
            self.min.clone()
        }
        fn max(&self, _row: usize, _col: &str, _dtype: &DataType) -> Option<Scalar> {
            self.max.clone()
        }
        fn null_count(&self, _row: usize, _col: &str) -> Option<i64> {
            self.nulls
        }
        fn row_count(&self, _row: usize) -> Option<i64> {
            self.rows
        }
    }

    fn invoke_with_verdict(
        op_name: &str,
        inverted: bool,
        n_rows: usize,
    ) -> Vec<OpaquePruneVerdict> {
        let cb = build_test_callbacks();
        let stats_provider = FixedBatchStats {
            min: None,
            max: None,
            nulls: None,
            rows: None,
        };
        let stats = BatchStatsAccessor::new(&stats_provider);
        let exprs: Vec<Expression> = vec![];
        let mut verdicts = vec![OpaquePruneVerdict::Unknown; n_rows];
        invoke_eval_against_stats(
            &cb,
            op_name,
            &ChildAccessor::new(&exprs),
            &stats,
            inverted,
            &mut verdicts,
        );
        verdicts
    }

    #[test]
    fn invoke_eval_against_stats_round_trips_name_and_inverted() {
        CALLBACK_LOG.with(|c| c.set(None));
        CALLBACK_VERDICT.with(|c| c.set(OpaquePruneVerdict::Skip));

        let verdicts = invoke_with_verdict("STARTS_WITH", true, 1);
        let logged = CALLBACK_LOG.with(Cell::take);
        assert_eq!(logged, Some(("STARTS_WITH".to_string(), true)));
        assert_eq!(verdicts[0], OpaquePruneVerdict::Skip);
    }

    #[test]
    fn invoke_eval_against_stats_keep_verdict() {
        CALLBACK_VERDICT.with(|c| c.set(OpaquePruneVerdict::Keep));
        let verdicts = invoke_with_verdict("FOO", false, 1);
        assert_eq!(verdicts[0], OpaquePruneVerdict::Keep);
    }

    #[test]
    fn invoke_eval_against_stats_unknown_verdict_passes_through() {
        CALLBACK_VERDICT.with(|c| c.set(OpaquePruneVerdict::Unknown));
        let verdicts = invoke_with_verdict("FOO", false, 1);
        assert_eq!(verdicts[0], OpaquePruneVerdict::Unknown);
    }

    // === Lifecycle tests for OpaquePruningCallbacks ============================

    static FREE_COUNT: AtomicUsize = AtomicUsize::new(0);

    extern "C" fn counting_free_state(_state: *mut c_void) {
        FREE_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    extern "C" fn noop_eval(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const BatchStatsAccessor<'_>,
        _n_rows: usize,
        _inverted: bool,
        _verdicts: *mut OpaquePruneVerdict,
    ) {
    }

    fn build_counting_callbacks() -> OpaquePruningCallbacks {
        OpaquePruningCallbacks {
            engine_state: ptr::null_mut(),
            eval_against_stats: noop_eval,
            free_state: counting_free_state,
        }
    }

    #[test]
    fn drop_callbacks_fires_free_state() {
        FREE_COUNT.store(0, Ordering::SeqCst);
        {
            let _cb = build_counting_callbacks();
            assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 0);
        }
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn create_then_free_pruning_context_fires_free_state_once() {
        FREE_COUNT.store(0, Ordering::SeqCst);
        let cb = build_counting_callbacks();
        // SAFETY: callbacks are valid and engine_state is null.
        let ctx = unsafe { create_opaque_pruning_context(cb) };
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 0);
        // SAFETY: ctx was just produced by create_opaque_pruning_context.
        unsafe { free_opaque_pruning_context(ctx) };
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn arc_shared_pruning_context_frees_only_on_last_drop() {
        FREE_COUNT.store(0, Ordering::SeqCst);
        let cb = Arc::new(build_counting_callbacks());
        let cloned = Arc::clone(&cb);
        drop(cb);
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 0);
        drop(cloned);
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 1);
    }
}
