//! FFI bindings for the kernel's [`commit_range`] module.
//!
//! Mirrors the Java `JvmCommitRange*` family on the Rust side via Panama FFM downcalls.
//! Phase 1 design choice: [`CommitsIterator`] eagerly materialises all commits at init
//! time. The kernel's [`CommitRange::commits`] returns a borrowed iterator (`'a` tied to
//! `&self` and `&engine`); rather than carry that borrow across FFI via a self-referential
//! struct, we drain it into a [`std::collections::VecDeque`] up front. Each [`CommitActions`]
//! already owns its own `read_json_files` iterator (per `commit_range::actions`), so the
//! queued values are fully detached from the parent [`CommitRange`].
//!
//! TODO: switch to a lazy variant once the kernel exposes a
//! `commits_owned(self: Arc<Self>, engine: Arc<dyn Engine>, ...) -> 'static + Send`-shaped
//! method.
//!
//! [`commit_range`]: delta_kernel::commit_range

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use delta_kernel::commit_range::{
    CommitActions, CommitBoundary, CommitRange, CommitRangeBuilder, DeltaAction,
};
use delta_kernel::{DeltaResult, EngineData};
use delta_kernel_ffi_macros::handle_descriptor;

#[cfg(feature = "default-engine-base")]
use crate::engine_data::ArrowFFIData;
use crate::handle::Handle;
use crate::{
    AllocateErrorFn, ExternEngine, ExternResult, IntoExternResult, KernelStringSlice,
    NullableCvoid, SharedExternEngine, TryFromStringSlice,
};

// =====================================================================================
// repr(C) types
// =====================================================================================

/// FFI-friendly mirror of [`CommitBoundary`]. Currently only [`Self::Version`] is
/// supported on the kernel side; the timestamp variant will be added when the kernel
/// gains support for [`CommitBoundary::Timestamp`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum KernelCommitBoundary {
    /// A specific commit version.
    Version(u64),
}

impl From<KernelCommitBoundary> for CommitBoundary {
    fn from(b: KernelCommitBoundary) -> Self {
        match b {
            KernelCommitBoundary::Version(v) => CommitBoundary::Version(v),
        }
    }
}

/// Bitmap encoding of a [`HashSet<DeltaAction>`].
///
/// Each bit corresponds to one [`DeltaAction`] variant (see `KERNEL_ACTION_*`
/// constants). Zero means "no action types"; a typical streaming caller passes
/// `KERNEL_ACTION_ADD | KERNEL_ACTION_REMOVE | KERNEL_ACTION_METADATA |
/// KERNEL_ACTION_COMMIT_INFO`.
pub type KernelDeltaActionSet = u8;

pub const KERNEL_ACTION_ADD: KernelDeltaActionSet = 1 << 0;
pub const KERNEL_ACTION_REMOVE: KernelDeltaActionSet = 1 << 1;
pub const KERNEL_ACTION_METADATA: KernelDeltaActionSet = 1 << 2;
pub const KERNEL_ACTION_PROTOCOL: KernelDeltaActionSet = 1 << 3;
pub const KERNEL_ACTION_COMMIT_INFO: KernelDeltaActionSet = 1 << 4;
pub const KERNEL_ACTION_CDC: KernelDeltaActionSet = 1 << 5;

fn decode_action_set(bits: KernelDeltaActionSet) -> HashSet<DeltaAction> {
    let mut set = HashSet::new();
    if bits & KERNEL_ACTION_ADD != 0 {
        set.insert(DeltaAction::Add);
    }
    if bits & KERNEL_ACTION_REMOVE != 0 {
        set.insert(DeltaAction::Remove);
    }
    if bits & KERNEL_ACTION_METADATA != 0 {
        set.insert(DeltaAction::Metadata);
    }
    if bits & KERNEL_ACTION_PROTOCOL != 0 {
        set.insert(DeltaAction::Protocol);
    }
    if bits & KERNEL_ACTION_COMMIT_INFO != 0 {
        set.insert(DeltaAction::CommitInfo);
    }
    if bits & KERNEL_ACTION_CDC != 0 {
        set.insert(DeltaAction::Cdc);
    }
    set
}

// =====================================================================================
// Internal owned-iterator wrappers
// =====================================================================================

/// Owns an eagerly-drained [`VecDeque`] of [`CommitActions`]. See module doc.
pub struct CommitsIterator {
    pending: VecDeque<DeltaResult<CommitActions>>,
}

/// Owned action-batch iterator. Wraps the `'static + Send` boxed iterator returned by
/// [`CommitActions::into_actions`].
pub struct ActionBatchIterator {
    inner: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
}

// =====================================================================================
// Handle declarations
// =====================================================================================

/// Shared (`Arc`-backed) handle to a [`CommitRange`]. Multiple commits-iterators can be
/// opened from the same range.
#[handle_descriptor(target=CommitRange, mutable=false, sized=true)]
pub struct SharedCommitRange;

/// Exclusive (`Box`-backed) handle to a [`CommitRangeBuilder`]. Consumed by
/// [`commit_range_builder_build`].
#[handle_descriptor(target=CommitRangeBuilder, mutable=true, sized=true)]
pub struct ExclusiveCommitRangeBuilder;

/// Exclusive handle to a [`CommitsIterator`]. Single-pass.
#[handle_descriptor(target=CommitsIterator, mutable=true, sized=true)]
pub struct ExclusiveCommitsIterator;

/// Exclusive handle to a [`CommitActions`]. Consumed by
/// [`commit_actions_into_actions_iter`].
#[handle_descriptor(target=CommitActions, mutable=true, sized=true)]
pub struct ExclusiveCommitActions;

/// Exclusive handle to an [`ActionBatchIterator`]. Single-pass.
#[handle_descriptor(target=ActionBatchIterator, mutable=true, sized=true)]
pub struct ExclusiveActionBatchIterator;

// =====================================================================================
// Arrow result struct (mirrors ScanMetadataArrowResult; gated on default-engine-base)
// =====================================================================================

/// Per-batch Arrow C Data export, returned by [`action_batch_iter_next_arrow`].
///
/// Released via [`free_action_batch_arrow_result`].
#[cfg(feature = "default-engine-base")]
#[repr(C)]
pub struct ActionBatchArrowResult {
    pub arrow_data: ArrowFFIData,
}

// =====================================================================================
// Builder symbols
// =====================================================================================

/// Begin a [`CommitRangeBuilder`] rooted at `table_path` with the given
/// `start_boundary`.
///
/// # Safety
/// `table_path` must be a valid UTF-8 string slice.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_init(
    table_path: KernelStringSlice,
    start_boundary: KernelCommitBoundary,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<ExclusiveCommitRangeBuilder>> {
    let path = unsafe { TryFromStringSlice::try_from_slice(&table_path) };
    commit_range_builder_init_impl(path, start_boundary).into_extern_result(&allocate_error)
}

fn commit_range_builder_init_impl(
    table_path: DeltaResult<&str>,
    start_boundary: KernelCommitBoundary,
) -> DeltaResult<Handle<ExclusiveCommitRangeBuilder>> {
    let table_path = table_path?;
    let builder = CommitRange::builder_for(table_path, start_boundary.into());
    Ok(Box::new(builder).into())
}

/// Append an end boundary to the builder. The kernel's
/// [`CommitRangeBuilder::with_end_boundary`] takes `self` by value, so this consumes
/// the input handle and returns a new one. **Caller must use the returned handle for
/// any further calls; the input handle is dead.**
///
/// # Safety
/// CONSUMES `builder`. The input handle is invalid after this call regardless of
/// outcome.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_with_end(
    builder: Handle<ExclusiveCommitRangeBuilder>,
    end_boundary: KernelCommitBoundary,
) -> Handle<ExclusiveCommitRangeBuilder> {
    let builder_box: Box<CommitRangeBuilder> = unsafe { builder.into_inner() };
    let updated = (*builder_box).with_end_boundary(end_boundary.into());
    Box::new(updated).into()
}

/// Consume the builder and build a [`CommitRange`].
///
/// # Safety
/// CONSUMES `builder`. Caller must NOT use the builder handle after this returns,
/// regardless of success or failure.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_build(
    builder: Handle<ExclusiveCommitRangeBuilder>,
    engine: Handle<SharedExternEngine>,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedCommitRange>> {
    let _ = allocate_error;
    let builder_box: Box<CommitRangeBuilder> = unsafe { builder.into_inner() };
    let extern_engine = unsafe { engine.clone_as_arc() };
    commit_range_builder_build_impl(&builder_box, extern_engine.as_ref())
        .into_extern_result(&extern_engine.as_ref())
}

fn commit_range_builder_build_impl(
    builder: &CommitRangeBuilder,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<Handle<SharedCommitRange>> {
    let kernel_engine: Arc<dyn delta_kernel::Engine> = extern_engine.engine();
    let range = builder.build(kernel_engine.as_ref())?;
    Ok(Arc::new(range).into())
}

/// Drop a [`CommitRangeBuilder`] handle without building.
///
/// # Safety
/// `builder` must be valid and not yet consumed by [`commit_range_builder_build`] or
/// [`commit_range_builder_with_end`].
#[no_mangle]
pub unsafe extern "C" fn free_commit_range_builder(builder: Handle<ExclusiveCommitRangeBuilder>) {
    builder.drop_handle();
}

/// Drop a [`CommitRange`].
///
/// # Safety
/// `range` must be a valid handle returned by [`commit_range_builder_build`].
#[no_mangle]
pub unsafe extern "C" fn free_commit_range(range: Handle<SharedCommitRange>) {
    range.drop_handle();
}

// =====================================================================================
// Range -> commits iterator
// =====================================================================================

/// Open a commits iterator. Eagerly materialises all commits at this call (see module
/// doc for rationale). Returns an owned iterator handle.
///
/// # Safety
/// `range` and `engine` must be valid handles. `action_set` must be a bitmap of
/// `KERNEL_ACTION_*` constants.
#[no_mangle]
pub unsafe extern "C" fn commit_range_commits_iter_init(
    range: Handle<SharedCommitRange>,
    engine: Handle<SharedExternEngine>,
    action_set: KernelDeltaActionSet,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<ExclusiveCommitsIterator>> {
    let _ = allocate_error;
    let range_ref = unsafe { range.as_ref() };
    let extern_engine = unsafe { engine.clone_as_arc() };
    commit_range_commits_iter_init_impl(range_ref, extern_engine.as_ref(), action_set)
        .into_extern_result(&extern_engine.as_ref())
}

fn commit_range_commits_iter_init_impl(
    range: &CommitRange,
    extern_engine: &dyn ExternEngine,
    action_set: KernelDeltaActionSet,
) -> DeltaResult<Handle<ExclusiveCommitsIterator>> {
    let kernel_engine: Arc<dyn delta_kernel::Engine> = extern_engine.engine();
    let actions = decode_action_set(action_set);
    // Eager drain: the borrowed iterator from `range.commits(...)` lives only until
    // the end of this expression. Each emitted CommitActions is fully owned (no
    // borrow into `range` or `kernel_engine`), so the queued values survive once
    // the closure here returns.
    let pending: VecDeque<DeltaResult<CommitActions>> =
        range.commits(kernel_engine.as_ref(), &actions).collect();
    Ok(Box::new(CommitsIterator { pending }).into())
}

/// Pull the next commit from `iter`. The visitor is invoked at most once per call,
/// receiving an owned [`Handle<ExclusiveCommitActions>`]. The visitor takes ownership;
/// it must eventually call [`commit_actions_into_actions_iter`] (consume) or
/// [`free_commit_actions`] (drop) on the handle.
///
/// Returns `Ok(true)` if a commit was emitted, `Ok(false)` if exhausted.
///
/// # Safety
/// `iter` must be valid and not freed. `engine_visitor` must be a non-null function
/// pointer with the declared signature.
#[no_mangle]
pub unsafe extern "C" fn commits_iter_next(
    mut iter: Handle<ExclusiveCommitsIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        commit: Handle<ExclusiveCommitActions>,
    ),
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    let iter_ref = unsafe { iter.as_mut() };
    commits_iter_next_impl(iter_ref, engine_context, engine_visitor)
        .into_extern_result(&allocate_error)
}

fn commits_iter_next_impl(
    iter: &mut CommitsIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        commit: Handle<ExclusiveCommitActions>,
    ),
) -> DeltaResult<bool> {
    match iter.pending.pop_front().transpose()? {
        Some(commit) => {
            (engine_visitor)(engine_context, Box::new(commit).into());
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Drop a commits iterator. Any unconsumed [`CommitActions`] queued inside are also
/// dropped.
///
/// # Safety
/// `iter` must be valid.
#[no_mangle]
pub unsafe extern "C" fn free_commits_iter(iter: Handle<ExclusiveCommitsIterator>) {
    iter.drop_handle();
}

// =====================================================================================
// CommitActions accessors + consume
// =====================================================================================

/// Read the version of a [`CommitActions`] handle.
///
/// # Safety
/// `commit` must be valid and not yet consumed by
/// [`commit_actions_into_actions_iter`] or freed.
#[no_mangle]
pub unsafe extern "C" fn commit_actions_version(commit: Handle<ExclusiveCommitActions>) -> u64 {
    let commit_ref = unsafe { commit.as_ref() };
    commit_ref.version()
}

/// Read the timestamp of a [`CommitActions`] handle (millis since epoch).
///
/// # Safety
/// `commit` must be valid and not yet consumed.
#[no_mangle]
pub unsafe extern "C" fn commit_actions_timestamp(commit: Handle<ExclusiveCommitActions>) -> i64 {
    let commit_ref = unsafe { commit.as_ref() };
    commit_ref.timestamp()
}

/// Consume `commit` and return an action-batch iterator over its inner action stream.
///
/// # Safety
/// CONSUMES `commit`. Caller must NOT use the handle after this returns.
#[no_mangle]
pub unsafe extern "C" fn commit_actions_into_actions_iter(
    commit: Handle<ExclusiveCommitActions>,
) -> Handle<ExclusiveActionBatchIterator> {
    let commit_box: Box<CommitActions> = unsafe { commit.into_inner() };
    let inner = (*commit_box).into_actions();
    Box::new(ActionBatchIterator { inner }).into()
}

/// Drop a [`CommitActions`] handle (only if not consumed by
/// [`commit_actions_into_actions_iter`]).
///
/// # Safety
/// `commit` must be valid and not yet consumed.
#[no_mangle]
pub unsafe extern "C" fn free_commit_actions(commit: Handle<ExclusiveCommitActions>) {
    commit.drop_handle();
}

// =====================================================================================
// Action batch iterator (Arrow C Data export path)
// =====================================================================================

/// Pull the next action batch from `iter`, exporting it via the Arrow C Data
/// Interface.
///
/// Returns `Ok(non-null)` with a fresh [`ActionBatchArrowResult`] on success (caller
/// must free via [`free_action_batch_arrow_result`]), or `Ok(null)` on exhaustion.
///
/// # Safety
/// `iter` and `engine` must be valid handles. The returned pointer is owned by the
/// caller; failing to free it leaks the underlying Arrow buffers.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn action_batch_iter_next_arrow(
    mut iter: Handle<ExclusiveActionBatchIterator>,
    engine: Handle<SharedExternEngine>,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*mut ActionBatchArrowResult> {
    let _ = allocate_error;
    let iter_ref = unsafe { iter.as_mut() };
    let extern_engine = unsafe { engine.as_ref() };
    action_batch_iter_next_arrow_impl(iter_ref).into_extern_result(&extern_engine)
}

#[cfg(feature = "default-engine-base")]
fn action_batch_iter_next_arrow_impl(
    iter: &mut ActionBatchIterator,
) -> DeltaResult<*mut ActionBatchArrowResult> {
    match iter.inner.next().transpose()? {
        Some(engine_data) => {
            let arrow_data = ArrowFFIData::try_from_engine_data(engine_data)?;
            Ok(Box::into_raw(Box::new(ActionBatchArrowResult { arrow_data })))
        }
        None => Ok(std::ptr::null_mut()),
    }
}

/// Free an [`ActionBatchArrowResult`].
///
/// # Safety
/// `result` must be a valid pointer returned by [`action_batch_iter_next_arrow`] or
/// null. Must be called at most once per non-null pointer.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn free_action_batch_arrow_result(result: *mut ActionBatchArrowResult) {
    if result.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(result) });
}

/// Drop an action-batch iterator. Any in-flight inner state is dropped.
///
/// # Safety
/// `iter` must be valid.
#[no_mangle]
pub unsafe extern "C" fn free_action_batch_iter(iter: Handle<ExclusiveActionBatchIterator>) {
    iter.drop_handle();
}

#[cfg(all(test, feature = "default-engine-base"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::Arc;

    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::object_store::memory::InMemory;
    use test_utils::{actions_to_string, add_commit, TestAction};

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::{engine_to_handle, free_engine, kernel_string_slice};

    /// Build an in-memory Delta table with `n` commits (versions 0..n-1) and return the
    /// engine handle plus the table root URI. Each commit contains a single AddFile;
    /// v=0 also carries the protocol+metadata baseline.
    async fn setup_n_commit_table(n: usize) -> (Handle<SharedExternEngine>, &'static str) {
        let table_root = "memory:///";
        let storage = Arc::new(InMemory::new());
        for v in 0..n {
            let actions = if v == 0 {
                vec![
                    TestAction::Metadata,
                    TestAction::Add(format!("v{v}.parquet")),
                ]
            } else {
                vec![TestAction::Add(format!("v{v}.parquet"))]
            };
            add_commit(table_root, storage.as_ref(), v as u64, actions_to_string(actions))
                .await
                .unwrap();
        }
        let engine = DefaultEngineBuilder::new(storage).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        (engine, table_root)
    }

    /// Test 1.T1: Build a CommitRange (with end-boundary applied via the
    /// consume-and-replace pattern) and free it. Verifies no panic / leak on the
    /// happy path through the builder pipeline.
    #[tokio::test]
    async fn test_commit_range_builder_build_freed() {
        let (engine, table_root) = setup_n_commit_table(1).await;

        let builder = unsafe {
            ok_or_panic(commit_range_builder_init(
                kernel_string_slice!(table_root),
                KernelCommitBoundary::Version(0),
                allocate_err,
            ))
        };

        // Consume-and-replace: returns a new handle. Old `builder` is dead.
        let builder =
            unsafe { commit_range_builder_with_end(builder, KernelCommitBoundary::Version(0)) };

        let range = unsafe {
            ok_or_panic(commit_range_builder_build(
                builder,
                engine.shallow_copy(),
                allocate_err,
            ))
        };

        unsafe {
            free_commit_range(range);
            free_engine(engine);
        }
    }

    /// Visitor used by `test_commits_iter_full_round_trip`. Pushes each emitted
    /// commit handle into a `Vec<Handle<ExclusiveCommitActions>>` reached via the
    /// `engine_context` pointer.
    extern "C" fn capture_commit_visitor(
        ctx: NullableCvoid,
        commit: Handle<ExclusiveCommitActions>,
    ) {
        let ctx_ptr = ctx
            .expect("non-null context")
            .as_ptr() as *mut Vec<Handle<ExclusiveCommitActions>>;
        unsafe { (*ctx_ptr).push(commit) };
    }

    /// Test 1.T2: Build a 2-commit range, drain `commits_iter_next` via a visitor,
    /// verify versions/timestamps are correct, then consume the first
    /// `CommitActions` into an action-batch iterator and pull batches via
    /// `action_batch_iter_next_arrow` until exhaustion.
    #[tokio::test]
    async fn test_commits_iter_full_round_trip() {
        let (engine, table_root) = setup_n_commit_table(2).await;

        let builder = unsafe {
            ok_or_panic(commit_range_builder_init(
                kernel_string_slice!(table_root),
                KernelCommitBoundary::Version(0),
                allocate_err,
            ))
        };
        let builder =
            unsafe { commit_range_builder_with_end(builder, KernelCommitBoundary::Version(1)) };
        let range = unsafe {
            ok_or_panic(commit_range_builder_build(
                builder,
                engine.shallow_copy(),
                allocate_err,
            ))
        };

        // Open commits iterator with ADD + METADATA.
        let iter = unsafe {
            ok_or_panic(commit_range_commits_iter_init(
                range.shallow_copy(),
                engine.shallow_copy(),
                KERNEL_ACTION_ADD | KERNEL_ACTION_METADATA,
                allocate_err,
            ))
        };

        // Drain via visitor into an owned Vec of commit handles.
        let mut commits: Vec<Handle<ExclusiveCommitActions>> = Vec::new();
        let ctx = NonNull::new((&mut commits as *mut Vec<_>).cast::<c_void>());
        loop {
            let more = unsafe {
                ok_or_panic(commits_iter_next(
                    iter.shallow_copy(),
                    ctx,
                    capture_commit_visitor,
                    allocate_err,
                ))
            };
            if !more {
                break;
            }
        }

        assert_eq!(commits.len(), 2, "two commits expected");
        for (i, commit) in commits.iter().enumerate() {
            let v = unsafe { commit_actions_version(commit.shallow_copy()) };
            assert_eq!(v, i as u64, "commit {i} version");
            let ts = unsafe { commit_actions_timestamp(commit.shallow_copy()) };
            assert!(ts > 0, "commit {i} timestamp should be positive");
        }

        // Take the first commit by ownership; consume it into an action-batch iterator.
        // commits.remove(0) moves the handle out — no longer accessed via shallow_copy.
        let first = commits.remove(0);
        let batch_iter = unsafe { commit_actions_into_actions_iter(first) };

        let ptr = unsafe {
            ok_or_panic(action_batch_iter_next_arrow(
                batch_iter.shallow_copy(),
                engine.shallow_copy(),
                allocate_err,
            ))
        };
        assert!(!ptr.is_null(), "first action batch must be non-null");
        unsafe { free_action_batch_arrow_result(ptr) };

        // Drain to exhaustion. Stop at the first null. Some commits may have multiple
        // batches; loop until we see null.
        loop {
            let ptr = unsafe {
                ok_or_panic(action_batch_iter_next_arrow(
                    batch_iter.shallow_copy(),
                    engine.shallow_copy(),
                    allocate_err,
                ))
            };
            if ptr.is_null() {
                break;
            }
            unsafe { free_action_batch_arrow_result(ptr) };
        }

        unsafe {
            free_action_batch_iter(batch_iter);
            for c in commits {
                free_commit_actions(c);
            }
            free_commits_iter(iter);
            free_commit_range(range);
            free_engine(engine);
        }
    }
}
