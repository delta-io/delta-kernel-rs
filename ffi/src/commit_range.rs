use std::sync::{Arc, Mutex, MutexGuard};

use delta_kernel::commit_range::{CommitAction, CommitRange, DeltaAction};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::{DeltaResult, DeltaResultIteratorStatic, Error, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

use crate::engine_funcs::{ExclusiveFileReadResultIterator, FileReadResultIterator};
use crate::handle::Handle;
use crate::{
    unwrap_and_parse_path_as_url, ExternEngine, ExternResult, IntoExternResult, KernelStringSlice,
    NullableCvoid, SharedExternEngine, SharedSnapshot,
};

/// An opaque, shared handle owning a [`CommitRange`] produced by
/// [`commit_range_builder_build`]. The caller owns the handle and must release it with
/// [`free_commit_range`].
#[handle_descriptor(target=CommitRange, mutable=false, sized=true)]
pub struct SharedCommitRange;

/// Opaque builder for constructing a [`CommitRange`] from a table path.
///
/// Create with [`commit_range_builder_for`]. Optionally pin the end of the range with
/// [`commit_range_builder_set_end_version`]. Finally, call [`commit_range_builder_build`] to
/// consume the builder and obtain the range. To discard the builder without building, call
/// [`free_commit_range_builder`].
pub struct FfiCommitRangeBuilder {
    engine: Arc<dyn ExternEngine>,
    table_root: Url,
    start_version: Version,
    end_version: Option<Version>,
}

/// An opaque handle with exclusive (Box-like) ownership of a [`FfiCommitRangeBuilder`].
#[handle_descriptor(target=FfiCommitRangeBuilder, mutable=true, sized=true)]
pub struct MutableFfiCommitRangeBuilder;

fn make_commit_range_builder(
    table_root: Url,
    start_version: Version,
    engine: Arc<dyn ExternEngine>,
) -> Handle<MutableFfiCommitRangeBuilder> {
    Box::new(FfiCommitRangeBuilder {
        engine,
        table_root,
        start_version,
        end_version: None,
    })
    .into()
}

/// Get a builder for constructing a [`CommitRange`] from a table path, starting at
/// `start_version`.
///
/// The caller owns the returned handle and must eventually call either
/// [`commit_range_builder_build`] to produce a [`CommitRange`], or [`free_commit_range_builder`]
/// to drop it without building.
///
/// # Safety
///
/// Caller is responsible for passing a valid path and engine handle.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_for(
    path: KernelStringSlice,
    start_version: Version,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<MutableFfiCommitRangeBuilder>> {
    let engine_ref = unsafe { engine.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    url.map(|url| make_commit_range_builder(url, start_version, engine_arc))
        .into_extern_result(&engine_ref)
}

/// Pin the end version of the range. When omitted, the range extends to the latest committed
/// version observed at build time.
///
/// # Safety
///
/// Caller must pass a valid builder pointer.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_set_end_version(
    builder: &mut Handle<MutableFfiCommitRangeBuilder>,
    end_version: Version,
) {
    unsafe { builder.as_mut() }.end_version = Some(end_version);
}

/// Consume the builder and return a [`CommitRange`]. After calling, the builder pointer is no
/// longer valid. The builder is always freed by this call, whether or not it succeeds.
///
/// Returns an error if the resolved version range is invalid (start > end), the listed commits
/// are non-contiguous, or the requested start version is not present.
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_build(
    builder: Handle<MutableFfiCommitRangeBuilder>,
) -> ExternResult<Handle<SharedCommitRange>> {
    let builder_box = unsafe { builder.into_inner() };
    let engine = builder_box.engine.clone();
    commit_range_builder_build_impl(*builder_box).into_extern_result(&engine.as_ref())
}

fn commit_range_builder_build_impl(
    builder: FfiCommitRangeBuilder,
) -> DeltaResult<Handle<SharedCommitRange>> {
    let engine = builder.engine.engine();
    let mut kernel_builder = CommitRange::builder_for(builder.table_root, builder.start_version);
    if let Some(end_version) = builder.end_version {
        kernel_builder = kernel_builder.with_end_version(end_version);
    }
    let range = kernel_builder.build(engine.as_ref())?;
    Ok(Arc::new(range).into())
}

/// Free a commit range builder without building a range (e.g. on an error path).
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_commit_range_builder(builder: Handle<MutableFfiCommitRangeBuilder>) {
    builder.drop_handle();
}

/// Free a [`CommitRange`].
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_commit_range(commit_range: Handle<SharedCommitRange>) {
    commit_range.drop_handle();
}

/// FFI-safe mirror of [`DeltaAction`]: the Delta log action kinds a caller can request from
/// [`commit_range_commits`].
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelDeltaAction {
    Add = 0,
    Remove = 1,
    Metadata = 2,
    Protocol = 3,
    CommitInfo = 4,
    Cdc = 5,
    DomainMetadata = 6,
    SetTxn = 7,
    CheckpointMetadata = 8,
    Sidecar = 9,
}

impl From<KernelDeltaAction> for DeltaAction {
    fn from(value: KernelDeltaAction) -> Self {
        match value {
            KernelDeltaAction::Add => DeltaAction::Add,
            KernelDeltaAction::Remove => DeltaAction::Remove,
            KernelDeltaAction::Metadata => DeltaAction::Metadata,
            KernelDeltaAction::Protocol => DeltaAction::Protocol,
            KernelDeltaAction::CommitInfo => DeltaAction::CommitInfo,
            KernelDeltaAction::Cdc => DeltaAction::Cdc,
            KernelDeltaAction::DomainMetadata => DeltaAction::DomainMetadata,
            KernelDeltaAction::SetTxn => DeltaAction::SetTxn,
            KernelDeltaAction::CheckpointMetadata => DeltaAction::CheckpointMetadata,
            KernelDeltaAction::Sidecar => DeltaAction::Sidecar,
        }
    }
}

/// Copy a C array of [`KernelDeltaAction`] into a `Vec<DeltaAction>`. A null pointer or zero
/// length yields an empty vector.
///
/// # Safety
///
/// `ptr` must point to `len` valid [`KernelDeltaAction`] values that remain valid for this call.
unsafe fn parse_actions(ptr: *const KernelDeltaAction, len: usize) -> Vec<DeltaAction> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice.iter().map(|action| (*action).into()).collect()
}

/// An opaque, shared handle to a single [`CommitAction`] yielded by [`commit_range_commits_next`].
/// The engine owns each handle passed to its visitor and must release it with
/// [`free_commit_action`].
#[handle_descriptor(target=CommitAction, mutable=false, sized=true)]
pub struct SharedCommitAction;

/// The commit version of this commit action.
///
/// # Safety
///
/// Caller must pass a valid commit action handle.
#[no_mangle]
pub unsafe extern "C" fn commit_action_version(commit_action: Handle<SharedCommitAction>) -> u64 {
    let commit_action = unsafe { commit_action.as_ref() };
    commit_action.version()
}

/// The commit timestamp (milliseconds since epoch) of this commit action.
///
/// # Safety
///
/// Caller must pass a valid commit action handle.
#[no_mangle]
pub unsafe extern "C" fn commit_action_timestamp(commit_action: Handle<SharedCommitAction>) -> i64 {
    let commit_action = unsafe { commit_action.as_ref() };
    commit_action.timestamp()
}

/// Get an iterator over this commit's action batches, projected to the read schema requested when
/// the iterator was created (the `actions` passed to [`commit_range_commits`]). Each batch is the
/// raw actions recorded in the commit JSON, with no column-mapping translation applied.
///
/// The caller owns the returned iterator: drain it with [`read_result_next`] and release it with
/// [`free_read_result_iter`].
///
/// # Safety
///
/// Caller is responsible for passing valid commit action and engine handles.
///
/// [`read_result_next`]: crate::engine_funcs::read_result_next
/// [`free_read_result_iter`]: crate::engine_funcs::free_read_result_iter
#[no_mangle]
pub unsafe extern "C" fn commit_action_get_actions(
    commit_action: Handle<SharedCommitAction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveFileReadResultIterator>> {
    let engine_ref = unsafe { engine.as_ref() };
    let commit_action = unsafe { commit_action.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    commit_action_get_actions_impl(commit_action, engine_arc).into_extern_result(&engine_ref)
}

fn commit_action_get_actions_impl(
    commit_action: &CommitAction,
    engine: Arc<dyn ExternEngine>,
) -> DeltaResult<Handle<ExclusiveFileReadResultIterator>> {
    let actions = commit_action.get_actions(engine.engine().as_ref())?;
    Ok(FileReadResultIterator::into_handle(actions, engine))
}

/// Free a [`CommitAction`].
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_commit_action(commit_action: Handle<SharedCommitAction>) {
    commit_action.drop_handle();
}

type CommitActionIter = DeltaResultIteratorStatic<CommitAction>;

/// Iterator handle returned by [`commit_range_commits`]. Holds the boxed kernel iterator behind a
/// mutex (so it is safe to share across threads) plus an engine reference for error allocation.
pub struct FfiCommitActionsIterator {
    data: Mutex<CommitActionIter>,
    engine: Arc<dyn ExternEngine>,
}

impl FfiCommitActionsIterator {
    fn lock_iter(&self) -> DeltaResult<MutexGuard<'_, CommitActionIter>> {
        self.data
            .lock()
            .map_err(|_| Error::generic("poisoned commit-actions iterator mutex"))
    }
}

#[handle_descriptor(target=FfiCommitActionsIterator, mutable=false, sized=true)]
pub struct SharedCommitActionsIterator;

/// Get an iterator over the commits in `commit_range`, yielding one [`CommitAction`] per commit.
/// Use [`commit_range_commits_with_snapshot`] to seed the iterator from a start snapshot instead.
///
/// - `engine`: performs the per-commit JSON reads and allocates errors.
/// - `actions` / `actions_len`: the action kinds to project into each commit's read schema.
///
/// Returns an error if `actions` is empty or contains duplicate kinds. The caller owns the
/// returned iterator and must release it with [`free_commit_actions_iter`].
///
/// # Safety
///
/// Caller is responsible for passing valid handles, and an `actions` pointer to `actions_len`
/// valid [`KernelDeltaAction`] values.
#[no_mangle]
pub unsafe extern "C" fn commit_range_commits(
    commit_range: Handle<SharedCommitRange>,
    engine: Handle<SharedExternEngine>,
    actions: *const KernelDeltaAction,
    actions_len: usize,
) -> ExternResult<Handle<SharedCommitActionsIterator>> {
    let engine_ref = unsafe { engine.as_ref() };
    let commit_range = unsafe { commit_range.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let actions = unsafe { parse_actions(actions, actions_len) };
    commit_range_commits_impl(commit_range, engine_arc, None, actions)
        .into_extern_result(&engine_ref)
}

/// Like [`commit_range_commits`], but seeds the iterator's protocol/metadata from `start_snapshot`.
///
/// The snapshot's version must match the range's anchor (the start version for ascending ranges,
/// the end version for descending ranges).
///
/// Returns an error if `actions` is empty or contains duplicate kinds, or if `start_snapshot`
/// belongs to a different table, its version does not match the range anchor, or its table does
/// not support scanning. The caller owns the returned iterator and must release it with
/// [`free_commit_actions_iter`].
///
/// # Safety
///
/// Caller is responsible for passing valid handles, and an `actions` pointer to `actions_len`
/// valid [`KernelDeltaAction`] values.
#[no_mangle]
pub unsafe extern "C" fn commit_range_commits_with_snapshot(
    commit_range: Handle<SharedCommitRange>,
    engine: Handle<SharedExternEngine>,
    start_snapshot: Handle<SharedSnapshot>,
    actions: *const KernelDeltaAction,
    actions_len: usize,
) -> ExternResult<Handle<SharedCommitActionsIterator>> {
    let engine_ref = unsafe { engine.as_ref() };
    let commit_range = unsafe { commit_range.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let start_snapshot = unsafe { start_snapshot.clone_as_arc() };
    let actions = unsafe { parse_actions(actions, actions_len) };
    commit_range_commits_impl(commit_range, engine_arc, Some(start_snapshot), actions)
        .into_extern_result(&engine_ref)
}

fn commit_range_commits_impl(
    commit_range: &CommitRange,
    engine: Arc<dyn ExternEngine>,
    start_snapshot: Option<SnapshotRef>,
    actions: Vec<DeltaAction>,
) -> DeltaResult<Handle<SharedCommitActionsIterator>> {
    let inner = commit_range.commits(engine.engine(), start_snapshot, &actions)?;
    let boxed: CommitActionIter = Box::new(inner);
    let iter = FfiCommitActionsIterator {
        data: Mutex::new(boxed),
        engine,
    };
    Ok(Arc::new(iter).into())
}

/// Call `engine_visitor` with the next [`CommitAction`] in the range, returning `true` if an item
/// was yielded and `false` once the iterator is exhausted. The visitor receives a
/// [`SharedCommitAction`] must free via [`free_commit_action`]. Per-commit protocol validation
/// runs here, so a malformed commit surfaces as an error from this call.
///
/// # Safety
///
/// The iterator must be valid (returned by [`commit_range_commits`]) and not yet freed by
/// [`free_commit_actions_iter`]. The visitor function pointer must be non-null.
#[no_mangle]
pub unsafe extern "C" fn commit_range_commits_next(
    data: Handle<SharedCommitActionsIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        commit_action: Handle<SharedCommitAction>,
    ),
) -> ExternResult<bool> {
    let data = unsafe { data.as_ref() };
    commit_range_commits_next_impl(data, engine_context, engine_visitor)
        .into_extern_result(&data.engine.as_ref())
}

fn commit_range_commits_next_impl(
    data: &FfiCommitActionsIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        commit_action: Handle<SharedCommitAction>,
    ),
) -> DeltaResult<bool> {
    let mut iter = data.lock_iter()?;
    match iter.next().transpose()? {
        Some(commit_action) => {
            (engine_visitor)(engine_context, Arc::new(commit_action).into());
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Free a commit-actions iterator.
///
/// # Safety
///
/// Caller is responsible for passing a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn free_commit_actions_iter(iter: Handle<SharedCommitActionsIterator>) {
    iter.drop_handle();
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use test_utils::{actions_to_string, add_commit, TestAction};

    use super::*;
    use crate::engine_data::engine_data_length;
    use crate::engine_funcs::{free_read_result_iter, read_result_next};
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_with_message, assert_timestamp_is_recent,
        build_snapshot, ok_or_panic,
    };
    use crate::{
        engine_to_handle, free_engine, free_engine_data, free_snapshot, kernel_string_slice,
        ExclusiveEngineData,
    };

    /// Build an in-memory engine handle pre-loaded with a metadata-only commit at each version in
    /// `0..=last_version`. Returns `(engine_handle, table_root)`; the caller frees the engine.
    async fn setup_engine_with_commits(
        last_version: u64,
    ) -> (Handle<SharedExternEngine>, &'static str) {
        let table_root = "memory:///test_table/";
        let storage = Arc::new(InMemory::new());
        for version in 0..=last_version {
            add_commit(
                table_root,
                storage.as_ref(),
                version,
                actions_to_string(vec![TestAction::Metadata]),
            )
            .await
            .unwrap();
        }
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        (engine_to_handle(Arc::new(engine), allocate_err), table_root)
    }

    /// Build a `[start_version, end_version]` commit range handle via the FFI builder API.
    unsafe fn build_range(
        table_root: &str,
        start_version: u64,
        end_version: u64,
        engine: Handle<SharedExternEngine>,
    ) -> Handle<SharedCommitRange> {
        let mut builder = unsafe {
            ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(table_root),
                start_version,
                engine,
            ))
        };
        unsafe { commit_range_builder_set_end_version(&mut builder, end_version) };
        unsafe { ok_or_panic(commit_range_builder_build(builder)) }
    }

    /// Engine visitor that reads each commit action's version, assert its timestamp is recent,
    /// then frees the handle.
    extern "C" fn collect_versions_and_assert_timestamp(
        ctx: NullableCvoid,
        commit_action: Handle<SharedCommitAction>,
    ) {
        let versions = unsafe { &mut *(ctx.unwrap().as_ptr() as *mut Vec<u64>) };
        let version = unsafe { commit_action_version(commit_action.shallow_copy()) };
        let timestamp = unsafe { commit_action_timestamp(commit_action.shallow_copy()) };
        assert_timestamp_is_recent(timestamp);
        versions.push(version);
        unsafe { free_commit_action(commit_action) }
    }

    /// Drain a commit-actions iterator, returning the versions yielded in order.
    unsafe fn drain_versions(iter: Handle<SharedCommitActionsIterator>) -> Vec<u64> {
        let mut versions = Vec::<u64>::new();
        let ctx = Some(unsafe { NonNull::new_unchecked(&mut versions) }.cast());
        while unsafe {
            ok_or_panic(commit_range_commits_next(
                iter.shallow_copy(),
                ctx,
                collect_versions_and_assert_timestamp,
            ))
        } {}
        versions
    }

    #[rstest]
    #[case::with_end_version(Some(2), 2)]
    #[case::without_end_version(None, 3)]
    #[tokio::test]
    async fn test_commit_range_builder_build_succeeds(
        #[case] builder_end_version: Option<Version>,
        #[case] expected_end_version: Version,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root) = setup_engine_with_commits(3).await;

        let mut builder = unsafe {
            ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(table_root),
                0,
                engine.shallow_copy(),
            ))
        };
        if let Some(end_version) = builder_end_version {
            unsafe { commit_range_builder_set_end_version(&mut builder, end_version) };
        }
        let range = unsafe { ok_or_panic(commit_range_builder_build(builder)) };

        assert_eq!(unsafe { range.as_ref() }.start_version(), 0);
        assert_eq!(
            unsafe { range.as_ref() }.end_version(),
            expected_end_version
        );

        unsafe { free_commit_range(range) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_range_builder_for_invalid_path_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _) = setup_engine_with_commits(0).await;

        let invalid_path = "not a valid url!";
        let result = unsafe {
            commit_range_builder_for(kernel_string_slice!(invalid_path), 0, engine.shallow_copy())
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidTableLocationError,
            None,
        );

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_free_commit_range_builder() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root) = setup_engine_with_commits(3).await;

        let builder = unsafe {
            ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(table_root),
                0,
                engine.shallow_copy(),
            ))
        };
        unsafe { free_commit_range_builder(builder) }

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_range_builder_build_errors_on_missing_start_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Table has only v=0, but we request a range starting at v=5.
        let (engine, table_root) = setup_engine_with_commits(0).await;

        let builder = unsafe {
            ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(table_root),
                5,
                engine.shallow_copy(),
            ))
        };
        let result = unsafe { commit_range_builder_build(builder) };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[rstest]
    #[case::no_snapshot_yields_each_commit(false, 0, 1, Some(vec![0, 1]))]
    #[case::snapshot_matched_anchor_yields_commit(true, 1, 1, Some(vec![1]))]
    #[case::snapshot_mismatched_anchor_errors(true, 0, 0, None)]
    #[tokio::test]
    async fn test_commit_range_commits(
        #[case] with_snapshot: bool,
        #[case] range_start: u64,
        #[case] range_end: u64,
        #[case] expected: Option<Vec<u64>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root) = setup_engine_with_commits(1).await;
        let range =
            unsafe { build_range(table_root, range_start, range_end, engine.shallow_copy()) };
        let actions = [KernelDeltaAction::Metadata, KernelDeltaAction::Protocol];

        let snapshot = with_snapshot.then(|| unsafe {
            build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy())
        });
        let result = unsafe {
            match &snapshot {
                Some(snapshot) => commit_range_commits_with_snapshot(
                    range.shallow_copy(),
                    engine.shallow_copy(),
                    snapshot.shallow_copy(),
                    actions.as_ptr(),
                    actions.len(),
                ),
                None => commit_range_commits(
                    range.shallow_copy(),
                    engine.shallow_copy(),
                    actions.as_ptr(),
                    actions.len(),
                ),
            }
        };

        match expected {
            Some(expected_versions) => {
                let iter = ok_or_panic(result);
                let versions = unsafe { drain_versions(iter.shallow_copy()) };
                assert_eq!(versions, expected_versions);
                unsafe { free_commit_actions_iter(iter) }
            }
            None => {
                assert_extern_result_error_with_message(result, KernelError::GenericError, None);
            }
        }

        if let Some(snapshot) = snapshot {
            unsafe { free_snapshot(snapshot) }
        }
        unsafe { free_commit_range(range) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_range_commits_errors_on_empty_actions(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root) = setup_engine_with_commits(1).await;
        let range = unsafe { build_range(table_root, 0, 1, engine.shallow_copy()) };

        let result = unsafe {
            commit_range_commits(
                range.shallow_copy(),
                engine.shallow_copy(),
                std::ptr::null(),
                0,
            )
        };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);

        unsafe { free_commit_range(range) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    /// Context for [`drain_commit_actions`]: the engine handle used to fetch each commit's action
    /// batches, plus a running total of action rows read across all commits.
    struct GetActionsCtx {
        engine: Handle<SharedExternEngine>,
        total_rows: usize,
    }

    /// Engine visitor that calculuates `EngineData` batch's row to an accumulator, then free the
    /// batch.
    extern "C" fn count_rows(ctx: NullableCvoid, data: Handle<ExclusiveEngineData>) {
        let total = unsafe { &mut *(ctx.unwrap().as_ptr() as *mut usize) };
        let mut data = data;
        *total += unsafe { engine_data_length(&mut data) };
        unsafe { free_engine_data(data) }
    }

    /// Engine visitor that read one commit's action batches via [`commit_action_get_actions`],
    /// accumulating their row count, then free the read-result iterator and the commit action.
    extern "C" fn drain_commit_actions(ctx: NullableCvoid, action: Handle<SharedCommitAction>) {
        let ctx = unsafe { &mut *(ctx.unwrap().as_ptr() as *mut GetActionsCtx) };
        let iter = unsafe {
            ok_or_panic(commit_action_get_actions(
                action.shallow_copy(),
                ctx.engine.shallow_copy(),
            ))
        };
        let rows_ctx = Some(unsafe { NonNull::new_unchecked(&mut ctx.total_rows) }.cast());
        while unsafe { ok_or_panic(read_result_next(iter.shallow_copy(), rows_ctx, count_rows)) } {}
        unsafe { free_read_result_iter(iter) }
        unsafe { free_commit_action(action) }
    }

    #[tokio::test]
    async fn test_commit_action_get_actions_yields_action_batches(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root) = setup_engine_with_commits(1).await;
        let range = unsafe { build_range(table_root, 0, 1, engine.shallow_copy()) };

        let actions = [KernelDeltaAction::Metadata];
        let iter = unsafe {
            ok_or_panic(commit_range_commits(
                range.shallow_copy(),
                engine.shallow_copy(),
                actions.as_ptr(),
                actions.len(),
            ))
        };

        let mut ctx = GetActionsCtx {
            engine: engine.shallow_copy(),
            total_rows: 0,
        };
        let ctx_ptr = Some(unsafe { NonNull::new_unchecked(&mut ctx) }.cast());
        while unsafe {
            ok_or_panic(commit_range_commits_next(
                iter.shallow_copy(),
                ctx_ptr,
                drain_commit_actions,
            ))
        } {}

        assert_eq!(ctx.total_rows, 6);

        unsafe { free_commit_actions_iter(iter) }
        unsafe { free_commit_range(range) }
        unsafe { free_engine(engine) }
        Ok(())
    }
}
