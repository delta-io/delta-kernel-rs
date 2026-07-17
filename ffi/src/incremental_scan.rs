//! FFI bindings for the incremental scan API.
//!
//! An incremental scan streams the file-action diff between a base version and a target
//! [`SharedSnapshot`]. The flow mirrors the Rust API:
//!
//! ```text
//! snapshot_incremental_scan_builder(snapshot, base_version, engine)
//!   -> incremental_scan_builder_with_predicate(builder, engine, predicate) // optional; prunes live Adds
//!   -> incremental_scan_builder_build(builder)   // -> OptionalValue<stream>; None => full-scan fallback
//! ```
//!
//! Pull the diff's live-Add batches as Arrow with repeated
//! [`incremental_scan_stream_next_arrow`] calls (newest-commit first), then consume the stream
//! with [`incremental_scan_stream_into_summary`] to recover the live-Add and Remove key sets.
//! `into_summary` is the terminal consumer: it drains any batches `next_arrow` left behind, then
//! frees the stream. Any error from `next_arrow` kills the stream, so a later `next_arrow` or
//! `into_summary` call fails; rebuild the stream to retry.
//!
//! [`incremental_scan_builder_build`] returns [`OptionalValue::None`] (not an error) when the
//! target snapshot's commit list can't cover the range, which is the signal to fall back to a
//! full scan.
//!
//! The Arrow batch step (`incremental_scan_stream_next_arrow`, which reuses
//! [`crate::scan::ScanMetadataArrowResult`]) requires the `default-engine-base` feature; the
//! builder, summary, and visitor entry points are always available.
//!
//! Pass-through fields decoded from a batch (`stats`, `partitionValues`, `baseRowId`) must be
//! interpreted against the protocol at that row's own commit version, not against the target
//! snapshot: in-range protocol/metadata changes are not yet surfaced (see
//! <https://github.com/delta-io/delta-kernel-rs/issues/2552>). For example, a `columnMapping`
//! change within the range shifts the physical column names keying `stats` and `partitionValues`,
//! so decoding older-commit rows against the target snapshot's names reads them wrong.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use delta_kernel::incremental_scan::{IncrementalScanStream, IncrementalScanSummary};
use delta_kernel::log_replay::FileActionKey;
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::{DeltaResult, Error, PredicateRef, Version};
use delta_kernel_ffi_macros::handle_descriptor;

#[cfg(feature = "default-engine-base")]
use crate::engine_data::ArrowFFIData;
use crate::handle::Handle;
use crate::scan::{decode_engine_predicate, EnginePredicate};
#[cfg(feature = "default-engine-base")]
use crate::scan::{CTransforms, ScanMetadataArrowResult};
use crate::{
    kernel_string_slice, ExternEngine, ExternResult, IntoExternResult, KernelStringSlice,
    NullableCvoid, OptionalValue, SharedExternEngine, SharedSnapshot,
};

/// Opaque builder for constructing an [`IncrementalScanStream`] from a snapshot.
///
/// Create with [`snapshot_incremental_scan_builder`]. Call [`incremental_scan_builder_build`] to
/// consume the builder and obtain the stream, or [`free_incremental_scan_builder`] to drop it
/// without building.
pub struct FfiIncrementalScanBuilder {
    engine: Arc<dyn ExternEngine>,
    target_snapshot: SnapshotRef,
    base_version: Version,
    predicate: Option<PredicateRef>,
}

/// An opaque handle with exclusive (Box-like) ownership of a [`FfiIncrementalScanBuilder`].
#[handle_descriptor(target=FfiIncrementalScanBuilder, mutable=true, sized=true)]
pub struct MutableFfiIncrementalScanBuilder;

/// An incremental scan stream, guarded by a mutex so it can cross the FFI boundary as a shared
/// handle. The stream itself is single-consumer; the mutex serializes concurrent `next` calls.
///
/// The engine `Arc` is retained so the JSON reader's runtime outlives the stream even if the
/// caller drops its own engine handle first.
pub struct FfiIncrementalScanStream {
    stream: Mutex<Option<IncrementalScanStream>>,
    engine: Arc<dyn ExternEngine>,
}

/// An opaque, shared handle owning an [`FfiIncrementalScanStream`]. Release with
/// [`free_incremental_scan_stream`], or consume it with
/// [`incremental_scan_stream_into_summary`].
#[handle_descriptor(target=FfiIncrementalScanStream, mutable=false, sized=true)]
pub struct SharedIncrementalScanStream;

/// An opaque, shared handle owning an [`IncrementalScanSummary`]. Release with
/// [`free_incremental_scan_summary`].
#[handle_descriptor(target=IncrementalScanSummary, mutable=false, sized=true)]
pub struct SharedIncrementalScanSummary;

/// Get a builder for an incremental scan over the range `(base_version, snapshot.version()]`.
///
/// The caller owns the returned handle and must eventually call either
/// [`incremental_scan_builder_build`] to produce a stream, or [`free_incremental_scan_builder`]
/// to drop it without building. Does not consume the snapshot handle.
///
/// # Safety
///
/// Caller must pass a valid snapshot handle and engine handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_incremental_scan_builder(
    snapshot: Handle<SharedSnapshot>,
    base_version: Version,
    engine: Handle<SharedExternEngine>,
) -> Handle<MutableFfiIncrementalScanBuilder> {
    let target_snapshot = unsafe { snapshot.clone_as_arc() };
    let engine = unsafe { engine.clone_as_arc() };
    Box::new(FfiIncrementalScanBuilder {
        engine,
        target_snapshot,
        base_version,
        predicate: None,
    })
    .into()
}

/// Apply a predicate to an incremental scan builder to prune streamed live Adds by their file
/// stats (data-skipping). Removes are never pruned; skipping is conservative, so the engine must
/// still re-apply the predicate at read time.
///
/// Consumes the `builder` handle and returns a new handle with the predicate applied. The
/// `builder` handle must not be used after this call. A later call replaces any previously
/// applied predicate. Returns an error if the engine's predicate visitor fails to produce a valid
/// predicate (i.e. returns an invalid expression ID); on error, the builder is dropped.
///
/// # Safety
///
/// `builder` and `engine` must be valid handles. The `builder` handle must not be used after this
/// call. `predicate` must be a valid, non-null [`EnginePredicate`] whose `visitor` and `predicate`
/// fields are safe to call and read.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_builder_with_predicate(
    builder: Handle<MutableFfiIncrementalScanBuilder>,
    engine: Handle<SharedExternEngine>,
    predicate: &mut EnginePredicate,
) -> ExternResult<Handle<MutableFfiIncrementalScanBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { builder.into_inner() };
    incremental_scan_builder_with_predicate_impl(*builder, predicate).into_extern_result(&engine)
}

fn incremental_scan_builder_with_predicate_impl(
    mut builder: FfiIncrementalScanBuilder,
    predicate: &mut EnginePredicate,
) -> DeltaResult<Handle<MutableFfiIncrementalScanBuilder>> {
    builder.predicate = Some(Arc::new(decode_engine_predicate(predicate)?));
    Ok(Box::new(builder).into())
}

/// Consume the builder and return the incremental scan stream, or [`OptionalValue::None`] when
/// the target snapshot's commit list can't cover `(base_version, target_version]`.
///
/// [`OptionalValue::None`] is not an error. It means an incremental advance isn't possible
/// (the target snapshot no longer retains commit `base_version + 1`, the first version in the
/// range, typically because a checkpoint above `base_version` truncated it), so the caller
/// should fall back to a full scan. The builder is always freed by this call, whether or not it
/// succeeds.
///
/// Returns an error if `base_version >= target_version`, the target snapshot's protocol has an
/// unsupported reader feature, a [`incremental_scan_builder_with_predicate`] predicate references
/// a column absent from the table schema, or the engine fails to open the commit stream.
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_builder_build(
    builder: Handle<MutableFfiIncrementalScanBuilder>,
) -> ExternResult<OptionalValue<Handle<SharedIncrementalScanStream>>> {
    let builder = unsafe { builder.into_inner() };
    let engine = builder.engine.clone();
    incremental_scan_builder_build_impl(*builder).into_extern_result(&engine.as_ref())
}

fn incremental_scan_builder_build_impl(
    builder: FfiIncrementalScanBuilder,
) -> DeltaResult<OptionalValue<Handle<SharedIncrementalScanStream>>> {
    let engine = builder.engine.engine();
    let maybe_stream = builder
        .target_snapshot
        .incremental_scan_builder(builder.base_version)
        .with_predicate(builder.predicate)
        .build(engine.as_ref())?;
    let handle = maybe_stream.map(|stream| {
        Arc::new(FfiIncrementalScanStream {
            stream: Mutex::new(Some(stream)),
            engine: builder.engine,
        })
        .into()
    });
    Ok(handle.into())
}

/// Free an incremental scan builder without building (e.g. on an error path).
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_incremental_scan_builder(
    builder: Handle<MutableFfiIncrementalScanBuilder>,
) {
    builder.drop_handle();
}

/// Get the next live-Add batch from the stream as Arrow via the C Data Interface.
///
/// Returns `Ok(non-null)` with the next [`ScanMetadataArrowResult`] (an Arrow batch paired with a
/// boolean selection vector; its `transforms` are always null, as an incremental scan never
/// rewrites rows), `Ok(null)` when the stream is exhausted, or `Err` on failure. Only rows
/// selected by the vector are live Adds; batches are yielded newest-commit first. The engine must
/// free each non-null result with [`crate::scan::free_scan_metadata_arrow_result`].
///
/// Any error kills the stream: later [`incremental_scan_stream_next_arrow`] and
/// [`incremental_scan_stream_into_summary`] calls both fail, so no partial summary is exposed after
/// a failure. Rebuild the stream to retry.
///
/// Decode pass-through fields against each row's own commit protocol, not the target snapshot;
/// see the module-level note on <https://github.com/delta-io/delta-kernel-rs/issues/2552>.
///
/// # Safety
///
/// Caller must pass a valid stream handle.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_stream_next_arrow(
    stream: Handle<SharedIncrementalScanStream>,
) -> ExternResult<*mut ScanMetadataArrowResult> {
    let stream = unsafe { stream.as_ref() };
    incremental_scan_stream_next_arrow_impl(stream).into_extern_result(&stream.engine.as_ref())
}

#[cfg(feature = "default-engine-base")]
fn incremental_scan_stream_next_arrow_impl(
    stream: &FfiIncrementalScanStream,
) -> DeltaResult<*mut ScanMetadataArrowResult> {
    let mut guard = lock_stream(stream)?;
    let Some(inner) = guard.as_mut() else {
        // The stream was already consumed by `into_summary` or dropped by a prior error.
        return Err(Error::generic(
            "incremental scan stream was already consumed",
        ));
    };
    let result = next_arrow_batch(inner);
    // Kill the stream on any error so a later next_arrow / into_summary can't return a partial
    // result; the caller must rebuild to retry.
    if result.is_err() {
        guard.take();
    }
    result
}

#[cfg(feature = "default-engine-base")]
fn next_arrow_batch(
    stream: &mut IncrementalScanStream,
) -> DeltaResult<*mut ScanMetadataArrowResult> {
    let Some(filtered) = stream.next().transpose()? else {
        return Ok(std::ptr::null_mut());
    };
    let (engine_data, selection_vector) = filtered.into_parts();
    let arrow_data = ArrowFFIData::try_from_engine_data(engine_data)?;
    let result = Box::new(ScanMetadataArrowResult {
        arrow_data,
        selection_vector: selection_vector.into(),
        transforms: Box::into_raw(Box::new(CTransforms::empty())),
    });
    Ok(Box::into_raw(result))
}

/// Drain any unread batches, then consume the stream and return its summary of live Add and
/// Remove file keys for the range.
///
/// Consumes the stream handle: the pointer is no longer valid after this call, whether or not it
/// succeeds. Returns an error if a prior [`incremental_scan_stream_next_arrow`] call errored, or
/// if draining the remaining batches fails.
///
/// `live_adds` are the file keys still present at the target version; `removes` are those removed
/// within the range. A file re-tagged in place (e.g. by OPTIMIZE or clustering) keeps its `(path,
/// dv_unique_id)` key, so it appears in `live_adds` but not `removes`.
///
/// # Safety
///
/// Caller must pass a valid stream handle and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_stream_into_summary(
    stream: Handle<SharedIncrementalScanStream>,
) -> ExternResult<Handle<SharedIncrementalScanSummary>> {
    let inner_stream = unsafe { stream.into_inner() };
    let engine = inner_stream.engine.clone();
    incremental_scan_stream_into_summary_impl(&inner_stream).into_extern_result(&engine.as_ref())
}

fn incremental_scan_stream_into_summary_impl(
    stream: &FfiIncrementalScanStream,
) -> DeltaResult<Handle<SharedIncrementalScanSummary>> {
    let inner = lock_stream(stream)?
        .take()
        .ok_or_else(|| Error::generic("incremental scan stream was already consumed"))?;
    let summary = inner.into_summary()?;
    Ok(Arc::new(summary).into())
}

fn lock_stream(
    stream: &FfiIncrementalScanStream,
) -> DeltaResult<std::sync::MutexGuard<'_, Option<IncrementalScanStream>>> {
    stream
        .stream
        .lock()
        .map_err(|_| Error::generic("poisoned incremental scan stream mutex"))
}

/// The base (exclusive lower bound) version of the scanned range.
///
/// # Safety
///
/// Caller must pass a valid summary handle.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_summary_base_version(
    summary: Handle<SharedIncrementalScanSummary>,
) -> Version {
    unsafe { summary.as_ref() }.base_version
}

/// The target (inclusive upper bound) version of the scanned range.
///
/// # Safety
///
/// Caller must pass a valid summary handle.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_summary_target_version(
    summary: Handle<SharedIncrementalScanSummary>,
) -> Version {
    unsafe { summary.as_ref() }.target_version
}

/// Visit each live-Add file key in the summary, invoking `callback` once per key with its path
/// and (nullable) deletion-vector unique id.
///
/// Match on the whole `(path, dv_unique_id)` key, not the path alone: the same path with different
/// DV ids refers to distinct logical files. `dv_unique_id` is passed as a zero-length slice when
/// the file has no deletion vector.
///
/// The slices passed to the callback are only valid for the duration of the callback.
///
/// # Safety
///
/// Caller must pass a valid summary handle and a non-null `callback` function pointer.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_summary_visit_live_adds(
    summary: Handle<SharedIncrementalScanSummary>,
    engine_context: NullableCvoid,
    callback: FileKeyCallback,
) {
    let summary = unsafe { summary.as_ref() };
    visit_file_keys(&summary.live_adds, engine_context, callback);
}

/// Visit each Remove file key in the summary, invoking `callback` once per key with its path and
/// (nullable) deletion-vector unique id.
///
/// See [`incremental_scan_summary_visit_live_adds`] for the callback contract.
///
/// # Safety
///
/// Caller must pass a valid summary handle and a non-null `callback` function pointer.
#[no_mangle]
pub unsafe extern "C" fn incremental_scan_summary_visit_removes(
    summary: Handle<SharedIncrementalScanSummary>,
    engine_context: NullableCvoid,
    callback: FileKeyCallback,
) {
    let summary = unsafe { summary.as_ref() };
    visit_file_keys(&summary.removes, engine_context, callback);
}

/// Callback invoked once per file key by the summary visitors. `dv_unique_id` is a zero-length
/// slice when the file carries no deletion vector. Both slices are only valid for the duration
/// of the call.
pub type FileKeyCallback = extern "C" fn(
    engine_context: NullableCvoid,
    path: KernelStringSlice,
    dv_unique_id: KernelStringSlice,
);

fn visit_file_keys(
    keys: &HashSet<FileActionKey>,
    engine_context: NullableCvoid,
    callback: FileKeyCallback,
) {
    for key in keys {
        let path = key.path();
        let dv = key.dv_unique_id().unwrap_or("");
        callback(
            engine_context,
            kernel_string_slice!(path),
            kernel_string_slice!(dv),
        );
    }
}

/// Free an incremental scan stream. Use this when abandoning a stream without draining it to a
/// summary. After [`incremental_scan_stream_into_summary`] consumes the stream, do not call this.
///
/// # Safety
///
/// Caller must pass a valid stream handle and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_incremental_scan_stream(stream: Handle<SharedIncrementalScanStream>) {
    stream.drop_handle();
}

/// Free an incremental scan summary.
///
/// # Safety
///
/// Caller must pass a valid summary handle and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_incremental_scan_summary(
    summary: Handle<SharedIncrementalScanSummary>,
) {
    summary.drop_handle();
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ffi::c_void;
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use test_utils::{actions_to_string, add_commit, TestAction};

    use super::*;
    use crate::error::KernelError;
    use crate::expressions::kernel_visitor::{
        visit_expression_column, visit_expression_literal_int, visit_predicate_gt,
        KernelExpressionVisitorState,
    };
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_with_message, ok_or_panic, recover_error,
    };
    #[cfg(feature = "default-engine-base")]
    use crate::scan::free_scan_metadata_arrow_result;
    use crate::{
        engine_to_handle, free_engine, free_snapshot, get_snapshot_builder, kernel_string_slice,
        snapshot_builder_build, snapshot_builder_set_version, NullableCvoid, TryFromStringSlice,
    };

    /// Build an in-memory engine handle and a snapshot pinned to the last version, from a v0
    /// metadata commit (`v0_metadata`) followed by `commits` (raw JSON) at v1.. Returns
    /// `(engine_handle, snapshot_handle)`; the caller frees both.
    async fn setup_from_raw(
        v0_metadata: String,
        commits: Vec<String>,
    ) -> (Handle<SharedExternEngine>, Handle<SharedSnapshot>) {
        let table_root = "memory:///";
        let storage = Arc::new(InMemory::new());
        add_commit(table_root, storage.as_ref(), 0, v0_metadata)
            .await
            .unwrap();
        let target_version = commits.len() as u64;
        for (idx, body) in commits.into_iter().enumerate() {
            add_commit(table_root, storage.as_ref(), (idx + 1) as u64, body)
                .await
                .unwrap();
        }
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        unsafe { snapshot_builder_set_version(&mut builder, target_version) };
        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
        (engine, snapshot)
    }

    /// Standard `id`/`val` metadata commit plus `TestAction` commit bodies at v1..
    async fn setup(
        commits: Vec<Vec<TestAction>>,
    ) -> (Handle<SharedExternEngine>, Handle<SharedSnapshot>) {
        let v0 = actions_to_string(vec![TestAction::Metadata]);
        setup_from_raw(v0, commits.into_iter().map(actions_to_string).collect()).await
    }

    // Protocol + metadata commit enabling the deletionVectors feature, so DV-bearing Adds
    // parse. `TestAction` can't emit a `deletionVector` field, so DV tests write raw JSON.
    const DV_METADATA: &str = "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[]}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}";

    fn add_with_dv(path: &str, storage_type: &str, path_or_inline: &str, offset: i32) -> String {
        format!(
            "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":null,\"deletionVector\":{{\"storageType\":\"{storage_type}\",\"pathOrInlineDv\":\"{path_or_inline}\",\"offset\":{offset},\"sizeInBytes\":10,\"cardinality\":1}}}}}}"
        )
    }

    /// Build an engine + snapshot from raw commit-JSON strings at v1.., with a DV-enabling
    /// metadata commit at v0.
    async fn setup_raw(
        commits: Vec<String>,
    ) -> (Handle<SharedExternEngine>, Handle<SharedSnapshot>) {
        setup_from_raw(DV_METADATA.to_string(), commits).await
    }

    // Collect the (path, dv_unique_id) pairs a summary visitor reports. `RefCell` because the
    // C callback takes `&self` context; the visit is single-threaded.
    extern "C" fn collect_key(
        engine_context: NullableCvoid,
        path: KernelStringSlice,
        dv_unique_id: KernelStringSlice,
    ) {
        let keys =
            engine_context.unwrap().as_ptr() as *const RefCell<Vec<(String, Option<String>)>>;
        let path = unsafe { String::try_from_slice(&path) }.unwrap();
        let dv = unsafe { String::try_from_slice(&dv_unique_id) }.unwrap();
        let dv = if dv.is_empty() { None } else { Some(dv) };
        unsafe { &*keys }.borrow_mut().push((path, dv));
    }

    fn visit_live_adds(
        summary: &Handle<SharedIncrementalScanSummary>,
    ) -> Vec<(String, Option<String>)> {
        let keys = RefCell::new(Vec::new());
        let ctx = std::ptr::NonNull::new(&keys as *const _ as *mut std::ffi::c_void);
        unsafe {
            incremental_scan_summary_visit_live_adds(summary.shallow_copy(), ctx, collect_key)
        };
        keys.into_inner()
    }

    fn visit_removes(
        summary: &Handle<SharedIncrementalScanSummary>,
    ) -> Vec<(String, Option<String>)> {
        let keys = RefCell::new(Vec::new());
        let ctx = std::ptr::NonNull::new(&keys as *const _ as *mut std::ffi::c_void);
        unsafe { incremental_scan_summary_visit_removes(summary.shallow_copy(), ctx, collect_key) };
        keys.into_inner()
    }

    /// Build a stream over `(base_version, target]`, asserting the range is covered. Tests that
    /// exercise the builder-error or `OptionalValue::None` paths call the FFI directly instead.
    fn build_stream(
        snapshot: &Handle<SharedSnapshot>,
        engine: &Handle<SharedExternEngine>,
        base_version: Version,
    ) -> Handle<SharedIncrementalScanStream> {
        let builder = unsafe {
            snapshot_incremental_scan_builder(
                snapshot.shallow_copy(),
                base_version,
                engine.shallow_copy(),
            )
        };
        let maybe_stream: Option<Handle<SharedIncrementalScanStream>> =
            unsafe { ok_or_panic(incremental_scan_builder_build(builder)) }.into();
        maybe_stream.expect("range covered, expected Some(stream)")
    }

    #[tokio::test]
    async fn build_and_summary_reports_live_adds_and_removes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Range (0, 3]: A added at v1, removed at v3; B added at v2 stays live; C added at v3.
        let (engine, snapshot) = setup(vec![
            vec![TestAction::Add("A".to_string())],
            vec![TestAction::Add("B".to_string())],
            vec![
                TestAction::Add("C".to_string()),
                TestAction::Remove("A".to_string()),
            ],
        ])
        .await;

        let stream = build_stream(&snapshot, &engine, 0);

        let summary = unsafe { ok_or_panic(incremental_scan_stream_into_summary(stream)) };

        assert_eq!(
            unsafe { incremental_scan_summary_base_version(summary.shallow_copy()) },
            0
        );
        assert_eq!(
            unsafe { incremental_scan_summary_target_version(summary.shallow_copy()) },
            3
        );

        let mut live_adds: Vec<_> = visit_live_adds(&summary)
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        live_adds.sort();
        assert_eq!(live_adds, vec!["B".to_string(), "C".to_string()]);

        let removes: Vec<_> = visit_removes(&summary)
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert_eq!(removes, vec!["A".to_string()]);

        unsafe { free_incremental_scan_summary(summary) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    async fn next_arrow_drains_then_null_then_summary() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup(vec![
            vec![TestAction::Add("A".to_string())],
            vec![TestAction::Add("B".to_string())],
        ])
        .await;

        let stream = build_stream(&snapshot, &engine, 0);

        let mut batches = 0;
        loop {
            let ptr =
                unsafe { ok_or_panic(incremental_scan_stream_next_arrow(stream.shallow_copy())) };
            if ptr.is_null() {
                break;
            }
            batches += 1;
            unsafe { free_scan_metadata_arrow_result(ptr) };
        }
        assert_eq!(batches, 2, "one live-Add batch per commit");

        // The stream is drained; into_summary still recovers the key sets.
        let summary = unsafe { ok_or_panic(incremental_scan_stream_into_summary(stream)) };
        let mut live_adds: Vec<_> = visit_live_adds(&summary)
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        live_adds.sort();
        assert_eq!(live_adds, vec!["A".to_string(), "B".to_string()]);

        unsafe { free_incremental_scan_summary(summary) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn build_errors_when_base_not_below_target() -> Result<(), Box<dyn std::error::Error>> {
        // Target snapshot is v2; base_version 2 makes the range empty.
        let (engine, snapshot) = setup(vec![
            vec![TestAction::Add("A".to_string())],
            vec![TestAction::Add("B".to_string())],
        ])
        .await;

        let builder = unsafe {
            snapshot_incremental_scan_builder(snapshot.shallow_copy(), 2, engine.shallow_copy())
        };
        let result = unsafe { incremental_scan_builder_build(builder) };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn free_builder_without_building() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup(vec![vec![TestAction::Add("A".to_string())]]).await;

        let builder = unsafe {
            snapshot_incremental_scan_builder(snapshot.shallow_copy(), 0, engine.shallow_copy())
        };
        unsafe { free_incremental_scan_builder(builder) };

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // The whole point of the (path, dv_unique_id) key is that the same path with different DV
    // ids is a distinct file. Assert the FFI visitor passes the full key through, not just the
    // path: v1 adds (X, dv=uabc@1), v2 adds (X, dv=uxyz@2) -> two distinct live Adds.
    #[tokio::test]
    async fn visit_reports_full_dv_unique_id() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup_raw(vec![
            add_with_dv("X.parquet", "u", "abc", 1),
            add_with_dv("X.parquet", "u", "xyz", 2),
        ])
        .await;

        let stream = build_stream(&snapshot, &engine, 0);
        let summary = unsafe { ok_or_panic(incremental_scan_stream_into_summary(stream)) };

        let mut live_adds = visit_live_adds(&summary);
        live_adds.sort();
        assert_eq!(
            live_adds,
            vec![
                ("X.parquet".to_string(), Some("uabc@1".to_string())),
                ("X.parquet".to_string(), Some("uxyz@2".to_string())),
            ],
        );

        unsafe { free_incremental_scan_summary(summary) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // into_summary consumes the stream by taking the inner Option. When a second Arc reference
    // to the same stream survives (via clone_handle, which bumps the refcount), a further
    // terminal call hits the taken-Option guard and errors cleanly instead of double-consuming.
    // (A single-refcount handle reused after into_summary is use-after-free per the # Safety
    // contract, not this path; this test holds a real second reference.)
    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    // The surviving handle keeps a second engine `Arc` live past the drain, so the background
    // executor's thread outlives the test; Miri flags the unjoined thread at teardown even though
    // the assertions pass. Skip under Miri, matching the tokio-FFI tests in `transaction`.
    #[cfg_attr(miri, ignore)]
    async fn terminal_calls_after_into_summary_error() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup(vec![vec![TestAction::Add("A".to_string())]]).await;

        let stream = build_stream(&snapshot, &engine, 0);

        // A second owning reference keeps the object alive after into_summary consumes it.
        let surviving = unsafe { stream.clone_handle() };
        let summary = unsafe { ok_or_panic(incremental_scan_stream_into_summary(stream)) };

        let next = unsafe { incremental_scan_stream_next_arrow(surviving.shallow_copy()) };
        assert_extern_result_error_with_message(next, KernelError::GenericError, None);
        let again = unsafe { incremental_scan_stream_into_summary(surviving) };
        assert_extern_result_error_with_message(again, KernelError::GenericError, None);

        unsafe { free_incremental_scan_summary(summary) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // A malformed `deletionVector` makes the kernel stream yield an error when the batch is
    // processed. The first `next_arrow` surfaces it, and the error kills the stream: a second
    // `next_arrow` and a following `into_summary` both fail rather than exposing a partial summary.
    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    async fn next_arrow_error_kills_stream() -> Result<(), Box<dyn std::error::Error>> {
        // A DV with `storageType` set but `pathOrInlineDv` null fails the required-field read
        // when the stream extracts the file key.
        let bad_dv = "{\"add\":{\"path\":\"X.parquet\",\"partitionValues\":{},\"size\":100,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":null,\"deletionVector\":{\"storageType\":\"u\",\"pathOrInlineDv\":null,\"offset\":1,\"sizeInBytes\":10,\"cardinality\":1}}}";
        let (engine, snapshot) = setup_raw(vec![bad_dv.to_string()]).await;

        let stream = build_stream(&snapshot, &engine, 0);

        // The first call surfaces the read error; later calls hit the taken-Option guard and report
        // GenericError. Either way, no partial result leaks through.
        let first = unsafe { incremental_scan_stream_next_arrow(stream.shallow_copy()) };
        let ExternResult::Err(e) = first else {
            panic!("malformed DV must error");
        };
        drop(unsafe { recover_error(e) });

        let again = unsafe { incremental_scan_stream_next_arrow(stream.shallow_copy()) };
        assert_extern_result_error_with_message(again, KernelError::GenericError, None);
        let summary = unsafe { incremental_scan_stream_into_summary(stream) };
        assert_extern_result_error_with_message(summary, KernelError::GenericError, None);

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // Abandoning a never-drained stream must free the retained engine Arc and undrained kernel
    // stream without leak or panic (the symmetric counterpart to free_builder_without_building).
    #[tokio::test]
    async fn free_stream_without_draining() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup(vec![vec![TestAction::Add("A".to_string())]]).await;

        let stream = build_stream(&snapshot, &engine, 0);
        unsafe { free_incremental_scan_stream(stream) };

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // into_summary must drain the batches the caller didn't read. Read exactly one of two
    // batches, then assert the summary still reports both commits' Adds.
    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    async fn into_summary_drains_unread_batches() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup(vec![
            vec![TestAction::Add("A".to_string())],
            vec![TestAction::Add("B".to_string())],
        ])
        .await;

        let stream = build_stream(&snapshot, &engine, 0);

        // Read exactly one batch, leaving the other for into_summary to drain.
        let ptr = unsafe { ok_or_panic(incremental_scan_stream_next_arrow(stream.shallow_copy())) };
        assert!(!ptr.is_null());
        unsafe { free_scan_metadata_arrow_result(ptr) };

        let summary = unsafe { ok_or_panic(incremental_scan_stream_into_summary(stream)) };
        let mut live_adds: Vec<_> = visit_live_adds(&summary)
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        live_adds.sort();
        assert_eq!(live_adds, vec!["A".to_string(), "B".to_string()]);

        unsafe { free_incremental_scan_summary(summary) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // Metadata declaring a single `id: integer` column, so `id` predicates are eligible for data
    // skipping against `add.stats`.
    const ID_METADATA: &str = "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}";

    // Raw Add carrying `id` stats over the closed range `[id_min, id_max]`.
    fn add_with_id_stats(path: &str, id_min: i32, id_max: i32) -> String {
        let stats = format!(
            "{{\\\"numRecords\\\":10,\\\"nullCount\\\":{{\\\"id\\\":0}},\\\"minValues\\\":{{\\\"id\\\":{id_min}}},\\\"maxValues\\\":{{\\\"id\\\":{id_max}}}}}"
        );
        format!(
            "{{\"add\":{{\"path\":\"{path}\",\"partitionValues\":{{}},\"size\":100,\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{stats}\"}}}}"
        )
    }

    /// Build an engine + snapshot from raw commit-JSON strings at v1.., with an `id`-column
    /// metadata commit at v0.
    async fn setup_with_id_metadata(
        commits: Vec<String>,
    ) -> (Handle<SharedExternEngine>, Handle<SharedSnapshot>) {
        setup_from_raw(ID_METADATA.to_string(), commits).await
    }

    /// Predicate visitor that constructs `id > 25`.
    extern "C" fn visit_id_gt_25(
        _pred_ptr: *mut c_void,
        state: &mut KernelExpressionVisitorState,
    ) -> usize {
        let id = "id";
        let col = unsafe {
            ok_or_panic(visit_expression_column(
                state,
                kernel_string_slice!(id),
                allocate_err,
            ))
        };
        let lit = visit_expression_literal_int(state, 25);
        visit_predicate_gt(state, col, lit)
    }

    // A predicate applied through the builder prunes streamed live Adds by their `add.stats`:
    // `id > 25` drops the [0, 9] file and keeps the [20, 30] file. Removes are unaffected.
    #[tokio::test]
    async fn with_predicate_prunes_live_adds() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) = setup_with_id_metadata(vec![[
            add_with_id_stats("low.parquet", 0, 9),
            add_with_id_stats("high.parquet", 20, 30),
        ]
        .join("\n")])
        .await;

        let builder = unsafe {
            snapshot_incremental_scan_builder(snapshot.shallow_copy(), 0, engine.shallow_copy())
        };
        let mut predicate = EnginePredicate {
            predicate: std::ptr::null_mut(),
            visitor: visit_id_gt_25,
        };
        let builder = unsafe {
            ok_or_panic(incremental_scan_builder_with_predicate(
                builder,
                engine.shallow_copy(),
                &mut predicate,
            ))
        };
        let maybe_stream: Option<Handle<SharedIncrementalScanStream>> =
            unsafe { ok_or_panic(incremental_scan_builder_build(builder)) }.into();
        let stream = maybe_stream.expect("range covered, expected Some(stream)");
        let summary = unsafe { ok_or_panic(incremental_scan_stream_into_summary(stream)) };

        let live_adds: Vec<_> = visit_live_adds(&summary)
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert_eq!(
            live_adds,
            vec!["high.parquet".to_string()],
            "only the file whose stats overlap `id > 25` survives"
        );

        unsafe { free_incremental_scan_summary(summary) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // A predicate referencing a column absent from the table schema fails at build (fail-fast),
    // surfacing the unresolved column rather than silently keeping every Add.
    #[tokio::test]
    async fn with_predicate_unknown_column_errors_at_build(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot) =
            setup_with_id_metadata(vec![add_with_id_stats("a.parquet", 0, 9)]).await;

        let builder = unsafe {
            snapshot_incremental_scan_builder(snapshot.shallow_copy(), 0, engine.shallow_copy())
        };
        // `id > 25` is fine to decode, but we point the visitor at a missing column below.
        let mut predicate = EnginePredicate {
            predicate: std::ptr::null_mut(),
            visitor: visit_nonexistent_gt_25,
        };
        let builder = unsafe {
            ok_or_panic(incremental_scan_builder_with_predicate(
                builder,
                engine.shallow_copy(),
                &mut predicate,
            ))
        };
        let result = unsafe { incremental_scan_builder_build(builder) };
        assert_extern_result_error_with_message(result, KernelError::MissingColumnError, None);

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    /// Predicate visitor that constructs `nonexistent > 25` over a column not in the schema.
    extern "C" fn visit_nonexistent_gt_25(
        _pred_ptr: *mut c_void,
        state: &mut KernelExpressionVisitorState,
    ) -> usize {
        let missing = "nonexistent";
        let col = unsafe {
            ok_or_panic(visit_expression_column(
                state,
                kernel_string_slice!(missing),
                allocate_err,
            ))
        };
        let lit = visit_expression_literal_int(state, 25);
        visit_predicate_gt(state, col, lit)
    }

    // The predicate's selection vector must line up with the drained Arrow batch, not just the
    // summary: two Adds land in one batch, `id > 25` prunes the low file, and only the high file's
    // row is selected. Guards the FFI-owned "only `true` rows are live Adds" contract for
    // `next_arrow` (the summary path is covered separately by `with_predicate_prunes_live_adds`).
    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    async fn next_arrow_selection_vector_reflects_predicate_pruning(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use delta_kernel::arrow::array::{Array, RecordBatch, StringArray, StructArray};
        use delta_kernel::arrow::ffi::from_ffi;

        let (engine, snapshot) = setup_with_id_metadata(vec![[
            add_with_id_stats("low.parquet", 0, 9),
            add_with_id_stats("high.parquet", 20, 30),
        ]
        .join("\n")])
        .await;

        let builder = unsafe {
            snapshot_incremental_scan_builder(snapshot.shallow_copy(), 0, engine.shallow_copy())
        };
        let mut predicate = EnginePredicate {
            predicate: std::ptr::null_mut(),
            visitor: visit_id_gt_25,
        };
        let builder = unsafe {
            ok_or_panic(incremental_scan_builder_with_predicate(
                builder,
                engine.shallow_copy(),
                &mut predicate,
            ))
        };
        let maybe_stream: Option<Handle<SharedIncrementalScanStream>> =
            unsafe { ok_or_panic(incremental_scan_builder_build(builder)) }.into();
        let stream = maybe_stream.expect("range covered, expected Some(stream)");

        let ptr = unsafe { ok_or_panic(incremental_scan_stream_next_arrow(stream.shallow_copy())) };
        assert!(!ptr.is_null(), "expected one batch for the single commit");
        let ScanMetadataArrowResult {
            arrow_data,
            selection_vector,
            transforms,
        } = unsafe { *Box::from_raw(ptr) };
        // Incremental scan never rewrites rows, but the transforms box is always allocated.
        assert!(!transforms.is_null());
        drop(unsafe { Box::from_raw(transforms) });
        let array_data = unsafe { from_ffi(arrow_data.array, &arrow_data.schema) }?;
        let batch: RecordBatch = StructArray::from(array_data).into();
        let sv = unsafe { selection_vector.into_vec() };

        // The batch carries every action row read; only the row whose `add.path` is the high file
        // is selected. Inspect the column rather than assuming row order.
        let add_paths = batch
            .column_by_name("add")
            .and_then(|c| c.as_any().downcast_ref::<StructArray>())
            .and_then(|s| s.column_by_name("path"))
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("add.path column");
        let selected: Vec<&str> = (0..batch.num_rows())
            .filter(|&i| sv[i] && !add_paths.is_null(i))
            .map(|i| add_paths.value(i))
            .collect();
        assert_eq!(
            selected,
            vec!["high.parquet"],
            "only the high file is selected"
        );

        let ptr = unsafe { ok_or_panic(incremental_scan_stream_next_arrow(stream.shallow_copy())) };
        assert!(ptr.is_null(), "stream exhausted after one batch");

        unsafe { free_incremental_scan_stream(stream) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    // `build` returns `OptionalValue::None` (not an error) when the target snapshot's commit list
    // can't cover `base_version + 1` -- the signal to fall back to a full scan. A checkpoint above
    // the base truncates the retained commit JSONs so version 1 is no longer listed; a base of 0
    // then requires commit 1 and the range is uncovered. Needs a multi-thread runtime because
    // `checkpoint_snapshot` issues nested `block_on` calls.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_returns_none_when_range_not_covered() -> Result<(), Box<dyn std::error::Error>> {
        use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;

        use crate::{checkpoint_snapshot, version, FfiCheckpointWriteResult};

        let table_root = "memory:///";
        let storage = Arc::new(InMemory::new());
        add_commit(
            table_root,
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await
        .unwrap();
        for v in 1..=3u64 {
            add_commit(
                table_root,
                storage.as_ref(),
                v,
                actions_to_string(vec![TestAction::Add(format!("file{v}.parquet"))]),
            )
            .await
            .unwrap();
        }

        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        let engine = engine_to_handle(
            Arc::new(
                DefaultEngineBuilder::new(storage.clone())
                    .with_task_executor(executor)
                    .build(),
            ),
            allocate_err,
        );

        // Checkpoint at v2 so the log segment for a later snapshot starts at the checkpoint and
        // drops commit 1's JSON.
        let mut builder_v2 = unsafe {
            ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        unsafe { snapshot_builder_set_version(&mut builder_v2, 2) };
        let snapshot_v2 = unsafe { ok_or_panic(snapshot_builder_build(builder_v2)) };
        // checkpoint_snapshot borrows its handle (clone_as_arc) but does not consume it, so pass a
        // shallow copy and free the original separately.
        let checkpointed = match unsafe {
            ok_or_panic(checkpoint_snapshot(
                snapshot_v2.shallow_copy(),
                engine.shallow_copy(),
                None,
            ))
        } {
            FfiCheckpointWriteResult::Written(s) | FfiCheckpointWriteResult::AlreadyExists(s) => s,
        };
        assert_eq!(unsafe { version(checkpointed.shallow_copy()) }, 2);
        unsafe { free_snapshot(checkpointed) };
        unsafe { free_snapshot(snapshot_v2) };

        // Fresh snapshot at v3: its log segment starts at the v2 checkpoint, so commit 1 is gone.
        let mut builder_v3 = unsafe {
            ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        unsafe { snapshot_builder_set_version(&mut builder_v3, 3) };
        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder_v3)) };

        // base_version 0 needs commit 1, which the checkpoint truncated => None, not an error.
        let builder = unsafe {
            snapshot_incremental_scan_builder(snapshot.shallow_copy(), 0, engine.shallow_copy())
        };
        let maybe_stream: Option<Handle<SharedIncrementalScanStream>> =
            unsafe { ok_or_panic(incremental_scan_builder_build(builder)) }.into();
        assert!(
            maybe_stream.is_none(),
            "range not covered => None (full-scan fallback signal), not an error"
        );

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }
}
