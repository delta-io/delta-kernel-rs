//! Builders for constructing and updating snapshots.

use std::sync::Arc;

use delta_kernel::snapshot::{IncrementalReplay, Snapshot, SnapshotHint, SnapshotRef};
use delta_kernel::{Engine, KernelResult, LogPath, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

use super::hint;
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    ExternEngine, KernelStringSlice, SharedExternEngine, SharedSnapshot, log_path,
    unwrap_and_parse_path_as_url,
};

/// Opaque builder for constructing a [`SharedSnapshot`].
///
/// Create with [`get_snapshot_builder`] (from a table path) or [`get_snapshot_builder_from`]
/// (incrementally from an existing snapshot). Configure with [`snapshot_builder_with_version`],
/// [`snapshot_builder_with_incremental_crc_replay`], [`snapshot_builder_with_log_tail`], and
/// [`snapshot_builder_with_max_catalog_version`] (for catalog-managed tables). Builders returned
/// by [`get_snapshot_builder`] may instead receive a complete typed snapshot hint with
/// [`hint::snapshot_builder_with_snapshot_hint`]. Use [`snapshot_builder_with_correlation_id`] to
/// associate the build's metrics with a caller-supplied identifier.
/// Finally, call [`snapshot_builder_build`] to consume the builder and obtain the snapshot. If you
/// need to discard the builder without building, call [`free_snapshot_builder`].
pub struct FfiSnapshotBuilder {
    pub(super) engine: Arc<dyn ExternEngine>,
    pub(super) source: FfiSnapshotBuilderSource,
    version: Option<Version>,
    correlation_id: Option<String>,
    incremental_replay: IncrementalReplay,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
    pub(super) snapshot_hint: Option<Box<SnapshotHint>>,
}

/// An opaque handle with exclusive (Box-like) ownership of a [`FfiSnapshotBuilder`].
#[handle_descriptor(target=FfiSnapshotBuilder, mutable=true, sized=true)]
pub struct ExclusiveSnapshotBuilder;

/// Controls whether a snapshot build advances a stale CRC by replaying later commits.
///
/// A CRC at the target version is used regardless of this setting. See [`IncrementalReplay`] for
/// the effect of each mode. The default is [`FfiIncrementalReplay::Disabled`].
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiIncrementalReplay {
    /// Do not advance a stale CRC.
    Disabled,
    /// Advance only when the CRC is at most this many commits behind the target version.
    UpToCommits(u64),
    /// Advance a stale CRC regardless of its distance from the target version.
    Unlimited,
}

impl From<FfiIncrementalReplay> for IncrementalReplay {
    fn from(mode: FfiIncrementalReplay) -> Self {
        match mode {
            FfiIncrementalReplay::Disabled => Self::Disabled,
            FfiIncrementalReplay::UpToCommits(commits) => Self::UpToCommits(commits),
            FfiIncrementalReplay::Unlimited => Self::Unlimited,
        }
    }
}

/// Get a builder for creating a [`SharedSnapshot`] from a table path.
///
/// Use [`snapshot_builder_with_version`] to pin a specific version, then call
/// [`snapshot_builder_build`] to obtain the snapshot. The caller owns the returned handle and must
/// eventually call either [`snapshot_builder_build`] to produce a [`SharedSnapshot`], or
/// [`free_snapshot_builder`] to drop it without building.
///
/// # Safety
///
/// Caller is responsible for passing a valid path and engine handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_snapshot_builder(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let engine_ref = unsafe { engine.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let source = match url {
        Ok(url) => FfiSnapshotBuilderSource::TableRoot(url),
        Err(e) => {
            let result: KernelResult<Handle<ExclusiveSnapshotBuilder>> = Err(e);
            return unsafe { result.into_extern_result(&engine_ref) };
        }
    };
    let result = make_snapshot_builder(source, engine_arc);
    unsafe { result.into_extern_result(&engine_ref) }
}

/// Get a builder for incrementally updating an existing snapshot.
///
/// This avoids re-reading the full log. Use [`snapshot_builder_with_version`] to target a specific
/// version, then call [`snapshot_builder_build`] to obtain the updated snapshot. The caller owns
/// the returned handle and must eventually call either [`snapshot_builder_build`] to produce a
/// [`SharedSnapshot`], or [`free_snapshot_builder`] to drop it without building.
///
/// # Safety
///
/// Caller is responsible for passing valid handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_snapshot_builder_from(
    prev_snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let engine_ref = unsafe { engine.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let snapshot_arc = unsafe { prev_snapshot.clone_as_arc() };
    let result = make_snapshot_builder(
        FfiSnapshotBuilderSource::ExistingSnapshot(snapshot_arc),
        engine_arc,
    );
    unsafe { result.into_extern_result(&engine_ref) }
}

/// Sets an explicit target version on a snapshot builder. When omitted, a hinted build uses the
/// hinted version. An ordinary build uses `max_catalog_version` when configured, otherwise it
/// discovers the latest version using the filesystem and any tail supplied through
/// [`snapshot_builder_with_log_tail`].
/// A hint and explicit target must match or build returns `InvalidSnapshotHint`.
///
/// An explicit target does not require a catalog bound solely because that version is
/// catalog-managed. An explicit target must not exceed a supplied catalog bound.
///
/// # Safety
///
/// Caller must pass a valid builder handle. The input handle is consumed; the returned handle owns
/// the builder with the target version applied.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_version(
    builder: Handle<ExclusiveSnapshotBuilder>,
    version: Version,
) -> Handle<ExclusiveSnapshotBuilder> {
    let mut builder = unsafe { builder.into_inner() };
    builder.version = Some(version);
    builder.into()
}

/// Attach a caller-supplied identifier to this snapshot build's metric events.
///
/// Copies the identifier before returning. Repeated calls replace the previous value; an empty
/// value clears it. Invalid UTF-8 or a null pointer with nonzero length returns an error.
///
/// # Safety
///
/// `builder` must be a valid handle. It is consumed on both success and error and must not be
/// used or freed afterward. The caller owns the returned builder on success. `correlation_id`
/// is borrowed for this call. If its pointer is non-null, it must address `len` readable bytes
/// until the call returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_correlation_id(
    builder: Handle<ExclusiveSnapshotBuilder>,
    correlation_id: KernelStringSlice,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let mut builder = unsafe { builder.into_inner() };
    let engine = builder.engine.clone();
    let result = unsafe { correlation_id.try_to_string() }.map(|id| {
        builder.correlation_id = Some(id).filter(|id| !id.is_empty());
        builder.into()
    });
    unsafe { result.into_extern_result(&engine.as_ref()) }
}

/// Set the replay mode for advancing a stale CRC during a fresh or incremental snapshot build.
/// By default, stale CRC replay is disabled. A resolved CRC provides file
/// statistics, domain metadata, and set transactions to subsequent operations.
///
/// Setting a non-disabled mode (Disabled or UpToCommits(0)) is mutually exclusive with providing a
/// snapshot hint, and [`snapshot_builder_build`] returns an error if that occurs.
///
/// # Safety
///
/// Caller must pass a valid builder handle and mode tag. The input handle is consumed; the
/// returned handle owns the builder with the replay mode applied.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_incremental_crc_replay(
    builder: Handle<ExclusiveSnapshotBuilder>,
    mode: FfiIncrementalReplay,
) -> Handle<ExclusiveSnapshotBuilder> {
    let mut builder = unsafe { builder.into_inner() };
    builder.incremental_replay = mode.into();
    builder.into()
}

/// Set the catalog-supplied log tail on a snapshot builder.
///
/// A nonempty tail must be an ascending, contiguous sequence of commits without duplicates.
/// For replay, it replaces filesystem commit files from its first version onward, including
/// filesystem commits newer than the tail.
///
/// Staged commits supplied in the tail must have been ratified by the managing catalog and require
/// [`snapshot_builder_with_max_catalog_version`] even with an explicit target. See that function
/// for constraints on the last tail version when a bound is supplied.
///
/// # Errors
///
/// Returns an error if `log_tail` does not satisfy the [`crate::FfiSlice`] contract or contains an
/// invalid log path.
///
/// # Safety
///
/// Caller must pass a valid builder handle. The builder is consumed unconditionally and must not
/// be used or freed after this call. On error, the builder is dropped. The `log_tail` array and its
/// contents must follow the [`crate::FfiSlice`] contract and remain valid for the duration of this
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_log_tail(
    builder: Handle<ExclusiveSnapshotBuilder>,
    log_tail: log_path::LogPathArray,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let mut builder = unsafe { builder.into_inner() };
    let engine_arc = builder.engine.clone();
    let engine_ref = engine_arc.as_ref();
    let result = unsafe { snapshot_builder_with_log_tail_impl(&mut builder, log_tail) }
        .map(|_| builder.into());
    unsafe { result.into_extern_result(&engine_ref) }
}

/// Bound the snapshot version to what the catalog has ratified.
///
/// Without an explicit target, this selects the catalog version even if newer filesystem commits
/// exist. An explicit target must not exceed the bound. These rules apply whether or not the
/// resulting snapshot is catalog-managed.
///
/// Supply the catalog's bound when loading its latest ratified version, including a downgrade.
/// An empty catalog tail does not remove this requirement. A latest version request that returns
/// a catalog-managed snapshot without a bound fails with `MaxCatalogVersionError`.
///
/// With a bound and no explicit target, the last log-tail entry, if any, must match the bound.
/// With an explicit target, the last entry, if any, must be at least the target version.
/// See [`snapshot_builder_with_log_tail`] for staged-commit input requirements.
///
/// # Safety
///
/// Caller must pass a valid builder handle. The input handle is consumed; the returned handle owns
/// the builder with the maximum catalog version applied.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_max_catalog_version(
    builder: Handle<ExclusiveSnapshotBuilder>,
    max_catalog_version: Version,
) -> Handle<ExclusiveSnapshotBuilder> {
    let mut builder = unsafe { builder.into_inner() };
    builder.max_catalog_version = Some(max_catalog_version);
    builder.into()
}

/// Consume the builder and return a snapshot. After calling, the builder pointer is _no longer
/// valid_. The builder is always freed by this call, whether or not it succeeds.
///
/// Failures that [`SnapshotBuilder::build`] reports as Delta errors go to the
/// `allocate_delta_error` callback; all other failures go to `allocate_kernel_error`. For example,
/// a latest-version build of a table root without a Delta log reports `DELTA_PATH_DOES_NOT_EXIST`,
/// while the same build with [`snapshot_builder_with_version`] reports `EmptyLogError`. A local
/// table path that does not exist fails earlier, when [`get_snapshot_builder`] or engine
/// construction parses it, with `InvalidTableLocationError`.
///
/// [`SnapshotBuilder::build`]: delta_kernel::snapshot::SnapshotBuilder::build
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_build(
    mut builder: Handle<ExclusiveSnapshotBuilder>,
) -> ExternResult<Handle<SharedSnapshot>> {
    // Clone the engine Arc before consuming the handle so we can still use it for error reporting
    let engine_arc = unsafe { builder.as_mut() }.engine.clone();
    let engine_ref = engine_arc.as_ref();
    let builder_box = unsafe { builder.into_inner() };
    let result = snapshot_builder_build_impl(*builder_box);
    unsafe { result.into_extern_result(&engine_ref) }
}

/// Free a snapshot builder without building a snapshot (e.g. on an error path).
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_snapshot_builder(builder: Handle<ExclusiveSnapshotBuilder>) {
    unsafe { builder.drop_handle() }
}

pub(super) enum FfiSnapshotBuilderSource {
    TableRoot(Url),
    ExistingSnapshot(SnapshotRef),
}

fn make_snapshot_builder(
    source: FfiSnapshotBuilderSource,
    engine: Arc<dyn ExternEngine>,
) -> KernelResult<Handle<ExclusiveSnapshotBuilder>> {
    Ok(Box::new(FfiSnapshotBuilder {
        engine,
        source,
        version: None,
        correlation_id: None,
        incremental_replay: IncrementalReplay::Disabled,
        log_tail: Vec::new(),
        max_catalog_version: None,
        snapshot_hint: None,
    })
    .into())
}

unsafe fn snapshot_builder_with_log_tail_impl(
    builder: &mut FfiSnapshotBuilder,
    log_tail: log_path::LogPathArray,
) -> KernelResult<()> {
    builder.log_tail = unsafe { log_tail.log_paths() }?;
    Ok(())
}

fn snapshot_builder_build_impl(
    builder: FfiSnapshotBuilder,
) -> delta_kernel::Result<Handle<SharedSnapshot>> {
    let FfiSnapshotBuilder {
        engine,
        source,
        version,
        correlation_id,
        incremental_replay,
        log_tail,
        max_catalog_version,
        snapshot_hint,
    } = builder;
    let engine = engine.engine();

    struct BuildOptions {
        version: Option<Version>,
        correlation_id: Option<String>,
        incremental_replay: IncrementalReplay,
        log_tail: Vec<LogPath>,
        max_catalog_version: Option<Version>,
        snapshot_hint: Option<Box<SnapshotHint>>,
    }

    fn build<Mode>(
        mut builder: delta_kernel::snapshot::SnapshotBuilder<Mode>,
        engine: &dyn Engine,
        options: BuildOptions,
        apply_snapshot_hint: impl FnOnce(
            delta_kernel::snapshot::SnapshotBuilder<Mode>,
            Box<SnapshotHint>,
        )
            -> KernelResult<delta_kernel::snapshot::SnapshotBuilder<Mode>>,
    ) -> delta_kernel::Result<SnapshotRef> {
        let BuildOptions {
            version,
            correlation_id,
            incremental_replay,
            log_tail,
            max_catalog_version,
            snapshot_hint,
        } = options;
        builder = builder.with_incremental_crc_replay(incremental_replay);
        if let Some(correlation_id) = correlation_id {
            builder = builder.with_correlation_id(correlation_id);
        }
        if let Some(version) = version {
            builder = builder.at_version(version);
        }
        if !log_tail.is_empty() {
            builder = builder.with_log_tail(log_tail);
        }
        if let Some(max_catalog_version) = max_catalog_version {
            builder = builder.with_max_catalog_version(max_catalog_version);
        }
        if let Some(snapshot_hint) = snapshot_hint {
            builder = apply_snapshot_hint(builder, snapshot_hint)?;
        }
        builder.build(engine)
    }

    let options = BuildOptions {
        version,
        correlation_id,
        incremental_replay,
        log_tail,
        max_catalog_version,
        snapshot_hint,
    };
    let snapshot = match source {
        FfiSnapshotBuilderSource::TableRoot(url) => build(
            Snapshot::builder_for(url),
            engine.as_ref(),
            options,
            |builder, hint| Ok(builder.with_snapshot_hint(hint)),
        ),
        FfiSnapshotBuilderSource::ExistingSnapshot(snapshot) => build(
            Snapshot::builder_from(snapshot),
            engine.as_ref(),
            options,
            |_, _| {
                // The public API rejects this combination; retain the invariant here for
                // internal construction paths.
                Err(hint::invalid(
                    "A snapshot hint cannot be used with Snapshot::builder_from",
                ))
            },
        ),
    }?;
    Ok(snapshot.into())
}

#[cfg(test)]
mod tests {
    use delta_kernel::metrics::MetricEvent;
    use rstest::rstest;
    use test_utils::{CapturingReporter, TestAction, actions_to_string};

    use super::*;
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{
        assert_extern_result_error_with_message, error_only_engine_handle,
        install_thread_local_metrics_reporter, ok_or_panic, recover_error, setup_snapshot,
    };
    use crate::{free_engine, free_snapshot, kernel_string_slice};

    #[test]
    fn correlation_id_setter_copies_replaces_and_clears_input() {
        let engine = error_only_engine_handle();
        let table_root = "memory:///table/";
        let builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        let mut id = String::from("request-1");
        let builder = unsafe {
            ok_or_panic(snapshot_builder_with_correlation_id(
                builder,
                kernel_string_slice!(id),
            ))
        };
        id.clear();
        assert_eq!(
            unsafe { builder.as_ref() }.correlation_id.as_deref(),
            Some("request-1")
        );
        id.push_str("request-2");
        let builder = unsafe {
            ok_or_panic(snapshot_builder_with_correlation_id(
                builder,
                kernel_string_slice!(id),
            ))
        };
        drop(id);
        assert_eq!(
            unsafe { builder.as_ref() }.correlation_id.as_deref(),
            Some("request-2")
        );
        let builder = unsafe {
            ok_or_panic(snapshot_builder_with_correlation_id(
                builder,
                KernelStringSlice {
                    ptr: std::ptr::null(),
                    len: 0,
                },
            ))
        };
        assert!(unsafe { builder.as_ref() }.correlation_id.is_none());
        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[rstest]
    fn correlation_id_setter_consumes_builder_on_error(#[values(false, true)] null_pointer: bool) {
        let engine = error_only_engine_handle();
        let table_root = "memory:///table/";
        let engine_owner = unsafe { engine.clone_as_arc() };
        let baseline = Arc::strong_count(&engine_owner);
        let builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        assert_eq!(Arc::strong_count(&engine_owner), baseline + 1);
        let invalid_utf8 = [0xffu8];
        let id = KernelStringSlice {
            ptr: if null_pointer {
                std::ptr::null()
            } else {
                invalid_utf8.as_ptr().cast()
            },
            len: 1,
        };
        let result = unsafe { snapshot_builder_with_correlation_id(builder, id) };
        let expected = if null_pointer {
            FFIKernelError::GenericError
        } else {
            FFIKernelError::Utf8Error
        };
        assert_extern_result_error_with_message(result, expected, None);
        assert_eq!(Arc::strong_count(&engine_owner), baseline);
        unsafe { free_engine(engine) };
    }

    #[rstest]
    #[tokio::test]
    async fn snapshot_builder_correlation_id_reaches_metrics(
        #[values(false, true)] incremental: bool,
        #[values(false, true)] fail: bool,
    ) {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let table_root = "memory:///";
        let builder = unsafe {
            ok_or_panic(if incremental {
                get_snapshot_builder_from(snapshot.shallow_copy(), engine.shallow_copy())
            } else {
                get_snapshot_builder(kernel_string_slice!(table_root), engine.shallow_copy())
            })
        };
        let id = String::from("snapshot-request");
        let builder = unsafe {
            ok_or_panic(snapshot_builder_with_correlation_id(
                builder,
                kernel_string_slice!(id),
            ))
        };
        drop(id);
        let builder = if fail {
            unsafe { snapshot_builder_with_version(builder, 99) }
        } else {
            builder
        };
        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        match unsafe { snapshot_builder_build(builder) } {
            ExternResult::Ok(snapshot) => {
                unsafe { free_snapshot(snapshot) };
                assert!(!fail);
            }
            ExternResult::Err(error) => {
                unsafe { recover_error(error) };
                assert!(fail);
            }
        }
        let mut completions = 0;
        for event in reporter.events() {
            let correlation_id = match event {
                MetricEvent::SnapshotBuildSuccess(event) => {
                    completions += 1;
                    assert!(!fail);
                    event.correlation_id
                }
                MetricEvent::SnapshotBuildFailure(event) => {
                    completions += 1;
                    assert!(fail);
                    event.correlation_id
                }
                MetricEvent::LogSegmentLoadSuccess(event) => event.correlation_id,
                MetricEvent::LogSegmentLoadFailure(event) => event.correlation_id,
                MetricEvent::ProtocolMetadataLoadSuccess(event) => event.correlation_id,
                MetricEvent::ProtocolMetadataLoadFailure(event) => event.correlation_id,
                _ => continue,
            };
            assert_eq!(correlation_id.as_deref(), Some("snapshot-request"));
        }
        assert_eq!(completions, 1);
        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }
}
