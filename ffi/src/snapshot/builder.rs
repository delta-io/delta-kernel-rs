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
    log_path, unwrap_and_parse_path_as_url, ExternEngine, KernelStringSlice, SharedExternEngine,
    SharedSnapshot,
};

/// Opaque builder for constructing a [`SharedSnapshot`].
///
/// Create with [`get_snapshot_builder`] (from a table path) or [`get_snapshot_builder_from`]
/// (incrementally from an existing snapshot). Configure with [`snapshot_builder_with_version`],
/// [`snapshot_builder_with_incremental_crc_replay`], [`snapshot_builder_with_log_tail`], and
/// [`snapshot_builder_with_max_catalog_version`] (for catalog-managed tables). Builders returned
/// by [`get_snapshot_builder`] may instead receive a complete typed snapshot hint with
/// [`hint::snapshot_builder_with_snapshot_hint`].
/// Finally, call [`snapshot_builder_build`] to consume the builder and obtain the snapshot. If you
/// need to discard the builder without building, call [`free_snapshot_builder`].
pub struct FfiSnapshotBuilder {
    pub(super) engine: Arc<dyn ExternEngine>,
    pub(super) source: FfiSnapshotBuilderSource,
    version: Option<Version>,
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
/// hinted version; an ordinary build uses `max_catalog_version` when configured, otherwise the
/// latest listed version. A hint and explicit target must match or build returns
/// `InvalidSnapshotHint`.
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

/// Set the log tail on a snapshot builder for catalog-managed tables.
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

/// Set the max catalog version on a snapshot builder for catalog-managed tables. This bounds the
/// snapshot version to what the catalog has ratified.
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
) -> KernelResult<Handle<SharedSnapshot>> {
    let FfiSnapshotBuilder {
        engine,
        source,
        version,
        incremental_replay,
        log_tail,
        max_catalog_version,
        snapshot_hint,
    } = builder;
    let engine = engine.engine();

    struct BuildOptions {
        version: Option<Version>,
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
    ) -> KernelResult<SnapshotRef> {
        let BuildOptions {
            version,
            incremental_replay,
            log_tail,
            max_catalog_version,
            snapshot_hint,
        } = options;
        builder = builder.with_incremental_crc_replay(incremental_replay);
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
