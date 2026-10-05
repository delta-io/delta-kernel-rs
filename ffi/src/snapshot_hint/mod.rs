//! Construction and export of snapshot hints.

use delta_kernel::snapshot::{
    PublicationWatermark, SnapshotHint, SnapshotHintError, SnapshotHintFreshness,
};
use delta_kernel::{KernelError, KernelResult, Version};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::delta_types::{FfiCrc, FfiLastCheckpoint, FfiMetadata, FfiProtocol};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::LogPathArray;
use crate::{
    ExclusiveSnapshotBuilder, FfiSnapshotBuilder, FfiSnapshotBuilderSource, NullableCvoid,
    SharedExternEngine, SharedSnapshot,
};

mod export;

/// An owned snapshot hint exported by [`snapshot_to_snapshot_hint`].
///
/// Copy its retained state with [`visit_snapshot_hint`], then release it with
/// [`free_snapshot_hint`]. It remains valid after the source snapshot is released.
#[handle_descriptor(target=SnapshotHint, mutable=true, sized=true)]
pub struct ExclusiveSnapshotHint;

/// Exports retained snapshot state without engine I/O.
///
/// The hint preserves the snapshot's build-time freshness claim; it does not establish that the
/// version is still latest. The caller owns the returned hint independently of the snapshot.
/// The engine is used only to allocate errors.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` if the retained segment contains unsupported compaction files.
///
/// # Safety
///
/// The snapshot and engine handles are borrowed and must remain valid for this call. The returned
/// handle must be freed exactly once with [`free_snapshot_hint`].
#[no_mangle]
pub unsafe extern "C" fn snapshot_to_snapshot_hint(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveSnapshotHint>> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    snapshot
        .to_snapshot_hint()
        .map(|hint| Box::new(hint).into())
        .into_extern_result(&engine)
}

/// Releases an exported snapshot hint.
///
/// # Safety
///
/// The handle must be valid and caller-owned. It is consumed and must not be used or freed again.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot_hint(hint: Handle<ExclusiveSnapshotHint>) {
    unsafe { hint.into_inner() };
}

/// Receives one complete borrowed hint. All nested storage expires when the callback returns.
pub type SnapshotHintVisitor = extern "C" fn(context: NullableCvoid, hint: *const FfiSnapshotHint);

/// Visits retained hint state without engine I/O or transferring ownership.
///
/// `context` is forwarded to `visitor`, which is invoked once on success. The engine allocates
/// errors only. Returns `true` after invoking the callback. Callers must copy every value they
/// retain beyond the callback.
///
/// # Errors
///
/// Returns an error if checkpoint schema serialization fails. Returns `UnsupportedError` if the
/// CRC contains experimental adaptive-metadata `lastManifestCommit` or Add `backReference` state,
/// which the typed representation cannot preserve. The native hint remains usable on either result.
///
/// # Safety
///
/// Both handles are borrowed and must remain valid throughout the call. Access to the exclusive
/// hint handle must be serialized, including reads. The callback must not free, consume, or
/// reenter the hint, retain any borrowed pointer, or unwind across the FFI boundary.
#[no_mangle]
pub unsafe extern "C" fn visit_snapshot_hint(
    hint: Handle<ExclusiveSnapshotHint>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: SnapshotHintVisitor,
) -> ExternResult<bool> {
    let hint = unsafe { hint.as_ref() };
    let engine = unsafe { engine.as_ref() };
    export::visit(hint, context, visitor)
        .map(|()| true)
        .into_extern_result(&engine)
}

/// Freshness claim attached to a connector-provided snapshot hint.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiSnapshotHintFreshness {
    /// The connector has not established that the hinted version is latest.
    Unverified,
    /// The connector has established that the hinted version is latest.
    Latest,
}

/// C-layout representation of kernel [`PublicationWatermark`].
///
/// Publication and ratification are separate: a staged commit may belong to the snapshot without
/// having been published to the table's log directory. The snapshot version and freshness claim
/// do not establish how far publication has advanced.
///
/// Connectors that only supply log paths can request inference. Connectors exporting a built
/// snapshot must preserve its observed publication state, which may not be recoverable from the
/// retained paths. [`visit_snapshot_hint`] always reports one of the two explicit variants.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub enum FfiPublicationWatermark {
    /// Derive the highest published commit version from the supplied log paths.
    ///
    /// Staged commit paths do not count as published commits. If no published commit path is
    /// supplied, the inferred watermark is absent.
    InferFromLogPaths,
    /// Explicitly record that no published commits were observed.
    ///
    /// This is not a request for inference, even if the supplied paths include published commits.
    NoPublishedCommits,
    /// Preserve the highest observed published commit version.
    ///
    /// The watermark is retained independently of the supplied paths, which may omit published
    /// commits or still name their staged locations after publication.
    PublishedThrough(Version),
}

/// Complete borrowed representation of a connector-provided snapshot hint.
///
/// Every pointer reachable from this value is borrowed only for the duration of
/// [`snapshot_builder_with_snapshot_hint`] or a [`SnapshotHintVisitor`] callback.
/// Installation copies the input into owned kernel values.
#[repr(C)]
pub struct FfiSnapshotHint {
    /// Target table version described by the hint.
    pub version: Version,
    /// Connector-provided freshness claim for `version`.
    pub freshness: FfiSnapshotHintFreshness,
    /// Complete set of log paths needed to construct the snapshot.
    pub log_paths: LogPathArray,
    /// Protocol action at `version`.
    pub protocol: FfiProtocol,
    /// Metadata action at `version`.
    pub metadata: FfiMetadata,
    /// Optional `_last_checkpoint` state. Null means absent.
    pub last_checkpoint: *const FfiLastCheckpoint,
    /// Optional CRC state. Null means absent.
    pub crc: *const FfiCrc,
    /// Whether to infer publication from paths or preserve an explicit observation.
    pub publication_watermark: FfiPublicationWatermark,
}

fn invalid_with_source(message: impl Into<String>, source: KernelError) -> KernelError {
    SnapshotHintError::Connector {
        message: message.into(),
        source: Some(Box::new(source)),
    }
    .into()
}

pub(crate) fn invalid(message: impl Into<String>) -> KernelError {
    SnapshotHintError::Connector {
        message: message.into(),
        source: None,
    }
    .into()
}

fn invalid_crc(source: KernelError) -> KernelError {
    invalid_with_source("supplied CRC is invalid", source)
}

impl From<FfiSnapshotHintFreshness> for SnapshotHintFreshness {
    fn from(value: FfiSnapshotHintFreshness) -> Self {
        match value {
            FfiSnapshotHintFreshness::Unverified => Self::Unverified,
            FfiSnapshotHintFreshness::Latest => Self::Latest,
        }
    }
}

impl From<FfiPublicationWatermark> for PublicationWatermark {
    fn from(value: FfiPublicationWatermark) -> Self {
        match value {
            FfiPublicationWatermark::InferFromLogPaths => Self::InferFromLogPaths,
            FfiPublicationWatermark::NoPublishedCommits => Self::NoPublishedCommits,
            FfiPublicationWatermark::PublishedThrough(version) => Self::PublishedThrough(version),
        }
    }
}

impl From<PublicationWatermark> for FfiPublicationWatermark {
    fn from(value: PublicationWatermark) -> Self {
        match value {
            PublicationWatermark::InferFromLogPaths => Self::InferFromLogPaths,
            PublicationWatermark::NoPublishedCommits => Self::NoPublishedCommits,
            PublicationWatermark::PublishedThrough(version) => Self::PublishedThrough(version),
        }
    }
}

unsafe fn snapshot_builder_with_snapshot_hint_impl(
    builder: &mut FfiSnapshotBuilder,
    value: &FfiSnapshotHint,
) -> KernelResult<()> {
    let FfiSnapshotBuilderSource::TableRoot(table_root) = &builder.source else {
        return Err(KernelError::unsupported(
            "snapshot hints cannot be set on builders created by get_snapshot_builder_from",
        ));
    };
    let freshness = value.freshness.into();
    let log_paths = unsafe { value.log_paths.log_paths() }
        .map_err(|source| invalid_with_source("supplied log paths are invalid", source))?;
    let protocol = unsafe { value.protocol.try_to_kernel() }
        .map_err(|source| invalid_with_source("supplied protocol is invalid", source))?;
    let metadata = unsafe { value.metadata.try_to_kernel() }
        .map_err(|source| invalid_with_source("supplied metadata is invalid", source))?;
    let last_checkpoint_hint = unsafe { value.last_checkpoint.as_ref() }
        .map(|checkpoint| unsafe { checkpoint.try_to_kernel() })
        .transpose()
        .map_err(|source| invalid_with_source("supplied _last_checkpoint is invalid", source))?;
    let crc = unsafe { value.crc.as_ref() }
        .map(|crc_value| unsafe { crc_value.try_to_kernel() })
        .map(|result| result.map_err(invalid_crc))
        .transpose()?
        .map(std::sync::Arc::new);
    let snapshot_hint = SnapshotHint::try_new(
        table_root,
        value.version,
        value.publication_watermark.into(),
        log_paths,
        protocol,
        metadata,
        last_checkpoint_hint,
        crc,
        freshness,
    )?;
    builder.snapshot_hint = Some(Box::new(snapshot_hint));
    Ok(())
}

/// Copies and installs a complete typed snapshot hint, returning the updated builder handle on
/// success.
///
/// The input is converted and validated before replacing any previously installed hint. Every
/// supplied log location must be beneath the builder's table log root, including paths discarded by
/// checkpoint selection. Kernel preserves the locations; the connector must canonicalize them into
/// the same URL form as the table root. Build performs the remaining structural and
/// table-configuration validation. `Latest` makes `is_built_as_latest()` true, and kernel trusts
/// that caller claim. `Unverified` makes it false.
///
/// # Errors
///
/// Returns `UnsupportedError` when the builder was created by
/// [`get_snapshot_builder_from`](crate::get_snapshot_builder_from). Returns
/// `InvalidSnapshotHint` when a supplied field cannot be decoded, a log path is outside the table
/// log root, or a log path names an unsupported log compaction file.
/// Structural log-segment and table-configuration errors are returned when the builder is built.
/// A failed call drops the builder.
///
/// # Safety
///
/// The builder is consumed unconditionally and must not be used or freed after this call. Every
/// enum must have a valid tag. Every selected pointer must be aligned and address initialized
/// storage for its declared element count, and all such storage must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_with_snapshot_hint(
    builder: Handle<ExclusiveSnapshotBuilder>,
    value: &FfiSnapshotHint,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let mut builder = unsafe { builder.into_inner() };
    let engine = builder.engine.clone();
    unsafe { snapshot_builder_with_snapshot_hint_impl(&mut builder, value) }
        .map(|_| builder.into())
        .into_extern_result(&engine.as_ref())
}

#[cfg(test)]
mod tests;
