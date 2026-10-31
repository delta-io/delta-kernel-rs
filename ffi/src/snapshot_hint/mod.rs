//! Construction and export of snapshot hints.

use delta_kernel::snapshot::{
    SelectedCheckpointKind, SnapshotHint, SnapshotHintError, SnapshotHintFreshness,
};
use delta_kernel::{KernelError, KernelResult, Version};

use crate::delta_types::{
    FfiCrc, FfiLastCheckpoint, FfiMetadata, FfiProtocol, FfiPublicationWatermark,
};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::LogPathArray;
use crate::snapshot_provenance::FfiCrcProvenance;
use crate::{
    ExclusiveSnapshotBuilder, FfiSnapshotBuilder, FfiSnapshotBuilderSource, NullableCvoid,
    SharedExternEngine, SharedSnapshot,
};

mod export;

/// Exports retained snapshot state through one callback without engine I/O.
///
/// The hint preserves the snapshot's build-time freshness claim; it does not establish that the
/// version is still latest. `context` is forwarded to `visitor`, which receives a complete borrowed
/// [`FfiSnapshotHint`]. Copy every value retained beyond the callback. The engine allocates errors
/// only. Returns `true` after invoking the callback once; errors do not invoke the callback.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` if the retained segment contains unsupported compaction files.
/// Returns an error if checkpoint schema serialization fails. Returns `UnsupportedError` if the
/// CRC contains experimental adaptive-metadata `lastManifestCommit` or Add `backReference` state
/// when built with `adaptive-metadata-in-dev`, which the typed representation cannot preserve.
///
/// # Safety
///
/// The snapshot and engine handles are borrowed and must remain valid throughout the call. The
/// callback must not free or consume either handle, retain any borrowed pointer, or unwind across
/// the FFI boundary. The hint and all nested storage expire when the callback returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_to_snapshot_hint(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: SnapshotHintVisitor,
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = snapshot
        .to_snapshot_hint()
        .and_then(|hint| export::visit(&hint, context, visitor))
        .map(|()| true);
    unsafe { result.into_extern_result(&engine) }
}

/// Receives one complete borrowed hint. All nested storage expires when the callback returns.
pub type SnapshotHintVisitor = extern "C" fn(context: NullableCvoid, hint: *const FfiSnapshotHint);

/// Freshness claim carried by a snapshot hint.
///
/// Connector inputs supply this claim; exports preserve the snapshot's build-time claim.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiSnapshotHintFreshness {
    /// The hinted version was not established as latest.
    Unverified,
    /// The hinted version was established as latest when the claim was made, not necessarily
    /// still latest.
    Latest,
}

/// Complete borrowed representation of a hint supplied by a connector or exported from a snapshot.
///
/// Every pointer reachable from this value is borrowed only for the duration of
/// [`snapshot_builder_with_snapshot_hint`] or a [`SnapshotHintVisitor`] callback.
/// Installation copies the input into owned kernel values.
#[repr(C)]
pub struct FfiSnapshotHint {
    /// Target table version described by the hint.
    pub version: Version,
    /// Freshness claim for `version`.
    pub freshness: FfiSnapshotHintFreshness,
    /// Complete set of log paths needed to construct the snapshot.
    pub log_paths: LogPathArray,
    /// Protocol action at `version`.
    pub protocol: FfiProtocol,
    /// Metadata action at `version`.
    pub metadata: FfiMetadata,
    /// Optional `_last_checkpoint` state. Null means absent.
    pub last_checkpoint: *const FfiLastCheckpoint,
    /// Optional CRC state at `version`. Null means absent or stale.
    /// V2 exports retain stale baselines in [`FfiSnapshotHintV2::base_crc`].
    pub crc: *const FfiCrc,
    /// Whether to infer publication from paths or preserve an explicit observation.
    ///
    /// Export always uses an explicit variant, never `InferFromLogPaths`.
    pub publication_watermark: FfiPublicationWatermark,
}

/// Versioned hint representation preserving snapshot provenance and stale checksum baselines.
/// All nested pointers are borrowed for the import call or export callback only.
#[repr(C)]
pub struct FfiSnapshotHintV2 {
    /// Original hint fields, including only an at-version CRC in `base.crc`.
    pub base: FfiSnapshotHint,
    /// Selected checkpoint specification.
    pub checkpoint_kind: SelectedCheckpointKind,
    /// Retained checksum observation.
    pub crc_provenance: FfiCrcProvenance,
    /// Retained checksum baseline, including a stale CRC. Null means absent.
    /// Imports use this field as the authoritative CRC and ignore `base.crc`.
    pub base_crc: *const FfiCrc,
}

/// Receives one borrowed V2 hint; all nested storage expires on return.
pub type SnapshotHintVisitorV2 =
    extern "C" fn(context: NullableCvoid, hint: *const FfiSnapshotHintV2);

/// Exports a hint with provenance without engine I/O. Invokes `visitor` once on success.
/// Uses `engine` only to allocate errors; errors follow [`snapshot_to_snapshot_hint`].
///
/// # Safety
/// Both handles must remain valid and owned by the caller throughout the call. The callback
/// must not consume either handle, retain borrowed pointers, or unwind across the FFI boundary.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_to_snapshot_hint_v2(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: SnapshotHintVisitorV2,
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = snapshot
        .to_snapshot_hint()
        .and_then(|hint| export::visit_v2(&hint, context, visitor))
        .map(|()| true);
    unsafe { result.into_extern_result(&engine) }
}

/// Copies and installs a V2 hint, preserving provenance without reading storage.
/// Validation and errors follow [`snapshot_builder_with_snapshot_hint`]. Provenance consistency
/// is checked when the snapshot is built. The existing hint layout remains unchanged.
///
/// # Safety
/// The builder is consumed unconditionally. All enum tags and selected nested pointers must
/// be valid, aligned, and initialized for their declared types and lengths throughout the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_snapshot_hint_v2(
    builder: Handle<ExclusiveSnapshotBuilder>,
    value: &FfiSnapshotHintV2,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let mut builder = unsafe { builder.into_inner() };
    let engine = builder.engine.clone();
    let result =
        unsafe { snapshot_builder_with_snapshot_hint_impl(&mut builder, &value.base, Some(value)) }
            .map(|_| builder.into());
    unsafe { result.into_extern_result(&engine.as_ref()) }
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

unsafe fn snapshot_builder_with_snapshot_hint_impl(
    builder: &mut FfiSnapshotBuilder,
    value: &FfiSnapshotHint,
    provenance: Option<&FfiSnapshotHintV2>,
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
    let crc_pointer = provenance.map_or(value.crc, |hint| hint.base_crc);
    let crc = unsafe { crc_pointer.as_ref() }
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
    let snapshot_hint = match provenance {
        Some(hint) => {
            snapshot_hint.with_provenance(hint.checkpoint_kind, hint.crc_provenance.to_kernel())
        }
        None => snapshot_hint,
    };
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_builder_with_snapshot_hint(
    builder: Handle<ExclusiveSnapshotBuilder>,
    value: &FfiSnapshotHint,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let mut builder = unsafe { builder.into_inner() };
    let engine = builder.engine.clone();
    let result = unsafe { snapshot_builder_with_snapshot_hint_impl(&mut builder, value, None) }
        .map(|_| builder.into());
    unsafe { result.into_extern_result(&engine.as_ref()) }
}

#[cfg(test)]
mod tests;
