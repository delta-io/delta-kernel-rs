//! Construction and export of snapshot hints.

use delta_kernel::snapshot::{
    SnapshotHint, SnapshotHintError, SnapshotHintFreshness, SnapshotState,
};
use delta_kernel::{KernelError, KernelResult, Version};

use crate::delta_types::{
    FfiCrc, FfiLastCheckpoint, FfiMetadata, FfiProtocol, FfiPublicationWatermark,
};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::LogPathArray;
use crate::{
    ExclusiveSnapshotBuilder, FfiSnapshotBuilder, FfiSnapshotBuilderSource, NullableCvoid,
    SharedExternEngine, SharedSnapshot,
};

mod export;
mod state;

use state::BorrowedSnapshotState;

mod core;
pub use core::*;

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
#[no_mangle]
pub unsafe extern "C" fn snapshot_to_snapshot_hint(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: SnapshotHintVisitor,
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    snapshot
        .to_snapshot_hint()
        .and_then(|hint| export::visit(&hint, context, visitor))
        .map(|()| true)
        .into_extern_result(&engine)
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
    /// Optional CRC state. Null means absent.
    pub crc: *const FfiCrc,
    /// Whether to infer publication from paths or preserve an explicit observation.
    ///
    /// Export always uses an explicit variant, never `InferFromLogPaths`.
    pub publication_watermark: FfiPublicationWatermark,
}

/// Borrowed snapshot components needed to validate and plan a default scan.
///
/// CRC state is omitted because scan planning never reads it. Every pointer is borrowed only for
/// the FFI call receiving this value.
#[repr(C)]
pub struct FfiSnapshotScanState {
    /// Optional ordered, repeatable log-path source. When present, `log_paths` must be empty.
    /// Its batches are borrowed until the next read or the end of this FFI call.
    pub log_path_source: *const crate::log_path::FfiLogPathSource,
    /// Target table version described by the scan state.
    pub version: Version,
    /// Freshness claim for `version`.
    pub freshness: FfiSnapshotHintFreshness,
    /// Complete set of log paths needed to construct the log segment.
    pub log_paths: LogPathArray,
    /// Protocol action at `version`.
    pub protocol: FfiProtocol,
    /// Metadata action at `version`.
    pub metadata: FfiMetadata,
    /// Optional `_last_checkpoint` state. Null means absent.
    pub last_checkpoint: *const FfiLastCheckpoint,
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
    schema: Option<String>,
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
    let metadata = match schema {
        Some(schema) => unsafe { value.metadata.try_to_kernel_with_schema(schema) },
        None => unsafe { value.metadata.try_to_kernel() },
    }
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
    unsafe { snapshot_builder_with_snapshot_hint_impl(&mut builder, value, None) }
        .map(|_| builder.into())
        .into_extern_result(&engine.as_ref())
}

unsafe fn report(builder: &FfiSnapshotBuilder, result: KernelResult<bool>) -> ExternResult<bool> {
    unsafe { result.into_extern_result(&builder.engine.as_ref()) }
}

/// Copies and installs a complete typed snapshot hint while preserving builder ownership.
///
/// This compatibility entry point is used by connectors that keep an exclusive builder handle in
/// a mutable foreign-language slot. A failed call leaves the builder unchanged.
///
/// # Safety
///
/// The builder and hint storage are borrowed for this call and must be valid. Every selected
/// pointer in `value` must address initialized storage for its declared element count.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_set_snapshot_hint(
    builder: &mut Handle<ExclusiveSnapshotBuilder>,
    value: &FfiSnapshotHint,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result =
        unsafe { snapshot_builder_with_snapshot_hint_impl(builder, value, None) }.map(|()| true);
    unsafe { report(builder, result) }
}

/// Install a snapshot hint using transferred schema storage instead of an inline schema string.
/// Validation and builder replacement follow [`snapshot_builder_set_snapshot_hint`].
///
/// # Safety
/// Consumes `upload` unconditionally. The builder and hint storage are borrowed as documented by
/// [`snapshot_builder_set_snapshot_hint`]. The inline schema must be empty. No pinned Java array
/// may remain acquired because errors may invoke the connector.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_set_snapshot_hint_with_schema(
    builder: &mut Handle<ExclusiveSnapshotBuilder>,
    value: &FfiSnapshotHint,
    upload: Handle<ExclusiveSnapshotSchemaUpload>,
) -> ExternResult<bool> {
    let upload = unsafe { upload.into_inner() };
    let builder = unsafe { builder.as_mut() };
    let result = (|| {
        if value.metadata.schema_string.len != 0 {
            return Err(invalid("Schema supplied both inline and as an upload"));
        }
        let schema = upload.finish()?;
        unsafe { snapshot_builder_with_snapshot_hint_impl(builder, value, Some(schema)) }
            .map(|()| true)
    })();
    unsafe { report(builder, result) }
}

pub(super) fn validate_handoff(
    owned: &delta_kernel::Snapshot,
    host: &dyn SnapshotState,
) -> KernelResult<bool> {
    if owned.version() != host.version() || owned.is_built_as_latest() != host.is_latest() {
        return Err(invalid(
            "host version or freshness differs from the native snapshot",
        ));
    }
    if owned.table_root() != host.table_root() {
        return Err(invalid("host table root differs from the native snapshot"));
    }
    if !owned.matches_state(host)? {
        return Err(invalid("host state differs from the native snapshot"));
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
