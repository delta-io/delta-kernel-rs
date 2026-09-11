//! Typed FFI construction of connector-provided snapshot hints.

use delta_kernel::snapshot::{SnapshotHint, SnapshotHintError, SnapshotHintFreshness};
use delta_kernel::{DeltaResult, Error, Version};

use crate::delta_types::{FfiCrc, FfiLastCheckpoint, FfiMetadata, FfiProtocol};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::LogPathArray;
use crate::{FfiSnapshotBuilder, FfiSnapshotBuilderSource, MutableFfiSnapshotBuilder};

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

/// Complete borrowed representation of a connector-provided snapshot hint.
///
/// Every pointer reachable from this value is borrowed only for the duration of
/// [`snapshot_builder_set_snapshot_hint`]. The setter copies the input into owned kernel values.
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
}

fn invalid_with_source(message: impl Into<String>, source: Error) -> Error {
    SnapshotHintError::Connector {
        message: message.into(),
        source: Some(Box::new(source)),
    }
    .into()
}

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    SnapshotHintError::Connector {
        message: message.into(),
        source: None,
    }
    .into()
}

fn invalid_crc(source: Error) -> Error {
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

fn report(builder: &FfiSnapshotBuilder, result: DeltaResult<bool>) -> ExternResult<bool> {
    unsafe { result.into_extern_result(&builder.engine.as_ref()) }
}

unsafe fn snapshot_builder_set_snapshot_hint_impl(
    builder: &mut FfiSnapshotBuilder,
    value: &FfiSnapshotHint,
) -> DeltaResult<bool> {
    if matches!(
        &builder.source,
        FfiSnapshotBuilderSource::ExistingSnapshot(_)
    ) {
        return Err(Error::unsupported(
            "snapshot hints cannot be set on builders created by get_snapshot_builder_from",
        ));
    }
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
        value.version,
        log_paths,
        protocol,
        metadata,
        last_checkpoint_hint,
        crc,
        freshness,
    )?;
    builder.snapshot_hint = Some(Box::new(snapshot_hint));
    Ok(true)
}

/// Copies and installs a complete typed snapshot hint on a snapshot builder.
///
/// The input is converted and validated before replacing any previously installed hint. Build
/// performs the remaining structural and table-configuration validation. Kernel does not verify
/// that supplied log locations belong to the builder's table; the caller must ensure every log
/// path addresses that table. `Latest` makes `is_built_as_latest()` true, and kernel trusts that
/// caller claim. `Unverified` makes it false.
///
/// # Errors
///
/// Returns `UnsupportedError` when the builder was created by
/// [`get_snapshot_builder_from`](crate::get_snapshot_builder_from). Returns
/// `InvalidSnapshotHint` when a supplied field cannot be decoded or a log path names an unsupported
/// log compaction file.
/// Structural log-segment and table-configuration errors are returned when the builder is built.
/// A failed call leaves the builder unchanged.
///
/// # Safety
///
/// The builder is borrowed and remains caller-owned. Every enum must have a valid tag. Every
/// selected pointer must be aligned and address initialized storage for its declared element count,
/// and all such storage must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_set_snapshot_hint(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    value: &FfiSnapshotHint,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result = unsafe { snapshot_builder_set_snapshot_hint_impl(builder, value) };
    report(builder, result)
}

#[cfg(test)]
mod tests;
