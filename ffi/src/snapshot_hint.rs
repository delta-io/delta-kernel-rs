//! Typed FFI construction of connector-provided snapshot hints.

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::crc::Crc;
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::snapshot::{SnapshotHint, SnapshotHintError, SnapshotHintFreshness};
use delta_kernel::{DeltaResult, Error, Version};

use crate::delta_types::{
    checkpoint_metadata, domain_metadata, file_stats, invalid, metadata, optional_i64,
    optional_value, protocol, raw_slice, set_transaction, sidecar, string, string_map,
    FfiCheckpointMetadata, FfiDomainMetadata, FfiDomainMetadataArray, FfiFileSizeHistogram,
    FfiMetadata, FfiProtocol, FfiSetTransaction, FfiSetTransactionArray, FfiSidecar,
    FfiSidecarArray, FfiStringMap,
};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::LogPathArray;
use crate::{
    FfiSnapshotBuilder, FfiSnapshotBuilderSource, KernelStringSlice, MutableFfiSnapshotBuilder,
    OptionalValue,
};

/// Integer freshness claim attached to a connector-provided snapshot hint.
pub type FfiSnapshotHintFreshness = u32;

/// The connector has not established that the hinted version is latest.
pub const SNAPSHOT_HINT_FRESHNESS_UNVERIFIED: FfiSnapshotHintFreshness = 0;

/// The connector has established that the hinted version is latest.
pub const SNAPSHOT_HINT_FRESHNESS_LATEST: FfiSnapshotHintFreshness = 1;

/// Integer discriminator for a V2 checkpoint non-file action.
pub type FfiSnapshotHintV2ActionKind = u32;

/// Metadata action discriminator.
pub const SNAPSHOT_HINT_V2_ACTION_METADATA: FfiSnapshotHintV2ActionKind = 0;
/// Protocol action discriminator.
pub const SNAPSHOT_HINT_V2_ACTION_PROTOCOL: FfiSnapshotHintV2ActionKind = 1;
/// Set-transaction action discriminator.
pub const SNAPSHOT_HINT_V2_ACTION_TRANSACTION: FfiSnapshotHintV2ActionKind = 2;
/// Domain-metadata action discriminator.
pub const SNAPSHOT_HINT_V2_ACTION_DOMAIN_METADATA: FfiSnapshotHintV2ActionKind = 3;
/// Checkpoint-metadata action discriminator.
pub const SNAPSHOT_HINT_V2_ACTION_CHECKPOINT_METADATA: FfiSnapshotHintV2ActionKind = 4;

/// Typed payload pointer for a V2 checkpoint non-file action.
#[repr(C)]
#[derive(Clone, Copy)]
pub union FfiSnapshotHintV2ActionValue {
    /// Metadata payload.
    pub metadata: *const FfiMetadata,
    /// Protocol payload.
    pub protocol: *const FfiProtocol,
    /// Set-transaction payload.
    pub transaction: *const FfiSetTransaction,
    /// Domain-metadata payload.
    pub domain_metadata: *const FfiDomainMetadata,
    /// Checkpoint-metadata payload.
    pub checkpoint_metadata: *const FfiCheckpointMetadata,
}

/// One typed V2 checkpoint non-file action.
#[repr(C)]
pub struct FfiSnapshotHintV2Action {
    /// Type of the object addressed by `value`.
    pub kind: FfiSnapshotHintV2ActionKind,
    /// Corresponding typed payload pointer, valid for the duration of the setter call.
    pub value: FfiSnapshotHintV2ActionValue,
}

/// Borrowed array of typed V2 checkpoint non-file actions.
#[repr(C)]
pub struct FfiSnapshotHintV2ActionArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiSnapshotHintV2Action,
    /// Number of actions.
    pub len: usize,
}

/// Typed V2 checkpoint fields.
#[repr(C)]
pub struct FfiSnapshotHintLastCheckpointV2 {
    /// Checkpoint file name.
    pub path: KernelStringSlice,
    /// Optional checkpoint file size.
    pub size_in_bytes: OptionalValue<i64>,
    /// Optional checkpoint file modification time.
    pub modification_time: OptionalValue<i64>,
    /// Optional sidecar information. `Some` may contain an empty array.
    pub sidecar_files: OptionalValue<FfiSidecarArray>,
    /// Optional non-file actions. `Some` may contain an empty array. Embedded protocol and
    /// metadata actions must match the hint's top-level protocol and metadata.
    pub non_file_actions: OptionalValue<FfiSnapshotHintV2ActionArray>,
}

/// Typed `_last_checkpoint` fields.
#[repr(C)]
pub struct FfiSnapshotHintLastCheckpoint {
    /// Checkpoint version.
    pub version: Version,
    /// Number of actions in the checkpoint.
    pub size: i64,
    /// Optional number of checkpoint parts. Present values must fit in `u32` so the accepted range
    /// is consistent across targets.
    pub parts: OptionalValue<u64>,
    /// Optional total checkpoint size in bytes.
    pub size_in_bytes: OptionalValue<i64>,
    /// Optional number of Add actions.
    pub num_of_add_files: OptionalValue<i64>,
    /// Optional canonical checkpoint schema string.
    pub checkpoint_schema: OptionalValue<KernelStringSlice>,
    /// Optional checkpoint JSON checksum.
    pub checksum: OptionalValue<KernelStringSlice>,
    /// Optional checkpoint tags.
    pub tags: OptionalValue<FfiStringMap>,
    /// Optional typed V2 checkpoint information.
    pub v2_checkpoint: *const FfiSnapshotHintLastCheckpointV2,
}

/// Typed CRC fields for a snapshot hint.
#[repr(C)]
pub struct FfiSnapshotHintCrc {
    /// Total table size in bytes.
    pub table_size_bytes: i64,
    /// Number of active files.
    pub num_files: i64,
    /// Optional in-commit timestamp.
    pub in_commit_timestamp: OptionalValue<i64>,
    /// Optional file-size histogram. Its arrays must have equal lengths of at least two; bin
    /// boundaries must start at zero and increase strictly; counts and byte totals must be
    /// non-negative.
    pub file_size_histogram: *const FfiFileSizeHistogram,
    /// Optional complete transaction list. `None` means the list is not known to be complete.
    pub set_transactions: OptionalValue<FfiSetTransactionArray>,
    /// Optional complete domain-metadata list. `None` means the list is not known to be complete.
    pub domain_metadata: OptionalValue<FfiDomainMetadataArray>,
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
    pub last_checkpoint: *const FfiSnapshotHintLastCheckpoint,
    /// Optional CRC state. Null means absent.
    pub crc: *const FfiSnapshotHintCrc,
}

fn invalid_with_source(message: impl Into<String>, source: Error) -> Error {
    SnapshotHintError::Connector {
        message: message.into(),
        source: Some(Box::new(source)),
    }
    .into()
}

fn invalid_crc(source: Error) -> Error {
    invalid_with_source("supplied CRC is invalid", source)
}

fn parse_freshness(value: FfiSnapshotHintFreshness) -> DeltaResult<SnapshotHintFreshness> {
    match value {
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED => Ok(SnapshotHintFreshness::Unverified),
        SNAPSHOT_HINT_FRESHNESS_LATEST => Ok(SnapshotHintFreshness::Latest),
        value => Err(invalid(format!("unknown snapshot hint freshness: {value}"))),
    }
}

fn optional_usize(value: &OptionalValue<u64>) -> DeltaResult<Option<usize>> {
    optional_value(value, |value| {
        let value = u32::try_from(*value)
            .map_err(|_| invalid(format!("checkpoint part count exceeds u32: {value}")))?;
        Ok(value as usize)
    })
}

/// Borrows a required native payload after checking that it is non-null.
///
/// # Safety
///
/// `ptr` must be aligned and address an initialized `T`. The backing storage must remain valid for
/// the lifetime of the returned reference.
unsafe fn required_ref<'a, T>(ptr: *const T, name: &str) -> DeltaResult<&'a T> {
    unsafe { ptr.as_ref() }.ok_or_else(|| invalid(format!("snapshot hint {name} value is null")))
}

unsafe fn required_payload<T, U>(
    ptr: *const T,
    name: &str,
    parse: impl FnOnce(&T) -> DeltaResult<U>,
) -> DeltaResult<U> {
    parse(unsafe { required_ref(ptr, name) }?)
}

unsafe fn v2_action(value: &FfiSnapshotHintV2Action) -> DeltaResult<HintAction> {
    Ok(match value.kind {
        SNAPSHOT_HINT_V2_ACTION_METADATA => HintAction::Metadata(unsafe {
            required_payload(value.value.metadata, "metadata action", |value| {
                metadata(value)
            })?
        }),
        SNAPSHOT_HINT_V2_ACTION_PROTOCOL => HintAction::Protocol(unsafe {
            required_payload(value.value.protocol, "protocol action", |value| {
                protocol(value)
            })?
        }),
        SNAPSHOT_HINT_V2_ACTION_TRANSACTION => HintAction::Txn(unsafe {
            required_payload(value.value.transaction, "transaction action", |value| {
                set_transaction(value)
            })?
        }),
        SNAPSHOT_HINT_V2_ACTION_DOMAIN_METADATA => HintAction::DomainMetadata(unsafe {
            required_payload(
                value.value.domain_metadata,
                "domain-metadata action",
                |value| domain_metadata(value),
            )?
        }),
        SNAPSHOT_HINT_V2_ACTION_CHECKPOINT_METADATA => HintAction::CheckpointMetadata(unsafe {
            required_payload(
                value.value.checkpoint_metadata,
                "checkpoint-metadata action",
                |value| checkpoint_metadata(value),
            )?
        }),
        kind => {
            return Err(invalid(format!(
                "unknown snapshot hint V2 action kind: {kind}"
            )))
        }
    })
}

unsafe fn v2_checkpoint(value: &FfiSnapshotHintLastCheckpointV2) -> DeltaResult<LastCheckpointV2> {
    let parse_sidecar = |value: &FfiSidecar| unsafe { sidecar(value) };
    let sidecar_files = optional_value(&value.sidecar_files, |array| {
        unsafe { raw_slice(array.ptr, array.len, "sidecar array") }?
            .iter()
            .map(parse_sidecar)
            .collect::<DeltaResult<Vec<_>>>()
    })?;
    let parse_action = |value: &FfiSnapshotHintV2Action| unsafe { v2_action(value) };
    let non_file_actions = optional_value(&value.non_file_actions, |array| {
        unsafe { raw_slice(array.ptr, array.len, "non-file action array") }?
            .iter()
            .map(parse_action)
            .collect::<DeltaResult<Vec<_>>>()
    })?;
    Ok(LastCheckpointV2::from_parts(
        unsafe { string(&value.path) }?,
        optional_i64(&value.size_in_bytes),
        optional_i64(&value.modification_time),
        sidecar_files,
        non_file_actions,
    ))
}

unsafe fn last_checkpoint(
    value: &FfiSnapshotHintLastCheckpoint,
) -> DeltaResult<LastCheckpointHint> {
    let checkpoint_schema =
        optional_value(&value.checkpoint_schema, |value| unsafe { string(value) })?;
    let v2_checkpoint = (!value.v2_checkpoint.is_null())
        .then(|| unsafe { v2_checkpoint(&*value.v2_checkpoint) })
        .transpose()?;
    LastCheckpointHint::from_parts(
        value.version,
        value.size,
        optional_usize(&value.parts)?,
        optional_i64(&value.size_in_bytes),
        optional_i64(&value.num_of_add_files),
        checkpoint_schema,
        optional_value(&value.checksum, |value| unsafe { string(value) })?,
        optional_value(&value.tags, |value| unsafe { string_map(value) })?,
        v2_checkpoint,
    )
}

unsafe fn crc(
    value: &FfiSnapshotHintCrc,
    version: Version,
    metadata: Metadata,
    protocol: Protocol,
) -> DeltaResult<Crc> {
    let parse_set_transaction = |value: &FfiSetTransaction| unsafe { set_transaction(value) };
    let set_transactions = optional_value(&value.set_transactions, |array| {
        unsafe { raw_slice(array.ptr, array.len, "set-transaction array") }?
            .iter()
            .map(parse_set_transaction)
            .collect::<DeltaResult<Vec<_>>>()
    })?;
    let parse_domain_metadata = |value: &FfiDomainMetadata| unsafe { domain_metadata(value) };
    let domain_metadata = optional_value(&value.domain_metadata, |array| {
        unsafe { raw_slice(array.ptr, array.len, "domain-metadata array") }?
            .iter()
            .map(parse_domain_metadata)
            .collect::<DeltaResult<Vec<_>>>()
    })?;
    Ok(Crc::new_with_complete_file_stats(
        version,
        metadata,
        protocol,
        unsafe {
            file_stats(
                value.num_files,
                value.table_size_bytes,
                value.file_size_histogram,
            )
        }?,
        optional_i64(&value.in_commit_timestamp),
        set_transactions,
        domain_metadata,
    ))
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
        return Err(invalid(
            "snapshot hints require a builder created from a table path",
        ));
    }
    let freshness = parse_freshness(value.freshness)?;
    let log_paths = unsafe { value.log_paths.log_paths() }
        .map_err(|source| invalid_with_source("supplied log paths are invalid", source))?;
    let protocol = unsafe { protocol(&value.protocol) }?;
    let metadata = unsafe { metadata(&value.metadata) }?;
    let last_checkpoint_hint = unsafe { value.last_checkpoint.as_ref() }
        .map(|checkpoint| unsafe { last_checkpoint(checkpoint) })
        .transpose()?;
    let crc = unsafe { value.crc.as_ref() }
        .map(|crc_value| unsafe {
            crc(crc_value, value.version, metadata.clone(), protocol.clone())
        })
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
/// Returns an error when the builder was created from an existing snapshot or any supplied field
/// is invalid. A failed call leaves the builder unchanged.
///
/// # Safety
///
/// The builder is borrowed and remains caller-owned. Each action `kind` must select its initialized
/// union member. Every selected pointer must be aligned and address initialized storage for its
/// declared element count, and all such storage must remain valid for this call.
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
