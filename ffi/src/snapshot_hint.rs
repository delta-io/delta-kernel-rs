//! Typed FFI construction of connector-provided snapshot hints.

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::crc::{Crc, DomainMetadataState, FileStats, SetTransactionState};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::snapshot::{SnapshotHint, SnapshotHintError, SnapshotHintFreshness};
use delta_kernel::{DeltaResult, Error, Version};

use crate::delta_types::{
    checkpoint_metadata, domain_metadata, file_stats, metadata, optional_array, optional_i64,
    optional_string, optional_string_map, optional_value, protocol, set_transaction, sidecar,
    string, FfiCheckpointMetadata, FfiDomainMetadata, FfiDomainMetadataArray, FfiFileSizeHistogram,
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
pub struct FfiSnapshotHintV2Checkpoint {
    /// Checkpoint file name.
    pub path: KernelStringSlice,
    /// Optional checkpoint file size.
    pub size_in_bytes: OptionalValue<i64>,
    /// Optional checkpoint file modification time.
    pub modification_time: OptionalValue<i64>,
    /// Whether sidecar information is present.
    pub has_sidecar_files: bool,
    /// Sidecars. Ignored when `has_sidecar_files` is false.
    pub sidecar_files: FfiSidecarArray,
    /// Whether non-file actions are present.
    pub has_non_file_actions: bool,
    /// Non-file actions. Ignored when `has_non_file_actions` is false.
    pub non_file_actions: FfiSnapshotHintV2ActionArray,
}

/// Typed `_last_checkpoint` fields.
#[repr(C)]
pub struct FfiSnapshotHintLastCheckpoint {
    /// Checkpoint version.
    pub version: Version,
    /// Number of actions in the checkpoint.
    pub size: i64,
    /// Optional number of checkpoint parts.
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
    pub v2_checkpoint: *const FfiSnapshotHintV2Checkpoint,
}

/// Typed CRC fields for a snapshot hint.
#[repr(C)]
pub struct FfiSnapshotHintCrc {
    /// Total table size in bytes.
    pub table_size_bytes: i64,
    /// Number of active files.
    pub num_files: i64,
    /// In-commit timestamp, required when the supplied table state enables in-commit timestamps.
    pub in_commit_timestamp: OptionalValue<i64>,
    /// Optional file-size histogram. Its arrays must have equal lengths of at least two; bin
    /// boundaries must start at zero and increase strictly; counts and byte totals must be
    /// non-negative and sum to `num_files` and `table_size_bytes`, respectively.
    pub file_size_histogram: *const FfiFileSizeHistogram,
    /// Whether the transaction list is present and complete.
    pub has_set_transactions: bool,
    /// Set transactions. Ignored when `has_set_transactions` is false.
    pub set_transactions: FfiSetTransactionArray,
    /// Whether the domain-metadata list is present and complete.
    pub has_domain_metadata: bool,
    /// Domain metadata. Ignored when `has_domain_metadata` is false.
    pub domain_metadata: FfiDomainMetadataArray,
}

// The FFI CRC input borrows nested arrays, so retain an owned copy of its CRC-specific fields.
// The final CRC takes version, metadata, and protocol from the parent state at finish, keeping
// setters order-independent and repeated setters last-write-wins.
struct PendingCrc {
    file_stats: FileStats,
    in_commit_timestamp: Option<i64>,
    set_transaction_state: SetTransactionState,
    domain_metadata_state: DomainMetadataState,
}

impl PendingCrc {
    fn finish(self, version: Version, metadata: Metadata, protocol: Protocol) -> Crc {
        Crc::new_complete(
            version,
            metadata,
            protocol,
            self.file_stats,
            self.in_commit_timestamp,
            self.set_transaction_state,
            self.domain_metadata_state,
        )
    }
}

pub(crate) struct SnapshotHintVisitorState {
    version: Version,
    freshness: SnapshotHintFreshness,
    log_paths: Option<Vec<delta_kernel::LogPath>>,
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    last_checkpoint_hint: Option<LastCheckpointHint>,
    crc: Option<PendingCrc>,
}

pub(crate) enum FfiSnapshotHintState {
    None,
    Building(Box<SnapshotHintVisitorState>),
    Ready(Box<SnapshotHint>),
}

pub(super) fn invalid(message: impl Into<String>) -> Error {
    SnapshotHintError::Connector {
        message: message.into(),
        source: None,
    }
    .into()
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
        usize::try_from(*value)
            .map_err(|_| invalid(format!("checkpoint part count overflows usize: {value}")))
    })
}

unsafe fn required_ref<'a, T>(ptr: *const T, name: &str) -> DeltaResult<&'a T> {
    unsafe { ptr.as_ref() }.ok_or_else(|| invalid(format!("snapshot hint {name} value is null")))
}

unsafe fn v2_action(value: &FfiSnapshotHintV2Action) -> DeltaResult<HintAction> {
    Ok(match value.kind {
        SNAPSHOT_HINT_V2_ACTION_METADATA => HintAction::Metadata(unsafe {
            metadata(
                required_ref(value.value.metadata, "metadata action")?,
                invalid,
            )?
        }),
        SNAPSHOT_HINT_V2_ACTION_PROTOCOL => HintAction::Protocol(unsafe {
            protocol(
                required_ref(value.value.protocol, "protocol action")?,
                invalid,
            )?
        }),
        SNAPSHOT_HINT_V2_ACTION_TRANSACTION => HintAction::Txn(unsafe {
            set_transaction(required_ref(value.value.transaction, "transaction action")?)?
        }),
        SNAPSHOT_HINT_V2_ACTION_DOMAIN_METADATA => HintAction::DomainMetadata(unsafe {
            domain_metadata(required_ref(
                value.value.domain_metadata,
                "domain-metadata action",
            )?)?
        }),
        SNAPSHOT_HINT_V2_ACTION_CHECKPOINT_METADATA => HintAction::CheckpointMetadata(unsafe {
            checkpoint_metadata(
                required_ref(
                    value.value.checkpoint_metadata,
                    "checkpoint-metadata action",
                )?,
                invalid,
            )?
        }),
        kind => {
            return Err(invalid(format!(
                "unknown snapshot hint V2 action kind: {kind}"
            )))
        }
    })
}

unsafe fn v2_checkpoint(value: &FfiSnapshotHintV2Checkpoint) -> DeltaResult<LastCheckpointV2> {
    let parse_sidecar = |value: &FfiSidecar| unsafe { sidecar(value, invalid) };
    let sidecar_files = unsafe {
        optional_array(
            value.has_sidecar_files,
            value.sidecar_files.ptr,
            value.sidecar_files.len,
            "sidecar array",
            invalid,
            parse_sidecar,
        )
    }?;
    let parse_action = |value: &FfiSnapshotHintV2Action| unsafe { v2_action(value) };
    let non_file_actions = unsafe {
        optional_array(
            value.has_non_file_actions,
            value.non_file_actions.ptr,
            value.non_file_actions.len,
            "non-file action array",
            invalid,
            parse_action,
        )
    }?;
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
    let checkpoint_schema = unsafe { optional_string(&value.checkpoint_schema) }?;
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
        unsafe { optional_string(&value.checksum) }?,
        unsafe { optional_string_map(&value.tags, invalid) }?,
        v2_checkpoint,
    )
}

unsafe fn pending_crc(value: &FfiSnapshotHintCrc) -> DeltaResult<PendingCrc> {
    let parse_set_transaction = |value: &FfiSetTransaction| unsafe { set_transaction(value) };
    let set_transactions = unsafe {
        optional_array(
            value.has_set_transactions,
            value.set_transactions.ptr,
            value.set_transactions.len,
            "set-transaction array",
            invalid,
            parse_set_transaction,
        )
    }?;
    let parse_domain_metadata = |value: &FfiDomainMetadata| unsafe { domain_metadata(value) };
    let domain_metadata = unsafe {
        optional_array(
            value.has_domain_metadata,
            value.domain_metadata.ptr,
            value.domain_metadata.len,
            "domain-metadata array",
            invalid,
            parse_domain_metadata,
        )
    }?;
    Ok(PendingCrc {
        file_stats: unsafe {
            file_stats(
                value.num_files,
                value.table_size_bytes,
                value.file_size_histogram,
                invalid,
            )
        }?,
        in_commit_timestamp: optional_i64(&value.in_commit_timestamp),
        set_transaction_state: set_transactions
            .map(SetTransactionState::try_complete)
            .transpose()?
            .unwrap_or_default(),
        domain_metadata_state: domain_metadata
            .map(DomainMetadataState::try_complete)
            .transpose()?
            .unwrap_or_default(),
    })
}

fn visitor(builder: &mut FfiSnapshotBuilder) -> DeltaResult<&mut SnapshotHintVisitorState> {
    match &mut builder.snapshot_hint {
        FfiSnapshotHintState::Building(visitor) => Ok(visitor),
        FfiSnapshotHintState::Ready(_) => {
            Err(invalid("snapshot hint visitor has already been finished"))
        }
        FfiSnapshotHintState::None => Err(invalid("snapshot hint visitor has not been started")),
    }
}

fn report(builder: &FfiSnapshotBuilder, result: DeltaResult<bool>) -> ExternResult<bool> {
    unsafe { result.into_extern_result(&builder.engine.as_ref()) }
}

/// Begins typed snapshot-hint construction on a snapshot builder.
///
/// Only builders returned by [`crate::get_snapshot_builder`] support snapshot hints. Calling this
/// again discards any existing hint state on the builder, including when the new `freshness` is
/// invalid.
/// `Latest` makes the built snapshot report `is_built_as_latest() == true`; kernel trusts this
/// caller claim. `Unverified` makes it report false.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when the builder was created from an existing snapshot or
/// `freshness` is not a known value.
///
/// # Safety
///
/// `builder` must be a valid, exclusively borrowed snapshot-builder handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_begin(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    version: Version,
    freshness: FfiSnapshotHintFreshness,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    builder.snapshot_hint = FfiSnapshotHintState::None;
    let result = match &builder.source {
        FfiSnapshotBuilderSource::ExistingSnapshot(_) => Err(invalid(
            "snapshot hints require a builder created from a table path",
        )),
        FfiSnapshotBuilderSource::TableRoot(_) => parse_freshness(freshness).map(|freshness| {
            builder.snapshot_hint =
                FfiSnapshotHintState::Building(Box::new(SnapshotHintVisitorState {
                    version,
                    freshness,
                    log_paths: None,
                    protocol: None,
                    metadata: None,
                    last_checkpoint_hint: None,
                    crc: None,
                }));
            true
        }),
    };
    report(builder, result)
}

/// Copies the complete log-path set into an active snapshot-hint visitor.
/// Log compaction paths are copied but rejected when the visitor is finished.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active or a supplied path is invalid.
///
/// # Safety
///
/// The builder and every pointer reachable from `log_paths` must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_set_log_paths(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    log_paths: LogPathArray,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result = (|| {
        let state = visitor(builder)?;
        let paths = unsafe { log_paths.log_paths() }
            .map_err(|source| invalid_with_source("supplied log paths are invalid", source))?;
        state.log_paths = Some(paths);
        Ok(true)
    })();
    report(builder, result)
}

/// Copies typed protocol state into an active snapshot-hint visitor.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active. Invalid protocol fields retain their
/// protocol-specific error code.
///
/// # Safety
///
/// The builder and every pointer reachable from `value` must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_set_protocol(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    value: &FfiProtocol,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result = (|| {
        let state = visitor(builder)?;
        state.protocol = Some(unsafe { protocol(value, invalid) }?);
        Ok(true)
    })();
    report(builder, result)
}

/// Copies typed metadata state into an active snapshot-hint visitor.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active. Invalid UTF-8 inputs retain their
/// source error code; schema and table validation occurs when the builder is built.
///
/// # Safety
///
/// The builder and every pointer reachable from `value` must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_set_metadata(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    value: &FfiMetadata,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result = (|| {
        let state = visitor(builder)?;
        state.metadata = Some(unsafe { metadata(value, invalid) }?);
        Ok(true)
    })();
    report(builder, result)
}

/// Copies typed `_last_checkpoint` state into an active snapshot-hint visitor.
///
/// Omit this call when the snapshot hint has no checkpoint hint.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active. Invalid checkpoint fields retain their
/// source error code.
///
/// # Safety
///
/// The builder and every pointer reachable from `value` must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_set_last_checkpoint(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    value: &FfiSnapshotHintLastCheckpoint,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result = (|| {
        let state = visitor(builder)?;
        state.last_checkpoint_hint = Some(unsafe { last_checkpoint(value) }?);
        Ok(true)
    })();
    report(builder, result)
}

/// Copies typed CRC state into an active snapshot-hint visitor.
///
/// Omit this call when the hint has no CRC. CRC, protocol, and metadata setters may be called in
/// any order.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active or the supplied CRC state is invalid.
///
/// # Safety
///
/// The builder and every pointer reachable from `value` must remain valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_set_crc(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    value: &FfiSnapshotHintCrc,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let result = (|| {
        let state = visitor(builder)?;
        state.crc = Some(unsafe { pending_crc(value) }.map_err(invalid_crc)?);
        Ok(true)
    })();
    report(builder, result)
}

/// Completes typed snapshot-hint construction and installs the hint on the snapshot builder.
///
/// Log paths, protocol, and metadata are required. This function rejects unsupported log paths and
/// groups the supplied files; build performs the remaining log-segment and table-configuration
/// validation without reading them. The caller must ensure every path belongs to this table and
/// that protocol and metadata describe the hinted version. This function consumes the visitor even
/// on error, so callers must call `snapshot_builder_snapshot_hint_begin` before retrying.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active, a required field is absent, or the
/// supplied log paths include a log-compaction file.
///
/// # Safety
///
/// `builder` must be a valid, exclusively borrowed snapshot-builder handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_snapshot_hint_finish(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
) -> ExternResult<bool> {
    let builder = unsafe { builder.as_mut() };
    let state = std::mem::replace(&mut builder.snapshot_hint, FfiSnapshotHintState::None);
    let result = match state {
        FfiSnapshotHintState::Building(state) => Ok(state),
        FfiSnapshotHintState::Ready(hint) => {
            builder.snapshot_hint = FfiSnapshotHintState::Ready(hint);
            Err(invalid("snapshot hint visitor has already been finished"))
        }
        FfiSnapshotHintState::None => Err(invalid("snapshot hint visitor has not been started")),
    }
    .and_then(|state| {
        let log_paths = state
            .log_paths
            .ok_or_else(|| invalid("snapshot hint log paths were not supplied"))?;
        let protocol = state
            .protocol
            .ok_or_else(|| invalid("snapshot hint protocol was not supplied"))?;
        let metadata = state
            .metadata
            .ok_or_else(|| invalid("snapshot hint metadata was not supplied"))?;
        let crc = state.crc.map(|crc| {
            std::sync::Arc::new(crc.finish(state.version, metadata.clone(), protocol.clone()))
        });
        SnapshotHint::try_new(
            state.version,
            log_paths,
            protocol,
            metadata,
            state.last_checkpoint_hint,
            crc,
            state.freshness,
        )
    })
    .map(|hint| {
        builder.snapshot_hint = FfiSnapshotHintState::Ready(Box::new(hint));
        true
    });
    report(builder, result)
}

#[cfg(test)]
mod tests;
