//! Typed FFI construction of connector-provided snapshot hints.

use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::crc::{
    Crc, DomainMetadataState, ReconstructedCrc, ReconstructedFileStats, SetTransactionState,
};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::snapshot::{SnapshotHint, SnapshotHintError, SnapshotHintFreshness};
use delta_kernel::{DeltaResult, Error, Version};

use crate::delta_types::{
    checkpoint_metadata, domain_metadata, file_size_histogram, metadata, optional_array,
    optional_i64, optional_string, optional_string_map, optional_value, protocol, set_transaction,
    sidecar, string, FfiCheckpointMetadata, FfiDomainMetadata, FfiDomainMetadataArray,
    FfiFileSizeHistogram, FfiMetadata, FfiOptionalI64, FfiOptionalString, FfiOptionalStringMap,
    FfiOptionalU64, FfiProtocol, FfiSetTransaction, FfiSetTransactionArray, FfiSidecar,
    FfiSidecarArray,
};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::LogPathArray;
use crate::{
    FfiSnapshotBuilder, FfiSnapshotBuilderSource, KernelStringSlice, MutableFfiSnapshotBuilder,
};

/// Integer freshness claim attached to a connector-provided snapshot hint.
pub type FfiSnapshotHintFreshness = u32;

/// The connector has not established that the hinted version is latest.
pub const SNAPSHOT_HINT_FRESHNESS_UNVERIFIED: FfiSnapshotHintFreshness = 0;

/// The connector has established that the hinted version is latest.
pub const SNAPSHOT_HINT_FRESHNESS_LATEST: FfiSnapshotHintFreshness = 1;

/// Integer discriminator for a V2 checkpoint non-file action.
pub type FfiSnapshotHintActionKind = u32;

/// Metadata action discriminator.
pub const SNAPSHOT_HINT_ACTION_METADATA: FfiSnapshotHintActionKind = 0;
/// Protocol action discriminator.
pub const SNAPSHOT_HINT_ACTION_PROTOCOL: FfiSnapshotHintActionKind = 1;
/// Set-transaction action discriminator.
pub const SNAPSHOT_HINT_ACTION_TRANSACTION: FfiSnapshotHintActionKind = 2;
/// Domain-metadata action discriminator.
pub const SNAPSHOT_HINT_ACTION_DOMAIN_METADATA: FfiSnapshotHintActionKind = 3;
/// Checkpoint-metadata action discriminator.
pub const SNAPSHOT_HINT_ACTION_CHECKPOINT_METADATA: FfiSnapshotHintActionKind = 4;

/// Typed payload pointer for a V2 checkpoint non-file action.
#[repr(C)]
#[derive(Clone, Copy)]
pub union FfiSnapshotHintActionValue {
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
pub struct FfiSnapshotHintAction {
    /// Type of the object addressed by `value`.
    pub kind: FfiSnapshotHintActionKind,
    /// Corresponding typed payload pointer, valid for the duration of the setter call.
    pub value: FfiSnapshotHintActionValue,
}

/// Borrowed array of typed V2 checkpoint non-file actions.
#[repr(C)]
pub struct FfiSnapshotHintActionArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiSnapshotHintAction,
    /// Number of actions.
    pub len: usize,
}

/// Typed V2 checkpoint fields.
#[repr(C)]
pub struct FfiSnapshotHintV2Checkpoint {
    /// Checkpoint file name.
    pub path: KernelStringSlice,
    /// Optional checkpoint file size.
    pub size_in_bytes: FfiOptionalI64,
    /// Optional checkpoint file modification time.
    pub modification_time: FfiOptionalI64,
    /// Whether sidecar information is present.
    pub has_sidecar_files: bool,
    /// Sidecars. Ignored when `has_sidecar_files` is false.
    pub sidecar_files: FfiSidecarArray,
    /// Whether non-file actions are present.
    pub has_non_file_actions: bool,
    /// Non-file actions. Ignored when `has_non_file_actions` is false.
    pub non_file_actions: FfiSnapshotHintActionArray,
}

/// Typed `_last_checkpoint` fields.
#[repr(C)]
pub struct FfiSnapshotHintLastCheckpoint {
    /// Checkpoint version.
    pub version: Version,
    /// Number of actions in the checkpoint.
    pub size: i64,
    /// Optional number of checkpoint parts.
    pub parts: FfiOptionalU64,
    /// Optional total checkpoint size in bytes.
    pub size_in_bytes: FfiOptionalI64,
    /// Optional number of Add actions.
    pub num_of_add_files: FfiOptionalI64,
    /// Optional canonical checkpoint schema string.
    pub checkpoint_schema: FfiOptionalString,
    /// Optional checkpoint JSON checksum.
    pub checksum: FfiOptionalString,
    /// Optional checkpoint tags.
    pub tags: FfiOptionalStringMap,
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
    pub in_commit_timestamp: FfiOptionalI64,
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

pub(crate) struct SnapshotHintVisitorState {
    version: Version,
    freshness: SnapshotHintFreshness,
    log_paths: Option<Vec<delta_kernel::LogPath>>,
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    last_checkpoint_hint: Option<LastCheckpointHint>,
    crc: Option<Arc<Crc>>,
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

fn optional_usize(value: &FfiOptionalU64) -> DeltaResult<Option<usize>> {
    optional_value(value.has_value, || {
        usize::try_from(value.value).map_err(|_| {
            invalid(format!(
                "checkpoint part count overflows usize: {}",
                value.value
            ))
        })
    })
}

unsafe fn required_ref<'a, T>(ptr: *const T, name: &str) -> DeltaResult<&'a T> {
    unsafe { ptr.as_ref() }.ok_or_else(|| invalid(format!("snapshot hint {name} value is null")))
}

unsafe fn action(value: &FfiSnapshotHintAction) -> DeltaResult<HintAction> {
    Ok(match value.kind {
        SNAPSHOT_HINT_ACTION_METADATA => HintAction::Metadata(unsafe {
            metadata(
                required_ref(value.value.metadata, "metadata action")?,
                invalid,
            )?
        }),
        SNAPSHOT_HINT_ACTION_PROTOCOL => HintAction::Protocol(unsafe {
            protocol(
                required_ref(value.value.protocol, "protocol action")?,
                invalid,
            )?
        }),
        SNAPSHOT_HINT_ACTION_TRANSACTION => HintAction::Txn(unsafe {
            set_transaction(required_ref(value.value.transaction, "transaction action")?)?
        }),
        SNAPSHOT_HINT_ACTION_DOMAIN_METADATA => HintAction::DomainMetadata(unsafe {
            domain_metadata(required_ref(
                value.value.domain_metadata,
                "domain-metadata action",
            )?)?
        }),
        SNAPSHOT_HINT_ACTION_CHECKPOINT_METADATA => HintAction::CheckpointMetadata(unsafe {
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
                "unknown snapshot hint action kind: {kind}"
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
    let parse_action = |value: &FfiSnapshotHintAction| unsafe { action(value) };
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

unsafe fn crc(
    value: &FfiSnapshotHintCrc,
    version: Version,
    metadata: Metadata,
    protocol: Protocol,
) -> DeltaResult<Crc> {
    let file_size_histogram = (!value.file_size_histogram.is_null())
        .then(|| unsafe { file_size_histogram(&*value.file_size_histogram, invalid) })
        .transpose()?;
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
    Crc::try_new_complete(ReconstructedCrc::new(
        version,
        metadata,
        protocol,
        ReconstructedFileStats {
            num_files: value.num_files,
            table_size_bytes: value.table_size_bytes,
            file_size_histogram,
        },
        optional_i64(&value.in_commit_timestamp),
        set_transactions
            .map(SetTransactionState::try_complete)
            .transpose()?
            .unwrap_or_default(),
        domain_metadata
            .map(DomainMetadataState::try_complete)
            .transpose()?
            .unwrap_or_default(),
    ))
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

/// Copies typed protocol state into an active snapshot-hint visitor. Replacing the protocol clears
/// any previously supplied CRC because CRC state is bound to its protocol.
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
        state.crc = None;
        Ok(true)
    })();
    report(builder, result)
}

/// Copies typed metadata state into an active snapshot-hint visitor. Replacing the metadata clears
/// any previously supplied CRC because CRC state is bound to its metadata.
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
        state.crc = None;
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
/// Protocol and metadata must be supplied before this call. Omit this call when the hint has no
/// CRC.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active, protocol or metadata is absent, or the
/// supplied CRC state is invalid.
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
        let metadata = state
            .metadata
            .clone()
            .ok_or_else(|| invalid("snapshot hint metadata must be supplied before CRC"))?;
        let protocol = state
            .protocol
            .clone()
            .ok_or_else(|| invalid("snapshot hint protocol must be supplied before CRC"))?;
        let crc = unsafe { crc(value, state.version, metadata, protocol) }.map_err(invalid_crc)?;
        state.crc = Some(Arc::new(crc));
        Ok(true)
    })();
    report(builder, result)
}

/// Completes typed snapshot-hint construction and installs the hint on the snapshot builder.
///
/// Log paths, protocol, and metadata are required. Build validates structural consistency and
/// table configuration without reading the supplied files. The caller must ensure every path
/// belongs to this table and that protocol and metadata describe the hinted version. This function
/// consumes the visitor even on error, so callers must call `snapshot_builder_snapshot_hint_begin`
/// before retrying.
///
/// # Errors
///
/// Returns `InvalidSnapshotHint` when no visitor is active or a required field is absent.
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
        SnapshotHint::try_new(
            state.version,
            state
                .log_paths
                .ok_or_else(|| invalid("snapshot hint log paths were not supplied"))?,
            state
                .protocol
                .ok_or_else(|| invalid("snapshot hint protocol was not supplied"))?,
            state
                .metadata
                .ok_or_else(|| invalid("snapshot hint metadata was not supplied"))?,
            state.last_checkpoint_hint,
            state.crc,
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
mod tests {
    use std::error::Error as _;
    use std::sync::Arc;

    use delta_kernel::actions::{CheckpointMetadata, Sidecar};
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;

    use super::*;
    use crate::delta_types::{
        optional_strings, strings, FfiI64Array, FfiOptionalStringArray, FfiStringArray,
        FfiStringMap, FfiStringMapEntry,
    };
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_with_message, ok_or_panic,
    };
    use crate::log_path::FfiLogPath;
    use crate::{
        engine_to_handle, free_engine, free_snapshot, free_snapshot_builder, get_snapshot_builder,
        get_snapshot_builder_from, snapshot_builder_build,
    };

    fn slice(value: &'static str) -> KernelStringSlice {
        unsafe { KernelStringSlice::new_unsafe(value) }
    }

    fn none_string() -> FfiOptionalString {
        FfiOptionalString {
            has_value: false,
            value: slice(""),
        }
    }

    fn none_i64() -> FfiOptionalI64 {
        FfiOptionalI64 {
            has_value: false,
            value: 0,
        }
    }

    fn empty_strings(present: bool) -> FfiOptionalStringArray {
        FfiOptionalStringArray {
            has_value: present,
            value: FfiStringArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        }
    }

    fn empty_map() -> FfiStringMap {
        FfiStringMap {
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    fn none_map() -> FfiOptionalStringMap {
        FfiOptionalStringMap {
            has_value: false,
            value: empty_map(),
        }
    }

    fn test_protocol() -> FfiProtocol {
        FfiProtocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: empty_strings(false),
            writer_features: empty_strings(false),
        }
    }

    fn test_metadata() -> FfiMetadata {
        FfiMetadata {
            id: slice("table-id"),
            name: none_string(),
            description: none_string(),
            format_provider: slice("parquet"),
            format_options: empty_map(),
            schema_string: slice(r#"{"type":"struct","fields":[]}"#),
            partition_columns: FfiStringArray {
                ptr: std::ptr::null(),
                len: 0,
            },
            created_time: none_i64(),
            configuration: empty_map(),
        }
    }

    fn empty_crc() -> FfiSnapshotHintCrc {
        FfiSnapshotHintCrc {
            table_size_bytes: 0,
            num_files: 0,
            in_commit_timestamp: none_i64(),
            file_size_histogram: std::ptr::null(),
            has_set_transactions: false,
            set_transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
            has_domain_metadata: false,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        }
    }

    unsafe fn finish_minimal_hint(builder: &mut Handle<MutableFfiSnapshotBuilder>) {
        let log_path = FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
            1,
            1,
        );
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                builder,
                &test_protocol(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                builder,
                &test_metadata(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_finish(builder));
        }
    }

    #[test]
    fn invalid_crc_preserves_source() {
        let error = invalid_crc(Error::internal_error("invalid CRC state"));
        let Error::SnapshotHint(source) = error else {
            panic!("expected SnapshotHint")
        };
        assert!(source
            .source()
            .expect("connector error must preserve its source")
            .to_string()
            .contains("invalid CRC state"));
    }

    #[test]
    fn typed_components_construct_rich_snapshot_state() {
        let protocol = unsafe { protocol(&test_protocol(), invalid) }.unwrap();
        let metadata = unsafe { metadata(&test_metadata(), invalid) }.unwrap();

        let transaction = FfiSetTransaction {
            app_id: slice("app"),
            version: 7,
            last_updated: FfiOptionalI64 {
                has_value: true,
                value: 123,
            },
        };
        let domain = FfiDomainMetadata {
            domain: slice("example.domain"),
            configuration: slice("payload"),
            removed: false,
        };
        let boundaries = [0, 1024];
        let counts = [1, 0];
        let bytes = [512, 0];
        let histogram = FfiFileSizeHistogram {
            sorted_bin_boundaries: FfiI64Array {
                ptr: boundaries.as_ptr(),
                len: boundaries.len(),
            },
            file_counts: FfiI64Array {
                ptr: counts.as_ptr(),
                len: counts.len(),
            },
            total_bytes: FfiI64Array {
                ptr: bytes.as_ptr(),
                len: bytes.len(),
            },
        };
        let crc_value = FfiSnapshotHintCrc {
            table_size_bytes: 512,
            num_files: 1,
            in_commit_timestamp: none_i64(),
            file_size_histogram: &histogram,
            has_set_transactions: true,
            set_transactions: FfiSetTransactionArray {
                ptr: &transaction,
                len: 1,
            },
            has_domain_metadata: true,
            domain_metadata: FfiDomainMetadataArray {
                ptr: &domain,
                len: 1,
            },
        };
        let complete_crc =
            unsafe { crc(&crc_value, 5, metadata.clone(), protocol.clone()) }.unwrap();
        assert_eq!(complete_crc.file_stats().unwrap().num_files(), 1);
        assert_eq!(
            complete_crc.set_transaction_state.expect_complete().len(),
            1
        );
        assert_eq!(
            complete_crc.domain_metadata_state.expect_complete().len(),
            1
        );
        let partial_crc =
            unsafe { crc(&empty_crc(), 5, metadata.clone(), protocol.clone()) }.unwrap();
        assert!(matches!(
            partial_crc.set_transaction_state,
            SetTransactionState::Partial(ref values) if values.is_empty()
        ));
        assert!(matches!(
            partial_crc.domain_metadata_state,
            DomainMetadataState::Partial(ref values) if values.is_empty()
        ));

        let checkpoint_metadata = FfiCheckpointMetadata {
            version: 5,
            tags: none_map(),
        };
        let non_file_action = FfiSnapshotHintAction {
            kind: SNAPSHOT_HINT_ACTION_CHECKPOINT_METADATA,
            value: FfiSnapshotHintActionValue {
                checkpoint_metadata: &checkpoint_metadata,
            },
        };
        let sidecar = FfiSidecar {
            path: slice("sidecar.parquet"),
            size_in_bytes: 42,
            modification_time: 123,
            tags: none_map(),
        };
        let v2 = FfiSnapshotHintV2Checkpoint {
            path: slice("00000000000000000005.checkpoint.uuid.parquet"),
            size_in_bytes: none_i64(),
            modification_time: none_i64(),
            has_sidecar_files: true,
            sidecar_files: FfiSidecarArray {
                ptr: &sidecar,
                len: 1,
            },
            has_non_file_actions: true,
            non_file_actions: FfiSnapshotHintActionArray {
                ptr: &non_file_action,
                len: 1,
            },
        };
        let checkpoint = FfiSnapshotHintLastCheckpoint {
            version: 5,
            size: 1,
            parts: FfiOptionalU64 {
                has_value: false,
                value: 0,
            },
            size_in_bytes: none_i64(),
            num_of_add_files: none_i64(),
            checkpoint_schema: none_string(),
            checksum: none_string(),
            tags: none_map(),
            v2_checkpoint: &v2,
        };
        let checkpoint = unsafe { last_checkpoint(&checkpoint) }.unwrap();
        assert_eq!(checkpoint.version, 5);
    }

    #[test]
    fn typed_arrays_preserve_absent_and_present_empty() {
        assert_eq!(
            unsafe { optional_strings(&empty_strings(false), invalid) }.unwrap(),
            None
        );
        assert_eq!(
            unsafe { optional_strings(&empty_strings(true), invalid) }.unwrap(),
            Some(vec![])
        );
    }

    #[test]
    fn typed_metadata_preserves_non_empty_optional_and_container_fields() {
        let partition = [slice("p")];
        let format_options = [FfiStringMapEntry {
            key: slice("compression"),
            value: slice("zstd"),
        }];
        let configuration = [FfiStringMapEntry {
            key: slice("key"),
            value: slice("value"),
        }];
        let value = FfiMetadata {
            id: slice("table-id"),
            name: FfiOptionalString {
                has_value: true,
                value: slice("table-name"),
            },
            description: FfiOptionalString {
                has_value: true,
                value: slice("description"),
            },
            format_provider: slice("parquet"),
            format_options: FfiStringMap {
                ptr: format_options.as_ptr(),
                len: format_options.len(),
            },
            schema_string: slice(
                r#"{"type":"struct","fields":[{"name":"p","type":"string","nullable":true,"metadata":{}}]}"#,
            ),
            partition_columns: FfiStringArray {
                ptr: partition.as_ptr(),
                len: partition.len(),
            },
            created_time: FfiOptionalI64 {
                has_value: true,
                value: 123,
            },
            configuration: FfiStringMap {
                ptr: configuration.as_ptr(),
                len: configuration.len(),
            },
        };

        let actual = unsafe { metadata(&value, invalid) }.unwrap();
        assert_eq!(actual.name(), Some("table-name"));
        assert_eq!(actual.description(), Some("description"));
        assert_eq!(actual.created_time(), Some(123));
        assert_eq!(actual.partition_columns(), &["p"]);
        assert_eq!(
            actual.configuration().get("key").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn typed_metadata_rejects_duplicate_map_keys() {
        let configuration = [
            FfiStringMapEntry {
                key: slice("key"),
                value: slice("first"),
            },
            FfiStringMapEntry {
                key: slice("key"),
                value: slice("second"),
            },
        ];
        let value = FfiMetadata {
            configuration: FfiStringMap {
                ptr: configuration.as_ptr(),
                len: configuration.len(),
            },
            ..test_metadata()
        };

        let error = unsafe { metadata(&value, invalid) }.unwrap_err();
        assert!(error.to_string().contains("duplicate map key: key"));
    }

    #[test]
    fn typed_array_rejects_null_nonempty_pointer() {
        let invalid = FfiStringArray {
            ptr: std::ptr::null(),
            len: 1,
        };
        assert!(unsafe { strings(&invalid, super::invalid) }.is_err());
    }

    #[test]
    fn typed_actions_validate_tags_and_convert_each_payload() {
        let metadata = test_metadata();
        let protocol = test_protocol();
        let transaction = FfiSetTransaction {
            app_id: slice("app"),
            version: 1,
            last_updated: none_i64(),
        };
        let domain_metadata = FfiDomainMetadata {
            domain: slice("domain"),
            configuration: slice("{}"),
            removed: false,
        };
        let checkpoint_metadata = FfiCheckpointMetadata {
            version: 1,
            tags: none_map(),
        };
        let actions = [
            FfiSnapshotHintAction {
                kind: SNAPSHOT_HINT_ACTION_METADATA,
                value: FfiSnapshotHintActionValue {
                    metadata: &metadata,
                },
            },
            FfiSnapshotHintAction {
                kind: SNAPSHOT_HINT_ACTION_PROTOCOL,
                value: FfiSnapshotHintActionValue {
                    protocol: &protocol,
                },
            },
            FfiSnapshotHintAction {
                kind: SNAPSHOT_HINT_ACTION_TRANSACTION,
                value: FfiSnapshotHintActionValue {
                    transaction: &transaction,
                },
            },
            FfiSnapshotHintAction {
                kind: SNAPSHOT_HINT_ACTION_DOMAIN_METADATA,
                value: FfiSnapshotHintActionValue {
                    domain_metadata: &domain_metadata,
                },
            },
            FfiSnapshotHintAction {
                kind: SNAPSHOT_HINT_ACTION_CHECKPOINT_METADATA,
                value: FfiSnapshotHintActionValue {
                    checkpoint_metadata: &checkpoint_metadata,
                },
            },
        ];
        for value in &actions {
            unsafe { action(value) }.unwrap();
        }

        let invalid_action = FfiSnapshotHintAction {
            kind: u32::MAX,
            value: FfiSnapshotHintActionValue {
                metadata: std::ptr::null(),
            },
        };
        assert!(matches!(
            unsafe { action(&invalid_action) },
            Err(Error::SnapshotHint(_))
        ));
        let null_action = FfiSnapshotHintAction {
            kind: SNAPSHOT_HINT_ACTION_METADATA,
            value: FfiSnapshotHintActionValue {
                metadata: std::ptr::null(),
            },
        };
        assert!(matches!(
            unsafe { action(&null_action) },
            Err(Error::SnapshotHint(_))
        ));
    }

    #[test]
    fn typed_visitor_rejects_unknown_freshness_and_unfinished_build() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        let result = unsafe { snapshot_builder_snapshot_hint_begin(&mut builder, 0, u32::MAX) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: unknown snapshot hint freshness: 4294967295"),
        );
        let result =
            unsafe { snapshot_builder_snapshot_hint_set_metadata(&mut builder, &test_metadata()) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor has not been started"),
        );

        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                5,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
        }
        let result = unsafe { snapshot_builder_snapshot_hint_begin(&mut builder, 6, u32::MAX) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: unknown snapshot hint freshness: 4294967295"),
        );
        let result =
            unsafe { snapshot_builder_snapshot_hint_set_metadata(&mut builder, &test_metadata()) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor has not been started"),
        );

        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
        }
        let result = unsafe { snapshot_builder_build(builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor is unfinished"),
        );
        unsafe { free_engine(engine) };
    }

    #[test]
    fn invalid_begin_clears_finished_snapshot_hint() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe { finish_minimal_hint(&mut builder) };

        let result = unsafe { snapshot_builder_snapshot_hint_begin(&mut builder, 0, u32::MAX) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: unknown snapshot hint freshness: 4294967295"),
        );
        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor has not been started"),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[derive(Clone, Copy)]
    enum InactiveVisitor {
        NotStarted,
        Finished,
    }

    #[derive(Clone, Copy)]
    enum MalformedPayload {
        LogPaths,
        Protocol,
        Metadata,
        LastCheckpoint,
    }

    #[rstest::rstest]
    #[case::not_started_log_paths(InactiveVisitor::NotStarted, MalformedPayload::LogPaths)]
    #[case::not_started_protocol(InactiveVisitor::NotStarted, MalformedPayload::Protocol)]
    #[case::not_started_metadata(InactiveVisitor::NotStarted, MalformedPayload::Metadata)]
    #[case::not_started_checkpoint(InactiveVisitor::NotStarted, MalformedPayload::LastCheckpoint)]
    #[case::finished_log_paths(InactiveVisitor::Finished, MalformedPayload::LogPaths)]
    #[case::finished_protocol(InactiveVisitor::Finished, MalformedPayload::Protocol)]
    #[case::finished_metadata(InactiveVisitor::Finished, MalformedPayload::Metadata)]
    #[case::finished_checkpoint(InactiveVisitor::Finished, MalformedPayload::LastCheckpoint)]
    fn typed_setters_report_lifecycle_before_malformed_payload(
        #[case] lifecycle: InactiveVisitor,
        #[case] payload: MalformedPayload,
    ) {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        if let InactiveVisitor::Finished = lifecycle {
            unsafe { finish_minimal_hint(&mut builder) };
        }

        let invalid_array = FfiStringArray {
            ptr: std::ptr::null(),
            len: 1,
        };
        let protocol = FfiProtocol {
            reader_features: FfiOptionalStringArray {
                has_value: true,
                value: invalid_array,
            },
            ..test_protocol()
        };
        let metadata = FfiMetadata {
            partition_columns: FfiStringArray {
                ptr: std::ptr::null(),
                len: 1,
            },
            ..test_metadata()
        };
        let checkpoint = FfiSnapshotHintLastCheckpoint {
            version: 0,
            size: 1,
            parts: FfiOptionalU64 {
                has_value: false,
                value: 0,
            },
            size_in_bytes: none_i64(),
            num_of_add_files: none_i64(),
            checkpoint_schema: FfiOptionalString {
                has_value: true,
                value: slice("not a schema"),
            },
            checksum: none_string(),
            tags: none_map(),
            v2_checkpoint: std::ptr::null(),
        };
        let result = unsafe {
            match payload {
                MalformedPayload::LogPaths => snapshot_builder_snapshot_hint_set_log_paths(
                    &mut builder,
                    LogPathArray {
                        ptr: std::ptr::null(),
                        len: 1,
                    },
                ),
                MalformedPayload::Protocol => {
                    snapshot_builder_snapshot_hint_set_protocol(&mut builder, &protocol)
                }
                MalformedPayload::Metadata => {
                    snapshot_builder_snapshot_hint_set_metadata(&mut builder, &metadata)
                }
                MalformedPayload::LastCheckpoint => {
                    snapshot_builder_snapshot_hint_set_last_checkpoint(&mut builder, &checkpoint)
                }
            }
        };
        let expected = match lifecycle {
            InactiveVisitor::NotStarted => {
                "Invalid snapshot hint: snapshot hint visitor has not been started"
            }
            InactiveVisitor::Finished => {
                "Invalid snapshot hint: snapshot hint visitor has already been finished"
            }
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some(expected),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[rstest::rstest]
    #[case("not-a-url")]
    #[case("memory:///hinted-table/_delta_log/not-a-log-file")]
    fn typed_visitor_wraps_invalid_log_path_errors(#[case] location: &'static str) {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
        }
        let log_path = FfiLogPath::new(slice(location), 1, 1);
        let result = unsafe {
            snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            )
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: supplied log paths are invalid"),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_visitor_enforces_crc_setter_order() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
        }

        let crc = empty_crc();
        let result = unsafe { snapshot_builder_snapshot_hint_set_crc(&mut builder, &crc) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint metadata must be supplied before CRC"),
        );
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                &mut builder,
                &test_metadata(),
            ));
        }
        let result = unsafe { snapshot_builder_snapshot_hint_set_crc(&mut builder, &crc) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint protocol must be supplied before CRC"),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[derive(Clone, Copy)]
    enum CrcDependency {
        Protocol,
        Metadata,
    }

    #[rstest::rstest]
    #[case::protocol(CrcDependency::Protocol)]
    #[case::metadata(CrcDependency::Metadata)]
    fn typed_visitor_invalidates_crc_when_dependency_changes(#[case] dependency: CrcDependency) {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        let log_path = FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
            1,
            1,
        );
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                &mut builder,
                &test_protocol(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                &mut builder,
                &test_metadata(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_crc(
                &mut builder,
                &empty_crc(),
            ));
            match dependency {
                CrcDependency::Protocol => ok_or_panic(
                    snapshot_builder_snapshot_hint_set_protocol(&mut builder, &test_protocol()),
                ),
                CrcDependency::Metadata => ok_or_panic(
                    snapshot_builder_snapshot_hint_set_metadata(&mut builder, &test_metadata()),
                ),
            };
            ok_or_panic(snapshot_builder_snapshot_hint_finish(&mut builder));
        }

        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
        assert!(unsafe { snapshot.as_ref() }
            .get_file_stats_if_present()
            .is_none());
        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_log_paths_reject_null_nonempty_pointer() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
        }
        let result = unsafe {
            snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: std::ptr::null(),
                    len: 1,
                },
            )
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: supplied log paths are invalid"),
        );
        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_visitor_finish_requires_protocol_and_metadata() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        let log_path = FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
            1,
            1,
        );

        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            ));
        }
        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint protocol was not supplied"),
        );

        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                &mut builder,
                &test_protocol(),
            ));
        }
        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint metadata was not supplied"),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_visitor_rejects_log_compaction_paths() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        let log_path = FfiLogPath::new(
            slice(concat!(
                "memory:///hinted-table/_delta_log/",
                "00000000000000000000.00000000000000000001.compacted.json"
            )),
            1,
            1,
        );
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                1,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                &mut builder,
                &test_protocol(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                &mut builder,
                &test_metadata(),
            ));
        }
        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: log compaction files are not supported"),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_crc_reports_malformed_histogram_as_invalid_snapshot_hint() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                &mut builder,
                &test_protocol(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                &mut builder,
                &test_metadata(),
            ));
        }

        let boundary = [0];
        let histogram = FfiFileSizeHistogram {
            sorted_bin_boundaries: FfiI64Array {
                ptr: boundary.as_ptr(),
                len: boundary.len(),
            },
            file_counts: FfiI64Array {
                ptr: boundary.as_ptr(),
                len: boundary.len(),
            },
            total_bytes: FfiI64Array {
                ptr: boundary.as_ptr(),
                len: boundary.len(),
            },
        };
        let crc = FfiSnapshotHintCrc {
            table_size_bytes: 0,
            num_files: 0,
            in_commit_timestamp: none_i64(),
            file_size_histogram: &histogram,
            has_set_transactions: false,
            set_transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
            has_domain_metadata: false,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        };
        let result = unsafe { snapshot_builder_snapshot_hint_set_crc(&mut builder, &crc) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: supplied CRC is invalid"),
        );

        unsafe {
            free_snapshot_builder(builder);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_visitor_builds_latest_snapshot_without_storage_files() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        let log_path = FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
            1,
            1,
        );
        let last_checkpoint = FfiSnapshotHintLastCheckpoint {
            version: 0,
            size: 1,
            parts: FfiOptionalU64 {
                has_value: false,
                value: 0,
            },
            size_in_bytes: none_i64(),
            num_of_add_files: none_i64(),
            checkpoint_schema: none_string(),
            checksum: none_string(),
            tags: none_map(),
            v2_checkpoint: std::ptr::null(),
        };
        let crc = FfiSnapshotHintCrc {
            table_size_bytes: 0,
            num_files: 0,
            in_commit_timestamp: none_i64(),
            file_size_histogram: std::ptr::null(),
            has_set_transactions: false,
            set_transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
            has_domain_metadata: false,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        };

        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_LATEST,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: &log_path,
                    len: 1,
                },
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                &mut builder,
                &test_protocol(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                &mut builder,
                &test_metadata(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_last_checkpoint(
                &mut builder,
                &last_checkpoint,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_crc(&mut builder, &crc));
            ok_or_panic(snapshot_builder_snapshot_hint_finish(&mut builder));
        }

        let result =
            unsafe { snapshot_builder_snapshot_hint_set_metadata(&mut builder, &test_metadata()) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor has already been finished"),
        );

        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor has already been finished"),
        );

        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
        let snapshot_ref = unsafe { snapshot.as_ref() };
        assert_eq!(snapshot_ref.version(), 0);
        assert!(snapshot_ref.is_built_as_latest());
        assert_eq!(
            snapshot_ref
                .get_file_stats_if_present()
                .unwrap()
                .num_files(),
            0
        );

        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    fn assert_typed_checkpoint_build(
        log_paths: &[FfiLogPath],
        protocol: &FfiProtocol,
        last_checkpoint: &FfiSnapshotHintLastCheckpoint,
        expected_filenames: &[&str],
        expected_hint: &LastCheckpointHint,
    ) {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_log_paths(
                &mut builder,
                LogPathArray {
                    ptr: log_paths.as_ptr(),
                    len: log_paths.len(),
                },
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_protocol(
                &mut builder,
                protocol,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_metadata(
                &mut builder,
                &test_metadata(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_last_checkpoint(
                &mut builder,
                last_checkpoint,
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_set_crc(
                &mut builder,
                &empty_crc(),
            ));
            ok_or_panic(snapshot_builder_snapshot_hint_finish(&mut builder));
        }

        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
        let snapshot_ref = unsafe { snapshot.as_ref() };
        assert_eq!(snapshot_ref.version(), 0);
        assert_eq!(
            snapshot_ref
                .get_file_stats_if_present()
                .unwrap()
                .num_files(),
            0
        );
        let segment = snapshot_ref.log_segment();
        assert_eq!(segment.checkpoint_version, Some(0));
        assert_eq!(
            segment
                .listed
                .checkpoint_parts
                .iter()
                .map(|part| part.filename.as_str())
                .collect::<Vec<_>>(),
            expected_filenames
        );
        assert_eq!(segment.checkpoint_hint(), Some(expected_hint));

        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    fn typed_multipart_checkpoint_build() {
        const PART_1: &str = "00000000000000000000.checkpoint.0000000001.0000000002.parquet";
        const PART_2: &str = "00000000000000000000.checkpoint.0000000002.0000000002.parquet";
        const PART_1_URL: &str = concat!(
            "memory:///hinted-table/_delta_log/",
            "00000000000000000000.checkpoint.0000000001.0000000002.parquet"
        );
        const PART_2_URL: &str = concat!(
            "memory:///hinted-table/_delta_log/",
            "00000000000000000000.checkpoint.0000000002.0000000002.parquet"
        );
        let log_paths = [
            FfiLogPath::new(slice(PART_1_URL), 1, 1),
            FfiLogPath::new(slice(PART_2_URL), 1, 1),
        ];
        let checkpoint = FfiSnapshotHintLastCheckpoint {
            version: 0,
            size: 2,
            parts: FfiOptionalU64 {
                has_value: true,
                value: 2,
            },
            size_in_bytes: none_i64(),
            num_of_add_files: none_i64(),
            checkpoint_schema: none_string(),
            checksum: none_string(),
            tags: none_map(),
            v2_checkpoint: std::ptr::null(),
        };
        let expected =
            LastCheckpointHint::from_parts(0, 2, Some(2), None, None, None, None, None, None)
                .unwrap();
        assert_typed_checkpoint_build(
            &log_paths,
            &test_protocol(),
            &checkpoint,
            &[PART_1, PART_2],
            &expected,
        );
    }

    fn typed_v2_checkpoint_build() {
        const CHECKPOINT: &str =
            "00000000000000000000.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet";
        const CHECKPOINT_URL: &str = concat!(
            "memory:///hinted-table/_delta_log/",
            "00000000000000000000.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"
        );
        let features = [slice("v2Checkpoint")];
        let protocol = FfiProtocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: FfiOptionalStringArray {
                has_value: true,
                value: FfiStringArray {
                    ptr: features.as_ptr(),
                    len: features.len(),
                },
            },
            writer_features: FfiOptionalStringArray {
                has_value: true,
                value: FfiStringArray {
                    ptr: features.as_ptr(),
                    len: features.len(),
                },
            },
        };
        let sidecar = FfiSidecar {
            path: slice("sidecar.parquet"),
            size_in_bytes: 42,
            modification_time: 123,
            tags: none_map(),
        };
        let checkpoint_metadata = FfiCheckpointMetadata {
            version: 0,
            tags: none_map(),
        };
        let action = FfiSnapshotHintAction {
            kind: SNAPSHOT_HINT_ACTION_CHECKPOINT_METADATA,
            value: FfiSnapshotHintActionValue {
                checkpoint_metadata: &checkpoint_metadata,
            },
        };
        let v2 = FfiSnapshotHintV2Checkpoint {
            path: slice(CHECKPOINT),
            size_in_bytes: none_i64(),
            modification_time: none_i64(),
            has_sidecar_files: true,
            sidecar_files: FfiSidecarArray {
                ptr: &sidecar,
                len: 1,
            },
            has_non_file_actions: true,
            non_file_actions: FfiSnapshotHintActionArray {
                ptr: &action,
                len: 1,
            },
        };
        let checkpoint = FfiSnapshotHintLastCheckpoint {
            version: 0,
            size: 2,
            parts: FfiOptionalU64 {
                has_value: false,
                value: 0,
            },
            size_in_bytes: none_i64(),
            num_of_add_files: none_i64(),
            checkpoint_schema: none_string(),
            checksum: none_string(),
            tags: none_map(),
            v2_checkpoint: &v2,
        };
        let expected = LastCheckpointHint::from_parts(
            0,
            2,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(LastCheckpointV2::from_parts(
                CHECKPOINT.to_string(),
                None,
                None,
                Some(vec![Sidecar::new(
                    "sidecar.parquet".to_string(),
                    42,
                    123,
                    None,
                )]),
                Some(vec![HintAction::CheckpointMetadata(
                    CheckpointMetadata::new(0, None),
                )]),
            )),
        )
        .unwrap();
        let log_paths = [FfiLogPath::new(slice(CHECKPOINT_URL), 1, 1)];
        assert_typed_checkpoint_build(&log_paths, &protocol, &checkpoint, &[CHECKPOINT], &expected);
    }

    #[rstest::rstest]
    #[case::multipart_v1(typed_multipart_checkpoint_build)]
    #[case::uuid_v2(typed_v2_checkpoint_build)]
    fn typed_checkpoint_build_preserves_identity_and_reconstructed_state(#[case] run_case: fn()) {
        run_case();
    }

    #[test]
    fn typed_visitor_rejects_begin_on_existing_snapshot_builder() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut initial_builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe { finish_minimal_hint(&mut initial_builder) };
        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(initial_builder)) };

        let mut update_builder = unsafe {
            ok_or_panic(get_snapshot_builder_from(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
            ))
        };
        let result = unsafe {
            snapshot_builder_snapshot_hint_begin(
                &mut update_builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            )
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some(
                "Invalid snapshot hint: snapshot hints require a builder created from a table path",
            ),
        );

        unsafe {
            free_snapshot_builder(update_builder);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[test]
    fn typed_visitor_rejects_missing_fields_and_partial_state_can_be_freed() {
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
            allocate_err,
        );
        let mut missing_fields_builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut missing_fields_builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
        }
        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut missing_fields_builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint log paths were not supplied"),
        );
        let result = unsafe { snapshot_builder_snapshot_hint_finish(&mut missing_fields_builder) };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidSnapshotHint,
            Some("Invalid snapshot hint: snapshot hint visitor has not been started"),
        );

        let mut partial_builder = unsafe {
            ok_or_panic(get_snapshot_builder(
                slice("memory:///hinted-table/"),
                engine.shallow_copy(),
            ))
        };
        unsafe {
            ok_or_panic(snapshot_builder_snapshot_hint_begin(
                &mut partial_builder,
                0,
                SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
            ));
            free_snapshot_builder(missing_fields_builder);
            free_snapshot_builder(partial_builder);
            free_engine(engine);
        }
    }
}
