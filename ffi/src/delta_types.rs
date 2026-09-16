//! Reusable borrowed C FFI representations of Delta state.

use std::collections::HashMap;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::{
    Add, CheckpointMetadata, DomainMetadata, Metadata, Protocol, SetTransaction, Sidecar,
};
use delta_kernel::crc::{
    Crc, DeletedRecordCountsHistogram, DomainMetadataState, FileSizeHistogram, FileStats,
    FileStatsState, SetTransactionState,
};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::{DeltaResult, Error, Version};

use crate::{FfiFileStats, KernelI64Slice, KernelStringSlice, OptionalValue};

/// Borrowed array of UTF-8 strings.
#[repr(C)]
pub struct FfiStringArray {
    /// Pointer to `len` string slices, or null when `len` is zero.
    pub ptr: *const KernelStringSlice,
    /// Number of strings in the array.
    pub len: usize,
}

/// One borrowed UTF-8 map entry.
#[repr(C)]
pub struct FfiStringMapEntry {
    /// Entry key.
    pub key: KernelStringSlice,
    /// Entry value.
    pub value: KernelStringSlice,
}

/// Borrowed array of UTF-8 map entries.
#[repr(C)]
pub struct FfiStringMap {
    /// Pointer to `len` entries, or null when `len` is zero.
    pub ptr: *const FfiStringMapEntry,
    /// Number of entries.
    pub len: usize,
}

/// One borrowed UTF-8 map entry whose value may be null.
#[repr(C)]
pub struct FfiNullableStringMapEntry {
    /// Entry key.
    pub key: KernelStringSlice,
    /// Optional entry value.
    pub value: OptionalValue<KernelStringSlice>,
}

/// Borrowed array of UTF-8 map entries whose values may be null.
#[repr(C)]
pub struct FfiNullableStringMap {
    /// Pointer to `len` entries, or null when `len` is zero.
    pub ptr: *const FfiNullableStringMapEntry,
    /// Number of entries.
    pub len: usize,
}

/// Borrowed Delta protocol state.
#[repr(C)]
pub struct FfiProtocol {
    /// Minimum reader protocol version.
    pub min_reader_version: i32,
    /// Minimum writer protocol version.
    pub min_writer_version: i32,
    /// Optional reader feature list.
    pub reader_features: OptionalValue<FfiStringArray>,
    /// Optional writer feature list.
    pub writer_features: OptionalValue<FfiStringArray>,
}

/// Borrowed Delta metadata state.
#[repr(C)]
pub struct FfiMetadata {
    /// Table identifier.
    pub id: KernelStringSlice,
    /// Optional table name.
    pub name: OptionalValue<KernelStringSlice>,
    /// Optional table description.
    pub description: OptionalValue<KernelStringSlice>,
    /// Data format provider.
    pub format_provider: KernelStringSlice,
    /// Data format options.
    pub format_options: FfiStringMap,
    /// Canonical Delta schema string.
    pub schema_string: KernelStringSlice,
    /// Logical partition column names.
    pub partition_columns: FfiStringArray,
    /// Optional metadata creation time in milliseconds since the Unix epoch.
    pub created_time: OptionalValue<i64>,
    /// Table configuration entries.
    pub configuration: FfiStringMap,
}

/// Borrowed Delta set-transaction action.
#[repr(C)]
pub struct FfiSetTransaction {
    /// Application identifier.
    pub app_id: KernelStringSlice,
    /// Application-specific transaction version.
    pub version: i64,
    /// Optional last-updated time in milliseconds since the Unix epoch.
    pub last_updated: OptionalValue<i64>,
}

/// Borrowed Delta domain-metadata action.
#[repr(C)]
pub struct FfiDomainMetadata {
    /// Domain identifier.
    pub domain: KernelStringSlice,
    /// Domain configuration payload.
    pub configuration: KernelStringSlice,
    /// Whether this action removes the domain.
    pub removed: bool,
}

/// Borrowed Delta checkpoint-metadata action.
#[repr(C)]
pub struct FfiCheckpointMetadata {
    /// Checkpoint version.
    pub version: i64,
    /// Optional action tags.
    pub tags: OptionalValue<FfiStringMap>,
}

/// Borrowed Delta checkpoint sidecar action.
#[repr(C)]
pub struct FfiSidecar {
    /// Sidecar path.
    pub path: KernelStringSlice,
    /// Sidecar size in bytes.
    pub size_in_bytes: i64,
    /// Sidecar modification time in milliseconds since the Unix epoch.
    pub modification_time: i64,
    /// Optional sidecar tags.
    pub tags: OptionalValue<FfiStringMap>,
}

/// Borrowed file-size histogram state.
#[repr(C)]
pub struct FfiFileSizeHistogram {
    /// Sorted lower boundary of every histogram bin.
    pub sorted_bin_boundaries: KernelI64Slice,
    /// File count in every histogram bin.
    pub file_counts: KernelI64Slice,
    /// Total bytes in every histogram bin.
    pub total_bytes: KernelI64Slice,
}

/// Borrowed array of Delta checkpoint sidecar actions.
#[repr(C)]
pub struct FfiSidecarArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiSidecar,
    /// Number of actions.
    pub len: usize,
}

/// Borrowed array of Delta set-transaction actions.
#[repr(C)]
pub struct FfiSetTransactionArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiSetTransaction,
    /// Number of actions.
    pub len: usize,
}

/// Borrowed array of Delta domain-metadata actions.
#[repr(C)]
pub struct FfiDomainMetadataArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiDomainMetadata,
    /// Number of actions.
    pub len: usize,
}

/// One typed checkpoint non-file action.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum FfiCheckpointNonFileAction {
    /// Metadata action.
    Metadata(*const FfiMetadata),
    /// Protocol action.
    Protocol(*const FfiProtocol),
    /// Set-transaction action.
    Transaction(*const FfiSetTransaction),
    /// Domain-metadata action.
    DomainMetadata(*const FfiDomainMetadata),
    /// Checkpoint-metadata action.
    CheckpointMetadata(*const FfiCheckpointMetadata),
}

/// Borrowed array of typed checkpoint non-file actions.
#[repr(C)]
pub struct FfiCheckpointNonFileActionArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiCheckpointNonFileAction,
    /// Number of actions.
    pub len: usize,
}

/// Borrowed fields of the `v2Checkpoint` object in `_last_checkpoint`.
#[repr(C)]
pub struct FfiLastCheckpointV2 {
    /// Checkpoint file name.
    pub path: KernelStringSlice,
    /// Optional checkpoint file size.
    pub size_in_bytes: OptionalValue<i64>,
    /// Optional checkpoint file modification time.
    pub modification_time: OptionalValue<i64>,
    /// Optional sidecar information. `Some` may contain an empty array.
    pub sidecar_files: OptionalValue<FfiSidecarArray>,
    /// Optional non-file actions. `Some` may contain an empty array.
    pub non_file_actions: OptionalValue<FfiCheckpointNonFileActionArray>,
}

/// Borrowed `_last_checkpoint` fields.
#[repr(C)]
pub struct FfiLastCheckpoint {
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
    /// Optional `v2Checkpoint` information.
    pub v2_checkpoint: *const FfiLastCheckpointV2,
}

/// Deletion-vector storage representation.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiDeletionVectorStorageType {
    /// Persisted relative path.
    PersistedRelative,
    /// Inline data.
    Inline,
    /// Persisted absolute path.
    PersistedAbsolute,
}

/// Borrowed Delta deletion-vector descriptor.
#[repr(C)]
pub struct FfiDeletionVectorDescriptor {
    /// Storage representation for the deletion vector.
    pub storage_type: FfiDeletionVectorStorageType,
    /// Encoded relative path, inline data, or absolute path selected by `storage_type`.
    pub path_or_inline_dv: KernelStringSlice,
    /// Optional byte offset in an external deletion-vector file.
    pub offset: OptionalValue<i32>,
    /// Serialized deletion-vector size in bytes.
    pub size_in_bytes: i32,
    /// Number of rows removed by the deletion vector.
    pub cardinality: i64,
}

/// Borrowed Delta Add action.
#[repr(C)]
pub struct FfiAdd {
    /// Data-file path.
    pub path: KernelStringSlice,
    /// Partition values keyed by logical column name.
    pub partition_values: FfiStringMap,
    /// Data-file size in bytes.
    pub size: i64,
    /// Data-file modification time in milliseconds since the Unix epoch.
    pub modification_time: i64,
    /// Whether this action changes table data.
    pub data_change: bool,
    /// Optional JSON-encoded file statistics.
    pub stats: OptionalValue<KernelStringSlice>,
    /// Optional action tags, whose values may be null.
    pub tags: OptionalValue<FfiNullableStringMap>,
    /// Optional deletion-vector descriptor.
    pub deletion_vector: *const FfiDeletionVectorDescriptor,
    /// Optional base row ID.
    pub base_row_id: OptionalValue<i64>,
    /// Optional default row commit version.
    pub default_row_commit_version: OptionalValue<i64>,
    /// Optional clustering implementation name.
    pub clustering_provider: OptionalValue<KernelStringSlice>,
}

/// Borrowed array of Delta Add actions.
#[repr(C)]
pub struct FfiAddArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiAdd,
    /// Number of actions.
    pub len: usize,
}

/// File-statistics completeness.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiFileStatsStateKind {
    /// Complete file statistics.
    Complete,
    /// Indeterminate file statistics.
    Indeterminate,
}

/// Borrowed file-statistics state.
#[repr(C)]
pub struct FfiFileStatsState {
    /// State variant.
    pub kind: FfiFileStatsStateKind,
    /// Complete scalar statistics. Ignored for [`FfiFileStatsStateKind::Indeterminate`].
    pub file_stats: FfiFileStats,
    /// Optional complete file-size histogram. Ignored for
    /// [`FfiFileStatsStateKind::Indeterminate`].
    pub file_size_histogram: *const FfiFileSizeHistogram,
}

/// Set-transaction completeness.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiSetTransactionStateKind {
    /// Complete set-transaction state.
    Complete,
    /// Partial set-transaction state.
    Partial,
}

/// Borrowed set-transaction state.
#[repr(C)]
pub struct FfiSetTransactionState {
    /// State variant.
    pub kind: FfiSetTransactionStateKind,
    /// Known transactions. May be empty for either state variant.
    pub transactions: FfiSetTransactionArray,
}

/// Domain-metadata completeness.
///
/// cbindgen:prefix-with-name=true
#[derive(Clone, Copy)]
#[repr(C)]
pub enum FfiDomainMetadataStateKind {
    /// Complete domain-metadata state.
    Complete,
    /// Partial domain-metadata state.
    Partial,
}

/// Borrowed domain-metadata state.
#[repr(C)]
pub struct FfiDomainMetadataState {
    /// State variant.
    pub kind: FfiDomainMetadataStateKind,
    /// Known domain metadata. May be empty for either state variant.
    pub domain_metadata: FfiDomainMetadataArray,
}

/// Borrowed deleted-record-count histogram.
#[repr(C)]
pub struct FfiDeletedRecordCountsHistogram {
    /// File counts for the histogram bins.
    pub deleted_record_counts: KernelI64Slice,
}

/// Borrowed in-memory CRC state.
#[repr(C)]
pub struct FfiCrc {
    /// Table version described by the CRC.
    pub version: Version,
    /// Table metadata at `version`.
    pub metadata: FfiMetadata,
    /// Table protocol at `version`.
    pub protocol: FfiProtocol,
    /// File-statistics completeness and payload.
    pub file_stats_state: FfiFileStatsState,
    /// Optional in-commit timestamp.
    pub in_commit_timestamp: OptionalValue<i64>,
    /// Set-transaction state.
    pub set_transaction_state: FfiSetTransactionState,
    /// Domain-metadata state.
    pub domain_metadata_state: FfiDomainMetadataState,
    /// Optional transaction identifier.
    pub txn_id: OptionalValue<KernelStringSlice>,
    /// Optional complete collection of live Add actions.
    pub all_files: OptionalValue<FfiAddArray>,
    /// Optional number of records deleted through deletion vectors.
    pub num_deleted_records: OptionalValue<i64>,
    /// Optional number of active deletion vectors.
    pub num_deletion_vectors: OptionalValue<i64>,
    /// Optional deleted-record-count histogram.
    pub deleted_record_counts_histogram: *const FfiDeletedRecordCountsHistogram,
}

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::generic(message.into())
}

/// Borrows a native array after validating its nullable layout.
///
/// # Safety
///
/// For nonzero `len`, `ptr` must be aligned and address `len` initialized values. The backing
/// storage must remain valid for the lifetime of the returned slice.
pub(crate) unsafe fn raw_slice<'a, O: ?Sized, T>(
    _owner: &'a O,
    ptr: *const T,
    len: usize,
    name: &str,
) -> DeltaResult<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(invalid(format!("{name} pointer is null with length {len}")));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Borrows a required native payload, with its lifetime bounded by `owner`.
///
/// # Safety
///
/// `ptr` must be aligned and address an initialized `T` that remains valid while `owner` is
/// borrowed.
unsafe fn required_ref<'a, O: ?Sized, T>(
    _owner: &'a O,
    ptr: *const T,
    name: &str,
) -> DeltaResult<&'a T> {
    unsafe { ptr.as_ref() }.ok_or_else(|| invalid(format!("{name} value is null")))
}

impl FfiStringArray {
    pub(crate) unsafe fn try_to_strings(&self) -> DeltaResult<Vec<String>> {
        unsafe { raw_slice(self, self.ptr, self.len, "string array") }?
            .iter()
            .map(|value| unsafe { value.try_to_string() })
            .collect()
    }
}

impl FfiStringMap {
    pub(crate) unsafe fn try_to_hash_map(&self) -> DeltaResult<HashMap<String, String>> {
        let entries = unsafe { raw_slice(self, self.ptr, self.len, "string map") }?;
        let mut result = HashMap::with_capacity(entries.len());
        for entry in entries {
            let key = unsafe { entry.key.try_to_string() }?;
            let value = unsafe { entry.value.try_to_string() }?;
            if result.insert(key.clone(), value).is_some() {
                return Err(invalid(format!("duplicate map key: {key}")));
            }
        }
        Ok(result)
    }
}

impl FfiNullableStringMap {
    pub(crate) unsafe fn try_to_hash_map(&self) -> DeltaResult<HashMap<String, Option<String>>> {
        let entries = unsafe { raw_slice(self, self.ptr, self.len, "nullable string map") }?;
        let mut result = HashMap::with_capacity(entries.len());
        for entry in entries {
            let key = unsafe { entry.key.try_to_string() }?;
            let value = Option::<&KernelStringSlice>::from(&entry.value)
                .map(|value| unsafe { value.try_to_string() })
                .transpose()?;
            if result.insert(key.clone(), value).is_some() {
                return Err(invalid(format!("duplicate map key: {key}")));
            }
        }
        Ok(result)
    }
}

impl FfiProtocol {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<Protocol> {
        let reader_features = Option::<&FfiStringArray>::from(&self.reader_features)
            .map(|value| unsafe { value.try_to_strings() })
            .transpose()?;
        let writer_features = Option::<&FfiStringArray>::from(&self.writer_features)
            .map(|value| unsafe { value.try_to_strings() })
            .transpose()?;
        Protocol::try_new(
            self.min_reader_version,
            self.min_writer_version,
            reader_features,
            writer_features,
        )
    }
}

impl FfiMetadata {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<Metadata> {
        let name = Option::<&KernelStringSlice>::from(&self.name)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        let description = Option::<&KernelStringSlice>::from(&self.description)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        Ok(Metadata::from_parts(
            unsafe { self.id.try_to_string() }?,
            name,
            description,
            unsafe { self.format_provider.try_to_string() }?,
            unsafe { self.format_options.try_to_hash_map() }?,
            unsafe { self.schema_string.try_to_string() }?,
            unsafe { self.partition_columns.try_to_strings() }?,
            Option::<&i64>::from(&self.created_time).copied(),
            unsafe { self.configuration.try_to_hash_map() }?,
        ))
    }
}

impl FfiSetTransaction {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<SetTransaction> {
        Ok(SetTransaction::new(
            unsafe { self.app_id.try_to_string() }?,
            self.version,
            Option::<&i64>::from(&self.last_updated).copied(),
        ))
    }
}

impl FfiDomainMetadata {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<DomainMetadata> {
        let domain = unsafe { self.domain.try_to_string() }?;
        let configuration = unsafe { self.configuration.try_to_string() }?;
        Ok(if self.removed {
            DomainMetadata::remove(domain, configuration)
        } else {
            DomainMetadata::new(domain, configuration)
        })
    }
}

impl FfiCheckpointMetadata {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<CheckpointMetadata> {
        let tags = Option::<&FfiStringMap>::from(&self.tags)
            .map(|value| unsafe { value.try_to_hash_map() })
            .transpose()?;
        Ok(CheckpointMetadata::new(self.version, tags))
    }
}

impl FfiSidecar {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<Sidecar> {
        if self.size_in_bytes < 0 {
            return Err(invalid(format!(
                "sidecar size must be non-negative: {}",
                self.size_in_bytes
            )));
        }
        let tags = Option::<&FfiStringMap>::from(&self.tags)
            .map(|value| unsafe { value.try_to_hash_map() })
            .transpose()?;
        Ok(Sidecar::new(
            unsafe { self.path.try_to_string() }?,
            self.size_in_bytes,
            self.modification_time,
            tags,
        ))
    }
}

impl FfiFileSizeHistogram {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<FileSizeHistogram> {
        let (boundaries_ptr, boundaries_len) = self.sorted_bin_boundaries.as_raw_parts();
        let (counts_ptr, counts_len) = self.file_counts.as_raw_parts();
        let (bytes_ptr, bytes_len) = self.total_bytes.as_raw_parts();
        FileSizeHistogram::try_new(
            unsafe { raw_slice(self, boundaries_ptr, boundaries_len, "integer array") }?.to_vec(),
            unsafe { raw_slice(self, counts_ptr, counts_len, "integer array") }?.to_vec(),
            unsafe { raw_slice(self, bytes_ptr, bytes_len, "integer array") }?.to_vec(),
        )
    }
}

impl FfiSidecarArray {
    unsafe fn try_to_kernel(&self) -> DeltaResult<Vec<Sidecar>> {
        unsafe { raw_slice(self, self.ptr, self.len, "sidecar array") }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiCheckpointNonFileAction {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<HintAction> {
        Ok(match self {
            Self::Metadata(value) => HintAction::Metadata(unsafe {
                required_ref(self, *value, "metadata action")?.try_to_kernel()?
            }),
            Self::Protocol(value) => HintAction::Protocol(unsafe {
                required_ref(self, *value, "protocol action")?.try_to_kernel()?
            }),
            Self::Transaction(value) => HintAction::Txn(unsafe {
                required_ref(self, *value, "transaction action")?.try_to_kernel()?
            }),
            Self::DomainMetadata(value) => HintAction::DomainMetadata(unsafe {
                required_ref(self, *value, "domain-metadata action")?.try_to_kernel()?
            }),
            Self::CheckpointMetadata(value) => HintAction::CheckpointMetadata(unsafe {
                required_ref(self, *value, "checkpoint-metadata action")?.try_to_kernel()?
            }),
        })
    }
}

impl FfiCheckpointNonFileActionArray {
    unsafe fn try_to_kernel(&self) -> DeltaResult<Vec<HintAction>> {
        unsafe { raw_slice(self, self.ptr, self.len, "non-file action array") }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiLastCheckpointV2 {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<LastCheckpointV2> {
        let sidecar_files = Option::<&FfiSidecarArray>::from(&self.sidecar_files)
            .map(|value| unsafe { value.try_to_kernel() })
            .transpose()?;
        let non_file_actions =
            Option::<&FfiCheckpointNonFileActionArray>::from(&self.non_file_actions)
                .map(|value| unsafe { value.try_to_kernel() })
                .transpose()?;
        Ok(LastCheckpointV2::from_parts(
            unsafe { self.path.try_to_string() }?,
            Option::<&i64>::from(&self.size_in_bytes).copied(),
            Option::<&i64>::from(&self.modification_time).copied(),
            sidecar_files,
            non_file_actions,
        ))
    }
}

impl FfiLastCheckpoint {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<LastCheckpointHint> {
        let parts = Option::<&u64>::from(&self.parts)
            .map(|value| {
                let value = u32::try_from(*value)
                    .map_err(|_| invalid(format!("checkpoint part count exceeds u32: {value}")))?;
                Ok::<usize, Error>(value as usize)
            })
            .transpose()?;
        let checkpoint_schema = Option::<&KernelStringSlice>::from(&self.checkpoint_schema)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        let checksum = Option::<&KernelStringSlice>::from(&self.checksum)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        let tags = Option::<&FfiStringMap>::from(&self.tags)
            .map(|value| unsafe { value.try_to_hash_map() })
            .transpose()?;
        let v2_checkpoint = unsafe { self.v2_checkpoint.as_ref() }
            .map(|value| unsafe { value.try_to_kernel() })
            .transpose()?;
        LastCheckpointHint::from_parts(
            self.version,
            self.size,
            parts,
            Option::<&i64>::from(&self.size_in_bytes).copied(),
            Option::<&i64>::from(&self.num_of_add_files).copied(),
            checkpoint_schema,
            checksum,
            tags,
            v2_checkpoint,
        )
    }
}

impl From<FfiDeletionVectorStorageType> for DeletionVectorStorageType {
    fn from(value: FfiDeletionVectorStorageType) -> Self {
        match value {
            FfiDeletionVectorStorageType::PersistedRelative => Self::PersistedRelative,
            FfiDeletionVectorStorageType::Inline => Self::Inline,
            FfiDeletionVectorStorageType::PersistedAbsolute => Self::PersistedAbsolute,
        }
    }
}

impl FfiDeletionVectorDescriptor {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<DeletionVectorDescriptor> {
        DeletionVectorDescriptor::try_new(
            self.storage_type.into(),
            unsafe { self.path_or_inline_dv.try_to_string() }?,
            Option::<&i32>::from(&self.offset).copied(),
            self.size_in_bytes,
            self.cardinality,
        )
    }
}

impl FfiAdd {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<Add> {
        let deletion_vector = unsafe { self.deletion_vector.as_ref() }
            .map(|value| unsafe { value.try_to_kernel() })
            .transpose()?;
        let stats = Option::<&KernelStringSlice>::from(&self.stats)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        let tags = Option::<&FfiNullableStringMap>::from(&self.tags)
            .map(|value| unsafe { value.try_to_hash_map() })
            .transpose()?;
        let clustering_provider = Option::<&KernelStringSlice>::from(&self.clustering_provider)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        Ok(Add::from_parts(
            unsafe { self.path.try_to_string() }?,
            unsafe { self.partition_values.try_to_hash_map() }?,
            self.size,
            self.modification_time,
            self.data_change,
            stats,
            tags,
            deletion_vector,
            Option::<&i64>::from(&self.base_row_id).copied(),
            Option::<&i64>::from(&self.default_row_commit_version).copied(),
            clustering_provider,
        ))
    }
}

impl FfiAddArray {
    unsafe fn try_to_kernel(&self) -> DeltaResult<Vec<Add>> {
        unsafe { raw_slice(self, self.ptr, self.len, "Add action array") }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiFileStatsState {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<FileStatsState> {
        match self.kind {
            FfiFileStatsStateKind::Complete => Ok(FileStatsState::Complete(unsafe {
                let histogram = self
                    .file_size_histogram
                    .as_ref()
                    .map(|value| value.try_to_kernel())
                    .transpose()?;
                FileStats::try_new(
                    self.file_stats.num_files,
                    self.file_stats.table_size_bytes,
                    histogram,
                )
            }?)),
            FfiFileStatsStateKind::Indeterminate => Ok(FileStatsState::Indeterminate),
        }
    }
}

impl FfiSetTransactionArray {
    unsafe fn try_to_hash_map(&self) -> DeltaResult<HashMap<String, SetTransaction>> {
        let values = unsafe { raw_slice(self, self.ptr, self.len, "set-transaction array") }?;
        let mut result = HashMap::with_capacity(values.len());
        for value in values {
            let key = unsafe { value.app_id.try_to_string() }?;
            result.insert(key, unsafe { value.try_to_kernel() }?);
        }
        Ok(result)
    }
}

impl FfiSetTransactionState {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<SetTransactionState> {
        let transactions = unsafe { self.transactions.try_to_hash_map() }?;
        match self.kind {
            FfiSetTransactionStateKind::Complete => Ok(SetTransactionState::Complete(transactions)),
            FfiSetTransactionStateKind::Partial => Ok(SetTransactionState::Partial(transactions)),
        }
    }
}

impl FfiDomainMetadataArray {
    unsafe fn try_to_hash_map(&self) -> DeltaResult<HashMap<String, DomainMetadata>> {
        Ok(
            unsafe { raw_slice(self, self.ptr, self.len, "domain-metadata array") }?
                .iter()
                .map(|value| unsafe { value.try_to_kernel() })
                .collect::<DeltaResult<Vec<_>>>()?
                .into_iter()
                .map(|value| (value.domain().to_string(), value))
                .collect(),
        )
    }
}

impl FfiDomainMetadataState {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<DomainMetadataState> {
        let domain_metadata = unsafe { self.domain_metadata.try_to_hash_map() }?;
        match self.kind {
            FfiDomainMetadataStateKind::Complete => {
                Ok(DomainMetadataState::Complete(domain_metadata))
            }
            FfiDomainMetadataStateKind::Partial => {
                Ok(DomainMetadataState::Partial(domain_metadata))
            }
        }
    }
}

impl FfiDeletedRecordCountsHistogram {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<DeletedRecordCountsHistogram> {
        let (ptr, len) = self.deleted_record_counts.as_raw_parts();
        Ok(DeletedRecordCountsHistogram::from_parts(
            unsafe { raw_slice(self, ptr, len, "deleted-record-count histogram") }?.to_vec(),
        ))
    }
}

impl FfiCrc {
    pub(crate) unsafe fn try_to_kernel(&self) -> DeltaResult<Crc> {
        let txn_id = Option::<&KernelStringSlice>::from(&self.txn_id)
            .map(|value| unsafe { value.try_to_string() })
            .transpose()?;
        let all_files = Option::<&FfiAddArray>::from(&self.all_files)
            .map(|value| unsafe { value.try_to_kernel() })
            .transpose()?;
        let deleted_record_counts_histogram =
            unsafe { self.deleted_record_counts_histogram.as_ref() }
                .map(|value| unsafe { value.try_to_kernel() })
                .transpose()?;
        Ok(Crc::from_parts(
            self.version,
            unsafe { self.metadata.try_to_kernel() }?,
            unsafe { self.protocol.try_to_kernel() }?,
            unsafe { self.file_stats_state.try_to_kernel() }?,
            Option::<&i64>::from(&self.in_commit_timestamp).copied(),
            unsafe { self.set_transaction_state.try_to_kernel() }?,
            unsafe { self.domain_metadata_state.try_to_kernel() }?,
            txn_id,
            all_files,
            Option::<&i64>::from(&self.num_deleted_records).copied(),
            Option::<&i64>::from(&self.num_deletion_vectors).copied(),
            deleted_record_counts_histogram,
        ))
    }
}

#[cfg(test)]
mod tests;
