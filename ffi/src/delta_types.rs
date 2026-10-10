//! Reusable borrowed C FFI representations of Delta state.
//!
//! Backing structs (e.g. `CrcBacking`) own temporary C-layout arrays and records while borrowing
//! the Rust-layout source payloads. FFI views must not outlive either their backing storage or
//! the borrowed source payloads.
//!
//! Fields prefixed with `_` keep storage referenced by cached FFI records alive, even though
//! `as_ffi()` does not read those fields directly. For example, `CrcBacking::_add_backing` owns
//! the buffers and deletion-vector descriptors referenced by `adds`. In `CheckpointV2Backing`,
//! `_sidecar_tags` owns tag arrays referenced by `sidecars`, and `_action_records` owns records
//! addressed by `actions`; `_action_backing` retains the protocol and metadata arrays those
//! records reference. The `_` prefix suppresses unused-field warnings; these fields obey the
//! same lifetime rules as other backing storage.
//!
//! Build views after inline pointees are in place, and do not move those pointees or reallocate
//! referenced buffers while the views are in use.

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
use delta_kernel::snapshot::PublicationWatermark;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{KernelError, KernelResult, Version};

use crate::{
    kernel_string_slice, optional_pointer, FfiFileStats, FfiSlice, KernelI64Slice,
    KernelStringSlice, OptionalValue,
};

/// Borrowed array of UTF-8 strings.
pub type FfiStringArray = FfiSlice<KernelStringSlice>;

/// One borrowed column path, stored as UTF-8 path segments.
#[repr(C)]
pub struct FfiColumnName {
    /// Column path segments.
    pub path: FfiStringArray,
}

/// Borrowed array of column paths.
pub type FfiColumnNameArray = FfiSlice<FfiColumnName>;

/// One borrowed UTF-8 map entry.
#[repr(C)]
pub struct FfiStringMapEntry {
    /// Entry key.
    pub key: KernelStringSlice,
    /// Entry value.
    pub value: KernelStringSlice,
}

/// Borrowed array of UTF-8 map entries.
pub type FfiStringMap = FfiSlice<FfiStringMapEntry>;

/// One borrowed UTF-8 map entry whose value may be null.
#[repr(C)]
pub struct FfiNullableStringMapEntry {
    /// Entry key.
    pub key: KernelStringSlice,
    /// Optional entry value.
    pub value: OptionalValue<KernelStringSlice>,
}

/// Borrowed array of UTF-8 map entries whose values may be null.
pub type FfiNullableStringMap = FfiSlice<FfiNullableStringMapEntry>;

/// C-layout representation of kernel [`PublicationWatermark`].
///
/// Publication and ratification are separate: a staged commit may belong to the snapshot without
/// having been published to the table's log directory. The snapshot version and freshness claim
/// do not establish how far publication has advanced.
///
/// Connectors that only supply log paths can request inference. Connectors exporting a built
/// snapshot must preserve its observed publication state, which may not be recoverable from the
/// retained paths. [`snapshot_to_snapshot_hint`](crate::snapshot_hint::snapshot_to_snapshot_hint)
/// always reports one of the two explicit variants.
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

/// Owns feature-list descriptors while borrowing the protocol's payloads.
pub(crate) struct ProtocolBacking<'a> {
    source: &'a Protocol,
    reader_features: Option<Vec<KernelStringSlice>>,
    writer_features: Option<Vec<KernelStringSlice>>,
}

impl<'a> ProtocolBacking<'a> {
    /// Creates C-layout storage borrowing `source`.
    pub(crate) fn new(source: &'a Protocol) -> Self {
        let features = |values: Option<&[TableFeature]>| {
            values.map(|values| {
                values
                    .iter()
                    .map(|feature| {
                        let name = feature.as_ref();
                        kernel_string_slice!(name)
                    })
                    .collect()
            })
        };
        Self {
            source,
            reader_features: features(source.reader_features()),
            writer_features: features(source.writer_features()),
        }
    }

    /// Returns a view borrowing this backing storage and its source.
    pub(crate) fn as_ffi(&self) -> FfiProtocol {
        FfiProtocol {
            min_reader_version: self.source.min_reader_version(),
            min_writer_version: self.source.min_writer_version(),
            reader_features: self
                .reader_features
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
            writer_features: self
                .writer_features
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
        }
    }
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

/// Owns metadata arrays while borrowing the metadata's payloads.
pub(crate) struct MetadataBacking<'a> {
    source: &'a Metadata,
    format_options: Vec<FfiStringMapEntry>,
    partition_columns: Vec<KernelStringSlice>,
    configuration: Vec<FfiStringMapEntry>,
}

impl<'a> MetadataBacking<'a> {
    /// Creates C-layout storage borrowing `source`.
    pub(crate) fn new(source: &'a Metadata) -> Self {
        Self {
            source,
            // The retained source reference keeps all borrowed map payloads alive.
            format_options: unsafe { FfiStringMapEntry::from_map_unsafe(source.format_options()) },
            partition_columns: source
                .partition_columns()
                .iter()
                .map(|value| kernel_string_slice!(value))
                .collect(),
            configuration: unsafe { FfiStringMapEntry::from_map_unsafe(source.configuration()) },
        }
    }

    /// Returns a view borrowing this backing storage and its source.
    pub(crate) fn as_ffi(&self) -> FfiMetadata {
        let id = self.source.id();
        let provider = self.source.format_provider();
        let schema = self.source.schema_string();
        FfiMetadata {
            id: kernel_string_slice!(id),
            name: self
                .source
                .name()
                .map(|value| kernel_string_slice!(value))
                .into(),
            description: self
                .source
                .description()
                .map(|value| kernel_string_slice!(value))
                .into(),
            format_provider: kernel_string_slice!(provider),
            format_options: unsafe { FfiSlice::new_unsafe(&self.format_options) },
            schema_string: kernel_string_slice!(schema),
            partition_columns: unsafe { FfiSlice::new_unsafe(&self.partition_columns) },
            created_time: self.source.created_time().into(),
            configuration: unsafe { FfiSlice::new_unsafe(&self.configuration) },
        }
    }
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
pub type FfiSidecarArray = FfiSlice<FfiSidecar>;

/// Borrowed array of Delta set-transaction actions.
pub type FfiSetTransactionArray = FfiSlice<FfiSetTransaction>;

/// Borrowed array of Delta domain-metadata actions.
pub type FfiDomainMetadataArray = FfiSlice<FfiDomainMetadata>;

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

enum CheckpointActionBacking<'a> {
    Metadata(MetadataBacking<'a>),
    Protocol(ProtocolBacking<'a>),
    Transaction(&'a SetTransaction),
    DomainMetadata(&'a DomainMetadata),
    CheckpointMetadata {
        source: &'a CheckpointMetadata,
        tags: Option<Vec<FfiStringMapEntry>>,
    },
}

impl<'a> CheckpointActionBacking<'a> {
    fn new(value: &'a HintAction) -> Self {
        match value {
            HintAction::Metadata(value) => Self::Metadata(MetadataBacking::new(value)),
            HintAction::Protocol(value) => Self::Protocol(ProtocolBacking::new(value)),
            HintAction::Txn(value) => Self::Transaction(value),
            HintAction::DomainMetadata(value) => Self::DomainMetadata(value),
            HintAction::CheckpointMetadata(value) => Self::CheckpointMetadata {
                source: value,
                tags: value
                    .tags()
                    .map(|values| unsafe { FfiStringMapEntry::from_map_unsafe(values) }),
            },
        }
    }

    fn record(&self) -> CheckpointActionRecord {
        match self {
            Self::Metadata(value) => CheckpointActionRecord::Metadata(value.as_ffi()),
            Self::Protocol(value) => CheckpointActionRecord::Protocol(value.as_ffi()),
            Self::Transaction(value) => CheckpointActionRecord::Transaction(transaction(value)),
            Self::DomainMetadata(value) => CheckpointActionRecord::DomainMetadata(domain(value)),
            Self::CheckpointMetadata { source, tags } => {
                CheckpointActionRecord::CheckpointMetadata(FfiCheckpointMetadata {
                    version: source.version(),
                    tags: tags
                        .as_deref()
                        .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                        .into(),
                })
            }
        }
    }
}

enum CheckpointActionRecord {
    Metadata(FfiMetadata),
    Protocol(FfiProtocol),
    Transaction(FfiSetTransaction),
    DomainMetadata(FfiDomainMetadata),
    CheckpointMetadata(FfiCheckpointMetadata),
}

impl CheckpointActionRecord {
    fn as_ffi(&self) -> FfiCheckpointNonFileAction {
        match self {
            Self::Metadata(value) => FfiCheckpointNonFileAction::Metadata(value),
            Self::Protocol(value) => FfiCheckpointNonFileAction::Protocol(value),
            Self::Transaction(value) => FfiCheckpointNonFileAction::Transaction(value),
            Self::DomainMetadata(value) => FfiCheckpointNonFileAction::DomainMetadata(value),
            Self::CheckpointMetadata(value) => {
                FfiCheckpointNonFileAction::CheckpointMetadata(value)
            }
        }
    }
}

/// Borrowed array of typed checkpoint non-file actions.
pub type FfiCheckpointNonFileActionArray = FfiSlice<FfiCheckpointNonFileAction>;

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

struct CheckpointV2Backing<'a> {
    source: &'a LastCheckpointV2,
    _sidecar_tags: Option<Vec<Option<Vec<FfiStringMapEntry>>>>,
    sidecars: Option<Vec<FfiSidecar>>,
    _action_backing: Option<Vec<CheckpointActionBacking<'a>>>,
    _action_records: Option<Vec<CheckpointActionRecord>>,
    actions: Option<Vec<FfiCheckpointNonFileAction>>,
}

impl<'a> CheckpointV2Backing<'a> {
    fn new(source: &'a LastCheckpointV2) -> Self {
        let sidecar_tags: Option<Vec<_>> = source.sidecar_files().map(|values| {
            values
                .iter()
                .map(|value| {
                    value
                        .tags
                        .as_ref()
                        .map(|values| unsafe { FfiStringMapEntry::from_map_unsafe(values) })
                })
                .collect()
        });
        let sidecars: Option<Vec<_>> =
            source
                .sidecar_files()
                .zip(sidecar_tags.as_ref())
                .map(|(values, tags)| {
                    values
                        .iter()
                        .zip(tags)
                        .map(|(value, tags)| {
                            let path = &value.path;
                            FfiSidecar {
                                path: kernel_string_slice!(path),
                                size_in_bytes: value.size_in_bytes,
                                modification_time: value.modification_time,
                                tags: tags
                                    .as_deref()
                                    .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                                    .into(),
                            }
                        })
                        .collect()
                });
        let action_backing: Option<Vec<_>> = source
            .non_file_actions()
            .map(|values| values.iter().map(CheckpointActionBacking::new).collect());
        let action_records: Option<Vec<_>> = action_backing
            .as_ref()
            .map(|values| values.iter().map(CheckpointActionBacking::record).collect());
        // Take variant addresses only after every record is in its final vector position.
        let actions: Option<Vec<_>> = action_records
            .as_ref()
            .map(|values| values.iter().map(CheckpointActionRecord::as_ffi).collect());
        Self {
            source,
            _sidecar_tags: sidecar_tags,
            sidecars,
            _action_backing: action_backing,
            _action_records: action_records,
            actions,
        }
    }

    fn as_ffi(&self) -> FfiLastCheckpointV2 {
        let path = self.source.path();
        FfiLastCheckpointV2 {
            path: kernel_string_slice!(path),
            size_in_bytes: self.source.size_in_bytes().into(),
            modification_time: self.source.modification_time().into(),
            sidecar_files: self
                .sidecars
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
            non_file_actions: self
                .actions
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
        }
    }
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

/// Owns checkpoint records and arrays while borrowing the checkpoint's payloads.
pub(crate) struct CheckpointBacking<'a> {
    source: &'a LastCheckpointHint,
    schema: Option<String>,
    tags: Option<Vec<FfiStringMapEntry>>,
    _v2_backing: Option<CheckpointV2Backing<'a>>,
    v2: Option<FfiLastCheckpointV2>,
}

impl<'a> CheckpointBacking<'a> {
    /// Creates C-layout storage borrowing `source`, or returns a schema serialization error.
    pub(crate) fn try_new(source: &'a LastCheckpointHint) -> KernelResult<Self> {
        let schema = source
            .checkpoint_schema()
            .map(|schema| serde_json::to_string(schema.as_ref()))
            .transpose()?;
        let tags = source
            .tags()
            .map(|values| unsafe { FfiStringMapEntry::from_map_unsafe(values) });
        let v2_backing = source.v2_checkpoint().map(CheckpointV2Backing::new);
        // The V2 record points into vector buffers, which remain stable when their owners move.
        let v2 = v2_backing.as_ref().map(CheckpointV2Backing::as_ffi);
        Ok(Self {
            source,
            schema,
            tags,
            _v2_backing: v2_backing,
            v2,
        })
    }

    /// Returns a view borrowing this backing storage and its source. Do not move the backing
    /// while using the view, which may point to its inline V2 record.
    pub(crate) fn as_ffi(&self) -> FfiLastCheckpoint {
        FfiLastCheckpoint {
            version: self.source.version(),
            size: self.source.size(),
            parts: self.source.parts().map(|parts| parts as u64).into(),
            size_in_bytes: self.source.size_in_bytes().into(),
            num_of_add_files: self.source.num_of_add_files().into(),
            checkpoint_schema: self
                .schema
                .as_deref()
                .map(|value| kernel_string_slice!(value))
                .into(),
            checksum: self
                .source
                .checksum()
                .map(|value| kernel_string_slice!(value))
                .into(),
            tags: self
                .tags
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
            v2_checkpoint: optional_pointer(self.v2.as_ref()),
        }
    }
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
    /// Persisted raw (unencoded) table-relative path. Only valid when built with the
    /// `adaptive-metadata-in-dev` cargo feature.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    PersistedUnencodedRelative,
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

struct AddBacking<'a> {
    source: &'a Add,
    partition_values: Vec<FfiStringMapEntry>,
    tags: Option<Vec<FfiNullableStringMapEntry>>,
    deletion_vector: Option<FfiDeletionVectorDescriptor>,
}

impl<'a> AddBacking<'a> {
    fn new(source: &'a Add) -> Self {
        Self {
            source,
            partition_values: unsafe {
                FfiStringMapEntry::from_map_unsafe(source.partition_values())
            },
            tags: source.tags.as_ref().map(|values| {
                values
                    .iter()
                    .map(|(key, value)| FfiNullableStringMapEntry {
                        key: kernel_string_slice!(key),
                        value: value
                            .as_deref()
                            .map(|value| kernel_string_slice!(value))
                            .into(),
                    })
                    .collect()
            }),
            deletion_vector: source.deletion_vector.as_ref().map(|value| {
                let path = &value.path_or_inline_dv;
                FfiDeletionVectorDescriptor {
                    storage_type: match value.storage_type {
                        DeletionVectorStorageType::PersistedRelative => {
                            FfiDeletionVectorStorageType::PersistedRelative
                        }
                        DeletionVectorStorageType::Inline => FfiDeletionVectorStorageType::Inline,
                        DeletionVectorStorageType::PersistedAbsolute => {
                            FfiDeletionVectorStorageType::PersistedAbsolute
                        }
                        #[cfg(feature = "adaptive-metadata-in-dev")]
                        DeletionVectorStorageType::PersistedUnencodedRelative => {
                            FfiDeletionVectorStorageType::PersistedUnencodedRelative
                        }
                    },
                    path_or_inline_dv: kernel_string_slice!(path),
                    offset: value.offset.into(),
                    size_in_bytes: value.size_in_bytes,
                    cardinality: value.cardinality,
                }
            }),
        }
    }

    fn as_ffi(&self) -> FfiAdd {
        let path = self.source.path();
        FfiAdd {
            path: kernel_string_slice!(path),
            partition_values: unsafe { FfiSlice::new_unsafe(&self.partition_values) },
            size: self.source.size(),
            modification_time: self.source.modification_time(),
            data_change: self.source.data_change(),
            stats: self
                .source
                .stats
                .as_deref()
                .map(|value| kernel_string_slice!(value))
                .into(),
            tags: self
                .tags
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
            deletion_vector: optional_pointer(self.deletion_vector.as_ref()),
            base_row_id: self.source.base_row_id.into(),
            default_row_commit_version: self.source.default_row_commit_version.into(),
            clustering_provider: self
                .source
                .clustering_provider
                .as_deref()
                .map(|value| kernel_string_slice!(value))
                .into(),
        }
    }
}

/// Borrowed array of Delta Add actions.
pub type FfiAddArray = FfiSlice<FfiAdd>;

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

/// Owns CRC records and arrays while borrowing the CRC's payloads.
pub(crate) struct CrcBacking<'a> {
    source: &'a Crc,
    protocol: ProtocolBacking<'a>,
    metadata: MetadataBacking<'a>,
    histogram: Option<FfiFileSizeHistogram>,
    transactions: Vec<FfiSetTransaction>,
    domains: Vec<FfiDomainMetadata>,
    _add_backing: Option<Vec<AddBacking<'a>>>,
    adds: Option<Vec<FfiAdd>>,
    deleted_histogram: Option<FfiDeletedRecordCountsHistogram>,
}

impl<'a> CrcBacking<'a> {
    /// Creates C-layout storage borrowing `source`.
    pub(crate) fn new(source: &'a Crc) -> Self {
        let protocol = ProtocolBacking::new(&source.protocol);
        let metadata = MetadataBacking::new(&source.metadata);
        let histogram = source.file_stats().and_then(|stats| {
            stats
                .file_size_histogram()
                .map(|value| FfiFileSizeHistogram {
                    sorted_bin_boundaries: unsafe {
                        FfiSlice::new_unsafe(value.sorted_bin_boundaries())
                    },
                    file_counts: unsafe { FfiSlice::new_unsafe(value.file_counts()) },
                    total_bytes: unsafe { FfiSlice::new_unsafe(value.total_bytes()) },
                })
        });
        let transactions = match &source.set_transaction_state {
            SetTransactionState::Complete(values) | SetTransactionState::Partial(values) => values,
        };
        let transactions = transactions.values().map(transaction).collect();
        let domains = match &source.domain_metadata_state {
            DomainMetadataState::Complete(values) | DomainMetadataState::Partial(values) => values,
        };
        let domains = domains.values().map(domain).collect();
        let add_backing: Option<Vec<_>> = source
            .all_files()
            .map(|values| values.iter().map(AddBacking::new).collect());
        let adds = add_backing
            .as_ref()
            .map(|values| values.iter().map(AddBacking::as_ffi).collect());
        let deleted_histogram =
            source
                .deleted_record_counts_histogram()
                .map(|value| FfiDeletedRecordCountsHistogram {
                    deleted_record_counts: unsafe {
                        FfiSlice::new_unsafe(value.deleted_record_counts())
                    },
                });
        Self {
            source,
            protocol,
            metadata,
            histogram,
            transactions,
            domains,
            _add_backing: add_backing,
            adds,
            deleted_histogram,
        }
    }

    /// Returns a view borrowing this backing storage and its source. Do not move the backing
    /// while using the view, which may point to its inline histogram records.
    pub(crate) fn as_ffi(&self) -> FfiCrc {
        let file_stats_state = match self.source.file_stats_state() {
            FileStatsState::Indeterminate => FfiFileStatsState {
                kind: FfiFileStatsStateKind::Indeterminate,
                file_stats: FfiFileStats {
                    num_files: 0,
                    table_size_bytes: 0,
                },
                file_size_histogram: std::ptr::null(),
            },
            FileStatsState::Complete(stats) => FfiFileStatsState {
                kind: FfiFileStatsStateKind::Complete,
                file_stats: FfiFileStats {
                    num_files: stats.num_files(),
                    table_size_bytes: stats.table_size_bytes(),
                },
                file_size_histogram: optional_pointer(self.histogram.as_ref()),
            },
        };
        let transaction_kind = match &self.source.set_transaction_state {
            SetTransactionState::Complete(_) => FfiSetTransactionStateKind::Complete,
            SetTransactionState::Partial(_) => FfiSetTransactionStateKind::Partial,
        };
        let domain_kind = match &self.source.domain_metadata_state {
            DomainMetadataState::Complete(_) => FfiDomainMetadataStateKind::Complete,
            DomainMetadataState::Partial(_) => FfiDomainMetadataStateKind::Partial,
        };
        FfiCrc {
            version: self.source.version,
            metadata: self.metadata.as_ffi(),
            protocol: self.protocol.as_ffi(),
            file_stats_state,
            in_commit_timestamp: self.source.in_commit_timestamp_opt.into(),
            set_transaction_state: FfiSetTransactionState {
                kind: transaction_kind,
                transactions: unsafe { FfiSlice::new_unsafe(&self.transactions) },
            },
            domain_metadata_state: FfiDomainMetadataState {
                kind: domain_kind,
                domain_metadata: unsafe { FfiSlice::new_unsafe(&self.domains) },
            },
            txn_id: self
                .source
                .txn_id()
                .map(|value| kernel_string_slice!(value))
                .into(),
            all_files: self
                .adds
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
            num_deleted_records: self.source.num_deleted_records().into(),
            num_deletion_vectors: self.source.num_deletion_vectors().into(),
            deleted_record_counts_histogram: optional_pointer(self.deleted_histogram.as_ref()),
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

pub(crate) fn invalid(message: impl Into<String>) -> KernelError {
    KernelError::generic(message.into())
}

impl FfiStringMapEntry {
    /// Creates entries borrowing the keys and values in `values`, without copying their bytes.
    ///
    /// # Safety
    ///
    /// The map must remain valid and unmodified for every use of the returned entries.
    pub(crate) unsafe fn from_map_unsafe(values: &HashMap<String, String>) -> Vec<Self> {
        values
            .iter()
            .map(|(key, value)| Self {
                key: kernel_string_slice!(key),
                value: kernel_string_slice!(value),
            })
            .collect()
    }
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
) -> KernelResult<&'a T> {
    unsafe { ptr.as_ref() }.ok_or_else(|| invalid(format!("{name} value is null")))
}

impl FfiStringArray {
    pub(crate) unsafe fn try_to_strings(&self) -> KernelResult<Vec<String>> {
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|value| unsafe { value.try_to_string() })
            .collect()
    }
}

impl FfiStringMap {
    pub(crate) unsafe fn try_to_hash_map(&self) -> KernelResult<HashMap<String, String>> {
        let entries = unsafe { self.try_as_slice() }?;
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
    pub(crate) unsafe fn try_to_hash_map(&self) -> KernelResult<HashMap<String, Option<String>>> {
        let entries = unsafe { self.try_as_slice() }?;
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
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<Protocol> {
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
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<Metadata> {
        let schema = unsafe { self.schema_string.try_to_string() }?;
        unsafe { self.try_to_kernel_with_schema(schema) }
    }

    pub(crate) unsafe fn try_to_kernel_with_schema(
        &self,
        schema: String,
    ) -> KernelResult<Metadata> {
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
            schema,
            unsafe { self.partition_columns.try_to_strings() }?,
            Option::<&i64>::from(&self.created_time).copied(),
            unsafe { self.configuration.try_to_hash_map() }?,
        ))
    }
}

impl FfiSetTransaction {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<SetTransaction> {
        Ok(SetTransaction::new(
            unsafe { self.app_id.try_to_string() }?,
            self.version,
            Option::<&i64>::from(&self.last_updated).copied(),
        ))
    }
}

impl FfiDomainMetadata {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<DomainMetadata> {
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
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<CheckpointMetadata> {
        let tags = Option::<&FfiStringMap>::from(&self.tags)
            .map(|value| unsafe { value.try_to_hash_map() })
            .transpose()?;
        Ok(CheckpointMetadata::new(self.version, tags))
    }
}

impl FfiSidecar {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<Sidecar> {
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
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<FileSizeHistogram> {
        FileSizeHistogram::try_new(
            unsafe { self.sorted_bin_boundaries.try_as_slice() }?.to_vec(),
            unsafe { self.file_counts.try_as_slice() }?.to_vec(),
            unsafe { self.total_bytes.try_as_slice() }?.to_vec(),
        )
    }
}

impl FfiSidecarArray {
    unsafe fn try_to_kernel(&self) -> KernelResult<Vec<Sidecar>> {
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiCheckpointNonFileAction {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<HintAction> {
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
    unsafe fn try_to_kernel(&self) -> KernelResult<Vec<HintAction>> {
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiLastCheckpointV2 {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<LastCheckpointV2> {
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
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<LastCheckpointHint> {
        let parts = Option::<&u64>::from(&self.parts)
            .map(|value| {
                let value = u32::try_from(*value)
                    .map_err(|_| invalid(format!("checkpoint part count exceeds u32: {value}")))?;
                Ok::<usize, KernelError>(value as usize)
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
            #[cfg(feature = "adaptive-metadata-in-dev")]
            FfiDeletionVectorStorageType::PersistedUnencodedRelative => {
                Self::PersistedUnencodedRelative
            }
        }
    }
}

impl FfiDeletionVectorDescriptor {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<DeletionVectorDescriptor> {
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
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<Add> {
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
    unsafe fn try_to_kernel(&self) -> KernelResult<Vec<Add>> {
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiFileStatsState {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<FileStatsState> {
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
    unsafe fn try_to_vec(&self) -> KernelResult<Vec<SetTransaction>> {
        let values = unsafe { self.try_as_slice() }?;
        values
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiSetTransactionState {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<SetTransactionState> {
        let transactions = unsafe { self.transactions.try_to_vec() }?;
        match self.kind {
            FfiSetTransactionStateKind::Complete => SetTransactionState::try_complete(transactions),
            FfiSetTransactionStateKind::Partial => SetTransactionState::try_partial(transactions),
        }
    }
}

impl FfiDomainMetadataArray {
    unsafe fn try_to_vec(&self) -> KernelResult<Vec<DomainMetadata>> {
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|value| unsafe { value.try_to_kernel() })
            .collect()
    }
}

impl FfiDomainMetadataState {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<DomainMetadataState> {
        let domain_metadata = unsafe { self.domain_metadata.try_to_vec() }?;
        match self.kind {
            FfiDomainMetadataStateKind::Complete => {
                DomainMetadataState::try_complete(domain_metadata)
            }
            FfiDomainMetadataStateKind::Partial => {
                DomainMetadataState::try_partial(domain_metadata)
            }
        }
    }
}

impl FfiDeletedRecordCountsHistogram {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<DeletedRecordCountsHistogram> {
        DeletedRecordCountsHistogram::try_new(
            unsafe { self.deleted_record_counts.try_as_slice() }?.to_vec(),
        )
    }
}

impl FfiCrc {
    pub(crate) unsafe fn try_to_kernel(&self) -> KernelResult<Crc> {
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
        Crc::try_from_parts(
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
            #[cfg(feature = "adaptive-metadata-in-dev")]
            None,
        )
    }
}

fn transaction(value: &SetTransaction) -> FfiSetTransaction {
    let app_id = value.app_id();
    FfiSetTransaction {
        app_id: kernel_string_slice!(app_id),
        version: value.version(),
        last_updated: value.last_updated().into(),
    }
}

fn domain(value: &DomainMetadata) -> FfiDomainMetadata {
    let domain = value.domain();
    let configuration = value.configuration();
    FfiDomainMetadata {
        domain: kernel_string_slice!(domain),
        configuration: kernel_string_slice!(configuration),
        removed: value.is_removed(),
    }
}

#[cfg(test)]
mod tests;
