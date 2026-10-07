//! Backing structs (e.g. `CrcBacking`) own temporary C-layout arrays and records while borrowing
//! the Rust-layout source payloads. FFI views must not outlive either their backing storage or
//! the borrowed source payloads.
//!
//! Fields prefixed with `_` keep storage referenced by cached FFI records alive, even though
//! `as_ffi()` does not read those fields directly. For example, `CrcBacking::_add_backing` owns
//! the buffers and deletion-vector descriptors referenced by `adds`. In `CheckpointV2Backing`,
//! `_sidecar_tags` owns tag arrays referenced by `sidecars`, and `_action_records` owns records
//! addressed by `actions`. The `_` prefix suppresses unused-field warnings; these fields obey
//! the same lifetime rules as other backing storage.
//!
//! Build views after inline pointees are in place, and do not move those pointees or reallocate
//! referenced buffers while the views are in use.

use delta_kernel::actions::deletion_vector::DeletionVectorStorageType;
use delta_kernel::actions::{
    Add, CheckpointMetadata, DomainMetadata, Metadata, Protocol, SetTransaction,
};
use delta_kernel::crc::{Crc, DomainMetadataState, FileStatsState, SetTransactionState};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::snapshot::{SnapshotHint, SnapshotHintFreshness};
#[cfg(feature = "adaptive-metadata-in-dev")]
use delta_kernel::KernelError;
use delta_kernel::KernelResult;

use super::{FfiSnapshotHint, FfiSnapshotHintFreshness, SnapshotHintVisitor};
use crate::delta_types::*;
use crate::log_path::FfiLogPath;
use crate::{
    kernel_string_slice, optional_pointer, FfiFileStats, FfiSlice, KernelStringSlice, NullableCvoid,
};

pub(super) fn visit(
    hint: &SnapshotHint,
    context: NullableCvoid,
    visitor: SnapshotHintVisitor,
) -> KernelResult<()> {
    #[cfg(feature = "adaptive-metadata-in-dev")]
    if hint.crc().is_some_and(|crc| {
        crc.last_manifest_commit().is_some()
            || crc
                .all_files()
                .is_some_and(|files| files.iter().any(|add| add.back_reference().is_some()))
    }) {
        return Err(KernelError::unsupported(
            "typed snapshot hint export cannot represent CRC lastManifestCommit or Add backReference",
        ));
    }
    let files = hint.log_segment_files();
    // Checkpoint adoption can retain the latest commit separately from replay files.
    let mut paths: Vec<_> = files
        .checkpoint_parts
        .iter()
        .chain(&files.ascending_commit_files)
        .chain(files.latest_commit_file.iter())
        .chain(files.latest_crc_file.iter())
        .collect();
    // Deduplication only removes adjacent entries; sorting groups copies of the same retained path.
    paths.sort_unstable_by_key(|path| path.location.location.as_str());
    paths.dedup_by_key(|path| path.location.location.as_str());
    let paths: Vec<_> = paths
        .into_iter()
        .map(|path| {
            let location = path.location.location.as_str();
            FfiLogPath::new(
                kernel_string_slice!(location),
                path.location.last_modified,
                path.location.size,
            )
        })
        .collect();
    let protocol = ProtocolBacking::new(hint.protocol());
    let metadata = MetadataBacking::new(hint.metadata());
    let checkpoint = hint
        .last_checkpoint_hint()
        .map(CheckpointBacking::try_new)
        .transpose()?;
    let crc = hint.crc().map(CrcBacking::new);
    let checkpoint_view = checkpoint.as_ref().map(CheckpointBacking::as_ffi);
    let crc_view = crc.as_ref().map(CrcBacking::as_ffi);
    let value = FfiSnapshotHint {
        version: hint.version(),
        freshness: match hint.freshness() {
            SnapshotHintFreshness::Latest => FfiSnapshotHintFreshness::Latest,
            SnapshotHintFreshness::Unverified => FfiSnapshotHintFreshness::Unverified,
        },
        log_paths: unsafe { FfiSlice::new_unsafe(&paths) },
        protocol: protocol.as_ffi(),
        metadata: metadata.as_ffi(),
        last_checkpoint: optional_pointer(checkpoint_view.as_ref()),
        crc: optional_pointer(crc_view.as_ref()),
        publication_watermark: hint.publication_watermark().into(),
    };
    visitor(context, &value);
    Ok(())
}

struct ProtocolBacking<'a> {
    source: &'a Protocol,
    reader_features: Option<Vec<KernelStringSlice>>,
    writer_features: Option<Vec<KernelStringSlice>>,
}

impl<'a> ProtocolBacking<'a> {
    fn new(source: &'a Protocol) -> Self {
        let features = |values: Option<&[delta_kernel::table_features::TableFeature]>| {
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

    fn as_ffi(&self) -> FfiProtocol {
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

struct MetadataBacking<'a> {
    source: &'a Metadata,
    format_options: Vec<FfiStringMapEntry>,
    partition_columns: Vec<KernelStringSlice>,
    configuration: Vec<FfiStringMapEntry>,
}

impl<'a> MetadataBacking<'a> {
    fn new(source: &'a Metadata) -> Self {
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

    fn as_ffi(&self) -> FfiMetadata {
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

struct CheckpointBacking<'a> {
    source: &'a LastCheckpointHint,
    schema: Option<String>,
    tags: Option<Vec<FfiStringMapEntry>>,
    _v2_backing: Option<CheckpointV2Backing<'a>>,
    v2: Option<FfiLastCheckpointV2>,
}

impl<'a> CheckpointBacking<'a> {
    fn try_new(source: &'a LastCheckpointHint) -> KernelResult<Self> {
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

    fn as_ffi(&self) -> FfiLastCheckpoint {
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

struct CrcBacking<'a> {
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
    fn new(source: &'a Crc) -> Self {
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

    fn as_ffi(&self) -> FfiCrc {
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
