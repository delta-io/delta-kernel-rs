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
                .is_some_and(|files| files.iter().any(Add::has_back_reference))
    }) {
        return Err(KernelError::unsupported(
            "typed snapshot hint export cannot represent CRC lastManifestCommit or Add backReference",
        ));
    }
    let files = hint.log_segment_files();
    // Checkpoint adoption can retain the latest commit separately from replay files.
    // Include it, then remove duplicates when it also appears in ascending_commit_files.
    let mut paths: Vec<_> = files
        .checkpoint_parts
        .iter()
        .chain(&files.ascending_commit_files)
        .chain(files.latest_commit_file.iter())
        .chain(files.latest_crc_file.iter())
        .collect();
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
    with_checkpoint(hint.last_checkpoint_hint(), |checkpoint| {
        with_crc(hint.crc(), |crc| {
            let value = FfiSnapshotHint {
                version: hint.version(),
                freshness: match hint.freshness() {
                    SnapshotHintFreshness::Latest => FfiSnapshotHintFreshness::Latest,
                    SnapshotHintFreshness::Unverified => FfiSnapshotHintFreshness::Unverified,
                },
                // All backing vectors remain live until the visitor returns.
                log_paths: unsafe { FfiSlice::new_unsafe(&paths) },
                protocol: protocol.as_ffi(),
                metadata: metadata.as_ffi(),
                last_checkpoint: optional_pointer(checkpoint.as_ref()),
                crc: optional_pointer(crc.as_ref()),
                publication_watermark: hint.publication_watermark().into(),
            };
            visitor(context, &value);
        });
    })
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

fn with_checkpoint<R>(
    value: Option<&LastCheckpointHint>,
    f: impl FnOnce(Option<FfiLastCheckpoint>) -> R,
) -> KernelResult<R> {
    let Some(value) = value else {
        return Ok(f(None));
    };
    let schema = value
        .checkpoint_schema()
        .map(|schema| serde_json::to_string(schema.as_ref()))
        .transpose()?;
    let tags = value
        .tags()
        .map(|values| unsafe { FfiStringMapEntry::from_map_unsafe(values) });
    Ok(with_checkpoint_v2(value.v2_checkpoint(), |v2| {
        f(Some(FfiLastCheckpoint {
            version: value.version(),
            size: value.size(),
            parts: value.parts().map(|parts| parts as u64).into(),
            size_in_bytes: value.size_in_bytes().into(),
            num_of_add_files: value.num_of_add_files().into(),
            checkpoint_schema: schema
                .as_deref()
                .map(|value| kernel_string_slice!(value))
                .into(),
            checksum: value
                .checksum()
                .map(|value| kernel_string_slice!(value))
                .into(),
            tags: tags
                .as_deref()
                .map(|values| unsafe { FfiSlice::new_unsafe(values) })
                .into(),
            v2_checkpoint: optional_pointer(v2.as_ref()),
        }))
    }))
}

fn with_checkpoint_v2<R>(
    value: Option<&LastCheckpointV2>,
    f: impl FnOnce(Option<FfiLastCheckpointV2>) -> R,
) -> R {
    let Some(value) = value else {
        return f(None);
    };
    let sidecar_tags: Option<Vec<_>> = value.sidecar_files().map(|values| {
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
        value
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
    let action_backing: Option<Vec<_>> = value
        .non_file_actions()
        .map(|values| values.iter().map(CheckpointActionBacking::new).collect());
    let action_records: Option<Vec<_>> = action_backing
        .as_ref()
        .map(|values| values.iter().map(CheckpointActionBacking::record).collect());
    // Take variant addresses only after every record is in its final vector position.
    let actions: Option<Vec<_>> = action_records
        .as_ref()
        .map(|values| values.iter().map(CheckpointActionRecord::as_ffi).collect());
    let path = value.path();
    f(Some(FfiLastCheckpointV2 {
        path: kernel_string_slice!(path),
        size_in_bytes: value.size_in_bytes().into(),
        modification_time: value.modification_time().into(),
        sidecar_files: sidecars
            .as_deref()
            .map(|values| unsafe { FfiSlice::new_unsafe(values) })
            .into(),
        non_file_actions: actions
            .as_deref()
            .map(|values| unsafe { FfiSlice::new_unsafe(values) })
            .into(),
    }))
}

fn with_crc<R>(value: Option<&Crc>, f: impl FnOnce(Option<FfiCrc>) -> R) -> R {
    let Some(value) = value else {
        return f(None);
    };
    let protocol = ProtocolBacking::new(&value.protocol);
    let metadata = MetadataBacking::new(&value.metadata);
    let histogram = value.file_stats().and_then(|stats| {
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
    let file_stats_state = match value.file_stats_state() {
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
            file_size_histogram: optional_pointer(histogram.as_ref()),
        },
    };
    let (transaction_kind, transactions) = match &value.set_transaction_state {
        SetTransactionState::Complete(values) => (FfiSetTransactionStateKind::Complete, values),
        SetTransactionState::Partial(values) => (FfiSetTransactionStateKind::Partial, values),
    };
    let transactions: Vec<_> = transactions.values().map(transaction).collect();
    let (domain_kind, domains) = match &value.domain_metadata_state {
        DomainMetadataState::Complete(values) => (FfiDomainMetadataStateKind::Complete, values),
        DomainMetadataState::Partial(values) => (FfiDomainMetadataStateKind::Partial, values),
    };
    let domains: Vec<_> = domains.values().map(domain).collect();
    let add_backing: Option<Vec<_>> = value
        .all_files()
        .map(|values| values.iter().map(AddBacking::new).collect());
    let adds: Option<Vec<_>> = add_backing
        .as_ref()
        .map(|values| values.iter().map(AddBacking::as_ffi).collect());
    let deleted_histogram =
        value
            .deleted_record_counts_histogram()
            .map(|value| FfiDeletedRecordCountsHistogram {
                deleted_record_counts: unsafe {
                    FfiSlice::new_unsafe(value.deleted_record_counts())
                },
            });
    f(Some(FfiCrc {
        version: value.version,
        metadata: metadata.as_ffi(),
        protocol: protocol.as_ffi(),
        file_stats_state,
        in_commit_timestamp: value.in_commit_timestamp_opt.into(),
        set_transaction_state: FfiSetTransactionState {
            kind: transaction_kind,
            transactions: unsafe { FfiSlice::new_unsafe(&transactions) },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: domain_kind,
            domain_metadata: unsafe { FfiSlice::new_unsafe(&domains) },
        },
        txn_id: value
            .txn_id()
            .map(|value| kernel_string_slice!(value))
            .into(),
        all_files: adds
            .as_deref()
            .map(|values| unsafe { FfiSlice::new_unsafe(values) })
            .into(),
        num_deleted_records: value.num_deleted_records().into(),
        num_deletion_vectors: value.num_deletion_vectors().into(),
        deleted_record_counts_histogram: optional_pointer(deleted_histogram.as_ref()),
    }))
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
