//! FFI-safe representations of kernel [`MetricEvent`]s.
//!
//! [`MetricEvent`] is a `repr(C)` version of [`delta_kernel::metrics::MetricEvent`], to allow
//! engines to receive structured metrics across the FFI boundary. To enable metrics reporting call
//! [`crate::ffi_tracing::enable_metrics_reporting`].
//!
//! - Durations are reported as `u64`, either nanoseconds or milliseconds, indicated by an `ns` or
//!   `ms` suffix
//! - Operation ids are UUIDs in kernel (see [`MetricId`]). Here we report the raw 16 bytes of the
//!   UUID.

use delta_kernel::metrics as kernel;

use crate::{kernel_string_slice, KernelStringSlice};

/// The 16 raw bytes of an operation's UUID, used to correlate events from the same operation.
#[repr(C)]
pub struct MetricId {
    pub bytes: [u8; 16],
}

impl From<kernel::MetricId> for MetricId {
    fn from(id: kernel::MetricId) -> Self {
        Self {
            bytes: id.as_bytes(),
        }
    }
}

// we add cbindgen:prefix-with-name=true below otherwise we get name conflicts. For example
// CommitFailureReason::Error would become just `Error`, which conflicts with the type in
// error.rs. Not all variants cause conflicts, but since they refer to common kernel concepts we
// prefix all enums to avoid future issues.

/// Which scan execution path produced a [`ScanMetadataCompleted`] event.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum ScanType {
    SequentialPhase,
    ParallelPhase,
    Full,
}

impl From<kernel::ScanType> for ScanType {
    fn from(t: kernel::ScanType) -> Self {
        match t {
            kernel::ScanType::SequentialPhase => Self::SequentialPhase,
            kernel::ScanType::ParallelPhase => Self::ParallelPhase,
            kernel::ScanType::Full => Self::Full,
        }
    }
}

/// Why a transaction commit did not succeed.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum CommitFailureReason {
    Conflict,
    RetryableIo,
    Error,
}

impl From<kernel::CommitFailureReason> for CommitFailureReason {
    fn from(r: kernel::CommitFailureReason) -> Self {
        match r {
            kernel::CommitFailureReason::Conflict => Self::Conflict,
            kernel::CommitFailureReason::RetryableIo => Self::RetryableIo,
            kernel::CommitFailureReason::Error => Self::Error,
        }
    }
}

/// Whether a table is path-based or catalog-managed.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum TableType {
    PathBased,
    CatalogManaged,
}

impl From<kernel::TableType> for TableType {
    fn from(t: kernel::TableType) -> Self {
        match t {
            kernel::TableType::PathBased => Self::PathBased,
            kernel::TableType::CatalogManaged => Self::CatalogManaged,
        }
    }
}

/// A log segment was loaded.
#[repr(C)]
pub struct LogSegmentLoadSuccess {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
    pub duration_ns: u64,
    pub num_commit_files: u64,
    pub num_checkpoint_files: u64,
    pub num_compaction_files: u64,
    /// Whether the segment had a CRC file. When false, `crc_versions_behind` is 0 and meaningless.
    pub has_crc: bool,
    /// Versions the latest CRC file is behind the loaded version (0 when at the loaded version).
    /// Only meaningful when `has_crc` is true.
    pub crc_versions_behind: u64,
}

/// Listing the log segment failed.
#[repr(C)]
pub struct LogSegmentLoadFailure {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
}

/// How a snapshot load resolved Protocol and Metadata.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum ProtocolMetadataSource {
    CrcAtTarget,
    CrcAdvancedByReplay,
    CrcSeededPmOnlyReplay,
    FullReplay,
    Unknown,
}

impl From<kernel::ProtocolMetadataSource> for ProtocolMetadataSource {
    fn from(s: kernel::ProtocolMetadataSource) -> Self {
        match s {
            kernel::ProtocolMetadataSource::CrcAtTarget => Self::CrcAtTarget,
            kernel::ProtocolMetadataSource::CrcAdvancedByReplay => Self::CrcAdvancedByReplay,
            kernel::ProtocolMetadataSource::CrcSeededPmOnlyReplay => Self::CrcSeededPmOnlyReplay,
            kernel::ProtocolMetadataSource::FullReplay => Self::FullReplay,
            _ => Self::Unknown,
        }
    }
}

/// Protocol and metadata actions were read from the log.
#[repr(C)]
pub struct ProtocolMetadataLoadSuccess {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
    pub source: ProtocolMetadataSource,
    pub duration_ns: u64,
}

/// Reading protocol and metadata from the log failed.
#[repr(C)]
pub struct ProtocolMetadataLoadFailure {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
}

/// A snapshot was built successfully.
#[repr(C)]
pub struct SnapshotBuildSuccess {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
    pub version: u64,
    pub duration_ns: u64,
}

/// Building a snapshot failed.
#[repr(C)]
pub struct SnapshotBuildFailure {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
}

/// A transaction was committed successfully. `operation` and `correlation_id` are slices into
/// engine-supplied data that are only valid for the duration of the callback; an empty slice means
/// the value was unset.
#[repr(C)]
pub struct TransactionCommitSuccess {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
    pub commit_version: u64,
    pub num_add_files: u64,
    pub num_remove_files: u64,
    pub num_dv_updates: u64,
    pub add_files_bytes: u64,
    pub remove_files_bytes: u64,
    pub is_blind_append: bool,
    pub data_change: bool,
    pub operation: KernelStringSlice,
    pub prepare_duration_ns: u64,
    pub committer_duration_ns: u64,
    pub total_duration_ns: u64,
}

/// A transaction commit did not succeed; `reason` distinguishes conflict, retryable IO, and
/// terminal errors.
#[repr(C)]
pub struct TransactionCommitFailure {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
    pub reason: CommitFailureReason,
}

/// A domain metadata load completed.
#[repr(C)]
pub struct DomainMetadataLoadSuccess {
    pub from_cache: bool,
    pub num_domains_returned: u64,
    pub duration_ns: u64,
}

/// A `SetTransaction` (app id) load completed.
#[repr(C)]
pub struct SetTransactionLoadSuccess {
    pub from_cache: bool,
    pub found: bool,
    pub duration_ns: u64,
}

/// A CRC file was read and parsed successfully.
#[repr(C)]
pub struct CrcReadSuccess {
    pub bytes_read: u64,
    pub duration_ns: u64,
}

/// A `JsonHandler::read_json_files` call completed.
#[repr(C)]
pub struct JsonReadCompleted {
    pub num_files: u64,
    pub bytes_read: u64,
}

/// A `ParquetHandler::read_parquet_files` call completed.
#[repr(C)]
pub struct ParquetReadCompleted {
    pub num_files: u64,
    pub bytes_read: u64,
}

/// A scan metadata operation completed. See [`delta_kernel::metrics::ScanMetadataCompleted`] for
/// field semantics.
#[repr(C)]
pub struct ScanMetadataCompleted {
    pub operation_id: MetricId,
    pub correlation_id: KernelStringSlice,
    pub table_type: TableType,
    pub scan_type: ScanType,
    pub duration_ns: u64,
    pub num_add_files_seen: u64,
    pub num_active_add_files: u64,
    pub active_add_files_bytes: u64,
    pub num_remove_files_seen: u64,
    pub num_non_file_actions: u64,
    pub num_predicate_filtered: u64,
    pub peak_hash_set_size: u64,
    pub dedup_visitor_time_ns: u64,
    pub predicate_eval_time_ns: u64,
}

/// A storage list operation completed.
#[repr(C)]
pub struct StorageListCompleted {
    pub duration_ns: u64,
    pub num_files: u64,
}

/// A storage read operation completed.
#[repr(C)]
pub struct StorageReadCompleted {
    pub duration_ns: u64,
    pub num_files: u64,
    pub bytes_read: u64,
}

/// A storage copy or rename operation completed.
#[repr(C)]
pub struct StorageCopyCompleted {
    pub duration_ns: u64,
}

/// C version of [`delta_kernel::metrics::MetricEvent`]. Passed by value to a callback registered
/// via [`crate::ffi_tracing::enable_metrics_reporting`].
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum MetricEvent {
    LogSegmentLoadSuccess(LogSegmentLoadSuccess),
    LogSegmentLoadFailure(LogSegmentLoadFailure),
    ProtocolMetadataLoadSuccess(ProtocolMetadataLoadSuccess),
    ProtocolMetadataLoadFailure(ProtocolMetadataLoadFailure),
    SnapshotBuildSuccess(SnapshotBuildSuccess),
    SnapshotBuildFailure(SnapshotBuildFailure),
    TransactionCommitSuccess(TransactionCommitSuccess),
    TransactionCommitFailure(TransactionCommitFailure),
    DomainMetadataLoadSuccess(DomainMetadataLoadSuccess),
    DomainMetadataLoadFailure,
    SetTransactionLoadSuccess(SetTransactionLoadSuccess),
    SetTransactionLoadFailure,
    CrcReadSuccess(CrcReadSuccess),
    CrcReadFailure,
    JsonReadCompleted(JsonReadCompleted),
    ParquetReadCompleted(ParquetReadCompleted),
    ScanMetadataCompleted(ScanMetadataCompleted),
    StorageListCompleted(StorageListCompleted),
    StorageReadCompleted(StorageReadCompleted),
    StorageCopyCompleted(StorageCopyCompleted),
}

/// Borrow the caller-supplied correlation id as a [`KernelStringSlice`], or an empty slice when
/// unset. The slice is valid only as long as the source kernel event is alive.
fn correlation_id_slice(correlation_id: Option<&str>) -> KernelStringSlice {
    match correlation_id {
        // Safety: the borrowed str lives as long as the source event, which outlives both the FFI
        // event and the callback it is passed to.
        Some(id) => unsafe { KernelStringSlice::new_unsafe(id) },
        None => KernelStringSlice::empty(),
    }
}

impl MetricEvent {
    /// Build an FFI event from a kernel event. Note that this event is only valid as long as the
    /// passed argument is still alive, since we might make `KernelStringSlices` out of some
    /// fields.
    pub(crate) fn from_kernel(event: &kernel::MetricEvent) -> Self {
        use kernel::MetricEvent as K;
        let ns = |d: std::time::Duration| d.as_nanos() as u64;
        match event {
            K::LogSegmentLoadSuccess(kernel::LogSegmentLoadSuccess {
                operation_id,
                correlation_id,
                table_type,
                num_commit_files,
                num_checkpoint_files,
                num_compaction_files,
                crc_versions_behind,
                duration,
                ..
            }) => Self::LogSegmentLoadSuccess(LogSegmentLoadSuccess {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
                duration_ns: ns(*duration),
                num_commit_files: *num_commit_files,
                num_checkpoint_files: *num_checkpoint_files,
                num_compaction_files: *num_compaction_files,
                // C has no Option: `has_crc` gates `crc_versions_behind` (0 when absent).
                has_crc: crc_versions_behind.is_some(),
                crc_versions_behind: crc_versions_behind.unwrap_or(0),
            }),
            K::LogSegmentLoadFailure(kernel::LogSegmentLoadFailure {
                operation_id,
                correlation_id,
                table_type,
                ..
            }) => Self::LogSegmentLoadFailure(LogSegmentLoadFailure {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
            }),
            K::ProtocolMetadataLoadSuccess(kernel::ProtocolMetadataLoadSuccess {
                operation_id,
                correlation_id,
                table_type,
                source,
                duration,
                ..
            }) => Self::ProtocolMetadataLoadSuccess(ProtocolMetadataLoadSuccess {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
                source: (*source).into(),
                duration_ns: ns(*duration),
            }),
            K::ProtocolMetadataLoadFailure(kernel::ProtocolMetadataLoadFailure {
                operation_id,
                correlation_id,
                table_type,
                ..
            }) => Self::ProtocolMetadataLoadFailure(ProtocolMetadataLoadFailure {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
            }),
            K::SnapshotBuildSuccess(kernel::SnapshotBuildSuccess {
                operation_id,
                correlation_id,
                table_type,
                version,
                duration,
                ..
            }) => Self::SnapshotBuildSuccess(SnapshotBuildSuccess {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
                version: *version,
                duration_ns: ns(*duration),
            }),
            K::SnapshotBuildFailure(kernel::SnapshotBuildFailure {
                operation_id,
                correlation_id,
                table_type,
                ..
            }) => Self::SnapshotBuildFailure(SnapshotBuildFailure {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
            }),
            K::TransactionCommitSuccess(kernel::TransactionCommitSuccess {
                operation_id,
                correlation_id,
                table_type,
                commit_version,
                num_add_files,
                num_remove_files,
                num_dv_updates,
                add_files_bytes,
                remove_files_bytes,
                is_blind_append,
                data_change,
                operation,
                prepare_duration,
                committer_duration,
                total_duration,
            }) => {
                let operation = match operation.as_ref() {
                    Some(o) => kernel_string_slice!(o),
                    None => KernelStringSlice::empty(),
                };
                Self::TransactionCommitSuccess(TransactionCommitSuccess {
                    operation_id: (*operation_id).into(),
                    correlation_id: correlation_id_slice(correlation_id.as_deref()),
                    table_type: (*table_type).into(),
                    commit_version: *commit_version,
                    num_add_files: *num_add_files,
                    num_remove_files: *num_remove_files,
                    num_dv_updates: *num_dv_updates,
                    add_files_bytes: *add_files_bytes,
                    remove_files_bytes: *remove_files_bytes,
                    is_blind_append: *is_blind_append,
                    data_change: *data_change,
                    operation,
                    prepare_duration_ns: ns(*prepare_duration),
                    committer_duration_ns: ns(*committer_duration),
                    total_duration_ns: ns(*total_duration),
                })
            }
            K::TransactionCommitFailure(kernel::TransactionCommitFailure {
                operation_id,
                correlation_id,
                table_type,
                reason,
            }) => Self::TransactionCommitFailure(TransactionCommitFailure {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
                reason: (*reason).into(),
            }),
            K::DomainMetadataLoadSuccess(kernel::DomainMetadataLoadSuccess {
                from_cache,
                num_domains_returned,
                duration,
            }) => Self::DomainMetadataLoadSuccess(DomainMetadataLoadSuccess {
                from_cache: *from_cache,
                num_domains_returned: *num_domains_returned,
                duration_ns: ns(*duration),
            }),
            K::DomainMetadataLoadFailure => Self::DomainMetadataLoadFailure,
            K::SetTransactionLoadSuccess(kernel::SetTransactionLoadSuccess {
                from_cache,
                found,
                duration,
            }) => Self::SetTransactionLoadSuccess(SetTransactionLoadSuccess {
                from_cache: *from_cache,
                found: *found,
                duration_ns: ns(*duration),
            }),
            K::SetTransactionLoadFailure => Self::SetTransactionLoadFailure,
            K::CrcReadSuccess(kernel::CrcReadSuccess {
                bytes_read,
                duration,
            }) => Self::CrcReadSuccess(CrcReadSuccess {
                bytes_read: *bytes_read,
                duration_ns: ns(*duration),
            }),
            K::CrcReadFailure => Self::CrcReadFailure,
            K::JsonReadCompleted(kernel::JsonReadCompleted {
                num_files,
                bytes_read,
            }) => Self::JsonReadCompleted(JsonReadCompleted {
                num_files: *num_files,
                bytes_read: *bytes_read,
            }),
            K::ParquetReadCompleted(kernel::ParquetReadCompleted {
                num_files,
                bytes_read,
            }) => Self::ParquetReadCompleted(ParquetReadCompleted {
                num_files: *num_files,
                bytes_read: *bytes_read,
            }),
            K::ScanMetadataCompleted(kernel::ScanMetadataCompleted {
                operation_id,
                correlation_id,
                table_type,
                scan_type,
                duration,
                num_add_files_seen,
                num_active_add_files,
                active_add_files_bytes,
                num_remove_files_seen,
                num_non_file_actions,
                num_predicate_filtered,
                peak_hash_set_size,
                dedup_visitor_time,
                predicate_eval_time,
            }) => Self::ScanMetadataCompleted(ScanMetadataCompleted {
                operation_id: (*operation_id).into(),
                correlation_id: correlation_id_slice(correlation_id.as_deref()),
                table_type: (*table_type).into(),
                scan_type: (*scan_type).into(),
                duration_ns: ns(*duration),
                num_add_files_seen: *num_add_files_seen,
                num_active_add_files: *num_active_add_files,
                active_add_files_bytes: *active_add_files_bytes,
                num_remove_files_seen: *num_remove_files_seen,
                num_non_file_actions: *num_non_file_actions,
                num_predicate_filtered: *num_predicate_filtered,
                peak_hash_set_size: *peak_hash_set_size as u64, // note usize -> u64 cast
                dedup_visitor_time_ns: ns(*dedup_visitor_time),
                predicate_eval_time_ns: ns(*predicate_eval_time),
            }),
            K::StorageListCompleted(kernel::StorageListCompleted {
                duration,
                num_files,
            }) => Self::StorageListCompleted(StorageListCompleted {
                duration_ns: ns(*duration),
                num_files: *num_files,
            }),
            K::StorageReadCompleted(kernel::StorageReadCompleted {
                duration,
                num_files,
                bytes_read,
            }) => Self::StorageReadCompleted(StorageReadCompleted {
                duration_ns: ns(*duration),
                num_files: *num_files,
                bytes_read: *bytes_read,
            }),
            K::StorageCopyCompleted(kernel::StorageCopyCompleted { duration }) => {
                Self::StorageCopyCompleted(StorageCopyCompleted {
                    duration_ns: ns(*duration),
                })
            }
        }
    }
}

/// Invoke `f` with the FFI [`MetricEvent`] built from `event`.
pub(crate) fn with_ffi_event<R>(
    event: &kernel::MetricEvent,
    f: impl FnOnce(MetricEvent) -> R,
) -> R {
    f(MetricEvent::from_kernel(event))
}

#[cfg(test)]
mod tests {
    use std::mem::discriminant;
    use std::time::Duration;

    use super::*;
    use crate::TryFromStringSlice;

    #[test]
    fn from_kernel_transaction_commit_success_carries_operation_and_id() {
        let id = kernel::MetricId::new();
        let event =
            kernel::MetricEvent::TransactionCommitSuccess(kernel::TransactionCommitSuccess {
                operation_id: id,
                correlation_id: Some("req-7".into()),
                table_type: kernel::TableType::CatalogManaged,
                commit_version: 7,
                num_add_files: 2,
                num_remove_files: 1,
                num_dv_updates: 4,
                add_files_bytes: 100,
                remove_files_bytes: 50,
                is_blind_append: true,
                data_change: false,
                operation: Some("WRITE".to_string()),
                prepare_duration: Duration::from_nanos(10),
                committer_duration: Duration::from_nanos(20),
                total_duration: Duration::from_nanos(30),
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::TransactionCommitSuccess(e) = ffi else {
                panic!("expected TransactionCommitSuccess");
            };
            assert_eq!(e.operation_id.bytes, id.as_bytes());
            assert!(matches!(e.table_type, TableType::CatalogManaged));
            assert_eq!(e.commit_version, 7);
            assert_eq!(e.num_dv_updates, 4);
            assert_eq!(e.add_files_bytes, 100);
            assert!(e.is_blind_append);
            assert_eq!(e.prepare_duration_ns, 10);
            assert_eq!(e.total_duration_ns, 30);
            let op: &str = unsafe { TryFromStringSlice::try_from_slice(&e.operation).unwrap() };
            assert_eq!(op, "WRITE");
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "req-7");
        });
    }

    #[test]
    fn from_kernel_transaction_commit_success_unset_operation_is_empty_slice() {
        let event =
            kernel::MetricEvent::TransactionCommitSuccess(kernel::TransactionCommitSuccess {
                operation_id: kernel::MetricId::new(),
                correlation_id: None,
                table_type: kernel::TableType::PathBased,
                commit_version: 1,
                num_add_files: 0,
                num_remove_files: 0,
                num_dv_updates: 0,
                add_files_bytes: 0,
                remove_files_bytes: 0,
                is_blind_append: false,
                data_change: true,
                operation: None,
                prepare_duration: Duration::from_nanos(0),
                committer_duration: Duration::from_nanos(0),
                total_duration: Duration::from_nanos(0),
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::TransactionCommitSuccess(e) = ffi else {
                panic!("expected TransactionCommitSuccess");
            };
            assert!(matches!(e.table_type, TableType::PathBased));
            let op: &str = unsafe { TryFromStringSlice::try_from_slice(&e.operation).unwrap() };
            assert_eq!(op, "");
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "");
        });
    }

    #[test]
    fn from_kernel_maps_failure_reason() {
        let id = kernel::MetricId::new();
        let failure =
            kernel::MetricEvent::TransactionCommitFailure(kernel::TransactionCommitFailure {
                operation_id: id,
                correlation_id: Some("req-fail".into()),
                table_type: kernel::TableType::CatalogManaged,
                reason: kernel::CommitFailureReason::Conflict,
            });
        with_ffi_event(&failure, |ffi| {
            let MetricEvent::TransactionCommitFailure(e) = ffi else {
                panic!("expected TransactionCommitFailure");
            };
            assert!(matches!(e.reason, CommitFailureReason::Conflict));
            assert!(matches!(e.table_type, TableType::CatalogManaged));
            assert_eq!(e.operation_id.bytes, id.as_bytes());
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "req-fail");
        });
    }

    #[test]
    fn from_kernel_scan_metadata_completed_maps_all_fields() {
        let id = kernel::MetricId::new();
        let event = kernel::MetricEvent::ScanMetadataCompleted(kernel::ScanMetadataCompleted {
            operation_id: id,
            correlation_id: Some("scan-req".into()),
            table_type: kernel::TableType::CatalogManaged,
            scan_type: kernel::ScanType::ParallelPhase,
            duration: Duration::from_nanos(13),
            num_add_files_seen: 17,
            num_active_add_files: 19,
            active_add_files_bytes: 23,
            num_remove_files_seen: 29,
            num_non_file_actions: 31,
            num_predicate_filtered: 37,
            peak_hash_set_size: 41,
            dedup_visitor_time: Duration::from_nanos(43),
            predicate_eval_time: Duration::from_nanos(47),
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ScanMetadataCompleted(e) = ffi else {
                panic!("expected ScanMetadataCompleted");
            };
            assert_eq!(e.operation_id.bytes, id.as_bytes());
            assert!(matches!(e.table_type, TableType::CatalogManaged));
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "scan-req");
            assert_eq!(
                discriminant(&e.scan_type),
                discriminant(&ScanType::ParallelPhase)
            );
            assert_eq!(e.duration_ns, 13);
            assert_eq!(e.num_add_files_seen, 17);
            assert_eq!(e.num_active_add_files, 19);
            assert_eq!(e.active_add_files_bytes, 23);
            assert_eq!(e.num_remove_files_seen, 29);
            assert_eq!(e.num_non_file_actions, 31);
            assert_eq!(e.num_predicate_filtered, 37);
            assert_eq!(e.peak_hash_set_size, 41);
            assert_eq!(e.dedup_visitor_time_ns, 43);
            assert_eq!(e.predicate_eval_time_ns, 47);
        });
    }

    #[test]
    fn from_kernel_protocol_metadata_load_success_maps_source() {
        let id = kernel::MetricId::new();
        let event =
            kernel::MetricEvent::ProtocolMetadataLoadSuccess(kernel::ProtocolMetadataLoadSuccess {
                operation_id: id,
                correlation_id: Some("pm-req".into()),
                table_type: kernel::TableType::CatalogManaged,
                load_type: kernel::LogSegmentLoadType::Full,
                source: kernel::ProtocolMetadataSource::CrcAdvancedByReplay,
                duration: Duration::from_nanos(53),
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ProtocolMetadataLoadSuccess(e) = ffi else {
                panic!("expected ProtocolMetadataLoadSuccess");
            };
            assert_eq!(e.operation_id.bytes, id.as_bytes());
            assert!(matches!(e.table_type, TableType::CatalogManaged));
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "pm-req");
            assert_eq!(
                discriminant(&e.source),
                discriminant(&ProtocolMetadataSource::CrcAdvancedByReplay)
            );
            assert_eq!(e.duration_ns, 53);
        });
    }

    #[test]
    fn from_kernel_log_segment_load_success_carries_correlation_id_and_table_type() {
        let id = kernel::MetricId::new();
        let event = kernel::MetricEvent::LogSegmentLoadSuccess(kernel::LogSegmentLoadSuccess {
            operation_id: id,
            correlation_id: Some("req-42".into()),
            table_type: kernel::TableType::CatalogManaged,
            load_type: kernel::LogSegmentLoadType::Full,
            num_commit_files: 3,
            num_checkpoint_files: 1,
            num_compaction_files: 2,
            crc_versions_behind: Some(5),
            duration: Duration::from_nanos(61),
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::LogSegmentLoadSuccess(e) = ffi else {
                panic!("expected LogSegmentLoadSuccess");
            };
            assert_eq!(e.operation_id.bytes, id.as_bytes());
            assert!(matches!(e.table_type, TableType::CatalogManaged));
            assert_eq!(e.duration_ns, 61);
            assert_eq!(e.num_commit_files, 3);
            assert_eq!(e.num_checkpoint_files, 1);
            assert_eq!(e.num_compaction_files, 2);
            assert!(e.has_crc);
            assert_eq!(e.crc_versions_behind, 5);
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "req-42");
        });
    }

    #[test]
    fn from_kernel_log_segment_load_success_unset_correlation_id_is_empty_slice() {
        let event = kernel::MetricEvent::LogSegmentLoadSuccess(kernel::LogSegmentLoadSuccess {
            operation_id: kernel::MetricId::new(),
            correlation_id: None,
            table_type: kernel::TableType::PathBased,
            load_type: kernel::LogSegmentLoadType::Full,
            num_commit_files: 0,
            num_checkpoint_files: 0,
            num_compaction_files: 0,
            crc_versions_behind: None,
            duration: Duration::from_nanos(0),
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::LogSegmentLoadSuccess(e) = ffi else {
                panic!("expected LogSegmentLoadSuccess");
            };
            assert!(matches!(e.table_type, TableType::PathBased));
            assert!(!e.has_crc);
            let cid: &str =
                unsafe { TryFromStringSlice::try_from_slice(&e.correlation_id).unwrap() };
            assert_eq!(cid, "");
        });
    }

    #[test]
    fn from_kernel_set_transaction_load_success_distinguishes_bools() {
        // from_cache and found are adjacent bools that are trivial to swap; assert distinct values.
        let event =
            kernel::MetricEvent::SetTransactionLoadSuccess(kernel::SetTransactionLoadSuccess {
                from_cache: true,
                found: false,
                duration: Duration::from_nanos(53),
            });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::SetTransactionLoadSuccess(e) = ffi else {
                panic!("expected SetTransactionLoadSuccess");
            };
            assert!(e.from_cache);
            assert!(!e.found);
            assert_eq!(e.duration_ns, 53);
        });
    }
}
