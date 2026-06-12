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

/// A log segment was loaded.
#[repr(C)]
pub struct LogSegmentLoadSuccess {
    pub operation_id: MetricId,
    pub duration_ns: u64,
    pub num_commit_files: u64,
    pub num_checkpoint_files: u64,
    pub num_compaction_files: u64,
    pub has_latest_crc_file: bool,
}

/// Listing the log segment failed.
#[repr(C)]
pub struct LogSegmentLoadFailure {
    pub operation_id: MetricId,
}

/// Protocol and metadata actions were read from the log.
#[repr(C)]
pub struct ProtocolMetadataLoadSuccess {
    pub operation_id: MetricId,
    pub duration_ns: u64,
}

/// Reading protocol and metadata from the log failed.
#[repr(C)]
pub struct ProtocolMetadataLoadFailure {
    pub operation_id: MetricId,
}

/// A snapshot was built successfully.
#[repr(C)]
pub struct SnapshotBuildSuccess {
    pub operation_id: MetricId,
    pub version: u64,
    pub duration_ns: u64,
}

/// Building a snapshot failed.
#[repr(C)]
pub struct SnapshotBuildFailure {
    pub operation_id: MetricId,
}

/// A transaction was committed successfully. `operation` is a slice into engine-supplied data that
/// is only valid for the duration of the callback; an empty slice means it was unset.
#[repr(C)]
pub struct TransactionCommitSuccess {
    pub operation_id: MetricId,
    pub commit_version: u64,
    pub num_add_files: u64,
    pub num_remove_files: u64,
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
    pub scan_type: ScanType,
    pub duration_ns: u64,
    pub num_add_files_seen: u64,
    pub num_active_add_files: u64,
    pub active_add_files_bytes: u64,
    pub num_remove_files_seen: u64,
    pub num_non_file_actions: u64,
    pub num_predicate_filtered: u64,
    pub peak_hash_set_size: u64,
    pub dedup_visitor_time_ms: u64,
    pub predicate_eval_time_ms: u64,
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

impl MetricEvent {
    /// Build an FFI event from a kernel event. Note that this event is only valid as long as the
    /// passed argument is still alive, since we might make `KernelStringSlices` out of some
    /// fields.
    pub(crate) fn from_kernel(event: &kernel::MetricEvent) -> Self {
        use kernel::MetricEvent as K;
        let ns = |d: std::time::Duration| d.as_nanos() as u64;
        match event {
            K::LogSegmentLoadSuccess(e) => Self::LogSegmentLoadSuccess(LogSegmentLoadSuccess {
                operation_id: e.operation_id.into(),
                duration_ns: ns(e.duration),
                num_commit_files: e.num_commit_files,
                num_checkpoint_files: e.num_checkpoint_files,
                num_compaction_files: e.num_compaction_files,
                has_latest_crc_file: e.has_latest_crc_file,
            }),
            K::LogSegmentLoadFailure(e) => Self::LogSegmentLoadFailure(LogSegmentLoadFailure {
                operation_id: e.operation_id.into(),
            }),
            K::ProtocolMetadataLoadSuccess(e) => {
                Self::ProtocolMetadataLoadSuccess(ProtocolMetadataLoadSuccess {
                    operation_id: e.operation_id.into(),
                    duration_ns: ns(e.duration),
                })
            }
            K::ProtocolMetadataLoadFailure(e) => {
                Self::ProtocolMetadataLoadFailure(ProtocolMetadataLoadFailure {
                    operation_id: e.operation_id.into(),
                })
            }
            K::SnapshotBuildSuccess(e) => Self::SnapshotBuildSuccess(SnapshotBuildSuccess {
                operation_id: e.operation_id.into(),
                version: e.version,
                duration_ns: ns(e.duration),
            }),
            K::SnapshotBuildFailure(e) => Self::SnapshotBuildFailure(SnapshotBuildFailure {
                operation_id: e.operation_id.into(),
            }),
            K::TransactionCommitSuccess(e) => {
                let operation = match e.operation.as_ref() {
                    Some(o) => kernel_string_slice!(o),
                    None => KernelStringSlice::empty(),
                };
                Self::TransactionCommitSuccess(TransactionCommitSuccess {
                    operation_id: e.operation_id.into(),
                    commit_version: e.commit_version,
                    num_add_files: e.num_add_files,
                    num_remove_files: e.num_remove_files,
                    add_files_bytes: e.add_files_bytes,
                    remove_files_bytes: e.remove_files_bytes,
                    is_blind_append: e.is_blind_append,
                    data_change: e.data_change,
                    operation,
                    prepare_duration_ns: ns(e.prepare_duration),
                    committer_duration_ns: ns(e.committer_duration),
                    total_duration_ns: ns(e.total_duration),
                })
            }
            K::TransactionCommitFailure(e) => {
                Self::TransactionCommitFailure(TransactionCommitFailure {
                    operation_id: e.operation_id.into(),
                    reason: e.reason.into(),
                })
            }
            K::DomainMetadataLoadSuccess(e) => {
                Self::DomainMetadataLoadSuccess(DomainMetadataLoadSuccess {
                    from_cache: e.from_cache,
                    num_domains_returned: e.num_domains_returned,
                    duration_ns: ns(e.duration),
                })
            }
            K::DomainMetadataLoadFailure => Self::DomainMetadataLoadFailure,
            K::SetTransactionLoadSuccess(e) => {
                Self::SetTransactionLoadSuccess(SetTransactionLoadSuccess {
                    from_cache: e.from_cache,
                    found: e.found,
                    duration_ns: ns(e.duration),
                })
            }
            K::SetTransactionLoadFailure => Self::SetTransactionLoadFailure,
            K::CrcReadSuccess(e) => Self::CrcReadSuccess(CrcReadSuccess {
                bytes_read: e.bytes_read,
                duration_ns: ns(e.duration),
            }),
            K::CrcReadFailure => Self::CrcReadFailure,
            K::JsonReadCompleted(e) => Self::JsonReadCompleted(JsonReadCompleted {
                num_files: e.num_files,
                bytes_read: e.bytes_read,
            }),
            K::ParquetReadCompleted(e) => Self::ParquetReadCompleted(ParquetReadCompleted {
                num_files: e.num_files,
                bytes_read: e.bytes_read,
            }),
            K::ScanMetadataCompleted(e) => Self::ScanMetadataCompleted(ScanMetadataCompleted {
                operation_id: e.operation_id.into(),
                scan_type: e.scan_type.into(),
                duration_ns: ns(e.duration),
                num_add_files_seen: e.num_add_files_seen,
                num_active_add_files: e.num_active_add_files,
                active_add_files_bytes: e.active_add_files_bytes,
                num_remove_files_seen: e.num_remove_files_seen,
                num_non_file_actions: e.num_non_file_actions,
                num_predicate_filtered: e.num_predicate_filtered,
                peak_hash_set_size: e.peak_hash_set_size as u64, // note usize -> u64 cast
                dedup_visitor_time_ms: e.dedup_visitor_time_ms,
                predicate_eval_time_ms: e.predicate_eval_time_ms,
            }),
            K::StorageListCompleted(e) => Self::StorageListCompleted(StorageListCompleted {
                duration_ns: ns(e.duration),
                num_files: e.num_files,
            }),
            K::StorageReadCompleted(e) => Self::StorageReadCompleted(StorageReadCompleted {
                duration_ns: ns(e.duration),
                num_files: e.num_files,
                bytes_read: e.bytes_read,
            }),
            K::StorageCopyCompleted(e) => Self::StorageCopyCompleted(StorageCopyCompleted {
                duration_ns: ns(e.duration),
            }),
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
                commit_version: 7,
                num_add_files: 2,
                num_remove_files: 1,
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
            assert_eq!(e.commit_version, 7);
            assert_eq!(e.add_files_bytes, 100);
            assert!(e.is_blind_append);
            assert_eq!(e.prepare_duration_ns, 10);
            assert_eq!(e.total_duration_ns, 30);
            let op: &str = unsafe { TryFromStringSlice::try_from_slice(&e.operation).unwrap() };
            assert_eq!(op, "WRITE");
        });
    }

    #[test]
    fn from_kernel_transaction_commit_success_unset_operation_is_empty_slice() {
        let event =
            kernel::MetricEvent::TransactionCommitSuccess(kernel::TransactionCommitSuccess {
                operation_id: kernel::MetricId::new(),
                commit_version: 1,
                num_add_files: 0,
                num_remove_files: 0,
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
            let op: &str = unsafe { TryFromStringSlice::try_from_slice(&e.operation).unwrap() };
            assert_eq!(op, "");
        });
    }

    #[test]
    fn from_kernel_maps_failure_reason() {
        let id = kernel::MetricId::new();
        let failure =
            kernel::MetricEvent::TransactionCommitFailure(kernel::TransactionCommitFailure {
                operation_id: id,
                reason: kernel::CommitFailureReason::Conflict,
            });
        with_ffi_event(&failure, |ffi| {
            let MetricEvent::TransactionCommitFailure(e) = ffi else {
                panic!("expected TransactionCommitFailure");
            };
            assert!(matches!(e.reason, CommitFailureReason::Conflict));
            assert_eq!(e.operation_id.bytes, id.as_bytes());
        });
    }

    #[test]
    fn from_kernel_scan_metadata_completed_maps_all_fields() {
        let id = kernel::MetricId::new();
        let event = kernel::MetricEvent::ScanMetadataCompleted(kernel::ScanMetadataCompleted {
            operation_id: id,
            scan_type: kernel::ScanType::ParallelPhase,
            duration: Duration::from_nanos(13),
            num_add_files_seen: 17,
            num_active_add_files: 19,
            active_add_files_bytes: 23,
            num_remove_files_seen: 29,
            num_non_file_actions: 31,
            num_predicate_filtered: 37,
            peak_hash_set_size: 41,
            dedup_visitor_time_ms: 43,
            predicate_eval_time_ms: 47,
        });
        with_ffi_event(&event, |ffi| {
            let MetricEvent::ScanMetadataCompleted(e) = ffi else {
                panic!("expected ScanMetadataCompleted");
            };
            assert_eq!(e.operation_id.bytes, id.as_bytes());
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
            assert_eq!(e.dedup_visitor_time_ms, 43);
            assert_eq!(e.predicate_eval_time_ms, 47);
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
