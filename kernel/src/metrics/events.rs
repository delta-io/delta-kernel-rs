//! Metric event types and utilities.

use std::fmt;
use std::time::Duration;
use uuid::Uuid;

/// Unique identifier for a metrics operation.
///
/// Each operation (Snapshot, Transaction, Scan) gets a unique MetricId that
/// is used to correlate all events from that operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricId(Uuid);

impl MetricId {
    /// Generate a new unique MetricId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MetricId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MetricId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metric events emitted during Delta Kernel operations.
///
/// Some events include an `operation_id` (MetricId) that uniquely identifies the operation
/// instance. This allows correlating multiple events from the same operation.
#[derive(Debug, Clone)]
pub enum MetricEvent {
    /// Log segment loading completed (listing and organizing log files).
    LogSegmentLoaded {
        operation_id: MetricId,
        duration: Duration,
        num_commit_files: u64,
        num_checkpoint_files: u64,
        num_compaction_files: u64,
    },

    /// Protocol and metadata loading completed.
    ProtocolMetadataLoaded {
        operation_id: MetricId,
        duration: Duration,
    },

    /// Snapshot creation completed successfully.
    SnapshotCompleted {
        operation_id: MetricId,
        version: u64,
        total_duration: Duration,
    },

    /// Snapshot creation failed.
    SnapshotFailed {
        operation_id: MetricId,
        duration: Duration,
    },

    /// Storage list operation completed.
    /// These events track storage-level latencies and are emitted automatically
    /// by the default storage handler implementation.
    StorageListCompleted { duration: Duration, num_files: u64 },

    /// Storage read operation completed.
    StorageReadCompleted {
        duration: Duration,
        num_files: u64,
        bytes_read: u64,
    },

    /// Storage copy operation completed.
    StorageCopyCompleted { duration: Duration },

    /// Scan metadata iteration completed.
    ///
    /// Emitted when the scan metadata iterator is exhausted. This event captures metrics about the
    /// log replay process, including file counts and timing information.
    ScanMetadataCompleted {
        /// Unique ID to correlate this scan with other events.
        operation_id: MetricId,
        /// Total duration from scan start to iterator exhaustion.
        total_duration: Duration,
        /// Add files that entered the deduplication visitor. This excludes files filtered by
        /// data skipping before deduplication. For the total number of add actions in the log,
        /// this value plus `num_predicate_filtered` gives a closer approximation.
        num_add_files_seen: u64,
        /// Add files that survived log replay (files to read).
        num_active_add_files: u64,
        /// Remove files seen (from delta/commit files only).
        num_remove_files_seen: u64,
        /// Non-file actions seen (protocol, metadata, etc.).
        num_non_file_actions: u64,
        /// Files filtered by predicates (data skipping + partition pruning).
        num_predicate_filtered: u64,
        /// Peak size of the deduplication hash set.
        peak_hash_set_size: usize,
        /// Time spent in the deduplication visitor (milliseconds).
        dedup_visitor_time_ms: u64,
        /// Time spent evaluating predicates (milliseconds).
        predicate_eval_time_ms: u64,
    },
}

impl fmt::Display for MetricEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricEvent::LogSegmentLoaded {
                operation_id,
                duration,
                num_commit_files,
                num_checkpoint_files,
                num_compaction_files,
            } => write!(
                f,
                "LogSegmentLoaded(id={operation_id}, duration={duration:?}, commits={num_commit_files}, checkpoints={num_checkpoint_files}, compactions={num_compaction_files})"
            ),
            MetricEvent::ProtocolMetadataLoaded {
                operation_id,
                duration,
            } => write!(
                f,
                "ProtocolMetadataLoaded(id={operation_id}, duration={duration:?})"
            ),
            MetricEvent::SnapshotCompleted {
                operation_id,
                version,
                total_duration,
            } => write!(
                f,
                "SnapshotCompleted(id={operation_id}, version={version}, duration={total_duration:?})"
            ),
            MetricEvent::SnapshotFailed {
                operation_id,
                duration,
            } => write!(
                f,
                "SnapshotFailed(id={operation_id}, duration={duration:?})"
            ),
            MetricEvent::StorageListCompleted {
                duration,
                num_files,
            } => write!(
                f,
                "StorageListCompleted(duration={duration:?}, files={num_files})"
            ),
            MetricEvent::StorageReadCompleted {
                duration,
                num_files,
                bytes_read,
            } => write!(
                f,
                "StorageReadCompleted(duration={duration:?}, files={num_files}, bytes={bytes_read})"
            ),
            MetricEvent::StorageCopyCompleted { duration } => write!(
                f,
                "StorageCopyCompleted(duration={duration:?})"
            ),
            MetricEvent::ScanMetadataCompleted {
                operation_id,
                total_duration,
                num_add_files_seen,
                num_active_add_files,
                num_remove_files_seen,
                num_non_file_actions,
                num_predicate_filtered,
                peak_hash_set_size,
                dedup_visitor_time_ms,
                predicate_eval_time_ms,
            } => write!(
                f,
                "ScanMetadataCompleted(id={operation_id}, duration={total_duration:?}, \
                 add_files_seen={num_add_files_seen}, active_add_files={num_active_add_files}, \
                 remove_files_seen={num_remove_files_seen}, non_file_actions={num_non_file_actions}, \
                 predicate_filtered={num_predicate_filtered}, peak_hash_set_size={peak_hash_set_size}, \
                 dedup_visitor_time_ms={dedup_visitor_time_ms}, predicate_eval_time_ms={predicate_eval_time_ms})"
            ),
        }
    }
}
