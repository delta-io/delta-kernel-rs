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
                "LogSegmentLoaded(id={}, duration={:?}, commits={}, checkpoints={}, compactions={})",
                operation_id, duration, num_commit_files, num_checkpoint_files, num_compaction_files
            ),
            MetricEvent::ProtocolMetadataLoaded {
                operation_id,
                duration,
            } => write!(
                f,
                "ProtocolMetadataLoaded(id={}, duration={:?})",
                operation_id, duration
            ),
            MetricEvent::SnapshotCompleted {
                operation_id,
                version,
                total_duration,
            } => write!(
                f,
                "SnapshotCompleted(id={}, version={}, duration={:?})",
                operation_id, version, total_duration
            ),
            MetricEvent::SnapshotFailed {
                operation_id,
                duration,
            } => write!(
                f,
                "SnapshotFailed(id={}, duration={:?})",
                operation_id, duration
            ),
            MetricEvent::StorageListCompleted {
                duration,
                num_files,
            } => write!(
                f,
                "StorageListCompleted(duration={:?}, files={})",
                duration, num_files
            ),
            MetricEvent::StorageReadCompleted {
                duration,
                num_files,
                bytes_read,
            } => write!(
                f,
                "StorageReadCompleted(duration={:?}, files={}, bytes={})",
                duration, num_files, bytes_read
            ),
            MetricEvent::StorageCopyCompleted { duration } => write!(
                f,
                "StorageCopyCompleted(duration={:?})",
                duration
            ),
        }
    }
}
