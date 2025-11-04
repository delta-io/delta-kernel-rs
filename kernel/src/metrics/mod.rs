//! Metrics collection for Delta Kernel operations.
//!
//! This module provides metrics tracking for various Delta operations including
//! snapshot creation, scans, and transactions. Metrics are collected during operations
//! and reported as events via the `MetricsReporter` trait.
//!
//! Each operation (Snapshot, Transaction, Scan) is assigned a unique operation ID ([`MetricId`])
//! when it starts, and all subsequent events for that operation reference this ID.
//! This allows reporters to correlate events and track operation lifecycles.
//!
//! # Example: Implementing a Custom MetricsReporter
//!
//! ```
//! use std::sync::Arc;
//! use delta_kernel::metrics::{MetricsReporter, MetricEvent};
//!
//! #[derive(Debug)]
//! struct LoggingReporter;
//!
//! impl MetricsReporter for LoggingReporter {
//!     fn report(&self, event: MetricEvent) {
//!         match event {
//!             MetricEvent::SnapshotStarted { operation_id, table_path } => {
//!                 println!("Snapshot started: {} for table {}", operation_id, table_path);
//!             }
//!             MetricEvent::LogSegmentLoaded { operation_id, duration, num_commit_files, .. } => {
//!                 println!("  Log segment loaded in {:?}: {} commits", duration, num_commit_files);
//!             }
//!             MetricEvent::SnapshotCompleted { operation_id, version, total_duration } => {
//!                 println!("Snapshot completed: v{} in {:?}", version, total_duration);
//!             }
//!             MetricEvent::SnapshotFailed { operation_id } => {
//!                 println!("Snapshot failed: {}", operation_id);
//!             }
//!             _ => {}
//!         }
//!     }
//! }
//! ```
//!
//! # Example: Implementing a Composite Reporter
//!
//! If you need to send metrics to multiple destinations, you can create a composite reporter:
//!
//! ```
//! use std::sync::Arc;
//! use delta_kernel::metrics::{MetricsReporter, MetricEvent};
//!
//! #[derive(Debug)]
//! struct CompositeReporter {
//!     reporters: Vec<Arc<dyn MetricsReporter>>,
//! }
//!
//! impl MetricsReporter for CompositeReporter {
//!     fn report(&self, event: MetricEvent) {
//!         for reporter in &self.reporters {
//!             reporter.report(event.clone());
//!         }
//!     }
//! }
//! ```
//!
//! # Storage Metrics
//!
//! Storage operations (list, read, copy) are automatically instrumented when using
//! `DefaultEngine` with a metrics reporter. The default storage handler implementation
//! emits `StorageListCompleted`, `StorageReadCompleted`, and `StorageCopyCompleted`
//! events that track latencies at the storage layer.
//!
//! These metrics are standalone and track aggregate storage performance without
//! correlating to specific Snapshot/Transaction operations.

use std::fmt;
use std::time::{Duration, Instant};
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

/// Trait for reporting metrics events from Delta operations.
///
/// Implementations of this trait receive metric events as they occur during operations
/// and can forward them to monitoring systems like Prometheus, DataDog, etc.
///
/// Events are emitted throughout an operation's lifecycle, allowing real-time monitoring.
pub trait MetricsReporter: Send + Sync + std::fmt::Debug {
    /// Report a metric event.
    fn report(&self, event: MetricEvent);
}

/// Metric events emitted during Delta Kernel operations.
///
/// Some events include an `operation_id` (MetricId) that uniquely identifies the operation
/// instance. This allows correlating multiple events from the same operation.
#[derive(Debug, Clone)]
pub enum MetricEvent {
    /// A snapshot creation operation has started.
    SnapshotStarted {
        operation_id: MetricId,
        table_path: String,
    },

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
    SnapshotFailed { operation_id: MetricId },

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
            MetricEvent::SnapshotStarted {
                operation_id,
                table_path,
            } => write!(
                f,
                "SnapshotStarted(id={}, table={})",
                operation_id, table_path
            ),
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
            MetricEvent::SnapshotFailed { operation_id } => {
                write!(f, "SnapshotFailed(id={})", operation_id)
            }
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

/// A simple timer for tracking operation durations.
///
/// # Example
/// ```
/// use delta_kernel::metrics::Timer;
///
/// let timer = Timer::new();
/// // ... do work ...
/// let duration = timer.elapsed();
/// ```
#[derive(Debug)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Create a new timer that starts immediately.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get the elapsed time as a Duration since this timer was created.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}
