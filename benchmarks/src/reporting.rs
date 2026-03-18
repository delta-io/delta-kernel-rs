//! Benchmark reporting utilities for collecting and displaying operation metrics.
//!
//! [`CountingReporter`] implements [`MetricsReporter`] using atomic counters to accumulate
//! storage-level and operation-level metrics. Attach it to a `DefaultEngine` via
//! `DefaultEngineBuilder::with_metrics_reporter`, then after each Criterion timing pass:
//!
//! 1. Call [`CountingReporter::reset`] to zero all counters.
//! 2. Call `runner.execute()` once to collect a single-iteration sample.
//! 3. Call [`CountingReporter::print_summary`] to display the IO profile.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use delta_kernel::metrics::{MetricEvent, MetricsReporter};

/// Accumulates storage and operation metrics via the [`MetricsReporter`] interface.
///
/// All counters use [`Ordering::Relaxed`] -- sufficient here since there are no
/// ordering dependencies between counters.
#[derive(Debug, Default)]
pub struct CountingReporter {
    // Storage-layer IO counters
    /// Number of `list_from` calls (one per [`MetricEvent::StorageListCompleted`]).
    pub list_calls: AtomicU64,
    /// Total files returned across all list calls.
    pub list_files_seen: AtomicU64,
    /// Number of `read_files` calls (one per [`MetricEvent::StorageReadCompleted`]).
    pub read_calls: AtomicU64,
    /// Total individual files read across all read calls.
    pub read_files: AtomicU64,
    /// Total bytes consumed across all read calls.
    pub bytes_read: AtomicU64,
    /// Number of `copy_atomic` calls (one per [`MetricEvent::StorageCopyCompleted`]).
    pub copy_calls: AtomicU64,

    // Operation-level counters
    /// Number of completed snapshot constructions.
    pub snapshot_completions: AtomicU64,
    /// Number of log segment loads (listing + organizing log files).
    pub log_segment_loads: AtomicU64,
    /// Total commit files seen across all log segment loads.
    pub commit_files: AtomicU64,
    /// Total checkpoint files seen across all log segment loads.
    pub checkpoint_files: AtomicU64,
    /// Total log compaction files seen across all log segment loads.
    pub compaction_files: AtomicU64,
}

impl CountingReporter {
    /// Create a new reporter with all counters at zero.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reset all counters to zero.
    ///
    /// Call this immediately before a single profiling iteration to get per-call counts.
    pub fn reset(&self) {
        self.list_calls.store(0, Ordering::Relaxed);
        self.list_files_seen.store(0, Ordering::Relaxed);
        self.read_calls.store(0, Ordering::Relaxed);
        self.read_files.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.copy_calls.store(0, Ordering::Relaxed);
        self.snapshot_completions.store(0, Ordering::Relaxed);
        self.log_segment_loads.store(0, Ordering::Relaxed);
        self.commit_files.store(0, Ordering::Relaxed);
        self.checkpoint_files.store(0, Ordering::Relaxed);
        self.compaction_files.store(0, Ordering::Relaxed);
    }

    /// Print a human-readable IO and operation summary for one profiling iteration.
    ///
    /// Intended to be called after [`reset`][Self::reset] and one `execute()` call,
    /// so values reflect a single operation's cost.
    pub fn print_summary(&self, label: &str) {
        let list_calls = self.list_calls.load(Ordering::Relaxed);
        let list_files = self.list_files_seen.load(Ordering::Relaxed);
        let read_calls = self.read_calls.load(Ordering::Relaxed);
        let read_files = self.read_files.load(Ordering::Relaxed);
        let bytes_read = self.bytes_read.load(Ordering::Relaxed);
        let log_loads = self.log_segment_loads.load(Ordering::Relaxed);
        let commits = self.commit_files.load(Ordering::Relaxed);
        let checkpoints = self.checkpoint_files.load(Ordering::Relaxed);
        let compactions = self.compaction_files.load(Ordering::Relaxed);

        println!(
            "  [io] {label}\n\
             \x20       storage  : {list_calls} list ({list_files} files seen)  \
             {read_calls} read ({read_files} files, {} KiB)\n\
             \x20       log replay: {log_loads} segment load(s) -- \
             {commits} commits  {checkpoints} checkpoints  {compactions} compactions",
            bytes_read / 1024,
        );
    }
}

impl MetricsReporter for CountingReporter {
    fn report(&self, event: MetricEvent) {
        match event {
            MetricEvent::StorageListCompleted { num_files, .. } => {
                self.list_calls.fetch_add(1, Ordering::Relaxed);
                self.list_files_seen.fetch_add(num_files, Ordering::Relaxed);
            }
            MetricEvent::StorageReadCompleted {
                num_files,
                bytes_read,
                ..
            } => {
                self.read_calls.fetch_add(1, Ordering::Relaxed);
                self.read_files.fetch_add(num_files, Ordering::Relaxed);
                self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
            }
            MetricEvent::StorageCopyCompleted { .. } => {
                self.copy_calls.fetch_add(1, Ordering::Relaxed);
            }
            MetricEvent::SnapshotCompleted { .. } => {
                self.snapshot_completions.fetch_add(1, Ordering::Relaxed);
            }
            MetricEvent::LogSegmentLoaded {
                num_commit_files,
                num_checkpoint_files,
                num_compaction_files,
                ..
            } => {
                self.log_segment_loads.fetch_add(1, Ordering::Relaxed);
                self.commit_files
                    .fetch_add(num_commit_files, Ordering::Relaxed);
                self.checkpoint_files
                    .fetch_add(num_checkpoint_files, Ordering::Relaxed);
                self.compaction_files
                    .fetch_add(num_compaction_files, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}
