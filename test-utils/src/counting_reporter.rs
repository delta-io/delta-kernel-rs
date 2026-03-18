//! A [`MetricsReporter`] implementation that accumulates operation counts via atomic counters.
//!
//! Useful in tests to assert exact IO costs and in benchmarks to print per-call IO profiles.
//! Attach it to a `DefaultEngine` via `DefaultEngineBuilder::with_metrics_reporter`, then
//! inspect the counters or call [`CountingReporter::print_summary`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use delta_kernel::metrics::{MetricEvent, MetricsReporter};

/// Accumulates storage and operation metrics via the [`MetricsReporter`] interface.
///
/// All counters use [`Ordering::Relaxed`] -- sufficient here since there are no
/// ordering dependencies between counters.
#[derive(Debug, Default)]
pub struct CountingReporter {
    // Storage-layer IO counters (StorageHandler::list_from / read_files / copy_atomic)
    /// Number of `list_from` calls (one per [`MetricEvent::StorageListCompleted`]).
    pub list_calls: AtomicU64,
    /// Total files returned across all list calls.
    pub list_files_seen: AtomicU64,
    /// Number of `StorageHandler::read_files` calls (one per [`MetricEvent::StorageReadCompleted`]).
    pub storage_read_calls: AtomicU64,
    /// Total individual files read via `StorageHandler::read_files`.
    pub storage_read_files: AtomicU64,
    /// Total bytes consumed via `StorageHandler::read_files`.
    pub storage_bytes_read: AtomicU64,
    /// Number of `copy_atomic` calls (one per [`MetricEvent::StorageCopyCompleted`]).
    pub copy_calls: AtomicU64,

    // JSON handler IO counters (DefaultJsonHandler::read_json_files)
    /// Number of `read_json_files` calls (one per [`MetricEvent::JsonReadCompleted`]).
    pub json_read_calls: AtomicU64,
    /// Total JSON files requested across all `read_json_files` calls.
    pub json_files_read: AtomicU64,
    /// Total on-disk bytes of JSON files requested.
    pub json_bytes_read: AtomicU64,

    // Parquet handler IO counters (DefaultParquetHandler::read_parquet_files)
    /// Number of `read_parquet_files` calls (one per [`MetricEvent::ParquetReadCompleted`]).
    pub parquet_read_calls: AtomicU64,
    /// Total Parquet files requested across all `read_parquet_files` calls.
    pub parquet_files_read: AtomicU64,
    /// Total on-disk bytes of Parquet files requested.
    pub parquet_bytes_read: AtomicU64,

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
    /// Useful before a single profiling iteration to get per-call counts.
    pub fn reset(&self) {
        self.list_calls.store(0, Ordering::Relaxed);
        self.list_files_seen.store(0, Ordering::Relaxed);
        self.storage_read_calls.store(0, Ordering::Relaxed);
        self.storage_read_files.store(0, Ordering::Relaxed);
        self.storage_bytes_read.store(0, Ordering::Relaxed);
        self.copy_calls.store(0, Ordering::Relaxed);
        self.json_read_calls.store(0, Ordering::Relaxed);
        self.json_files_read.store(0, Ordering::Relaxed);
        self.json_bytes_read.store(0, Ordering::Relaxed);
        self.parquet_read_calls.store(0, Ordering::Relaxed);
        self.parquet_files_read.store(0, Ordering::Relaxed);
        self.parquet_bytes_read.store(0, Ordering::Relaxed);
        self.snapshot_completions.store(0, Ordering::Relaxed);
        self.log_segment_loads.store(0, Ordering::Relaxed);
        self.commit_files.store(0, Ordering::Relaxed);
        self.checkpoint_files.store(0, Ordering::Relaxed);
        self.compaction_files.store(0, Ordering::Relaxed);
    }

    /// Print a human-readable IO and operation summary.
    ///
    /// Intended to be called after [`reset`][Self::reset] and one operation so values
    /// reflect a single call's cost. Output goes to stdout and is visible with
    /// `cargo test -- --nocapture` or `cargo nextest run -- --no-capture`.
    pub fn print_summary(&self, label: &str) {
        let list_calls = self.list_calls.load(Ordering::Relaxed);
        let list_files = self.list_files_seen.load(Ordering::Relaxed);
        let storage_reads = self.storage_read_calls.load(Ordering::Relaxed);
        let storage_files = self.storage_read_files.load(Ordering::Relaxed);
        let storage_bytes = self.storage_bytes_read.load(Ordering::Relaxed);
        let json_calls = self.json_read_calls.load(Ordering::Relaxed);
        let json_files = self.json_files_read.load(Ordering::Relaxed);
        let json_bytes = self.json_bytes_read.load(Ordering::Relaxed);
        let parquet_calls = self.parquet_read_calls.load(Ordering::Relaxed);
        let parquet_files = self.parquet_files_read.load(Ordering::Relaxed);
        let parquet_bytes = self.parquet_bytes_read.load(Ordering::Relaxed);
        let log_loads = self.log_segment_loads.load(Ordering::Relaxed);
        let commits = self.commit_files.load(Ordering::Relaxed);
        let checkpoints = self.checkpoint_files.load(Ordering::Relaxed);
        let compactions = self.compaction_files.load(Ordering::Relaxed);

        println!(
            "  [io] {label}\n\
             \x20       storage : {list_calls} list ({list_files} files seen)  \
             {storage_reads} raw read ({storage_files} files, {} KiB)\n\
             \x20       json    : {json_calls} call(s)  {json_files} files  {} KiB\n\
             \x20       parquet : {parquet_calls} call(s)  {parquet_files} files  {} KiB\n\
             \x20       log     : {log_loads} segment load(s) -- \
             {commits} commits  {checkpoints} checkpoints  {compactions} compactions",
            storage_bytes / 1024,
            json_bytes / 1024,
            parquet_bytes / 1024,
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
                self.storage_read_calls.fetch_add(1, Ordering::Relaxed);
                self.storage_read_files
                    .fetch_add(num_files, Ordering::Relaxed);
                self.storage_bytes_read
                    .fetch_add(bytes_read, Ordering::Relaxed);
            }
            MetricEvent::StorageCopyCompleted { .. } => {
                self.copy_calls.fetch_add(1, Ordering::Relaxed);
            }
            MetricEvent::JsonReadCompleted {
                num_files,
                bytes_read,
            } => {
                self.json_read_calls.fetch_add(1, Ordering::Relaxed);
                self.json_files_read.fetch_add(num_files, Ordering::Relaxed);
                self.json_bytes_read
                    .fetch_add(bytes_read, Ordering::Relaxed);
            }
            MetricEvent::ParquetReadCompleted {
                num_files,
                bytes_read,
            } => {
                self.parquet_read_calls.fetch_add(1, Ordering::Relaxed);
                self.parquet_files_read
                    .fetch_add(num_files, Ordering::Relaxed);
                self.parquet_bytes_read
                    .fetch_add(bytes_read, Ordering::Relaxed);
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
