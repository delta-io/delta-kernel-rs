//! A [`MetricsReporter`] implementation that accumulates operation counts via atomic counters.
//!
//! Useful in tests to assert exact IO costs and in benchmarks to print per-call IO profiles.
//! Attach it to a `DefaultEngine` via `DefaultEngineBuilder::with_metrics_reporter`, then
//! inspect the counters or call [`CountingReporter::print_summary`].

use std::sync::atomic::{AtomicU64, Ordering};

use delta_kernel::metrics::{MetricEvent, MetricsReporter};

/// An atomic `u64` counter using [`Ordering::Relaxed`] throughout.
///
/// Relaxed ordering is sufficient here: metrics are reported after the operations they
/// describe have completed, so there are no inter-counter ordering dependencies.
#[derive(Debug, Default)]
pub struct RelaxedCounter(AtomicU64);

impl RelaxedCounter {
    /// Increment the counter by one.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n` to the counter.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Read the current value.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Reset the counter to zero.
    pub fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

/// Accumulates storage and operation metrics via the [`MetricsReporter`] interface.
///
/// # Note: update [`reset`] and the `MetricsReporter` impl when adding fields.
///
/// [`reset`]: Self::reset
#[derive(Debug, Default)]
pub struct CountingReporter {
    // Storage-layer IO counters (StorageHandler::list_from / read_files / copy_atomic)
    /// Number of `list_from` calls (one per [`MetricEvent::StorageListCompleted`]).
    pub list_calls: RelaxedCounter,
    /// Total files returned across all list calls.
    pub list_files_seen: RelaxedCounter,
    /// Number of `StorageHandler::read_files` calls (one per [`MetricEvent::StorageReadCompleted`]).
    pub storage_read_calls: RelaxedCounter,
    /// Total individual files read via `StorageHandler::read_files`.
    pub storage_read_files: RelaxedCounter,
    /// Total bytes consumed via `StorageHandler::read_files`.
    pub storage_bytes_read: RelaxedCounter,
    /// Number of `copy_atomic` calls (one per [`MetricEvent::StorageCopyCompleted`]).
    pub copy_calls: RelaxedCounter,

    // JSON handler IO counters (DefaultJsonHandler::read_json_files)
    /// Number of `read_json_files` calls (one per [`MetricEvent::JsonReadCompleted`]).
    pub json_read_calls: RelaxedCounter,
    /// Total JSON files requested across all `read_json_files` calls.
    pub json_files_read: RelaxedCounter,
    /// Total on-disk bytes of JSON files requested.
    pub json_bytes_read: RelaxedCounter,

    // Parquet handler IO counters (DefaultParquetHandler::read_parquet_files)
    /// Number of `read_parquet_files` calls (one per [`MetricEvent::ParquetReadCompleted`]).
    pub parquet_read_calls: RelaxedCounter,
    /// Total Parquet files requested across all `read_parquet_files` calls.
    pub parquet_files_read: RelaxedCounter,
    /// Total on-disk bytes of Parquet files requested.
    pub parquet_bytes_read: RelaxedCounter,

    // Operation-level counters
    /// Number of completed snapshot constructions.
    pub snapshot_completions: RelaxedCounter,
    /// Number of full (non-incremental) log segment loads. Each fresh snapshot construction
    /// from a table root contributes one load; incremental snapshot updates do not.
    pub log_segment_loads: RelaxedCounter,
    /// Total commit files included in the log segment across all log segment loads.
    pub commit_files: RelaxedCounter,
    /// Total checkpoint part files read across all log segment loads. For a single-part
    /// checkpoint this is 1; for a multi-part checkpoint it equals the number of parts
    /// that make up the selected checkpoint.
    pub checkpoint_files: RelaxedCounter,
    /// Total log compaction files seen across all log segment loads.
    pub compaction_files: RelaxedCounter,
}

impl CountingReporter {
    /// Create a new reporter with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all counters to zero.
    ///
    /// Useful before a single profiling iteration to get per-call counts.
    pub fn reset(&self) {
        self.list_calls.reset();
        self.list_files_seen.reset();
        self.storage_read_calls.reset();
        self.storage_read_files.reset();
        self.storage_bytes_read.reset();
        self.copy_calls.reset();
        self.json_read_calls.reset();
        self.json_files_read.reset();
        self.json_bytes_read.reset();
        self.parquet_read_calls.reset();
        self.parquet_files_read.reset();
        self.parquet_bytes_read.reset();
        self.snapshot_completions.reset();
        self.log_segment_loads.reset();
        self.commit_files.reset();
        self.checkpoint_files.reset();
        self.compaction_files.reset();
    }

    /// Print a human-readable IO and operation summary.
    ///
    /// Intended to be called after [`reset`][Self::reset] and one operation so values
    /// reflect a single call's cost. Output is visible with `cargo test -- --nocapture`
    /// or `cargo nextest run -- --no-capture`.
    pub fn print_summary(&self, label: &str) {
        let list_calls = self.list_calls.get();
        let list_files = self.list_files_seen.get();
        let storage_reads = self.storage_read_calls.get();
        let storage_files = self.storage_read_files.get();
        let storage_bytes = self.storage_bytes_read.get();
        let copy_calls = self.copy_calls.get();
        let json_calls = self.json_read_calls.get();
        let json_files = self.json_files_read.get();
        let json_bytes = self.json_bytes_read.get();
        let parquet_calls = self.parquet_read_calls.get();
        let parquet_files = self.parquet_files_read.get();
        let parquet_bytes = self.parquet_bytes_read.get();
        let log_loads = self.log_segment_loads.get();
        let commits = self.commit_files.get();
        let checkpoints = self.checkpoint_files.get();
        let compactions = self.compaction_files.get();

        println!(
            "  [io] {label}\n\
             \x20       storage : {list_calls} list ({list_files} files seen)  \
             {storage_reads} raw read ({storage_files} files, {} KiB)  {copy_calls} copy\n\
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
                self.list_calls.inc();
                self.list_files_seen.add(num_files);
            }
            MetricEvent::StorageReadCompleted {
                num_files,
                bytes_read,
                ..
            } => {
                self.storage_read_calls.inc();
                self.storage_read_files.add(num_files);
                self.storage_bytes_read.add(bytes_read);
            }
            MetricEvent::StorageCopyCompleted { .. } => {
                self.copy_calls.inc();
            }
            MetricEvent::JsonReadCompleted {
                num_files,
                bytes_read,
            } => {
                self.json_read_calls.inc();
                self.json_files_read.add(num_files);
                self.json_bytes_read.add(bytes_read);
            }
            MetricEvent::ParquetReadCompleted {
                num_files,
                bytes_read,
            } => {
                self.parquet_read_calls.inc();
                self.parquet_files_read.add(num_files);
                self.parquet_bytes_read.add(bytes_read);
            }
            MetricEvent::SnapshotCompleted { .. } => {
                self.snapshot_completions.inc();
            }
            MetricEvent::LogSegmentLoaded {
                num_commit_files,
                num_checkpoint_files,
                num_compaction_files,
                ..
            } => {
                self.log_segment_loads.inc();
                self.commit_files.add(num_commit_files);
                self.checkpoint_files.add(num_checkpoint_files);
                self.compaction_files.add(num_compaction_files);
            }
            // Intentionally not tracked -- add counters if needed.
            MetricEvent::ProtocolMetadataLoaded { .. } | MetricEvent::SnapshotFailed { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use delta_kernel::metrics::MetricId;

    use super::*;

    fn dur() -> Duration {
        Duration::from_millis(1)
    }

    #[test]
    fn report_storage_list_completed_increments_list_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageListCompleted {
            duration: dur(),
            num_files: 10,
        });
        reporter.report(MetricEvent::StorageListCompleted {
            duration: dur(),
            num_files: 5,
        });
        assert_eq!(reporter.list_calls.get(), 2);
        assert_eq!(reporter.list_files_seen.get(), 15);
    }

    #[test]
    fn report_storage_read_completed_increments_read_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageReadCompleted {
            duration: dur(),
            num_files: 3,
            bytes_read: 1024,
        });
        assert_eq!(reporter.storage_read_calls.get(), 1);
        assert_eq!(reporter.storage_read_files.get(), 3);
        assert_eq!(reporter.storage_bytes_read.get(), 1024);
    }

    #[test]
    fn report_storage_copy_completed_increments_copy_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageCopyCompleted { duration: dur() });
        assert_eq!(reporter.copy_calls.get(), 1);
    }

    #[test]
    fn report_snapshot_completed_increments_snapshot_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::SnapshotCompleted {
            operation_id: MetricId::new(),
            version: 0,
            total_duration: dur(),
        });
        assert_eq!(reporter.snapshot_completions.get(), 1);
    }

    #[test]
    fn report_log_segment_loaded_increments_log_replay_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::LogSegmentLoaded {
            operation_id: MetricId::new(),
            duration: dur(),
            num_commit_files: 7,
            num_checkpoint_files: 2,
            num_compaction_files: 1,
        });
        assert_eq!(reporter.log_segment_loads.get(), 1);
        assert_eq!(reporter.commit_files.get(), 7);
        assert_eq!(reporter.checkpoint_files.get(), 2);
        assert_eq!(reporter.compaction_files.get(), 1);
    }

    #[test]
    fn report_untracked_events_does_not_panic() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::ProtocolMetadataLoaded {
            operation_id: MetricId::new(),
            duration: dur(),
        });
        reporter.report(MetricEvent::SnapshotFailed {
            operation_id: MetricId::new(),
            duration: dur(),
        });
        assert_eq!(reporter.snapshot_completions.get(), 0);
    }

    #[test]
    fn reset_zeros_all_counters() {
        let reporter = Arc::new(CountingReporter::new());
        reporter.report(MetricEvent::StorageListCompleted {
            duration: dur(),
            num_files: 10,
        });
        reporter.report(MetricEvent::StorageReadCompleted {
            duration: dur(),
            num_files: 3,
            bytes_read: 1024,
        });
        reporter.report(MetricEvent::StorageCopyCompleted { duration: dur() });
        reporter.report(MetricEvent::LogSegmentLoaded {
            operation_id: MetricId::new(),
            duration: dur(),
            num_commit_files: 7,
            num_checkpoint_files: 2,
            num_compaction_files: 1,
        });

        reporter.reset();

        assert_eq!(reporter.list_calls.get(), 0);
        assert_eq!(reporter.list_files_seen.get(), 0);
        assert_eq!(reporter.storage_read_calls.get(), 0);
        assert_eq!(reporter.storage_read_files.get(), 0);
        assert_eq!(reporter.storage_bytes_read.get(), 0);
        assert_eq!(reporter.copy_calls.get(), 0);
        assert_eq!(reporter.json_read_calls.get(), 0);
        assert_eq!(reporter.json_files_read.get(), 0);
        assert_eq!(reporter.json_bytes_read.get(), 0);
        assert_eq!(reporter.parquet_read_calls.get(), 0);
        assert_eq!(reporter.parquet_files_read.get(), 0);
        assert_eq!(reporter.parquet_bytes_read.get(), 0);
        assert_eq!(reporter.snapshot_completions.get(), 0);
        assert_eq!(reporter.log_segment_loads.get(), 0);
        assert_eq!(reporter.commit_files.get(), 0);
        assert_eq!(reporter.checkpoint_files.get(), 0);
        assert_eq!(reporter.compaction_files.get(), 0);
    }
}
