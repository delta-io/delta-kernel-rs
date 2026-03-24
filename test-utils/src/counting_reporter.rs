//! A [`MetricsReporter`] implementation that accumulates operation counts via atomic counters.
//!
//! Useful in tests to assert exact IO costs and in benchmarks to print per-call IO profiles.
//! Attach it to a `DefaultEngine` via `DefaultEngineBuilder::with_metrics_reporter`, then
//! inspect the counters or call [`CountingReporter::print_summary`].

use std::sync::atomic::{AtomicU64, Ordering};

use delta_kernel::metrics::{MetricEvent, MetricsReporter};

/// Accumulates storage and operation metrics via the [`MetricsReporter`] interface.
///
/// All counters use [`Ordering::Relaxed`] -- sufficient here since there are no
/// ordering dependencies between counters.
///
/// # Note: update [`reset`] and the `MetricsReporter` impl when adding fields.
///
/// [`reset`]: Self::reset
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
    /// Number of full (non-incremental) log segment loads. Each fresh snapshot construction
    /// from a table root contributes one load; incremental snapshot updates do not.
    pub log_segment_loads: AtomicU64,
    /// Total commit files seen across all log segment loads.
    pub commit_files: AtomicU64,
    /// Total checkpoint part files selected for reading across all log segment loads.
    /// For a single-part checkpoint this is 1; for a multi-part checkpoint it equals the
    /// number of parts that make up the selected checkpoint.
    pub checkpoint_files: AtomicU64,
    /// Total log compaction files seen across all log segment loads.
    pub compaction_files: AtomicU64,
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

    /// Print a human-readable IO and operation summary.
    ///
    /// Intended to be called after [`reset`][Self::reset] and one operation so values
    /// reflect a single call's cost. Output is visible with `cargo test -- --nocapture`
    /// or `cargo nextest run -- --no-capture`.
    pub fn print_summary(&self, label: &str) {
        let list_calls = self.list_calls.load(Ordering::Relaxed);
        let list_files = self.list_files_seen.load(Ordering::Relaxed);
        let read_calls = self.read_calls.load(Ordering::Relaxed);
        let read_files = self.read_files.load(Ordering::Relaxed);
        let kib = self.bytes_read.load(Ordering::Relaxed) / 1024;
        let copy_calls = self.copy_calls.load(Ordering::Relaxed);
        let log_loads = self.log_segment_loads.load(Ordering::Relaxed);
        let commits = self.commit_files.load(Ordering::Relaxed);
        let checkpoints = self.checkpoint_files.load(Ordering::Relaxed);
        let compactions = self.compaction_files.load(Ordering::Relaxed);

        println!("  [io] {label}");
        println!("    storage  : {list_calls} list ({list_files} files seen)  {read_calls} read ({read_files} files, {kib} KiB)  {copy_calls} copy");
        println!("    log replay: {log_loads} segment load(s) -- {commits} commits  {checkpoints} checkpoints  {compactions} compactions");
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
        assert_eq!(reporter.list_calls.load(Ordering::Relaxed), 2);
        assert_eq!(reporter.list_files_seen.load(Ordering::Relaxed), 15);
    }

    #[test]
    fn report_storage_read_completed_increments_read_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageReadCompleted {
            duration: dur(),
            num_files: 3,
            bytes_read: 1024,
        });
        assert_eq!(reporter.read_calls.load(Ordering::Relaxed), 1);
        assert_eq!(reporter.read_files.load(Ordering::Relaxed), 3);
        assert_eq!(reporter.bytes_read.load(Ordering::Relaxed), 1024);
    }

    #[test]
    fn report_storage_copy_completed_increments_copy_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageCopyCompleted { duration: dur() });
        assert_eq!(reporter.copy_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn report_snapshot_completed_increments_snapshot_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::SnapshotCompleted {
            operation_id: MetricId::new(),
            version: 0,
            total_duration: dur(),
        });
        assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 1);
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
        assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 1);
        assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 7);
        assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 2);
        assert_eq!(reporter.compaction_files.load(Ordering::Relaxed), 1);
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
        // No counters change -- just verify no panic
        assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 0);
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

        assert_eq!(reporter.list_calls.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.list_files_seen.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.read_calls.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.read_files.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.bytes_read.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.copy_calls.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.snapshot_completions.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.log_segment_loads.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.commit_files.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.checkpoint_files.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.compaction_files.load(Ordering::Relaxed), 0);
    }
}
