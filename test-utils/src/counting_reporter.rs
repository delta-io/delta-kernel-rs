//! A [`MetricsReporter`] implementation that accumulates operation counts via atomic counters.
//!
//! Useful in tests to assert exact IO costs and in benchmarks to print per-call IO profiles.
//! Attach it to a `DefaultEngine` via `DefaultEngineBuilder::with_metrics_reporter`, then
//! inspect the counters or call [`CountingReporter::print_summary`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use delta_kernel::metrics::{MetricEvent, MetricsReporter, WithMetricsReporterLayer as _};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Install `reporter` as a thread-local metrics-collecting subscriber, with the safety net
/// required to avoid tracing callsite-cache poisoning. Returns a guard that uninstalls the
/// subscriber when dropped.
///
/// This is the recommended way to wire a [`MetricsReporter`] into a test. Use it instead
/// of hand-rolling [`ensure_metrics_compatible_global_subscriber`] +
/// [`tracing_subscriber::registry()`] + [`set_default`] -- forgetting the first call
/// silently produces flaky tests where counters intermittently read zero (see the
/// [`ensure_metrics_compatible_global_subscriber`] doc for the underlying mechanism).
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use test_utils::{install_thread_local_metrics_reporter, CountingReporter};
///
/// let reporter = Arc::new(CountingReporter::new());
/// let _guard = install_thread_local_metrics_reporter(reporter.clone());
/// // ... run kernel operations; `reporter`'s counters now reflect their I/O ...
/// ```
///
/// [`set_default`]: tracing_subscriber::util::SubscriberInitExt::set_default
pub fn install_thread_local_metrics_reporter(reporter: Arc<dyn MetricsReporter>) -> DefaultGuard {
    ensure_metrics_compatible_global_subscriber();
    tracing_subscriber::registry()
        .with_metrics_reporter_layer(reporter)
        .set_default()
}

/// Ensure a process-global tracing subscriber is installed exactly once for the lifetime
/// of the test binary.
///
/// Most tests should call [`install_thread_local_metrics_reporter`] instead -- it bundles
/// this call with the thread-local subscriber install. Reach for this directly only when
/// you need to layer additional `tracing_subscriber` layers (e.g. `fmt`) alongside the
/// metrics layer.
///
/// # Why
///
/// `tracing` caches per-callsite `Interest` *process-globally* on first registration. The
/// kernel emits metrics from `Drop` impls that fire `tracing::span!` on whichever thread
/// happens to drop the iterator -- often a tokio worker thread owned by the
/// `DefaultEngine`'s background runtime, which has no thread-local subscriber. When such
/// a thread is the first to hit a metric callsite, it consults `NoSubscriber` (the
/// fallback), gets `Interest::never`, and that verdict is cached process-globally. Every
/// later emission of that callsite -- including emissions from threads that *do* have the
/// test's subscriber installed -- is then a no-op. Counters that depend on those metrics
/// sit at zero and assertions fail.
///
/// `set_default` (thread-local) does *not* invalidate the callsite cache; only
/// `set_global_default` does. This helper installs a bare `Registry` as the global
/// default once per process. It does nothing on its own (no metrics layer attached to
/// it), but it ensures every thread sees a real subscriber whenever it first hits a
/// callsite, which keeps the cached interest in the `always`/`sometimes` regime.
/// Per-test `set_default(...)` then routes events to the test's `CountingReporter` as
/// usual; metrics emissions on threads without a thread-local override (tokio workers,
/// libtest scaffolding) silently fall through to the bare `Registry`, which is correct
/// because those threads have no test counter to update.
pub fn ensure_metrics_compatible_global_subscriber() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        // Install a bare Registry. `try_init` calls `set_global_default`, which can only
        // succeed once per process and triggers an interest-cache rebuild internally on
        // success. If a global default is already installed (e.g. by a test runner via
        // `RUST_LOG`), respect it and rebuild the cache ourselves so any callsites that
        // were registered before this call are re-evaluated against the current dispatcher.
        if tracing_subscriber::registry().try_init().is_err() {
            tracing::callsite::rebuild_interest_cache();
        }
    });
}

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
    /// Number of `StorageHandler::read_files` calls (one per
    /// [`MetricEvent::StorageReadCompleted`]).
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
    /// Total commit (JSON delta) files in the commit tail across all log segment loads.
    /// These are the commits between the last checkpoint and the snapshot version — not
    /// all historical commits in the table. Commits older than the selected checkpoint
    /// are not included.
    pub commit_files: RelaxedCounter,
    /// Total checkpoint part files read across all log segment loads. For a single-part
    /// checkpoint this is 1; for a multi-part checkpoint it equals the number of parts
    /// that make up the selected checkpoint.
    pub checkpoint_files: RelaxedCounter,
    /// Total log compaction files in the commit tail across all log segment loads.
    pub compaction_files: RelaxedCounter,
    /// Total number of times a latest CRC file was found in the log segment, across all
    /// log segment loads.
    pub latest_crc_files_found: RelaxedCounter,

    // CRC reader IO counters
    /// Number of CRC read calls (one per [`MetricEvent::CrcReadCompleted`]).
    pub crc_read_calls: RelaxedCounter,
    /// Total number of bytes read from CRC files, across all CRC read calls.
    pub crc_bytes_read: RelaxedCounter,
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
        self.latest_crc_files_found.reset();
        self.crc_read_calls.reset();
        self.crc_bytes_read.reset();
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
        let storage_kib = self.storage_bytes_read.get() / 1024;
        let copy_calls = self.copy_calls.get();
        let json_calls = self.json_read_calls.get();
        let json_files = self.json_files_read.get();
        let json_kib = self.json_bytes_read.get() / 1024;
        let parquet_calls = self.parquet_read_calls.get();
        let parquet_files = self.parquet_files_read.get();
        let parquet_kib = self.parquet_bytes_read.get() / 1024;
        let crc_calls = self.crc_read_calls.get();
        let crc_kib = self.crc_bytes_read.get() / 1024;
        let log_loads = self.log_segment_loads.get();
        let commits = self.commit_files.get();
        let checkpoints = self.checkpoint_files.get();
        let compactions = self.compaction_files.get();
        let crc_files_found = self.latest_crc_files_found.get();

        println!("  [io] {label}");
        println!("    storage : {list_calls} list ({list_files} files seen)  {storage_reads} raw read ({storage_files} files, {storage_kib} KiB)  {copy_calls} copy");
        println!("    json    : {json_calls} call(s)  {json_files} files  {json_kib} KiB");
        println!("    parquet : {parquet_calls} call(s)  {parquet_files} files  {parquet_kib} KiB");
        println!("    crc     : {crc_calls} call(s)  {crc_kib} KiB");
        println!("    log     : {log_loads} segment load(s) -- {commits} commits  {checkpoints} checkpoints  {compactions} compactions  {crc_files_found} latest_crc_files");
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
                has_latest_crc_file,
                ..
            } => {
                self.log_segment_loads.inc();
                self.commit_files.add(num_commit_files);
                self.checkpoint_files.add(num_checkpoint_files);
                self.compaction_files.add(num_compaction_files);
                self.latest_crc_files_found.add(has_latest_crc_file as u64);
            }
            MetricEvent::CrcReadCompleted { bytes_read, .. } => {
                self.crc_read_calls.inc();
                self.crc_bytes_read.add(bytes_read);
            }
            // Intentionally not tracked -- add counters if needed.
            MetricEvent::ProtocolMetadataLoaded { .. }
            | MetricEvent::SnapshotFailed { .. }
            | MetricEvent::ScanMetadataCompleted { .. } => {}
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
            has_latest_crc_file: true,
        });
        assert_eq!(reporter.log_segment_loads.get(), 1);
        assert_eq!(reporter.commit_files.get(), 7);
        assert_eq!(reporter.checkpoint_files.get(), 2);
        assert_eq!(reporter.compaction_files.get(), 1);
        assert_eq!(reporter.latest_crc_files_found.get(), 1);
    }

    #[test]
    fn report_log_segment_loaded_without_crc_does_not_increment_crc_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::LogSegmentLoaded {
            operation_id: MetricId::new(),
            duration: dur(),
            num_commit_files: 3,
            num_checkpoint_files: 1,
            num_compaction_files: 0,
            has_latest_crc_file: false,
        });
        assert_eq!(reporter.log_segment_loads.get(), 1);
        assert_eq!(reporter.latest_crc_files_found.get(), 0);
    }

    #[test]
    fn report_crc_read_completed_increments_crc_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::CrcReadCompleted {
            duration: dur(),
            bytes_read: 512,
        });
        reporter.report(MetricEvent::CrcReadCompleted {
            duration: dur(),
            bytes_read: 256,
        });
        assert_eq!(reporter.crc_read_calls.get(), 2);
        assert_eq!(reporter.crc_bytes_read.get(), 768);
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
            has_latest_crc_file: true,
        });
        reporter.report(MetricEvent::CrcReadCompleted {
            duration: dur(),
            bytes_read: 512,
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
        assert_eq!(reporter.latest_crc_files_found.get(), 0);
        assert_eq!(reporter.crc_read_calls.get(), 0);
        assert_eq!(reporter.crc_bytes_read.get(), 0);
    }
}
