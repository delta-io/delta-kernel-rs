//! A [`MetricsReporter`] implementation that accumulates operation counts via thread-safe counters.
//!
//! Useful in tests to assert exact IO costs and in benchmarks to print per-call IO profiles.
//! Install it with [`install_thread_local_metrics_reporter`], then inspect the counters or call
//! [`CountingReporter::print_summary`].

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

pub use delta_kernel::engine::RelaxedCounter;
use delta_kernel::metrics::{
    CommitFailureReason, CrcReadOutcome, DomainMetadataLoadOutcome, LastCheckpointReadOutcome,
    LogSegmentLoadOutcome, LogSegmentLoadStats, MetricEvent, MetricsReporter,
    SetTransactionLoadOutcome, SnapshotBuildOutcome, SnapshotLoadType, TransactionCommitOutcome,
    TransactionCommitStats, WithMetricsReporterLayer as _,
};
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

/// Status label for a snapshot completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnapshotCompletionStatus {
    /// The snapshot was constructed successfully.
    Success,
    /// Snapshot construction failed.
    Failure,
}

#[derive(Debug)]
struct LabeledCounter<L>(Mutex<HashMap<L, u64>>);

impl<L> Default for LabeledCounter<L> {
    fn default() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

impl<L: Eq + Hash> LabeledCounter<L> {
    fn counts(&self) -> MutexGuard<'_, HashMap<L, u64>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn get(&self, labels: &L) -> u64 {
        self.counts().get(labels).copied().unwrap_or_default()
    }

    fn reset(&self) {
        self.counts().clear();
    }

    fn inc(&self, labels: L) {
        let mut counts = self.counts();
        *counts.entry(labels).or_default() += 1;
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
    snapshot_completions: LabeledCounter<(SnapshotCompletionStatus, SnapshotLoadType)>,
    /// Number of successful log-segment loads, including full and incremental loads.
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
    /// Number of CRC reads with a [`CrcReadOutcome::Success`] result.
    pub crc_read_calls: RelaxedCounter,
    /// Total bytes in successfully read and parsed CRC files.
    pub crc_bytes_read: RelaxedCounter,

    // _last_checkpoint reader counters
    /// Number of `_last_checkpoint` read attempts.
    pub last_checkpoint_reads: RelaxedCounter,
    /// Number of hints parsed and retained.
    pub last_checkpoint_read_successes: RelaxedCounter,
    /// Number of read attempts where storage reported the hint absent.
    pub last_checkpoint_read_not_found: RelaxedCounter,
    /// Number of read attempts that produced an invalid hint.
    pub last_checkpoint_read_invalid: RelaxedCounter,
    /// Number of read attempts that returned another error.
    pub last_checkpoint_read_errors: RelaxedCounter,
    /// Number of read attempts with an unrecorded or unrecognized outcome.
    pub last_checkpoint_read_unknown: RelaxedCounter,

    // Domain metadata load counters (Snapshot::get_domain_metadatas_internal)
    pub domain_metadata_loads: RelaxedCounter,
    pub domain_metadata_load_failures: RelaxedCounter,
    pub domain_metadata_cache_hits: RelaxedCounter,
    pub domain_metadata_domains_returned: RelaxedCounter,

    // SetTransaction load counters (Snapshot::get_app_id_version)
    pub set_transaction_loads: RelaxedCounter,
    pub set_transaction_load_failures: RelaxedCounter,
    pub set_transaction_cache_hits: RelaxedCounter,
    pub set_transaction_found: RelaxedCounter,

    // Transaction commit counters (Transaction::commit)
    pub transaction_commits: RelaxedCounter,
    pub commit_conflicts: RelaxedCounter,
    pub commit_retryable_failures: RelaxedCounter,
    pub commit_errors: RelaxedCounter,
    pub commit_add_files: RelaxedCounter,
    pub commit_remove_files: RelaxedCounter,
    pub commit_add_bytes: RelaxedCounter,
    pub commit_remove_bytes: RelaxedCounter,
}

impl CountingReporter {
    /// Create a new reporter with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of snapshot completions with the supplied status and load type.
    pub fn snapshot_completion_count(
        &self,
        status: SnapshotCompletionStatus,
        load_type: SnapshotLoadType,
    ) -> u64 {
        self.snapshot_completions.get(&(status, load_type))
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
        self.last_checkpoint_reads.reset();
        self.last_checkpoint_read_successes.reset();
        self.last_checkpoint_read_not_found.reset();
        self.last_checkpoint_read_invalid.reset();
        self.last_checkpoint_read_errors.reset();
        self.last_checkpoint_read_unknown.reset();
        self.domain_metadata_loads.reset();
        self.domain_metadata_load_failures.reset();
        self.domain_metadata_cache_hits.reset();
        self.domain_metadata_domains_returned.reset();
        self.set_transaction_loads.reset();
        self.set_transaction_load_failures.reset();
        self.set_transaction_cache_hits.reset();
        self.set_transaction_found.reset();
        self.transaction_commits.reset();
        self.commit_conflicts.reset();
        self.commit_retryable_failures.reset();
        self.commit_errors.reset();
        self.commit_add_files.reset();
        self.commit_remove_files.reset();
        self.commit_add_bytes.reset();
        self.commit_remove_bytes.reset();
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
        println!(
            "    storage : {list_calls} list ({list_files} files seen)  {storage_reads} raw read ({storage_files} files, {storage_kib} KiB)  {copy_calls} copy"
        );
        println!("    json    : {json_calls} call(s)  {json_files} files  {json_kib} KiB");
        println!("    parquet : {parquet_calls} call(s)  {parquet_files} files  {parquet_kib} KiB");
        println!("    crc     : {crc_calls} call(s)  {crc_kib} KiB");
        println!(
            "    log     : {log_loads} segment load(s) -- {commits} commits  {checkpoints} checkpoints  {compactions} compactions  {crc_files_found} latest_crc_files"
        );
    }
}

impl MetricsReporter for CountingReporter {
    fn report(&self, event: MetricEvent) {
        match event {
            MetricEvent::StorageListCompleted(e) => {
                self.list_calls.inc();
                self.list_files_seen.add(e.num_files);
            }
            MetricEvent::StorageReadCompleted(e) => {
                self.storage_read_calls.inc();
                self.storage_read_files.add(e.num_files);
                self.storage_bytes_read.add(e.bytes_read);
            }
            MetricEvent::StorageCopyCompleted(_) => self.copy_calls.inc(),
            MetricEvent::JsonReadCompleted(e) => {
                self.json_read_calls.inc();
                self.json_files_read.add(e.num_files);
                self.json_bytes_read.add(e.bytes_read);
            }
            MetricEvent::ParquetReadCompleted(e) => {
                self.parquet_read_calls.inc();
                self.parquet_files_read.add(e.num_files);
                self.parquet_bytes_read.add(e.bytes_read);
            }
            MetricEvent::SnapshotBuildCompleted(e) => {
                let status = match e.outcome {
                    SnapshotBuildOutcome::Success { .. } => SnapshotCompletionStatus::Success,
                    SnapshotBuildOutcome::Error => SnapshotCompletionStatus::Failure,
                };
                self.snapshot_completions.inc((status, e.load_type));
            }
            MetricEvent::LogSegmentLoadCompleted(e) => {
                if let LogSegmentLoadOutcome::Success {
                    stats:
                        LogSegmentLoadStats {
                            num_commit_files,
                            num_checkpoint_files,
                            num_compaction_files,
                            crc_versions_behind,
                        },
                } = e.outcome
                {
                    self.log_segment_loads.inc();
                    self.commit_files.add(num_commit_files);
                    self.checkpoint_files.add(num_checkpoint_files);
                    self.compaction_files.add(num_compaction_files);
                    self.latest_crc_files_found
                        .add(crc_versions_behind.is_some() as u64);
                }
            }
            MetricEvent::CrcReadCompleted(e) => {
                if let CrcReadOutcome::Success { bytes_read } = e.outcome {
                    self.crc_read_calls.inc();
                    self.crc_bytes_read.add(bytes_read);
                }
            }
            MetricEvent::LastCheckpointReadCompleted(e) => {
                self.last_checkpoint_reads.inc();
                match e.outcome {
                    LastCheckpointReadOutcome::Success => self.last_checkpoint_read_successes.inc(),
                    LastCheckpointReadOutcome::NotFound => {
                        self.last_checkpoint_read_not_found.inc()
                    }
                    LastCheckpointReadOutcome::Invalid => self.last_checkpoint_read_invalid.inc(),
                    LastCheckpointReadOutcome::Error => self.last_checkpoint_read_errors.inc(),
                    LastCheckpointReadOutcome::Unknown => self.last_checkpoint_read_unknown.inc(),
                }
            }
            MetricEvent::DomainMetadataLoadCompleted(e) => match e.outcome {
                DomainMetadataLoadOutcome::Success {
                    from_cache,
                    num_domains_returned,
                } => {
                    self.domain_metadata_loads.inc();
                    if from_cache {
                        self.domain_metadata_cache_hits.inc();
                    }
                    self.domain_metadata_domains_returned
                        .add(num_domains_returned);
                }
                DomainMetadataLoadOutcome::Error => self.domain_metadata_load_failures.inc(),
            },
            MetricEvent::SetTransactionLoadCompleted(e) => match e.outcome {
                SetTransactionLoadOutcome::Success { from_cache, found } => {
                    self.set_transaction_loads.inc();
                    if from_cache {
                        self.set_transaction_cache_hits.inc();
                    }
                    if found {
                        self.set_transaction_found.inc();
                    }
                }
                SetTransactionLoadOutcome::Error => self.set_transaction_load_failures.inc(),
            },
            MetricEvent::TransactionCommitCompleted(e) => match e.outcome {
                TransactionCommitOutcome::Success {
                    stats:
                        TransactionCommitStats {
                            num_add_files,
                            num_remove_files,
                            add_files_bytes,
                            remove_files_bytes,
                            ..
                        },
                } => {
                    self.transaction_commits.inc();
                    self.commit_add_files.add(num_add_files);
                    self.commit_remove_files.add(num_remove_files);
                    self.commit_add_bytes.add(add_files_bytes);
                    self.commit_remove_bytes.add(remove_files_bytes);
                }
                TransactionCommitOutcome::Failure { reason } => match reason {
                    CommitFailureReason::Conflict => self.commit_conflicts.inc(),
                    CommitFailureReason::RetryableIo => self.commit_retryable_failures.inc(),
                    CommitFailureReason::Error => self.commit_errors.inc(),
                },
            },
            MetricEvent::ProtocolMetadataLoadCompleted(_)
            | MetricEvent::CheckpointWriteCompleted(_)
            | MetricEvent::ChecksumWriteCompleted(_)
            | MetricEvent::ScanMetadataCompleted(_) => {}
        }
    }
}

/// A [`MetricsReporter`] that retains every [`MetricEvent`] for inspection, for tests that need
/// to assert on event payloads (not just counts, which [`CountingReporter`] handles).
#[derive(Debug, Default)]
pub struct CapturingReporter {
    events: Mutex<Vec<MetricEvent>>,
}

impl MetricsReporter for CapturingReporter {
    fn report(&self, event: MetricEvent) {
        self.events.lock().unwrap().push(event);
    }
}

impl CapturingReporter {
    /// All events captured so far, in report order.
    pub fn events(&self) -> Vec<MetricEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use delta_kernel::metrics::{
        CrcReadCompleted, CrcReadOutcome, DomainMetadataLoadCompleted, DomainMetadataLoadOutcome,
        LogSegmentLoadCompleted, LogSegmentLoadOutcome, LogSegmentLoadStats, LogSegmentLoadType,
        MetricContext, MetricId, ProtocolMetadataLoadCompleted, ProtocolMetadataLoadOutcome,
        ProtocolMetadataSource, SetTransactionLoadCompleted, SetTransactionLoadOutcome,
        SnapshotBuildCompleted, SnapshotBuildOutcome, SnapshotLoadType, StorageCopyCompleted,
        StorageListCompleted, StorageReadCompleted, TableType, TransactionCommitCompleted,
        TransactionCommitOutcome, TransactionCommitStats,
    };
    use rstest::rstest;

    use super::*;

    fn dur() -> Duration {
        Duration::from_millis(1)
    }

    #[rstest]
    #[case(LastCheckpointReadOutcome::Success)]
    #[case(LastCheckpointReadOutcome::NotFound)]
    #[case(LastCheckpointReadOutcome::Invalid)]
    #[case(LastCheckpointReadOutcome::Error)]
    #[case(LastCheckpointReadOutcome::Unknown)]
    fn last_checkpoint_read_counts_one_attempt_and_outcome_then_resets(
        #[case] outcome: LastCheckpointReadOutcome,
    ) {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::LastCheckpointReadCompleted(
            delta_kernel::metrics::LastCheckpointReadCompleted {
                outcome,
                duration: dur(),
            },
        ));
        assert_eq!(reporter.last_checkpoint_reads.get(), 1);
        let counters = [
            (
                &reporter.last_checkpoint_read_successes,
                LastCheckpointReadOutcome::Success,
            ),
            (
                &reporter.last_checkpoint_read_not_found,
                LastCheckpointReadOutcome::NotFound,
            ),
            (
                &reporter.last_checkpoint_read_invalid,
                LastCheckpointReadOutcome::Invalid,
            ),
            (
                &reporter.last_checkpoint_read_errors,
                LastCheckpointReadOutcome::Error,
            ),
            (
                &reporter.last_checkpoint_read_unknown,
                LastCheckpointReadOutcome::Unknown,
            ),
        ];
        for (counter, expected) in counters {
            assert_eq!(counter.get(), u64::from(outcome == expected));
        }
        reporter.reset();
        assert_eq!(reporter.last_checkpoint_reads.get(), 0);
        for (counter, _) in counters {
            assert_eq!(counter.get(), 0);
        }
    }

    #[test]
    fn report_storage_list_completed_increments_list_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageListCompleted(StorageListCompleted {
            duration: dur(),
            num_files: 10,
        }));
        reporter.report(MetricEvent::StorageListCompleted(StorageListCompleted {
            duration: dur(),
            num_files: 5,
        }));
        assert_eq!(reporter.list_calls.get(), 2);
        assert_eq!(reporter.list_files_seen.get(), 15);
    }

    #[test]
    fn report_storage_read_completed_increments_read_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageReadCompleted(StorageReadCompleted {
            duration: dur(),
            num_files: 3,
            bytes_read: 1024,
        }));
        assert_eq!(reporter.storage_read_calls.get(), 1);
        assert_eq!(reporter.storage_read_files.get(), 3);
        assert_eq!(reporter.storage_bytes_read.get(), 1024);
    }

    #[test]
    fn report_storage_copy_completed_increments_copy_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::StorageCopyCompleted(StorageCopyCompleted {
            duration: dur(),
        }));
        assert_eq!(reporter.copy_calls.get(), 1);
    }

    #[rstest]
    #[case::success(SnapshotCompletionStatus::Success)]
    #[case::failure(SnapshotCompletionStatus::Failure)]
    fn report_snapshot_completions_share_one_counter_across_load_types(
        #[case] status: SnapshotCompletionStatus,
    ) {
        let reporter = CountingReporter::new();
        for load_type in [
            SnapshotLoadType::Full,
            SnapshotLoadType::Incremental,
            SnapshotLoadType::SnapshotHint,
        ] {
            match status {
                SnapshotCompletionStatus::Success => {
                    reporter.report(MetricEvent::SnapshotBuildCompleted(
                        SnapshotBuildCompleted {
                            context: MetricContext {
                                operation_id: MetricId::new(),
                                table_type: TableType::PathBased,
                                correlation_id: None,
                            },
                            load_type,
                            duration: dur(),
                            outcome: SnapshotBuildOutcome::Success { version: 7 },
                        },
                    ));
                }
                SnapshotCompletionStatus::Failure => {
                    reporter.report(MetricEvent::SnapshotBuildCompleted(
                        SnapshotBuildCompleted {
                            context: MetricContext {
                                operation_id: MetricId::new(),
                                table_type: TableType::PathBased,
                                correlation_id: None,
                            },
                            load_type,
                            duration: dur(),
                            outcome: SnapshotBuildOutcome::Error,
                        },
                    ));
                }
            }
            let other_status = match status {
                SnapshotCompletionStatus::Success => SnapshotCompletionStatus::Failure,
                SnapshotCompletionStatus::Failure => SnapshotCompletionStatus::Success,
            };
            assert_eq!(reporter.snapshot_completion_count(status, load_type), 1);
            assert_eq!(
                reporter.snapshot_completion_count(other_status, load_type),
                0
            );
        }
    }

    #[test]
    fn report_log_segment_loaded_increments_log_replay_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::LogSegmentLoadCompleted(
            LogSegmentLoadCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                load_type: LogSegmentLoadType::Full,
                duration: dur(),
                outcome: LogSegmentLoadOutcome::Success {
                    stats: LogSegmentLoadStats {
                        num_commit_files: 7,
                        num_checkpoint_files: 2,
                        num_compaction_files: 1,
                        crc_versions_behind: Some(0),
                    },
                },
            },
        ));
        assert_eq!(reporter.log_segment_loads.get(), 1);
        assert_eq!(reporter.commit_files.get(), 7);
        assert_eq!(reporter.checkpoint_files.get(), 2);
        assert_eq!(reporter.compaction_files.get(), 1);
        assert_eq!(reporter.latest_crc_files_found.get(), 1);
    }

    #[test]
    fn report_log_segment_loaded_without_crc_does_not_increment_crc_counter() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::LogSegmentLoadCompleted(
            LogSegmentLoadCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                load_type: LogSegmentLoadType::Full,
                duration: dur(),
                outcome: LogSegmentLoadOutcome::Success {
                    stats: LogSegmentLoadStats {
                        num_commit_files: 3,
                        num_checkpoint_files: 1,
                        num_compaction_files: 0,
                        crc_versions_behind: None,
                    },
                },
            },
        ));
        assert_eq!(reporter.log_segment_loads.get(), 1);
        assert_eq!(reporter.latest_crc_files_found.get(), 0);
    }

    #[test]
    fn report_crc_read_completed_increments_crc_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::CrcReadCompleted(CrcReadCompleted {
            duration: dur(),
            outcome: CrcReadOutcome::Success { bytes_read: 512 },
        }));
        reporter.report(MetricEvent::CrcReadCompleted(CrcReadCompleted {
            duration: dur(),
            outcome: CrcReadOutcome::Success { bytes_read: 256 },
        }));
        assert_eq!(reporter.crc_read_calls.get(), 2);
        assert_eq!(reporter.crc_bytes_read.get(), 768);
    }

    #[test]
    fn report_domain_metadata_loaded_increments_domain_metadata_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::DomainMetadataLoadCompleted(
            DomainMetadataLoadCompleted {
                duration: dur(),
                outcome: DomainMetadataLoadOutcome::Success {
                    from_cache: true,
                    num_domains_returned: 3,
                },
            },
        ));
        reporter.report(MetricEvent::DomainMetadataLoadCompleted(
            DomainMetadataLoadCompleted {
                duration: dur(),
                outcome: DomainMetadataLoadOutcome::Success {
                    from_cache: false,
                    num_domains_returned: 1,
                },
            },
        ));
        assert_eq!(reporter.domain_metadata_loads.get(), 2);
        assert_eq!(reporter.domain_metadata_cache_hits.get(), 1);
        assert_eq!(reporter.domain_metadata_domains_returned.get(), 4);
    }

    #[test]
    fn report_set_transaction_loaded_increments_set_transaction_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::SetTransactionLoadCompleted(
            SetTransactionLoadCompleted {
                duration: dur(),
                outcome: SetTransactionLoadOutcome::Success {
                    from_cache: true,
                    found: true,
                },
            },
        ));
        reporter.report(MetricEvent::SetTransactionLoadCompleted(
            SetTransactionLoadCompleted {
                duration: dur(),
                outcome: SetTransactionLoadOutcome::Success {
                    from_cache: false,
                    found: false,
                },
            },
        ));
        assert_eq!(reporter.set_transaction_loads.get(), 2);
        assert_eq!(reporter.set_transaction_cache_hits.get(), 1);
        assert_eq!(reporter.set_transaction_found.get(), 1);
    }

    #[test]
    fn report_transaction_commit_success_accumulates_files_and_bytes() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::TransactionCommitCompleted(
            TransactionCommitCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                duration: dur(),
                outcome: TransactionCommitOutcome::Success {
                    stats: TransactionCommitStats {
                        commit_version: 1,
                        num_add_files: 3,
                        num_remove_files: 2,
                        num_dv_updates: 0,
                        add_files_bytes: 600,
                        remove_files_bytes: 150,
                        is_blind_append: false,
                        data_change: true,
                        operation: Some("WRITE".to_string()),
                        prepare_duration: dur(),
                        committer_duration: dur(),
                    },
                },
            },
        ));
        assert_eq!(reporter.transaction_commits.get(), 1);
        assert_eq!(reporter.commit_add_files.get(), 3);
        assert_eq!(reporter.commit_remove_files.get(), 2);
        assert_eq!(reporter.commit_add_bytes.get(), 600);
        assert_eq!(reporter.commit_remove_bytes.get(), 150);
    }

    #[test]
    fn report_transaction_commit_failure_increments_matching_reason_counter() {
        let reporter = CountingReporter::new();
        for reason in [
            CommitFailureReason::Conflict,
            CommitFailureReason::RetryableIo,
            CommitFailureReason::Error,
        ] {
            reporter.report(MetricEvent::TransactionCommitCompleted(
                TransactionCommitCompleted {
                    context: MetricContext {
                        operation_id: MetricId::new(),
                        table_type: TableType::PathBased,
                        correlation_id: None,
                    },
                    duration: std::time::Duration::ZERO,
                    outcome: TransactionCommitOutcome::Failure { reason },
                },
            ));
        }
        assert_eq!(reporter.commit_conflicts.get(), 1);
        assert_eq!(reporter.commit_retryable_failures.get(), 1);
        assert_eq!(reporter.commit_errors.get(), 1);
        assert_eq!(reporter.transaction_commits.get(), 0);
    }

    #[test]
    fn report_untracked_events_does_not_panic() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::ProtocolMetadataLoadCompleted(
            ProtocolMetadataLoadCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                load_type: LogSegmentLoadType::Full,
                duration: dur(),
                outcome: ProtocolMetadataLoadOutcome::Success {
                    source: ProtocolMetadataSource::FullReplay,
                },
            },
        ));
        assert_eq!(
            reporter.snapshot_completion_count(
                SnapshotCompletionStatus::Success,
                SnapshotLoadType::Full,
            ),
            0
        );
        assert_eq!(
            reporter.snapshot_completion_count(
                SnapshotCompletionStatus::Failure,
                SnapshotLoadType::Full,
            ),
            0
        );
    }

    #[test]
    fn reset_zeros_all_counters() {
        let reporter = Arc::new(CountingReporter::new());
        reporter.report(MetricEvent::StorageListCompleted(StorageListCompleted {
            duration: dur(),
            num_files: 10,
        }));
        reporter.report(MetricEvent::StorageReadCompleted(StorageReadCompleted {
            duration: dur(),
            num_files: 3,
            bytes_read: 1024,
        }));
        reporter.report(MetricEvent::StorageCopyCompleted(StorageCopyCompleted {
            duration: dur(),
        }));
        reporter.report(MetricEvent::LogSegmentLoadCompleted(
            LogSegmentLoadCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                load_type: LogSegmentLoadType::Full,
                duration: dur(),
                outcome: LogSegmentLoadOutcome::Success {
                    stats: LogSegmentLoadStats {
                        num_commit_files: 7,
                        num_checkpoint_files: 2,
                        num_compaction_files: 1,
                        crc_versions_behind: Some(0),
                    },
                },
            },
        ));
        reporter.report(MetricEvent::CrcReadCompleted(CrcReadCompleted {
            duration: dur(),
            outcome: CrcReadOutcome::Success { bytes_read: 512 },
        }));
        reporter.report(MetricEvent::DomainMetadataLoadCompleted(
            DomainMetadataLoadCompleted {
                duration: dur(),
                outcome: DomainMetadataLoadOutcome::Success {
                    from_cache: true,
                    num_domains_returned: 2,
                },
            },
        ));
        reporter.report(MetricEvent::SetTransactionLoadCompleted(
            SetTransactionLoadCompleted {
                duration: dur(),
                outcome: SetTransactionLoadOutcome::Success {
                    from_cache: true,
                    found: true,
                },
            },
        ));
        reporter.report(MetricEvent::SnapshotBuildCompleted(
            SnapshotBuildCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                load_type: SnapshotLoadType::SnapshotHint,
                duration: dur(),
                outcome: SnapshotBuildOutcome::Success { version: 0 },
            },
        ));
        reporter.report(MetricEvent::SnapshotBuildCompleted(
            SnapshotBuildCompleted {
                context: MetricContext {
                    operation_id: MetricId::new(),
                    table_type: TableType::PathBased,
                    correlation_id: None,
                },
                load_type: SnapshotLoadType::SnapshotHint,
                duration: dur(),
                outcome: SnapshotBuildOutcome::Error,
            },
        ));

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
        assert_eq!(
            reporter.snapshot_completion_count(
                SnapshotCompletionStatus::Success,
                SnapshotLoadType::SnapshotHint,
            ),
            0
        );
        assert_eq!(
            reporter.snapshot_completion_count(
                SnapshotCompletionStatus::Failure,
                SnapshotLoadType::SnapshotHint,
            ),
            0
        );
        assert_eq!(reporter.log_segment_loads.get(), 0);
        assert_eq!(reporter.commit_files.get(), 0);
        assert_eq!(reporter.checkpoint_files.get(), 0);
        assert_eq!(reporter.compaction_files.get(), 0);
        assert_eq!(reporter.latest_crc_files_found.get(), 0);
        assert_eq!(reporter.crc_read_calls.get(), 0);
        assert_eq!(reporter.crc_bytes_read.get(), 0);
        assert_eq!(reporter.domain_metadata_loads.get(), 0);
        assert_eq!(reporter.domain_metadata_load_failures.get(), 0);
        assert_eq!(reporter.domain_metadata_cache_hits.get(), 0);
        assert_eq!(reporter.domain_metadata_domains_returned.get(), 0);
        assert_eq!(reporter.set_transaction_loads.get(), 0);
        assert_eq!(reporter.set_transaction_load_failures.get(), 0);
        assert_eq!(reporter.set_transaction_cache_hits.get(), 0);
        assert_eq!(reporter.set_transaction_found.get(), 0);
    }

    #[test]
    fn report_load_failures_increment_failure_counters() {
        let reporter = CountingReporter::new();
        reporter.report(MetricEvent::DomainMetadataLoadCompleted(
            DomainMetadataLoadCompleted {
                outcome: DomainMetadataLoadOutcome::Error,
                duration: std::time::Duration::ZERO,
            },
        ));
        reporter.report(MetricEvent::SetTransactionLoadCompleted(
            SetTransactionLoadCompleted {
                outcome: SetTransactionLoadOutcome::Error,
                duration: std::time::Duration::ZERO,
            },
        ));
        assert_eq!(reporter.domain_metadata_load_failures.get(), 1);
        assert_eq!(reporter.set_transaction_load_failures.get(), 1);
        assert_eq!(reporter.domain_metadata_loads.get(), 0);
        assert_eq!(reporter.set_transaction_loads.get(), 0);
    }
}
