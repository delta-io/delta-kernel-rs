//! Metrics for scan log replay operations.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use delta_kernel_derive::internal_api;
use tracing::info;

/// Metrics collected during scan log replay.
///
/// These metrics track file action counts during log replay. The counters track actions as they
/// are encountered during processing, before deduplication filtering.
#[internal_api]
pub(crate) struct ScanMetrics {
    /// Number of add actions seen during log replay (before deduplication)
    num_add_actions_seen: AtomicU64,
    /// Number of remove actions seen during log replay (before deduplication)
    num_remove_actions_seen: AtomicU64,
    num_non_file_actions: AtomicU64,
    hash_set_size: AtomicUsize,
    data_skipping_filtered: AtomicU64,
    partition_pruning_filtered: AtomicU64,
    // Timing metrics (in nanoseconds)
    dedup_visitor_time_ns: AtomicU64,
    data_skipping_time_ns: AtomicU64,
    partition_pruning_time_ns: AtomicU64,
}

impl Default for ScanMetrics {
    fn default() -> Self {
        Self {
            num_add_actions_seen: AtomicU64::new(0),
            num_remove_actions_seen: AtomicU64::new(0),
            num_non_file_actions: AtomicU64::new(0),
            hash_set_size: AtomicUsize::new(0),
            data_skipping_filtered: AtomicU64::new(0),
            partition_pruning_filtered: AtomicU64::new(0),
            dedup_visitor_time_ns: AtomicU64::new(0),
            data_skipping_time_ns: AtomicU64::new(0),
            partition_pruning_time_ns: AtomicU64::new(0),
        }
    }
}

impl ScanMetrics {
    /// Resets all counters to zero. This must be called before starting a new phase of log replay.
    ///
    /// The `reset_counters` call uses `SeqCst` ordering to establish a happens-before relationship
    /// with subsequent `Relaxed` increments, ensuring all threads see the reset before any
    /// increments occur.
    pub(crate) fn reset_counters(&self) {
        // NOTE: We do not reset hash set size because that never decreases. All subsequent uses of
        // the processor will reuse the same hashset.
        self.num_add_actions_seen.store(0, Ordering::SeqCst);
        self.num_remove_actions_seen.store(0, Ordering::SeqCst);
        self.num_non_file_actions.store(0, Ordering::SeqCst);
        self.data_skipping_filtered.store(0, Ordering::SeqCst);
        self.partition_pruning_filtered.store(0, Ordering::SeqCst);
        self.dedup_visitor_time_ns.store(0, Ordering::SeqCst);
        self.data_skipping_time_ns.store(0, Ordering::SeqCst);
        self.partition_pruning_time_ns.store(0, Ordering::SeqCst);
    }

    pub(crate) fn incr_add_actions_seen(&self) {
        self.num_add_actions_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_remove_actions_seen(&self) {
        self.num_remove_actions_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_partition_pruning_filtered(&self) {
        self.partition_pruning_filtered
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_non_file_actions(&self) {
        self.num_non_file_actions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_data_skipping_filtered(&self, value: u64) {
        self.data_skipping_filtered
            .fetch_add(value, Ordering::Relaxed);
    }

    pub(crate) fn set_hash_set(&self, value: usize) {
        self.hash_set_size.fetch_max(value, Ordering::SeqCst);
    }

    pub(crate) fn add_dedup_visitor_time_ns(&self, duration_ns: u64) {
        self.dedup_visitor_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn add_data_skipping_time_ns(&self, duration_ns: u64) {
        self.data_skipping_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn add_partition_pruning_time_ns(&self, duration_ns: u64) {
        self.partition_pruning_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn log_with_message(&self, message: impl AsRef<str>) {
        let num_adds = self.num_add_actions_seen.load(Ordering::Relaxed);
        let num_removes = self.num_remove_actions_seen.load(Ordering::Relaxed);
        let num_non_file_actions = self.num_non_file_actions.load(Ordering::Relaxed);
        let hash_set_size = self.hash_set_size.load(Ordering::Relaxed);
        let data_skipping_filtered = self.data_skipping_filtered.load(Ordering::Relaxed);
        let partition_pruning_filtered = self.partition_pruning_filtered.load(Ordering::Relaxed);
        let dedup_visitor_time_ms = self.dedup_visitor_time_ns.load(Ordering::Relaxed) / 1_000_000;
        let data_skipping_time_ms = self.data_skipping_time_ns.load(Ordering::Relaxed) / 1_000_000;
        let partition_pruning_time_ms =
            self.partition_pruning_time_ns.load(Ordering::Relaxed) / 1_000_000;
        info!(
            num_adds,
            num_removes,
            num_non_file_actions,
            hash_set_size,
            data_skipping_filtered,
            partition_pruning_filtered,
            dedup_visitor_time_ms,
            data_skipping_time_ms,
            partition_pruning_time_ms,
            "{}",
            message.as_ref()
        );
    }
}
