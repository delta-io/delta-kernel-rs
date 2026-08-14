//! Metrics for scan log replay operations.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::metrics::{MetricId, ScanMetadataCompleted, ScanType, TableType};

/// Metrics collected during scan log replay. Metrics are updated and read using relaxed ordering
/// to keep updates fast across parallel executing threads.
pub(crate) struct ScanMetrics {
    /// Add files that entered deduplication. This normally excludes add files filtered by data
    /// skipping. During parse-error fallback, deduplication runs first, so add files filtered by
    /// retry-time data skipping are included.
    num_add_files_seen: AtomicU64,
    /// Add files that survived log replay (files to read). includes files that survived
    /// dataskipping, partition pruning, and add/remove deduplication.
    num_active_add_files: AtomicU64,
    /// Number of bytes in the active add files as reported by the add action size field
    active_add_files_bytes: AtomicU64,
    /// Remove files seen (from delta/commit files only).
    num_remove_files_seen: AtomicU64,
    /// Non-file actions seen (protocol, metadata, etc.).
    num_non_file_actions: AtomicU64,
    /// Files filtered by predicates (data skipping + partition pruning).
    num_predicate_filtered: AtomicU64,
    /// Peak size of the deduplication hash set.
    peak_hash_set_size: AtomicUsize,
    /// Time spent in the deduplication visitor (ns).
    dedup_visitor_time_ns: AtomicU64,
    /// Time spent evaluating predicates (ns). This includes data skipping and partition pruning.
    predicate_eval_time_ns: AtomicU64,
}

impl Default for ScanMetrics {
    fn default() -> Self {
        Self {
            num_add_files_seen: AtomicU64::new(0),
            num_active_add_files: AtomicU64::new(0),
            active_add_files_bytes: AtomicU64::new(0),
            num_remove_files_seen: AtomicU64::new(0),
            num_non_file_actions: AtomicU64::new(0),
            num_predicate_filtered: AtomicU64::new(0),
            peak_hash_set_size: AtomicUsize::new(0),
            dedup_visitor_time_ns: AtomicU64::new(0),
            predicate_eval_time_ns: AtomicU64::new(0),
        }
    }
}

impl ScanMetrics {
    pub(crate) fn incr_add_files_seen(&self) {
        self.num_add_files_seen.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that we've seen an active add file, plus its size
    pub(crate) fn record_active_add_file(&self, bytes: u64) {
        self.num_active_add_files.fetch_add(1, Ordering::Relaxed);
        self.active_add_files_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn incr_remove_files_seen(&self) {
        self.num_remove_files_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_non_file_actions(&self) {
        self.num_non_file_actions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_predicate_filtered(&self, value: u64) {
        self.num_predicate_filtered
            .fetch_add(value, Ordering::Relaxed);
    }

    pub(crate) fn update_peak_hash_set_size(&self, value: usize) {
        self.peak_hash_set_size.fetch_max(value, Ordering::Relaxed);
    }

    pub(crate) fn add_dedup_visitor_time_ns(&self, duration_ns: u64) {
        self.dedup_visitor_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn add_predicate_eval_time_ns(&self, duration_ns: u64) {
        self.predicate_eval_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Reset counters to zero for a new phase.
    ///
    /// This is used between sequential and parallel phases to get fresh metrics
    /// without reconstructing the entire processor. The peak hash set size is
    /// preserved since it represents a high-water mark across all phases.
    pub(crate) fn reset_counters(&self) {
        self.num_add_files_seen.store(0, Ordering::Relaxed);
        self.num_active_add_files.store(0, Ordering::Relaxed);
        self.active_add_files_bytes.store(0, Ordering::Relaxed);
        self.num_remove_files_seen.store(0, Ordering::Relaxed);
        self.num_non_file_actions.store(0, Ordering::Relaxed);
        self.num_predicate_filtered.store(0, Ordering::Relaxed);
        self.dedup_visitor_time_ns.store(0, Ordering::Relaxed);
        self.predicate_eval_time_ns.store(0, Ordering::Relaxed);
    }

    /// Snapshot all counters into a [`ScanMetadataCompleted`] event payload.
    ///
    /// `scan_type` identifies whether this event was emitted by full scan metadata replay or by
    /// a phase of parallel scan metadata replay.
    pub(crate) fn to_event(
        &self,
        operation_id: MetricId,
        is_catalog_managed: bool,
        correlation_id: Option<Arc<str>>,
        scan_type: ScanType,
        duration: Duration,
    ) -> ScanMetadataCompleted {
        ScanMetadataCompleted {
            operation_id,
            table_type: TableType::from_catalog_managed(is_catalog_managed),
            correlation_id,
            scan_type,
            duration,
            num_add_files_seen: self.num_add_files_seen.load(Ordering::Relaxed),
            num_active_add_files: self.num_active_add_files.load(Ordering::Relaxed),
            active_add_files_bytes: self.active_add_files_bytes.load(Ordering::Relaxed),
            num_remove_files_seen: self.num_remove_files_seen.load(Ordering::Relaxed),
            num_non_file_actions: self.num_non_file_actions.load(Ordering::Relaxed),
            num_predicate_filtered: self.num_predicate_filtered.load(Ordering::Relaxed),
            peak_hash_set_size: self.peak_hash_set_size.load(Ordering::Relaxed),
            dedup_visitor_time: Duration::from_nanos(
                self.dedup_visitor_time_ns.load(Ordering::Relaxed),
            ),
            predicate_eval_time: Duration::from_nanos(
                self.predicate_eval_time_ns.load(Ordering::Relaxed),
            ),
        }
    }

    /// Log all metrics with a message in the current tracing span context.
    pub(crate) fn log(&self, message: impl AsRef<str>) {
        let add_files_seen = self.num_add_files_seen.load(Ordering::Relaxed);
        let active_add_files = self.num_active_add_files.load(Ordering::Relaxed);
        let active_add_files_bytes = self.active_add_files_bytes.load(Ordering::Relaxed);
        let remove_files_seen = self.num_remove_files_seen.load(Ordering::Relaxed);
        let non_file_actions = self.num_non_file_actions.load(Ordering::Relaxed);
        let predicate_filtered = self.num_predicate_filtered.load(Ordering::Relaxed);
        let peak_hash_set_size = self.peak_hash_set_size.load(Ordering::Relaxed);
        let dedup_visitor_time_ns = self.dedup_visitor_time_ns.load(Ordering::Relaxed);
        let predicate_eval_time_ns = self.predicate_eval_time_ns.load(Ordering::Relaxed);
        info!(
            add_files_seen,
            active_add_files,
            active_add_files_bytes,
            remove_files_seen,
            non_file_actions,
            predicate_filtered,
            peak_hash_set_size,
            dedup_visitor_time_ns,
            predicate_eval_time_ns,
            "{}",
            message.as_ref()
        );
    }
}
