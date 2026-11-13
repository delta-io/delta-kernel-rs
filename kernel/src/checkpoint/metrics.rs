use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::Span;

use crate::action_reconciliation::log_replay::ReconciliationMetrics;
use crate::log_replay::ActionsBatch;

/// Shared only by the batch pipeline of one checkpoint attempt. Custom writer calls leave it off.
#[derive(Clone, Default)]
pub(crate) struct CheckpointMetrics(Option<Arc<CheckpointCounters>>);

impl CheckpointMetrics {
    pub(crate) fn new(span: &Span) -> Self {
        Self((!span.is_disabled()).then(Default::default))
    }

    pub(crate) fn report_on_drop(&self, span: Span) -> impl Drop + '_ {
        CheckpointMetricsReport {
            metrics: self,
            span,
        }
    }

    pub(crate) fn start(&self, phase: CheckpointPhase) -> Option<CheckpointTimer<'_>> {
        let counters = self.0.as_ref()?;
        let elapsed = match phase {
            CheckpointPhase::InputSetup => &counters.input_setup_duration_ns,
            CheckpointPhase::InputRead => &counters.input_read_duration_ns,
            CheckpointPhase::Reconciliation => &counters.reconciliation_duration_ns,
            CheckpointPhase::StatsTransform => &counters.stats_transform_duration_ns,
            CheckpointPhase::Selection => &counters.selection_duration_ns,
            CheckpointPhase::SidecarSplit => &counters.sidecar_split_duration_ns,
        };
        Some(CheckpointTimer {
            elapsed,
            start: Instant::now(),
        })
    }

    pub(crate) fn record_input(&self, batch: &ActionsBatch) {
        let Some(counters) = &self.0 else { return };
        counters.num_input_batches.fetch_add(1, Ordering::Relaxed);
        let rows = if batch.is_log_batch {
            &counters.num_log_rows
        } else {
            &counters.num_checkpoint_rows
        };
        rows.fetch_add(batch.actions.len() as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_reconciliation(&self, metrics: &ReconciliationMetrics) {
        let Some(counters) = &self.0 else { return };
        counters
            .num_add_files_seen
            .store(metrics.num_add_files_seen, Ordering::Relaxed);
        counters
            .num_remove_files_seen
            .store(metrics.num_remove_files_seen, Ordering::Relaxed);
        counters
            .num_duplicate_file_actions
            .store(metrics.num_duplicate_file_actions, Ordering::Relaxed);
        counters
            .num_expired_tombstones
            .store(metrics.num_expired_tombstones, Ordering::Relaxed);
        counters
            .num_reconciled_actions
            .store(metrics.num_reconciled_actions, Ordering::Relaxed);
        counters
            .peak_hash_set_size
            .store(metrics.peak_hash_set_size, Ordering::Relaxed);
    }
}

pub(crate) enum CheckpointPhase {
    InputSetup,
    InputRead,
    Reconciliation,
    StatsTransform,
    Selection,
    SidecarSplit,
}

pub(crate) struct CheckpointTimer<'a> {
    elapsed: &'a AtomicU64,
    start: Instant,
}

impl Drop for CheckpointTimer<'_> {
    fn drop(&mut self) {
        self.elapsed.fetch_add(
            u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

#[derive(Default)]
struct CheckpointCounters {
    input_setup_duration_ns: AtomicU64,
    input_read_duration_ns: AtomicU64,
    reconciliation_duration_ns: AtomicU64,
    stats_transform_duration_ns: AtomicU64,
    selection_duration_ns: AtomicU64,
    sidecar_split_duration_ns: AtomicU64,
    num_input_batches: AtomicU64,
    num_log_rows: AtomicU64,
    num_checkpoint_rows: AtomicU64,
    num_add_files_seen: AtomicU64,
    num_remove_files_seen: AtomicU64,
    num_duplicate_file_actions: AtomicU64,
    num_expired_tombstones: AtomicU64,
    num_reconciled_actions: AtomicU64,
    peak_hash_set_size: AtomicU64,
}

struct CheckpointMetricsReport<'a> {
    metrics: &'a CheckpointMetrics,
    span: Span,
}

impl Drop for CheckpointMetricsReport<'_> {
    fn drop(&mut self) {
        let Some(counters) = &self.metrics.0 else {
            return;
        };
        // Record before #[instrument(err)] converts the event, including on early returns.
        for (name, counter) in [
            ("input_setup_duration_ns", &counters.input_setup_duration_ns),
            ("input_read_duration_ns", &counters.input_read_duration_ns),
            (
                "reconciliation_duration_ns",
                &counters.reconciliation_duration_ns,
            ),
            (
                "stats_transform_duration_ns",
                &counters.stats_transform_duration_ns,
            ),
            ("selection_duration_ns", &counters.selection_duration_ns),
            (
                "sidecar_split_duration_ns",
                &counters.sidecar_split_duration_ns,
            ),
            ("num_input_batches", &counters.num_input_batches),
            ("num_log_rows", &counters.num_log_rows),
            ("num_checkpoint_rows", &counters.num_checkpoint_rows),
            ("num_add_files_seen", &counters.num_add_files_seen),
            ("num_remove_files_seen", &counters.num_remove_files_seen),
            (
                "num_duplicate_file_actions",
                &counters.num_duplicate_file_actions,
            ),
            ("num_expired_tombstones", &counters.num_expired_tombstones),
            ("num_reconciled_actions", &counters.num_reconciled_actions),
            ("peak_hash_set_size", &counters.peak_hash_set_size),
        ] {
            self.span.record(name, counter.load(Ordering::Relaxed));
        }
    }
}
