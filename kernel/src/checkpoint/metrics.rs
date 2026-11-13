use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::Span;

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
            CheckpointPhase::Input => &counters.input_duration_ns,
            CheckpointPhase::Reconciliation => &counters.reconciliation_duration_ns,
            CheckpointPhase::StatsTransform => &counters.stats_transform_duration_ns,
            CheckpointPhase::OutputPreparation => &counters.output_preparation_duration_ns,
        };
        Some(CheckpointTimer {
            elapsed,
            start: Instant::now(),
        })
    }

    pub(crate) fn record_input(&self, batch: &ActionsBatch) {
        let Some(counters) = &self.0 else { return };
        let rows = if batch.is_log_batch {
            &counters.num_log_rows
        } else {
            &counters.num_checkpoint_rows
        };
        rows.fetch_add(batch.actions.len() as u64, Ordering::Relaxed);
    }
}

pub(crate) enum CheckpointPhase {
    Input,
    Reconciliation,
    StatsTransform,
    OutputPreparation,
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
    input_duration_ns: AtomicU64,
    reconciliation_duration_ns: AtomicU64,
    stats_transform_duration_ns: AtomicU64,
    output_preparation_duration_ns: AtomicU64,
    num_log_rows: AtomicU64,
    num_checkpoint_rows: AtomicU64,
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
            ("input_duration_ns", &counters.input_duration_ns),
            (
                "reconciliation_duration_ns",
                &counters.reconciliation_duration_ns,
            ),
            (
                "stats_transform_duration_ns",
                &counters.stats_transform_duration_ns,
            ),
            (
                "output_preparation_duration_ns",
                &counters.output_preparation_duration_ns,
            ),
            ("num_log_rows", &counters.num_log_rows),
            ("num_checkpoint_rows", &counters.num_checkpoint_rows),
        ] {
            self.span.record(name, counter.load(Ordering::Relaxed));
        }
    }
}
