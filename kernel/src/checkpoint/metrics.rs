//! Collects batch processing metrics for one [`crate::Snapshot::checkpoint`] attempt.
//!
//! Clones share timers for input, reconciliation, statistics transforms, and output preparation,
//! plus input action counts from the log tail and previous checkpoint. See
//! [`crate::metrics::CheckpointProcessingStats`] for the timing and counting boundaries.
//! A report guard records the accumulated values when the attempt ends, including on early returns.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::Span;

use crate::log_replay::ActionsBatch;

/// Shared by the batch pipeline of one [`crate::Snapshot::checkpoint`] attempt.
///
/// [`super::CheckpointWriter::checkpoint_data`] uses a disabled collector: that API leaves writing
/// and finalization to the caller and does not emit a checkpoint-write metric event.
#[derive(Clone, Default)]
pub(crate) struct CheckpointMetrics(Option<Arc<CheckpointCounters>>);

impl CheckpointMetrics {
    pub(crate) fn new(span: &Span) -> Self {
        Self((!span.is_disabled()).then(Default::default))
    }

    pub(crate) fn disabled() -> Self {
        Self(None)
    }

    /// Reports accumulated metrics on drop. Keep the guard alive until the checkpoint attempt ends
    /// so that it includes all completed processing work.
    #[must_use = "dropping this guard early records incomplete checkpoint metrics"]
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
            &counters.num_actions_from_log_tail
        } else {
            &counters.num_actions_from_checkpoint
        };
        rows.fetch_add(batch.actions.len() as u64, Ordering::Relaxed);
    }
}

/// Timed sections of checkpoint batch production.
///
/// Each section excludes pulls from earlier stages. Timers record elapsed time on scope exit,
/// including error returns; [`Self::Input`] times input setup and iterator pulls themselves.
pub(crate) enum CheckpointPhase {
    /// Building the input through `LogSegment::read_actions` and advancing its iterator over log,
    /// checkpoint, and sidecar batches. Includes final and failing pulls, plus input consumed by
    /// collation sampling and the first sidecar peek.
    Input,
    /// Processing each received batch with `ActionReconciliationProcessor`, before dropping batches
    /// with no selected rows. Includes action deduplication, retention checks, and selection-vector
    /// construction. Input iterator calls have already completed.
    Reconciliation,
    /// Evaluating statistics and partition-value transforms on reconciled batches with selected
    /// rows, including wrapping the result in `FilteredEngineData`. Excludes evaluator setup and
    /// the checkpoint metadata action generated for V2 output, which bypasses these transforms.
    StatsTransform,
    /// Preparing an output batch after its upstream iterator returns. Without sidecars, covers
    /// selection-vector application. With sidecars, also covers both action pickers and buffering
    /// non-file batches in `SidecarSplitter`. Excludes Parquet encoding and storage writes.
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
    num_actions_from_log_tail: AtomicU64,
    num_actions_from_checkpoint: AtomicU64,
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
            (
                "num_actions_from_log_tail",
                &counters.num_actions_from_log_tail,
            ),
            (
                "num_actions_from_checkpoint",
                &counters.num_actions_from_checkpoint,
            ),
        ] {
            self.span.record(name, counter.load(Ordering::Relaxed));
        }
    }
}
