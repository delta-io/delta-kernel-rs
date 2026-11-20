//! Collects processing and output metrics for one [`crate::Snapshot::checkpoint`] attempt.
//!
//! Clones share timers and counters. See [`crate::metrics::CheckpointProcessingStats`] and
//! [`crate::metrics::CheckpointOutputStats`] for the timing and counting boundaries.
//! A report guard records the accumulated values when the attempt ends, including on early returns.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tracing::Span;
use url::Url;

use crate::log_replay::ActionsBatch;
use crate::{FileDataReadResultIterator, FileSize, KernelResult, ParquetHandler};

/// Shared by processing and output work in one [`crate::Snapshot::checkpoint`] attempt.
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
            CheckpointPhase::LogTailInput => &counters.log_tail_input_duration_ns,
            CheckpointPhase::CheckpointInput => &counters.checkpoint_input_duration_ns,
            CheckpointPhase::Reconciliation => &counters.reconciliation_duration_ns,
            CheckpointPhase::StatsTransform => &counters.stats_transform_duration_ns,
            CheckpointPhase::OutputPreparation => &counters.output_preparation_duration_ns,
            CheckpointPhase::Verification => &counters.verification_duration_ns,
        };
        Some(CheckpointTimer {
            elapsed,
            start: Instant::now(),
        })
    }

    /// Wraps a source before chaining so terminal and failing pulls remain attributable.
    pub(crate) fn input_iterator<I>(
        &self,
        phase: CheckpointPhase,
        input: I,
    ) -> CheckpointInputIterator<I> {
        CheckpointInputIterator {
            input,
            metrics: self.clone(),
            phase,
        }
    }

    /// Times a Parquet call, excluding only pulls made by that call. A sidecar's initial peek
    /// happens before this method, so subtracting the attempt's processing totals would undercount.
    pub(crate) fn write_parquet_file(
        &self,
        parquet: &dyn ParquetHandler,
        location: Url,
        mut data: FileDataReadResultIterator,
        is_sidecar: bool,
    ) -> KernelResult<FileSize> {
        let Some(counters) = &self.0 else {
            return parquet.write_parquet_file(location, data);
        };
        let input_duration_ns = Arc::new(AtomicU64::default());
        let input_elapsed = input_duration_ns.clone();
        let data = std::iter::from_fn(move || {
            let _timer = CheckpointTimer {
                elapsed: &input_elapsed,
                start: Instant::now(),
            };
            data.next()
        });
        let start = Instant::now();
        let result = parquet.write_parquet_file(location, Box::new(data));
        let duration = start.elapsed().saturating_sub(Duration::from_nanos(
            input_duration_ns.load(Ordering::Relaxed),
        ));
        counters.parquet_write_duration_ns.fetch_add(
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if let Ok(size) = &result {
            counters.bytes_written.fetch_add(*size, Ordering::Relaxed);
            if is_sidecar {
                counters
                    .num_sidecars_written
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    fn record_input(&self, batch: &ActionsBatch) {
        let Some(counters) = &self.0 else { return };
        let rows = if batch.is_log_batch {
            &counters.num_actions_from_log_tail
        } else {
            &counters.num_actions_from_checkpoint
        };
        rows.fetch_add(batch.actions.len() as u64, Ordering::Relaxed);
    }

    #[cfg(feature = "incremental-checkpoints-in-dev")]
    pub(crate) fn record_checkpoint_rows(&self, rows: usize) {
        if let Some(counters) = &self.0 {
            counters
                .num_actions_from_checkpoint
                .fetch_add(rows as u64, Ordering::Relaxed);
        }
    }
}

/// Timed sections of checkpoint input, processing, and verification.
///
/// Each section excludes pulls from earlier stages. Timers record elapsed time on scope exit,
/// including error returns. Input timers include setup and iterator pulls themselves, including
/// input consumed by collation sampling and the first sidecar peek.
#[derive(Clone, Copy)]
pub(crate) enum CheckpointPhase {
    /// Constructing and advancing the original-commit stream in `LogSegment`, before it is
    /// chained with checkpoint input. Includes final and failing calls that produce no batch.
    LogTailInput,
    /// Constructing and advancing the previous-checkpoint stream in `LogSegment`, including schema
    /// and footer reads, sidecar-reference extraction, and sidecar input. Includes final and
    /// failing calls. The source's role determines this phase, including for a JSON main
    /// checkpoint.
    CheckpointInput,
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
    /// Querying storage metadata and comparing the reported and observed sizes of each newly
    /// written Parquet file. Includes failed metadata requests and size mismatches.
    Verification,
}

pub(crate) struct CheckpointInputIterator<I> {
    input: I,
    metrics: CheckpointMetrics,
    phase: CheckpointPhase,
}

impl<I: Iterator<Item = KernelResult<ActionsBatch>>> Iterator for CheckpointInputIterator<I> {
    type Item = KernelResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = {
            let _timer = self.metrics.start(self.phase);
            self.input.next()
        };
        if let Some(Ok(batch)) = &batch {
            self.metrics.record_input(batch);
        }
        batch
    }
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
    log_tail_input_duration_ns: AtomicU64,
    checkpoint_input_duration_ns: AtomicU64,
    reconciliation_duration_ns: AtomicU64,
    stats_transform_duration_ns: AtomicU64,
    output_preparation_duration_ns: AtomicU64,
    num_actions_from_log_tail: AtomicU64,
    num_actions_from_checkpoint: AtomicU64,
    parquet_write_duration_ns: AtomicU64,
    verification_duration_ns: AtomicU64,
    bytes_written: AtomicU64,
    num_sidecars_written: AtomicU64,
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
            (
                "log_tail_input_duration_ns",
                &counters.log_tail_input_duration_ns,
            ),
            (
                "checkpoint_input_duration_ns",
                &counters.checkpoint_input_duration_ns,
            ),
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
            (
                "parquet_write_duration_ns",
                &counters.parquet_write_duration_ns,
            ),
            (
                "verification_duration_ns",
                &counters.verification_duration_ns,
            ),
            ("bytes_written", &counters.bytes_written),
            ("num_sidecars_written", &counters.num_sidecars_written),
        ] {
            self.span.record(name, counter.load(Ordering::Relaxed));
        }
    }
}
