use std::sync::Arc;
use std::time::Instant;

use delta_kernel_derive::internal_api;
use tracing::{info, info_span, Span};

use crate::log_replay::{ActionsBatch, ParallelLogReplayProcessor};
use crate::metrics::{MetricId, MetricsReporter};
use crate::parallel::parallel_phase::ParallelPhase;
use crate::parallel::sequential_phase::{AfterSequential, SequentialPhase};
use crate::scan::log_replay::{ScanLogReplayProcessor, SerializableScanState};
use crate::scan::ScanMetadata;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, EngineData, Error, FileMeta};

/// Result of sequential scan metadata processing.
///
/// This enum indicates whether distributed processing is needed:
/// - `Done`: All processing completed sequentially - no distributed phase needed.
/// - `Parallel`: Contains state and files for parallel processing.
pub enum AfterSequentialScanMetadata {
    Done,
    Parallel {
        state: Box<ParallelState>,
        files: Vec<FileMeta>,
    },
}

/// Sequential scan metadata processing.
///
/// This phase processes commits and single-part checkpoint manifests sequentially.
/// After exhaustion, call `finish()` to get the result which indicates whether
/// a distributed phase is needed.
pub struct SequentialScanMetadata {
    pub(crate) sequential: SequentialPhase<ScanLogReplayProcessor>,
    span: Span,
    operation_id: MetricId,
    start: Instant,
    reporter: Option<Arc<dyn MetricsReporter>>,
}

impl SequentialScanMetadata {
    pub(crate) fn new(
        sequential: SequentialPhase<ScanLogReplayProcessor>,
        reporter: Option<Arc<dyn MetricsReporter>>,
    ) -> Self {
        let operation_id = MetricId::new();
        Self {
            sequential,
            span: info_span!("sequential_scan_metadata", %operation_id),
            operation_id,
            start: Instant::now(),
            reporter,
        }
    }

    pub fn finish(self) -> DeltaResult<AfterSequentialScanMetadata> {
        let _guard = self.span.enter();
        match self.sequential.finish()? {
            AfterSequential::Done(processor) => {
                let event = processor
                    .get_metrics()
                    .to_event(self.operation_id, self.start.elapsed());
                info!(%event, "Sequential scan metadata completed");
                if let Some(r) = &self.reporter {
                    r.report(event);
                }
                Ok(AfterSequentialScanMetadata::Done)
            }
            AfterSequential::Parallel { processor, files } => {
                // Emit sequential phase metrics
                let event = processor
                    .get_metrics()
                    .to_event(self.operation_id, self.start.elapsed());
                info!(%event, "Sequential scan metadata completed");
                if let Some(r) = &self.reporter {
                    r.report(event);
                }
                processor.get_metrics().reset_counters();

                Ok(AfterSequentialScanMetadata::Parallel {
                    state: Box::new(ParallelState {
                        inner: processor,
                        operation_id: self.operation_id,
                        start: Instant::now(),
                        reporter: self.reporter,
                    }),
                    files,
                })
            }
        }
    }
}

impl Iterator for SequentialScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        let _guard = self.span.enter();
        self.sequential.next()
    }
}

/// State for parallel scan metadata processing.
///
/// This state can be serialized and distributed to remote workers, or wrapped
/// in Arc and shared across threads for local parallel processing.
pub struct ParallelState {
    inner: ScanLogReplayProcessor,
    operation_id: MetricId,
    start: Instant,
    reporter: Option<Arc<dyn MetricsReporter>>,
}

impl ParallelLogReplayProcessor for Arc<ParallelState> {
    type Output = ScanMetadata;

    fn process_actions_batch(&self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
        self.inner.process_actions_batch(actions_batch)
    }
}

impl ParallelState {
    /// Report the accumulated metrics from parallel processing.
    ///
    /// Call this after all parallel workers complete. The metrics will be logged
    /// and reported via the metrics reporter.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use delta_kernel::scan::ParallelState;
    /// # use tracing::instrument;
    /// #[instrument(skip_all, name = "parallel_scan")]
    /// async fn process(state: Arc<ParallelState>) {
    ///     // ... spawn workers that share Arc<ParallelState> ...
    ///     // ... wait for workers to complete ...
    ///
    ///     // Report accumulated metrics
    ///     state.report_metrics();
    /// }
    /// ```
    pub fn report_metrics(&self) {
        let event = self
            .inner
            .get_metrics()
            .to_event(self.operation_id, self.start.elapsed());
        info!(%event, "Parallel scan metadata completed");
        if let Some(r) = &self.reporter {
            r.report(event);
        }
    }

    /// Get the schema to use for reading checkpoint files.
    ///
    /// Returns the checkpoint read schema which may have stats excluded
    /// if skip_stats was enabled when the scan was created.
    pub fn file_read_schema(&self) -> SchemaRef {
        self.inner.checkpoint_info().checkpoint_read_schema.clone()
    }

    /// Serialize the processor state for distributed processing.
    ///
    /// Returns a `SerializableScanState` containing all information needed to
    /// reconstruct this state on remote compute nodes.
    ///
    /// # Errors
    /// Returns an error if the state cannot be serialized (e.g., contains opaque predicates).
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn into_serializable_state(self) -> DeltaResult<SerializableScanState> {
        self.inner.into_serializable_state()
    }

    /// Reconstruct a ParallelState from serialized state.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators and filters
    /// - `state`: The serialized state from a previous `into_serializable_state()` call
    /// - `operation_id`: The operation ID to use for metrics correlation
    /// - `reporter`: Optional metrics reporter
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn from_serializable_state(
        engine: &dyn Engine,
        state: SerializableScanState,
        operation_id: MetricId,
        reporter: Option<Arc<dyn MetricsReporter>>,
    ) -> DeltaResult<Self> {
        let inner = ScanLogReplayProcessor::from_serializable_state(engine, state)?;
        Ok(Self {
            inner,
            operation_id,
            start: Instant::now(),
            reporter,
        })
    }

    /// Serialize the processor state directly to bytes.
    ///
    /// This is a convenience method that combines `into_serializable_state()` with
    /// JSON serialization. For more control over serialization format, use
    /// `into_serializable_state()` directly.
    ///
    /// # Errors
    /// Returns an error if the state cannot be serialized.
    #[allow(unused)]
    pub fn into_bytes(self) -> DeltaResult<Vec<u8>> {
        let state = self.into_serializable_state()?;
        serde_json::to_vec(&state)
            .map_err(|e| Error::generic(format!("Failed to serialize ParallelState to bytes: {e}")))
    }

    /// Reconstruct a ParallelState from bytes.
    ///
    /// This is a convenience method that combines JSON deserialization with
    /// `from_serializable_state()`. The bytes must have been produced by `into_bytes()`.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators and filters
    /// - `bytes`: The serialized bytes from a previous `into_bytes()` call
    /// - `operation_id`: The operation ID to use for metrics correlation
    /// - `reporter`: Optional metrics reporter
    #[allow(unused)]
    pub fn from_bytes(
        engine: &dyn Engine,
        bytes: &[u8],
        operation_id: MetricId,
        reporter: Option<Arc<dyn MetricsReporter>>,
    ) -> DeltaResult<Self> {
        let state: SerializableScanState =
            serde_json::from_slice(bytes).map_err(Error::MalformedJson)?;
        Self::from_serializable_state(engine, state, operation_id, reporter)
    }
}

pub struct ParallelScanMetadata {
    pub(crate) processor: ParallelPhase<Arc<ParallelState>>,
    span: Span,
}

impl ParallelScanMetadata {
    pub fn try_new(
        engine: Arc<dyn Engine>,
        state: Arc<ParallelState>,
        leaf_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let read_schema = state.file_read_schema();
        Ok(Self {
            processor: ParallelPhase::try_new(engine, state, leaf_files, read_schema)?,
            // TODO: Associate the same scan ID from sequential phase to correlate phases
            span: info_span!("parallel_scan_metadata"),
        })
    }

    pub fn new_from_iter(
        state: Arc<ParallelState>,
        iter: impl IntoIterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        Self {
            processor: ParallelPhase::new_from_iter(state.clone(), iter),
            // TODO: Associate the same scan ID from sequential phase to correlate phases
            span: info_span!("parallel_scan_metadata"),
        }
    }
}

impl Iterator for ParallelScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        let _guard = self.span.enter();
        self.processor.next()
    }
}
