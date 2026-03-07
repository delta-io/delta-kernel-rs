use std::sync::Arc;
use std::time::Instant;

use delta_kernel_derive::internal_api;

use crate::log_replay::{ActionsBatch, ParallelLogReplayProcessor};
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
    pub(crate) sequential_span: tracing::Span,
    pub(crate) scan_span: tracing::Span,
    start_time: Instant,
}

impl SequentialScanMetadata {
    pub(crate) fn new(
        sequential: SequentialPhase<ScanLogReplayProcessor>,
        scan_span: tracing::Span,
    ) -> Self {
        let sequential_span = tracing::info_span!(parent: &scan_span, "scan_metadata_sequential");
        Self {
            sequential,
            sequential_span,
            scan_span,
            start_time: Instant::now(),
        }
    }

    pub fn finish(self) -> DeltaResult<AfterSequentialScanMetadata> {
        let _guard = self.sequential_span.enter();
        let elapsed = self.start_time.elapsed();

        match self.sequential.finish()? {
            AfterSequential::Done(processor) => {
                // Log counter metrics
                processor
                    .get_metrics()
                    .log_with_message("Sequential scan metadata completed");

                // Log timing
                tracing::info!(
                    sequential_duration_ms = elapsed.as_millis(),
                    "Sequential scan metadata timing"
                );
                Ok(AfterSequentialScanMetadata::Done)
            }
            AfterSequential::Parallel { processor, files } => {
                // Log counter metrics for sequential phase
                processor
                    .get_metrics()
                    .log_with_message("Sequential scan metadata completed");

                // Log timing
                tracing::info!(
                    sequential_duration_ms = elapsed.as_millis(),
                    num_parallel_files = files.len(),
                    "Sequential scan metadata timing"
                );

                // Reset counters for parallel phase
                processor.get_metrics().reset_counters();

                // Enable logging on drop for parallel phase
                processor
                    .get_metrics()
                    .set_log_on_drop("Completed parallel scan metadata");

                Ok(AfterSequentialScanMetadata::Parallel {
                    state: Box::new(ParallelState {
                        inner: processor,
                        scan_span: self.scan_span,
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
        let _guard = self.sequential_span.enter();
        self.sequential.next()
    }
}

/// State for parallel scan metadata processing.
///
/// This state can be serialized and distributed to remote workers, or wrapped
/// in Arc and shared across threads for local parallel processing.
pub struct ParallelState {
    inner: ScanLogReplayProcessor,
    pub(crate) scan_span: tracing::Span,
}

impl ParallelLogReplayProcessor for Arc<ParallelState> {
    type Output = ScanMetadata;

    fn process_actions_batch(&self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
        self.inner.process_actions_batch(actions_batch)
    }
}

impl ParallelState {
    /// Log the accumulated metrics from parallel processing.
    ///
    /// Call this method when parallel processing is complete to log the final
    /// metrics accumulated across all workers.
    pub fn log_parallel_metrics(&self) {
        self.inner
            .get_metrics()
            .log_with_message("Completed parallel scan metadata");
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
    /// - `scan_span`: The parent scan span for tracing
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn from_serializable_state(
        engine: &dyn Engine,
        state: SerializableScanState,
        scan_span: tracing::Span,
    ) -> DeltaResult<Self> {
        let inner = ScanLogReplayProcessor::from_serializable_state(engine, state)?;
        // Enable logging on drop for the reconstructed state. This will include all the metrics
        // across parallel workers.
        inner
            .get_metrics()
            .set_log_on_drop("Completed parallel scan metadata");
        Ok(Self { inner, scan_span })
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
        serde_json::to_vec(&state).map_err(|e| {
            Error::generic(format!("Failed to serialize ParallelState to bytes: {}", e))
        })
    }

    /// Reconstruct a ParallelState from bytes.
    ///
    /// This is a convenience method that combines JSON deserialization with
    /// `from_serializable_state()`. The bytes must have been produced by `into_bytes()`.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators and filters
    /// - `bytes`: The serialized bytes from a previous `into_bytes()` call
    /// - `scan_span`: The parent scan span for tracing
    #[allow(unused)]
    pub fn from_bytes(
        engine: &dyn Engine,
        bytes: &[u8],
        scan_span: tracing::Span,
    ) -> DeltaResult<Self> {
        let state: SerializableScanState =
            serde_json::from_slice(bytes).map_err(Error::MalformedJson)?;
        Self::from_serializable_state(engine, state, scan_span)
    }
}

pub struct ParallelScanMetadata {
    pub(crate) processor: ParallelPhase<Arc<ParallelState>>,
    parallel_span: tracing::Span,
    start_time: Instant,
}

impl ParallelScanMetadata {
    pub fn try_new(
        engine: Arc<dyn Engine>,
        state: Arc<ParallelState>,
        leaf_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let parallel_span = tracing::info_span!(parent: &state.scan_span, "scan_metadata_parallel");
        let read_schema = state.file_read_schema();
        Ok(Self {
            processor: ParallelPhase::try_new(engine, state, leaf_files, read_schema)?,
            parallel_span,
            start_time: Instant::now(),
        })
    }

    pub fn new_from_iter(
        state: Arc<ParallelState>,
        iter: impl IntoIterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        let parallel_span = tracing::info_span!(parent: &state.scan_span, "scan_metadata_parallel");
        Self {
            processor: ParallelPhase::new_from_iter(state.clone(), iter),
            parallel_span,
            start_time: Instant::now(),
        }
    }
}

impl Iterator for ParallelScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        let _guard = self.parallel_span.enter();
        self.processor.next()
    }
}

impl Drop for ParallelScanMetadata {
    fn drop(&mut self) {
        let _guard = self.parallel_span.enter();
        let elapsed = self.start_time.elapsed();
        let thread_id = std::thread::current().id();

        tracing::info!(
            parallel_duration_ms = elapsed.as_millis(),
            thread_id = ?thread_id,
            "Parallel scan metadata completed"
        );
    }
}
