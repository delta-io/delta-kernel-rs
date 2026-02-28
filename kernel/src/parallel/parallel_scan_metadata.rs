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

/// Result of Phase 1 scan metadata processing.
///
/// This enum indicates whether distributed processing is needed:
/// - `Done`: All processing completed sequentially - no distributed phase needed.
/// - `Phase2`: Contains state and files for parallel processing.
pub enum AfterPhase1ScanMetadata {
    Done,
    Phase2 {
        state: Phase2State,
        files: Vec<FileMeta>,
    },
}

/// Sequential (Phase 1) scan metadata processing.
///
/// This phase processes commits and single-part checkpoint manifests sequentially.
/// After exhaustion, call `finish()` to get the result which indicates whether
/// a distributed phase is needed.
pub struct Phase1ScanMetadata {
    pub(crate) sequential: SequentialPhase<ScanLogReplayProcessor>,
    pub(crate) phase1_span: tracing::Span,
    pub(crate) scan_span: tracing::Span,
    start_time: Instant,
}

impl Phase1ScanMetadata {
    pub(crate) fn new(
        sequential: SequentialPhase<ScanLogReplayProcessor>,
        scan_span: tracing::Span,
    ) -> Self {
        let phase1_span = tracing::info_span!(parent: &scan_span, "scan_metadata_phase1");
        Self {
            sequential,
            phase1_span,
            scan_span,
            start_time: Instant::now(),
        }
    }

    pub fn finish(self) -> DeltaResult<AfterPhase1ScanMetadata> {
        let _guard = self.phase1_span.enter();
        let elapsed = self.start_time.elapsed();

        match self.sequential.finish()? {
            AfterSequential::Done(_) => {
                tracing::info!(
                    phase1_duration_ms = elapsed.as_millis(),
                    "Phase 1 (sequential) completed"
                );
                Ok(AfterPhase1ScanMetadata::Done)
            }
            AfterSequential::Parallel { processor, files } => {
                tracing::info!(
                    phase1_duration_ms = elapsed.as_millis(),
                    num_phase2_files = files.len(),
                    "Phase 1 (sequential) completed, Phase 2 needed"
                );

                Ok(AfterPhase1ScanMetadata::Phase2 {
                    state: Phase2State {
                        inner: processor,
                        scan_span: self.scan_span,
                    },
                    files,
                })
            }
        }
    }
}

impl Iterator for Phase1ScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        self.sequential.next()
    }
}

/// State for Phase 2 parallel scan metadata processing.
///
/// This state can be serialized and distributed to remote workers, or wrapped
/// in Arc and shared across threads for local parallel processing.
pub struct Phase2State {
    inner: ScanLogReplayProcessor,
    pub(crate) scan_span: tracing::Span,
}

impl ParallelLogReplayProcessor for Arc<Phase2State> {
    type Output = ScanMetadata;

    fn process_actions_batch(&self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
        self.inner.process_actions_batch(actions_batch)
    }
}

impl Phase2State {
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

    /// Reconstruct a Phase2State from serialized state.
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
        Ok(Self {
            inner,
            scan_span,
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
            .map_err(|e| Error::generic(format!("Failed to serialize Phase2State to bytes: {}", e)))
    }

    /// Reconstruct a Phase2State from bytes.
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

pub struct Phase2ScanMetadata {
    pub(crate) processor: ParallelPhase<Arc<Phase2State>>,
    phase2_span: tracing::Span,
    _scan_span: tracing::Span,
    start_time: Instant,
}

impl Phase2ScanMetadata {
    pub fn try_new(
        engine: Arc<dyn Engine>,
        state: Arc<Phase2State>,
        leaf_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let phase2_span = tracing::info_span!(parent: &state.scan_span, "scan_metadata_phase2");
        let read_schema = state.file_read_schema();
        Ok(Self {
            processor: ParallelPhase::try_new(engine, state.clone(), leaf_files, read_schema)?,
            phase2_span,
            _scan_span: state.scan_span.clone(),
            start_time: Instant::now(),
        })
    }

    pub fn new_from_iter(
        state: Arc<Phase2State>,
        iter: impl IntoIterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        let phase2_span = tracing::info_span!(parent: &state.scan_span, "scan_metadata_phase2");
        Self {
            processor: ParallelPhase::new_from_iter(state.clone(), iter),
            phase2_span,
            _scan_span: state.scan_span.clone(),
            start_time: Instant::now(),
        }
    }
}

impl Iterator for Phase2ScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        self.processor.next()
    }
}

impl Drop for Phase2ScanMetadata {
    fn drop(&mut self) {
        let _guard = self.phase2_span.enter();
        let elapsed = self.start_time.elapsed();
        let thread_id = std::thread::current().id();

        tracing::info!(
            phase2_duration_ms = elapsed.as_millis(),
            thread_id = ?thread_id,
            "Phase 2 (parallel) completed"
        );
    }
}
