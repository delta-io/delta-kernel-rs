use std::sync::Arc;

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
    pub(crate) span: tracing::Span,
}

impl Phase1ScanMetadata {
    pub(crate) fn new(
        sequential: SequentialPhase<ScanLogReplayProcessor>,
        table_root: &str,
        version: u64,
    ) -> Self {
        let span = tracing::info_span!(
            "scan_metadata_phase1",
            table_root = %table_root,
            version = %version
        );
        Self { sequential, span }
    }

    pub fn finish(self) -> DeltaResult<AfterPhase1ScanMetadata> {
        let _guard = self.span.enter();
        match self.sequential.finish()? {
            AfterSequential::Done(_) => Ok(AfterPhase1ScanMetadata::Done),
            AfterSequential::Parallel { processor, files } => Ok(AfterPhase1ScanMetadata::Phase2 {
                state: Phase2State { inner: processor },
                files,
            }),
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
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn from_serializable_state(
        engine: &dyn Engine,
        state: SerializableScanState,
    ) -> DeltaResult<Self> {
        let inner = ScanLogReplayProcessor::from_serializable_state(engine, state)?;
        Ok(Self { inner })
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
    #[allow(unused)]
    pub fn from_bytes(engine: &dyn Engine, bytes: &[u8]) -> DeltaResult<Self> {
        let state: SerializableScanState =
            serde_json::from_slice(bytes).map_err(Error::MalformedJson)?;
        Self::from_serializable_state(engine, state)
    }
}

pub struct Phase2ScanMetadata {
    pub(crate) processor: ParallelPhase<Arc<Phase2State>>,
}

impl Phase2ScanMetadata {
    pub fn try_new(
        engine: Arc<dyn Engine>,
        state: Arc<Phase2State>,
        leaf_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let read_schema = state.file_read_schema();
        Ok(Self {
            processor: ParallelPhase::try_new(engine, state, leaf_files, read_schema)?,
        })
    }

    pub fn new_from_iter(
        state: Arc<Phase2State>,
        iter: impl IntoIterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        Self {
            processor: ParallelPhase::new_from_iter(state, iter),
        }
    }
}

impl Iterator for Phase2ScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        self.processor.next()
    }
}
