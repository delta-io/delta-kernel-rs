use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::log_segment::CheckpointReadInfo;
use crate::parallel::parallel_phase::ParallelPhase;
use crate::parallel::sequential_phase::{AfterSequential, SequentialPhase};
use crate::scan::log_replay::{ScanLogReplayProcessor, SerializableScanState};
use crate::scan::ScanMetadata;
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
    pub(crate) fn new(sequential: SequentialPhase<ScanLogReplayProcessor>) -> Self {
        let span = tracing::info_span!("scan_metadata_phase1");
        Self { sequential, span }
    }

    pub fn finish(self) -> DeltaResult<AfterPhase1ScanMetadata> {
        let _guard = self.span.enter();
        match self.sequential.finish()? {
            AfterSequential::Done(_) => Ok(AfterPhase1ScanMetadata::Done),
            AfterSequential::Parallel { processor, files } => {
                let checkpoint_info = processor.checkpoint_info().clone();
                Ok(AfterPhase1ScanMetadata::Phase2 {
                    state: Phase2State {
                        inner: processor.into(),
                        checkpoint_info,
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
/// This state can be serialized and distributed to remote workers, or shared
/// directly across threads for local parallel processing.
#[derive(Clone)]
pub struct Phase2State {
    inner: Arc<ScanLogReplayProcessor>,
    pub(crate) checkpoint_info: CheckpointReadInfo,
}

impl AsRef<Phase2State> for Phase2State {
    fn as_ref(&self) -> &Phase2State {
        self
    }
}

impl Phase2State {
    /// Serialize the processor state for distributed processing.
    ///
    /// Returns a `SerializableScanState` containing all information needed to
    /// reconstruct this state on remote compute nodes.
    ///
    /// # Errors
    /// Returns an error if the state cannot be serialized (e.g., contains opaque predicates).
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn into_serializable_state(&self) -> DeltaResult<SerializableScanState> {
        let mut state = self.inner.into_serializable_state()?;
        state.checkpoint_info = self.checkpoint_info.clone();
        Ok(state)
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
        let checkpoint_info = state.checkpoint_info.clone();
        let processor = ScanLogReplayProcessor::from_serializable_state(engine, state)?;
        Ok(Self {
            inner: processor,
            checkpoint_info,
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
    pub fn into_bytes(&self) -> DeltaResult<Vec<u8>> {
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
    pub(crate) processor: ParallelPhase<Arc<ScanLogReplayProcessor>>,
}

impl Phase2ScanMetadata {
    pub fn try_new(
        engine: Arc<dyn Engine>,
        state: impl AsRef<Phase2State>,
        leaf_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            processor: ParallelPhase::try_new(engine, state.as_ref().inner.clone(), leaf_files)?,
        })
    }

    pub fn new_from_iter(
        state: Phase2State,
        iter: impl IntoIterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        Self {
            processor: ParallelPhase::new_from_iter(state.inner.clone(), iter),
        }
    }
}

impl Iterator for Phase2ScanMetadata {
    type Item = DeltaResult<ScanMetadata>;

    fn next(&mut self) -> Option<Self::Item> {
        self.processor.next()
    }
}
