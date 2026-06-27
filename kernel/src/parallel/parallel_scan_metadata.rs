use std::sync::Arc;
use std::time::Instant;

use delta_kernel_derive::internal_api;
use tracing::{info_span, Span};

use crate::log_replay::{ActionsBatch, ParallelLogReplayProcessor};
use crate::metrics::events::emit_scan_metadata_completed;
use crate::metrics::{MetricId, ScanType};
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
    operation_id: MetricId,
    /// Opaque, caller-supplied correlation id propagated to both phases' metric events.
    correlation_id: Option<Arc<str>>,
    start: Instant,
    span: Span,
}

impl SequentialScanMetadata {
    pub(crate) fn new(
        sequential: SequentialPhase<ScanLogReplayProcessor>,
        correlation_id: Option<Arc<str>>,
    ) -> Self {
        let operation_id = MetricId::new();
        Self {
            sequential,
            operation_id,
            correlation_id,
            start: Instant::now(),
            // TODO: Associate a unique scan ID with this span to correlate sequential and parallel
            // phases
            span: info_span!("sequential_scan_metadata"),
        }
    }

    pub fn finish(self) -> DeltaResult<AfterSequentialScanMetadata> {
        let _guard = self.span.enter();
        match self.sequential.finish()? {
            AfterSequential::Done(processor) => {
                let event = processor.get_metrics().to_event(
                    self.operation_id,
                    processor.is_catalog_managed(),
                    self.correlation_id,
                    ScanType::SequentialPhase,
                    self.start.elapsed(),
                );
                processor
                    .get_metrics()
                    .log("Sequential scan metadata completed");
                emit_scan_metadata_completed(&event);
                Ok(AfterSequentialScanMetadata::Done)
            }
            AfterSequential::Parallel { processor, files } => {
                let event = processor.get_metrics().to_event(
                    self.operation_id,
                    processor.is_catalog_managed(),
                    self.correlation_id.clone(),
                    ScanType::SequentialPhase,
                    self.start.elapsed(),
                );
                processor
                    .get_metrics()
                    .log("Sequential scan metadata completed");
                emit_scan_metadata_completed(&event);
                processor.get_metrics().reset_counters();

                Ok(AfterSequentialScanMetadata::Parallel {
                    state: Box::new(ParallelState {
                        inner: processor,
                        operation_id: self.operation_id,
                        correlation_id: self.correlation_id,
                        parallel_start: Instant::now(),
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
    /// Operation ID inherited from the sequential phase for event correlation.
    operation_id: MetricId,
    /// Opaque, caller-supplied correlation id inherited from the sequential phase. Does not
    /// survive the serialization boundary; a reconstructed state carries `None` (tracked in
    /// #2736).
    correlation_id: Option<Arc<str>>,
    /// Start time for the parallel phase, set when this state is created.
    parallel_start: Instant,
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
    /// Call this after all parallel workers complete. The metrics will be logged
    /// in the current tracing span context.
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
    ///     // Log accumulated metrics
    ///     state.log_metrics();
    /// }
    /// ```
    pub fn log_metrics(&self) {
        let event = self.inner.get_metrics().to_event(
            self.operation_id,
            self.inner.is_catalog_managed(),
            self.correlation_id.clone(),
            ScanType::ParallelPhase,
            self.parallel_start.elapsed(),
        );
        self.inner
            .get_metrics()
            .log("Parallel scan metadata completed");
        emit_scan_metadata_completed(&event);
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
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn from_serializable_state(
        engine: &dyn Engine,
        state: SerializableScanState,
    ) -> DeltaResult<Self> {
        let inner = ScanLogReplayProcessor::from_serializable_state(engine, state)?;
        Ok(Self {
            inner,
            // Neither operation_id nor correlation_id survive the serialization boundary today; a
            // reconstructed state starts a fresh operation_id and has no correlation_id (#2736).
            operation_id: MetricId::new(),
            correlation_id: None,
            parallel_start: Instant::now(),
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
    #[allow(unused)]
    pub fn from_bytes(engine: &dyn Engine, bytes: &[u8]) -> DeltaResult<Self> {
        let state: SerializableScanState =
            serde_json::from_slice(bytes).map_err(Error::MalformedJson)?;
        Self::from_serializable_state(engine, state)
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::ParallelState;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::CheckpointReadInfo;
    use crate::metrics::{MetricEvent, ScanType, TableType};
    use crate::scan::log_replay::{ScanLogReplayProcessor, ScanStatsOptions};
    use crate::scan::state_info::StateInfo;
    use crate::scan::PhysicalPredicate;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::table_features::ColumnMappingMode;
    use crate::utils::test_utils::{install_thread_local_metrics_reporter, CapturingReporter};

    #[test]
    fn test_parallel_state_log_metrics_carries_round_tripped_table_type() {
        let engine = SyncEngine::new();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )]));
        let state_info = Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema,
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: true,
            skip_row_transforms: false,
        });
        let processor = ScanLogReplayProcessor::new(
            &engine,
            state_info,
            CheckpointReadInfo::without_stats_parsed(),
            ScanStatsOptions::default(),
        )
        .unwrap();
        let serialized = processor.into_serializable_state().unwrap();
        let state = ParallelState::from_serializable_state(&engine, serialized).unwrap();

        let reporter = Arc::new(CapturingReporter::default());
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        state.log_metrics();

        let event = reporter
            .events()
            .into_iter()
            .find_map(|e| match e {
                MetricEvent::ScanMetadataCompleted(c) => Some(c),
                _ => None,
            })
            .expect("ScanMetadataCompleted emitted");
        assert_eq!(event.table_type, TableType::CatalogManaged);
        assert_eq!(event.scan_type, ScanType::ParallelPhase);
    }
}
