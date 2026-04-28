use std::sync::Arc;
use std::time::Instant;

use delta_kernel_derive::internal_api;
use tracing::{info_span, Span};

use crate::log_replay::{ActionsBatch, ParallelLogReplayProcessor};
use crate::metrics::reporter::emit_scan_metadata_completed;
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
    start: Instant,
    deferred_parallel_checkpoint_planning: Option<DeferredParallelCheckpointPlanning>,
    span: Span,
}

impl SequentialScanMetadata {
    pub(crate) fn new(sequential: SequentialPhase<ScanLogReplayProcessor>) -> Self {
        let operation_id = MetricId::new();
        Self {
            sequential,
            operation_id,
            start: Instant::now(),
            deferred_parallel_checkpoint_planning: None,
            // TODO: Associate a unique scan ID with this span to correlate sequential and parallel
            // phases
            span: info_span!("sequential_scan_metadata"),
        }
    }

    pub(crate) fn new_with_deferred_parallel_checkpoint_planning(
        sequential: SequentialPhase<ScanLogReplayProcessor>,
        engine: Arc<dyn Engine>,
        action_schema: SchemaRef,
    ) -> Self {
        let operation_id = MetricId::new();
        Self {
            sequential,
            operation_id,
            start: Instant::now(),
            deferred_parallel_checkpoint_planning: Some(DeferredParallelCheckpointPlanning {
                engine,
                action_schema,
            }),
            span: info_span!("sequential_scan_metadata"),
        }
    }

    pub fn finish(self) -> DeltaResult<AfterSequentialScanMetadata> {
        let Self {
            sequential,
            operation_id,
            start,
            deferred_parallel_checkpoint_planning,
            span,
        } = self;
        let _guard = span.enter();
        match sequential.finish()? {
            AfterSequential::Done(processor) => {
                let event = processor.get_metrics().to_event(
                    operation_id,
                    ScanType::SequentialPhase,
                    start.elapsed(),
                );
                processor
                    .get_metrics()
                    .log("Sequential scan metadata completed");
                emit_scan_metadata_completed(&event);
                Ok(AfterSequentialScanMetadata::Done)
            }
            AfterSequential::Parallel {
                mut processor,
                files,
            } => {
                if let Some(deferred_planning) = deferred_parallel_checkpoint_planning.as_ref() {
                    if let Some(checkpoint_info) = deferred_planning.finalize(&processor, &files)? {
                        processor.set_checkpoint_info(
                            deferred_planning.engine.as_ref(),
                            checkpoint_info,
                        )?;
                    }
                }

                let event = processor.get_metrics().to_event(
                    operation_id,
                    ScanType::SequentialPhase,
                    start.elapsed(),
                );
                processor
                    .get_metrics()
                    .log("Sequential scan metadata completed");
                emit_scan_metadata_completed(&event);
                processor.get_metrics().reset_counters();

                Ok(AfterSequentialScanMetadata::Parallel {
                    state: Box::new(ParallelState {
                        inner: processor,
                        operation_id,
                        parallel_start: Instant::now(),
                    }),
                    files,
                })
            }
        }
    }

    #[cfg(test)]
    fn checkpoint_info_for_tests(&self) -> &crate::log_segment::CheckpointReadInfo {
        self.sequential.processor().checkpoint_info()
    }
}

struct DeferredParallelCheckpointPlanning {
    engine: Arc<dyn Engine>,
    action_schema: SchemaRef,
}

impl DeferredParallelCheckpointPlanning {
    fn finalize(
        &self,
        processor: &ScanLogReplayProcessor,
        files: &[FileMeta],
    ) -> DeltaResult<Option<crate::log_segment::CheckpointReadInfo>> {
        processor.projected_checkpoint_read_info_from_files(
            self.engine.as_ref(),
            self.action_schema.clone(),
            files,
        )
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
            operation_id: MetricId::new(),
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
    use super::*;
    use crate::arrow::array::{Array, Int64Array, StructArray};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::{column_expr, Expression as Expr};
    use crate::scan::ScanMetadata;
    use crate::schema::DataType;
    use crate::utils::test_utils::load_test_table;

    fn schema_has_add_field(schema: &SchemaRef, field_name: &str) -> bool {
        let Some(add_field) = schema.field("add") else {
            return false;
        };
        let DataType::Struct(add_struct) = add_field.data_type() else {
            return false;
        };
        add_struct.field(field_name).is_some()
    }

    fn collect_selected_num_records(
        rows: &mut Vec<i64>,
        scan_metadata: ScanMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        let stats_parsed = batch
            .column_by_name("stats_parsed")
            .expect("stats_parsed column should be present")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("stats_parsed column should be StructArray");
        let num_records = stats_parsed
            .column_by_name("numRecords")
            .expect("stats_parsed.numRecords should be present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("stats_parsed.numRecords should be Int64Array");

        for (row_index, &selected) in selection_vector.iter().enumerate().take(batch.num_rows()) {
            if selected {
                assert!(
                    !stats_parsed.is_null(row_index),
                    "stats_parsed should be non-null for selected row {row_index}"
                );
                rows.push(num_records.value(row_index));
            }
        }

        Ok(())
    }

    #[test]
    fn test_parallel_scan_metadata_preserves_stats_parsed_for_single_part_struct_stats_checkpoint(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snapshot, _tempdir) = load_test_table("v1-single-part-struct-stats-only")?;

        let scan = snapshot
            .scan_builder()
            .include_all_stats_columns()
            .build()?;

        let mut expected_num_records = Vec::new();
        for scan_metadata in scan.scan_metadata(engine.as_ref())? {
            collect_selected_num_records(&mut expected_num_records, scan_metadata?)?;
        }
        expected_num_records.sort_unstable();
        assert!(!expected_num_records.is_empty());

        let mut sequential = scan.parallel_scan_metadata(engine)?;
        let mut actual_num_records = Vec::new();
        for scan_metadata in sequential.by_ref() {
            collect_selected_num_records(&mut actual_num_records, scan_metadata?)?;
        }

        assert!(matches!(
            sequential.finish()?,
            AfterSequentialScanMetadata::Done
        ));

        actual_num_records.sort_unstable();
        assert_eq!(actual_num_records, expected_num_records);

        Ok(())
    }

    #[test]
    fn test_parallel_scan_metadata_plans_partition_values_parsed_for_single_part_checkpoint(
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("app-txn-checkpoint")?;

        let scan = snapshot
            .scan_builder()
            .with_predicate(Arc::new(Expr::eq(
                column_expr!("modified"),
                Expr::literal("2021-02-02".to_string()),
            )))
            .build()?;

        let sequential = scan.parallel_scan_metadata(engine)?;
        let checkpoint_info = sequential.checkpoint_info_for_tests();

        assert!(checkpoint_info.has_partition_values_parsed);
        assert!(
            schema_has_add_field(
                &checkpoint_info.checkpoint_read_schema,
                "partitionValues_parsed"
            ),
            "checkpoint read schema should include add.partitionValues_parsed"
        );

        Ok(())
    }

    #[test]
    fn test_parallel_scan_metadata_preserves_stats_parsed_for_v2_sidecar_checkpoints(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for table_name in [
            "v2-parquet-sidecars-struct-stats-only",
            "v2-json-sidecars-struct-stats-only",
        ] {
            let (engine, snapshot, _tempdir) = load_test_table(table_name)?;
            let scan = snapshot
                .scan_builder()
                .include_all_stats_columns()
                .build()?;

            let mut expected_num_records = Vec::new();
            for scan_metadata in scan.scan_metadata(engine.as_ref())? {
                collect_selected_num_records(&mut expected_num_records, scan_metadata?)?;
            }
            expected_num_records.sort_unstable();
            assert!(!expected_num_records.is_empty());

            let mut sequential = scan.parallel_scan_metadata(engine.clone())?;
            let mut actual_num_records = Vec::new();

            for scan_metadata in sequential.by_ref() {
                collect_selected_num_records(&mut actual_num_records, scan_metadata?)?;
            }

            let AfterSequentialScanMetadata::Parallel { state, files } = sequential.finish()?
            else {
                panic!("expected parallel phase for {table_name}");
            };

            assert!(!files.is_empty(), "expected sidecar files for {table_name}");
            assert!(
                schema_has_add_field(&state.file_read_schema(), "stats_parsed"),
                "parallel state file read schema should include add.stats_parsed for {table_name}"
            );

            let state = Arc::new(*state);
            let mut parallel = ParallelScanMetadata::try_new(engine, state, files)?;
            for scan_metadata in parallel.by_ref() {
                collect_selected_num_records(&mut actual_num_records, scan_metadata?)?;
            }

            actual_num_records.sort_unstable();
            assert_eq!(
                actual_num_records, expected_num_records,
                "parallel sidecar scan metadata should preserve stats_parsed.numRecords for {table_name}"
            );
        }

        Ok(())
    }
}
