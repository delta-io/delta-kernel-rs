//! Sequential log replay processor that happens before the parallel phase.
//!
//! This module provides sequential phase log replay that processes commits and
//! single-part checkpoint manifests, then returns the processor and any files (sidecars or
//! multi-part checkpoint parts) for parallel processing by the parallel phase. This phase
//! must be completed before the parallel phase can start.
//!
//! For multi-part checkpoints, the sequential phase skips manifest processing and returns
//! the checkpoint parts for parallel processing.
#![allow(unused)]

use std::sync::Arc;

use delta_kernel_derive::internal_api;
use itertools::Itertools;

use crate::log_reader::checkpoint_manifest::CheckpointManifestReader;
use crate::log_reader::commit::CommitReader;
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
use crate::scan::COMMIT_READ_SCHEMA;
use crate::schema::SchemaRef;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileMeta};

/// Sequential log replay processor for parallel execution.
///
/// This iterator processes log replay sequentially:
/// 1. Commit files (JSON)
/// 2. Manifest (single-part checkpoint, if present)
///
/// After exhaustion, call `finish()` to extract:
/// - The processor (for serialization and distribution)
/// - Files (sidecars or multi-part checkpoint parts) for parallel processing
///
/// # Type Parameters
/// - `P`: A [`LogReplayProcessor`] implementation that processes action batches
///
/// # Example
///
/// ```ignore
/// let mut sequential = SequentialPhase::try_new(processor, log_segment, engine)?;
///
/// // Iterate over sequential batches
/// for batch in sequential.by_ref() {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// // Extract processor and files for distribution (if needed)
/// match sequential.finish()? {
///     AfterSequential::Parallel { processor, files } => {
///         // Parallel phase needed - distribute files for parallel processing.
///         // If crossing the network boundary, the processor must be serialized.
///         let serialized = processor.serialize()?;
///         let partitions = partition_files(files, num_workers);
///         for (worker, partition) in partitions {
///             worker.send(serialized.clone(), partition)?;
///         }
///     }
///     AfterSequential::Done(processor) => {
///         // No parallel phase needed - all processing complete sequentially
///         println!("Log replay complete");
///     }
/// }
/// ```
/// cbindgen:ignore
#[internal_api]
pub(crate) struct SequentialPhase<P: LogReplayProcessor> {
    // The processor that will be used to process the action batches
    processor: P,
    // The commit reader that will be used to read the commit files
    commit_phase: Option<CommitReader>,
    // The checkpoint manifest reader that will be used to read the checkpoint manifest files.
    // If the checkpoint is single-part, this will be Some(CheckpointManifestReader).
    checkpoint_manifest_phase: Option<CheckpointManifestReader>,
    // Whether the iterator has been fully exhausted
    is_finished: bool,
    // Checkpoint parts for potential parallel phase processing
    checkpoint_parts: Vec<FileMeta>,
}

/// Result of sequential log replay processing.
/// cbindgen:ignore
#[internal_api]
pub(crate) enum AfterSequential<P: LogReplayProcessor> {
    /// All processing complete sequentially - no parallel phase needed.
    Done(P),
    /// Parallel phase needed - distribute files for parallel processing.
    Parallel { processor: P, files: Vec<FileMeta> },
}

impl<P: LogReplayProcessor> SequentialPhase<P> {
    /// Create a new sequential phase log replay.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `log_segment`: The log segment to process
    /// - `engine`: Engine for reading files
    #[internal_api]
    pub(crate) fn try_new(
        processor: P,
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
        checkpoint_read_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let commit_phase = Some(CommitReader::try_new(
            engine.as_ref(),
            log_segment,
            COMMIT_READ_SCHEMA.clone(),
        )?);

        // Concurrently start reading the checkpoint manifest. Only create a checkpoint manifest
        // reader if the checkpoint is single-part.
        let checkpoint_manifest_phase = match log_segment.listed.checkpoint_parts.as_slice() {
            [single_part] => Some(CheckpointManifestReader::try_new_with_schema(
                engine,
                single_part,
                log_segment.log_root.clone(),
                checkpoint_read_schema,
            )?),
            _ => None,
        };

        let checkpoint_parts = log_segment
            .listed
            .checkpoint_parts
            .iter()
            .map(|path| path.location.clone())
            .collect_vec();

        Ok(Self {
            processor,
            commit_phase,
            checkpoint_manifest_phase,
            is_finished: false,
            checkpoint_parts,
        })
    }

    /// Complete sequential phase and extract processor + files for distribution.
    ///
    /// Must be called after the iterator is exhausted.
    ///
    /// # Returns
    /// - `Done`: All processing done sequentially - no parallel phase needed
    /// - `Parallel`: Parallel phase needed. The resulting files may be processed in parallel.
    ///
    /// # Errors
    /// Returns an error if called before iterator exhaustion.
    #[internal_api]
    pub(crate) fn finish(self) -> DeltaResult<AfterSequential<P>> {
        if !self.is_finished {
            return Err(Error::generic(
                "Must exhaust iterator before calling finish()",
            ));
        }

        let parallel_files = match self.checkpoint_manifest_phase {
            Some(manifest_reader) => manifest_reader.extract_sidecars()?,
            None => {
                let parts = self.checkpoint_parts;
                require!(
                    parts.len() != 1,
                    Error::generic(
                        "Invariant violation: If there is exactly one checkpoint part,
                        there must be a manifest reader"
                    )
                );
                // If this is a multi-part checkpoint, use the checkpoint parts for parallel phase
                parts
            }
        };

        if parallel_files.is_empty() {
            Ok(AfterSequential::Done(self.processor))
        } else {
            Ok(AfterSequential::Parallel {
                processor: self.processor,
                files: parallel_files,
            })
        }
    }
}

impl<P: LogReplayProcessor> Iterator for SequentialPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self
            .commit_phase
            .as_mut()
            .and_then(|commit_phase| commit_phase.next())
            .or_else(|| {
                self.commit_phase = None;
                self.checkpoint_manifest_phase.as_mut()?.next()
            });

        let Some(result) = next else {
            self.is_finished = true;
            return None;
        };

        Some(result.and_then(|batch| self.processor.process_actions_batch(batch)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int64Array, StructArray};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::expressions::{column_expr, Expression as Expr};
    use crate::scan::{AfterSequentialScanMetadata, ScanMetadata};
    use crate::schema::DataType;
    use crate::utils::test_utils::{assert_result_error_with_message, load_test_table};

    /// Core helper function to verify sequential processing with expected adds and sidecars.
    fn verify_sequential_processing(
        table_name: &str,
        expected_adds: &[&str],
        expected_sidecars: &[&str],
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;

        let scan = snapshot.scan_builder().build()?;
        let mut sequential = scan.parallel_scan_metadata(engine)?;

        // Process all batches and collect Add file paths
        let mut file_paths = Vec::new();
        for result in sequential.by_ref() {
            let metadata = result?;
            file_paths =
                metadata.visit_scan_files(file_paths, |ps: &mut Vec<String>, file_stat| {
                    ps.push(file_stat.path);
                })?;
        }

        // Assert collected adds match expected
        file_paths.sort();
        assert_eq!(
            file_paths, expected_adds,
            "Sequential phase should collect expected Add file paths"
        );

        // Call finish() and verify result based on expected sidecars
        let result = sequential.finish()?;
        match (expected_sidecars, result) {
            (sidecars, AfterSequentialScanMetadata::Done) => {
                assert!(
                    sidecars.is_empty(),
                    "Expected Done but got sidecars {sidecars:?}"
                );
            }
            (expected_sidecars, AfterSequentialScanMetadata::Parallel { files, .. }) => {
                assert_eq!(
                    files.len(),
                    expected_sidecars.len(),
                    "Should collect exactly {} sidecar files",
                    expected_sidecars.len()
                );

                // Extract and verify sidecar file paths
                let mut collected_paths = files
                    .iter()
                    .map(|fm| {
                        fm.location
                            .path_segments()
                            .and_then(|mut segments| segments.next_back())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect_vec();

                collected_paths.sort();
                assert_eq!(collected_paths, expected_sidecars);
            }
        }

        Ok(())
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
    fn test_sequential_v2_with_commits_only() -> DeltaResult<()> {
        verify_sequential_processing(
            "table-without-dv-small",
            &["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"],
            &[], // No sidecars
        )
    }

    #[test]
    fn test_sequential_v2_with_sidecars() -> DeltaResult<()> {
        verify_sequential_processing(
            "v2-checkpoints-json-with-sidecars",
            &[], // No adds in sequential phase (all in checkpoint sidecars)
            &[
                "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet",
                "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet",
            ],
        )
    }

    #[test]
    fn test_sequential_finish_before_exhaustion_error() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;

        let scan = snapshot.scan_builder().build()?;
        let sequential = scan.parallel_scan_metadata(engine)?;

        // Try to call finish() before exhausting the iterator
        let result = sequential.finish();
        assert_result_error_with_message(result, "Must exhaust iterator before calling finish()");

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
        let checkpoint_info = sequential.sequential.processor.checkpoint_info();

        assert!(checkpoint_info.has_partition_values_parsed);
        let add_field = checkpoint_info
            .checkpoint_read_schema
            .field("add")
            .expect("checkpoint read schema should include add");
        let DataType::Struct(add_struct) = add_field.data_type() else {
            panic!("add field should be a struct");
        };
        assert!(
            add_struct.field("partitionValues_parsed").is_some(),
            "checkpoint read schema should include add.partitionValues_parsed"
        );

        Ok(())
    }

    #[test]
    fn test_sequential_checkpoint_without_sidecars() -> DeltaResult<()> {
        verify_sequential_processing(
            "v2-checkpoints-json-without-sidecars",
            &[
                // Adds from checkpoint manifest processed in sequential phase
                "test%25file%25prefix-part-00000-0e32f92c-e232-4daa-b734-369d1a800502-c000.snappy.parquet",
                "test%25file%25prefix-part-00000-91daf7c5-9ba0-4f76-aefd-0c3b21d33c6c-c000.snappy.parquet",
                "test%25file%25prefix-part-00001-a5c41be1-ded0-4b18-a638-a927d233876e-c000.snappy.parquet",
            ],
            &[], // No sidecars
        )
    }

    #[test]
    fn test_sequential_parquet_checkpoint_with_sidecars() -> DeltaResult<()> {
        verify_sequential_processing(
            "v2-checkpoints-parquet-with-sidecars",
            &[], // No adds in sequential phase
            &[
                // Expected sidecars
                "00000000000000000006.checkpoint.0000000001.0000000002.76931b15-ead3-480d-b86c-afe55a577fc3.parquet",
                "00000000000000000006.checkpoint.0000000002.0000000002.4367b29c-0e87-447f-8e81-9814cc01ad1f.parquet",
            ],
        )
    }

    #[test]
    fn test_sequential_checkpoint_no_commits() -> DeltaResult<()> {
        verify_sequential_processing(
            "with_checkpoint_no_last_checkpoint",
            &["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"],
            &[],
        )
    }
}
