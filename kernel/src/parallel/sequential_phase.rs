//! Sequential log replay processor that happens before the parallel phase.
//!
//! This module provides sequential phase log replay that processes commits and
//! single-part checkpoint manifests, then returns the processor and any files (sidecars or
//! multi-part checkpoint parts) for parallel processing by the parallel phase. This phase
//! must be completed before the parallel phase can start.
//!
//! For multi-part checkpoints, the sequential phase skips manifest processing and returns
//! the checkpoint parts for parallel processing.

use std::sync::Arc;

use itertools::Itertools;

use crate::actions::get_commit_schema;
use crate::log_reader::checkpoint_manifest::CheckpointManifestReader;
use crate::log_reader::commit::CommitReader;
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
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
#[allow(unused)]
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
#[allow(unused)]
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
    #[allow(unused)]
    pub(crate) fn try_new(
        processor: P,
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit_phase = Some(CommitReader::try_new(
            engine.as_ref(),
            log_segment,
            get_commit_schema().clone(),
        )?);

        // Concurrently start reading the checkpoint manifest. Only create a checkpoint manifest
        // reader if the checkpoint is single-part.
        let checkpoint_manifest_phase = match log_segment.checkpoint_parts.as_slice() {
            [single_part] => Some(CheckpointManifestReader::try_new(
                engine,
                single_part,
                log_segment.log_root.clone(),
            )?),
            _ => None,
        };

        let checkpoint_parts = log_segment
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
    /// - `Parallel`: Parallel phase needed. The resulting files may be processed
    ///   in parallel.
    ///
    /// # Errors
    /// Returns an error if called before iterator exhaustion.
    #[allow(unused)]
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
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use crate::utils::test_utils::{assert_result_error_with_message, load_test_table};
    use std::sync::Arc;

    /// Core helper function to verify sequential processing with expected adds and sidecars.
    fn verify_sequential_processing(
        table_name: &str,
        expected_adds: &[&str],
        expected_sidecars: &[&str],
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;
        let log_segment = snapshot.log_segment();

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, log_segment, engine.clone())?;

        // Process all batches and collect Add file paths
        let mut file_paths = Vec::new();
        for result in sequential.by_ref() {
            let metadata = result?;
            file_paths = metadata.visit_scan_files(
                file_paths,
                |ps: &mut Vec<String>, path, _, _, _, _, _| {
                    ps.push(path.to_string());
                },
            )?;
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
            (sidecars, AfterSequential::Done(_)) => {
                assert!(
                    sidecars.is_empty(),
                    "Expected Done but got sidecars {:?}",
                    sidecars
                );
            }
            (expected_sidecars, AfterSequential::Parallel { files, .. }) => {
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
        let log_segment = snapshot.log_segment();

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, log_segment, engine.clone())?;

        // Call next() once but don't exhaust the iterator
        if let Some(result) = sequential.next() {
            result?;
        }

        // Try to call finish() before exhausting the iterator
        let result = sequential.finish();
        assert_result_error_with_message(result, "Must exhaust iterator before calling finish()");

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
            &["part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet"], // Add from commit 3
            &[],                                                                      // No sidecars
        )
    }
}
