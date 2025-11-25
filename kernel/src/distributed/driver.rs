//! Driver (Phase 1) log replay composition for distributed execution.
//!
//! This module provides driver-side execution that processes commits and manifest,
//! then returns processor + files for distribution to executors.
//!
//! Supports streaming operations via `LogReplayProcessor`.

use std::sync::Arc;

use itertools::Itertools;

use crate::actions::get_commit_schema;
use crate::log_reader::checkpoint_manifest::CheckpointManifestReader;
use crate::log_reader::commit::CommitReader;
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileMeta};

/// Driver-side log replay (Phase 1) for distributed execution.
///
/// This iterator processes:
/// 1. Commit files (JSON)
/// 2. Manifest (single-part checkpoint, if present)
///
/// After exhaustion, call `finish()` to extract:
/// - The processor (for serialization and distribution)
/// - Files to distribute (sidecars or multi-part checkpoint parts)
///
/// # Example
///
/// ```ignore
/// let mut driver = DriverProcessor::try_new(processor, log_segment, engine)?;
///
/// // Iterate over driver-side batches
/// for batch in driver {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// // Extract processor and files for distribution (if needed)
/// match driver.finish()? {
///     Some((processor, files)) => {
///         // Executor phase needed - distribute files
///         let serialized = processor.serialize()?;
///         let partitions = partition_files(files, num_executors);
///         for (executor, partition) in partitions {
///             executor.send(serialized.clone(), partition)?;
///         }
///     }
///     None => {
///         // No executor phase needed - all processing complete
///         println!("Log replay complete on driver");
///     }
/// }
/// ```
pub(crate) struct DriverProcessor<P> {
    processor: P,
    commit_phase: CommitReader,
    /// Pre-computed next state after commit for concurrent IO
    checkpoint_manifest_phase: Option<CheckpointManifestReader>,
    /// Whether the iterator has been fully exhausted
    is_finished: bool,
    log_segment: Arc<LogSegment>,
}

/// Result of driver phase processing.
pub(crate) enum DriverProcessorResult<P> {
    /// All processing complete on driver - no executor phase needed.
    Complete(P),
    /// Executor phase needed - distribute files to executors for parallel processing.
    NeedsExecutorPhase { processor: P, files: Vec<FileMeta> },
}

impl<P: LogReplayProcessor> DriverProcessor<P> {
    /// Create a new driver-side log replay.
    ///
    /// Works for streaming operations via `LogReplayProcessor`.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `log_segment`: The log segment to process
    /// - `engine`: Engine for reading files
    pub(crate) fn try_new(
        processor: P,
        log_segment: Arc<LogSegment>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit_phase =
            CommitReader::try_new(engine.as_ref(), &log_segment, get_commit_schema().clone())?;

        // Concurrently compute the next state after commit. Only create a checkpoint manifest
        // reader if the checkpoint is single-part
        let checkpoint_manifest_phase =
            if let [single_part] = log_segment.checkpoint_parts.as_slice() {
                Some(CheckpointManifestReader::try_new(
                    single_part,
                    log_segment.log_root.clone(),
                    engine,
                )?)
            } else {
                None
            };

        Ok(Self {
            processor,
            commit_phase,
            checkpoint_manifest_phase,
            is_finished: false,
            log_segment,
        })
    }
}

impl<P: LogReplayProcessor> Iterator for DriverProcessor<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self
            .commit_phase
            .next()
            .or_else(|| self.checkpoint_manifest_phase.as_mut()?.next());

        if next.is_none() {
            self.is_finished = true;
        }

        next.map(|batch_res| {
            batch_res.and_then(|batch| self.processor.process_actions_batch(batch))
        })
    }
}

// ============================================================================
// Streaming API: available when P implements LogReplayProcessor
// ============================================================================

impl<P: LogReplayProcessor> DriverProcessor<P> {
    /// Complete driver phase and extract processor + files for distribution.
    ///
    /// Must be called after the iterator is exhausted.
    ///
    /// # Returns
    /// - `Complete`: All processing done on driver - no executor phase needed
    /// - `NeedsExecutorPhase`: Executor phase needed - distribute files to executors
    ///
    /// # Errors
    /// Returns an error if called before iterator exhaustion.
    pub(crate) fn finish(self) -> DeltaResult<DriverProcessorResult<P>> {
        if !self.is_finished {
            return Err(Error::generic(
                "Must exhaust iterator before calling finish()",
            ));
        }

        let executor_files = match self.checkpoint_manifest_phase {
            Some(manifest_reader) => manifest_reader.extract_sidecars()?,
            None => {
                let parts = &self.log_segment.checkpoint_parts;
                require!(
                    parts.len() != 1,
                    Error::generic(
                        "Invariant violation: If there is exactly one checkpoint part,
                        there must be a manifest reader"
                    )
                );
                // If this is a mult-part checkpoint, use the checkpoint parts for executor phase
                parts.iter().map(|path| path.location.clone()).collect_vec()
            }
        };

        if executor_files.is_empty() {
            Ok(DriverProcessorResult::Complete(self.processor))
        } else {
            Ok(DriverProcessorResult::NeedsExecutorPhase {
                processor: self.processor,
                files: executor_files,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use object_store::local::LocalFileSystem;
    use std::path::PathBuf;
    use std::sync::Arc as StdArc;

    fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(
        StdArc<DefaultEngine<TokioBackgroundExecutor>>,
        StdArc<crate::Snapshot>,
        url::Url,
    )> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(table_name);

        let path = std::fs::canonicalize(path)
            .map_err(|e| crate::Error::Generic(format!("Failed to canonicalize path: {}", e)))?;

        let url = url::Url::from_directory_path(path)
            .map_err(|_| crate::Error::Generic("Failed to create URL from path".to_string()))?;

        let store = StdArc::new(LocalFileSystem::new());
        let engine = StdArc::new(DefaultEngine::new(store));
        let snapshot = crate::Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

        Ok((engine, snapshot, url))
    }

    #[test]
    fn test_driver_v2_with_commits_only() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverProcessor::try_new(processor, log_segment, engine.clone())?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();

        while let Some(result) = driver.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(
                vec![],
                |ps: &mut Vec<String>, path, _, _, _, _, _| {
                    ps.push(path.to_string());
                },
            )?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // table-without-dv-small has exactly 1 commit
        assert_eq!(
            batch_count, 1,
            "DriverProcessor should process exactly 1 batch for table-without-dv-small"
        );

        file_paths.sort();
        let expected_files =
            vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "DriverProcessor should find exactly the expected file"
        );

        // No executor phase needed for commits-only table
        let result = driver.finish()?;
        match result {
            DriverProcessorResult::Complete(_processor) => {
                // Expected - no executor phase needed
            }
            DriverProcessorResult::NeedsExecutorPhase { .. } => {
                panic!("Expected Complete, but got NeedsExecutorPhase for commits-only table");
            }
        }

        Ok(())
    }

    #[test]
    fn test_driver_v2_with_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverProcessor::try_new(processor, log_segment, engine.clone())?;

        let mut driver_batch_count = 0;
        let mut driver_file_paths = Vec::new();

        while let Some(result) = driver.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(
                vec![],
                |ps: &mut Vec<String>, path, _, _, _, _, _| {
                    ps.push(path.to_string());
                },
            )?;
            driver_file_paths.extend(paths);
            driver_batch_count += 1;
        }

        // Driver processes commits after checkpoint (v7-v12) in batches, then manifest
        // For v2-checkpoints-json-with-sidecars: checkpoint at v6, commits 7-12 exist
        // The commits 7-12 contain no new add actions (only removes/metadata updates)
        // So driver produces batches from commits, but those batches contain 0 files
        // Note: A single batch may contain multiple commits
        assert!(
            driver_batch_count >= 1,
            "DriverProcessor should process at least 1 batch"
        );

        // The driver should process 0 files (all adds are in the checkpoint sidecars, commits after checkpoint have no new adds)
        driver_file_paths.sort();
        assert_eq!(
            driver_file_paths.len(), 0,
            "DriverProcessor should find 0 files (all adds are in checkpoint sidecars, commits 7-12 have no new add actions)"
        );

        // Should have executor phase with sidecars from the checkpoint
        let result = driver.finish()?;
        match result {
            DriverProcessorResult::NeedsExecutorPhase {
                processor: _processor,
                files,
            } => {
                assert_eq!(
                    files.len(),
                    2,
                    "DriverProcessor should collect exactly 2 sidecar files from checkpoint for distribution"
                );

                // Extract and verify the sidecar file paths
                let mut collected_paths: Vec<String> = files
                    .iter()
                    .map(|fm| {
                        // Get the filename from the URL path
                        fm.location
                            .path_segments()
                            .and_then(|segments| segments.last())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();

                collected_paths.sort();

                // Verify they're the expected sidecar files for version 6
                assert_eq!(collected_paths[0], "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet");
                assert_eq!(collected_paths[1], "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet");
            }
            DriverProcessorResult::Complete(_processor) => {
                panic!("Expected NeedsExecutorPhase for table with sidecars");
            }
        }

        Ok(())
    }
}
