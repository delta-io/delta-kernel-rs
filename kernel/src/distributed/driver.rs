//! Driver (Phase 1) log replay composition for distributed execution.
//!
//! This module provides driver-side execution that processes commits and manifest,
//! then returns processor + files for distribution to executors.
//!
//! Supports streaming operations via `LogReplayProcessor`.

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::actions::get_commit_schema;
use crate::log_reader::commit::CommitReader;
use crate::log_reader::manifest::{AfterManifest, ManifestPhase};
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
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
/// let mut driver = DriverPhase::try_new(processor, log_segment, engine)?;
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
pub(crate) struct DriverPhase<P> {
    processor: P,
    state: Option<DriverState>,
    /// Pre-computed next state after commit for concurrent IO
    next_state_after_commit: Option<DriverState>,
    /// Whether the iterator has been fully exhausted
    is_finished: bool,
}

enum DriverState {
    Commit(CommitReader),
    Manifest(ManifestPhase),
    /// Executor phase needed - has files to distribute
    ExecutorPhase {
        files: Vec<FileMeta>,
    },
    /// Done - no more work needed
    Done,
}

/// Result of driver phase processing.
pub(crate) enum DriverPhaseResult<P> {
    /// All processing complete on driver - no executor phase needed.
    Complete(P),
    /// Executor phase needed - distribute files to executors for parallel processing.
    NeedsExecutorPhase { processor: P, files: Vec<FileMeta> },
}

impl<P: LogReplayProcessor> DriverPhase<P> {
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
        let commit_schema = get_commit_schema();
        let commit = CommitReader::try_new(engine.as_ref(), &log_segment, commit_schema.clone())?;

        // Concurrently compute the next state after commit for parallel IO
        let next_state_after_commit = Some(Self::compute_state_after_commit(&log_segment, engine.clone())?);

        Ok(Self {
            processor,
            state: Some(DriverState::Commit(commit)),
            next_state_after_commit,
            is_finished: false,
        })
    }

    /// Compute the next state after CommitReader is exhausted.
    /// 
    /// This is called during construction to enable concurrent IO initialization.
    /// Returns the appropriate DriverState based on checkpoint configuration:
    /// - Single-part checkpoint → Manifest phase (pre-initialized)
    /// - Multi-part checkpoint → ExecutorPhase with all parts
    /// - No checkpoint → Done
    fn compute_state_after_commit(
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<DriverState> {
        if log_segment.checkpoint_parts.is_empty() {
            // No checkpoint
            Ok(DriverState::Done)
        } else if log_segment.checkpoint_parts.len() == 1 {
            // Single-part checkpoint: create manifest phase
            let checkpoint_part = &log_segment.checkpoint_parts[0];
            let manifest = ManifestPhase::new(
                checkpoint_part.location.clone(),
                log_segment.log_root.clone(),
                engine,
            )?;
            Ok(DriverState::Manifest(manifest))
        } else {
            // Multi-part checkpoint: all parts are leaf files
            let files: Vec<_> = log_segment
                .checkpoint_parts
                .iter()
                .map(|p| p.location.clone())
                .collect();
            Ok(DriverState::ExecutorPhase { files })
        }
    }
}

impl<P: LogReplayProcessor> Iterator for DriverPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get item from current phase
            let batch_result = match self.state.as_mut()? {
                DriverState::Commit(phase) => phase.next(),
                DriverState::Manifest(phase) => phase.next(),
                DriverState::ExecutorPhase { .. } | DriverState::Done => {
                    self.is_finished = true;
                    return None;
                }
            };

            match batch_result {
                Some(Ok(batch)) => {
                    // Process the batch through the processor
                    return Some(self.processor.process_actions_batch(batch));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    // Phase exhausted - transition
                    let old_state = self.state.take()?;
                    match self.transition(old_state) {
                        Ok(new_state) => self.state = Some(new_state),
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }
}

impl<P> DriverPhase<P> {
    fn transition(&mut self, state: DriverState) -> DeltaResult<DriverState> {
        match state {
            DriverState::Commit(_) => {
                // Use pre-computed state (always initialized in constructor)
                self.next_state_after_commit.take().ok_or_else(|| {
                    Error::generic("next_state_after_commit should be initialized in constructor")
                })
            }

            DriverState::Manifest(manifest) => {
                // After ManifestPhase exhausted, check for sidecars
                match manifest.finalize()? {
                    AfterManifest::Sidecars { sidecars } => {
                        Ok(DriverState::ExecutorPhase { files: sidecars })
                    }
                    AfterManifest::Done => Ok(DriverState::Done),
                }
            }

            // These states are terminal and should never be transitioned from
            DriverState::ExecutorPhase { .. } | DriverState::Done => {
                Err(Error::generic("Invalid state transition: terminal state reached"))
            }
        }
    }
}

// ============================================================================
// Streaming API: available when P implements LogReplayProcessor
// ============================================================================

impl<P: LogReplayProcessor> DriverPhase<P> {
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
    pub(crate) fn finish(self) -> DeltaResult<DriverPhaseResult<P>> {
        if !self.is_finished {
            return Err(Error::generic(
                "Must exhaust iterator before calling finish()",
            ));
        }

        match self.state {
            Some(DriverState::ExecutorPhase { files }) => {
                Ok(DriverPhaseResult::NeedsExecutorPhase {
                    processor: self.processor,
                    files,
                })
            }
            Some(DriverState::Done) | None => Ok(DriverPhaseResult::Complete(self.processor)),
            _ => Err(Error::generic("Unexpected state after iterator exhaustion")),
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

    fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<crate::Snapshot>,
        url::Url,
    )> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(table_name);

        let path = std::fs::canonicalize(path)
            .map_err(|e| crate::Error::Generic(format!("Failed to canonicalize path: {}", e)))?;

        let url = url::Url::from_directory_path(path)
            .map_err(|_| crate::Error::Generic("Failed to create URL from path".to_string()))?;

        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        let snapshot = crate::Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

        Ok((engine, snapshot, url))
    }

    #[test]
    fn test_driver_v2_with_commits_only() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverPhase::try_new(processor, log_segment, engine.clone())?;

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
            "DriverPhase should process exactly 1 batch for table-without-dv-small"
        );

        file_paths.sort();
        let expected_files =
            vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "DriverPhase should find exactly the expected file"
        );

        // No executor phase needed for commits-only table
        let result = driver.finish()?;
        match result {
            DriverPhaseResult::Complete(_processor) => {
                // Expected - no executor phase needed
            }
            DriverPhaseResult::NeedsExecutorPhase { .. } => {
                panic!("Expected Complete, but got NeedsExecutorPhase for commits-only table");
            }
        }

        Ok(())
    }

    #[test]
    fn test_driver_v2_with_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverPhase::try_new(processor, log_segment, engine.clone())?;

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
            "DriverPhase should process at least 1 batch"
        );

        // The driver should process 0 files (all adds are in the checkpoint sidecars, commits after checkpoint have no new adds)
        driver_file_paths.sort();
        assert_eq!(
            driver_file_paths.len(), 0,
            "DriverPhase should find 0 files (all adds are in checkpoint sidecars, commits 7-12 have no new add actions)"
        );

        // Should have executor phase with sidecars from the checkpoint
        let result = driver.finish()?;
        match result {
            DriverPhaseResult::NeedsExecutorPhase {
                processor: _processor,
                files,
            } => {
                assert_eq!(
                    files.len(),
                    2,
                    "DriverPhase should collect exactly 2 sidecar files from checkpoint for distribution"
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
            DriverPhaseResult::Complete(_processor) => {
                panic!("Expected NeedsExecutorPhase for table with sidecars");
            }
        }

        Ok(())
    }

    #[test]
    fn test_distributed_scan_serialization() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info.clone())?;
        
        // Serialize the processor (takes ownership)
        let (serialized_state, deduplicator) = processor.serialize()?;
        
        // Verify we can reconstruct the processor
        let reconstructed = ScanLogReplayProcessor::from_serialized(
            engine.as_ref(),
            serialized_state,
            deduplicator,
        )?;

        // Verify schemas match (compare against original state_info)
        assert_eq!(
            state_info.logical_schema,
            reconstructed.state_info.logical_schema,
            "Logical schemas should match after serialization"
        );
        assert_eq!(
            state_info.physical_schema,
            reconstructed.state_info.physical_schema,
            "Physical schemas should match after serialization"
        );
        
        // Verify transform spec matches
        match (&state_info.transform_spec, &reconstructed.state_info.transform_spec) {
            (Some(original), Some(reconstructed)) => {
                assert_eq!(
                    **original,
                    **reconstructed,
                    "Transform spec should be equal after serialization"
                );
            }
            (None, None) => {
                // Both None - correct
            }
            _ => panic!("Transform spec presence mismatch after serialization"),
        }
        
        // Verify column mapping mode matches
        assert_eq!(
            state_info.column_mapping_mode,
            reconstructed.state_info.column_mapping_mode,
            "Column mapping mode should match after serialization"
        );

        Ok(())
    }

    #[test]
    fn test_distributed_scan_with_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        
        // Create a scan
        let scan = crate::scan::ScanBuilder::new(snapshot.clone())
            .build()?;

        // Get distributed driver
        let mut driver = scan.scan_metadata_distributed(engine.clone())?;

        let mut driver_batch_count = 0;
        let mut driver_file_paths = Vec::new();

        // Process driver-side batches
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

        // Driver should process commits but find no files (all in sidecars)
        assert_eq!(
            driver_file_paths.len(), 0,
            "Driver should find 0 files (all adds are in checkpoint sidecars)"
        );

        // Should have executor phase with sidecars
        let result = driver.finish()?;
        match result {
            crate::distributed::DriverPhaseResult::NeedsExecutorPhase {
                processor,
                files,
            } => {
                assert_eq!(
                    files.len(),
                    2,
                    "Should have exactly 2 sidecar files for distribution"
                );

                // Serialize processor for distribution
                let (serialized_state, deduplicator) = processor.serialize()?;

                // Verify the serialized state can be reconstructed
                let _reconstructed = ScanLogReplayProcessor::from_serialized(
                    engine.as_ref(),
                    serialized_state,
                    deduplicator,
                )?;

                // Verify sidecar file paths
                let mut collected_paths: Vec<String> = files
                    .iter()
                    .map(|fm| {
                        fm.location
                            .path_segments()
                            .and_then(|segments| segments.last())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();

                collected_paths.sort();

                assert_eq!(collected_paths[0], "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet");
                assert_eq!(collected_paths[1], "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet");
            }
            crate::distributed::DriverPhaseResult::Complete(_processor) => {
                panic!("Expected NeedsExecutorPhase for table with sidecars");
            }
        }

        Ok(())
    }

    #[test]
    fn test_deduplicator_state_preserved() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let mut processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info.clone())?;
        
        // Process some actions to populate the deduplicator
        let mut driver = DriverPhase::try_new(processor, log_segment, engine.clone())?;
        
        // Process all driver batches
        while let Some(_) = driver.next() {}
        
        let result = driver.finish()?;
        let processor = match result {
            crate::distributed::DriverPhaseResult::Complete(p) => p,
            crate::distributed::DriverPhaseResult::NeedsExecutorPhase { processor, .. } => processor,
        };

        let initial_dedup_count = processor.seen_file_keys.len();
        
        // Serialize and reconstruct (serialize takes ownership)
        let (serialized_state, deduplicator) = processor.serialize()?;
        assert_eq!(
            deduplicator.len(),
            initial_dedup_count,
            "Deduplicator size should be preserved during serialization"
        );

        let reconstructed = ScanLogReplayProcessor::from_serialized(
            engine.as_ref(),
            serialized_state,
            deduplicator.clone(),
        )?;

        assert_eq!(
            reconstructed.seen_file_keys.len(),
            initial_dedup_count,
            "Reconstructed processor should have same deduplicator size"
        );
        
        // Verify the deduplicator contents match (compare against returned deduplicator)
        for key in &deduplicator {
            assert!(
                reconstructed.seen_file_keys.contains(key),
                "Reconstructed deduplicator should contain key: {:?}",
                key
            );
        }

        Ok(())
    }

}
