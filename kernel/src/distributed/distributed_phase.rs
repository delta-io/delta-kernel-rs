use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::log_reader::checkpoint_leaf::CheckpointLeafReader;
use crate::log_replay::ParallelizableLogReplayProcessor;
use crate::scan::CHECKPOINT_READ_SCHEMA;
use crate::{DeltaResult, Engine, FileMeta};

#[internal_api]
#[allow(unused)]
pub(crate) struct DistributedPhase<P: ParallelizableLogReplayProcessor> {
    processor: Arc<P>,
    leaf_checkpoint_reader: CheckpointLeafReader,
}

impl<P: ParallelizableLogReplayProcessor> DistributedPhase<P> {
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn try_new(
        engine: Arc<dyn Engine>,
        processor: Arc<P>,
        sidecars: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let leaf_checkpoint_reader =
            CheckpointLeafReader::try_new(engine, sidecars, CHECKPOINT_READ_SCHEMA.clone())?;
        Ok(Self {
            processor,
            leaf_checkpoint_reader,
        })
    }
}

impl<P: ParallelizableLogReplayProcessor> Iterator for DistributedPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.leaf_checkpoint_reader.next().map(|batch_res| {
            batch_res.and_then(|batch| self.processor.process_actions_batch(batch))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::sequential_phase::{AfterSequential, SequentialPhase};
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use crate::utils::test_utils::load_test_table;
    use crate::{ExpressionRef, SnapshotRef};
    use std::sync::Arc;

    /// Helper to collect (path, transform) pairs from scan_metadata (single-threaded baseline).
    fn get_files_from_scan_metadata(
        snapshot: &SnapshotRef,
        engine: &dyn crate::Engine,
    ) -> DeltaResult<Vec<(String, Option<ExpressionRef>)>> {
        let scan = snapshot.clone().scan_builder().build()?;
        let scan_metadata_iter = scan.scan_metadata(engine)?;

        let mut files = vec![];
        for res in scan_metadata_iter {
            let scan_metadata = res?;
            files = scan_metadata.visit_scan_files(
                files,
                |ps: &mut Vec<(String, Option<ExpressionRef>)>, path, _, _, _, transform, _| {
                    ps.push((path.to_string(), transform.clone()));
                },
            )?;
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }

    /// Core helper function to verify the distributed workflow end-to-end.
    ///
    /// This function:
    /// 1. Runs single-threaded scan_metadata as baseline
    /// 2. Runs SequentialPhase to completion
    /// 3. Calls finish() to check if distributed phase is needed
    /// 4. If needed, runs DistributedPhase with the processor and files
    /// 5. Verifies distributed workflow produces same results as single-threaded scan
    ///
    /// # Parameters
    /// - `one_file_per_worker`: If true, creates one DistributedPhase per file (fine-grained).
    ///   If false, creates one DistributedPhase for all files (coarse-grained).
    fn verify_distributed_workflow(
        table_name: &str,
        with_serde: bool,
        one_file_per_worker: bool,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;

        // Get expected results from single-threaded scan_metadata
        let expected_files = get_files_from_scan_metadata(&snapshot, engine.as_ref())?;

        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, &log_segment, engine.clone())?;

        // Process all batches in sequential phase and collect (path, transform) pairs
        let mut sequential_files = Vec::new();
        for result in sequential.by_ref() {
            let metadata = result?;
            sequential_files = metadata.visit_scan_files(
                sequential_files,
                |ps: &mut Vec<(String, Option<ExpressionRef>)>, path, _, _, _, transform, _| {
                    ps.push((path.to_string(), transform.clone()));
                },
            )?;
        }

        // Call finish() to get processor and distributed files (if any)
        let result = sequential.finish()?;

        let mut all_distributed_files = sequential_files;

        match result {
            AfterSequential::Done(_processor) => {
                // No distributed phase - all files processed in sequential phase
            }
            AfterSequential::Distributed { processor, files } => {
                // Optionally serialize and deserialize the processor
                let processor = if with_serde {
                    let serializable_state = processor.into_serializable_state()?;
                    let state_str = serde_json::to_string(&serializable_state)?;
                    let state = serde_json::from_str(&state_str)?;

                    ScanLogReplayProcessor::from_serializable_state(engine.as_ref(), state)?
                } else {
                    Arc::new(processor)
                };

                // Choose distribution strategy based on test mode
                let partitions: Vec<Vec<FileMeta>> = if one_file_per_worker {
                    // Fine-grained: One file per worker (maximum parallelization)
                    files.into_iter().map(|f| vec![f]).collect()
                } else {
                    // Coarse-grained: All files in one worker
                    vec![files]
                };

                // Run a DistributedPhase instance for each partition with shared processor.
                // In a real distributed system, these would run in parallel on different executors.
                for partition_files in partitions {
                    assert!(!partition_files.is_empty());

                    let distributed = DistributedPhase::try_new(
                        engine.clone(),
                        processor.clone(),
                        partition_files,
                    )?;

                    // Collect results from this partition
                    for result in distributed {
                        let metadata = result?;
                        all_distributed_files = metadata.visit_scan_files(
                            all_distributed_files,
                            |ps: &mut Vec<(String, Option<ExpressionRef>)>,
                             path,
                             _,
                             _,
                             _,
                             transform,
                             _| {
                                ps.push((path.to_string(), transform.clone()));
                            },
                        )?;
                    }
                }
            }
        }

        // Sort and compare results
        all_distributed_files.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            all_distributed_files,
            expected_files,
            "Distributed workflow (sequential + distributed phases) should produce same (path, transform) pairs as single-threaded scan_metadata"
        );

        Ok(())
    }

    // ============================================================
    // Tests without serialization
    // ============================================================

    #[test]
    fn test_distributed_with_sidecars() -> DeltaResult<()> {
        verify_distributed_workflow(
            "v2-checkpoints-json-with-sidecars",
            false, // without serde
            false, // coarse-grained: all files in one worker
        )
    }

    #[test]
    fn test_no_distributed_phase_needed() -> DeltaResult<()> {
        verify_distributed_workflow(
            "table-without-dv-small",
            false, // without serde
            false, // coarse-grained (doesn't matter - no distributed phase)
        )
    }

    // ============================================================
    // Tests with serialization
    // ============================================================

    #[test]
    fn test_distributed_with_sidecars_serde() -> DeltaResult<()> {
        verify_distributed_workflow(
            "v2-checkpoints-json-with-sidecars",
            true, // with serde
            true, // fine-grained: one file per worker
        )
    }

    #[test]
    fn test_distributed_parquet_checkpoint_with_sidecars() -> DeltaResult<()> {
        verify_distributed_workflow(
            "v2-checkpoints-parquet-with-sidecars",
            false, // without serde
            true,  // fine-grained: one file per worker
        )
    }

    #[test]
    fn test_distributed_parquet_checkpoint_with_sidecars_serde() -> DeltaResult<()> {
        verify_distributed_workflow(
            "v2-checkpoints-parquet-with-sidecars",
            true,  // with serde
            false, // coarse-grained: all files in one worker
        )
    }
}
