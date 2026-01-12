//! Parallel phase of log replay - processes checkpoint leaf files (sidecars or multi-part parts).
//!
//! This phase runs after [`SequentialPhase`] completes and is designed for parallel execution.
//! Partition the leaf files across executors and create one `ParallelPhase` per partition.
//!
//! [`SequentialPhase`]: super::sequential_phase::SequentialPhase

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::log_replay::ActionsBatch;
use crate::log_replay::ParallelLogReplayProcessor;
use crate::scan::CHECKPOINT_READ_SCHEMA;
use crate::schema::SchemaRef;
use crate::EngineData;
use crate::{DeltaResult, Engine, FileMeta};

use itertools::Itertools;

/// Processes checkpoint leaf files in parallel using a shared processor.
///
/// Create multiple instances with partitioned files for parallel execution.
#[internal_api]
#[allow(unused)]
pub(crate) struct ParallelPhase<P: ParallelLogReplayProcessor> {
    processor: P,
    leaf_checkpoint_reader: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>>>,
}

impl<P: ParallelLogReplayProcessor> ParallelPhase<P> {
    /// Creates a new parallel phase for processing checkpoint leaf files.
    ///
    /// # Parameters
    /// - `engine`: Engine for reading parquet files
    /// - `processor`: Shared processor (wrap in `Arc` for distribution across executors)
    /// - `leaf_files`: Checkpoint leaf files (sidecars or multi-part checkpoint parts)
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn try_new(
        engine: Arc<dyn Engine>,
        processor: P,
        leaf_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let leaf_checkpoint_reader = engine
            .parquet_handler()
            .read_parquet_files(&leaf_files, Self::file_read_schema(), None)?
            .map_ok(|batch| ActionsBatch::new(batch, false));
        Ok(Self {
            processor,
            leaf_checkpoint_reader: Box::new(leaf_checkpoint_reader),
        })
    }

    /// Creates a new parallel phase for processing checkpoint from an existing iterator of
    /// EngineData.
    ///
    /// # Parameters
    /// - `processor`: Shared processor (wrap in `Arc` for distribution across executors)
    /// - `iter`: An iterator of EngineData from reading a leaf-level Checkpoint file (ie; one that
    ///   does not reference sidecar files).
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn try_new_from_iter(
        processor: P,
        iter: impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        let leaf_checkpoint_reader = iter.map_ok(|batch| ActionsBatch::new(batch, false));
        Self {
            processor,
            leaf_checkpoint_reader: Box::new(leaf_checkpoint_reader),
        }
    }

    pub(crate) fn file_read_schema() -> SchemaRef {
        CHECKPOINT_READ_SCHEMA.clone()
    }
}

/// Yields processed batches from checkpoint leaf files.
impl<P: ParallelLogReplayProcessor> Iterator for ParallelPhase<P> {
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
    use crate::parallel::sequential_phase::{AfterSequential, SequentialPhase};
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state::ScanFile;
    use crate::scan::state_info::StateInfo;
    use crate::scan::ScanMetadata;
    use crate::utils::test_utils::load_test_table;
    use std::sync::Arc;

    fn collect_scan_metadata(
        scan_metadata_iter: &mut impl Iterator<Item = DeltaResult<ScanMetadata>>,
    ) -> DeltaResult<Vec<ScanFile>> {
        let mut files = vec![];
        for res in scan_metadata_iter {
            let scan_metadata = res?;
            files = scan_metadata.visit_scan_files(files, |files, scan_file| {
                files.push(scan_file);
            })?;
        }
        Ok(files)
    }

    /// Core helper function to verify the parallel workflow end-to-end.
    ///
    /// This function:
    /// 1. Runs single-threaded scan_metadata as baseline
    /// 2. Runs SequentialPhase to completion
    /// 3. Calls finish() to check if parallel phase is needed
    /// 4. If needed, runs ParallelPhase with the processor and files
    /// 5. Verifies parallel workflow produces same results as single-threaded scan
    ///
    /// # Parameters
    /// - `one_file_per_worker`: If true, creates one ParallelPhase per file (fine-grained).
    ///   If false, creates one ParallelPhase for all files (coarse-grained).
    fn verify_parallel_workflow(
        table_name: &str,
        with_serde: bool,
        one_file_per_worker: bool,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;

        // Get expected results from single-threaded scan_metadata
        let expected_files = {
            let scan = snapshot.clone().scan_builder().build()?;
            let mut scan_metadata_iter = scan.scan_metadata(engine.as_ref())?;

            let mut files = collect_scan_metadata(scan_metadata_iter.by_ref())?;
            files.sort_by(|a, b| a.path.cmp(&b.path));
            files
        };

        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;

        let mut sequential = SequentialPhase::try_new(processor, &log_segment, engine.clone())?;
        let mut all_files = collect_scan_metadata(sequential.by_ref())?;
        let result = sequential.finish()?;

        match result {
            AfterSequential::Done(_processor) => {
                // No parallel phase - all files processed in sequential phase
            }
            AfterSequential::Parallel { processor, files } => {
                // Optionally serialize and deserialize the processor
                let processor = if with_serde {
                    let serialized_state = processor.into_serializable_state()?;
                    ScanLogReplayProcessor::from_serializable_state(
                        engine.as_ref(),
                        serialized_state,
                    )?
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

                // Run a ParallelPhase instance for each partition with shared processor.
                // In a real parallel system, these would run concurrently on different executors.
                for partition_files in partitions {
                    assert!(!partition_files.is_empty());

                    let mut parallel =
                        ParallelPhase::try_new(engine.clone(), processor.clone(), partition_files)?;
                    all_files.extend(collect_scan_metadata(parallel.by_ref())?);
                }
            }
        }

        all_files.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(
            all_files,
            expected_files,
            "Parallel workflow (sequential + parallel phases) should produce same (path, transform) pairs as single-threaded scan_metadata"
        );

        Ok(())
    }

    // ============================================================
    // Tests without serialization
    // ============================================================

    #[test]
    fn test_parallel_with_sidecars() -> DeltaResult<()> {
        verify_parallel_workflow(
            "v2-checkpoints-json-with-sidecars",
            false, // without serde
            false, // coarse-grained: all files in one worker
        )
    }

    #[test]
    fn test_no_parallel_phase_needed() -> DeltaResult<()> {
        verify_parallel_workflow(
            "table-without-dv-small",
            false, // without serde
            false, // coarse-grained (doesn't matter - no parallel phase)
        )
    }

    // ============================================================
    // Tests with serialization
    // ============================================================

    #[test]
    fn test_parallel_with_sidecars_serde() -> DeltaResult<()> {
        verify_parallel_workflow(
            "v2-checkpoints-json-with-sidecars",
            true, // with serde
            true, // fine-grained: one file per worker
        )
    }

    #[test]
    fn test_parallel_parquet_checkpoint_with_sidecars() -> DeltaResult<()> {
        verify_parallel_workflow(
            "v2-checkpoints-parquet-with-sidecars",
            false, // without serde
            true,  // fine-grained: one file per worker
        )
    }

    #[test]
    fn test_parallel_parquet_checkpoint_with_sidecars_serde() -> DeltaResult<()> {
        verify_parallel_workflow(
            "v2-checkpoints-parquet-with-sidecars",
            true,  // with serde
            false, // coarse-grained: all files in one worker
        )
    }
}
