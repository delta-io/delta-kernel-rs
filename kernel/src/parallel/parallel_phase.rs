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
/// This struct is designed for distributed execution where checkpoint leaf files (sidecars or
/// multi-part checkpoint parts) are partitioned across multiple executors. Each executor creates
/// its own `ParallelPhase` instance with a subset of files, but all instances share the same
/// processor (typically wrapped in `Arc`) to coordinate deduplication.
///
/// Implements `Iterator` to yield processed batches. The processor is responsible for filtering
/// out actions for files already seen in the sequential_phase.
///
/// # Example workflow
/// - Partition leaf files across N executors
/// - Create one `ParallelPhase<Arc<Processor>>` per executor with its file subset
/// - Each instance processes its files independently while sharing deduplication state
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

    /// Creates a new parallel phase from an existing iterator of EngineData.
    ///
    /// Use this constructor when you want to parallelize processing at the row group level.
    /// Instead of reading entire checkpoint files, you can provide an iterator that yields
    /// individual row groups, allowing finer-grained parallelization.
    ///
    /// # Parameters
    /// - `processor`: Shared processor (wrap in `Arc` for distribution across executors)
    /// - `iter`: Iterator yielding checkpoint action batches, typically from individual row groups
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn new_from_iter(
        processor: P,
        iter: impl IntoIterator<Item = DeltaResult<Box<dyn EngineData>>> + 'static,
    ) -> Self {
        let leaf_checkpoint_reader = iter
            .into_iter()
            .map_ok(|batch| ActionsBatch::new(batch, false));
        Self {
            processor,
            leaf_checkpoint_reader: Box::new(leaf_checkpoint_reader),
        }
    }

    /// Returns the schema used for reading checkpoint files.
    ///
    /// This schema defines the structure expected when reading checkpoint parquet files,
    /// including the action types (add, remove, etc.) and their fields.
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn file_read_schema() -> SchemaRef {
        CHECKPOINT_READ_SCHEMA.clone()
    }
}

/// Yields processed batches from checkpoint leaf files.
///
/// Each call to `next()` reads one batch from the checkpoint reader and processes it through
/// the processor. The processor applies filtering logic (e.g., removing files already seen in
/// the sequential phase) and returns the processed output.
///
/// # Errors
/// Returns `DeltaResult` errors for:
/// - File reading failures
/// - Parquet parsing errors
/// - Processing errors from the processor
impl<P: ParallelLogReplayProcessor> Iterator for ParallelPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.leaf_checkpoint_reader
            .next()
            .map(|batch| self.processor.process_actions_batch(batch?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::DefaultEngine;
    use crate::log_replay::FileActionKey;
    use crate::parallel::sequential_phase::{AfterSequential, SequentialPhase};
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state::ScanFile;
    use crate::scan::state_info::tests::get_simple_state_info;
    use crate::scan::state_info::StateInfo;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::{load_test_table, parse_json_batch};
    use crate::SnapshotRef;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;
    use url::Url;

    // ============================================================
    // Test helpers for focused ParallelPhase tests
    // ============================================================

    /// Writes a record batch to the in-memory store at a given path.
    async fn write_parquet_to_store(
        store: &Arc<InMemory>,
        path: &str,
        data: Box<dyn EngineData>,
    ) -> DeltaResult<()> {
        let batch = ArrowEngineData::try_from_engine_data(data)?;
        let record_batch = batch.record_batch();

        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
        writer.write(record_batch)?;
        writer.close()?;

        store.put(&Path::from(path), buffer.into()).await?;

        Ok(())
    }

    /// Gets the file size from the store for use in FileMeta
    async fn get_file_size(store: &Arc<InMemory>, path: &str) -> u64 {
        let object_meta = store.head(&Path::from(path)).await.unwrap();
        object_meta.size
    }

    /// Creates a simple table schema for tests
    fn test_schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]))
    }

    /// Creates a ScanLogReplayProcessor with a pre-populated HashMap.
    fn create_processor_with_seen_files(
        engine: &dyn crate::Engine,
        seen_paths: &[&str],
    ) -> DeltaResult<ScanLogReplayProcessor> {
        let state_info = Arc::new(get_simple_state_info(test_schema(), vec![])?);

        let seen_file_keys: HashSet<FileActionKey> = seen_paths
            .iter()
            .map(|path| FileActionKey::new(*path, None))
            .collect();

        ScanLogReplayProcessor::new_with_seen_files(engine, state_info, seen_file_keys)
    }

    // ============================================================
    // Focused ParallelPhase tests with in-memory sidecars
    // ============================================================

    /// Helper to run a ParallelPhase test with in-memory sidecars.
    ///
    /// # Parameters
    /// - `add_paths`: Paths of add actions to include in the sidecar
    /// - `seen_paths`: Paths to pre-populate in the processor's seen HashMap
    /// - `expected_paths`: Expected output paths after filtering
    async fn run_parallel_phase_test(
        add_paths: &[&str],
        seen_paths: &[&str],
        expected_paths: &[&str],
    ) -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::builder(store.clone()).build();

        // Create sidecar with add actions
        let json_adds = add_paths
            .iter()
            .enumerate()
            .map(|(i, path)| {
                format!(
                    r#"{{"add":{{"path":"{}","partitionValues":{{}},"size":{},"modificationTime":{},"dataChange":true}}}}"#,
                    path,
                    (i + 1) * 100,
                    (i + 1) * 1000
                )
            }).collect_vec();
        let sidecar_data = parse_json_batch(json_adds.into());

        // Write sidecar to store
        let sidecar_path = "_delta_log/_sidecars/test.parquet";
        write_parquet_to_store(&store, sidecar_path, sidecar_data).await?;

        // Create processor with seen files
        let processor = Arc::new(create_processor_with_seen_files(&engine, seen_paths)?);

        // Create FileMeta for the sidecar
        let file_meta = FileMeta {
            location: url.join(sidecar_path)?,
            last_modified: 0,
            size: get_file_size(&store, sidecar_path).await,
        };

        // Run ParallelPhase and collect paths
        let mut parallel =
            ParallelPhase::try_new(Arc::new(engine), processor.clone(), vec![file_meta])?;

        let mut all_paths = parallel.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                ps.push(scan_file.path);
            })
        })?;

        // Verify results
        all_paths.sort();
        let mut expected: Vec<&str> = expected_paths.to_vec();
        expected.sort();
        assert_eq!(all_paths, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_phase_empty_hashmap_all_adds_pass() -> DeltaResult<()> {
        run_parallel_phase_test(
            &["file1.parquet", "file2.parquet", "file3.parquet"],
            &[],
            &["file1.parquet", "file2.parquet", "file3.parquet"],
        )
        .await
    }

    #[tokio::test]
    async fn test_parallel_phase_with_removes_filters_matching_adds() -> DeltaResult<()> {
        run_parallel_phase_test(
            &["file1.parquet", "file2.parquet", "file3.parquet"],
            &["file2.parquet"],
            &["file1.parquet", "file3.parquet"],
        )
        .await
    }

    #[tokio::test]
    async fn test_parallel_phase_all_files_removed() -> DeltaResult<()> {
        run_parallel_phase_test(
            &["removed1.parquet", "removed2.parquet"],
            &["removed1.parquet", "removed2.parquet"],
            &[],
        )
        .await
    }

    #[tokio::test]
    async fn test_parallel_phase_multiple_sidecars() -> DeltaResult<()> {
        // This test uses multiple sidecar files, so we need custom logic
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngine::builder(store.clone()).build();

        // Create two sidecars
        let sidecar1_data = parse_json_batch(vec![
            r#"{"add":{"path":"sidecar1_file1.parquet","partitionValues":{},"size":100,"modificationTime":1000,"dataChange":true}}"#,
            r#"{"add":{"path":"sidecar1_file2.parquet","partitionValues":{},"size":200,"modificationTime":2000,"dataChange":true}}"#,
        ].into());
        let sidecar2_data = parse_json_batch(vec![
            r#"{"add":{"path":"sidecar2_file1.parquet","partitionValues":{},"size":300,"modificationTime":3000,"dataChange":true}}"#,
        ].into());

        let sidecar1_path = "_delta_log/_sidecars/sidecar1.parquet";
        let sidecar2_path = "_delta_log/_sidecars/sidecar2.parquet";
        write_parquet_to_store(&store, sidecar1_path, sidecar1_data).await?;
        write_parquet_to_store(&store, sidecar2_path, sidecar2_data).await?;

        let processor = Arc::new(create_processor_with_seen_files(
            &engine,
            &["sidecar1_file2.parquet"],
        )?);

        let file_metas = vec![
            FileMeta {
                location: url.join(sidecar1_path)?,
                last_modified: 0,
                size: get_file_size(&store, sidecar1_path).await,
            },
            FileMeta {
                location: url.join(sidecar2_path)?,
                last_modified: 0,
                size: get_file_size(&store, sidecar2_path).await,
            },
        ];

        let mut parallel = ParallelPhase::try_new(Arc::new(engine), processor.clone(), file_metas)?;

        let mut all_paths = parallel.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                ps.push(scan_file.path);
            })
        })?;

        all_paths.sort();
        assert_eq!(
            all_paths,
            vec!["sidecar1_file1.parquet", "sidecar2_file1.parquet"]
        );

        Ok(())
    }

    // ============================================================
    // Integration tests using real test tables
    // ============================================================

    /// Get expected file paths using the scan_metadata API (single-node approach).
    fn get_expected_paths(
        engine: &dyn crate::Engine,
        snapshot: &SnapshotRef,
    ) -> DeltaResult<Vec<String>> {
        let scan = snapshot.clone().scan_builder().build()?;
        let mut scan_metadata_iter = scan.scan_metadata(engine)?;

        let mut paths = scan_metadata_iter.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file: ScanFile| {
                ps.push(scan_file.path);
            })
        })?;
        paths.sort();
        Ok(paths)
    }

    /// Core helper function to verify the parallel workflow end-to-end.
    ///
    /// This function:
    /// 1. Runs SequentialPhase to completion
    /// 2. Calls finish() to check if parallel phase is needed
    /// 3. If needed, runs ParallelPhase with the processor and files
    /// 4. Verifies results match the scan_metadata API
    fn verify_parallel_workflow(
        table_name: &str,
        with_serde: bool,
        one_file_per_worker: bool,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;

        // Get expected paths using scan_metadata API
        let expected_paths = get_expected_paths(engine.as_ref(), &snapshot)?;

        let log_segment = Arc::new(snapshot.log_segment().clone());

        let state_info = Arc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);
        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut sequential = SequentialPhase::try_new(processor, &log_segment, engine.clone())?;

        // Process all batches in sequential phase and collect paths
        let mut all_paths = sequential.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                ps.push(scan_file.path);
            })
        })?;

        // Call finish() to get processor and parallel files (if any)
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
                    files.into_iter().map(|f| vec![f]).collect()
                } else {
                    vec![files]
                };

                let handles = partitions
                    .into_iter()
                    .map(|partition_files| {
                        let engine = engine.clone();
                        let processor = processor.clone();

                        thread::spawn(move || -> DeltaResult<Vec<String>> {
                            assert!(!partition_files.is_empty());
                            let mut parallel = ParallelPhase::try_new(
                                engine.clone(),
                                processor.clone(),
                                partition_files,
                            )?;

                            parallel.try_fold(Vec::new(), |acc, metadata_res| {
                                metadata_res?.visit_scan_files(
                                    acc,
                                    |ps: &mut Vec<String>, scan_file| {
                                        ps.push(scan_file.path);
                                    },
                                )
                            })
                        })
                    })
                    .collect_vec();

                // Collect results from all threads
                for handle in handles {
                    let paths = handle.join().expect("Thread panicked")?;
                    all_paths.extend(paths);
                }
            }
        }

        // Sort and compare against expected paths from scan_metadata
        all_paths.sort();
        assert_eq!(
            all_paths, expected_paths,
            "Parallel workflow paths don't match scan_metadata paths for table '{}'",
            table_name
        );

        Ok(())
    }

    #[test]
    fn test_parallel_with_json_sidecars() -> DeltaResult<()> {
        for with_serde in [false, true] {
            for one_file_per_worker in [false, true] {
                verify_parallel_workflow(
                    "v2-checkpoints-json-with-sidecars",
                    with_serde,
                    one_file_per_worker,
                )?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_parallel_with_parquet_sidecars() -> DeltaResult<()> {
        for with_serde in [false, true] {
            for one_file_per_worker in [false, true] {
                verify_parallel_workflow(
                    "v2-checkpoints-parquet-with-sidecars",
                    with_serde,
                    one_file_per_worker,
                )?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_no_parallel_phase_needed() -> DeltaResult<()> {
        for with_serde in [false, true] {
            for one_file_per_worker in [false, true] {
                verify_parallel_workflow(
                    "table-without-dv-small",
                    with_serde,
                    one_file_per_worker,
                )?;
            }
        }
        Ok(())
    }
}
