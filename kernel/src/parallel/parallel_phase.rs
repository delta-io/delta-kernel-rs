//! Parallel phase of log replay - processes checkpoint leaf files (sidecars or multi-part parts).
//!
//! This phase runs after [`SequentialPhase`] completes and is designed for parallel execution.
//! Partition the leaf files across executors and create one `ParallelPhase` per partition.
//!
//! [`SequentialPhase`]: super::sequential_phase::SequentialPhase
#![allow(unused)]

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
/// cbindgen:ignore
#[internal_api]
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
    /// - `read_schema`: Schema to use for reading checkpoint files
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn try_new(
        engine: Arc<dyn Engine>,
        processor: P,
        leaf_files: Vec<FileMeta>,
        read_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let leaf_checkpoint_reader = engine
            .parquet_handler()
            .read_parquet_files(&leaf_files, read_schema, None)?
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
    use crate::actions::get_log_add_schema;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::DefaultEngine;
    use crate::log_replay::FileActionKey;
    use crate::log_segment::CheckpointReadInfo;
    use crate::parallel::parallel_scan_metadata::AfterPhase1ScanMetadata;
    use crate::parallel::parallel_scan_metadata::{Phase2ScanMetadata, Phase2State};
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state::ScanFile;
    use crate::scan::state_info::tests::get_simple_state_info;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::{load_test_table, parse_json_batch};
    use crate::{PredicateRef, SnapshotRef};
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

        let checkpoint_info = CheckpointReadInfo::without_stats_parsed();

        ScanLogReplayProcessor::new_with_seen_files(
            engine,
            state_info,
            checkpoint_info,
            seen_file_keys,
            false,
        )
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

        let mut parallel = ParallelPhase::try_new(
            Arc::new(engine),
            processor.clone(),
            vec![file_meta],
            CHECKPOINT_READ_SCHEMA.clone(),
        )?;

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

        let mut parallel = ParallelPhase::try_new(
            Arc::new(engine),
            processor.clone(),
            file_metas,
            CHECKPOINT_READ_SCHEMA.clone(),
        )?;

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
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<Vec<String>> {
        let mut builder = snapshot.clone().scan_builder();
        if let Some(pred) = predicate {
            builder = builder.with_predicate(pred);
        }
        let scan = builder.build()?;
        let mut scan_metadata_iter = scan.scan_metadata(engine)?;

        let mut paths = scan_metadata_iter.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file: ScanFile| {
                ps.push(scan_file.path);
            })
        })?;
        paths.sort();
        Ok(paths)
    }

    fn verify_parallel_workflow(
        table_name: &str,
        predicate: Option<PredicateRef>,
        with_serde: bool,
        one_file_per_worker: bool,
        dispatcher: Option<tracing::Dispatch>,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table(table_name)?;

        let expected_paths = get_expected_paths(engine.as_ref(), &snapshot, predicate.clone())?;

        let mut builder = snapshot.scan_builder();
        if let Some(pred) = predicate {
            builder = builder.with_predicate(pred);
        }
        let scan = builder.build()?;
        let mut phase1 = scan.parallel_scan_metadata(engine.clone())?;

        let mut all_paths = phase1.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                ps.push(scan_file.path);
            })
        })?;

        match phase1.finish()? {
            AfterPhase1ScanMetadata::Done => {}
            AfterPhase1ScanMetadata::Phase2 { state, files } => {
                let final_state = if with_serde {
                    // Serialize and then deserialize to test the serde path
                    let scan_span = state.scan_span.clone();
                    let serialized_bytes = state.into_bytes()?;
                    Phase2State::from_bytes(engine.as_ref(), &serialized_bytes, scan_span)?
                } else {
                    // Non-serde: just use the state directly
                    state
                };
                let final_state = Arc::new(final_state);

                let partitions: Vec<Vec<FileMeta>> = if one_file_per_worker {
                    files.into_iter().map(|f| vec![f]).collect()
                } else {
                    vec![files]
                };

                let handles = partitions
                    .into_iter()
                    .map(|partition_files| {
                        let engine = engine.clone();
                        let state = final_state.clone();
                        let dispatcher = dispatcher.clone();

                        thread::spawn(move || -> DeltaResult<Vec<String>> {
                            // Set the dispatcher in this thread to capture logs
                            let _guard = dispatcher.map(|d| tracing::dispatcher::set_default(&d));

                            assert!(!partition_files.is_empty());

                            let mut phase2 = Phase2ScanMetadata::try_new(
                                engine.clone(),
                                state,
                                partition_files,
                            )?;

                            phase2.try_fold(Vec::new(), |acc, metadata_res| {
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

                for handle in handles {
                    let paths = handle.join().expect("Thread panicked")?;
                    all_paths.extend(paths);
                }
            }
        }

        all_paths.sort();
        assert_eq!(
            all_paths, expected_paths,
            "Parallel workflow paths don't match scan_metadata paths for table '{}'",
            table_name
        );

        Ok(())
    }

    /// Extract a metric value from logs by searching for "metric_name=value"
    fn extract_metric(logs: &str, metric_name: &str) -> u64 {
        if let Some(pos) = logs.find(&format!("{}=", metric_name)) {
            let after = &logs[pos + metric_name.len() + 1..];
            if let Some(space_pos) = after.find(char::is_whitespace) {
                let value_str = &after[..space_pos];
                value_str.parse().unwrap_or_else(|_| {
                    panic!("Failed to parse {} value: {}", metric_name, value_str)
                })
            } else {
                panic!("Failed to find end of {} value", metric_name);
            }
        } else {
            panic!("Failed to find {} in logs", metric_name);
        }
    }

    /// Expected metric values for a test case
    #[derive(Debug, Clone)]
    struct ExpectedMetrics {
        // Phase 1 counter metrics
        phase1_adds: u64,
        phase1_removes: u64,
        phase1_non_file_actions: u64,
        phase1_data_skipping_filtered: u64,
        phase1_partition_pruning_filtered: u64,

        // Phase 2 counter metrics (None if Phase 2 not expected)
        phase2_adds_per_run: Option<u64>,
        phase2_removes_per_run: Option<u64>,
        phase2_non_file_actions_per_run: Option<u64>,
        phase2_data_skipping_filtered_per_run: Option<u64>,
        phase2_partition_pruning_filtered_per_run: Option<u64>,
    }

    fn verify_metrics_in_logs(
        logs: &str,
        table_name: &str,
        expected: &ExpectedMetrics,
        num_test_runs: usize,
    ) {
        // Verify Phase 1 metrics were logged
        assert!(
            logs.contains("Completed Phase 1 scan metadata"),
            "Expected Phase 1 completion log for table '{}'",
            table_name
        );

        // Extract and verify counter values from Phase 1
        let phase1_adds = extract_metric(logs, "num_adds");
        let phase1_removes = extract_metric(logs, "num_removes");
        let phase1_non_file_actions = extract_metric(logs, "num_non_file_actions");
        let data_skipping_filtered = extract_metric(logs, "data_skipping_filtered");
        let partition_pruning_filtered = extract_metric(logs, "partition_pruning_filtered");

        assert_eq!(
            phase1_adds, expected.phase1_adds,
            "Phase 1 num_adds mismatch for {}",
            table_name
        );
        assert_eq!(
            phase1_removes, expected.phase1_removes,
            "Phase 1 num_removes mismatch for {}",
            table_name
        );
        assert_eq!(
            phase1_non_file_actions, expected.phase1_non_file_actions,
            "Phase 1 num_non_file_actions mismatch for {}",
            table_name
        );
        assert_eq!(
            data_skipping_filtered, expected.phase1_data_skipping_filtered,
            "Phase 1 data_skipping_filtered mismatch for {}",
            table_name
        );
        assert_eq!(
            partition_pruning_filtered, expected.phase1_partition_pruning_filtered,
            "Phase 1 partition_pruning_filtered mismatch for {}",
            table_name
        );

        // Verify timing metrics are present and parseable
        let _dedup_time = extract_metric(logs, "dedup_visitor_time_ms");
        let _data_skipping_time = extract_metric(logs, "data_skipping_time_ms");
        let _partition_pruning_time = extract_metric(logs, "partition_pruning_time_ms");
        let _phase1_duration = extract_metric(logs, "phase1_duration_ms");

        // Verify Phase 2 metrics if expected
        if let Some(expected_adds_per_run) = expected.phase2_adds_per_run {
            assert!(
                logs.contains("Phase 2 needed"),
                "Expected 'Phase 2 needed' in logs for table '{}'",
                table_name
            );
            assert!(
                logs.contains("Completed Phase 2 scan metadata"),
                "Expected Phase 2 completion log for table '{}'",
                table_name
            );
            assert!(
                logs.contains("phase2_duration_ms"),
                "Expected phase2_duration_ms in logs for table '{}'",
                table_name
            );
            assert!(
                logs.contains("thread_id"),
                "Expected thread_id in Phase 2 logs for table '{}'",
                table_name
            );
            assert!(
                logs.contains("Phase 2 (parallel) completed"),
                "Expected Phase 2 timing log for table '{}'",
                table_name
            );

            // Extract Phase 2 counter values
            // Note: There may be multiple "Completed Phase 2" logs (from multiple test configurations)
            // Validate that EVERY Phase 2 log has the expected counter values
            let mut total_phase2_adds = 0u64;
            let mut search_start = 0;
            let mut phase2_count = 0;

            while let Some(pos) = logs[search_start..].find("Completed Phase 2 scan metadata") {
                let absolute_pos = search_start + pos;
                let remaining = &logs[absolute_pos..];
                phase2_count += 1;

                // Extract and validate num_adds
                let adds = extract_metric(remaining, "num_adds");
                total_phase2_adds += adds;
                assert_eq!(
                    adds, expected_adds_per_run,
                    "Phase 2 log #{} num_adds mismatch for {}",
                    phase2_count, table_name
                );

                // Extract and validate num_removes
                let removes = extract_metric(remaining, "num_removes");
                assert_eq!(
                    removes,
                    expected.phase2_removes_per_run.unwrap(),
                    "Phase 2 log #{} num_removes mismatch for {}",
                    phase2_count,
                    table_name
                );

                // Extract and validate num_non_file_actions
                let non_file_actions = extract_metric(remaining, "num_non_file_actions");
                assert_eq!(
                    non_file_actions,
                    expected.phase2_non_file_actions_per_run.unwrap(),
                    "Phase 2 log #{} num_non_file_actions mismatch for {}",
                    phase2_count,
                    table_name
                );

                // Extract and validate data_skipping_filtered
                let data_skipping_filtered = extract_metric(remaining, "data_skipping_filtered");
                assert_eq!(
                    data_skipping_filtered,
                    expected.phase2_data_skipping_filtered_per_run.unwrap(),
                    "Phase 2 log #{} data_skipping_filtered mismatch for {}",
                    phase2_count,
                    table_name
                );

                // Extract and validate partition_pruning_filtered
                let partition_pruning_filtered =
                    extract_metric(remaining, "partition_pruning_filtered");
                assert_eq!(
                    partition_pruning_filtered,
                    expected.phase2_partition_pruning_filtered_per_run.unwrap(),
                    "Phase 2 log #{} partition_pruning_filtered mismatch for {}",
                    phase2_count,
                    table_name
                );

                search_start = absolute_pos + 1;
            }

            // Verify total adds across all Phase 2 runs
            let expected_total = expected_adds_per_run * num_test_runs as u64;
            assert_eq!(
                total_phase2_adds, expected_total,
                "Expected total of {} adds across all Phase 2 runs for {} ({} runs × {} adds each), found: {}",
                expected_total, table_name, num_test_runs, expected_adds_per_run, total_phase2_adds
            );
        }
    }

    /// Tests parallel workflow with sidecars and verifies metrics logging.
    ///
    /// This parameterized test covers both JSON and Parquet checkpoint sidecars,
    /// with all combinations of serialization and worker configurations.
    ///
    /// Note: This test captures logs from spawned threads by sharing the tracing dispatcher.
    /// If running with other tests in parallel causes flakiness, use `--test-threads=1`.
    #[rstest::rstest]
    #[case::json_sidecars(
        "v2-checkpoints-json-with-sidecars",
        None,
        ExpectedMetrics {
            phase1_adds: 0,
            phase1_removes: 0,
            phase1_non_file_actions: 5,
            phase1_data_skipping_filtered: 0,
            phase1_partition_pruning_filtered: 0,
            phase2_adds_per_run: Some(101),
            phase2_removes_per_run: Some(0),
            phase2_non_file_actions_per_run: Some(0),
            phase2_data_skipping_filtered_per_run: Some(0),
            phase2_partition_pruning_filtered_per_run: Some(0),
        }
    )]
    #[case::parquet_sidecars(
        "v2-checkpoints-parquet-with-sidecars",
        None,
        ExpectedMetrics {
            phase1_adds: 0,
            phase1_removes: 0,
            phase1_non_file_actions: 5,
            phase1_data_skipping_filtered: 0,
            phase1_partition_pruning_filtered: 0,
            phase2_adds_per_run: Some(101),
            phase2_removes_per_run: Some(0),
            phase2_non_file_actions_per_run: Some(0),
            phase2_data_skipping_filtered_per_run: Some(0),
            phase2_partition_pruning_filtered_per_run: Some(0),
        }
    )]
    #[case::json_sidecars_with_predicate(
        "v2-checkpoints-json-with-sidecars",
        Some({
            use crate::expressions::{column_expr, Expression as Expr};
            Arc::new(Expr::gt(column_expr!("id"), Expr::literal(20i64)))
        }),
        ExpectedMetrics {
            phase1_adds: 0,
            phase1_removes: 0,
            phase1_non_file_actions: 5,
            phase1_data_skipping_filtered: 0,
            phase1_partition_pruning_filtered: 0,
            // Data skipping predicate filters 4 files (101 → 97)
            phase2_adds_per_run: Some(97),
            phase2_removes_per_run: Some(0),
            phase2_non_file_actions_per_run: Some(0),
            phase2_data_skipping_filtered_per_run: Some(4),
            phase2_partition_pruning_filtered_per_run: Some(0),
        }
    )]
    fn test_parallel_workflow_with_metrics(
        #[case] table_name: &str,
        #[case] predicate: Option<PredicateRef>,
        #[case] expected: ExpectedMetrics,
        #[values(false, true)] with_serde: bool,
        #[values(false, true)] one_file_per_worker: bool,
    ) -> DeltaResult<()> {
        use test_utils::LoggingTest;

        // Set up log capture
        let logging_test = LoggingTest::new();

        // Capture the dispatcher to share with spawned threads
        let dispatcher = tracing::dispatcher::get_default(|d| d.clone());

        verify_parallel_workflow(
            table_name,
            predicate,
            with_serde,
            one_file_per_worker,
            Some(dispatcher),
        )?;

        // Verify metrics were logged
        let logs = logging_test.logs();
        verify_metrics_in_logs(&logs, table_name, &expected, 1);

        Ok(())
    }

    /// Tests parallel workflow with a table that doesn't need Phase 2.
    ///
    /// This test verifies that tables with single-part checkpoints complete
    /// in Phase 1 without requiring distributed processing.
    #[rstest::rstest]
    fn test_no_parallel_phase_needed(
        #[values(false, true)] with_serde: bool,
        #[values(false, true)] one_file_per_worker: bool,
    ) -> DeltaResult<()> {
        verify_parallel_workflow(
            "table-without-dv-small",
            None,
            with_serde,
            one_file_per_worker,
            None,
        )
    }

    #[test]
    fn test_parallel_with_skip_stats() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("v2-checkpoints-json-with-sidecars")?;

        // Get expected paths using single-node scan_metadata with skip_stats=true
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_skip_stats(true)
            .build()?;
        let mut single_node_iter = scan.scan_metadata(engine.as_ref())?;
        let mut expected_paths = single_node_iter.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                assert!(
                    scan_file.stats.is_none(),
                    "Single-node: scan_file.stats should be None when skip_stats=true"
                );
                ps.push(scan_file.path);
            })
        })?;
        expected_paths.sort();

        // Run parallel workflow with skip_stats=true
        let scan = snapshot.scan_builder().with_skip_stats(true).build()?;
        let mut phase1 = scan.parallel_scan_metadata(engine.clone())?;

        // Verify stats is None in phase1 results and collect paths
        let mut all_paths = phase1.try_fold(Vec::new(), |acc, metadata_res| {
            metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                assert!(
                    scan_file.stats.is_none(),
                    "Phase1: scan_file.stats should be None when skip_stats=true"
                );
                ps.push(scan_file.path);
            })
        })?;

        match phase1.finish()? {
            AfterPhase1ScanMetadata::Done => {}
            AfterPhase1ScanMetadata::Phase2 { state, files } => {
                // Verify stats is None in phase2 results and collect paths
                let mut phase2 =
                    Phase2ScanMetadata::try_new(engine.clone(), Arc::new(state), files)?;

                let phase2_paths = phase2.try_fold(Vec::new(), |acc, metadata_res| {
                    metadata_res?.visit_scan_files(acc, |ps: &mut Vec<String>, scan_file| {
                        assert!(
                            scan_file.stats.is_none(),
                            "Phase2: scan_file.stats should be None when skip_stats=true"
                        );
                        ps.push(scan_file.path);
                    })
                })?;

                all_paths.extend(phase2_paths);
            }
        }

        // Verify parallel workflow returns same files as single-node
        all_paths.sort();
        assert_eq!(
            all_paths, expected_paths,
            "Parallel workflow with skip_stats=true should return same files as single-node scan_metadata"
        );

        Ok(())
    }
}
