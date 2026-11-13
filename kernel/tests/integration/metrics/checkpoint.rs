//! Checkpoint success and failure metrics, including reconciled counts and hint publication.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::PredicateRef;
use delta_kernel::metrics::{
    CheckpointProcessingStats, CheckpointWriteMode, CheckpointWriteOutcome, MetricEvent,
};
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::CheckpointWriteResult;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
    FilteredEngineData, JsonHandler, KernelError, KernelResult, KernelResultIterator,
    ParquetFooter, ParquetHandler, Snapshot, StorageHandler,
};
use futures::TryStreamExt;
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::table_builder::{FeatureSet, LogState, TestTable, TestTableBuilder};
use test_utils::{CapturingReporter, install_thread_local_metrics_reporter};
use url::Url;

use crate::common::write_utils::get_scan_files;

#[rstest]
#[case::default_v1(false, None, CheckpointWriteMode::V1, 0)]
#[case::explicit_v1(false, Some(CheckpointSpec::V1), CheckpointWriteMode::V1, 0)]
#[case::default_v2(true, None, CheckpointWriteMode::V2WithoutSidecars, 0)]
#[case::explicit_v2(
    true,
    Some(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)),
    CheckpointWriteMode::V2WithoutSidecars,
    0
)]
#[case::v2_sidecars(
    true,
    Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(1),
    })),
    CheckpointWriteMode::V2WithSidecars,
    2
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_reports_reconciled_counts_and_stored_bytes(
    #[case] v2: bool,
    #[case] spec: Option<CheckpointSpec>,
    #[case] mode: CheckpointWriteMode,
    #[case] num_sidecars: u64,
    #[values(false, true)] previous_checkpoint: bool,
) -> delta_kernel::Result<()> {
    let features = if v2 {
        FeatureSet::new().v2_checkpoint()
    } else {
        FeatureSet::empty()
    };
    let table = TestTableBuilder::new()
        .with_features(features)
        .with_log_state(
            LogState::with_latest_version(2)
                .maybe_with_checkpoint_at(previous_checkpoint.then_some(1)),
        )
        .with_data(1, 1)
        .build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = checkpoint_engine(&table);
    let first_data = Snapshot::builder_for(table.table_root())
        .at_version(1)
        .build(engine.as_ref())?;
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let mut transaction = snapshot
        .update_table(UpdateTableOperation::Write, "checkpoint-metrics-test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    for files in get_scan_files(first_data, engine.as_ref())? {
        transaction.remove_files(files);
    }
    let snapshot = transaction
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let (result, checkpointed) = snapshot.checkpoint(engine.as_ref(), spec.as_ref())?;

    assert_eq!(result, CheckpointWriteResult::Written);
    assert_eq!(checkpointed.version(), snapshot.version());
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let MetricEvent::CheckpointWriteSuccess(event) = &events[0] else {
        panic!("expected CheckpointWriteSuccess");
    };
    assert_eq!(event.mode, mode);
    let operation_id = event.operation_id;
    assert_ne!(operation_id.as_bytes(), [0; 16]);
    assert_eq!(event.version, snapshot.version());
    let processing = &event.processing;
    assert_eq!(processing.num_add_files_seen, 2);
    assert_eq!(processing.num_remove_files_seen, 1);
    assert_eq!(processing.num_duplicate_file_actions, 1);
    assert_eq!(processing.num_expired_tombstones, 0);
    assert_eq!(processing.num_reconciled_actions, 4);
    assert_eq!(processing.peak_hash_set_size, 2);
    assert_eq!(processing.num_checkpoint_rows > 0, previous_checkpoint);
    assert!(processing.num_log_rows > 0);
    assert_eq!(
        processing.num_input_batches,
        processing.num_log_rows + processing.num_checkpoint_rows
    );
    assert!(processing.input_setup_duration > Duration::ZERO);
    assert!(processing.input_read_duration > Duration::ZERO);
    assert!(processing.reconciliation_duration > Duration::ZERO);
    assert!(processing.stats_transform_duration > Duration::ZERO);
    assert!(processing.selection_duration > Duration::ZERO);
    assert_eq!(
        processing.sidecar_split_duration > Duration::ZERO,
        num_sidecars > 0
    );
    assert_processing_within_duration(processing, event.duration);
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected Written");
    };
    assert_eq!(stats.num_add_files, 1);
    assert_eq!(stats.num_remove_files, 1);
    assert_eq!(stats.num_sidecars, num_sidecars);
    assert_eq!(stats.num_actions, 4 + u64::from(v2) + num_sidecars);

    let files: Vec<_> = table
        .store()
        .list(None)
        .try_collect()
        .await
        .map_err(KernelError::from)?;
    let checkpoint_files: Vec<_> = files
        .iter()
        .filter(|file| {
            let path = file.location.as_ref();
            path.starts_with("_delta_log/")
                && path.ends_with(".parquet")
                && !path.ends_with("00000000000000000001.checkpoint.parquet")
        })
        .collect();
    assert_eq!(checkpoint_files.len() as u64, 1 + num_sidecars);
    let mut bytes_written = 0;
    let mut num_actions = 0;
    for file in checkpoint_files {
        let bytes = table
            .store()
            .get(&file.location)
            .await
            .map_err(KernelError::from)?
            .bytes()
            .await
            .map_err(KernelError::from)?;
        bytes_written += bytes.len() as u64;
        num_actions += ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(KernelError::from)?
            .metadata()
            .file_metadata()
            .num_rows() as u64;
    }
    assert_eq!(stats.bytes_written, bytes_written);
    assert_eq!(stats.num_actions, num_actions);

    let (result, unchanged) = checkpointed.checkpoint(engine.as_ref(), spec.as_ref())?;
    assert_eq!(result, CheckpointWriteResult::AlreadyExists);
    assert!(Arc::ptr_eq(&unchanged, &checkpointed));
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 2);
    let MetricEvent::CheckpointWriteSuccess(event) = &events[1] else {
        panic!("expected CheckpointWriteSuccess");
    };
    assert_eq!(event.outcome, CheckpointWriteOutcome::AlreadyExists);
    assert_eq!(event.mode, CheckpointWriteMode::NotAttempted);
    assert_ne!(event.operation_id, operation_id);
    assert_eq!(event.version, snapshot.version());
    assert_eq!(event.processing, CheckpointProcessingStats::default());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sidecar_mode_with_no_file_actions_reports_zero_sidecars() -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = checkpoint_engine(&table);
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let (result, _checkpointed) = snapshot.checkpoint(
        engine.as_ref(),
        Some(&CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        })),
    )?;
    assert_eq!(result, CheckpointWriteResult::Written);
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let MetricEvent::CheckpointWriteSuccess(event) = &events[0] else {
        panic!("expected CheckpointWriteSuccess");
    };
    assert_eq!(event.mode, CheckpointWriteMode::V2WithSidecars);
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected Written");
    };
    assert_eq!(stats.num_add_files, 0);
    assert_eq!(stats.num_remove_files, 0);
    assert_eq!(stats.num_actions, 3);
    assert_eq!(stats.num_sidecars, 0);
    assert!(stats.bytes_written > 0);
    Ok(())
}

#[rstest]
#[case::v2_not_enabled(false, CheckpointSpec::V2(V2CheckpointConfig::NoSidecar))]
#[case::v1_on_v2_table(true, CheckpointSpec::V1)]
#[case::zero_sidecar_hint(
    true,
    CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(0),
    })
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_checkpoint_spec_reports_error_without_selecting_mode(
    #[case] v2: bool,
    #[case] spec: CheckpointSpec,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::empty()
        })
        .build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = checkpoint_engine(&table);
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;

    assert!(snapshot.checkpoint(engine.as_ref(), Some(&spec)).is_err());

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let MetricEvent::CheckpointWriteFailure(event) = &events[0] else {
        panic!("expected CheckpointWriteFailure");
    };
    assert_eq!(event.mode, CheckpointWriteMode::NotAttempted);
    assert_ne!(event.operation_id.as_bytes(), [0; 16]);
    assert_eq!(event.version, snapshot.version());
    assert_eq!(event.processing, CheckpointProcessingStats::default());
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_duration_includes_data_writes_and_excludes_hint_writes(
    #[values(false, true)] fail_hint: bool,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let checkpoint_delay = Duration::from_millis(20);
    let engine = ControlledCheckpointEngine::new(
        checkpoint_engine(&table),
        checkpoint_delay,
        None,
        Duration::from_millis(40),
        fail_hint,
    );
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;

    let started = Instant::now();
    let result = snapshot.checkpoint(&engine, None);
    let total_duration = started.elapsed();

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (mode, duration, processing) = if fail_hint {
        assert!(result.is_err());
        let MetricEvent::CheckpointWriteFailure(event) = &events[0] else {
            panic!("expected CheckpointWriteFailure");
        };
        (event.mode, event.duration, &event.processing)
    } else {
        assert_eq!(result?.0, CheckpointWriteResult::Written);
        let MetricEvent::CheckpointWriteSuccess(event) = &events[0] else {
            panic!("expected CheckpointWriteSuccess");
        };
        let CheckpointWriteOutcome::Written { stats } = event.outcome else {
            panic!("expected Written");
        };
        assert_eq!(stats.num_add_files, 1);
        (event.mode, event.duration, &event.processing)
    };
    assert_eq!(mode, CheckpointWriteMode::V1);
    assert_eq!(processing.num_add_files_seen, 1);
    assert_eq!(processing.num_reconciled_actions, 3);
    assert_processing_within_duration(processing, duration);
    assert!(duration >= checkpoint_delay);
    let hint_duration = engine.json.hint_duration.lock().unwrap().unwrap();
    assert!(total_duration.saturating_sub(duration) >= hint_duration);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_input_timing_includes_sidecar_peek_and_failing_pulls(
    #[values(false, true)] sidecars: bool,
    #[values(None, Some(0), Some(1))] fail_after: Option<usize>,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .with_log_state(LogState::with_latest_version(1))
        .build()?;
    let inner = checkpoint_engine(&table);
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(inner.as_ref())?;
    let mut engine =
        ControlledCheckpointEngine::new(inner, Duration::ZERO, None, Duration::ZERO, false);
    let delay = Duration::from_millis(20);
    let json = Arc::get_mut(&mut engine.json).unwrap();
    json.input_delay = delay;
    json.input_error_after = fail_after;
    let spec = CheckpointSpec::V2(if sidecars {
        V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }
    } else {
        V2CheckpointConfig::NoSidecar
    });

    let result = snapshot.checkpoint(&engine, Some(&spec));

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (processing, duration, operation_id) = match &events[0] {
        MetricEvent::CheckpointWriteSuccess(event) if fail_after.is_none() => {
            assert_eq!(result?.0, CheckpointWriteResult::Written);
            assert_eq!(event.processing.num_reconciled_actions, 3);
            (&event.processing, event.duration, event.operation_id)
        }
        MetricEvent::CheckpointWriteFailure(event) if fail_after.is_some() => {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected input failure")
            );
            let consumed = fail_after.unwrap() as u64;
            assert_eq!(event.processing.num_input_batches, consumed);
            assert_eq!(event.processing.num_log_rows, consumed);
            (&event.processing, event.duration, event.operation_id)
        }
        event => panic!("unexpected checkpoint event: {event:?}"),
    };
    assert!(processing.input_read_duration >= delay * (fail_after.unwrap_or(0) as u32 + 1));
    assert_processing_within_duration(processing, duration);

    // Reuse the snapshot for a fresh attempt to detect state leaking across calls.
    let (result, _) = snapshot.checkpoint(engine.inner.as_ref(), Some(&spec))?;
    assert_eq!(result, CheckpointWriteResult::Written);
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 2);
    let MetricEvent::CheckpointWriteSuccess(event) = &events[1] else {
        panic!("expected successful retry");
    };
    assert_ne!(event.operation_id, operation_id);
    assert_eq!(event.processing.num_add_files_seen, 1);
    assert_eq!(event.processing.num_reconciled_actions, 3);
    Ok(())
}

#[rstest]
#[case::write_error(InjectedWriteError::Generic)]
#[case::already_exists(InjectedWriteError::AlreadyExists)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_write_errors_preserve_mode_and_duration(
    #[case] error: InjectedWriteError,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new().build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let delay = Duration::from_millis(20);
    let engine = ControlledCheckpointEngine::new(
        checkpoint_engine(&table),
        delay,
        Some(error),
        Duration::ZERO,
        false,
    );
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;

    let result = snapshot.checkpoint(&engine, None);

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (mode, duration) = match error {
        InjectedWriteError::Generic => {
            assert!(result.is_err());
            let MetricEvent::CheckpointWriteFailure(event) = &events[0] else {
                panic!("expected CheckpointWriteFailure");
            };
            (event.mode, event.duration)
        }
        InjectedWriteError::AlreadyExists => {
            assert_eq!(result?.0, CheckpointWriteResult::AlreadyExists);
            let MetricEvent::CheckpointWriteSuccess(event) = &events[0] else {
                panic!("expected CheckpointWriteSuccess");
            };
            assert_eq!(event.outcome, CheckpointWriteOutcome::AlreadyExists);
            (event.mode, event.duration)
        }
    };
    assert_eq!(mode, CheckpointWriteMode::V1);
    assert!(duration >= delay);
    assert!(engine.json.hint_duration.lock().unwrap().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_with_newer_hint_reports_written_without_publishing_hint()
-> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2).with_checkpoint_at([2]))
        .build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = ControlledCheckpointEngine::new(
        checkpoint_engine(&table),
        Duration::ZERO,
        None,
        Duration::ZERO,
        true,
    );
    let snapshot = Snapshot::builder_for(table.table_root())
        .at_version(1)
        .build(&engine)?;

    let (result, _checkpointed) = snapshot.checkpoint(&engine, None)?;

    assert_eq!(result, CheckpointWriteResult::Written);
    assert!(engine.json.hint_duration.lock().unwrap().is_none());
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let MetricEvent::CheckpointWriteSuccess(event) = &events[0] else {
        panic!("expected CheckpointWriteSuccess");
    };
    assert_eq!(event.mode, CheckpointWriteMode::V1);
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected Written");
    };
    assert_eq!(stats.num_add_files, 1);
    assert_eq!(stats.num_remove_files, 0);
    assert_eq!(stats.num_actions, 3);
    assert_eq!(stats.num_sidecars, 0);
    Ok(())
}

fn checkpoint_engine(table: &TestTable) -> Arc<dyn Engine> {
    Arc::new(
        DefaultEngineBuilder::new(table.store().clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .with_batch_size(NonZeroUsize::new(1).unwrap())
            .build(),
    )
}

fn checkpoint_events(reporter: &CapturingReporter) -> Vec<MetricEvent> {
    reporter
        .events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                MetricEvent::CheckpointWriteSuccess(_) | MetricEvent::CheckpointWriteFailure(_)
            )
        })
        .collect()
}

fn assert_processing_within_duration(processing: &CheckpointProcessingStats, duration: Duration) {
    let stages = processing.input_setup_duration
        + processing.input_read_duration
        + processing.reconciliation_duration
        + processing.stats_transform_duration
        + processing.selection_duration
        + processing.sidecar_split_duration;
    assert!(
        stages <= duration,
        "pipeline timers overlap: {processing:?}, total={duration:?}"
    );
}

#[derive(Debug, Clone, Copy)]
enum InjectedWriteError {
    Generic,
    AlreadyExists,
}

/// Injects input delays/errors and controls writes while delegating data processing to the engine.
struct ControlledCheckpointEngine {
    inner: Arc<dyn Engine>,
    json: Arc<ControlledJsonHandler>,
    parquet: Arc<ControlledParquetHandler>,
}

impl ControlledCheckpointEngine {
    fn new(
        inner: Arc<dyn Engine>,
        checkpoint_delay: Duration,
        checkpoint_error: Option<InjectedWriteError>,
        hint_delay: Duration,
        fail_hint: bool,
    ) -> Self {
        Self {
            json: Arc::new(ControlledJsonHandler {
                inner: inner.json_handler(),
                hint_delay,
                fail_hint,
                hint_duration: Mutex::new(None),
                input_delay: Duration::ZERO,
                input_error_after: None,
            }),
            parquet: Arc::new(ControlledParquetHandler {
                inner: inner.parquet_handler(),
                checkpoint_delay,
                checkpoint_error,
            }),
            inner,
        }
    }
}

impl Engine for ControlledCheckpointEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

struct ControlledJsonHandler {
    inner: Arc<dyn JsonHandler>,
    hint_delay: Duration,
    fail_hint: bool,
    hint_duration: Mutex<Option<Duration>>,
    input_delay: Duration,
    input_error_after: Option<usize>,
}

impl JsonHandler for ControlledJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let mut input = self
            .inner
            .read_json_files(files, physical_schema, predicate)?;
        let delay = self.input_delay;
        let fail_after = self.input_error_after;
        let mut num_pulls = 0;
        Ok(Box::new(std::iter::from_fn(move || {
            std::thread::sleep(delay);
            let fail = fail_after == Some(num_pulls);
            num_pulls += 1;
            if fail {
                Some(Err(KernelError::generic("injected input failure")))
            } else {
                input.next()
            }
        })))
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        if path.path().ends_with("/_last_checkpoint") {
            let started = Instant::now();
            std::thread::sleep(self.hint_delay);
            let result = if self.fail_hint {
                Err(KernelError::generic("injected hint write failure"))
            } else {
                self.inner.write_json_file(path, data, overwrite)
            };
            *self.hint_duration.lock().unwrap() = Some(started.elapsed());
            result
        } else {
            self.inner.write_json_file(path, data, overwrite)
        }
    }
}

struct ControlledParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    checkpoint_delay: Duration,
    checkpoint_error: Option<InjectedWriteError>,
}

impl ParquetHandler for ControlledParquetHandler {
    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }

    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: FileDataReadResultIterator,
    ) -> KernelResult<FileSize> {
        std::thread::sleep(self.checkpoint_delay);
        match self.checkpoint_error {
            Some(InjectedWriteError::Generic) => {
                Err(KernelError::generic("injected checkpoint write failure"))
            }
            Some(InjectedWriteError::AlreadyExists) => {
                Err(KernelError::FileAlreadyExists(location.to_string()))
            }
            None => self.inner.write_parquet_file(location, data),
        }
    }
}
