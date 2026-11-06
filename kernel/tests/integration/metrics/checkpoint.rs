//! Completion metrics for checkpoint writes, including reconciled counts and hint publication.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::PredicateRef;
use delta_kernel::metrics::{
    CheckpointWrite, CheckpointWriteMode, CheckpointWriteOutcome, MetricEvent,
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
) -> KernelResult<()> {
    let features = if v2 {
        FeatureSet::new().v2_checkpoint()
    } else {
        FeatureSet::empty()
    };
    let table = TestTableBuilder::new()
        .with_features(features)
        .with_log_state(LogState::with_latest_version(2))
        .with_data(1, 1)
        .build()?;
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
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let (result, checkpointed) = snapshot.checkpoint(engine.as_ref(), spec.as_ref())?;

    assert_eq!(result, CheckpointWriteResult::Written);
    assert_eq!(checkpointed.version(), snapshot.version());
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.mode, mode);
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected Written");
    };
    assert_eq!(stats.num_add_files, 1);
    assert_eq!(stats.num_remove_files, 1);
    assert_eq!(stats.num_sidecars, num_sidecars);
    assert_eq!(stats.num_actions, 4 + u64::from(v2) + num_sidecars);

    let files: Vec<_> = table.store().list(None).try_collect().await?;
    let checkpoint_files: Vec<_> = files
        .iter()
        .filter(|file| {
            let path = file.location.as_ref();
            path.starts_with("_delta_log/") && path.ends_with(".parquet")
        })
        .collect();
    assert_eq!(checkpoint_files.len() as u64, 1 + num_sidecars);
    let mut bytes_written = 0;
    let mut num_actions = 0;
    for file in checkpoint_files {
        let bytes = table.store().get(&file.location).await?.bytes().await?;
        bytes_written += bytes.len() as u64;
        num_actions += ParquetRecordBatchReaderBuilder::try_new(bytes)?
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
    assert_eq!(events[1].outcome, CheckpointWriteOutcome::AlreadyExists);
    assert_eq!(events[1].mode, CheckpointWriteMode::NotAttempted);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sidecar_mode_with_no_file_actions_reports_zero_sidecars() -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .build()?;
    let engine = checkpoint_engine(&table);
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let (result, _checkpointed) = snapshot.checkpoint(
        engine.as_ref(),
        Some(&CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        })),
    )?;
    assert_eq!(result, CheckpointWriteResult::Written);
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let event = &events[0];
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
) -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_features(if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::empty()
        })
        .build()?;
    let engine = checkpoint_engine(&table);
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    assert!(snapshot.checkpoint(engine.as_ref(), Some(&spec)).is_err());

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, CheckpointWriteOutcome::Error);
    assert_eq!(events[0].mode, CheckpointWriteMode::NotAttempted);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_duration_includes_data_writes_and_excludes_hint_writes(
    #[values(false, true)] fail_hint: bool,
) -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .build()?;
    let checkpoint_delay = Duration::from_millis(20);
    let engine = ControlledCheckpointEngine::new(
        checkpoint_engine(&table),
        checkpoint_delay,
        None,
        Duration::from_millis(40),
        fail_hint,
    );
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let started = Instant::now();
    let result = snapshot.checkpoint(&engine, None);
    let total_duration = started.elapsed();

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.mode, CheckpointWriteMode::V1);
    assert!(event.duration >= checkpoint_delay);
    let hint_duration = engine.json.hint_duration.lock().unwrap().unwrap();
    assert!(total_duration.saturating_sub(event.duration) >= hint_duration);
    if fail_hint {
        assert!(result.is_err());
        assert_eq!(event.outcome, CheckpointWriteOutcome::Error);
    } else {
        assert_eq!(result?.0, CheckpointWriteResult::Written);
        let CheckpointWriteOutcome::Written { stats } = event.outcome else {
            panic!("expected Written");
        };
        assert_eq!(stats.num_add_files, 1);
    }
    Ok(())
}

#[rstest]
#[case::write_error(InjectedWriteError::Generic, CheckpointWriteOutcome::Error)]
#[case::already_exists(
    InjectedWriteError::AlreadyExists,
    CheckpointWriteOutcome::AlreadyExists
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_write_errors_preserve_mode_and_duration(
    #[case] error: InjectedWriteError,
    #[case] outcome: CheckpointWriteOutcome,
) -> KernelResult<()> {
    let table = TestTableBuilder::new().build()?;
    let delay = Duration::from_millis(20);
    let engine = ControlledCheckpointEngine::new(
        checkpoint_engine(&table),
        delay,
        Some(error),
        Duration::ZERO,
        false,
    );
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let result = snapshot.checkpoint(&engine, None);

    if outcome == CheckpointWriteOutcome::AlreadyExists {
        assert_eq!(result?.0, CheckpointWriteResult::AlreadyExists);
    } else {
        assert!(result.is_err());
    }
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, outcome);
    assert_eq!(events[0].mode, CheckpointWriteMode::V1);
    assert!(events[0].duration >= delay);
    assert!(engine.json.hint_duration.lock().unwrap().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_with_newer_hint_reports_written_without_publishing_hint() -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2).with_checkpoint_at([2]))
        .build()?;
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
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let (result, _checkpointed) = snapshot.checkpoint(&engine, None)?;

    assert_eq!(result, CheckpointWriteResult::Written);
    assert!(engine.json.hint_duration.lock().unwrap().is_none());
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mode, CheckpointWriteMode::V1);
    let CheckpointWriteOutcome::Written { stats } = events[0].outcome else {
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

fn checkpoint_events(reporter: &CapturingReporter) -> Vec<CheckpointWrite> {
    reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::CheckpointWrite(event) => Some(event),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum InjectedWriteError {
    Generic,
    AlreadyExists,
}

/// Controls checkpoint data and hint writes while delegating reads to the default engine.
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
        self.inner
            .read_json_files(files, physical_schema, predicate)
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
