//! Checkpoint success and failure metrics, including reconciled counts and hint publication.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::DelegatingEngine;
use delta_kernel::expressions::PredicateRef;
use delta_kernel::metrics::{
    CheckpointOutputStats, CheckpointProcessingStats, CheckpointWriteMode, CheckpointWriteOutcome,
    MetricEvent,
};
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::CheckpointWriteResult;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{
    Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize, FileSlice,
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
    #[values(1, 16)] batch_size: usize,
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
    let engine = checkpoint_engine(&table, batch_size);
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
    let processing = &event.processing_stats;
    // Include input actions discarded by reconciliation.
    let expected_counts = match (previous_checkpoint, v2) {
        (false, _) => (9, 0),
        (true, false) => (4, 3),
        (true, true) => (4, 4),
    };
    assert_eq!(
        (
            processing.num_actions_from_log_tail,
            processing.num_actions_from_checkpoint,
        ),
        expected_counts,
    );
    assert!(processing.log_tail_input_duration > Duration::ZERO);
    if previous_checkpoint {
        assert!(processing.checkpoint_input_duration > Duration::ZERO);
    }
    assert!(processing.reconciliation_duration > Duration::ZERO);
    assert!(processing.stats_transform_duration > Duration::ZERO);
    assert!(processing.output_preparation_duration > Duration::ZERO);
    assert_accounted_within_duration(processing, &event.output_stats, event.duration);
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected Written");
    };
    assert_eq!(stats.num_add_files, 1);
    assert_eq!(stats.num_remove_files, 1);
    assert_eq!(stats.num_sidecars, num_sidecars);
    assert_eq!(event.output_stats.num_sidecars_written, num_sidecars);
    assert!(event.output_stats.parquet_write_duration > Duration::ZERO);
    assert!(event.output_stats.verification_duration > Duration::ZERO);
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
    assert_eq!(stats.size_in_bytes, bytes_written);
    assert_eq!(event.output_stats.bytes_written, bytes_written);
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
    assert_eq!(event.processing_stats, CheckpointProcessingStats::default());
    assert_eq!(event.output_stats, CheckpointOutputStats::default());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sidecar_mode_with_no_file_actions_reports_zero_sidecars() -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .build()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine = checkpoint_engine(&table, 1);
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
    assert!(stats.size_in_bytes > 0);
    assert_eq!(event.output_stats.num_sidecars_written, 0);
    assert_eq!(event.output_stats.bytes_written, stats.size_in_bytes);
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
    let engine = checkpoint_engine(&table, 1);
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
    assert_eq!(event.processing_stats, CheckpointProcessingStats::default());
    assert_eq!(event.output_stats, CheckpointOutputStats::default());
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
        checkpoint_engine(&table, 1),
        checkpoint_delay,
        None,
        Duration::from_millis(40),
        fail_hint,
    );
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine.engine())?;

    let started = Instant::now();
    let result = snapshot.checkpoint(&engine.engine(), None);
    let total_duration = started.elapsed();

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (mode, duration, processing, output) = if fail_hint {
        assert!(result.is_err());
        let MetricEvent::CheckpointWriteFailure(event) = &events[0] else {
            panic!("expected CheckpointWriteFailure");
        };
        (
            event.mode,
            event.duration,
            &event.processing_stats,
            &event.output_stats,
        )
    } else {
        assert_eq!(result?.0, CheckpointWriteResult::Written);
        let MetricEvent::CheckpointWriteSuccess(event) = &events[0] else {
            panic!("expected CheckpointWriteSuccess");
        };
        let CheckpointWriteOutcome::Written { stats } = event.outcome else {
            panic!("expected Written");
        };
        assert_eq!(stats.num_add_files, 1);
        (
            event.mode,
            event.duration,
            &event.processing_stats,
            &event.output_stats,
        )
    };
    assert_eq!(mode, CheckpointWriteMode::V1);
    assert_eq!(processing.num_actions_from_log_tail, 5);
    assert_eq!(processing.num_actions_from_checkpoint, 0);
    assert_accounted_within_duration(processing, output, duration);
    assert!(output.parquet_write_duration >= checkpoint_delay);
    assert!(output.bytes_written > 0);
    assert_eq!(output.num_sidecars_written, 0);
    assert!(duration >= checkpoint_delay);
    let hint_duration = engine.json.hint_duration.lock().unwrap().unwrap();
    assert!(total_duration.saturating_sub(duration) >= hint_duration);
    Ok(())
}

#[rstest]
#[case::complete(false, None)]
#[case::setup_failure(true, None)]
#[case::first_pull_failure(false, Some(0))]
#[case::partial_input_failure(false, Some(1))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_input_timing_includes_setup_sidecar_peek_and_failing_pulls(
    #[values(false, true)] sidecars: bool,
    #[case] fail_setup: bool,
    #[case] fail_after: Option<usize>,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .with_log_state(LogState::with_latest_version(1))
        .build()?;
    let inner = checkpoint_engine(&table, 1);
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(inner.as_ref())?;
    let write_delay = Duration::from_millis(5);
    let mut engine =
        ControlledCheckpointEngine::new(inner, write_delay, None, Duration::ZERO, false);
    let delay = Duration::from_millis(20);
    let json = Arc::get_mut(&mut engine.json).unwrap();
    json.input.delay = delay;
    json.input.fail_after = fail_after;
    json.input.fail_setup = fail_setup;
    let spec = CheckpointSpec::V2(if sidecars {
        V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }
    } else {
        V2CheckpointConfig::NoSidecar
    });

    let result = snapshot.checkpoint(&engine.engine(), Some(&spec));

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (processing, output, duration, operation_id) = match &events[0] {
        MetricEvent::CheckpointWriteSuccess(event) if !fail_setup && fail_after.is_none() => {
            assert_eq!(result?.0, CheckpointWriteResult::Written);
            assert_eq!(event.processing_stats.num_actions_from_log_tail, 5);
            (
                &event.processing_stats,
                &event.output_stats,
                event.duration,
                event.operation_id,
            )
        }
        MetricEvent::CheckpointWriteFailure(event) if fail_setup || fail_after.is_some() => {
            assert!(result.unwrap_err().to_string().contains(if fail_setup {
                "injected input setup failure"
            } else {
                "injected input failure"
            }));
            let consumed = fail_after.unwrap_or(0) as u64;
            assert_eq!(event.processing_stats.num_actions_from_log_tail, consumed);
            (
                &event.processing_stats,
                &event.output_stats,
                event.duration,
                event.operation_id,
            )
        }
        event => panic!("unexpected checkpoint event: {event:?}"),
    };
    assert_eq!(processing.num_actions_from_checkpoint, 0);
    assert!(processing.log_tail_input_duration >= engine.json.input.minimum_duration());
    assert_accounted_within_duration(processing, output, duration);
    assert!(
        output.parquet_write_duration
            >= write_delay * engine.parquet.write_calls.load(Ordering::Relaxed) as u32
    );

    // Reuse the snapshot for a fresh attempt to detect state leaking across calls.
    let (result, _) = snapshot.checkpoint(engine.inner.as_ref(), Some(&spec))?;
    assert_eq!(result, CheckpointWriteResult::Written);
    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 2);
    let MetricEvent::CheckpointWriteSuccess(event) = &events[1] else {
        panic!("expected successful retry");
    };
    assert_ne!(event.operation_id, operation_id);
    assert_eq!(event.processing_stats.num_actions_from_log_tail, 5);
    assert_eq!(event.processing_stats.num_actions_from_checkpoint, 0);
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
        checkpoint_engine(&table, 1),
        delay,
        Some(error),
        Duration::ZERO,
        false,
    );
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine.engine())?;

    let result = snapshot.checkpoint(&engine.engine(), None);

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
        checkpoint_engine(&table, 1),
        Duration::ZERO,
        None,
        Duration::ZERO,
        true,
    );
    let snapshot = Snapshot::builder_for(table.table_root())
        .at_version(1)
        .build(&engine.engine())?;

    let (result, _checkpointed) = snapshot.checkpoint(&engine.engine(), None)?;

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

#[rstest]
#[case::complete(false, None)]
#[case::setup_failure(true, None)]
#[case::first_pull_failure(false, Some(0))]
#[case::partial_input_failure(false, Some(1))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn previous_checkpoint_input_timing_includes_setup_and_pulls(
    #[values(false, true)] sidecars: bool,
    #[case] fail_setup: bool,
    #[case] fail_after: Option<usize>,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(if sidecars {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::empty()
        })
        .with_log_state(
            LogState::with_latest_version(2)
                .with_checkpoint_at([1])
                .with_sidecars_if_enabled(Some(1)),
        )
        .with_data(1, 1)
        .build()?;
    let inner = checkpoint_engine(&table, 1);
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(inner.as_ref())?;
    let mut engine =
        ControlledCheckpointEngine::new(inner, Duration::ZERO, None, Duration::ZERO, false);
    let input = &mut Arc::get_mut(&mut engine.parquet).unwrap().input;
    input.delay = Duration::from_millis(10);
    input.fail_setup = fail_setup;
    input.fail_after = fail_after;

    let result = snapshot.checkpoint(&engine.engine(), None);

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (processing, output, duration) = match &events[0] {
        MetricEvent::CheckpointWriteSuccess(event) => {
            assert!(!fail_setup && fail_after.is_none());
            assert_eq!(result?.0, CheckpointWriteResult::Written);
            assert_eq!(event.processing_stats.num_actions_from_log_tail, 2);
            assert_eq!(
                event.processing_stats.num_actions_from_checkpoint,
                if sidecars { 5 } else { 3 }
            );
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        MetricEvent::CheckpointWriteFailure(event) => {
            assert!(fail_setup || fail_after.is_some());
            assert!(result.unwrap_err().to_string().contains("injected input"));
            assert_eq!(event.output_stats.bytes_written, 0);
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        event => panic!("unexpected checkpoint event: {event:?}"),
    };
    assert!(engine.parquet.input.minimum_duration() > Duration::ZERO);
    assert!(processing.checkpoint_input_duration >= engine.parquet.input.minimum_duration());
    assert_accounted_within_duration(processing, output, duration);
    Ok(())
}

#[rstest]
#[case::fourth_sidecar(4, InjectedWriteError::Generic, false)]
#[case::uploaded_but_failed(4, InjectedWriteError::Generic, true)]
#[case::main_checkpoint(5, InjectedWriteError::Generic, false)]
#[case::already_exists(4, InjectedWriteError::AlreadyExists, false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_retains_successful_sidecar_output_after_write_failure(
    #[case] fail_at: usize,
    #[case] error: InjectedWriteError,
    #[case] write_before_error: bool,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .with_log_state(LogState::with_latest_version(4))
        .with_data(1, 1)
        .build()?;
    let inner = checkpoint_engine(&table, 1);
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(inner.as_ref())?;
    let delay = Duration::from_millis(5);
    let mut engine =
        ControlledCheckpointEngine::new(inner, delay, Some(error), Duration::ZERO, false);
    let parquet = Arc::get_mut(&mut engine.parquet).unwrap();
    parquet.fail_write_at = fail_at;
    parquet.write_before_error = write_before_error;
    Arc::get_mut(&mut engine.storage)
        .unwrap()
        .verification_delay = delay;

    let result = snapshot.checkpoint(
        &engine.engine(),
        Some(&CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        })),
    );

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (processing, output, duration) = match (&events[0], error) {
        (MetricEvent::CheckpointWriteFailure(event), InjectedWriteError::Generic) => {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected checkpoint write failure")
            );
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        (MetricEvent::CheckpointWriteSuccess(event), InjectedWriteError::AlreadyExists) => {
            assert_eq!(result?.0, CheckpointWriteResult::AlreadyExists);
            assert_eq!(event.outcome, CheckpointWriteOutcome::AlreadyExists);
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        (event, _) => panic!("unexpected checkpoint event: {event:?}"),
    };
    assert_eq!(engine.parquet.write_calls.load(Ordering::Relaxed), fail_at);
    assert_eq!(output.num_sidecars_written, (fail_at - 1) as u64);
    let sidecar_dir = snapshot.table_root().join("_delta_log/_sidecars/").unwrap();
    let files = engine
        .inner
        .storage_handler()
        .list_from(&sidecar_dir)?
        .collect::<KernelResult<Vec<_>>>()?;
    assert_eq!(files.len(), fail_at - 1 + usize::from(write_before_error));
    let stored_bytes: u64 = files.iter().map(|file| file.size).sum();
    if write_before_error {
        assert!(output.bytes_written < stored_bytes);
        let successful_bytes: u64 = engine
            .parquet
            .written_files
            .lock()
            .unwrap()
            .iter()
            .map(|(_, size)| size)
            .sum();
        assert_eq!(output.bytes_written, successful_bytes);
    } else {
        assert_eq!(output.bytes_written, stored_bytes);
    }
    assert!(output.bytes_written > 0);
    assert!(processing.num_actions_from_log_tail > 0);
    assert!(output.parquet_write_duration >= delay * fail_at as u32);
    assert!(output.verification_duration >= delay * (fail_at - 1) as u32);
    assert_accounted_within_duration(processing, output, duration);
    Ok(())
}

#[rstest]
#[case::complete(None)]
#[case::head_failure(Some(InjectedVerificationError::Head))]
#[case::size_mismatch(Some(InjectedVerificationError::SizeMismatch))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_output_includes_writes_before_verification_failure(
    #[values(false, true)] sidecars: bool,
    #[case] error: Option<InjectedVerificationError>,
) -> delta_kernel::Result<()> {
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().v2_checkpoint())
        .with_log_state(LogState::with_latest_version(3))
        .with_data(1, 1)
        .build()?;
    let inner = checkpoint_engine(&table, 1);
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(inner.as_ref())?;
    let delay = Duration::from_millis(10);
    let mut engine =
        ControlledCheckpointEngine::new(inner, Duration::ZERO, None, Duration::ZERO, false);
    let storage = Arc::get_mut(&mut engine.storage).unwrap();
    storage.verification_delay = delay;
    storage.verification_error = error;
    storage.fail_verification_at = if sidecars { 2 } else { 1 };
    let spec = CheckpointSpec::V2(if sidecars {
        V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }
    } else {
        V2CheckpointConfig::NoSidecar
    });

    let result = snapshot.checkpoint(&engine.engine(), Some(&spec));

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (processing, output, duration) = match &events[0] {
        MetricEvent::CheckpointWriteFailure(event) => {
            assert!(error.is_some());
            let message = result.unwrap_err().to_string();
            assert!(
                message.contains("injected verification failure")
                    || message.contains("parquet file size mismatch")
            );
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        MetricEvent::CheckpointWriteSuccess(event) => {
            assert!(error.is_none());
            assert_eq!(result?.0, CheckpointWriteResult::Written);
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        event => panic!("unexpected checkpoint event: {event:?}"),
    };
    let expected_sidecars = if sidecars {
        if error.is_some() { 2 } else { 3 }
    } else {
        0
    };
    assert_eq!(output.num_sidecars_written, expected_sidecars);
    let written = engine.parquet.written_files.lock().unwrap();
    let stored_bytes = written
        .iter()
        .map(|(path, _)| {
            engine
                .inner
                .storage_handler()
                .head(path)
                .map(|meta| meta.size)
        })
        .collect::<KernelResult<Vec<_>>>()?
        .iter()
        .sum::<u64>();
    assert_eq!(output.bytes_written, stored_bytes);
    assert!(output.bytes_written > 0);
    assert!(output.verification_duration >= delay * written.len() as u32);
    assert_accounted_within_duration(processing, output, duration);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_checkpoint_input_is_accounted_by_source_role(
    #[values(false, true)] fail_setup: bool,
) -> delta_kernel::Result<()> {
    let name = "v2-checkpoints-json-with-sidecars";
    let test_dir = test_utils::load_test_data("tests/data", name).unwrap();
    let table_root =
        delta_kernel::try_parse_uri(test_dir.path().join(name).to_str().unwrap()).unwrap();
    let inner = test_utils::create_default_engine_mt_executor(&table_root)?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(table_root).build(inner.as_ref())?;
    let snapshot = snapshot
        .update_table(UpdateTableOperation::Write, "checkpoint-metrics-test", true)
        .build(inner.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(inner.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut engine =
        ControlledCheckpointEngine::new(inner, Duration::ZERO, None, Duration::ZERO, false);
    let json = Arc::get_mut(&mut engine.json).unwrap();
    json.input.delay = Duration::from_millis(10);
    json.checkpoint_input.delay = Duration::from_millis(10);
    json.checkpoint_input.fail_setup = fail_setup;
    Arc::get_mut(&mut engine.parquet).unwrap().input.delay = Duration::from_millis(5);

    let result = snapshot.checkpoint(&engine.engine(), None);

    let events = checkpoint_events(&reporter);
    assert_eq!(events.len(), 1);
    let (processing, output, duration) = match &events[0] {
        MetricEvent::CheckpointWriteSuccess(event) => {
            assert!(!fail_setup);
            assert_eq!(result?.0, CheckpointWriteResult::Written);
            assert!(event.processing_stats.num_actions_from_log_tail > 0);
            assert!(event.processing_stats.num_actions_from_checkpoint > 0);
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        MetricEvent::CheckpointWriteFailure(event) => {
            assert!(fail_setup);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected input setup failure")
            );
            (&event.processing_stats, &event.output_stats, event.duration)
        }
        event => panic!("unexpected checkpoint event: {event:?}"),
    };
    assert!(engine.json.checkpoint_input.minimum_duration() > Duration::ZERO);
    assert!(processing.log_tail_input_duration >= engine.json.input.minimum_duration());
    assert!(
        processing.checkpoint_input_duration
            >= engine.json.checkpoint_input.minimum_duration()
                + engine.parquet.input.minimum_duration()
    );
    assert_accounted_within_duration(processing, output, duration);
    Ok(())
}

fn checkpoint_engine(table: &TestTable, batch_size: usize) -> Arc<dyn Engine> {
    Arc::new(
        DefaultEngineBuilder::new(table.store().clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .with_batch_size(NonZeroUsize::new(batch_size).unwrap())
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

fn assert_accounted_within_duration(
    processing: &CheckpointProcessingStats,
    output: &CheckpointOutputStats,
    duration: Duration,
) {
    let stages = processing.log_tail_input_duration
        + processing.checkpoint_input_duration
        + processing.reconciliation_duration
        + processing.stats_transform_duration
        + processing.output_preparation_duration
        + output.parquet_write_duration
        + output.verification_duration;
    assert!(
        stages <= duration,
        "checkpoint timers overlap: {processing:?}, {output:?}, total={duration:?}"
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
    storage: Arc<ControlledStorageHandler>,
}

impl ControlledCheckpointEngine {
    fn new(
        inner: Arc<dyn Engine>,
        checkpoint_delay: Duration,
        checkpoint_error: Option<InjectedWriteError>,
        hint_delay: Duration,
        fail_hint: bool,
    ) -> Self {
        let written_files = Arc::new(Mutex::new(Vec::new()));
        Self {
            json: Arc::new(ControlledJsonHandler {
                inner: inner.json_handler(),
                hint_delay,
                fail_hint,
                hint_duration: Mutex::new(None),
                input: ControlledInput::default(),
                checkpoint_input: ControlledInput::default(),
            }),
            parquet: Arc::new(ControlledParquetHandler {
                inner: inner.parquet_handler(),
                checkpoint_delay,
                checkpoint_error,
                fail_write_at: 1,
                write_before_error: false,
                write_calls: AtomicUsize::default(),
                written_files: written_files.clone(),
                input: ControlledInput::default(),
            }),
            storage: Arc::new(ControlledStorageHandler {
                inner: inner.storage_handler(),
                written_files,
                verification_delay: Duration::ZERO,
                verification_error: None,
                fail_verification_at: 1,
                verification_calls: AtomicUsize::default(),
            }),
            inner,
        }
    }

    fn engine(&self) -> DelegatingEngine {
        DelegatingEngine::new(self.inner.clone())
            .without_plan_executor()
            .with_json_handler(self.json.clone())
            .with_parquet_handler(self.parquet.clone())
            .with_storage_handler(self.storage.clone())
    }
}

struct ControlledJsonHandler {
    inner: Arc<dyn JsonHandler>,
    hint_delay: Duration,
    fail_hint: bool,
    hint_duration: Mutex<Option<Duration>>,
    input: ControlledInput,
    checkpoint_input: ControlledInput,
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
        let control = if files
            .first()
            .is_some_and(|file| file.location.path().contains(".checkpoint."))
        {
            &self.checkpoint_input
        } else {
            &self.input
        };
        control.read(|| {
            self.inner
                .read_json_files(files, physical_schema, predicate)
        })
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
    fail_write_at: usize,
    write_before_error: bool,
    write_calls: AtomicUsize,
    written_files: Arc<Mutex<Vec<(Url, FileSize)>>>,
    input: ControlledInput,
}

impl ParquetHandler for ControlledParquetHandler {
    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.input.setup()?;
        self.inner.read_parquet_footer(file)
    }

    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.input.read(|| {
            self.inner
                .read_parquet_files(files, physical_schema, predicate)
        })
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: FileDataReadResultIterator,
    ) -> KernelResult<FileSize> {
        let call = self.write_calls.fetch_add(1, Ordering::Relaxed) + 1;
        std::thread::sleep(self.checkpoint_delay);
        let error = self.checkpoint_error.filter(|_| call == self.fail_write_at);
        let size = if error.is_none() || self.write_before_error {
            Some(self.inner.write_parquet_file(location.clone(), data)?)
        } else {
            None
        };
        match error {
            Some(InjectedWriteError::Generic) => {
                Err(KernelError::generic("injected checkpoint write failure"))
            }
            Some(InjectedWriteError::AlreadyExists) => {
                Err(KernelError::FileAlreadyExists(location.to_string()))
            }
            None => {
                let size = size.unwrap();
                self.written_files.lock().unwrap().push((location, size));
                Ok(size)
            }
        }
    }
}

#[derive(Clone, Default)]
struct ControlledInput {
    delay: Duration,
    fail_setup: bool,
    fail_after: Option<usize>,
    waits: Arc<AtomicUsize>,
}

impl ControlledInput {
    fn pause(&self) {
        std::thread::sleep(self.delay);
        self.waits.fetch_add(1, Ordering::Relaxed);
    }

    fn minimum_duration(&self) -> Duration {
        self.delay * self.waits.load(Ordering::Relaxed) as u32
    }

    fn setup(&self) -> KernelResult<()> {
        self.pause();
        if self.fail_setup {
            return Err(KernelError::generic("injected input setup failure"));
        }
        Ok(())
    }

    fn read(
        &self,
        create: impl FnOnce() -> KernelResult<FileDataReadResultIterator>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.setup()?;
        let mut input = create()?;
        let control = self.clone();
        let mut num_pulls = 0;
        Ok(Box::new(std::iter::from_fn(move || {
            control.pause();
            let fail = control.fail_after == Some(num_pulls);
            num_pulls += 1;
            if fail {
                Some(Err(KernelError::generic("injected input failure")))
            } else {
                input.next()
            }
        })))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InjectedVerificationError {
    Head,
    SizeMismatch,
}

/// Controls verification of files written by this attempt; other storage requests pass through.
struct ControlledStorageHandler {
    inner: Arc<dyn StorageHandler>,
    written_files: Arc<Mutex<Vec<(Url, FileSize)>>>,
    verification_delay: Duration,
    verification_error: Option<InjectedVerificationError>,
    fail_verification_at: usize,
    verification_calls: AtomicUsize,
}

impl StorageHandler for ControlledStorageHandler {
    fn list_from(&self, path: &Url) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
        self.inner.list_from(path)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
        self.inner.read_files(files)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.inner.copy_atomic(src, dest)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        self.inner.put(path, data, overwrite)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.inner.delete(path)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        let written = self
            .written_files
            .lock()
            .unwrap()
            .iter()
            .any(|(url, _)| url == path);
        if !written {
            return self.inner.head(path);
        }
        let call = self.verification_calls.fetch_add(1, Ordering::Relaxed) + 1;
        std::thread::sleep(self.verification_delay);
        let error = self
            .verification_error
            .filter(|_| call == self.fail_verification_at);
        if error == Some(InjectedVerificationError::Head) {
            return Err(KernelError::generic("injected verification failure"));
        }
        let mut meta = self.inner.head(path)?;
        if error == Some(InjectedVerificationError::SizeMismatch) {
            meta.size += 1;
        }
        Ok(meta)
    }
}
