use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::checkpoint::{CheckpointSpec, LastCheckpointHintStats, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::metrics::{MetricEvent, WithMetricsReporterLayer};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{CheckpointWriteResult, IncrementalReplay};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{Engine, Result, Snapshot};
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::{
    add_commit, ensure_metrics_compatible_global_subscriber, insert_data,
    install_thread_local_metrics_reporter, test_table_setup_mt, CapturingReporter,
};
use tracing_subscriber::prelude::*;

#[rstest]
#[case::v1(CheckpointSpec::V1, false)]
#[case::v2(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar), false)]
#[case::sidecars(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
    file_actions_per_sidecar_hint: Some(1),
}), false)]
#[case::custom_writer(CheckpointSpec::V1, true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_crc_mismatch_reports_without_blocking_write(
    #[case] spec: CheckpointSpec,
    #[case] custom_writer: bool,
    #[values("match", "numFiles", "tableSizeBytes", "both", "fileSizeHistogram")] field: &str,
    #[values(false, true)] post_commit: bool,
) -> Result<()> {
    let (directory, table_path, engine) = test_table_setup_mt()?;
    let reporter = Arc::new(CapturingReporter::default());
    let log_path = directory.path().join("validation.log");
    ensure_metrics_compatible_global_subscriber();
    let guard = tracing_subscriber::registry()
        .with_metrics_reporter_layer(reporter.clone())
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_writer(std::fs::File::create(&log_path)?)
                .with_filter(tracing_subscriber::filter::LevelFilter::ERROR),
        )
        .set_default();
    let snapshot = create_table(&table_path, schema_ref! { nullable "id": INTEGER }, "test")
        .with_table_properties([(
            "delta.checkpointPolicy",
            match &spec {
                CheckpointSpec::V1 => "classic",
                CheckpointSpec::V2(_) => "v2",
            },
        )])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let snapshot = insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![1]))])
        .await?
        .unwrap_post_commit_snapshot();
    let (_, snapshot) = snapshot.checkpoint(engine.as_ref(), Some(&spec))?;
    let snapshot = insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![2]))])
        .await?
        .unwrap_post_commit_snapshot();
    let mut expected_files = snapshot.get_file_stats_if_present().unwrap().num_files();
    let mut expected_bytes = snapshot
        .get_file_stats_if_present()
        .unwrap()
        .table_size_bytes();
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = directory
        .path()
        .join(format!("_delta_log/{:020}.crc", snapshot.version()));
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path)?)?;
    if field == "numFiles" || field == "both" {
        crc["numFiles"] = json!(expected_files + 1);
    }
    if field == "tableSizeBytes" || field == "both" {
        crc["tableSizeBytes"] = json!(expected_bytes + 1);
    }
    let mismatch = matches!(field, "numFiles" | "tableSizeBytes" | "both");
    if mismatch {
        crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    } else if field == "fileSizeHistogram" {
        crc["fileSizeHistogram"] = json!({
            "sortedBinBoundaries": [0, expected_bytes + 1],
            "fileCounts": [0, expected_files],
            "totalBytes": [0, expected_bytes],
        });
    }
    std::fs::write(&crc_path, serde_json::to_vec(&crc)?)?;
    let mut snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    if post_commit {
        snapshot = insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![3]))])
            .await?
            .unwrap_post_commit_snapshot();
        expected_files += 1;
        let stats = snapshot.get_file_stats_if_present().unwrap();
        expected_bytes =
            stats.table_size_bytes() - i64::from(field == "tableSizeBytes" || field == "both");
        assert!(!directory
            .path()
            .join(format!("_delta_log/{:020}.crc", snapshot.version()))
            .exists());
    }
    let cached_stats = snapshot.get_file_stats_if_present();
    let (result, checkpointed) = if custom_writer {
        let writer = snapshot.clone().create_checkpoint_writer(engine.as_ref())?;
        let data = writer.checkpoint_data(engine.as_ref())?;
        let state = data.state();
        let bytes = engine.parquet_handler().write_parquet_file(
            writer.checkpoint_path()?,
            Box::new(data.map(|batch| batch?.apply_selection_vector())),
        )?;
        assert!(!reporter
            .events()
            .iter()
            .any(|event| matches!(event, MetricEvent::CrcValidationFailure)));
        let stats = LastCheckpointHintStats::from_reconciliation_state(
            Arc::into_inner(state).unwrap(),
            bytes,
            0,
        )?;
        writer.finalize(engine.as_ref(), &stats)?;
        (CheckpointWriteResult::Written, snapshot.clone())
    } else {
        snapshot.checkpoint(engine.as_ref(), Some(&spec))?
    };
    assert_eq!(result, CheckpointWriteResult::Written);
    assert_eq!(checkpointed.get_file_stats_if_present(), cached_stats);
    drop(guard);

    let failures = reporter
        .events()
        .into_iter()
        .filter(|event| matches!(event, MetricEvent::CrcValidationFailure))
        .count();
    assert_eq!(failures, usize::from(mismatch));
    let logs = std::fs::read_to_string(log_path)?;
    assert_eq!(logs.contains("Checkpoint CRC mismatch"), mismatch);
    let hint: Value = serde_json::from_slice(&std::fs::read(
        directory.path().join("_delta_log/_last_checkpoint"),
    )?)?;
    assert_eq!(hint["version"], json!(snapshot.version()));

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let mut files = Vec::new();
    for batch in reloaded
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
    {
        files = batch?.visit_scan_files(files, |files, file| files.push(file.size))?;
    }
    assert_eq!(files.len() as i64, expected_files);
    assert_eq!(files.into_iter().sum::<i64>(), expected_bytes);
    Ok(())
}

#[rstest]
#[case::missing(false, IncrementalReplay::Disabled)]
#[case::stale(true, IncrementalReplay::Disabled)]
#[case::indeterminate(true, IncrementalReplay::Unlimited)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_crc_validation_skips_unavailable_totals(
    #[case] write_crc: bool,
    #[case] replay: IncrementalReplay,
) -> Result<()> {
    let (_directory, table_path, engine) = test_table_setup_mt()?;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = create_table(&table_path, schema_ref! { nullable "id": INTEGER }, "test")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    if write_crc {
        snapshot.write_checksum(engine.as_ref())?;
    }
    add_commit(
        &table_path,
        &LocalFileSystem::new(),
        1,
        format!(
            "{}\n{}",
            json!({"commitInfo": {"operation": "ANALYZE STATS"}}),
            json!({"add": {
                "path": "file.parquet", "size": 17, "partitionValues": {},
                "modificationTime": 0, "dataChange": true,
            }}),
        ),
    )
    .await
    .unwrap();
    let snapshot = Snapshot::builder_for(&table_path)
        .with_incremental_crc_replay(replay)
        .build(engine.as_ref())?;
    assert!(snapshot.get_file_stats_if_present().is_none());
    let (result, _) = snapshot.checkpoint(engine.as_ref(), None)?;
    assert_eq!(result, CheckpointWriteResult::Written);
    assert!(!reporter
        .events()
        .iter()
        .any(|event| matches!(event, MetricEvent::CrcValidationFailure)));
    Ok(())
}
