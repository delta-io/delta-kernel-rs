use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::checkpoint::{CheckpointSpec, LastCheckpointHintStats, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::Predicate;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::{CrcValidationResult, IncrementalReplay};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{CancellationTokenRef, DeltaResult, Engine, Error, Snapshot, SnapshotRef};
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    add_commit, assert_result_error_with_message, insert_data, read_actions_from_commit,
    test_table_setup_mt, TestCancellationToken,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointFormat {
    V1,
    V2,
    Sidecars,
}

impl CheckpointFormat {
    fn spec(self) -> CheckpointSpec {
        match self {
            Self::V1 => CheckpointSpec::V1,
            Self::V2 => CheckpointSpec::V2(V2CheckpointConfig::NoSidecar),
            Self::Sidecars => CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: Some(1),
            }),
        }
    }
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_matches_before_and_after_checkpoint(
    #[values(CheckpointFormat::V1, CheckpointFormat::V2, CheckpointFormat::Sidecars)]
    format: CheckpointFormat,
    #[values(false, true)] ict: bool,
) -> DeltaResult<()> {
    let (_dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, format, ict).await?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    let (_, snapshot) = snapshot.write_checksum(engine.as_ref())?;
    snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    snapshot.checkpoint(engine.as_ref(), Some(&format.spec()))?;
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    Ok(())
}

#[rstest]
#[case::file_count("numFiles")]
#[case::table_size("tableSizeBytes")]
#[case::histogram("fileSizeHistogram")]
#[case::metadata("metadata")]
#[case::protocol("protocol")]
#[case::domains("domainMetadata")]
#[case::transactions("setTransactions")]
#[case::timestamp("inCommitTimestampOpt")]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_rejects_corrupted_fields(
    #[case] field: &str,
    #[values(CheckpointFormat::V1, CheckpointFormat::V2, CheckpointFormat::Sidecars)]
    format: CheckpointFormat,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, format, true).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    match field {
        "numFiles" | "tableSizeBytes" => {
            crc[field] = json!(crc[field].as_i64().unwrap() + 1);
            crc.as_object_mut().unwrap().remove("fileSizeHistogram");
        }
        "fileSizeHistogram" => {
            let size = crc["tableSizeBytes"].as_i64().unwrap();
            crc[field] = json!({
                "sortedBinBoundaries": [0, size + 1],
                "fileCounts": [0, crc["numFiles"]],
                "totalBytes": [0, size],
            });
        }
        "metadata" => crc[field]["name"] = json!("incorrect name"),
        "protocol" => crc[field]["writerFeatures"]
            .as_array_mut()
            .unwrap()
            .push(json!("appendOnly")),
        "domainMetadata" => crc[field][0]["configuration"] = json!("incorrect configuration"),
        "setTransactions" => crc[field][0]["version"] = json!(99),
        "inCommitTimestampOpt" => crc[field] = json!(crc[field].as_i64().unwrap() + 1),
        _ => unreachable!(),
    }
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_mismatch(snapshot.validate_crc(engine.as_ref()), field);

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_schema(schema_ref! {})
        .build()?;
    let result = scan
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()));
    if ["numFiles", "tableSizeBytes", "fileSizeHistogram"].contains(&field) {
        assert_mismatch(result, field);
    } else {
        result?;
    }
    assert_mismatch(
        snapshot.checkpoint(engine.as_ref(), Some(&format.spec())),
        field,
    );
    assert!(!dir.path().join("_delta_log/_last_checkpoint").exists());
    for entry in std::fs::read_dir(dir.path().join("_delta_log")).unwrap() {
        assert!(!entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".checkpoint."));
    }
    Ok(())
}

#[rstest]
#[case::predicate_true(Predicate::TRUE)]
#[case::predicate_false(Predicate::FALSE)]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_skips_any_supplied_scan_predicate(
    #[case] predicate: Predicate,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["numFiles"] = json!(10);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;
    scan.scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_does_not_compare_incomplete_scans(
    #[values(false, true)] cancel: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["numFiles"] = json!(10);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let token = Arc::new(TestCancellationToken::default());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token.clone() as CancellationTokenRef)
        .build()?;
    let mut iter = scan.scan_metadata(engine.as_ref())?;
    iter.next().expect("expected a batch before cancellation")?;
    if cancel {
        token.cancel();
        assert!(matches!(iter.next(), Some(Err(Error::Cancelled))));
        assert!(iter.next().is_none());
    }
    drop(iter);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_reconciles_repeated_adds_and_removes_without_sizes(
    #[values(false, true)] checkpoint_base: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_snapshot(&path, engine.as_ref(), CheckpointFormat::V1, false)?;
    let snapshot = insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![1]))])
        .await?
        .unwrap_post_commit_snapshot();
    snapshot.write_checksum(engine.as_ref())?;
    let crc_bytes = std::fs::read(checksum_path(dir.path(), 1)).unwrap();
    if checkpoint_base {
        snapshot.checkpoint(engine.as_ref(), None)?;
    }
    let add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        2,
        json!({"add": add}).to_string(),
    )
    .await
    .unwrap();
    std::fs::write(checksum_path(dir.path(), 2), &crc_bytes).unwrap();
    let repeated = Snapshot::builder_for(&path)
        .at_version(2)
        .build(engine.as_ref())?;
    assert_eq!(
        repeated.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );

    let mut replacement = add.clone();
    replacement["path"] = json!("replacement.parquet");
    replacement["size"] = json!(333);
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        3,
        format!(
            "{}\n{}",
            json!({"remove": {"path": add["path"], "dataChange": true}}),
            json!({"add": replacement}),
        ),
    )
    .await
    .unwrap();
    let mut crc: Value = serde_json::from_slice(&crc_bytes).unwrap();
    crc["tableSizeBytes"] = json!(333);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(
        checksum_path(dir.path(), 3),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    let mut files = Vec::new();
    for batch in snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
    {
        files = batch?.visit_scan_files(files, |files, file| files.push((file.path, file.size)))?;
    }
    assert_eq!(files, vec![("replacement.parquet".to_string(), 333)]);
    snapshot.checkpoint(engine.as_ref(), None)?;
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_skips_missing_and_stale_checksums(
    #[values(false, true)] stale: bool,
) -> DeltaResult<()> {
    let (_dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_snapshot(&path, engine.as_ref(), CheckpointFormat::V1, false)?;
    if stale {
        snapshot.write_checksum(engine.as_ref())?;
    }
    snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = Snapshot::builder_for(&path)
        .with_incremental_crc_replay(IncrementalReplay::Disabled)
        .build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Skipped
    );
    snapshot.checkpoint(engine.as_ref(), None)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_handles_empty_tables_and_rejects_malformed_crc() -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_snapshot(&path, engine.as_ref(), CheckpointFormat::V1, false)?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    std::fs::write(checksum_path(dir.path(), 0), "malformed").unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_result_error_with_message(snapshot.validate_crc(engine.as_ref()), "expected value");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_custom_checkpoint_stops_on_mismatch() -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["numFiles"] = json!(10);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let writer = snapshot.create_checkpoint_writer(engine.as_ref())?;
    let mut iter = writer.checkpoint_data(engine.as_ref())?;
    let state = iter.state();
    assert_mismatch(
        iter.by_ref().try_for_each(|batch| batch.map(|_| ())),
        "numFiles",
    );
    assert!(!state.is_exhausted());
    drop(iter);
    let state = Arc::into_inner(state).unwrap();
    assert_result_error_with_message(
        LastCheckpointHintStats::from_reconciliation_state(state, 0, 0),
        "the reconciliation iterator must be fully consumed",
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_applies_transaction_retention(
    #[values(false, true)] expires: bool,
) -> DeltaResult<()> {
    let (_dir, path, engine) = test_table_setup_mt()?;
    let mut builder = create_table(&path, schema_ref! { nullable "id": INTEGER }, "test");
    if expires {
        builder = builder.with_table_properties([(
            "delta.setTransactionRetentionDuration",
            "interval 0 seconds",
        )]);
    }
    let snapshot = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let snapshot = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let (_, snapshot) = snapshot.write_checksum(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot.checkpoint(engine.as_ref(), None)?;
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    assert_eq!(
        snapshot.get_app_id_version("app", engine.as_ref())?,
        (!expires).then_some(1)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_allows_omitted_optional_fields_and_reordered_features() -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V2, true).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    for field in ["domainMetadata", "setTransactions", "fileSizeHistogram"] {
        crc.as_object_mut().unwrap().remove(field);
    }
    crc["protocol"]["writerFeatures"]
        .as_array_mut()
        .unwrap()
        .reverse();
    let size = crc["tableSizeBytes"].clone();
    crc["fileSizeHistogram"] = json!({
        "sortedBinBoundaries": [0, size.as_i64().unwrap() + 1],
        "fileCounts": [1, 0],
        "totalBytes": [size, 0],
    });
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    snapshot.checkpoint(engine.as_ref(), None)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_does_not_require_checkpoint_write_support() -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_snapshot(&path, engine.as_ref(), CheckpointFormat::V1, false)?;
    snapshot.write_checksum(engine.as_ref())?;
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 0)).unwrap()).unwrap();
    crc["protocol"]["writerFeatures"]
        .as_array_mut()
        .unwrap()
        .push(json!("unknownWriterFeature"));
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        1,
        json!({"protocol": crc["protocol"]}).to_string(),
    )
    .await
    .unwrap();
    std::fs::write(
        checksum_path(dir.path(), 1),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    assert_result_error_with_message(
        snapshot.create_checkpoint_writer(engine.as_ref()),
        "unknownWriterFeature",
    );
    Ok(())
}

fn create_snapshot(
    path: &str,
    engine: &dyn Engine,
    format: CheckpointFormat,
    ict: bool,
) -> DeltaResult<SnapshotRef> {
    let mut builder = create_table(
        path,
        schema_ref! { nullable "id": INTEGER },
        "crc-validation-test",
    )
    .with_table_properties([("delta.feature.domainMetadata", "supported")]);
    if format != CheckpointFormat::V1 {
        builder = builder.with_table_properties([("delta.checkpointPolicy", "v2")]);
    }
    if ict {
        builder = builder.with_table_properties([("delta.enableInCommitTimestamps", "true")]);
    }
    Ok(builder
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata("test.domain".to_string(), "configuration".to_string())
        .commit(engine)?
        .unwrap_post_commit_snapshot())
}

async fn populated_snapshot<E: TaskExecutor>(
    path: &str,
    engine: &Arc<DefaultEngine<E>>,
    format: CheckpointFormat,
    ict: bool,
) -> DeltaResult<SnapshotRef> {
    let snapshot = create_snapshot(path, engine.as_ref(), format, ict)?;
    let snapshot = insert_data(
        snapshot,
        engine,
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .await?
    .unwrap_post_commit_snapshot();
    Ok(snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_transaction_id("test-app".to_string(), 1)
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot())
}

fn checksum_path(table: &Path, version: u64) -> PathBuf {
    table.join(format!("_delta_log/{version:020}.crc"))
}

fn assert_mismatch<T: std::fmt::Debug>(result: DeltaResult<T>, expected_field: &str) {
    let error = result.unwrap_err();
    match error {
        Error::ChecksumMismatch { field, .. } => assert_eq!(field, expected_field),
        other => panic!("expected CRC mismatch for {expected_field}, got {other}"),
    }
}
