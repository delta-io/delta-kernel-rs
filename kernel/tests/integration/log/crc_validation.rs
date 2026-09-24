use std::num::NonZeroUsize;
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
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
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
#[case::txn_id("txnId")]
#[case::deleted_records("numDeletedRecordsOpt")]
#[case::deletion_vectors("numDeletionVectorsOpt")]
#[case::deleted_record_histogram("deletedRecordCountsHistogramOpt")]
#[case::file_list("allFiles")]
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
        "txnId" => crc[field] = json!("incorrect transaction id"),
        "numDeletedRecordsOpt" | "numDeletionVectorsOpt" => crc[field] = json!(1),
        "deletedRecordCountsHistogramOpt" => {
            crc[field] = json!({
                "deletedRecordCounts": [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
            })
        }
        "allFiles" => {
            let mut add = read_actions_from_commit(snapshot.table_root(), 1, "add")
                .unwrap()
                .remove(0);
            add["path"] = json!("incorrect.parquet");
            crc[field] = json!([add]);
        }
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
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), 0);
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["allFiles"] = json!([]);
    crc["numDeletedRecordsOpt"] = json!(0);
    crc["numDeletionVectorsOpt"] = json!(0);
    crc["deletedRecordCountsHistogramOpt"] = json!({"deletedRecordCounts": vec![0; 10]});
    std::fs::write(&crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot.checkpoint(engine.as_ref(), None)?;
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

#[rstest]
#[case::json(true, false)]
#[case::struct_only(false, true)]
#[case::omitted(false, false)]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_all_files_ignores_stats_across_checkpoint_formats(
    #[case] json_stats: bool,
    #[case] struct_stats: bool,
    #[values(CheckpointFormat::V1, CheckpointFormat::V2, CheckpointFormat::Sidecars)]
    format: CheckpointFormat,
    #[values("none", "name", "id")] mapping: &str,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table(&path, schema_ref! { nullable "id": INTEGER }, "crc-test")
        .with_table_properties([
            (
                "delta.checkpointPolicy",
                if format == CheckpointFormat::V1 {
                    "classic"
                } else {
                    "v2"
                },
            ),
            (
                "delta.checkpoint.writeStatsAsJson",
                if json_stats { "true" } else { "false" },
            ),
            (
                "delta.checkpoint.writeStatsAsStruct",
                if struct_stats { "true" } else { "false" },
            ),
            ("delta.columnMapping.mode", mapping),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut snapshot = snapshot;
    let mut adds = Vec::new();
    for value in [1, 2] {
        snapshot = insert_data(
            snapshot,
            &engine,
            vec![Arc::new(Int32Array::from(vec![value]))],
        )
        .await?
        .unwrap_post_commit_snapshot();
        adds.extend(
            read_actions_from_commit(snapshot.table_root(), snapshot.version(), "add").unwrap(),
        );
    }
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    adds.reverse();
    for add in &mut adds {
        add["dataChange"] = json!(false);
        let stats: Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
        add["stats"] = json!(serde_json::to_string_pretty(&stats).unwrap());
    }
    crc["allFiles"] = json!(adds);
    crc["numDeletedRecordsOpt"] = json!(0);
    crc["numDeletionVectorsOpt"] = json!(0);
    crc["deletedRecordCountsHistogramOpt"] =
        json!({"deletedRecordCounts": [2, 0, 0, 0, 0, 0, 0, 0, 0, 0]});
    crc["txnId"] =
        read_actions_from_commit(snapshot.table_root(), snapshot.version(), "commitInfo").unwrap()
            [0]["txnId"]
            .clone();
    std::fs::write(&crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot.checkpoint(engine.as_ref(), Some(&format.spec()))?;
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    let mut checkpoint = snapshot
        .create_checkpoint_writer(engine.as_ref())?
        .checkpoint_data(engine.as_ref())?;
    checkpoint.try_for_each(|batch| batch.map(|_| ()))?;
    crc["allFiles"][0]["stats"] = json!("{\"numRecords\":999}");
    std::fs::write(&crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    let mut checkpoint = snapshot
        .create_checkpoint_writer(engine.as_ref())?
        .checkpoint_data(engine.as_ref())?;
    checkpoint.try_for_each(|batch| batch.map(|_| ()))?;
    Ok(())
}

#[rstest]
#[case::changed_count(json!("{\"numRecords\":999}"))]
#[case::tight_bounds(json!("{\"tightBounds\":false}"))]
#[case::missing(json!(null))]
#[case::invalid_json(json!("not json"))]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_all_files_ignores_crc_statistics(
    #[case] stats: Value,
    #[values(false, true)] deletion_vectors: bool,
    #[values(false, true)] checkpoint_base: bool,
    #[values("none", "name", "id")] mapping: &str,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table(&path, schema_ref! { nullable "id": INTEGER }, "crc-test")
        .with_table_properties([
            ("delta.columnMapping.mode", mapping),
            (
                "delta.enableDeletionVectors",
                if deletion_vectors { "true" } else { "false" },
            ),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let snapshot = insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![1]))])
        .await?
        .unwrap_post_commit_snapshot();
    snapshot.write_checksum(engine.as_ref())?;
    if checkpoint_base {
        snapshot.checkpoint(engine.as_ref(), None)?;
    }
    let mut add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    add["stats"] = stats;
    let crc_path = checksum_path(dir.path(), 1);
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["allFiles"] = json!([add]);
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    let mut checkpoint = snapshot
        .create_checkpoint_writer(engine.as_ref())?
        .checkpoint_data(engine.as_ref())?;
    checkpoint.try_for_each(|batch| batch.map(|_| ()))?;
    Ok(())
}

#[rstest]
#[case::path("path", json!("wrong.parquet"))]
#[case::partition_values("partitionValues", json!({"wrong": "partition"}))]
#[case::modification_time("modificationTime", json!(-1))]
#[case::tags("tags", json!({"key": "value", "null": null}))]
#[case::base_row_id("baseRowId", json!(99))]
#[case::row_commit_version("defaultRowCommitVersion", json!(99))]
#[case::clustering_provider("clusteringProvider", json!("wrong"))]
#[case::deletion_vector("deletionVector", json!({
    "storageType": "i", "pathOrInlineDv": "encoded", "sizeInBytes": 1, "cardinality": 1
}))]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_all_files_rejects_field_mismatches(
    #[case] field: &str,
    #[case] value: Value,
    #[values(false, true)] checkpoint_base: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    if checkpoint_base {
        snapshot.checkpoint(engine.as_ref(), None)?;
    }
    let mut add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    add[field] = value;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["allFiles"] = json!([add]);
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_mismatch(snapshot.validate_crc(engine.as_ref()), "allFiles");
    let mut checkpoint = snapshot
        .create_checkpoint_writer(engine.as_ref())?
        .checkpoint_data(engine.as_ref())?;
    assert_mismatch(
        checkpoint.try_for_each(|batch| batch.map(|_| ())),
        "allFiles",
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_deletion_vectors_round_trip_after_replacement(
    #[values(0, 1)] cardinality: i64,
    #[values(false, true)] all_files: bool,
    #[values(CheckpointFormat::V1, CheckpointFormat::V2, CheckpointFormat::Sidecars)]
    format: CheckpointFormat,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table(&path, schema_ref! { nullable "id": INTEGER }, "crc-test")
        .with_table_properties([
            ("delta.enableDeletionVectors", "true"),
            (
                "delta.checkpointPolicy",
                if format == CheckpointFormat::V1 {
                    "classic"
                } else {
                    "v2"
                },
            ),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let snapshot = insert_data(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .await?
    .unwrap_post_commit_snapshot();
    snapshot.write_checksum(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref(), Some(&format.spec()))?;
    let mut add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    add["deletionVector"] = json!({
        "storageType": "i", "pathOrInlineDv": "encoded", "sizeInBytes": 1,
        "cardinality": cardinality,
    });
    add["tags"] = json!({"key": "value", "null": null});
    let actions = format!(
        "{}\n{}",
        json!({"remove": {"path": add["path"], "dataChange": true}}),
        json!({"add": add})
    );
    add_commit(snapshot.table_root(), &LocalFileSystem::new(), 2, actions)
        .await
        .unwrap();
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 1)).unwrap()).unwrap();
    crc["numDeletedRecordsOpt"] = json!(cardinality);
    crc["numDeletionVectorsOpt"] = json!(1);
    let mut bins = vec![0; 10];
    bins[usize::from(cardinality != 0)] = 1;
    crc["deletedRecordCountsHistogramOpt"] = json!({"deletedRecordCounts": bins});
    if all_files {
        crc["allFiles"] = json!([add]);
    }
    std::fs::write(
        checksum_path(dir.path(), 2),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot.checkpoint(engine.as_ref(), Some(&format.spec()))?;
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_txn_id_reads_later_batches_and_rejects_missing_id(
    #[values(false, true)] missing: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    let commit_info = if missing {
        json!({"operation": "WRITE"})
    } else {
        json!({"txnId": "target-txn"})
    };
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        3,
        format!(
            "{}\n{}",
            json!({"add": add}),
            json!({"commitInfo": commit_info})
        ),
    )
    .await
    .unwrap();
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 2)).unwrap()).unwrap();
    crc["txnId"] = json!("target-txn");
    std::fs::write(
        checksum_path(dir.path(), 3),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
        .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        )))
        .with_batch_size(NonZeroUsize::new(1).unwrap())
        .build();
    let snapshot = Snapshot::builder_for(&path).build(&engine)?;
    let result = snapshot.validate_crc(&engine);
    if missing {
        assert_mismatch(result, "txnId");
        assert_mismatch(snapshot.checkpoint(&engine, None), "txnId");
    } else {
        assert_eq!(result?, CrcValidationResult::Validated);
        snapshot.checkpoint(&engine, None)?;
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_scan_rejects_negative_live_sizes_but_ignores_superseded_adds(
    #[values(false, true)] superseded: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let mut add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    add["size"] = json!(-1);
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        3,
        json!({"add": add}).to_string(),
    )
    .await
    .unwrap();
    let version = if superseded {
        add["size"] = json!(0);
        add_commit(
            snapshot.table_root(),
            &LocalFileSystem::new(),
            4,
            json!({"add": add}).to_string(),
        )
        .await
        .unwrap();
        4
    } else {
        3
    };
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 2)).unwrap()).unwrap();
    crc["tableSizeBytes"] = json!(0);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(
        checksum_path(dir.path(), version),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let result = snapshot
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()));
    if superseded {
        result?;
    } else {
        assert_result_error_with_message(result, "negative Add size");
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_requires_target_commit_only_for_commit_fields(
    #[values(false, true)] txn_id: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref(), None)?;
    if txn_id {
        let crc_path = checksum_path(dir.path(), snapshot.version());
        let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
        crc["txnId"] =
            read_actions_from_commit(snapshot.table_root(), snapshot.version(), "commitInfo")
                .unwrap()[0]["txnId"]
                .clone();
        std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    }
    let commit = dir.path().join("_delta_log/00000000000000000002.json");
    std::fs::rename(&commit, dir.path().join("saved-commit.json")).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let result = snapshot.validate_crc(engine.as_ref());
    if txn_id {
        assert!(matches!(result, Err(Error::MissingVersion(2))));
    } else {
        assert_eq!(result?, CrcValidationResult::Validated);
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_all_files_ignores_superseded_incompatible_statistics(
    #[values(false, true)] checkpoint_base: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    let mut old_add = add.clone();
    old_add["stats"] = json!("{\"minValues\":{\"id\":\"incompatible\"}}");
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        3,
        json!({"add": old_add}).to_string(),
    )
    .await
    .unwrap();
    if checkpoint_base {
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .checkpoint(engine.as_ref(), None)?;
    }
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        4,
        json!({"add": add}).to_string(),
    )
    .await
    .unwrap();
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 2)).unwrap()).unwrap();
    crc["allFiles"] = json!([add]);
    std::fs::write(
        checksum_path(dir.path(), 4),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(
        snapshot.validate_crc(engine.as_ref())?,
        CrcValidationResult::Validated
    );
    snapshot.checkpoint(engine.as_ref(), None)?;
    Ok(())
}

#[rstest]
#[case::missing(json!(null))]
#[case::malformed(json!("{\"numRecords\":\"invalid\"}"))]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_all_files_ignores_replayed_statistics(
    #[case] stats: Value,
    #[values(false, true)] checkpoint_base: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, CheckpointFormat::V1, false).await?;
    snapshot.write_checksum(engine.as_ref())?;
    let add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    let mut actual = add.clone();
    actual["stats"] = stats;
    add_commit(
        snapshot.table_root(),
        &LocalFileSystem::new(),
        3,
        json!({"add": actual}).to_string(),
    )
    .await
    .unwrap();
    if checkpoint_base {
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .checkpoint(engine.as_ref(), None)?;
    }
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 2)).unwrap()).unwrap();
    crc["allFiles"] = json!([add]);
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
    let mut checkpoint = snapshot
        .create_checkpoint_writer(engine.as_ref())?
        .checkpoint_data(engine.as_ref())?;
    checkpoint.try_for_each(|batch| batch.map(|_| ()))?;
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
