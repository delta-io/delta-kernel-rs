use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::Predicate;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::IncrementalReplay;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Error, Snapshot, SnapshotRef};
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    add_commit, assert_result_error_with_message, insert_data, read_actions_from_commit,
    test_table_setup_mt,
};

#[rstest]
#[case::v1(CheckpointSpec::V1, false)]
#[case::v2(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar), true)]
#[case::sidecars(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
    file_actions_per_sidecar_hint: Some(1),
}), true)]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_compares_only_scalar_file_totals(
    #[case] spec: CheckpointSpec,
    #[case] v2: bool,
    #[values("match", "numFiles", "tableSizeBytes", "fileSizeHistogram", "metadata")] field: &str,
    #[values(false, true)] checkpoint_base: bool,
    #[values("none", "name", "id")] mapping: &str,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, v2, mapping).await?;
    snapshot.validate_crc(engine.as_ref())?;
    snapshot.write_checksum(engine.as_ref())?;
    if checkpoint_base {
        snapshot.checkpoint(engine.as_ref(), Some(&spec))?;
    }
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    let mismatch = match field {
        "numFiles" | "tableSizeBytes" => {
            crc[field] = json!(crc[field].as_i64().unwrap() + 1);
            crc.as_object_mut().unwrap().remove("fileSizeHistogram");
            Some(field)
        }
        "fileSizeHistogram" => {
            let size = crc["tableSizeBytes"].as_i64().unwrap();
            crc[field] = json!({
                "sortedBinBoundaries": [0, size + 1],
                "fileCounts": [0, 1],
                "totalBytes": [0, size],
            });
            None
        }
        "metadata" => {
            crc[field]["name"] = json!("not the log's name");
            None
        }
        "match" => None,
        _ => unreachable!(),
    };
    std::fs::write(&crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_crc_result(snapshot.validate_crc(engine.as_ref()), mismatch);

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_schema(schema_ref! {})
        .build()?;
    let mut batches = scan.scan_metadata(engine.as_ref())?;
    batches.next().unwrap()?;
    assert_crc_result(batches.try_for_each(|batch| batch.map(|_| ())), mismatch);

    let mut checkpoint = snapshot
        .clone()
        .create_checkpoint_writer(engine.as_ref())?
        .checkpoint_data(engine.as_ref())?;
    checkpoint.next().unwrap()?;
    assert_crc_result(checkpoint.try_for_each(|batch| batch.map(|_| ())), mismatch);
    if !checkpoint_base {
        assert_crc_result(snapshot.checkpoint(engine.as_ref(), Some(&spec)), mismatch);
        assert_eq!(
            dir.path().join("_delta_log/_last_checkpoint").exists(),
            mismatch.is_none()
        );
        if mismatch.is_none() {
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .validate_crc(engine.as_ref())?;
        }
    }
    Ok(())
}

#[rstest]
#[case::always_true(Predicate::TRUE)]
#[case::always_false(Predicate::FALSE)]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_skips_scans_with_supplied_predicates(
    #[case] predicate: Predicate,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, false, "none").await?;
    snapshot.write_checksum(engine.as_ref())?;
    let crc_path = checksum_path(dir.path(), snapshot.version());
    let mut crc: Value = serde_json::from_slice(&std::fs::read(&crc_path).unwrap()).unwrap();
    crc["numFiles"] = json!(99);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(crc_path, serde_json::to_vec(&crc).unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    Ok(())
}

#[rstest]
#[case::missing(false, IncrementalReplay::Disabled)]
#[case::stale(true, IncrementalReplay::Disabled)]
#[case::indeterminate(true, IncrementalReplay::Unlimited)]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_skips_unavailable_totals(
    #[case] write_crc: bool,
    #[case] replay: IncrementalReplay,
) -> DeltaResult<()> {
    let (_dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, false, "none").await?;
    if write_crc {
        snapshot.write_checksum(engine.as_ref())?;
    }
    snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("ANALYZE STATS".to_string())
        .commit(engine.as_ref())?
        .unwrap_committed();
    let snapshot = Snapshot::builder_for(&path)
        .with_incremental_crc_replay(replay)
        .build(engine.as_ref())?;
    assert!(snapshot.get_file_stats_if_present().is_none());
    if replay == IncrementalReplay::Unlimited {
        assert!(snapshot.crc_at_version().is_some());
    }
    snapshot.validate_crc(engine.as_ref())?;
    snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()))?;
    snapshot.checkpoint(engine.as_ref(), None)?;
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_deduplicates_adds_and_removes_without_sizes(
    #[values(false, true)] checkpoint_base: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, false, "none").await?;
    snapshot.write_checksum(engine.as_ref())?;
    if checkpoint_base {
        snapshot.checkpoint(engine.as_ref(), None)?;
    }
    let add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    let crc_bytes = std::fs::read(checksum_path(dir.path(), 1)).unwrap();
    add_commit(
        &path,
        &LocalFileSystem::new(),
        2,
        json!({"add": add}).to_string(),
    )
    .await
    .unwrap();
    std::fs::write(checksum_path(dir.path(), 2), &crc_bytes).unwrap();
    Snapshot::builder_for(&path)
        .build(engine.as_ref())?
        .validate_crc(engine.as_ref())?;
    let mut replacement = add.clone();
    replacement["path"] = json!("replacement.parquet");
    replacement["size"] = json!(333);
    add_commit(
        &path,
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
    snapshot.validate_crc(engine.as_ref())?;
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
    Snapshot::builder_for(&path)
        .build(engine.as_ref())?
        .validate_crc(engine.as_ref())?;
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn crc_validation_rejects_negative_live_sizes_but_ignores_superseded_adds(
    #[values(false, true)] superseded: bool,
) -> DeltaResult<()> {
    let (dir, path, engine) = test_table_setup_mt()?;
    let snapshot = populated_snapshot(&path, &engine, false, "none").await?;
    snapshot.write_checksum(engine.as_ref())?;
    let mut add = read_actions_from_commit(snapshot.table_root(), 1, "add")
        .unwrap()
        .remove(0);
    add["size"] = json!(-1);
    add_commit(
        &path,
        &LocalFileSystem::new(),
        2,
        json!({"add": add}).to_string(),
    )
    .await
    .unwrap();
    let version = if superseded {
        add["size"] = json!(0);
        add_commit(
            &path,
            &LocalFileSystem::new(),
            3,
            json!({"add": add}).to_string(),
        )
        .await
        .unwrap();
        3
    } else {
        2
    };
    let mut crc: Value =
        serde_json::from_slice(&std::fs::read(checksum_path(dir.path(), 1)).unwrap()).unwrap();
    crc["tableSizeBytes"] = json!(0);
    crc.as_object_mut().unwrap().remove("fileSizeHistogram");
    std::fs::write(
        checksum_path(dir.path(), version),
        serde_json::to_vec(&crc).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let validation = snapshot.validate_crc(engine.as_ref());
    let scan = snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .try_for_each(|batch| batch.map(|_| ()));
    let checkpoint = snapshot.checkpoint(engine.as_ref(), None);
    if superseded {
        validation?;
        scan?;
        checkpoint?;
    } else {
        assert_result_error_with_message(validation, "size");
        assert_result_error_with_message(scan, "size");
        assert_result_error_with_message(checkpoint, "size");
    }
    Ok(())
}

async fn populated_snapshot<E: TaskExecutor>(
    path: &str,
    engine: &Arc<DefaultEngine<E>>,
    v2: bool,
    mapping: &str,
) -> DeltaResult<SnapshotRef> {
    let snapshot = create_table(path, schema_ref! { nullable "id": INTEGER }, "crc-test")
        .with_table_properties([
            ("delta.checkpointPolicy", if v2 { "v2" } else { "classic" }),
            ("delta.columnMapping.mode", mapping),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    snapshot.validate_crc(engine.as_ref())?;
    Ok(insert_data(
        snapshot,
        engine,
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .await?
    .unwrap_post_commit_snapshot())
}

fn checksum_path(table: &Path, version: u64) -> PathBuf {
    table.join(format!("_delta_log/{version:020}.crc"))
}

fn assert_crc_result<T: std::fmt::Debug>(result: DeltaResult<T>, mismatch: Option<&str>) {
    match (result, mismatch) {
        (Ok(_), None) => (),
        (Err(Error::ChecksumMismatch { field, .. }), Some(expected)) => assert_eq!(field, expected),
        (result, expected) => panic!("Expected mismatch {expected:?}, got {result:?}"),
    }
}
