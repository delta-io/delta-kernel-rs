use ::test_utils::table_builder::{FeatureSet, TestTableBuilder, partitioned_schema};
use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use super::*;
use crate::actions::{CheckpointMetadata, SET_TRANSACTION_FIELD, SetTransaction};
use crate::arrow::array::StringArray;
use crate::crc::{Crc, FileStats, FileStatsState};
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::expressions::Predicate;
use crate::last_checkpoint_hint::HintAction;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::plans::ir::nodes::{FileType, Operator};
use crate::plans::{Operation, Plan, PlanResult};
use crate::schema::{SchemaStructPatchBuilder, schema_ref};
use crate::unit_test_utils::plans::{execute_plan as execute, json_rows};
use crate::{DeltaErrorCondition, Engine, EngineData, Error, Snapshot, SnapshotRef, Version};

fn checkpoint_table(
    v2: bool,
    mixed: bool,
) -> KernelResult<(tempfile::TempDir, SnapshotRef, SyncEngine)> {
    let features = FeatureSet::new().domain_metadata();
    let table = TestTableBuilder::new()
        .with_features(if v2 {
            features.v2_checkpoint()
        } else {
            features
        })
        .build()
        .unwrap();
    let source = SyncEngine::new_with_store(table.store().clone());
    let commit = source
        .storage_handler()
        .read_files(vec![(
            Url::parse(table.table_root())?.join("_delta_log/00000000000000000000.json")?,
            None,
        )])?
        .next()
        .unwrap()?;
    let dir = tempfile::tempdir()?;
    let log = dir.path().join("_delta_log");
    std::fs::create_dir(&log)?;
    std::fs::write(log.join("00000000000000000000.json"), commit)?;
    if mixed {
        let actions = [
            json!({"add":{"path":"keep.parquet","partitionValues":{},"size":17,
                "modificationTime":19,"dataChange":true,
                "stats":json!({"numRecords":1,"minValues":{"long_col":7},
                    "maxValues":{"long_col":7},"nullCount":{"long_col":0}}).to_string()}}),
            json!({"remove":{"path":"gone.parquet","deletionTimestamp":i64::MAX,
                "dataChange":true}}),
            json!({"txn":{"appId":"app","version":1,"lastUpdated":23}}),
            json!({"domainMetadata":{"domain":"test","configuration":"current","removed":false}}),
        ];
        std::fs::write(
            log.join("00000000000000000001.json"),
            actions.map(|action| action.to_string()).join("\n"),
        )?;
    }
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(Url::from_directory_path(dir.path()).unwrap())
        .build(&engine)
        .unwrap();
    Ok((dir, snapshot, engine))
}

fn write_checkpoint(
    writer: &CheckpointWriter,
    engine: &dyn Engine,
    format: FileType,
) -> Result<(Plan, Vec<u8>), Box<dyn std::error::Error>> {
    let plan = writer.declarative_checkpoint_plan(engine, format)?;
    let result = engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan.clone()))?;
    Ok((plan, writer.last_checkpoint_bytes_from_result(result)?))
}

fn persisted_rows(
    file: &FileMeta,
    format: FileType,
) -> KernelResult<(Option<SchemaRef>, Vec<Value>)> {
    let path = file.location.to_file_path().unwrap();
    if format == FileType::Json {
        let rows = std::fs::read_to_string(path)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((None, rows));
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
    let schema = Arc::new(StructType::try_from_arrow(reader.schema().clone())?);
    let batches = reader.build()?.collect::<Result<Vec<_>, _>>()?;
    Ok((Some(schema), json_rows(&batches, false)?))
}

fn canonical_actions(mut rows: Vec<Value>) -> Vec<Value> {
    for row in &mut rows {
        row.as_object_mut()
            .unwrap()
            .retain(|_, value| !value.is_null());
        row.sort_all_objects();
    }
    rows.sort_by_cached_key(Value::to_string);
    rows
}

fn reconciled_rows(
    writer: &CheckpointWriter,
    engine: &dyn Engine,
) -> KernelResult<(Vec<Value>, i64)> {
    let legacy = writer.checkpoint_data(engine)?;
    let state = legacy.state();
    let batches = legacy
        .map(|batch| batch?.apply_selection_vector()?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    assert!(state.is_exhausted());
    let mut rows = json_rows(&batches, false)?;
    rows.retain(|row| row.get("checkpointMetadata").is_none() && row.get("sidecar").is_none());
    // Runtime clears dataChange and omits tombstone stats/tags in checkpoint output.
    for row in &mut rows {
        for name in ["add", "remove"] {
            if let Some(action) = row.get_mut(name).and_then(Value::as_object_mut) {
                action.insert("dataChange".into(), false.into());
                if name == "remove" {
                    action.remove("stats");
                    action.remove("tags");
                }
            }
        }
    }
    Ok((canonical_actions(rows), state.actions_count()))
}

#[rstest]
#[case::v1(false, FileType::Parquet)]
#[case::v2_json(true, FileType::Json)]
#[case::v2_parquet(true, FileType::Parquet)]
#[tokio::test]
async fn checkpoint_preserves_native_timestamp_partitions(
    #[case] v2: bool,
    #[case] format: FileType,
    #[values(false, true)] partial_native: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_schema(
            partitioned_schema()
                .project(&["value", "part_ts", "part_long"])
                .unwrap(),
        )
        .with_partition_columns(["part_ts", "part_long"])
        .with_features(if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::new()
        })
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let add = |path, raw| {
        json!({"add":{"path":path,"partitionValues":{"part_ts":raw,"part_long":"7"},
        "size":1,"modificationTime":0,"dataChange":true}})
    };
    let mut native = add("native.parquet", "2024-01-15 12:30:45");
    // A Los Angeles writer already resolved the offset-less map value to UTC in this field.
    native["add"]["partitionValues_parsed"] =
        json!({"part_ts":"2024-01-15T20:30:45Z","part_long":7});
    let rows = [
        json!({"protocol":snapshot.table_configuration().protocol()}),
        json!({"metaData":snapshot.table_configuration().metadata()}),
        native,
    ];
    let read_schema = if partial_native {
        Arc::new(
            SchemaStructPatchBuilder::new()
                .drop_at(["add", "partitionValues_parsed"], "part_long")
                .build(&writer.read_schema)?,
        )
    } else {
        writer.read_schema.clone()
    };
    let batch = parse_json_impl(
        &StringArray::from(rows.map(|row| row.to_string()).to_vec()),
        read_schema,
    )?;
    let source_checkpoint = writer.checkpoint_path()?;
    engine.parquet_handler().write_parquet_file(
        source_checkpoint.clone(),
        Box::new(std::iter::once(Ok(
            Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
        ))),
    )?;
    ::test_utils::add_commit(
        table.table_root(),
        table.store().as_ref(),
        1,
        add("tail.parquet", "2024-01-15 20:30:45").to_string(),
    )
    .await
    .unwrap();
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let (_, bytes) = write_checkpoint(&writer, &engine, format)?;
    engine.storage_handler().put(
        &writer
            .snapshot
            .log_segment()
            .log_root
            .join("_last_checkpoint")?,
        bytes.into(),
        true,
    )?;
    engine.storage_handler().delete(&source_checkpoint)?;
    engine.storage_handler().delete(
        &writer
            .snapshot
            .log_segment()
            .log_root
            .join("00000000000000000000.json")?,
    )?;
    let reloaded = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(reloaded.log_segment().checkpoint_version, Some(1));
    let scan = reloaded
        .scan_builder()
        .with_predicate(Arc::new(Predicate::and(
            col!("part_ts").eq(lit(Scalar::Timestamp(1_705_350_645_000_000))),
            col!("part_long").eq(lit(7i64)),
        )))
        .build()?;
    let mut batches = Vec::new();
    for batch in scan.scan_metadata(&engine)? {
        batches.push(
            batch?
                .scan_files
                .apply_selection_vector()?
                .try_into_record_batch()?,
        );
    }
    let rows = json_rows(&batches, false)?;
    let mut paths = rows
        .iter()
        .map(|row| row["path"].as_str().unwrap())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    assert_eq!(paths, ["native.parquet", "tail.parquet"]);
    let native = rows
        .iter()
        .find(|row| row["path"] == "native.parquet")
        .unwrap();
    assert_eq!(
        native["fileConstantValues"]["partitionValues"]["part_ts"],
        "2024-01-15 12:30:45"
    );
    Ok(())
}

#[rstest]
#[case::v1(false, FileType::Parquet)]
#[case::v2_json(true, FileType::Json)]
#[case::v2_parquet(true, FileType::Parquet)]
fn checkpoint_plan_persists_statistics_and_reloads_without_older_commits(
    #[case] v2: bool,
    #[case] format: FileType,
    #[values("empty", "mixed", "predecessor")] state: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, mut snapshot, engine) = checkpoint_table(v2, state != "empty")?;
    let log = snapshot.log_segment().log_root.clone();
    let hint_path = log.join("_last_checkpoint")?.to_file_path().unwrap();
    let storage = engine.storage_handler();
    let mut predecessor_files = vec![];
    let mut predecessor_hint = None;
    if state == "predecessor" {
        let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
        let (_, bytes) = write_checkpoint(&writer, &engine, format)?;
        std::fs::write(&hint_path, &bytes)?;
        predecessor_hint = Some(bytes);
        for file in storage.list_from(&log)? {
            let file = file?;
            if file.location.path().ends_with(".json")
                && !file.location.path().contains(".checkpoint.")
            {
                storage.delete(&file.location)?;
            } else if file.location.path().contains(".checkpoint.") {
                predecessor_files.push(file.location);
            }
        }
        std::fs::write(
            log.join("00000000000000000002.json")?
                .to_file_path()
                .unwrap(),
            json!({"txn":{"appId":"app","version":2,"lastUpdated":29}}).to_string(),
        )?;
        snapshot = Snapshot::builder_for(snapshot.table_root())
            .build(&engine)
            .unwrap();
        assert_eq!(snapshot.log_segment().checkpoint_version, Some(1));
    }
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let (expected_actions, legacy_count) = reconciled_rows(&writer, &engine)?;
    assert_eq!(expected_actions.len(), if state == "empty" { 2 } else { 6 });
    let (plan, bytes) = write_checkpoint(&writer, &engine, format)?;
    assert_eq!(std::fs::read(&hint_path).ok(), predecessor_hint);
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes)?;
    let root_url = match &hint.v2_checkpoint {
        Some(locator) => log.join(&locator.path)?,
        None => writer.checkpoint_path()?,
    };
    let root = storage.head(&root_url)?;
    let root_name = root_url.path_segments().unwrap().next_back().unwrap();
    let (root_schema, root_rows) = persisted_rows(&root, format)?;
    let root_rows = canonical_actions(root_rows);
    let mut sidecars: Vec<Sidecar> = root_rows
        .iter()
        .filter_map(|row| row.get("sidecar"))
        .map(|row| serde_json::from_value(row.clone()))
        .collect::<Result<_, _>>()?;
    sidecars.sort_by(|a, b| a.path.cmp(&b.path));
    let actual_sidecars = storage
        .list_from(&log.join("_sidecars/")?)?
        .collect::<KernelResult<Vec<_>>>()?
        .into_iter()
        .filter(|file| !predecessor_files.contains(&file.location))
        .collect::<Vec<_>>();
    let expected_sidecars = actual_sidecars
        .iter()
        .map(|file| {
            let name = file.location.path().rsplit('/').next().unwrap();
            Sidecar::new(name.into(), file.size as i64, file.last_modified, None)
        })
        .collect::<Vec<_>>();
    assert_eq!(sidecars, expected_sidecars);
    assert_eq!(sidecars.is_empty(), !v2 || state == "empty");
    let sidecar_bytes: u64 = actual_sidecars.iter().map(|file| file.size).sum();
    let mut content = vec![];
    let mut non_content = vec![];
    if v2 {
        assert!(
            root_rows
                .iter()
                .all(|row| row.get("add").is_none() && row.get("remove").is_none())
        );
        let metadata = root_rows
            .iter()
            .filter_map(|row| row.get("checkpointMetadata"))
            .collect::<Vec<_>>();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0]["version"], writer.version);
        let tags = &metadata[0]["tags"];
        let tagged_schema: StructType =
            serde_json::from_str(tags[SIDECAR_FILE_SCHEMA_TAG].as_str().unwrap())?;
        assert_eq!(
            tags,
            &json!({
                "sidecarNumActions": if state == "empty" { "0" } else { "2" },
                "numOfAddFiles": if state == "empty" { "0" } else { "1" },
                "sidecarSizeInBytes": sidecar_bytes.to_string(),
                SIDECAR_FILE_SCHEMA_TAG: serde_json::to_string(&tagged_schema)?,
            })
        );
        for file in &actual_sidecars {
            let (schema, rows) = persisted_rows(file, FileType::Parquet)?;
            assert_eq!(schema.as_deref(), Some(&tagged_schema));
            assert_eq!(
                file.size,
                std::fs::metadata(file.location.to_file_path().unwrap())?.len()
            );
            assert!(
                rows.iter()
                    .all(|row| row.get("add").is_some() || row.get("remove").is_some())
            );
            content.extend(rows);
        }
        non_content = root_rows
            .iter()
            .filter(|row| row.get("sidecar").is_none())
            .cloned()
            .collect();
        assert_eq!(non_content.len(), if state == "empty" { 3 } else { 5 });
    }
    let mut actual_actions = root_rows
        .iter()
        .filter(|row| row.get("sidecar").is_none() && row.get("checkpointMetadata").is_none())
        .cloned()
        .collect::<Vec<_>>();
    actual_actions.extend(content);
    assert_eq!(canonical_actions(actual_actions), expected_actions);
    let mut value: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(value["checksum"], json_checksum::json_checksum(&value));
    let mut expected_hint = json!({
        "version": writer.version, "size": legacy_count + sidecars.len() as i64,
        "parts": null, "sizeInBytes": root.size + sidecar_bytes,
        "numOfAddFiles": if state == "empty" { 0 } else { 1 },
        "checkpointSchema": root_schema, "tags": null, "v2Checkpoint": null,
    });
    if v2 {
        let actions = non_content
            .into_iter()
            .map(serde_json::from_value::<HintAction>)
            .collect::<Result<Vec<_>, _>>()?;
        expected_hint["v2Checkpoint"] = json!({
            "path":root_name,"sizeInBytes":root.size,"modificationTime":root.last_modified,
            "sidecarFiles":sidecars,"nonFileActions":actions,
        });
        for key in ["sidecarFiles", "nonFileActions"] {
            for hint in [&mut value, &mut expected_hint] {
                hint["v2Checkpoint"][key]
                    .as_array_mut()
                    .unwrap()
                    .sort_by_cached_key(Value::to_string);
            }
        }
    } else {
        expected_hint["checkpointFiles"] = json!([{
            "path":root_name,"length":root.size,"isDir":false,"modificationTime":root.last_modified,
        }]);
    }
    expected_hint["checksum"] = value["checksum"].clone();
    assert_eq!(value, expected_hint);
    assert!(hint.applies_to(&[ParsedLogPath::new(root.clone()).unwrap()]));
    let root_file = root_url.to_file_path().unwrap();
    let completed_root = std::fs::read(&root_file)?;
    assert!(
        engine
            .require_plan_executor()?
            .execute_op(Operation::QueryPlan(plan))
            .is_err()
    );
    assert_eq!(std::fs::read(&root_file)?, completed_root);
    assert_eq!(std::fs::read(&hint_path).ok(), predecessor_hint);
    // Keep the target commit as a publication watermark; all older history is dispensable.
    std::fs::write(hint_path, bytes)?;
    let target_commit = log.join(&format!("{:020}.json", snapshot.version()))?;
    for file in storage.list_from(&log)? {
        let file = file?;
        if (file.location.path().ends_with(".json")
            && !file.location.path().contains(".checkpoint.")
            && file.location != target_commit)
            || predecessor_files.contains(&file.location)
        {
            storage.delete(&file.location)?;
        }
    }
    let reloaded = Snapshot::builder_for(snapshot.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(reloaded.version(), snapshot.version());
    assert_eq!(
        reloaded.log_segment().checkpoint_version,
        Some(snapshot.version())
    );
    let config = snapshot.table_configuration();
    let reloaded_config = reloaded.table_configuration();
    assert_eq!(reloaded_config.metadata(), config.metadata());
    assert_eq!(reloaded_config.protocol(), config.protocol());
    let reloaded_writer = reloaded.create_checkpoint_writer(&engine)?;
    assert_eq!(
        reconciled_rows(&reloaded_writer, &engine)?.0,
        expected_actions
    );
    Ok(())
}

#[rstest]
#[case::missing(None, None, true)]
#[case::matching(Some(1), Some(1), true)]
#[case::too_few(Some(1), Some(0), false)]
#[case::too_many(Some(1), Some(2), false)]
#[case::stale(Some(0), Some(0), true)]
#[case::indeterminate(Some(1), None, true)]
fn checkpoint_validates_add_count_against_crc(
    #[case] crc_version: Option<Version>,
    #[case] expected_adds: Option<i64>,
    #[case] succeeds: bool,
    #[values((false, FileType::Parquet), (true, FileType::Json), (true, FileType::Parquet))]
    checkpoint: (bool, FileType),
) -> Result<(), Box<dyn std::error::Error>> {
    let (v2, format) = checkpoint;
    let (_dir, snapshot, engine) = checkpoint_table(v2, true)?;
    let crc = crc_version.map(|version| {
        Arc::new(Crc {
            version,
            metadata: snapshot.table_configuration().metadata().clone(),
            protocol: snapshot.table_configuration().protocol().clone(),
            file_stats_state: expected_adds.map_or(
                FileStatsState::Indeterminate {
                    unsupported_operation: None,
                },
                |num_files| {
                    FileStatsState::Complete(FileStats {
                        num_files,
                        table_size_bytes: 17 * num_files,
                        file_size_histogram: None,
                    })
                },
            ),
            ..Default::default()
        })
    });
    let snapshot = Arc::new(Snapshot::new_with_crc(
        snapshot.log_segment().clone(),
        snapshot.table_configuration().clone(),
        crc,
        true,
        true,
    )?);
    let hint = snapshot.log_segment().log_root.join("_last_checkpoint")?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let result = engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(
            writer.declarative_checkpoint_plan(&engine, format)?,
        ))?;
    let result = writer.last_checkpoint_bytes_from_result(result);
    if succeeds {
        let hint: LastCheckpointHint = serde_json::from_slice(&result?)?;
        assert_eq!(hint.num_of_add_files, Some(1));
    } else {
        let error = result.unwrap_err();
        assert!(matches!(&error, Error::Delta(error)
            if error.condition() == DeltaErrorCondition::DeltaTxnLogFailedIntegrity
                && error.sql_state() == Some("XXKDS")
                && error.parameters()[0].name() == "version"
                && error.parameters()[0].value() == "1"
                && error.parameters()[1].name() == "mismatchStringOpt"));
        assert!(error.to_string().contains(&format!(
            "Number of files - Expected: {} Computed: 1",
            expected_adds.unwrap(),
        )));
    }
    assert!(engine.storage_handler().head(&hint).is_err());
    Ok(())
}

#[rstest]
fn failed_checkpoint_publication_preserves_predecessor_hint(
    #[values(FileType::Json, FileType::Parquet)] format: FileType,
    #[values("sidecar", "root", "hint", "late", "missing", "duplicate")] failure: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(true, true)?;
    let log = snapshot.log_segment().log_root.clone();
    let hint_path = log.join("_last_checkpoint")?;
    let previous_writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let (_, previous) = write_checkpoint(&previous_writer, &engine, format)?;
    engine
        .storage_handler()
        .put(&hint_path, previous.clone().into(), true)?;
    std::fs::write(
        log.join("00000000000000000002.json")?
            .to_file_path()
            .unwrap(),
        json!({"txn":{"appId":"app","version":2}}).to_string(),
    )?;
    let snapshot = Snapshot::builder_for(snapshot.table_root())
        .build(&engine)
        .unwrap();
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let mut plan = writer.declarative_checkpoint_plan(&engine, format)?;
    // Locate destinations only to inject real storage failures; assertions concern stored output.
    let root_path = plan
        .nodes
        .iter()
        .find_map(|node| match &node.op {
            Operator::WriteJson(write) => Some(write.file_path.clone()),
            Operator::WriteParquet(write) => Some(write.file_path.clone()),
            _ => None,
        })
        .unwrap();
    if failure == "sidecar" {
        let blocker = log.join("_sidecars/blocked")?.to_file_path().unwrap();
        std::fs::write(&blocker, "blocked")?;
        for node in &mut plan.nodes {
            if let Operator::WriteParquetFiles(write) = &mut node.op {
                write.base_url = Url::from_directory_path(&blocker).unwrap();
            }
        }
    } else if failure == "root" {
        engine
            .storage_handler()
            .put(&root_path, "occupied".into(), false)?;
    }
    let result = engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan));
    if matches!(failure, "sidecar" | "root") {
        assert!(result.is_err());
        if failure == "sidecar" {
            assert!(!root_path.to_file_path().unwrap().exists());
        } else {
            assert_eq!(
                std::fs::read(root_path.to_file_path().unwrap())?,
                b"occupied"
            );
        }
    } else {
        let result = result?;
        let root = engine.storage_handler().head(&root_path)?;
        assert!(!persisted_rows(&root, format)?.1.is_empty());
        if failure == "hint" {
            let bytes = writer.last_checkpoint_bytes_from_result(result)?;
            assert!(
                engine
                    .storage_handler()
                    .put(&hint_path, bytes.into(), false)
                    .is_err()
            );
        } else {
            let mut batches = result
                .into_data()?
                .map(|batch| batch.try_into_record_batch())
                .collect::<KernelResult<Vec<_>>>()?;
            if failure == "missing" {
                batches.clear();
            } else if failure == "duplicate" {
                batches.extend_from_within(..);
            }
            let batches = batches
                .into_iter()
                .map(|batch| Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>))
                .chain(
                    (failure == "late")
                        .then(|| Err(KernelError::generic("late checkpoint result error"))),
                );
            let error = writer
                .last_checkpoint_bytes_from_result(PlanResult::Data(Box::new(batches)))
                .unwrap_err();
            assert!(
                error.to_string().contains(match failure {
                    "late" => "late checkpoint result error",
                    "missing" => "Missing checkpoint statistics",
                    "duplicate" => "duplicate",
                    _ => unreachable!(),
                }),
                "{error}"
            );
        }
    }
    assert_eq!(std::fs::read(hint_path.to_file_path().unwrap())?, previous);
    Ok(())
}

#[rstest]
#[case::valid(vec![vec![1, 0, 17]], true)]
#[case::missing(vec![], false)]
#[case::duplicate(vec![vec![1, 0, 17], vec![1, 0, 17]], false)]
#[case::negative_actions(vec![vec![-1, 0, 17]], false)]
#[case::negative_adds(vec![vec![1, -1, 17]], false)]
#[case::too_many_adds(vec![vec![1, 2, 17]], false)]
#[case::negative_bytes(vec![vec![1, 0, -1]], false)]
#[case::short_bytes(vec![vec![1, 0, 16]], false)]
fn checkpoint_statistics_decoder_validates_counts_and_root(
    #[case] rows: Vec<Vec<i64>>,
    #[case] valid: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(false, false)?;
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let aggregate = engine.evaluation_handler().create_many(
        STATISTICS_SCHEMA.clone(),
        rows.into_iter()
            .map(|row| row.into_iter().map(Scalar::Long).collect())
            .collect(),
    )?;
    let root = FileMeta::new(writer.checkpoint_path()?, 19, 17);
    let oversized_schema = Arc::new(StructType::try_new([StructField::nullable(
        "x".repeat(CHECKPOINT_SCHEMA_THRESHOLD),
        DataType::LONG,
    )])?);
    let bytes = writer.last_checkpoint_bytes(aggregate.as_ref(), &root, &oversized_schema, []);
    assert_eq!(bytes.is_ok(), valid);
    if let Ok(bytes) = bytes {
        let hint: LastCheckpointHint = serde_json::from_slice(&bytes)?;
        assert_eq!(hint.checkpoint_schema, None);
        let wrong_root = FileMeta::new(
            snapshot
                .log_segment()
                .log_root
                .join("00000000000000000002.checkpoint.parquet")?,
            19,
            17,
        );
        assert!(
            writer
                .last_checkpoint_bytes(aggregate.as_ref(), &wrong_root, &oversized_schema, [])
                .is_err()
        );
    }
    Ok(())
}

#[rstest]
fn prefetch_lists_are_complete_or_omitted(
    #[values(0, 30, 31)] non_content_count: usize,
    #[values(0, 30, 31)] sidecar_count: usize,
) -> KernelResult<()> {
    let engine = SyncEngine::new();
    let schema = Arc::new(schema! {
        (&SET_TRANSACTION_FIELD),
        (&SIDECAR_FIELD),
        (&CHECKPOINT_METADATA_FIELD),
    });
    let null_txn = Scalar::null(SetTransaction::to_schema());
    let null_sidecar = Scalar::null(Sidecar::to_schema());
    let null_metadata = Scalar::null(CheckpointMetadata::to_schema());
    let mut rows = Vec::new();
    for index in 0..non_content_count {
        let (txn, metadata) = if index == 0 {
            (
                null_txn.clone(),
                CheckpointMetadata {
                    version: 7,
                    tags: None,
                }
                .into(),
            )
        } else {
            (
                SetTransaction {
                    app_id: format!("app-{index}"),
                    version: index as i64,
                    last_updated: Some(17),
                }
                .into(),
                null_metadata.clone(),
            )
        };
        rows.push(vec![txn, null_sidecar.clone(), metadata]);
    }
    for index in 0..sidecar_count {
        rows.push(vec![
            null_txn.clone(),
            Sidecar::new(format!("{index}.parquet"), 128, 17, None).into(),
            null_metadata.clone(),
        ]);
    }
    let batches = execute(
        &engine,
        CheckpointWriter::last_checkpoint_prefetch(PlanBuilder::values(schema, rows)?)?.build()?,
    )?;
    let mut hint = LastCheckpointV2::default();
    for batch in &batches {
        hint.visit_rows_of(&ArrowEngineData::new(batch.clone()))?;
    }
    assert_eq!(
        hint.non_file_actions.as_ref().map(Vec::len),
        (non_content_count <= 30).then_some(non_content_count)
    );
    assert_eq!(
        hint.sidecar_files.as_ref().map(Vec::len),
        (sidecar_count <= 30).then_some(sidecar_count)
    );
    if let Some(actions) = hint.non_file_actions {
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, HintAction::CheckpointMetadata(_)))
                .count(),
            usize::from(non_content_count > 0)
        );
    }
    if let Some(sidecars) = hint.sidecar_files {
        let mut actual = sidecars
            .into_iter()
            .map(|sidecar| sidecar.path)
            .collect::<Vec<_>>();
        let mut expected = (0..sidecar_count)
            .map(|index| format!("{index}.parquet"))
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[rstest]
#[case::below(CHECKPOINT_SCHEMA_THRESHOLD - 1, true)]
#[case::at(CHECKPOINT_SCHEMA_THRESHOLD, true)]
#[case::above(CHECKPOINT_SCHEMA_THRESHOLD + 1, false)]
fn checkpoint_schema_respects_dbr_length_limit(
    #[case] length: usize,
    #[case] included: bool,
    #[values("", "雪😀")] prefix: &str,
) -> KernelResult<()> {
    let empty = schema_ref! { nullable "": STRING };
    let overhead = serde_json::to_string(&empty)?.encode_utf16().count();
    let name = format!(
        "{prefix}{}",
        "x".repeat(length - overhead - prefix.encode_utf16().count())
    );
    let schema = Arc::new(StructType::try_new([StructField::nullable(
        name,
        DataType::STRING,
    )])?);
    assert_eq!(checkpoint_schema_json(&schema)?.is_some(), included);
    Ok(())
}
