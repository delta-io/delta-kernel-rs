use ::test_utils::table_builder::{FeatureSet, TestTableBuilder, partitioned_schema};
use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use super::*;
use crate::actions::{
    ADD_FIELD, Add, CheckpointMetadata, DOMAIN_METADATA_FIELD, METADATA_FIELD, PROTOCOL_FIELD,
    REMOVE_FIELD, Remove, SET_TRANSACTION_FIELD, SetTransaction,
};
use crate::arrow::array::StringArray;
use crate::checkpoint::{CheckpointNaming, CheckpointSpec, V2CheckpointConfig};
use crate::crc::{Crc, FileStats, FileStatsState};
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::engine::{DelegatingEngine, PanicEngine};
use crate::expressions::Predicate;
use crate::last_checkpoint_hint::HintAction;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::plans::ir::nodes::{FileType, Operator};
use crate::plans::{IoOperation, Operation, Plan, PlanExecutor, PlanResult};
use crate::schema::SchemaStructPatchBuilder;
use crate::snapshot::CheckpointWriteResult;
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
) -> Result<(Plan, Vec<u8>), Box<dyn std::error::Error>> {
    let plan = writer.declarative_checkpoint_plan(engine)?;
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
    let mut reference = writer.clone();
    reference.schema_context.stats_config.write_stats_as_struct = writer
        .snapshot
        .table_properties()
        .checkpoint_write_stats_as_struct
        .unwrap_or(true);
    let legacy = reference.checkpoint_data(engine)?;
    let state = legacy.state();
    let batches = legacy
        .map(|batch| batch?.apply_selection_vector()?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    assert!(state.is_exhausted());
    let mut rows = json_rows(&batches, false)?;
    rows.retain(|row| row.get("checkpointMetadata").is_none() && row.get("sidecar").is_none());
    // Checkpoint output clears dataChange and omits tombstone stats/tags.
    for row in &mut rows {
        for name in ["add", "remove"] {
            if let Some(action) = row.get_mut(name).and_then(Value::as_object_mut) {
                action.insert("dataChange".into(), false.into());
                if name == "remove" {
                    action.remove("stats");
                    action.remove("tags");
                } else if !writer.schema_context.is_v2 {
                    action.remove("stats_parsed");
                }
            }
        }
    }
    Ok((canonical_actions(rows), state.actions_count()))
}

#[rstest]
#[case::v1(false, CheckpointNaming::Classic, false)]
#[case::v2_inline_classic(true, CheckpointNaming::Classic, false)]
#[case::v2_inline_parquet(true, CheckpointNaming::Uuid, false)]
#[case::v2_sidecar_parquet(true, CheckpointNaming::Uuid, true)]
#[tokio::test]
async fn checkpoint_preserves_native_timestamp_partitions(
    #[case] v2: bool,
    #[case] naming: CheckpointNaming,
    #[case] with_sidecars: bool,
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
    let writer = snapshot
        .create_checkpoint_writer(&engine)?
        .with_checkpoint_naming(naming)
        .with_v2_checkpoint_config(if with_sidecars {
            V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: None,
            }
        } else {
            V2CheckpointConfig::NoSidecar
        });
    let (_, bytes) = write_checkpoint(&writer, &engine)?;
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
    let mut files = Vec::new();
    for metadata in scan.scan_metadata(&engine)? {
        files = metadata?.visit_scan_files(files, |files, file| files.push(file))?;
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(
        files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["native.parquet", "tail.parquet"]
    );
    assert_eq!(files[0].partition_values["part_ts"], "2024-01-15 12:30:45");
    Ok(())
}

#[rstest]
#[case::v2_inline_classic(true, CheckpointNaming::Classic, false, false)]
#[case::v2_inline_uuid_parquet(true, CheckpointNaming::Uuid, false, false)]
#[case::v2_sidecar_classic(true, CheckpointNaming::Classic, true, false)]
#[case::v2_sidecar_uuid_parquet(true, CheckpointNaming::Uuid, true, false)]
#[case::snapshot_v1(false, CheckpointNaming::Classic, false, true)]
#[case::snapshot_v2(true, CheckpointNaming::Classic, true, true)]
fn checkpoint_plan_persists_statistics_and_reloads_without_older_commits(
    #[case] v2: bool,
    #[case] naming: CheckpointNaming,
    #[case] with_sidecars: bool,
    #[case] public_api: bool,
    #[values("empty", "mixed", "predecessor")] state: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, mut snapshot, engine) = checkpoint_table(v2, state != "empty")?;
    let v2_config = if with_sidecars {
        V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: None,
        }
    } else {
        V2CheckpointConfig::NoSidecar
    };
    let log = snapshot.log_segment().log_root.clone();
    let hint_path = log.join("_last_checkpoint")?.to_file_path().unwrap();
    let storage = engine.storage_handler();
    let mut predecessor_files = vec![];
    let mut predecessor_hint = None;
    if state == "predecessor" {
        let writer = snapshot
            .clone()
            .create_checkpoint_writer(&engine)?
            .with_checkpoint_naming(naming)
            .with_v2_checkpoint_config(v2_config.clone());
        let (_, bytes) = write_checkpoint(&writer, &engine)?;
        std::fs::write(&hint_path, &bytes)?;
        predecessor_hint = Some(bytes);
        for file in storage.list_from(&log, None)? {
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
    let writer = snapshot
        .clone()
        .create_checkpoint_writer(&engine)?
        .with_checkpoint_naming(naming)
        .with_v2_checkpoint_config(v2_config.clone());
    let (expected_actions, legacy_count) = reconciled_rows(&writer, &engine)?;
    assert_eq!(expected_actions.len(), if state == "empty" { 2 } else { 6 });
    let (plan, bytes) = if public_api {
        let (outcome, checkpointed) = snapshot.checkpoint(&engine, None)?;
        assert_eq!(outcome, CheckpointWriteResult::Written);
        assert!(!Arc::ptr_eq(&snapshot, &checkpointed));
        assert_eq!(checkpointed.version(), snapshot.version());
        assert_eq!(
            checkpointed.log_segment().checkpoint_version,
            Some(snapshot.version())
        );
        assert_eq!(
            reconciled_rows(
                &checkpointed.clone().create_checkpoint_writer(&engine)?,
                &engine
            )?
            .0,
            expected_actions
        );
        let (outcome, unchanged) = checkpointed.checkpoint(&engine, None)?;
        assert_eq!(outcome, CheckpointWriteResult::AlreadyExists);
        assert!(Arc::ptr_eq(&checkpointed, &unchanged));
        (None, std::fs::read(&hint_path)?)
    } else {
        let (plan, bytes) = write_checkpoint(&writer, &engine)?;
        assert_eq!(std::fs::read(&hint_path).ok(), predecessor_hint);
        (Some(plan), bytes)
    };
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes)?;
    let root_url = match &hint.v2_checkpoint {
        Some(locator) => log.join(&locator.path)?,
        None => writer.checkpoint_path()?,
    };
    let root = storage.head(&root_url)?;
    let root_name = root_url.path_segments().unwrap().next_back().unwrap();
    let (root_schema, root_rows) = persisted_rows(&root, FileType::Parquet)?;
    let root_rows = canonical_actions(root_rows);
    let mut sidecars: Vec<Sidecar> = root_rows
        .iter()
        .filter_map(|row| row.get("sidecar"))
        .map(|row| serde_json::from_value(row.clone()))
        .collect::<Result<_, _>>()?;
    sidecars.sort_by(|a, b| a.path.cmp(&b.path));
    let actual_sidecars = storage
        .list_from(&log.join("_sidecars/")?, None)?
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
    assert_eq!(sidecars.is_empty(), !with_sidecars || state == "empty");
    let sidecar_bytes: u64 = actual_sidecars.iter().map(|file| file.size).sum();
    let mut content = vec![];
    let mut non_content = vec![];
    if v2 {
        if with_sidecars {
            assert!(
                root_rows
                    .iter()
                    .all(|row| row.get("add").is_none() && row.get("remove").is_none())
            );
        }
        let metadata = root_rows
            .iter()
            .filter_map(|row| row.get("checkpointMetadata"))
            .collect::<Vec<_>>();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0]["version"], writer.version);
        if with_sidecars {
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
        } else {
            assert!(metadata[0]["tags"].is_null());
            assert_eq!(
                root_rows
                    .iter()
                    .filter(|row| row.get("add").is_some() || row.get("remove").is_some())
                    .count(),
                if state == "empty" { 0 } else { 2 }
            );
        }
        non_content = root_rows
            .iter()
            .filter(|row| {
                row.get("sidecar").is_none()
                    && row.get("add").is_none()
                    && row.get("remove").is_none()
            })
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
        "checkpointSchema": root_schema, "tags": null, "v2Checkpoint": null, "checksum": value["checksum"],
    });
    if naming == CheckpointNaming::Uuid {
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
    assert_eq!(value, expected_hint);
    assert!(hint.applies_to(&[ParsedLogPath::new(root.clone()).unwrap()]));
    let root_file = root_url.to_file_path().unwrap();
    let completed_root = std::fs::read(&root_file)?;
    if let Some(plan) = plan {
        assert!(
            engine
                .require_plan_executor()?
                .execute_op(Operation::QueryPlan(plan))
                .is_err()
        );
    } else if naming == CheckpointNaming::Classic {
        // The original snapshot has not discovered the newly persisted checkpoint.
        let (outcome, unchanged) = snapshot.checkpoint(&engine, None)?;
        assert_eq!(outcome, CheckpointWriteResult::AlreadyExists);
        assert!(Arc::ptr_eq(&snapshot, &unchanged));
    }
    assert_eq!(std::fs::read(&root_file)?, completed_root);
    assert_eq!(
        std::fs::read(&hint_path).ok(),
        if public_api {
            Some(bytes.clone())
        } else {
            predecessor_hint
        }
    );
    // Keep the target commit as a publication watermark; all older history is dispensable.
    if !public_api {
        std::fs::write(hint_path, bytes)?;
    }
    let target_commit = log.join(&format!("{:020}.json", snapshot.version()))?;
    for file in storage.list_from(&log, None)? {
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
#[case::v1_uuid_parquet(
    false,
    CheckpointNaming::Uuid,
    V2CheckpointConfig::NoSidecar,
    "UUID-named checkpoints require V2"
)]
#[case::v1_sidecars(false, CheckpointNaming::Classic, V2CheckpointConfig::WithSidecar { file_actions_per_sidecar_hint: None }, "Sidecar checkpoint plans require the v2Checkpoint feature")]
#[case::zero_hint(true, CheckpointNaming::Classic, V2CheckpointConfig::WithSidecar { file_actions_per_sidecar_hint: Some(0) }, "file_actions_per_sidecar_hint must be greater than 0")]
fn checkpoint_rejects_invalid_layout_and_naming(
    #[case] v2: bool,
    #[case] naming: CheckpointNaming,
    #[case] config: V2CheckpointConfig,
    #[case] expected_error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(v2, false)?;
    let storage = engine.storage_handler();
    let log = snapshot.log_segment().log_root.clone();
    let before = storage
        .list_from(&log, None)?
        .collect::<KernelResult<Vec<_>>>()?;
    let writer = snapshot
        .create_checkpoint_writer(&engine)?
        .with_checkpoint_naming(naming)
        .with_v2_checkpoint_config(config);
    ::test_utils::assert_result_error_with_message(
        writer.declarative_checkpoint_plan(&PanicEngine),
        expected_error,
    );
    assert_eq!(
        storage
            .list_from(&log, None)?
            .collect::<KernelResult<Vec<_>>>()?,
        before
    );
    Ok(())
}

#[rstest]
#[case::short("parquet", 3, "Checkpoint byte count does not match root file")]
#[case::long("parquet", 5, "Checkpoint byte count does not match root file")]
#[case::json_root("json", 4, "Checkpoint root does not match this writer")]
fn inline_uuid_hint_rejects_invalid_root(
    #[case] extension: &str,
    #[case] size_in_bytes: i64,
    #[case] expected_error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(true, false)?;
    let writer = snapshot
        .create_checkpoint_writer(&engine)?
        .with_checkpoint_naming(CheckpointNaming::Uuid);
    let root = writer.snapshot.log_segment().log_root.join(&format!(
        "00000000000000000000.checkpoint.550e8400-e29b-41d4-a716-446655440000.{extension}"
    ))?;
    let schema = writer.declarative_checkpoint_plan(&engine)?.schema;
    let rows = [
        json!({"statistics":{"num_actions":3,"num_of_add_files":1,"size_in_bytes":size_in_bytes}}),
        json!({"root":{"location":root,"size":4,"last_modified":17,"checkpoint_schema":serde_json::to_string(&schema! { nullable "value": LONG })?}}),
    ];
    let batch = parse_json_impl(
        &StringArray::from(rows.map(|row| row.to_string()).to_vec()),
        schema,
    )?;
    let result = PlanResult::Data(Box::new(std::iter::once(Ok(
        Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>,
    ))));
    ::test_utils::assert_result_error_with_message(
        writer.last_checkpoint_bytes_from_result(result),
        expected_error,
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
    #[values(
        (true, CheckpointNaming::Uuid, false),
        (false, CheckpointNaming::Classic, true),
        (true, CheckpointNaming::Classic, true)
    )]
    checkpoint: (bool, CheckpointNaming, bool),
) -> Result<(), Box<dyn std::error::Error>> {
    let (v2, naming, public_api) = checkpoint;
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
    let result = if public_api {
        snapshot
            .checkpoint(&engine, None)
            .and_then(|(outcome, checkpointed)| {
                assert_eq!(outcome, CheckpointWriteResult::Written);
                assert_eq!(checkpointed.crc_at_version(), snapshot.crc_at_version());
                Ok(std::fs::read(hint.to_file_path().unwrap()).map_err(KernelError::from)?)
            })
    } else {
        let writer = snapshot
            .create_checkpoint_writer(&engine)?
            .with_checkpoint_naming(naming)
            .with_v2_checkpoint_config(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: None,
            });
        let result = engine
            .require_plan_executor()?
            .execute_op(Operation::QueryPlan(
                writer.declarative_checkpoint_plan(&engine)?,
            ))?;
        writer.last_checkpoint_bytes_from_result(result)
    };
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
    assert_eq!(
        engine.storage_handler().head(&hint).is_ok(),
        public_api && succeeds
    );
    Ok(())
}

#[rstest]
#[case::inline(V2CheckpointConfig::NoSidecar)]
#[case::sidecars(V2CheckpointConfig::WithSidecar {
    file_actions_per_sidecar_hint: None,
})]
#[case::small_hint(V2CheckpointConfig::WithSidecar {
    file_actions_per_sidecar_hint: Some(1),
})]
fn snapshot_checkpoint_honors_selected_layout(
    #[case] config: V2CheckpointConfig,
    #[values(false, true)] declarative: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(true, true)?;
    let hint_path = snapshot.log_segment().log_root.join("_last_checkpoint")?;
    let engine = DelegatingEngine::new(Arc::new(engine));
    let engine = if declarative {
        engine
    } else {
        engine.without_plan_executor()
    };
    let inline = matches!(&config, V2CheckpointConfig::NoSidecar);
    let result = snapshot.checkpoint(&engine, Some(&CheckpointSpec::V2(config)));
    assert_eq!(result?.0, CheckpointWriteResult::Written);
    let hint: LastCheckpointHint =
        serde_json::from_slice(&std::fs::read(hint_path.to_file_path().unwrap())?)?;
    assert!(hint.v2_checkpoint.is_none());
    let root = engine.storage_handler().head(
        &snapshot
            .create_checkpoint_writer(&engine)?
            .checkpoint_path()?,
    )?;
    let (_, rows) = persisted_rows(&root, FileType::Parquet)?;
    assert_eq!(
        rows.iter().any(|row| row["sidecar"]["path"].is_string()),
        !inline,
    );
    Ok(())
}

struct FailingCheckpointExecutor {
    inner: Arc<dyn PlanExecutor>,
    failure: &'static str,
}

impl PlanExecutor for FailingCheckpointExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        match op {
            Operation::QueryPlan(plan) => {
                let result = self.inner.execute_op(Operation::QueryPlan(plan))?;
                let error = match self.failure {
                    "late" => Some(KernelError::generic("late checkpoint result error")),
                    "data_exists" => Some(KernelError::Backtraced {
                        source: Box::new(KernelError::FileAlreadyExists("checkpoint".into())),
                        backtrace: Box::new(std::backtrace::Backtrace::force_capture()),
                    }),
                    _ => None,
                };
                Ok(PlanResult::Data(Box::new(
                    result.into_data()?.chain(error.map(Err)),
                )))
            }
            Operation::IoOperation(IoOperation::WriteBytes { .. }) => match self.failure {
                "hint_exists" => Err(KernelError::FileAlreadyExists("hint publication".into())),
                "unit" => Ok(PlanResult::Bytes(Box::new(std::iter::empty()))),
                _ => Err(KernelError::generic("hint publication failed")),
            },
            other => self.inner.execute_op(other),
        }
    }
}

#[rstest]
#[case::late("late", false)]
#[case::newer_hint_still_validates("late", true)]
#[case::newer_hint_skips_publication("hint_io", true)]
#[case::data_exists("data_exists", false)]
#[case::hint_io("hint_io", false)]
#[case::hint_exists("hint_exists", false)]
#[case::unit("unit", false)]
fn snapshot_checkpoint_preserves_hint_on_failure_or_newer_hint(
    #[case] failure: &'static str,
    #[case] newer_hint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(false, true)?;
    snapshot.checkpoint(&engine, None)?;
    let hint = snapshot
        .log_segment()
        .log_root
        .join("_last_checkpoint")?
        .to_file_path()
        .unwrap();
    let previous = std::fs::read(&hint)?;
    if !newer_hint {
        std::fs::write(
            snapshot
                .log_segment()
                .log_root
                .join("00000000000000000002.json")?
                .to_file_path()
                .unwrap(),
            json!({"txn":{"appId":"app","version":2}}).to_string(),
        )?;
    }
    let target = Snapshot::builder_for(snapshot.table_root())
        .at_version(if newer_hint { 0 } else { 2 })
        .build(&engine)?;
    let engine = DelegatingEngine::new(Arc::new(engine)).with_plan_executor(Arc::new(
        FailingCheckpointExecutor {
            inner: SyncEngine::new().require_plan_executor()?,
            failure,
        },
    ));
    let result = target.checkpoint(&engine, None);
    if failure == "data_exists" {
        let (outcome, unchanged) = result?;
        assert_eq!(outcome, CheckpointWriteResult::AlreadyExists);
        assert!(Arc::ptr_eq(&target, &unchanged));
    } else if newer_hint && failure == "hint_io" {
        let (outcome, checkpointed) = result?;
        assert_eq!(outcome, CheckpointWriteResult::Written);
        assert_eq!(
            checkpointed.log_segment().checkpoint_version,
            Some(target.version())
        );
    } else {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(match failure {
                "late" => "late checkpoint result error",
                "hint_exists" | "hint_io" => "hint publication",
                "unit" => "Unit",
                _ => unreachable!(),
            }),
            "{error}"
        );
    }
    assert_eq!(std::fs::read(hint)?, previous);
    Ok(())
}

#[rstest]
fn failed_checkpoint_publication_preserves_predecessor_hint(
    #[values("sidecar", "root", "hint", "late", "missing", "duplicate")] failure: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(true, true)?;
    let log = snapshot.log_segment().log_root.clone();
    let hint_path = log.join("_last_checkpoint")?;
    let previous_writer = snapshot
        .clone()
        .create_checkpoint_writer(&engine)?
        .with_checkpoint_naming(CheckpointNaming::Uuid)
        .with_v2_checkpoint_config(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: None,
        });
    let (_, previous) = write_checkpoint(&previous_writer, &engine)?;
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
    let writer = snapshot
        .clone()
        .create_checkpoint_writer(&engine)?
        .with_checkpoint_naming(CheckpointNaming::Uuid)
        .with_v2_checkpoint_config(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: None,
        });
    let mut plan = writer.declarative_checkpoint_plan(&engine)?;
    // Locate destinations only to inject real storage failures; assertions concern stored output.
    let root_path = plan
        .nodes
        .iter()
        .find_map(|node| match &node.op {
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
        assert!(!persisted_rows(&root, FileType::Parquet)?.1.is_empty());
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
#[case::manifest_empty(false, 0)]
#[case::manifest_at_limit(false, 30)]
#[case::manifest_over_limit(false, 31)]
#[case::inline_with_file_actions(true, 0)]
fn prefetch_lists_are_complete_or_omitted(
    #[values(0, 30, 31)] non_content_count: usize,
    #[case] inline: bool,
    #[case] sidecar_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, snapshot, engine) = checkpoint_table(true, false)?;
    let writer = snapshot
        .create_checkpoint_writer(&engine)?
        .with_v2_checkpoint_config(if inline {
            V2CheckpointConfig::NoSidecar
        } else {
            V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: None,
            }
        });
    let schema = Arc::new(schema! {
        (&SET_TRANSACTION_FIELD),
        (&SIDECAR_FIELD),
        (&CHECKPOINT_METADATA_FIELD),
        (&ADD_FIELD),
        (&REMOVE_FIELD),
        (&PROTOCOL_FIELD),
        (&METADATA_FIELD),
        (&DOMAIN_METADATA_FIELD),
    });
    let null_txn = Scalar::null(SetTransaction::to_schema());
    let null_sidecar = Scalar::null(Sidecar::to_schema());
    let null_metadata = Scalar::null(CheckpointMetadata::to_schema());
    let null_add = Scalar::null(Add::to_schema());
    let null_remove = Scalar::null(Remove::to_schema());
    let null_protocol = Scalar::null(PROTOCOL_FIELD.data_type().clone());
    let null_table_metadata = Scalar::null(METADATA_FIELD.data_type().clone());
    let null_domain_metadata = Scalar::null(DOMAIN_METADATA_FIELD.data_type().clone());
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
        rows.push(vec![
            txn,
            null_sidecar.clone(),
            metadata,
            null_add.clone(),
            null_remove.clone(),
            null_protocol.clone(),
            null_table_metadata.clone(),
            null_domain_metadata.clone(),
        ]);
    }
    for index in 0..sidecar_count {
        rows.push(vec![
            null_txn.clone(),
            Sidecar::new(format!("{index}.parquet"), 128, 17, None).into(),
            null_metadata.clone(),
            null_add.clone(),
            null_remove.clone(),
            null_protocol.clone(),
            null_table_metadata.clone(),
            null_domain_metadata.clone(),
        ]);
    }
    if inline {
        for index in 0..31 {
            rows.push(vec![
                null_txn.clone(),
                null_sidecar.clone(),
                null_metadata.clone(),
                Add {
                    path: format!("{index}.parquet"),
                    ..Default::default()
                }
                .into(),
                null_remove.clone(),
                null_protocol.clone(),
                null_table_metadata.clone(),
                null_domain_metadata.clone(),
            ]);
        }
        rows.push(vec![
            null_txn,
            null_sidecar,
            null_metadata,
            null_add,
            crate::expressions::StructData::try_new(
                Arc::new(Remove::to_schema()),
                Remove::to_schema()
                    .fields()
                    .map(|field| match field.name().as_str() {
                        "path" => "removed.parquet".into(),
                        "dataChange" => false.into(),
                        _ => Scalar::null(field.data_type().clone()),
                    })
                    .collect(),
            )?
            .into(),
            null_protocol,
            null_table_metadata,
            null_domain_metadata,
        ]);
    }
    let actions = PlanBuilder::values(schema, rows)?;
    let actions = if inline {
        actions.project_patch(|patch| patch.drop("sidecar"))?
    } else {
        actions
    };
    let batches = execute(&engine, writer.last_checkpoint_prefetch(actions)?.build()?)?;
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
