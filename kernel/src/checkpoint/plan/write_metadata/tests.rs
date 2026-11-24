use std::sync::Arc;

use ::test_utils::table_builder::{FeatureSet, LogState, TableConfig, TestTable, TestTableBuilder};
use bytes::Bytes;
use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use super::CHECKPOINT_SCHEMA_THRESHOLD;
use crate::arrow::array::StringArray;
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::CheckpointWriter;
use crate::committer::FileSystemCommitter;
use crate::crc::{Crc, FileStats, FileStatsState};
use crate::engine::PanicEngine;
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::path::ParsedLogPath;
use crate::plans::ir::nodes::{FILE_META_SCHEMA, Operator};
use crate::plans::{IoOperation, Operation, PlanResult};
use crate::schema::{DataType, StructField, StructType, schema, schema_ref};
use crate::transaction::UpdateTableOperation;
use crate::unit_test_utils::plans::json_rows;
use crate::{
    DeltaErrorCondition, Engine, EngineData, Error, FileMeta, KernelError, KernelResult, Snapshot,
    SnapshotRef, Version,
};

fn checkpoint_table(
    v2: bool,
    state: &str,
    oversized_schema: bool,
) -> Result<(TestTable, SnapshotRef, SyncEngine), Box<dyn std::error::Error>> {
    let log_state = if state == "empty" {
        LogState::with_latest_version(0)
    } else {
        LogState::with_latest_version(2)
            .maybe_with_checkpoint_at((state == "predecessor").then_some(1))
    };
    let features = FeatureSet::new().domain_metadata();
    let builder = TestTableBuilder::new()
        .with_features(if v2 {
            features.v2_checkpoint()
        } else {
            features
        })
        .with_log_state(log_state)
        .with_data(1, 1);
    let builder = if oversized_schema {
        let name = "x".repeat(CHECKPOINT_SCHEMA_THRESHOLD);
        let schema = ::test_utils::table_builder::partitioned_schema();
        let field = schema.field("part_long").unwrap().with_name(name.clone());
        let schema = schema.add([field])?.project(&["value", name.as_str()])?;
        builder.with_schema(schema).with_partition_columns([name])
    } else {
        builder
    };
    let table = builder.build()?;
    let engine = SyncEngine::new_with_store(table.store().clone());
    let mut snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    if state != "empty" {
        let first = Snapshot::builder_for(table.table_root())
            .at_version(1)
            .build(&engine)?;
        let scan = first.scan_builder().build()?;
        let mut transaction = snapshot
            .update_table(UpdateTableOperation::Delete, "checkpoint-plan-test", true)
            .with_transaction_id("app", 1)
            .with_domain_metadata("test", r#"{"version":1}"#)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        for metadata in scan.scan_metadata(&engine)? {
            transaction.remove_files(metadata?.scan_files);
        }
        snapshot = transaction.commit(&engine)?.unwrap_post_commit_snapshot();
    }
    Ok((table, snapshot, engine))
}

fn file_bytes(engine: &dyn Engine, path: &Url) -> KernelResult<Bytes> {
    engine
        .storage_handler()
        .read_files(vec![(path.clone(), None)])?
        .next()
        .expect("expected one file read")
}

fn data_result(batches: Vec<RecordBatch>) -> PlanResult {
    PlanResult::Data(Box::new(batches.into_iter().map(|batch| {
        Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
    })))
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

fn reconciled_rows(writer: &CheckpointWriter, engine: &dyn Engine) -> KernelResult<Vec<Value>> {
    let mut reference = writer.clone();
    reference.schema_context.stats_config.write_stats_as_struct = writer
        .snapshot
        .table_properties()
        .checkpoint_write_stats_as_struct
        .unwrap_or(true);
    let batches = reference
        .checkpoint_data(engine)?
        .map(|batch| batch?.apply_selection_vector()?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    let mut rows = json_rows(&batches, false)?;
    // The physical checkpoint clears dataChange and omits tombstone stats and tags.
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
    Ok(canonical_actions(rows))
}

#[rstest]
#[case::v1_empty(false, "empty", false)]
#[case::v1_mixed(false, "mixed", false)]
#[case::v1_predecessor(false, "predecessor", false)]
#[case::v1_nullable_json_and_native_dv_stats(false, "native_stats", false)]
#[case::v1_variant_native_and_json_stats(false, "variant_stats", false)]
#[case::v2_empty(true, "empty", false)]
#[case::v2_mixed(true, "mixed", false)]
#[case::v2_predecessor(true, "predecessor", false)]
#[case::v2_nullable_json_and_native_dv_stats(true, "native_stats", false)]
#[case::v2_variant_native_and_json_stats(true, "variant_stats", false)]
#[case::oversized_schema(false, "empty", true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_plan_persists_statistics_and_reloads_without_older_commits(
    #[case] v2: bool,
    #[case] state: &str,
    #[case] oversized_schema: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, snapshot, engine) = if matches!(state, "native_stats" | "variant_stats") {
        crate::checkpoint::plan::tests::checkpoint_representation_table(
            v2,
            true,
            false,
            state == "variant_stats",
        )
        .await?
    } else {
        checkpoint_table(v2, state, oversized_schema)?
    };
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let log = snapshot.log_segment().log_root.clone();
    let hint_path = log.join("_last_checkpoint")?;
    let storage = engine.storage_handler();
    let before = storage
        .list_from(&log, None)?
        .collect::<KernelResult<Vec<_>>>()?;
    let previous_hint = before
        .iter()
        .any(|file| file.location == hint_path)
        .then(|| file_bytes(&engine, &hint_path))
        .transpose()?;
    let mut expected = reconciled_rows(&writer, &engine)?;
    assert_eq!(
        expected.len(),
        (match state {
            "empty" => 2,
            "native_stats" | "variant_stats" => 5,
            _ => 6,
        }) + usize::from(v2),
    );
    let expected_adds = expected
        .iter()
        .filter(|row| row.get("add").is_some())
        .count();
    assert_eq!(
        expected_adds,
        match state {
            "empty" => 0,
            "native_stats" | "variant_stats" => 3,
            _ => 1,
        }
    );

    let plan = writer.declarative_checkpoint_plan(&engine)?;
    assert_eq!(
        plan.schema,
        schema_ref! {
            nullable "statistics": (schema! {
                not_null "num_actions": LONG,
                not_null "num_of_add_files": LONG,
                not_null "size_in_bytes": LONG,
            }),
            nullable "root": (StructType::try_new(
                FILE_META_SCHEMA.fields().cloned().chain([
                    StructField::not_null("checkpoint_schema", DataType::STRING),
                ]),
            )?),
        },
    );
    let mut writes = plan.nodes.iter().filter_map(|node| match &node.op {
        Operator::WriteParquet(write) => Some(write),
        _ => None,
    });
    let write = writes.next().expect("the plan must write a checkpoint");
    assert!(
        writes.next().is_none(),
        "the shared write must execute once"
    );
    let input_schema = write.write_schema.clone();
    assert_ne!(input_schema, *FILE_META_SCHEMA);
    assert!(input_schema.field("location").is_none());
    assert!(input_schema.field("num_actions").is_none());

    let executor = engine.require_plan_executor()?;
    let batches = executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(|batch| batch?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    let rows = json_rows(&batches, false)?;
    assert_eq!(rows.len(), 2);
    let statistics = rows.iter().find_map(|row| row.get("statistics")).unwrap();
    let root_row = rows.iter().find(|row| row.get("root").is_some()).unwrap();
    let root = &root_row["root"];
    let descriptor = FileMeta::new(
        Url::parse(root["location"].as_str().unwrap())?,
        root["last_modified"].as_i64().unwrap(),
        root["size"].as_u64().unwrap(),
    );
    assert_eq!(descriptor.location, writer.checkpoint_path()?);
    let completed = file_bytes(&engine, &descriptor.location)?;
    assert_eq!(descriptor.size, completed.len() as u64);
    assert_eq!(
        statistics["num_actions"].as_u64(),
        Some(expected.len() as u64)
    );
    assert_eq!(
        statistics["num_of_add_files"].as_u64(),
        Some(expected_adds as u64)
    );
    assert_eq!(statistics["size_in_bytes"].as_u64(), Some(descriptor.size));
    let stored = storage
        .list_from(&log, None)?
        .find(|file| {
            file.as_ref()
                .is_ok_and(|file| file.location == descriptor.location)
        })
        .expect("the completed descriptor must describe a stored file")?;
    assert_eq!(descriptor, stored);

    let reader = ParquetRecordBatchReaderBuilder::try_new(completed)?;
    let disk_schema = Arc::new(StructType::try_from_arrow(reader.schema().clone())?);
    assert_eq!(disk_schema, input_schema);
    let internal_schema = root["checkpoint_schema"].as_str().unwrap();
    assert_eq!(
        internal_schema.encode_utf16().count() > CHECKPOINT_SCHEMA_THRESHOLD,
        oversized_schema,
    );
    let returned_schema: StructType = serde_json::from_str(internal_schema)?;
    assert_eq!(&returned_schema, disk_schema.as_ref());
    if !v2 {
        assert!(
            disk_schema
                .field_at(&crate::expressions::column_name!("add.stats_parsed"))
                .is_err()
        );
    }
    let DataType::Struct(remove) = disk_schema.field("remove").unwrap().data_type() else {
        panic!("remove must be a struct");
    };
    assert!(remove.field("stats").is_none());
    assert!(remove.field("tags").is_none());
    let persisted = reader.build()?.collect::<Result<Vec<_>, _>>()?;
    let actual = canonical_actions(json_rows(&persisted, false)?);
    let mut comparable = actual.clone();
    if matches!(state, "native_stats" | "variant_stats") {
        for row in &mut expected {
            let bound = match row["add"]["path"].as_str() {
                Some("native.parquet") => 10,
                Some("base.parquet") => 200,
                _ => continue,
            };
            let mut stats = json!({"numRecords":10,"nullCount":{"value":0},
                "minValues":{"value":bound},"maxValues":{"value":bound}});
            if state == "variant_stats" {
                stats["nullCount"]["col"] = json!(2);
            }
            row["add"]["stats"] = stats;
        }
        for row in &mut comparable {
            if matches!(
                row["add"]["path"].as_str(),
                Some("native.parquet" | "base.parquet")
            ) {
                row["add"]["stats"] = serde_json::from_str(row["add"]["stats"].as_str().unwrap())?;
            }
        }
        expected.sort_by_cached_key(Value::to_string);
        comparable.sort_by_cached_key(Value::to_string);
    }
    assert_eq!(comparable, expected);
    if matches!(state, "native_stats" | "variant_stats") {
        let native = actual
            .iter()
            .find(|row| row["add"]["path"] == "native.parquet")
            .unwrap();
        let stats: Value = serde_json::from_str(native["add"]["stats"].as_str().unwrap())?;
        assert_eq!(stats["numRecords"], 10);
        assert_eq!(stats["minValues"]["value"], 10);
        assert_eq!(stats["maxValues"]["value"], 10);
        if state == "variant_stats" {
            assert_eq!(stats["nullCount"]["col"], 2);
            assert!(stats["minValues"].get("col").is_none());
            assert!(stats["maxValues"].get("col").is_none());
        }
        assert_eq!(native["add"]["deletionVector"]["cardinality"], 1);
        let original = actual
            .iter()
            .find(|row| row["add"]["path"] == "base.parquet")
            .unwrap();
        let stats: Value = serde_json::from_str(original["add"]["stats"].as_str().unwrap())?;
        assert_eq!(stats["minValues"]["value"], 200);
        assert_eq!(stats["maxValues"]["value"], 200);
        assert!(stats["minValues"].get("part_int").is_none());
        if state == "variant_stats" {
            assert_eq!(stats["nullCount"]["col"], 2);
            assert!(stats["minValues"].get("col").is_none());
            assert!(stats["maxValues"].get("col").is_none());
        }
        assert_eq!(expected_adds, 3);
    }
    let checkpoint_metadata = actual
        .iter()
        .filter_map(|row| row.get("checkpointMetadata"))
        .collect::<Vec<_>>();
    assert_eq!(checkpoint_metadata.len(), usize::from(v2));
    if let Some(metadata) = checkpoint_metadata.first() {
        assert_eq!(metadata["version"].as_u64(), Some(snapshot.version()));
    }
    assert!(actual.iter().all(|row| row.get("sidecar").is_none()));
    for row in &actual {
        for name in ["add", "remove"] {
            if let Some(action) = row.get(name) {
                assert_eq!(action["dataChange"], false);
            }
        }
    }

    let bytes = writer.last_checkpoint_bytes_from_result(data_result(batches))?;
    let value: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(
        value,
        json!({
            "version": snapshot.version(),
            "size": expected.len(),
            "parts": null,
            "sizeInBytes": descriptor.size,
            "numOfAddFiles": expected_adds,
            "checkpointSchema": (!oversized_schema).then_some(&disk_schema),
            "checkpointFiles": [{
                "path": descriptor.location.path_segments().unwrap().next_back().unwrap(),
                "length": descriptor.size,
                "isDir": false,
                "modificationTime": descriptor.last_modified,
            }],
            "tags": null,
            "v2Checkpoint": null,
            "checksum": null,
        }),
    );
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes)?;
    assert!(hint.v2_checkpoint.is_none());
    assert!(hint.applies_to(&[ParsedLogPath::new(descriptor).unwrap()]));
    if let Some(previous) = previous_hint {
        assert_eq!(file_bytes(&engine, &hint_path)?, previous);
        assert!(
            executor
                .execute_op(Operation::IoOperation(IoOperation::write_bytes(
                    hint_path.clone(),
                    bytes.clone().into(),
                    false,
                )))
                .is_err()
        );
        assert_eq!(file_bytes(&engine, &hint_path)?, previous);
    } else {
        assert!(
            storage
                .list_from(&log, None)?
                .all(|file| file.unwrap().location != hint_path)
        );
    }
    assert!(matches!(
        executor.execute_op(Operation::IoOperation(IoOperation::write_bytes(
            hint_path.clone(),
            bytes.clone().into(),
            true,
        )))?,
        PlanResult::Unit
    ));
    assert_eq!(file_bytes(&engine, &hint_path)?.as_ref(), bytes.as_slice());

    // Retain the target commit for timestamp lookup; older history cannot mask the checkpoint.
    for file in storage.list_from(&log, None)? {
        let file = file?;
        if ParsedLogPath::new(file.clone()).is_some_and(|path| path.version < snapshot.version()) {
            storage.delete(&file.location)?;
        }
    }
    let reloaded = Snapshot::builder_for(snapshot.table_root()).build(&engine)?;
    assert_eq!(reloaded.version(), snapshot.version());
    assert_eq!(
        reloaded.log_segment().checkpoint_version,
        Some(snapshot.version())
    );
    assert_eq!(
        reloaded.table_configuration().metadata(),
        snapshot.table_configuration().metadata()
    );
    assert_eq!(
        reloaded.table_configuration().protocol(),
        snapshot.table_configuration().protocol()
    );
    let mut reloaded_rows = reconciled_rows(
        &reloaded.clone().create_checkpoint_writer(&engine)?,
        &engine,
    )?;
    if matches!(state, "native_stats" | "variant_stats") {
        for row in &mut reloaded_rows {
            if matches!(
                row["add"]["path"].as_str(),
                Some("native.parquet" | "base.parquet")
            ) {
                row["add"]["stats"] = serde_json::from_str(row["add"]["stats"].as_str().unwrap())?;
            }
        }
        reloaded_rows.sort_by_cached_key(Value::to_string);
    }
    assert_eq!(reloaded_rows, expected);
    if matches!(state, "mixed" | "predecessor") {
        assert_eq!(
            reloaded.reader().get_app_id_version("app", &engine)?,
            Some(1)
        );
        assert_eq!(
            reloaded.reader().get_domain_metadata("test", &engine)?,
            Some(r#"{"version":1}"#.into())
        );
    }
    Ok(())
}

#[rstest]
#[case::v1_structured_stats(false, true, "V1 checkpoint plans require JSON-only statistics")]
fn checkpoint_rejects_unsupported_output_before_engine_access(
    #[case] v2: bool,
    #[case] structured_stats: bool,
    #[case] expected_error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let builder = TestTableBuilder::new()
        .with_table_config(TableConfig::new().write_stats_as_struct(structured_stats));
    let table = if v2 {
        builder.with_features(FeatureSet::new().v2_checkpoint())
    } else {
        builder
    }
    .build()?;
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let log = snapshot.log_segment().log_root.clone();
    let before = engine
        .storage_handler()
        .list_from(&log, None)?
        .collect::<KernelResult<Vec<_>>>()?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    ::test_utils::assert_result_error_with_message(
        writer.declarative_checkpoint_data_plan(&PanicEngine),
        expected_error,
    );
    ::test_utils::assert_result_error_with_message(
        writer.declarative_checkpoint_plan(&PanicEngine),
        expected_error,
    );
    assert_eq!(
        engine
            .storage_handler()
            .list_from(&log, None)?
            .collect::<KernelResult<Vec<_>>>()?,
        before
    );
    Ok(())
}

#[rstest]
#[case::v1_missing(false, None, None, true)]
#[case::v1_matching(false, Some(1), Some(1), true)]
#[case::v1_too_few(false, Some(1), Some(0), false)]
#[case::v1_too_many(false, Some(1), Some(2), false)]
#[case::v1_stale(false, Some(0), Some(0), true)]
#[case::v1_indeterminate(false, Some(1), None, true)]
#[case::v2_matching(true, Some(1), Some(1), true)]
#[case::v2_too_many(true, Some(1), Some(2), false)]
#[case::v2_indeterminate(true, Some(1), None, true)]
fn checkpoint_validates_add_count_against_crc(
    #[case] v2: bool,
    #[case] crc_version: Option<Version>,
    #[case] expected_adds: Option<i64>,
    #[case] succeeds: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_features(if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::new()
        })
        .with_log_state(LogState::with_latest_version(1))
        .with_data(1, 1)
        .build()?;
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
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
    let hint_path = snapshot.log_segment().log_root.join("_last_checkpoint")?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let result = engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(
            writer.declarative_checkpoint_plan(&engine)?,
        ))?;
    let bytes = writer.last_checkpoint_bytes_from_result(result);
    if succeeds {
        let hint: LastCheckpointHint = serde_json::from_slice(&bytes?)?;
        assert_eq!(hint.num_of_add_files, Some(1));
    } else {
        let error = bytes.unwrap_err();
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
    assert!(
        engine
            .storage_handler()
            .list_from(&writer.snapshot.log_segment().log_root, None)?
            .all(|file| file.unwrap().location != hint_path)
    );
    Ok(())
}

#[rstest]
#[case::valid([1, 0, 17], &["statistics", "root"], None)]
#[case::missing_statistics([1, 0, 17], &["root"], Some("Missing checkpoint statistics"))]
#[case::duplicate_statistics([1, 0, 17], &["statistics", "statistics", "root"], Some("Invalid or duplicate checkpoint statistics"))]
#[case::negative_actions([-1, 0, 17], &["statistics", "root"], Some("Invalid or duplicate checkpoint statistics"))]
#[case::negative_adds([1, -1, 17], &["statistics", "root"], Some("Invalid or duplicate checkpoint statistics"))]
#[case::too_many_adds([1, 2, 17], &["statistics", "root"], Some("Invalid or duplicate checkpoint statistics"))]
#[case::negative_bytes([1, 0, -1], &["statistics", "root"], Some("Invalid or duplicate checkpoint statistics"))]
#[case::short_bytes([1, 0, 16], &["statistics", "root"], Some("Checkpoint byte count does not match root file"))]
#[case::long_bytes([1, 0, 18], &["statistics", "root"], Some("Checkpoint byte count does not match root file"))]
#[case::missing_root([1, 0, 17], &["statistics"], Some("Missing checkpoint root"))]
#[case::missing_root_schema([1, 0, 17], &["statistics", "root_without_schema"], Some("Invalid checkpoint result row"))]
#[case::duplicate_root([1, 0, 17], &["statistics", "root", "root"], Some("Duplicate checkpoint root"))]
#[case::no_result_arm([1, 0, 17], &["statistics", "root", "empty"], Some("Invalid checkpoint result row"))]
#[case::multiple_result_arms([1, 0, 17], &["statistics", "root", "overlapping"], Some("Invalid checkpoint result row"))]
#[case::orphan_root_size([1, 0, 17], &["statistics_with_root_size", "root"], Some("Invalid checkpoint result row"))]
#[case::orphan_root_time([1, 0, 17], &["statistics_with_root_time", "root"], Some("Invalid checkpoint result row"))]
#[case::schema_on_statistics([1, 0, 17], &["statistics_with_schema", "root"], Some("Invalid checkpoint result row"))]
#[case::orphan_add_count([1, 0, 17], &["statistics", "root_with_adds"], Some("Invalid checkpoint result row"))]
#[case::orphan_byte_count([1, 0, 17], &["statistics", "root_with_bytes"], Some("Invalid checkpoint result row"))]
#[case::missing_action_count([1, 0, 17], &["statistics_without_actions", "root"], Some("Invalid checkpoint result row"))]
#[case::missing_add_count([1, 0, 17], &["statistics_without_adds", "root"], Some("Invalid checkpoint result row"))]
#[case::missing_byte_count([1, 0, 17], &["statistics_without_bytes", "root"], Some("Invalid checkpoint result row"))]
#[case::missing_root_location([1, 0, 17], &["statistics", "root_without_location"], Some("Invalid checkpoint result row"))]
#[case::missing_root_size([1, 0, 17], &["statistics", "root_without_size"], Some("Invalid checkpoint result row"))]
#[case::missing_root_time([1, 0, 17], &["statistics", "root_without_time"], Some("Invalid checkpoint result row"))]
#[case::late_error([1, 0, 17], &["statistics", "root", "late"], Some("late checkpoint result error"))]
fn checkpoint_statistics_decoder_validates_counts_and_root(
    #[case] counts: [i64; 3],
    #[case] arms: &[&str],
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, snapshot, engine) = checkpoint_table(false, "empty", false)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let statistics = json!({"num_actions": counts[0], "num_of_add_files": counts[1], "size_in_bytes": counts[2]});
    let root = json!({"location": writer.checkpoint_path()?, "size": 17, "last_modified": 19,
        "checkpoint_schema": serde_json::to_string(&schema! { nullable "value": LONG })?});
    let rows = arms
        .iter()
        .filter(|arm| **arm != "late")
        .map(|arm| {
            match *arm {
                "statistics" => json!({"statistics": statistics}),
                "root" => json!({"root": root}),
                "root_without_schema" => {
                    let mut partial = root.clone();
                    partial.as_object_mut().unwrap().remove("checkpoint_schema");
                    json!({"root": partial})
                }
                "empty" => json!({}),
                "overlapping" => json!({"statistics": statistics, "root": root}),
                "statistics_with_root_size" => {
                    json!({"statistics": statistics, "root": {"size": 17}})
                }
                "statistics_with_root_time" => {
                    json!({"statistics": statistics, "root": {"last_modified": 19}})
                }
                "statistics_with_schema" => {
                    json!({"statistics": statistics, "root": {"checkpoint_schema": "not JSON"}})
                }
                "root_with_adds" => json!({"root": root, "statistics": {"num_of_add_files": 0}}),
                "root_with_bytes" => json!({"root": root, "statistics": {"size_in_bytes": 17}}),
                "statistics_without_actions"
                | "statistics_without_adds"
                | "statistics_without_bytes" => {
                    let mut partial = statistics.clone();
                    let name = match *arm {
                        "statistics_without_actions" => "num_actions",
                        "statistics_without_adds" => "num_of_add_files",
                        _ => "size_in_bytes",
                    };
                    partial[name] = Value::Null;
                    json!({"statistics": partial})
                }
                "root_without_location" | "root_without_size" | "root_without_time" => {
                    let mut partial = root.clone();
                    let name = match *arm {
                        "root_without_location" => "location",
                        "root_without_size" => "size",
                        _ => "last_modified",
                    };
                    partial[name] = Value::Null;
                    json!({"root": partial})
                }
                _ => unreachable!(),
            }
            .to_string()
        })
        .collect::<Vec<_>>();
    let schema = writer.declarative_checkpoint_plan(&engine)?.schema;
    // Allow partial nested rows so malformed engine results reach the result visitor.
    let mut fields = Vec::new();
    for field in schema.fields() {
        let data_type = match field.data_type() {
            DataType::Struct(nested) => StructType::try_new(
                nested
                    .fields()
                    .map(|child| StructField::nullable(child.name(), child.data_type().clone())),
            )?
            .into(),
            other => other.clone(),
        };
        fields.push(StructField::nullable(field.name(), data_type));
    }
    let schema = Arc::new(StructType::try_new(fields)?);
    let batch = parse_json_impl(&StringArray::from(rows), schema)?;
    let batches = std::iter::once(Ok(
        Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
    ))
    .chain(
        arms.contains(&"late")
            .then(|| Err(KernelError::generic("late checkpoint result error"))),
    );
    let bytes = writer.last_checkpoint_bytes_from_result(PlanResult::Data(Box::new(batches)));
    if let Some(expected_error) = expected_error {
        ::test_utils::assert_result_error_with_message(bytes, expected_error);
    } else {
        let hint: LastCheckpointHint = serde_json::from_slice(&bytes?)?;
        assert_eq!(hint.size, counts[0]);
        assert_eq!(hint.num_of_add_files, Some(counts[1]));
        assert_eq!(hint.size_in_bytes, Some(counts[2]));
    }
    Ok(())
}

#[rstest]
#[case::wrong_version(
    false,
    "00000000000000000001.checkpoint.parquet",
    17,
    "Checkpoint root does not match this writer"
)]
#[case::wrong_directory(
    false,
    "other/00000000000000000000.checkpoint.parquet",
    17,
    "Checkpoint root does not match this writer"
)]
#[case::negative_size(false, "00000000000000000000.checkpoint.parquet", -1, "Negative root size")]
#[case::inline_v2_mismatched_bytes(
    true,
    "00000000000000000000.checkpoint.parquet",
    16,
    "Checkpoint byte count does not match root file"
)]
fn checkpoint_statistics_decoder_rejects_invalid_root(
    #[case] v2: bool,
    #[case] path: &str,
    #[case] size: i64,
    #[case] expected_error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, snapshot, engine) = checkpoint_table(v2, "empty", false)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let rows = [
        json!({"statistics":{"num_actions":1,"num_of_add_files":0,"size_in_bytes":17}}),
        json!({"root":{"location":writer.snapshot.log_segment().log_root.join(path)?,"size":size,"last_modified":19,"checkpoint_schema":serde_json::to_string(&schema! { nullable "value": LONG })?}}),
    ];
    let schema = writer.declarative_checkpoint_plan(&engine)?.schema;
    let batch = parse_json_impl(
        &StringArray::from(rows.map(|row| row.to_string()).to_vec()),
        schema,
    )?;
    ::test_utils::assert_result_error_with_message(
        writer.last_checkpoint_bytes_from_result(data_result(vec![batch])),
        expected_error,
    );
    Ok(())
}

#[rstest]
#[case::below(CHECKPOINT_SCHEMA_THRESHOLD - 1, true)]
#[case::at(CHECKPOINT_SCHEMA_THRESHOLD, true)]
#[case::above(CHECKPOINT_SCHEMA_THRESHOLD + 1, false)]
fn checkpoint_schema_respects_length_limit(
    #[case] length: usize,
    #[case] included: bool,
    #[values("", "雪😀")] prefix: &str,
) -> KernelResult<(), Box<dyn std::error::Error>> {
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
    let (_table, snapshot, engine) = checkpoint_table(false, "empty", false)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let bytes = writer.serialize_checkpoint_hint(
        &crate::checkpoint::LastCheckpointHintStats {
            num_actions: 1,
            num_of_add_files: 0,
            size_in_bytes: 17,
        },
        &FileMeta::new(writer.checkpoint_path()?, 19, 17),
        schema.clone(),
    )?;
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes)?;
    assert_eq!(hint.checkpoint_schema, included.then_some(schema));
    Ok(())
}
