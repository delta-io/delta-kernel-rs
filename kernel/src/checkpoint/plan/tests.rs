use std::sync::Arc;

use ::test_utils::add_commit;
use ::test_utils::table_builder::{FeatureSet, TableConfig, TestTableBuilder, partitioned_schema};
use itertools::Itertools as _;
use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::action_reconciliation::file_metadata::{
    build_checkpoint_read_schema, build_checkpoint_transform,
};
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::action_reconciliation::plan::VERSION;
use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::arrow::array::{Array as _, AsArray as _, Int64Array, StringArray};
use crate::arrow::compute::{cast, filter_record_batch, is_not_null, is_null, or};
use crate::arrow::datatypes::Int64Type;
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use crate::committer::file_meta_from_batches;
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::plans::{Operation, PlanExecutor as _, ScopedPlanExecutor};
use crate::schema::{SchemaStructPatchBuilder, StructType, schema};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::unit_test_utils::copy_test_table;
use crate::unit_test_utils::plans::{execute_plan as execute, json_rows};
use crate::{FilteredEngineData, Snapshot};

fn selected_batch(data: FilteredEngineData) -> KernelResult<RecordBatch> {
    data.apply_selection_vector()?.try_into_record_batch()
}

fn select_checkpoint_actions(
    batches: &[RecordBatch],
    actions: ActionSet,
) -> KernelResult<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            let selected = batch.project(
                &actions
                    .output_action_names()
                    .map(|name| batch.schema().index_of(name).unwrap())
                    .collect::<Vec<_>>(),
            )?;
            let keep = selected
                .columns()
                .iter()
                .map(|column| is_not_null(column.as_ref()).unwrap())
                .reduce(|left, right| or(&left, &right).unwrap())
                .unwrap();
            Ok(filter_record_batch(&selected, &keep)?)
        })
        .collect()
}

fn checkpoint_plans(
    writer: &CheckpointWriter,
    engine: &dyn Engine,
) -> KernelResult<Vec<(Plan, ActionSet)>> {
    if writer.schema_context.is_v2 {
        let (content, non_content) = writer.declarative_v2_checkpoint_data_plans(engine)?;
        return Ok(vec![
            (content, ActionSet::live_adds_with_tombstones()),
            (
                non_content,
                ActionSet {
                    live_adds: false,
                    include_tombstones: false,
                    ..ActionSet::ALL
                },
            ),
        ]);
    }
    Ok(vec![(
        writer.declarative_checkpoint_data_plan(engine)?,
        ActionSet::ALL,
    )])
}

fn assert_checkpoint_plan(
    engine: &dyn Engine,
    plan: Plan,
    actions: ActionSet,
    expected: &[RecordBatch],
) -> KernelResult<Vec<RecordBatch>> {
    let schema = plan.schema.clone();
    let actual = execute(engine, plan)?;
    let expected = checkpoint_reference_batches(engine, expected, actions)?;
    assert_eq!(
        schema.as_ref(),
        &StructType::try_from_arrow(expected[0].schema())?
    );
    assert_eq!(json_rows(&actual, true)?, json_rows(&expected, true)?);
    Ok(actual)
}

// Apply Runtime's checkpoint-only payload rules to the unchanged imperative reference.
fn checkpoint_reference_batches(
    engine: &dyn Engine,
    batches: &[RecordBatch],
    actions: ActionSet,
) -> KernelResult<Vec<RecordBatch>> {
    select_checkpoint_actions(batches, actions)?
        .into_iter()
        .map(|batch| {
            let input = Arc::new(StructType::try_from_arrow(batch.schema())?);
            let mut patch = ProjectionStructPatchBuilder::new(&input);
            if actions.live_adds {
                patch = patch.replace_expr_at(["add"], "dataChange", lit(false));
            }
            if actions.include_tombstones {
                patch = patch
                    .replace_expr_at(["remove"], "dataChange", lit(false))
                    .drop_at(["remove"], "stats")
                    .drop_at(["remove"], "tags");
            }
            let (output, expression) = patch.build()?;
            engine
                .evaluation_handler()
                .new_expression_evaluator(input, expression, output.into())?
                .evaluate(&ArrowEngineData::new(batch))?
                .try_into_record_batch()
        })
        .collect()
}

#[rstest]
#[case("partition_cm/name")]
#[case("app-txn-no-checkpoint")]
#[case("app-txn-checkpoint")]
#[case("v2-checkpoints-parquet-with-sidecars")]
#[case("v2-checkpoints-json-with-sidecars")]
#[case("v2-checkpoints-parquet-without-sidecars")]
#[case("v2-checkpoints-json-without-sidecars")]
#[case("v1-single-part-struct-stats-only")]
#[case("v1-multi-part-struct-stats-only")]
#[case("v1-multi-part-partitioned-struct-stats-only")]
#[case("v2-json-sidecars-struct-stats-only")]
#[case("v2-parquet-sidecars-struct-stats-only")]
fn checkpoint_plan_matches_existing_checkpoint_data(
    #[case] table: &str,
    #[values(false, true)] with_tail: bool,
    #[values(false, true)] json_stats: bool,
    #[values(false, true)] struct_stats: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (table_root, _tempdir) = copy_test_table(table)?;
    let engine = Arc::new(SyncEngine::new());
    let mut snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .unwrap();
    if with_tail {
        let mut action = add("tail.parquet", None);
        if table == "partition_cm/name" {
            let partitions = snapshot
                .table_configuration()
                .build_partition_values_parsed_schema()
                .unwrap();
            let physical_name = partitions.fields().next().unwrap().name();
            assert_ne!(physical_name, "category");
            action["add"]["partitionValues"] = json!({physical_name: "US"});
        }
        let version = snapshot.version() + 1;
        let path = snapshot
            .log_segment()
            .log_root
            .join(&format!("{version:020}.json"))?;
        std::fs::write(path.to_file_path().unwrap(), action.to_string())?;
        snapshot = Snapshot::builder_for(snapshot.table_root().clone())
            .build(engine.as_ref())
            .unwrap();
        assert_eq!(snapshot.version(), version);
    }
    let mut writer = snapshot.create_checkpoint_writer(engine.as_ref())?;
    writer.schema_context.stats_config.write_stats_as_json = json_stats;
    writer.schema_context.stats_config.write_stats_as_struct = struct_stats;
    let expected = writer
        .checkpoint_data(engine.as_ref())?
        .map(|data| selected_batch(data?))
        .collect::<KernelResult<Vec<_>>>()?;
    for (plan, actions) in checkpoint_plans(&writer, engine.as_ref())? {
        if !writer.schema_context.is_v2 {
            let executor = SyncPlanExecutor::new(None).get_scoped();
            let retained = executor.execute_and_retain("checkpoint", plan)?;
            assert_single_part_checkpoint_write(&writer, &executor, retained, &expected)?;
            continue;
        }
        if !actions.contains_content_actions() {
            let sidecars = writer
                .snapshot
                .table_root()
                .join("_delta_log/_sidecars/")?
                .to_file_path()
                .unwrap();
            if sidecars.exists() {
                std::fs::remove_dir_all(sidecars)?;
            }
        }
        assert_checkpoint_plan(engine.as_ref(), plan, actions, &expected)?;
    }
    Ok(())
}

fn assert_single_part_checkpoint_write(
    writer: &CheckpointWriter,
    executor: &dyn ScopedPlanExecutor,
    actions: RelationRef,
    expected: &[RecordBatch],
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let expected = checkpoint_reference_batches(&SyncEngine::new(), expected, ActionSet::ALL)?;
    assert_eq!(
        actions.schema().as_ref(),
        &StructType::try_from_arrow(expected[0].schema())?
    );
    let path = writer.checkpoint_path()?.to_file_path().unwrap();
    // Fixtures may already have a checkpoint at this version; reconstruction is retained first.
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    let root = executor.execute_and_retain(
        "root",
        writer.declarative_checkpoint_write_plan(actions.clone())?,
    )?;
    let result = executor.execute_op(Operation::QueryPlan(
        PlanBuilder::relation_source(root.clone()).build()?,
    ))?;
    let file = file_meta_from_batches(result.into_data()?)?;
    assert_eq!(file.location, writer.checkpoint_path()?);
    assert_eq!(file.size, std::fs::metadata(&path)?.len());
    let mut statistics = executor
        .execute_op(Operation::QueryPlan(
            writer.declarative_last_checkpoint_statistics_plan(
                actions.clone(),
                root,
                None,
                None,
            )?,
        ))?
        .into_data()?;
    let statistics_row = statistics.next().transpose()?.unwrap();
    assert!(statistics.next().is_none());
    let bytes =
        writer.last_checkpoint_bytes(statistics_row.as_ref(), &file, actions.schema(), [])?;
    let hint: Value = serde_json::from_slice(&bytes)?;
    assert!(hint["checksum"].is_string());
    assert_eq!(
        hint,
        json!({
            "version": writer.snapshot.version(),
            "size": expected.iter().map(RecordBatch::num_rows).sum::<usize>(),
            "parts": null,
            "sizeInBytes": file.size,
            "numOfAddFiles": expected.iter().map(|batch| {
                let adds = batch.column_by_name("add").unwrap();
                adds.len() - adds.null_count()
            }).sum::<usize>(),
            "checkpointSchema": actions.schema(),
            "checkpointFiles": [{
                "path": file.location.path_segments().unwrap().next_back().unwrap(),
                "length": file.size,
                "isDir": false,
                "modificationTime": file.last_modified,
            }],
            "checksum": hint["checksum"],
            "tags": null,
            "v2Checkpoint": null,
        })
    );
    // Publish Kernel's bytes, then read the checkpoint through the ordinary snapshot reader.
    std::fs::write(path.parent().unwrap().join("_last_checkpoint"), bytes)?;
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(writer.snapshot.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(
        snapshot.log_segment().checkpoint_version,
        Some(snapshot.version())
    );
    let mut reloaded = snapshot.create_checkpoint_writer(&engine)?;
    reloaded.schema_context.stats_config = writer.schema_context.stats_config;
    let actual = reloaded
        .checkpoint_data(&engine)?
        .map(|data| selected_batch(data?))
        .collect::<KernelResult<Vec<_>>>()?;
    assert_eq!(
        json_rows(
            &checkpoint_reference_batches(&engine, &actual, ActionSet::ALL)?,
            true
        )?,
        json_rows(&expected, true)?
    );
    let hint: LastCheckpointHint = serde_json::from_value(hint)?;
    assert!(hint.applies_to(&[crate::path::ParsedLogPath::new(file).unwrap()]));
    Ok(())
}

// A fixed-cutoff oracle using imperative replay and stats conversion.
fn existing_checkpoint_data(
    writer: &CheckpointWriter,
    engine: &dyn Engine,
    file_cutoff: i64,
    txn_cutoff: Option<i64>,
) -> KernelResult<Vec<RecordBatch>> {
    let mut processor = ActionReconciliationProcessor::new(file_cutoff, txn_cutoff, None);
    let context = &writer.schema_context;
    let (schema, transform) = build_checkpoint_transform(
        &context.stats_config,
        &writer.read_schema,
        &context.stats_schema,
        context.partition_schema.as_ref(),
    )?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        writer.read_schema.clone(),
        transform,
        schema.clone().into(),
    )?;
    let mut batches = vec![];
    for batch in writer
        .snapshot
        .log_segment()
        .read_actions(engine, writer.read_schema.clone())?
    {
        let (data, selected) = processor
            .process_actions_batch(batch?)?
            .filtered_data
            .into_parts();
        let mut batch = selected_batch(FilteredEngineData::try_new(
            evaluator.evaluate(data.as_ref())?,
            selected,
        )?)?;
        if txn_cutoff.is_some() {
            // Runtime expires a winning transaction without lastUpdated; the legacy reference
            // retains it. Replay has already suppressed older transactions for that app.
            let txn = batch["txn"].as_struct();
            let keep = or(&is_null(txn)?, &is_not_null(&txn["lastUpdated"])?)?;
            batch = filter_record_batch(&batch, &keep)?;
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn add(path: &str, dv_offset: Option<i32>) -> Value {
    let mut add = json!({"path":path,"size":10,"modificationTime":1,"dataChange":true,
        "partitionValues":{},"stats":"{\"numRecords\":2}"});
    if let Some(offset) = dv_offset {
        add["deletionVector"] = json!({"storageType":"p","pathOrInlineDv":"file:///dv.bin",
            "offset":offset,"sizeInBytes":1,"cardinality":1});
    }
    json!({"add":add})
}

#[rstest]
#[case::v1(false, false, 32, false)]
#[case::v2_inline(true, false, 32, false)]
#[case::v2_sidecars(true, true, 32, false)]
#[case::no_indexed_columns(false, false, 0, false)]
#[case::clustering_not_indexed(false, false, 0, true)]
#[tokio::test]
async fn checkpoint_plan_reconciles_all_groups_and_retains_after_replay(
    #[case] v2: bool,
    #[case] sidecars: bool,
    #[case] indexed_columns: i32,
    #[case] clustered: bool,
    #[values(false, true)] structured_stats: bool,
    #[values(None, Some(100i64))] txn_cutoff: Option<i64>,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let mut features = FeatureSet::new().deletion_vectors().domain_metadata();
    if v2 {
        features = features.v2_checkpoint();
    }
    let builder = TestTableBuilder::new()
        .with_features(features)
        .with_table_config(
            TableConfig::new()
                .write_stats_as_struct(structured_stats)
                .num_data_skipping_indexed_cols(indexed_columns.into()),
        );
    let table = if clustered {
        builder.with_clustering_columns(["long_col"])
    } else {
        builder
    }
    .build()
    .unwrap();
    let store = table.store();
    let engine = SyncEngine::new_with_store(store.clone());
    let initial = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let mut metadata = serde_json::to_value(initial.table_configuration().metadata())?;
    let mut protocol = json!({"protocol": initial.table_configuration().protocol()});
    let mut keep = add("keep.parquet", None);
    keep["add"]["stats"] = json!(
        json!({"numRecords":2,"minValues":{"long_col":7},
        "maxValues":{"long_col":7},"nullCount":{"long_col":0}})
        .to_string()
    );
    let tombstone = json!({"path":"same-path.parquet","deletionTimestamp":101,
        "dataChange":true,"extendedFileMetadata":true,"partitionValues":{"part":"US"},
        "size":10,"baseRowId":40,"defaultRowCommitVersion":1,
        "stats":"{\"numRecords\":2}","tags":{"source":"commit"},
        "deletionVector":{"storageType":"p","pathOrInlineDv":"file:///dv.bin","offset":0,
            "sizeInBytes":1,"cardinality":1}});
    let spec = if sidecars {
        CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        })
    } else if v2 {
        CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)
    } else {
        CheckpointSpec::V1
    };

    metadata["id"] = json!("new");
    protocol["protocol"]["writerFeatures"]
        .as_array_mut()
        .unwrap()
        .push(json!("appendOnly"));
    for (version, actions) in [
        vec![
            add("expired.parquet", None),
            add("recent.parquet", None),
            keep,
            add("same-path.parquet", Some(0)),
            add("same-path.parquet", Some(10)),
            json!({"txn":{"appId":"app","version":50,"lastUpdated":999}}),
            json!({"txn":{"appId":"legacy","version":50,"lastUpdated":999}}),
            json!({"domainMetadata":{"domain":"deleted","configuration":"old","removed":false}}),
            json!({"domainMetadata":{"domain":"updated","configuration":"old","removed":false}}),
        ],
        vec![
            protocol,
            json!({"metaData":metadata}),
            json!({"remove":{"path":"expired.parquet","deletionTimestamp":100,"dataChange":true}}),
            json!({"remove":{"path":"recent.parquet","deletionTimestamp":101,"dataChange":true}}),
            json!({"remove":tombstone}),
            json!({"txn":{"appId":"app","version":1,"lastUpdated":100}}),
            json!({"txn":{"appId":"legacy","version":1}}),
            json!({"txn":{"appId":"recent","version":1,"lastUpdated":101}}),
            json!({"domainMetadata":{"domain":"deleted","configuration":"","removed":true}}),
            json!({"domainMetadata":{"domain":"updated","configuration":"new","removed":false}}),
        ],
        vec![
            json!({"remove":{"path":"recent.parquet","deletionTimestamp":100,"dataChange":true}}),
            json!({"remove":{"path":"missing-timestamp.parquet","dataChange":true}}),
        ],
    ]
    .into_iter()
    .enumerate()
    {
        let version = version as u64 + 1;
        add_commit(
            table.table_root(),
            store.as_ref(),
            version,
            actions.iter().join("\n"),
        )
        .await
        .unwrap();
        if version == 1 {
            Snapshot::builder_for(table.table_root())
                .build(&engine)
                .unwrap()
                .checkpoint(&engine, Some(&spec))?;
        }
    }
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(1));
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let expected = existing_checkpoint_data(&writer, &engine, 100, txn_cutoff)?;
    // Fixed-cutoff checks exercise the shared retention expressions on fully replayed actions.
    // The fixture comparisons above exercise the public V1 and V2 factories with real cutoffs.
    let plan = writer
        .metadata_planner(&engine, ActionSet::ALL)?
        .replay()?
        .filter(Predicate::and(
            file_retention_predicate(100),
            non_content_retention_predicate(txn_cutoff),
        ))?
        .project_patch(checkpoint_file_projection)?
        .build()?;
    // Counts use the retained output after replay and retention, including tombstones and
    // non-content actions. Compare with the existing checkpoint implementation's rows.
    let executor = SyncPlanExecutor::new(Some(store.clone())).get_scoped();
    let retained = executor.execute_and_retain("checkpoint", plan.clone())?;
    let counts = CheckpointWriter::declarative_checkpoint_counts_plan(retained.clone())?;
    let mut rows = executor
        .execute_op(Operation::QueryPlan(counts))?
        .into_data()?;
    let batch = rows.next().unwrap()?.try_into_record_batch()?;
    let count = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as usize
    };
    assert_eq!(
        count(0),
        expected.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    assert_eq!(
        count(1),
        expected
            .iter()
            .map(|batch| {
                let adds = batch.column_by_name("add").unwrap();
                adds.len() - adds.null_count()
            })
            .sum::<usize>()
    );
    let actual = assert_checkpoint_plan(&engine, plan, ActionSet::ALL, &expected)?;
    for batch in &actual {
        let remove = batch.column_by_name("remove").unwrap().as_struct();
        for field in ["stats", "tags"] {
            assert!(remove.column_by_name(field).is_none());
        }
        for action in ["add", "remove"] {
            let files = batch.column_by_name(action).unwrap().as_struct();
            let paths = files.column_by_name("path").unwrap();
            let changes = files.column_by_name("dataChange").unwrap().as_boolean();
            for row in 0..batch.num_rows() {
                assert_eq!(files.is_null(row), paths.is_null(row));
                assert!(!changes.value(row));
            }
        }
    }
    let result = json_rows(&actual, true)?;
    assert_eq!(
        result
            .iter()
            .filter_map(|row| row["txn"]["appId"].as_str())
            .sorted()
            .collect::<Vec<_>>(),
        if txn_cutoff.is_none() {
            vec!["app", "legacy", "recent"]
        } else {
            vec!["recent"]
        },
    );
    let remove = &result
        .iter()
        .find(|row| row["remove"]["path"] == "same-path.parquet")
        .unwrap()["remove"];
    for field in [
        "deletionVector",
        "baseRowId",
        "defaultRowCommitVersion",
        "partitionValues",
        "size",
        "extendedFileMetadata",
    ] {
        assert_eq!(remove[field], tombstone[field]);
    }
    if v2 {
        return Ok(());
    }

    // Write the retained rows and compare the reconstructed state through the imperative reader.
    let write_result = executor.execute_op(Operation::QueryPlan(
        writer.declarative_checkpoint_write_plan(retained)?,
    ))?;
    file_meta_from_batches(write_result.into_data()?)?;
    let reloaded = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(reloaded.log_segment().checkpoint_version, Some(3));
    let reloaded_writer = reloaded.create_checkpoint_writer(&engine)?;
    assert_eq!(
        json_rows(
            &checkpoint_reference_batches(
                &engine,
                &existing_checkpoint_data(&reloaded_writer, &engine, 100, txn_cutoff)?,
                ActionSet::ALL
            )?,
            true
        )?,
        result
    );
    Ok(())
}

#[rstest]
#[case::complete(true, false, true)]
#[case::incompatible(false, false, true)]
#[case::partial_with_json(true, true, true)]
#[case::partial_native_only(true, true, false)]
#[tokio::test]
async fn checkpoint_uses_declared_metadata_representations(
    #[values(false, true)] v2: bool,
    #[case] compatible: bool,
    #[case] partial_native: bool,
    #[case] source_json: bool,
    #[values(false, true)] write_json: bool,
    #[values(false, true)] write_struct: bool,
    #[values(false, true)] parsed_partitions: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_features(if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::new()
        })
        .with_schema(
            partitioned_schema()
                .project(&["value", "part_long"])
                .unwrap(),
        )
        .with_partition_columns(["part_long"])
        .with_table_config(
            TableConfig::new()
                .write_stats_as_json(true)
                .write_stats_as_struct(true),
        )
        .build()
        .unwrap();
    let store = table.store();
    let engine = SyncEngine::new_with_store(store.clone());
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let metadata = json!({"metaData": snapshot.table_configuration().metadata()});
    let protocol = json!({"protocol": snapshot.table_configuration().protocol()});
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let stats = |id: i64| {
        json!({"numRecords":1,"minValues":{"value":id},
        "maxValues":{"value":id},"nullCount":{"value":0}})
    };
    let file = |path: &str, id: i64| {
        let mut row = add(path, None);
        row["add"]["partitionValues"] = json!({"part_long":id.to_string()});
        row["add"]["stats"] = json!(stats(id).to_string());
        row
    };
    let has_bounds = |id| !partial_native || source_json || id == 30;
    let available = if partial_native {
        build_checkpoint_read_schema(
            &writer.schema_context.checkpoint_base_schema,
            &schema! { nullable "numRecords": LONG },
            writer.schema_context.partition_schema.as_deref(),
        )?
    } else if compatible {
        writer.read_schema.clone()
    } else {
        build_checkpoint_read_schema(
            &writer.schema_context.checkpoint_base_schema,
            &schema! { nullable "minValues": { nullable "value": STRING } },
            Some(&schema! { nullable "part_long": STRING }),
        )?
    };
    let available = if source_json {
        available
    } else {
        Arc::new(
            SchemaStructPatchBuilder::new()
                .drop_at(["add"], "stats")
                .build(&available)?,
        )
    };
    let mut input = vec![protocol.to_string(), metadata.to_string()];
    for (path, id) in [("native.parquet", 10), ("base.parquet", 20)] {
        let mut row = file(path, id);
        row["add"]["stats_parsed"] = if compatible {
            stats(id)
        } else {
            json!({"minValues":{"value":"incompatible"}})
        };
        row["add"]["partitionValues_parsed"] = if compatible {
            json!({"part_long":id})
        } else {
            json!({"part_long":"incompatible"})
        };
        input.push(row.to_string());
    }
    let batch = parse_json_impl(&StringArray::from(input), available)?;
    engine.parquet_handler().write_parquet_file(
        writer.checkpoint_path()?,
        Box::new(std::iter::once(Ok(
            Box::new(ArrowEngineData::new(batch)) as Box<dyn crate::EngineData>
        ))),
    )?;
    add_commit(
        table.table_root(),
        store.as_ref(),
        1,
        file("tail.parquet", 30).to_string(),
    )
    .await
    .unwrap();
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let mut writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let shape = snapshot.checkpoint_info(&engine)?;
    let mut stats = if write_struct {
        StatsOptions::all_struct()
    } else {
        StatsOptions::none()
    };
    stats.synthesize_json = write_json;
    let context = &writer.schema_context;
    let state = MetadataPlanner::new(snapshot.log_segment(), ActionSet::ALL)
        .with_checkpoint_shape(shape)
        .with_stats(stats)
        .with_stats_schemas(
            (write_struct || write_json).then(|| context.stats_schema.clone()),
            write_struct.then(|| context.stats_schema.clone()),
        )
        .with_partition_values(
            PartitionValuesOptions {
                parsed_struct: parsed_partitions,
                ..Default::default()
            },
            context
                .partition_schema
                .clone()
                .filter(|_| parsed_partitions),
        )
        .replay()?;
    let plan = state.build()?;
    for row in json_rows(&execute(&engine, plan)?, true)? {
        let expected_version = i64::from(row["add"]["path"] == "tail.parquet");
        assert_eq!(row[VERSION], expected_version);
        if row["add"].is_null() {
            continue;
        }
        let id = match row["add"]["path"].as_str().unwrap() {
            "native.parquet" => 10,
            "base.parquet" => 20,
            "tail.parquet" => 30,
            path => panic!("unexpected add {path}"),
        };
        assert_eq!(row["add"].get("stats").is_some(), write_json);
        assert_eq!(row["add"].get("stats_parsed").is_some(), write_struct);
        assert_eq!(
            row["add"].get("partitionValues_parsed").is_some(),
            parsed_partitions
        );
        if write_json {
            let stats: Value = serde_json::from_str(row["add"]["stats"].as_str().unwrap())?;
            assert_eq!(stats["numRecords"], 1);
            assert_eq!(
                stats["minValues"]["value"].as_i64(),
                has_bounds(id).then_some(id)
            );
        }
        if write_struct {
            assert_eq!(row["add"]["stats_parsed"]["numRecords"], 1);
            assert_eq!(
                row["add"]["stats_parsed"]["minValues"]["value"].as_i64(),
                has_bounds(id).then_some(id)
            );
        }
        if parsed_partitions {
            assert_eq!(row["add"]["partitionValues_parsed"]["part_long"], id);
        }
    }
    writer.schema_context.stats_config.write_stats_as_json = write_json;
    writer.schema_context.stats_config.write_stats_as_struct = write_struct;
    let (plan, actions) = if v2 {
        let (content, _) = writer.declarative_v2_checkpoint_data_plans(&engine)?;
        (content, ActionSet::live_adds_with_tombstones())
    } else {
        (
            writer.declarative_checkpoint_data_plan(&engine)?,
            ActionSet::ALL,
        )
    };
    let actual = execute(&engine, plan)?;
    for batch in actual.iter().filter(|_| write_struct) {
        let batch = batch.normalize(".", None)?;
        let ids = batch["add.partitionValues_parsed.part_long"].as_primitive::<Int64Type>();
        let records = batch["add.stats_parsed.numRecords"].as_primitive::<Int64Type>();
        assert_eq!(
            records,
            &Int64Array::from_iter(ids.iter().map(|id| id.map(|_| 1)))
        );
        for field in ["minValues", "maxValues", "nullCount"] {
            let values = cast(
                &batch[format!("add.stats_parsed.{field}.value").as_str()],
                &crate::arrow::datatypes::DataType::Int64,
            )?;
            let values = values.as_primitive::<Int64Type>();
            let expected = Int64Array::from_iter(ids.iter().map(|id| {
                id.filter(|id| has_bounds(*id))
                    .map(|id| if field == "nullCount" { 0 } else { id })
            }));
            assert_eq!(values, &expected);
        }
    }
    if compatible && !partial_native {
        let expected = writer
            .checkpoint_data(&engine)?
            .map(|data| selected_batch(data?))
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(
            json_rows(&actual, true)?,
            json_rows(
                &checkpoint_reference_batches(&engine, &expected, actions)?,
                true
            )?
        );
    }
    let actual_rows = json_rows(&actual, true)?;
    for (path, id) in [
        ("native.parquet", 10),
        ("base.parquet", 20),
        ("tail.parquet", 30),
    ] {
        let row = actual_rows
            .iter()
            .find(|row| row["add"]["path"] == path)
            .unwrap();
        if write_struct {
            assert_eq!(row["add"]["partitionValues_parsed"]["part_long"], id);
        }
        if write_json {
            let stats: Value = serde_json::from_str(row["add"]["stats"].as_str().unwrap())?;
            assert_eq!(stats["numRecords"], 1);
            assert_eq!(
                stats["minValues"]["value"].as_i64(),
                has_bounds(id).then_some(id)
            );
        }
    }
    assert_eq!(
        ["add", "protocol", "metaData"].map(|kind| actual_rows
            .iter()
            .filter(|row| !row[kind].is_null())
            .count()),
        [3, usize::from(!v2), usize::from(!v2)]
    );

    Ok(())
}

#[rstest]
#[case("v2-checkpoints-json-with-sidecars")]
#[case("v2-checkpoints-parquet-with-sidecars")]
#[case("v2-checkpoints-json-without-sidecars")]
#[case("v2-checkpoints-parquet-without-sidecars")]
#[case("app-txn-checkpoint")]
fn checkpoint_source_reads_requested_actions(
    #[case] table: &str,
    #[values(
        ActionSet { protocol: true, metadata: true, ..Default::default() },
        ActionSet { live_adds: false, include_tombstones: false, ..ActionSet::ALL },
        ActionSet::live_adds(),
        ActionSet::live_adds_with_tombstones(),
        ActionSet::ALL,
        ActionSet::default()
    )]
    actions: ActionSet,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (table_root, _tempdir) = copy_test_table(table)?;
    let engine = SyncEngine::new();
    let latest = Snapshot::builder_for(table_root.clone())
        .build(&engine)
        .unwrap();
    let version = latest
        .log_segment()
        .checkpoint_version
        .expect("fixture checkpoint");
    let snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(version)
        .build(&engine)
        .unwrap();
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let names = actions.output_action_names().collect::<Vec<_>>();
    let mut processor = ActionReconciliationProcessor::new(i64::MIN, None, None);
    let expected = snapshot
        .log_segment()
        .read_actions(&engine, writer.read_schema.clone())?
        .map(|batch| selected_batch(processor.process_actions_batch(batch?)?.filtered_data))
        .collect::<KernelResult<Vec<_>>>()?;
    let mut expected: Vec<_> = json_rows(&expected, true)?
        .into_iter()
        .filter(|row| names.iter().any(|name| !row[name].is_null()))
        .map(|row| {
            let mut result = serde_json::Map::new();
            for name in &names {
                let mut value = row[*name].clone();
                if *name == "add"
                    && let Some(add) = value.as_object_mut()
                {
                    for field in ["stats", "stats_parsed", "partitionValues_parsed"] {
                        add.remove(field);
                    }
                }
                result.insert((*name).into(), value);
            }
            result.insert(VERSION.into(), json!(version));
            Value::Object(result)
        })
        .collect();
    expected.sort_by_cached_key(Value::to_string);
    assert_eq!(
        expected.is_empty(),
        actions.output_action_names().next().is_none()
    );
    let shape = actions
        .contains_content_actions()
        .then(|| snapshot.checkpoint_info(&engine))
        .transpose()?
        .flatten();
    // A non-content read must succeed without any sidecar files.
    let sidecars = table_root
        .join("_delta_log/_sidecars/")?
        .to_file_path()
        .unwrap();
    if !actions.contains_content_actions() && sidecars.exists() {
        std::fs::remove_dir_all(sidecars)?;
    }
    let plan = MetadataPlanner::new(snapshot.log_segment(), actions)
        .with_checkpoint_shape(shape)
        .replay()?
        .build()?;
    assert_eq!(json_rows(&execute(&engine, plan)?, true)?, expected);
    Ok(())
}
