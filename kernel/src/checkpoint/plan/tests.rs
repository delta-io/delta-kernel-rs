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
use crate::arrow::array::StringArray;
use crate::arrow::compute::{filter_record_batch, is_not_null, or};
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::CheckpointSpec;
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::schema::{StructType, schema};
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
    let expected = select_checkpoint_actions(expected, actions)?;
    assert_eq!(
        schema.as_ref(),
        &StructType::try_from_arrow(expected[0].schema())?
    );
    assert_eq!(json_rows(&actual, true)?, json_rows(&expected, true)?);
    Ok(actual)
}

#[rstest]
#[case("partition_cm/name")]
#[case("app-txn-no-checkpoint")]
#[case("app-txn-checkpoint")]
#[case("v1-single-part-struct-stats-only")]
#[case("v1-multi-part-struct-stats-only")]
#[case("v1-multi-part-partitioned-struct-stats-only")]
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

// A fixed-cutoff oracle using the existing imperative checkpoint replay and stats transform.
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
        batches.push(selected_batch(FilteredEngineData::try_new(
            evaluator.evaluate(data.as_ref())?,
            selected,
        )?)?);
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
#[case::v1(32, false)]
#[case::no_indexed_columns(0, false)]
#[case::clustering_not_indexed(0, true)]
#[tokio::test]
async fn checkpoint_plan_reconciles_all_groups_and_retains_after_replay(
    #[case] indexed_columns: i32,
    #[case] clustered: bool,
    #[values(false, true)] structured_stats: bool,
    #[values(None, Some(100i64))] txn_cutoff: Option<i64>,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let features = FeatureSet::new().deletion_vectors().domain_metadata();
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
    let spec = CheckpointSpec::V1;

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
            json!({"domainMetadata":{"domain":"deleted","configuration":"old","removed":false}}),
            json!({"domainMetadata":{"domain":"updated","configuration":"old","removed":false}}),
        ],
        vec![
            protocol,
            json!({"metaData":metadata}),
            json!({"remove":{"path":"expired.parquet","deletionTimestamp":100,"dataChange":true}}),
            json!({"remove":{"path":"recent.parquet","deletionTimestamp":101,"dataChange":true}}),
            json!({"remove":{"path":"same-path.parquet","deletionTimestamp":101,"dataChange":true,
            "deletionVector":{"storageType":"p","pathOrInlineDv":"file:///dv.bin","offset":0,
                "sizeInBytes":1,"cardinality":1}}}),
            json!({"txn":{"appId":"app","version":1,"lastUpdated":100}}),
            json!({"txn":{"appId":"legacy","version":1}}),
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
    // The fixture comparisons above exercise the public factory with real cutoffs.
    let plan = writer
        .metadata_planner(ActionSet::ALL)?
        .with_checkpoint_shape(writer.snapshot.checkpoint_info(&engine)?)
        .replay()?
        .filter(Predicate::and(
            file_retention_predicate(100),
            non_content_retention_predicate(txn_cutoff),
        ))?
        .project_patch(|patch| patch.drop(VERSION))?
        .build()?;
    let actual = assert_checkpoint_plan(&engine, plan, ActionSet::ALL, &expected)?;
    let result = json_rows(&actual, true)?;

    // The relation is directly writable using the existing ParquetHandler. Reload the resulting
    // checkpoint with the imperative reader and compare the reconstructed state again.
    engine.parquet_handler().write_parquet_file(
        writer.checkpoint_path()?,
        Box::new(actual.into_iter().map(|batch| {
            Ok(
                Box::new(crate::engine::arrow_data::ArrowEngineData::new(batch))
                    as Box<dyn crate::EngineData>,
            )
        })),
    )?;
    let reloaded = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(reloaded.log_segment().checkpoint_version, Some(3));
    let reloaded_writer = reloaded.create_checkpoint_writer(&engine)?;
    assert_eq!(
        json_rows(
            &select_checkpoint_actions(
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
#[tokio::test]
async fn checkpoint_uses_declared_metadata_representations(
    #[values(false, true)] compatible: bool,
    #[values(false, true)] write_json: bool,
    #[values(false, true)] write_struct: bool,
    #[values(false, true)] parsed_partitions: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
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
    let available = if compatible {
        writer.read_schema.clone()
    } else {
        build_checkpoint_read_schema(
            &writer.schema_context.checkpoint_base_schema,
            &schema! { nullable "minValues": { nullable "value": STRING } },
            Some(&schema! { nullable "part_long": STRING }),
        )?
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
            assert_eq!(stats["minValues"]["value"], id);
        }
        if write_struct {
            assert_eq!(row["add"]["stats_parsed"]["minValues"]["value"], id);
        }
        if parsed_partitions {
            assert_eq!(row["add"]["partitionValues_parsed"]["part_long"], id);
        }
    }
    writer.schema_context.stats_config.write_stats_as_json = write_json;
    writer.schema_context.stats_config.write_stats_as_struct = write_struct;
    let plan = writer.declarative_checkpoint_data_plan(&engine)?;
    let actual = execute(&engine, plan)?;
    if compatible {
        let expected = writer
            .checkpoint_data(&engine)?
            .map(|data| selected_batch(data?))
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(
            json_rows(&actual, true)?,
            json_rows(&select_checkpoint_actions(&expected, ActionSet::ALL)?, true)?
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
            assert_eq!(row["add"]["stats_parsed"]["minValues"]["value"], id);
            assert_eq!(row["add"]["partitionValues_parsed"]["part_long"], id);
        }
        if write_json {
            let stats: Value = serde_json::from_str(row["add"]["stats"].as_str().unwrap())?;
            assert_eq!(stats["minValues"]["value"], id);
        }
    }
    assert_eq!(
        ["add", "protocol", "metaData"].map(|kind| actual_rows
            .iter()
            .filter(|row| !row[kind].is_null())
            .count()),
        [3, 1, 1]
    );

    Ok(())
}

#[rstest]
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
    let expected = existing_checkpoint_data(&writer, &engine, i64::MIN, None)?;
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
