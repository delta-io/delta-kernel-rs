use std::sync::Arc;

use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::action_reconciliation::file_metadata::{
    build_checkpoint_read_schema, build_checkpoint_transform,
};
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::action_reconciliation::plan::VERSION;
use crate::action_reconciliation::plan::sources::crc_actions;
use crate::actions::ActionKind;
use crate::arrow::array::StringArray;
use crate::arrow::json::WriterBuilder;
use crate::arrow::json::writer::JsonArray;
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::plans::Operation;
use crate::plans::ir::nodes::Operator;
use crate::scan::{PartitionValuesOptions, StatsOptions};
use crate::schema::{StructType, schema};
use crate::unit_test_utils::copy_test_table;
use crate::{FilteredEngineData, Snapshot};

fn selected_batch(data: FilteredEngineData) -> KernelResult<RecordBatch> {
    data.apply_selection_vector()?.try_into_record_batch()
}

fn rows(batches: &[RecordBatch]) -> KernelResult<Vec<Value>> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    let mut rows: Vec<Value> = serde_json::from_slice(&writer.into_inner())?;
    rows.iter_mut().for_each(Value::sort_all_objects);
    rows.sort_by_cached_key(Value::to_string);
    Ok(rows)
}

fn execute(engine: &dyn Engine, plan: Plan) -> KernelResult<Vec<RecordBatch>> {
    engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(|batch| batch?.try_into_record_batch())
        .collect()
}

#[rstest]
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
    #[values(ReplayStrategy::AggregateAll, ReplayStrategy::AggregateTailAndAntiJoin)]
    strategy: ReplayStrategy,
) -> KernelResult<()> {
    let (table_root, _tempdir) = copy_test_table(table)?;
    let engine = Arc::new(SyncEngine::new());
    let mut snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    if with_tail {
        let version = snapshot.version() + 1;
        let path = snapshot
            .log_segment()
            .log_root
            .join(&format!("{version:020}.json"))?;
        std::fs::write(
            path.to_file_path().unwrap(),
            add("tail.parquet", None).to_string(),
        )?;
        snapshot = Snapshot::builder_for(snapshot.table_root().clone()).build(engine.as_ref())?;
        assert_eq!(snapshot.version(), version);
    }
    let mut writer = snapshot.create_checkpoint_writer(engine.as_ref())?;
    writer.schema_context.stats_config.write_stats_as_json = json_stats;
    writer.schema_context.stats_config.write_stats_as_struct = struct_stats;
    let expected = writer
        .checkpoint_data(engine.as_ref())?
        .map(|data| selected_batch(data?))
        .collect::<KernelResult<Vec<_>>>()?;
    let actual = execute(
        engine.as_ref(),
        writer.checkpoint_plan_with_cutoffs(
            engine.as_ref(),
            writer.deleted_file_retention_timestamp()?,
            writer.get_transaction_expiration_timestamp()?,
            strategy,
        )?,
    )?;
    assert_eq!(
        StructType::try_from_arrow(actual[0].schema())?,
        StructType::try_from_arrow(expected[0].schema())?,
        "{table}"
    );
    assert_eq!(rows(&actual)?, rows(&expected)?, "{table}");
    Ok(())
}

// A fixed-cutoff oracle using the existing imperative checkpoint replay and stats transform.
fn existing_checkpoint_with_cutoffs(
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
    if context.is_v2 {
        batches.push(selected_batch(
            writer
                .create_checkpoint_metadata_batch(engine, &schema)?
                .filtered_data,
        )?);
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

async fn commit(store: &InMemory, version: u64, actions: Vec<Value>) -> KernelResult<()> {
    let content = actions
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    store
        .put(
            &Path::from(format!("_delta_log/{version:020}.json")),
            content.into(),
        )
        .await?;
    Ok(())
}

#[rstest]
#[case::v1(false, false)]
#[case::v2_inline(true, false)]
#[case::v2_sidecars(true, true)]
#[tokio::test]
async fn checkpoint_plan_reconciles_all_groups_and_retains_after_replay(
    #[case] v2: bool,
    #[case] sidecars: bool,
    #[values(false, true)] structured_stats: bool,
    #[values(None, Some(100i64))] txn_cutoff: Option<i64>,
    #[values(ReplayStrategy::AggregateAll, ReplayStrategy::AggregateTailAndAntiJoin)]
    strategy: ReplayStrategy,
) -> KernelResult<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let schema = crate::schema::schema_ref! { nullable "id": LONG };
    let mut metadata = json!({"id":"old","format":{"provider":"parquet","options":{}},
        "schemaString":serde_json::to_string(schema.as_ref())?,"partitionColumns":[],
        "configuration":{"delta.checkpoint.writeStatsAsStruct":structured_stats.to_string()}});
    let mut reader_features = vec!["deletionVectors"];
    let mut writer_features = vec!["deletionVectors", "domainMetadata"];
    if v2 {
        reader_features.push("v2Checkpoint");
        writer_features.push("v2Checkpoint");
    }
    let mut protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,
        "readerFeatures":reader_features,"writerFeatures":writer_features}});
    commit(
        &store,
        0,
        vec![
            protocol.clone(),
            json!({"metaData":metadata}),
            add("expired.parquet", None),
            add("recent.parquet", None),
            add("keep.parquet", None),
            add("same-path.parquet", Some(0)),
            add("same-path.parquet", Some(10)),
            json!({"txn":{"appId":"app","version":50,"lastUpdated":999}}),
            json!({"domainMetadata":{"domain":"deleted","configuration":"old","removed":false}}),
            json!({"domainMetadata":{"domain":"updated","configuration":"old","removed":false}}),
        ],
    )
    .await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let spec = if sidecars {
        CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        })
    } else if v2 {
        CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)
    } else {
        CheckpointSpec::V1
    };
    snapshot.checkpoint(&engine, Some(&spec))?;

    metadata["id"] = json!("new");
    protocol["protocol"]["writerFeatures"]
        .as_array_mut()
        .unwrap()
        .push(json!("appendOnly"));
    commit(
        &store,
        1,
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
    )
    .await?;
    // Repeated removes select the payload from the newest commit, even if that winner expires.
    commit(
        &store,
        2,
        vec![
            json!({"remove":{"path":"recent.parquet","deletionTimestamp":100,"dataChange":true}}),
            json!({"remove":{"path":"missing-timestamp.parquet","dataChange":true}}),
        ],
    )
    .await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(0));
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let expected = existing_checkpoint_with_cutoffs(&writer, &engine, 100, txn_cutoff)?;
    let actual = execute(
        &engine,
        writer.checkpoint_plan_with_cutoffs(&engine, 100, txn_cutoff, strategy)?,
    )?;
    assert_eq!(
        StructType::try_from_arrow(actual[0].schema())?,
        StructType::try_from_arrow(expected[0].schema())?
    );
    assert_eq!(rows(&actual)?, rows(&expected)?);
    let result = rows(&actual)?;
    let adds: Vec<_> = result
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    assert_eq!(adds.len(), 2);
    assert!(adds.contains(&"keep.parquet"));
    assert!(adds.contains(&"same-path.parquet"));
    assert_eq!(
        result.iter().filter(|row| !row["remove"].is_null()).count(),
        1
    );
    assert_eq!(
        result
            .iter()
            .filter(|row| !row["protocol"].is_null())
            .count(),
        1
    );
    assert_eq!(
        result
            .iter()
            .filter(|row| !row["metaData"].is_null())
            .count(),
        1
    );
    assert!(result.iter().any(|row| row["metaData"]["id"] == "new"));
    assert!(result.iter().any(|row| row["txn"]["appId"] == "legacy"));
    assert_eq!(
        result
            .iter()
            .filter(|row| row["txn"]["appId"] == "app")
            .count(),
        usize::from(txn_cutoff.is_none())
    );
    assert_eq!(
        result
            .iter()
            .filter(|row| !row["domainMetadata"].is_null())
            .count(),
        1
    );
    assert!(
        result
            .iter()
            .any(|row| row["domainMetadata"]["configuration"] == "new")
    );

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
    let reloaded = Snapshot::builder_for("memory:///").build(&engine)?;
    assert_eq!(reloaded.log_segment().checkpoint_version, Some(2));
    let reloaded_writer = reloaded.create_checkpoint_writer(&engine)?;
    assert_eq!(
        rows(&existing_checkpoint_with_cutoffs(
            &reloaded_writer,
            &engine,
            100,
            txn_cutoff
        )?)?,
        result
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn checkpoint_and_scan_share_native_metadata_fallback(
    #[values(false, true)] compatible: bool,
    #[values(false, true)] write_json: bool,
    #[values(false, true)] write_struct: bool,
    #[values(ReplayStrategy::AggregateAll, ReplayStrategy::AggregateTailAndAntiJoin)]
    strategy: ReplayStrategy,
) -> KernelResult<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let table_schema = schema! { nullable "id": LONG, nullable "part": LONG };
    let metadata = json!({"metaData": {
        "id":"typed-metadata", "format":{"provider":"parquet","options":{}},
        "schemaString":serde_json::to_string(&table_schema)?, "partitionColumns":["part"],
        "configuration":{"delta.checkpoint.writeStatsAsJson":"true",
            "delta.checkpoint.writeStatsAsStruct":"true"}
    }});
    let protocol = json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}});
    commit(&store, 0, vec![protocol.clone(), metadata.clone()]).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let stats = |id: i64| {
        json!({"numRecords":1,"minValues":{"id":id},
        "maxValues":{"id":id},"nullCount":{"id":0}})
    };
    let file = |path: &str, id: i64| {
        let mut row = add(path, None);
        row["add"]["partitionValues"] = json!({"part":id.to_string()});
        row["add"]["stats"] = json!(stats(id).to_string());
        row
    };
    let mut native = file("native.parquet", 10);
    native["add"]["stats_parsed"] = if compatible {
        stats(10)
    } else {
        json!({"minValues":{"id":"incompatible"}})
    };
    native["add"]["partitionValues_parsed"] = if compatible {
        json!({"part":10})
    } else {
        json!({"part":"incompatible"})
    };
    // Within one compatible physical schema, some rows have native metadata and others need
    // fallback. A native-only row must also support JSON synthesis.
    if compatible {
        native["add"]["stats"] = Value::Null;
    }
    let available = if compatible {
        writer.read_schema.clone()
    } else {
        build_checkpoint_read_schema(
            &writer.schema_context.checkpoint_base_schema,
            &schema! { nullable "minValues": { nullable "id": STRING } },
            Some(&schema! { nullable "part": STRING }),
        )?
    };
    let input = StringArray::from(vec![
        protocol.to_string(),
        metadata.to_string(),
        native.to_string(),
        file("fallback.parquet", 20).to_string(),
    ]);
    let batch = parse_json_impl(&input, available)?;
    engine.parquet_handler().write_parquet_file(
        writer.checkpoint_path()?,
        Box::new(std::iter::once(Ok(
            Box::new(ArrowEngineData::new(batch)) as Box<dyn crate::EngineData>
        ))),
    )?;
    commit(&store, 1, vec![file("tail.parquet", 30)]).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let mut writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let options = ActionOptions {
        actions: [
            ActionKind::FileActions {
                includes_removes: true,
            },
            ActionKind::Protocol,
            ActionKind::Metadata,
        ]
        .into_iter()
        .collect(),
        json_stats: true,
        parsed_stats: Some(writer.schema_context.stats_schema.clone()),
        parsed_partition_values: true,
    };
    let shape = CheckpointShape::try_new_with_leaf_schema(
        engine.require_plan_executor()?.as_ref(),
        &snapshot,
    )?;
    let base = checkpoint_actions(
        snapshot.log_segment(),
        Some(&shape),
        Some(snapshot.table_configuration()),
        &options,
    )?;
    assert!(base.1.actions.contains(&ActionKind::FileActions {
        includes_removes: true
    }));
    assert_eq!(base.1.parsed_stats.is_some(), compatible);
    assert!(base.1.json_stats);
    assert_eq!(base.1.parsed_partition_values, compatible);
    let crc = crc_actions(0, vec![], &options)?;
    assert_eq!(
        crc.1.actions,
        [ActionKind::FileActions {
            includes_removes: false
        }]
        .into_iter()
        .collect::<Vec<_>>()
    );
    let padded = project_action_schema(crc.0, writer.read_schema.clone())?;
    assert!(padded.schema().contains("remove"));
    assert!(padded.schema().contains("sidecar"));
    let tail = commit_actions(snapshot.log_segment(), Some(0), &options)?;
    assert!(tail.1.json_stats);
    assert!(tail.1.parsed_stats.is_none());
    assert!(!tail.1.parsed_partition_values);
    // No caller-side schema alignment is needed for native checkpoints and JSON commits.
    let state = replay(tail, base, strategy)?;
    assert_eq!(state.1.parsed_stats.is_some(), compatible);
    assert_eq!(state.1.parsed_partition_values, compatible);
    assert!(state.1.actions.contains(&ActionKind::FileActions {
        includes_removes: true
    }));
    let state = parse_file_metadata(&snapshot, state, &options, false)?;
    assert!(state.1.parsed_stats.is_some());
    assert!(state.1.parsed_partition_values);
    assert!(state.1.actions.contains(&ActionKind::Protocol));
    assert!(state.1.actions.contains(&ActionKind::Metadata));
    for row in rows(&execute(&engine, state.0.build()?)?)? {
        let expected_version = if row["add"]["path"] == "tail.parquet" {
            1
        } else {
            0
        };
        assert_eq!(row[VERSION], expected_version);
    }
    writer.schema_context.stats_config.write_stats_as_json = write_json;
    writer.schema_context.stats_config.write_stats_as_struct = write_struct;
    let plan = writer.checkpoint_plan_with_cutoffs(
        &engine,
        writer.deleted_file_retention_timestamp()?,
        writer.get_transaction_expiration_timestamp()?,
        strategy,
    )?;
    let actual = execute(&engine, plan)?;
    if compatible {
        let expected = writer
            .checkpoint_data(&engine)?
            .map(|data| selected_batch(data?))
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(rows(&actual)?, rows(&expected)?);
    }
    let actual_rows = rows(&actual)?;
    for (path, id) in [
        ("native.parquet", 10),
        ("fallback.parquet", 20),
        ("tail.parquet", 30),
    ] {
        let row = actual_rows
            .iter()
            .find(|row| row["add"]["path"] == path)
            .unwrap();
        if write_struct {
            assert_eq!(row["add"]["stats_parsed"]["minValues"]["id"], id);
            assert_eq!(row["add"]["partitionValues_parsed"]["part"], id);
        }
        if write_json {
            let stats: Value = serde_json::from_str(row["add"]["stats"].as_str().unwrap())?;
            assert_eq!(stats["minValues"]["id"], id);
        }
    }
    assert_eq!(
        actual_rows
            .iter()
            .filter(|row| !row["add"].is_null())
            .count(),
        3
    );
    assert_eq!(
        actual_rows
            .iter()
            .filter(|row| !row["protocol"].is_null())
            .count(),
        1
    );
    assert_eq!(
        actual_rows
            .iter()
            .filter(|row| !row["metaData"].is_null())
            .count(),
        1
    );

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .with_predicate(Arc::new(col!("id").gt(lit(15i64))))
        .build()?;
    let scan_rows = rows(&execute(
        &engine,
        scan.declarative_metadata_scan_plan(&engine)?.unwrap(),
    )?)?;
    assert_eq!(scan_rows.len(), 2);
    for (path, id) in [("fallback.parquet", 20), ("tail.parquet", 30)] {
        let row = scan_rows
            .iter()
            .find(|row| row["add"]["path"] == path)
            .unwrap();
        assert_eq!(row["add"]["stats_parsed"]["minValues"]["id"], id);
        assert_eq!(row["add"]["partitionValues_parsed"]["part"], id);
    }
    Ok(())
}

#[rstest]
#[case("v2-checkpoints-json-with-sidecars")]
#[case("v2-checkpoints-parquet-with-sidecars")]
fn non_file_checkpoint_source_never_reads_sidecars(#[case] table: &str) -> KernelResult<()> {
    let (table_root, _tempdir) = copy_test_table(table)?;
    let engine = SyncEngine::new();
    let latest = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    let version = latest
        .log_segment()
        .checkpoint_version
        .expect("fixture checkpoint");
    let snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(version)
        .build(&engine)?;
    let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
    let names = ["protocol", "metaData", "txn", "domainMetadata"];
    let expected = writer
        .checkpoint_data(&engine)?
        .map(|data| selected_batch(data?))
        .collect::<KernelResult<Vec<_>>>()?;
    let mut expected: Vec<_> = rows(&expected)?
        .into_iter()
        .filter(|row| names.iter().any(|name| !row[name].is_null()))
        .map(|row| {
            let mut result = serde_json::Map::new();
            for name in names {
                result.insert(name.into(), row[name].clone());
            }
            result.insert(VERSION.into(), json!(version));
            Value::Object(result)
        })
        .collect();
    expected.sort_by_cached_key(Value::to_string);
    assert!(!expected.is_empty());
    let shape = CheckpointShape::try_new(engine.require_plan_executor()?.as_ref(), &snapshot)?;
    assert_eq!(
        shape.checkpoint_type,
        crate::checkpoint::CheckpointType::Manifest
    );
    // Make an accidental sidecar read fail even if a plan-shape assertion missed it.
    std::fs::remove_dir_all(
        table_root
            .join("_delta_log/_sidecars/")?
            .to_file_path()
            .unwrap(),
    )?;
    let options = ActionOptions {
        actions: [
            ActionKind::Protocol,
            ActionKind::Metadata,
            ActionKind::SetTransaction,
            ActionKind::DomainMetadata,
        ]
        .into_iter()
        .collect(),
        json_stats: false,
        parsed_stats: None,
        parsed_partition_values: false,
    };
    let source = checkpoint_actions(snapshot.log_segment(), None, None, &options)?;
    assert_eq!(source.1, options);
    let plan = source.0.build()?;
    for node in &plan.nodes {
        let files = match &node.op {
            Operator::DynamicScan(_) => panic!("non-file read discovered sidecars"),
            Operator::ScanJson(scan) => &scan.files,
            Operator::ScanParquet(scan) => &scan.files,
            _ => continue,
        };
        assert!(
            files
                .iter()
                .all(|file| !file.meta.location.as_str().contains("_sidecars/"))
        );
    }
    assert_eq!(rows(&execute(&engine, plan)?)?, expected);
    Ok(())
}

#[test]
fn parsed_partition_option_uses_snapshot_physical_names() -> KernelResult<()> {
    let (table_root, _tempdir) = copy_test_table("partition_cm/name")?;
    let engine = SyncEngine::new();
    let initial = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    let partition_schema = initial
        .table_configuration()
        .build_partition_values_parsed_schema()
        .unwrap();
    let physical_name = partition_schema.fields().next().unwrap().name();
    assert_ne!(physical_name, "category");
    let action = json!({"add": {
        "path": "a.parquet", "partitionValues": {physical_name: "US"}, "size": 1,
        "modificationTime": 1, "dataChange": true,
    }});
    std::fs::write(
        table_root
            .join("_delta_log/00000000000000000001.json")?
            .to_file_path()
            .unwrap(),
        action.to_string(),
    )?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let options = ActionOptions {
        actions: [ActionKind::FileActions {
            includes_removes: false,
        }]
        .into_iter()
        .collect(),
        json_stats: false,
        parsed_stats: None,
        parsed_partition_values: true,
    };
    let source = commit_actions(snapshot.log_segment(), Some(0), &options)?;
    assert!(!source.1.parsed_partition_values);
    let parsed = parse_file_metadata(&snapshot, source, &options, false)?;
    assert!(parsed.1.parsed_partition_values);
    let field = parsed
        .0
        .schema()
        .field_at(&crate::expressions::column_name!(
            "add.partitionValues_parsed"
        ))?;
    assert_eq!(
        field.data_type(),
        &crate::schema::DataType::from(partition_schema.clone())
    );
    let actual = rows(&execute(&engine, parsed.0.build()?)?)?;
    assert_eq!(actual.len(), 1);
    assert_eq!(
        actual[0]["add"]["partitionValues_parsed"][physical_name],
        "US"
    );
    Ok(())
}
