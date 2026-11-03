use std::sync::Arc;

use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::arrow::json::WriterBuilder;
use crate::arrow::json::writer::JsonArray;
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::plans::Operation;
use crate::unit_test_utils::load_test_table;
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
fn checkpoint_plan_matches_existing_checkpoint_data(#[case] table: &str) -> KernelResult<()> {
    let (engine, snapshot, _tempdir) = load_test_table(table)?;
    let writer = snapshot.create_checkpoint_writer(engine.as_ref())?;
    let expected = writer
        .checkpoint_data(engine.as_ref())?
        .map(|data| selected_batch(data?))
        .collect::<KernelResult<Vec<_>>>()?;
    let actual = execute(
        engine.as_ref(),
        writer.declarative_checkpoint_data_plan(engine.as_ref())?,
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
        writer.checkpoint_plan_with_cutoffs(&engine, 100, txn_cutoff)?,
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
