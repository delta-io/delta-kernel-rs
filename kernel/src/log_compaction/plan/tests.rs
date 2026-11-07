use ::test_utils::table_builder::{FeatureSet, LogState, TestTable, TestTableBuilder};
use ::test_utils::{add_commit, assert_batches_sorted_eq, sort_lines};
use itertools::Itertools as _;
use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
use crate::arrow::array::StringArray;
use crate::arrow::util::pretty::pretty_format_batches;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::log_replay::ActionsBatch;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::path::Path;
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::unit_test_utils::copy_test_table;
use crate::unit_test_utils::plans::{execute_plan, json_rows};

fn write_commit(table: &TestTable, version: Version, actions: &[Value]) {
    futures::executor::block_on(add_commit(
        table.table_root(),
        table.store().as_ref(),
        version,
        actions.iter().join("\n"),
    ))
    .unwrap();
}

fn add(path: &str, offset: Option<i32>) -> Value {
    let mut action = json!({"add": {"path": path, "partitionValues": {}, "size": 10,
        "modificationTime": 1, "dataChange": true, "tags": {"keep": "yes"},
        "stats": " { \"numRecords\": 2, \"unindexed\": 7 }"}});
    if let Some(offset) = offset {
        action["add"]["deletionVector"] = json!({"storageType": "p",
            "pathOrInlineDv": "file:///dv.bin", "offset": offset, "sizeInBytes": 1,
            "cardinality": 1});
    }
    action
}

fn remove(add: &Value, timestamp: i64) -> Value {
    let mut value = add["add"].clone();
    value.as_object_mut().unwrap().remove("stats");
    value.as_object_mut().unwrap().remove("modificationTime");
    value["deletionTimestamp"] = json!(timestamp);
    json!({"remove": value})
}

fn normalize_compaction_rows(mut state: Vec<Value>) -> Vec<Value> {
    for action in &mut state {
        // Compaction changes dataChange without changing reconstructed table state.
        for kind in [ADD_NAME, REMOVE_NAME] {
            if action[kind].is_object() {
                action[kind]["dataChange"] = json!(false);
            }
        }
        action.sort_all_objects();
    }
    state.sort_by_cached_key(Value::to_string);
    state
}

#[test]
fn compaction_matches_existing_compaction_intervals() -> KernelResult<()> {
    let (root, _tempdir) = copy_test_table("compacted-log-files-table")?;
    let log = root.join("_delta_log/")?.to_file_path().unwrap();
    let mut compactions = vec![];
    for entry in std::fs::read_dir(log)? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if name.ends_with(".compacted.json") {
            let mut versions = name.split('.');
            let start = versions.next().unwrap().parse::<Version>().unwrap();
            let end = versions.next().unwrap().parse::<Version>().unwrap();
            compactions.push((start, end, std::fs::read_to_string(&path)?));
            // Force reconstruction from the original commits, not the stored compaction.
            std::fs::remove_file(path)?;
        }
    }
    assert!(!compactions.is_empty());
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(root).build(&engine)?;
    for (start, end, json) in compactions {
        let plan = snapshot.declarative_log_compaction_data_plan(&engine, start, end)?;
        // These fixtures concatenate interval actions, including cancelled adds and CDC rows.
        // Reconcile their contents with the existing processor before comparing compacted output.
        let schema = super::super::COMPACTION_ACTIONS_SCHEMA.clone();
        let batch = parse_json_impl(
            &StringArray::from(json.lines().collect::<Vec<_>>()),
            schema.clone(),
        )?;
        let expected = ActionReconciliationProcessor::new(i64::MIN, None, None)
            .process_actions_batch(ActionsBatch::new(
                Box::new(ArrowEngineData::new(batch)),
                true,
            ))?
            .filtered_data
            .apply_selection_vector()?;
        let (output_schema, transform) = ProjectionStructPatchBuilder::new(&schema)
            .drop("sidecar")
            .replace_expr_at([ADD_NAME], "dataChange", lit(false))
            .replace_expr_at([REMOVE_NAME], "dataChange", lit(false))
            .build()?;
        assert_eq!(output_schema, plan.schema);
        let expected = engine
            .evaluation_handler()
            .new_expression_evaluator(schema, transform, output_schema.into())?
            .evaluate(expected.as_ref())?
            .try_into_record_batch()?;
        let formatted = pretty_format_batches(&[expected])?.to_string();
        let mut expected = formatted.lines().collect::<Vec<_>>();
        sort_lines!(expected);
        assert_batches_sorted_eq!(expected, &execute_plan(&engine, plan)?);
    }
    Ok(())
}

#[test]
fn compaction_preserves_interval_winners_without_retention() -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_features(
            FeatureSet::new()
                .deletion_vectors()
                .domain_metadata()
                .change_data_feed()
                .with_property("delta.deletedFileRetentionDuration", "interval 100000 days")
                .with_property(
                    "delta.setTransactionRetentionDuration",
                    "interval 1 milliseconds",
                ),
        )
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let initial = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let protocol = json!({"protocol": initial.table_configuration().protocol()});
    let cancelled = add("cancelled", None);
    let dv0 = add("dv", Some(0));
    let dv1 = add("dv", Some(1));
    let readded = add("readded", None);
    let txn = |version| json!({"txn":{"appId":"app","version":version,"lastUpdated":0}});
    let timeless_txn = json!({"txn":{"appId":"other","version":1}});
    let domain = |removed| {
        json!({"domainMetadata":{"domain":"test",
        "configuration":"configuration","removed":removed}})
    };
    // The builder cannot repeat protocol actions or create these file, txn, and domain histories.
    let commits = [
        vec![
            protocol.clone(),
            cancelled.clone(),
            dv0.clone(),
            dv1.clone(),
            txn(10),
            domain(false),
            remove(&readded, 0),
        ],
        vec![
            protocol.clone(),
            remove(&cancelled, 0),
            remove(&dv0, 0),
            readded.clone(),
            txn(9),
            domain(true),
            json!({"commitInfo":{"operation":"WRITE"}}),
            json!({"cdc":{"path":"ignored","size":1,"partitionValues":{},"dataChange":false}}),
        ],
        vec![
            remove(&cancelled, 1),
            timeless_txn.clone(),
            json!({"remove":{"path":"no-time","dataChange":true}}),
        ],
    ];
    let expected = vec![
        protocol,
        remove(&cancelled, 1),
        remove(&dv0, 0),
        dv1,
        readded.clone(),
        txn(9),
        timeless_txn,
        domain(true),
        json!({"remove":{"path":"no-time","dataChange":false}}),
    ];
    for (version, actions) in commits.iter().enumerate() {
        write_commit(&table, version as Version + 4, actions);
    }
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let plan = snapshot.declarative_log_compaction_data_plan(&engine, 4, 6)?;
    let compacted = json_rows(&execute_plan(&engine, plan)?, false)?;
    assert_eq!(compacted, normalize_compaction_rows(expected));
    // Planning and execution must also work without commits outside the interval.
    for version in 0..4 {
        futures::executor::block_on(
            table
                .store()
                .delete(&Path::from(format!("_delta_log/{version:020}.json"))),
        )?;
    }
    let plan = snapshot.declarative_log_compaction_data_plan(&engine, 4, 6)?;
    assert_eq!(json_rows(&execute_plan(&engine, plan)?, false)?, compacted);
    Ok(())
}

#[rstest]
#[case::ignored_actions(1, 2, None, true)]
#[case::reversed(2, 1, None, false)]
#[case::single_version(1, 1, None, false)]
#[case::beyond_snapshot(0, 4, None, false)]
#[case::missing_start(0, 3, Some(0), false)]
#[case::missing_middle(0, 3, Some(1), false)]
#[case::missing_end(0, 3, Some(3), false)]
fn compaction_validates_interval(
    #[case] start: Version,
    #[case] end: Version,
    #[case] missing: Option<Version>,
    #[case] valid: bool,
) -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_data(0, 0)
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    if let Some(version) = missing {
        futures::executor::block_on(
            table
                .store()
                .delete(&Path::from(format!("_delta_log/{version:020}.json"))),
        )?;
    }
    let plan = snapshot.declarative_log_compaction_data_plan(&engine, start, end);
    if valid {
        assert!(json_rows(&execute_plan(&engine, plan?)?, false)?.is_empty());
    } else {
        assert!(plan.is_err());
    }
    Ok(())
}
