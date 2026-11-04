use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use super::*;
use crate::arrow::json::WriterBuilder;
use crate::arrow::json::writer::JsonArray;
use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::plans::Operation;
use crate::unit_test_utils::MockTableConfigurationBuilder;

fn fixture(commits: &[Vec<Value>]) -> KernelResult<(Snapshot, tempfile::TempDir)> {
    let temp = tempfile::tempdir()?;
    let root = Url::from_directory_path(temp.path()).unwrap();
    let log_root = root.join("_delta_log/")?;
    std::fs::create_dir(log_root.to_file_path().unwrap())?;
    for (version, actions) in commits.iter().enumerate() {
        std::fs::write(
            log_root
                .join(&format!("{version:020}.json"))?
                .to_file_path()
                .unwrap(),
            actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
    }
    let engine = SyncEngine::new();
    let segment = LogSegment::for_table_changes(
        engine.storage_handler().as_ref(),
        log_root,
        0,
        Some((commits.len() - 1) as Version),
    )?;
    let config = MockTableConfigurationBuilder::new()
        .with_table_root(root.as_str())
        .with_version(segment.end_version)
        .with_properties([(
            "delta.setTransactionRetentionDuration",
            "interval 1 milliseconds",
        )])
        .try_build()?;
    Ok((Snapshot::new(segment, config)?, temp))
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

fn output(snapshot: &Snapshot, start: Version, end: Version) -> KernelResult<Vec<Value>> {
    let engine = SyncEngine::new();
    let plan = snapshot.declarative_log_compaction_data_plan(&engine, start, end)?;
    let batches = engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(|data| data?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    rows(&batches)
}

fn rows(batches: &[RecordBatch]) -> KernelResult<Vec<Value>> {
    let mut writer = WriterBuilder::new().build::<_, JsonArray>(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    Ok(serde_json::from_slice(&writer.into_inner())?)
}

fn reconstructed_state(root: &Url) -> KernelResult<Vec<Value>> {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(root.clone()).build(&engine)?;
    let batches = snapshot
        .create_checkpoint_writer(&engine)?
        .checkpoint_data(&engine)?
        .map(|data| data?.apply_selection_vector()?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    Ok(normalize_compaction_rows(rows(&batches)?))
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

#[rstest]
fn compaction_retains_interval_winners_without_checkpoint_retention(
    #[values(false, true)] include_protocol_metadata: bool,
) -> KernelResult<()> {
    let stable = add("stable", None);
    let cancelled = add("cancelled", None);
    let dv0 = add("dv", Some(0));
    let dv1 = add("dv", Some(1));
    let readded = add("readded", None);
    let old_txn = json!({"txn": {"appId": "app", "version": 10, "lastUpdated": 0}});
    let txn = json!({"txn": {"appId": "app", "version": 9, "lastUpdated": 0}});
    let timeless_txn = json!({"txn": {"appId": "other", "version": 1}});
    let domain = json!({"domainMetadata": {"domain": "test", "configuration": "old",
        "removed": false}});
    let deleted_domain = json!({"domainMetadata": {"domain": "test", "configuration": "gone",
        "removed": true}});
    let mut commits = vec![
        vec![add("outside-before", None)],
        vec![
            stable.clone(),
            cancelled.clone(),
            dv0.clone(),
            dv1.clone(),
            old_txn,
            domain,
            remove(&readded, 0),
        ],
        vec![
            remove(&cancelled, 0),
            remove(&dv0, 0),
            readded.clone(),
            txn.clone(),
            deleted_domain.clone(),
            json!({"commitInfo": {"operation": "WRITE"}}),
            json!({"cdc": {"path": "ignored", "size": 1, "partitionValues": {},
                "dataChange": false}}),
        ],
        vec![
            remove(&cancelled, 1),
            timeless_txn.clone(),
            json!({"remove": {"path": "no-time", "dataChange": true}}),
        ],
        vec![add("outside-after", None)],
    ];
    let mut expected = vec![
        stable,
        remove(&cancelled, 1),
        remove(&dv0, 0),
        dv1,
        readded,
        txn,
        timeless_txn,
        deleted_domain,
        json!({"remove": {"path": "no-time", "dataChange": false}}),
    ];
    if include_protocol_metadata {
        let protocol = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
        let metadata = json!({"metaData": {"id": "winning-metadata", "format": {
            "provider": "parquet", "options": {}}, "schemaString": "{}", "partitionColumns": [],
            "configuration": {}}});
        commits[1].extend([protocol.clone(), metadata.clone()]);
        commits[1].last_mut().unwrap()["metaData"]["id"] = json!("older-metadata");
        commits[3].extend([protocol.clone(), metadata.clone()]);
        expected.extend([protocol, metadata]);
    }
    let (snapshot, _temp) = fixture(&commits)?;
    // Planning and execution must ignore unrelated commits, even if they later disappear.
    for version in [0, 4] {
        std::fs::remove_file(
            snapshot
                .log_segment()
                .log_root
                .join(&format!("{version:020}.json"))?
                .to_file_path()
                .unwrap(),
        )?;
    }
    assert_eq!(
        normalize_compaction_rows(output(&snapshot, 1, 3)?),
        normalize_compaction_rows(expected)
    );
    Ok(())
}

#[rstest]
fn compacted_interval_preserves_reconstructed_state(
    #[values(0, 1)] start: Version,
    #[values(false, true)] with_tail: bool,
) -> KernelResult<()> {
    let protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,
        "readerFeatures":["deletionVectors"],
        "writerFeatures":["deletionVectors","domainMetadata","changeDataFeed"]}});
    let schema = crate::schema::schema_ref! { nullable "id": LONG };
    let mut metadata = json!({"metaData":{"id":"compaction-round-trip","name":"before",
        "format":{"provider":"parquet","options":{}},
        "schemaString":serde_json::to_string(schema.as_ref())?,"partitionColumns":[],
        "configuration":{"delta.deletedFileRetentionDuration":"interval 100000 days",
            "delta.setTransactionRetentionDuration":"interval 100000 days"}}});
    let cancelled = add("cancelled", None);
    let readded = add("readded", None);
    let dv0 = add("same-path", Some(0));
    let dv1 = add("same-path", Some(10));
    let temporary = add("temporary", None);
    let txn = |version| json!({"txn":{"appId":"app","version":version,"lastUpdated":0}});
    let domain = |removed| {
        json!({"domainMetadata":{"domain":"domain",
        "configuration":"configuration","removed":removed}})
    };
    let mut commits = vec![
        vec![
            protocol,
            metadata.clone(),
            cancelled.clone(),
            readded.clone(),
            dv0.clone(),
            dv1,
            add("unchanged", None),
            txn(1),
            domain(false),
        ],
        vec![
            remove(&cancelled, 0),
            remove(&readded, 0),
            remove(&dv0, 0),
            temporary.clone(),
            txn(2),
            domain(true),
        ],
        vec![readded.clone(), remove(&temporary, 0), add("new", None)],
        vec![
            json!({"commitInfo":{"operation":"WRITE"}}),
            json!({"cdc":{"path":"ignored","size":1,"partitionValues":{},
                "dataChange":false}}),
        ],
    ];
    metadata["metaData"]["name"] = json!("after");
    commits[2].push(metadata.clone());
    if with_tail {
        metadata["metaData"]["name"] = json!("tail");
        commits.extend([
            vec![cancelled, remove(&readded, 1), dv0, txn(3), domain(false)],
            vec![metadata, add("tail", None)],
        ]);
    }
    let (snapshot, _original_dir) = fixture(&commits)?;
    let root = snapshot.table_root();
    let expected = reconstructed_state(root)?;
    let snapshot = Snapshot::builder_for(root.clone()).build(&SyncEngine::new())?;
    let compacted = output(&snapshot, start, 3)?;
    assert_eq!(
        compacted
            .iter()
            .filter(|a| a.get("protocol").is_some())
            .count(),
        usize::from(start == 0)
    );
    // Discovery of compacted files is disabled. Represent the replacement at the interval's
    // end version, with empty earlier commits, to exercise ordinary snapshot reconstruction.
    for version in start..3 {
        commits[version as usize] = vec![json!({"commitInfo":{}})];
    }
    commits[3] = compacted;
    let (compacted, _compacted_dir) = fixture(&commits)?;
    assert_eq!(reconstructed_state(compacted.table_root())?, expected);
    Ok(())
}

#[test]
fn compaction_of_only_ignored_actions_produces_no_rows() -> KernelResult<()> {
    let (snapshot, _temp) = fixture(&[
        vec![json!({"commitInfo":{"operation":"WRITE"}})],
        vec![
            json!({"cdc":{"path":"ignored","size":1,"partitionValues":{},
            "dataChange":false}}),
        ],
    ])?;
    assert!(output(&snapshot, 0, 1)?.is_empty());
    Ok(())
}

#[rstest]
#[case(2, 1, None)]
#[case(1, 1, None)]
#[case(0, 4, None)]
#[case(0, 3, Some(0))]
#[case(0, 3, Some(1))]
#[case(0, 3, Some(3))]
fn compaction_rejects_invalid_or_incomplete_interval(
    #[case] start: Version,
    #[case] end: Version,
    #[case] missing: Option<Version>,
) -> KernelResult<()> {
    let (snapshot, _temp) = fixture(&vec![vec![]; 4])?;
    if let Some(version) = missing {
        std::fs::remove_file(
            snapshot
                .log_segment()
                .log_root
                .join(&format!("{version:020}.json"))?
                .to_file_path()
                .unwrap(),
        )?;
    }
    assert!(
        snapshot
            .declarative_log_compaction_data_plan(&SyncEngine::new(), start, end)
            .is_err()
    );
    Ok(())
}
