use super::{LogReplayScanner, TableChangesScanData};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{Add, Cdc, CommitInfo, Metadata, Protocol, Remove};
use crate::engine::sync::SyncEngine;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::scan::state::DvInfo;
use crate::schema::{DataType, StructField, StructType};
use crate::utils::test_utils::MockTable;
use crate::{DeltaResult, Engine, Error, Version};

use itertools::Itertools;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

fn get_schema() -> StructType {
    StructType::new([
        StructField::new("id", DataType::LONG, true),
        StructField::new("value", DataType::STRING, true),
    ])
}

fn get_segment(
    engine: &dyn Engine,
    path: &Path,
    start_version: Version,
    end_version: impl Into<Option<Version>>,
) -> DeltaResult<Vec<ParsedLogPath>> {
    let table_root = url::Url::from_directory_path(path).unwrap();
    let log_root = table_root.join("_delta_log/")?;
    let log_segment = LogSegment::for_table_changes(
        engine.get_file_system_client().as_ref(),
        log_root,
        start_version,
        end_version,
    )?;
    Ok(log_segment.ascending_commit_files)
}

fn get_commit_log_scanner(commit: ParsedLogPath) -> LogReplayScanner {
    LogReplayScanner::new(commit, None, get_schema().into())
}
fn result_to_sv(iter: impl Iterator<Item = DeltaResult<TableChangesScanData>>) -> Vec<bool> {
    iter.map_ok(|scan_data| scan_data.selection_vector.into_iter())
        .flatten_ok()
        .try_collect()
        .unwrap()
}

#[tokio::test]
async fn metadata_protocol() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = MockTable::new();
    let schema_string = serde_json::to_string(&get_schema()).unwrap();
    mock_table
        .commit(&[
            Metadata {
                schema_string,
                configuration: HashMap::from([
                    ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                    (
                        "delta.enableDeletionVectors".to_string(),
                        "true".to_string(),
                    ),
                ]),
                ..Default::default()
            }
            .into(),
            Protocol::try_new(3, 7, Some(["deletionVectors"]), Some(["deletionVectors"]))
                .unwrap()
                .into(),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let mut scanner = get_commit_log_scanner(commits.next().unwrap());

    scanner.prepare_phase(engine.as_ref()).unwrap();
    assert!(!scanner.has_cdc_action);
    assert!(scanner.remove_dvs.is_empty());

    assert_eq!(
        result_to_sv(scanner.into_scan_batches(engine.clone()).unwrap()),
        &[false, false]
    );
}
#[tokio::test]
async fn configuration_fails() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = MockTable::new();
    let schema_string = serde_json::to_string(&get_schema()).unwrap();
    mock_table
        .commit(&[Metadata {
            schema_string,
            configuration: HashMap::from([(
                "delta.enableDeletionVectors".to_string(),
                "true".to_string(),
            )]),
            ..Default::default()
        }
        .into()])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let mut scanner = get_commit_log_scanner(commits.next().unwrap());

    assert!(matches!(
        scanner.prepare_phase(engine.as_ref()),
        Err(Error::ChangeDataFeedUnsupported(_))
    ));
}

#[tokio::test]
async fn incompatible_schema() {
    let engine = SyncEngine::new();
    let mut mock_table = MockTable::new();

    // The original schema has two fields: `id` and value.
    let schema = get_schema().project(&["id"]).unwrap();
    let schema_string = serde_json::to_string(&schema).unwrap();
    mock_table
        .commit(&[Metadata {
            schema_string,
            configuration: HashMap::from([
                ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                (
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                ),
            ]),
            ..Default::default()
        }
        .into()])
        .await;

    let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let mut scanner = get_commit_log_scanner(commits.next().unwrap());

    assert!(matches!(
        scanner.prepare_phase(&engine),
        Err(Error::ChangeDataFeedIncompatibleSchema(_, _))
    ));
}

#[tokio::test]
async fn add_remove() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = MockTable::new();
    mock_table
        .commit(&[
            Add {
                path: "fake_path_1".into(),
                ..Default::default()
            }
            .into(),
            Remove {
                path: "fake_path_2".into(),
                ..Default::default()
            }
            .into(),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let mut scanner = get_commit_log_scanner(commit);

    scanner.prepare_phase(engine.as_ref()).unwrap();
    assert!(!scanner.has_cdc_action);
    assert_eq!(scanner.remove_dvs, HashMap::new());

    assert_eq!(
        result_to_sv(scanner.into_scan_batches(engine).unwrap()),
        &[true, true]
    );
}

#[tokio::test]
async fn cdc_selection() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = MockTable::new();
    mock_table
        .commit(&[
            Add {
                path: "fake_path_1".into(),
                ..Default::default()
            }
            .into(),
            Remove {
                path: "fake_path_2".into(),
                ..Default::default()
            }
            .into(),
            Cdc {
                path: "fake_path_3".into(),
                ..Default::default()
            }
            .into(),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let mut scanner = get_commit_log_scanner(commit);

    scanner.prepare_phase(engine.as_ref()).unwrap();
    assert!(scanner.has_cdc_action);
    assert_eq!(scanner.remove_dvs, HashMap::new());

    assert_eq!(
        result_to_sv(scanner.into_scan_batches(engine).unwrap()),
        &[false, false, true]
    );
}

#[tokio::test]
async fn dv() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = MockTable::new();

    let deletion_vector1 = DeletionVectorDescriptor {
        storage_type: "u".to_string(),
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    };
    let deletion_vector2 = DeletionVectorDescriptor {
        storage_type: "u".to_string(),
        path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
        offset: Some(1),
        size_in_bytes: 38,
        cardinality: 3,
    };
    mock_table
        .commit(&[
            Add {
                path: "fake_path_1".into(),
                ..Default::default()
            }
            .into(),
            Remove {
                path: "fake_path_1".into(),
                deletion_vector: Some(deletion_vector1.clone()),
                ..Default::default()
            }
            .into(),
            Remove {
                path: "fake_path_2".into(),
                deletion_vector: Some(deletion_vector2.clone()),
                ..Default::default()
            }
            .into(),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let mut scanner = get_commit_log_scanner(commit);

    scanner.prepare_phase(engine.as_ref()).unwrap();
    assert!(!scanner.has_cdc_action);
    assert_eq!(
        scanner.remove_dvs,
        HashMap::from([(
            "fake_path_1".to_string(),
            DvInfo {
                deletion_vector: Some(deletion_vector1.clone())
            }
        )])
    );

    assert_eq!(
        result_to_sv(scanner.into_scan_batches(engine).unwrap()),
        &[true, false, true]
    );
}
#[tokio::test]
async fn failing_protocol() {
    let engine = SyncEngine::new();
    let mut mock_table = MockTable::new();

    let protocol = Protocol::try_new(
        3,
        1,
        ["fake_feature".to_string()].into(),
        ["fake_feature".to_string()].into(),
    )
    .unwrap();

    mock_table
        .commit(&[
            Add {
                path: "fake_path_1".into(),
                ..Default::default()
            }
            .into(),
            Remove {
                path: "fake_path_2".into(),
                ..Default::default()
            }
            .into(),
            protocol.into(),
        ])
        .await;

    let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let mut scanner = get_commit_log_scanner(commit);

    assert!(scanner.prepare_phase(&engine).is_err());
}
#[tokio::test]
async fn in_commit_timestamp() {
    let engine = SyncEngine::new();
    let mut mock_table = MockTable::new();

    let timestamp = 123456;
    mock_table
        .commit(&[
            Add {
                path: "fake_path_1".into(),
                ..Default::default()
            }
            .into(),
            CommitInfo {
                timestamp: Some(timestamp),
                ..Default::default()
            }
            .into(),
        ])
        .await;

    let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let mut scanner = get_commit_log_scanner(commit);

    scanner.prepare_phase(&engine).unwrap();
    assert_eq!(scanner.timestamp, timestamp);
}

#[tokio::test]
async fn file_meta_timestamp() {
    let engine = SyncEngine::new();
    let mut mock_table = MockTable::new();

    mock_table
        .commit(&[Add {
            path: "fake_path_1".into(),
            ..Default::default()
        }
        .into()])
        .await;

    let mut commits = get_segment(&engine, mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let file_meta_ts = commit.location.last_modified;
    let mut scanner = get_commit_log_scanner(commit);

    scanner.prepare_phase(&engine).unwrap();
    assert_eq!(scanner.timestamp, file_meta_ts);
}
