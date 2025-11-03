use super::table_changes_action_iter;
use super::TableChangesScanMetadata;
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::actions::{Add, Cdc, Metadata, Protocol, Remove};
use crate::engine::sync::SyncEngine;
use crate::expressions::{column_expr, BinaryPredicateOp, Scalar};
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::scan::state::DvInfo;
use crate::scan::PhysicalPredicate;
use crate::schema::{DataType, StructField, StructType};
use crate::table_changes::log_replay::LogReplayScanner;
use crate::table_features::TableFeature;
use crate::utils::test_utils::{assert_result_error_with_message, Action, LocalMockTable};
use crate::Predicate;
use crate::{DeltaResult, Engine, Error, Version};

use itertools::Itertools;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

fn get_schema() -> StructType {
    StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
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
        engine.storage_handler().as_ref(),
        log_root,
        start_version,
        end_version,
    )?;
    Ok(log_segment.ascending_commit_files)
}

fn result_to_sv(iter: impl Iterator<Item = DeltaResult<TableChangesScanMetadata>>) -> Vec<bool> {
    iter.map_ok(|scan_metadata| scan_metadata.selection_vector.into_iter())
        .flatten_ok()
        .try_collect()
        .unwrap()
}

#[tokio::test]
async fn metadata_protocol() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableDeletionVectors".to_string(),
                            "true".to_string(),
                        ),
                        ("delta.columnMapping.mode".to_string(), "none".to_string()),
                    ]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new(
                    3,
                    7,
                    Some([TableFeature::DeletionVectors]),
                    Some([TableFeature::DeletionVectors]),
                )
                .unwrap(),
            ),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let scan_batches =
        table_changes_action_iter(engine, commits, get_schema().into(), None).unwrap();
    let sv = result_to_sv(scan_batches);
    assert_eq!(sv, &[false, false]);
}
#[tokio::test]
async fn cdf_not_enabled() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([Action::Metadata(
            Metadata::try_new(
                None,
                None,
                get_schema(),
                vec![],
                0,
                HashMap::from([(
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                )]),
            )
            .unwrap(),
        )])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, commits, get_schema().into(), None)
            .unwrap()
            .try_collect();

    assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))));
}

#[tokio::test]
async fn unsupported_reader_feature() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([Action::Protocol(
            Protocol::try_new(
                3,
                7,
                Some([TableFeature::DeletionVectors, TableFeature::ColumnMapping]),
                Some([TableFeature::DeletionVectors, TableFeature::ColumnMapping]),
            )
            .unwrap(),
        )])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, commits, get_schema().into(), None)
            .unwrap()
            .try_collect();

    assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))));
}
#[tokio::test]
async fn column_mapping_should_fail() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([Action::Metadata(
            Metadata::try_new(
                None,
                None,
                get_schema(),
                vec![],
                0,
                HashMap::from([
                    (
                        "delta.enableDeletionVectors".to_string(),
                        "true".to_string(),
                    ),
                    ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                    ("delta.columnMapping.mode".to_string(), "id".to_string()),
                ]),
            )
            .unwrap(),
        )])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, commits, get_schema().into(), None)
            .unwrap()
            .try_collect();

    assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))));
}

// Note: This should be removed once type widening support is added for CDF
#[tokio::test]
async fn incompatible_schemas_fail() {
    async fn assert_incompatible_schema(commit_schema: StructType, cdf_schema: StructType) {
        let engine = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();

        mock_table
            .commit([Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    commit_schema,
                    vec![],
                    0,
                    HashMap::from([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
                )
                .unwrap(),
            )])
            .await;

        let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let res: DeltaResult<Vec<_>> =
            table_changes_action_iter(engine, commits, cdf_schema.into(), None)
                .unwrap()
                .try_collect();

        assert!(matches!(
            res,
            Err(Error::ChangeDataFeedIncompatibleSchema(_, _))
        ));
    }

    // The CDF schema has fields: `id: int` and `value: string`.
    // This commit has schema with fields: `id: long`, `value: string` and `year: int` (nullable).
    let schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
        StructField::nullable("year", DataType::INTEGER),
    ]);
    assert_incompatible_schema(schema, get_schema()).await;

    // The CDF schema has fields: `id: int` and `value: string`.
    // This commit has schema with fields: `id: long` and `value: string`.
    let schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]);
    assert_incompatible_schema(schema, get_schema()).await;

    // NOTE: Once type widening is supported, this should not return an error.
    //
    // The CDF schema has fields: `id: long` and `value: string`.
    // This commit has schema with fields: `id: int` and `value: string`.
    let cdf_schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]);
    let commit_schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]);
    assert_incompatible_schema(cdf_schema, commit_schema).await;

    // Note: Once schema evolution is supported, this should not return an error.
    //
    // The CDF schema has fields: nullable `id`  and nullable `value`.
    // This commit has schema with fields: non-nullable `id` and nullable `value`.
    let schema = StructType::new_unchecked([
        StructField::not_null("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]);
    assert_incompatible_schema(schema, get_schema()).await;

    // The CDF schema has fields: `id: int` and `value: string`.
    // This commit has schema with fields:`id: string` and `value: string`.
    let schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::STRING),
        StructField::nullable("value", DataType::STRING),
    ]);
    assert_incompatible_schema(schema, get_schema()).await;

    // Note: Once schema evolution is supported, this should not return an error.
    // The CDF schema has fields: `id` (nullable) and `value` (nullable).
    // This commit has schema with fields: `id` (nullable).
    let schema = get_schema().project_as_struct(&["id"]).unwrap();
    assert_incompatible_schema(schema, get_schema()).await;
}

#[tokio::test]
async fn add_remove() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let sv = table_changes_action_iter(engine, commits, get_schema().into(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, HashMap::new().into());
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[true, true]);
}

#[tokio::test]
async fn filter_data_change() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_3".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_4".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_5".into(),
                data_change: false,
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let sv = table_changes_action_iter(engine, commits, get_schema().into(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, HashMap::new().into());
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[false; 5]);
}

#[tokio::test]
async fn cdc_selection() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Cdc(Cdc {
                path: "fake_path_3".into(),
                ..Default::default()
            }),
            Action::Cdc(Cdc {
                path: "fake_path_4".into(),
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let sv = table_changes_action_iter(engine, commits, get_schema().into(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, HashMap::new().into());
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[true, false, true, true]);
}

#[tokio::test]
async fn dv() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    let deletion_vector1 = DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    };
    let deletion_vector2 = DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
        offset: Some(1),
        size_in_bytes: 38,
        cardinality: 3,
    };
    // - fake_path_1 undergoes a restore. All rows are restored, so the deletion vector is removed.
    // - All remaining rows of fake_path_2 are deleted
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                deletion_vector: Some(deletion_vector1.clone()),
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                deletion_vector: Some(deletion_vector2.clone()),
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let expected_remove_dvs = HashMap::from([(
        "fake_path_1".to_string(),
        DvInfo {
            deletion_vector: Some(deletion_vector1.clone()),
        },
    )])
    .into();
    let sv = table_changes_action_iter(engine, commits, get_schema().into(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, expected_remove_dvs);
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[false, true, true]);
}

// Note: Data skipping does not work on Remove actions.
#[tokio::test]
async fn data_skipping_filter() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    let deletion_vector = Some(DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    });
    mock_table
        .commit([
            // Remove/Add pair with max value id = 6
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_1".into(),
                stats: Some("{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":6},\"nullCount\":{\"id\":3}}".into()),
                data_change: true,
                deletion_vector: deletion_vector.clone(),
                ..Default::default()
            }),
            // Remove/Add pair with max value id = 4
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_2".into(),
                stats: Some("{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":4},\"nullCount\":{\"id\":3}}".into()),
                data_change: true,
                deletion_vector,
                ..Default::default()
            }),
            // Add action with max value id = 5
            Action::Add(Add {
                path: "fake_path_3".into(),
                stats: Some("{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":3}}".into()),
                data_change: true,
                ..Default::default()
            }),
        ])
        .await;

    // Look for actions with id > 4
    let predicate = Predicate::binary(
        BinaryPredicateOp::GreaterThan,
        column_expr!("id"),
        Scalar::from(4),
    );
    let logical_schema = get_schema();
    let predicate = match PhysicalPredicate::try_new(&predicate, &logical_schema) {
        Ok(PhysicalPredicate::Some(p, s)) => Some((p, s)),
        other => panic!("Unexpected result: {other:?}"),
    };
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let sv = table_changes_action_iter(engine, commits, logical_schema.into(), predicate)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            scan_metadata.selection_vector
        })
        .collect_vec();

    // Note: since the first pair is a dv operation, remove action will always be filtered
    assert_eq!(sv, &[false, true, false, false, true]);
}

#[tokio::test]
async fn failing_protocol() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    let protocol = Protocol::try_new(
        3,
        7,
        ["fake_feature".to_string()].into(),
        ["fake_feature".to_string()].into(),
    )
    .unwrap();

    mock_table
        .commit([
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Protocol(protocol),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, commits, get_schema().into(), None)
            .unwrap()
            .try_collect();

    assert_result_error_with_message(
        res,
        "Change data feed is unsupported for the table at version 0",
    );
}

#[tokio::test]
async fn file_meta_timestamp() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let file_meta_ts = commit.location.last_modified;
    let scanner = LogReplayScanner::try_new(engine.as_ref(), commit, &get_schema().into()).unwrap();
    assert_eq!(scanner.timestamp, file_meta_ts);
}
