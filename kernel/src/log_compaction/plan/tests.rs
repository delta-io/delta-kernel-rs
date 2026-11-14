use std::sync::Arc;

use ::test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder, VersionTarget};
use ::test_utils::{
    add_commit, add_staged_commit, assert_batches_sorted_eq, build_snapshot, load_test_data,
    read_actions_from_commit, sort_lines,
};
use itertools::Itertools as _;
use rstest::rstest;
use serde_json::Value;
use url::Url;

use super::*;
use crate::action_reconciliation::plan::action_set::ActionSource;
use crate::actions::{DomainMetadata, Metadata, Protocol, Remove, SetTransaction};
use crate::arrow::array::StringArray;
use crate::arrow::compute::{concat_batches, filter_record_batch, is_not_null};
use crate::arrow::util::pretty::pretty_format_batches;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::arrow_utils::parse_json_impl;
use crate::engine::sync::SyncEngine;
use crate::log_path::LogPath;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::path::Path;
use crate::table_configuration::TableConfiguration;
use crate::unit_test_utils::plans::execute_plan;
use crate::unit_test_utils::recording_reads::RecordingReads;
use crate::unit_test_utils::{
    Action, LocalMockTable, MockProtocolBuilder, MockTableConfigurationBuilder, copy_test_table,
    create_add_action, create_remove_action,
};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, test_schema_flat_with_column_mapping,
};
use crate::{Error, KernelResult};

#[rstest]
#[case::stored_0_5("compacted-log-files-table", (0, 5), true,
    &[(0, "metaData"), (0, "protocol"), (2, "remove"),
      (3, "remove"), (4, "add"), (5, "add"), (5, "remove")])]
#[case::stored_6_10("compacted-log-files-table", (6, 10), true,
    &[(6, "remove"), (7, "add"), (8, "add"), (9, "remove"),
      (10, "add"), (10, "remove")])]
#[case::stored_6_15("compacted-log-files-table", (6, 15), true,
    &[(6, "remove"), (9, "remove"), (10, "remove"),
      (11, "remove"), (12, "remove"), (13, "add"),
      (13, "remove"), (14, "add"), (15, "add")])]
#[case::dv_full("log-replay-dv-key-cases", (0, 3), false,
    &[(0, "metaData"), (0, "protocol"), (1, "remove"), (2, "remove"),
      (3, "add"), (3, "remove")])]
#[case::dv_tail("log-replay-dv-key-cases", (1, 3), false,
    &[(1, "remove"), (2, "remove"), (3, "add"), (3, "remove")])]
#[case::metadata_protocol("log-replay-latest-metadata-protocol", (0, 2), false,
    &[(0, "add"), (1, "add"), (2, "metaData"), (2, "protocol")])]
#[case::restore("basic-with-inserts-overwrite-restore", (0, 3), false,
    &[(3, "add"), (3, "metaData"), (3, "protocol"),
      (3, "remove")])]
#[case::restore_tail("basic-with-inserts-overwrite-restore", (2, 3), false,
    &[(3, "add"), (3, "metaData"), (3, "protocol"),
      (3, "remove")])]
#[case::delete_readd("delete-re-add-same-file-different-transactions", (0, 3), false,
    &[(0, "metaData"), (0, "protocol"), (2, "add"), (3, "add")])]
#[case::mapped_dv_tail("dv-with-columnmapping", (1, 15), false,
    &[(1, "remove"), (2, "remove"), (3, "remove"),
      (4, "remove"), (5, "remove"), (6, "remove"), (7, "remove"),
      (8, "remove"), (9, "remove"), (10, "remove"), (11, "remove"),
      (12, "remove"), (13, "remove"), (14, "add"), (14, "remove"),
      (15, "add"), (15, "remove")])]
#[case::evolving_stats("data-skipping-change-stats-collected-across-versions", (0, 4), false,
    &[(0, "add"), (0, "protocol"), (2, "add"), (3, "metaData"),
      (4, "add")])]
#[case::partitioned_stats("data-skipping-partition-and-data-column", (0, 3), false,
    &[(0, "add"), (0, "metaData"), (0, "protocol"), (1, "add"),
      (2, "add"), (3, "add")])]
fn compaction_matches_existing_compaction_intervals(
    #[case] table: &str,
    #[case] interval: (Version, Version),
    #[case] stored: bool,
    #[case] expected_actions: &[(Version, &str)],
) -> Result<()> {
    let (root, _tempdir) = if !stored {
        let tempdir = load_test_data("tests/golden_data", table).unwrap();
        let root = Url::from_directory_path(tempdir.path().join(table).join("delta")).unwrap();
        (root, tempdir)
    } else {
        copy_test_table(table)?
    };
    let log = root
        .join("_delta_log/")
        .map_err(KernelError::from)?
        .to_file_path()
        .unwrap();
    let (start, end) = interval;
    if stored {
        std::fs::remove_file(log.join(format!("{start:020}.{end:020}.compacted.json")))
            .map_err(KernelError::from)?;
    }
    // Exercise the commit windows before checkpoint publication.
    for entry in std::fs::read_dir(&log).map_err(KernelError::from)? {
        let entry = entry.map_err(KernelError::from)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.contains(".checkpoint.") || name == "_last_checkpoint" {
            std::fs::remove_file(entry.path()).map_err(KernelError::from)?;
        }
    }
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(root.clone()).build(&engine).unwrap();
    let read_schema = ActionSet::ALL.schema(ActionSource::Commit)?;
    // Pin complete expected payloads to fixture commits; no replay computes them.
    let mut expected = Vec::new();
    for &(version, name) in expected_actions {
        for mut action in read_actions_from_commit(&root, version, name).unwrap() {
            if name == ADD_NAME || name == REMOVE_NAME {
                action["dataChange"] = Value::Bool(false);
            }
            expected.push(
                Value::Object([(name.to_string(), action)].into_iter().collect()).to_string(),
            );
        }
    }
    let expected = parse_json_impl(&StringArray::from(expected), read_schema.clone())?;
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), start, end)?.location;
    let written_metadata = snapshot.compact_log(&engine, start, end)?;
    let meta = engine.storage_handler().head(&path)?;
    assert_eq!(written_metadata, meta);
    // Parse both sides through Value so map entries have the same display order.
    let before = std::fs::read(path.to_file_path().unwrap()).map_err(KernelError::from)?;
    let written = serde_json::Deserializer::from_slice(&before)
        .into_iter::<Value>()
        .map_ok(|row| row.to_string())
        .collect::<serde_json::Result<Vec<_>>>()
        .map_err(KernelError::from)?;
    let written = parse_json_impl(&StringArray::from(written), read_schema)?;
    let expected = pretty_format_batches(&[expected])
        .map_err(KernelError::from)?
        .to_string();
    let mut expected = expected.lines().collect::<Vec<_>>();
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &[written]);

    assert!(matches!(
        snapshot.compact_log(&engine, start, end),
        Err(crate::Error::Kernel(KernelError::FileAlreadyExists(_)))
    ));
    assert_eq!(
        std::fs::read(path.to_file_path().unwrap()).map_err(KernelError::from)?,
        before
    );
    Ok(())
}

#[rstest]
#[case::full(0)]
#[case::tail(1)]
#[case::after_seed(2)]
#[tokio::test]
async fn compaction_preserves_interval_winners_without_retention(
    #[case] start: Version,
    #[values(None, Some(0), Some(i64::MAX))] deletion_timestamp: Option<i64>,
) -> Result<()> {
    let end = 3;
    let engine = SyncEngine::new();
    let mut table = LocalMockTable::new();
    let root = Url::from_directory_path(table.table_root()).unwrap();
    let mut metadata = MockTableConfigurationBuilder::new()
        .with_properties([
            (
                "delta.deletedFileRetentionDuration",
                "interval 1 milliseconds",
            ),
            (
                "delta.setTransactionRetentionDuration",
                "interval 1 milliseconds",
            ),
        ])
        .build()
        .metadata()
        .clone();
    let mut protocol = MockProtocolBuilder::new()
        .with_features([TableFeature::DomainMetadata])
        .build();
    table
        .commit([
            Action::Protocol(protocol.clone()),
            Action::Metadata(metadata.clone()),
        ])
        .await;
    for version in 1..=3 {
        let features = [
            TableFeature::DomainMetadata,
            TableFeature::ChangeDataFeed,
            TableFeature::DeletionVectors,
        ];
        protocol = MockProtocolBuilder::new()
            .with_features(&features[..version])
            .build();
        metadata = metadata.with_configuration_entry("test", version.to_string());
        let mut actions = vec![
            Action::Protocol(protocol.clone()),
            Action::Metadata(metadata.clone()),
        ];
        actions.extend(match version {
            1 => vec![
                create_add_action("cancelled"),
                create_add_action("restored"),
                Action::Txn(SetTransaction::new("app".into(), 10, Some(0))),
                Action::Txn(SetTransaction::new("other".into(), 1, None)),
                Action::DomainMetadata(DomainMetadata::new("test".into(), "old".into())),
                Action::DomainMetadata(DomainMetadata::new("other".into(), "keep".into())),
            ],
            2 => vec![
                Action::Remove(Remove {
                    path: "cancelled".into(),
                    data_change: true,
                    deletion_timestamp,
                    ..Default::default()
                }),
                create_remove_action("restored"),
                Action::Txn(SetTransaction::new("app".into(), 9, Some(0))),
                Action::DomainMetadata(DomainMetadata::remove("test".into(), "deleted".into())),
            ],
            _ => vec![create_add_action("restored")],
        });
        table.commit(actions).await;
    }
    let snapshot = Snapshot::builder_for(root).build(&engine).unwrap();
    for version in 0..start {
        // Compaction must not need the earlier table state.
        std::fs::remove_file(
            table
                .table_root()
                .join(format!("_delta_log/{version:020}.json")),
        )
        .map_err(KernelError::from)?;
    }
    let meta = snapshot.compact_log(&engine, start, end)?;
    let schema = ActionSet::ALL.schema(ActionSource::Commit)?;
    let batches = engine
        .json_handler()
        .read_json_files(&[meta], schema, None)?
        .map(|batch| batch?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    let batch = concat_batches(&batches[0].schema(), &batches).map_err(KernelError::from)?;
    let data = ArrowEngineData::new(batch.clone());
    assert_eq!(Protocol::try_new_from_data(&data)?, Some(protocol));
    assert_eq!(Metadata::try_new_from_data(&data)?, Some(metadata));
    let actual = batch.normalize(".", Some(1)).map_err(KernelError::from)?;
    let timestamp = deletion_timestamp
        .map(|value| value.to_string())
        .unwrap_or_default();
    #[rustfmt::skip]
    let expected = [
        (
            &["add.path", "add.dataChange"][..],
            r#"+----------+----------------+
| add.path | add.dataChange |
+----------+----------------+
| restored | false          |
+----------+----------------+"#.to_string(),
        ),
        (
            &["remove.path", "remove.dataChange", "remove.deletionTimestamp"][..],
            format!(r#"+-------------+-------------------+--------------------------+
| remove.path | remove.dataChange | remove.deletionTimestamp |
+-------------+-------------------+--------------------------+
| cancelled   | false             | {timestamp:<24} |
+-------------+-------------------+--------------------------+"#),
        ),
        (
            &["txn.appId", "txn.version", "txn.lastUpdated"][..],
            r#"+-----------+-------------+-----------------+
| txn.appId | txn.version | txn.lastUpdated |
+-----------+-------------+-----------------+
| app       | 9           | 0               |
| other     | 1           |                 |
+-----------+-------------+-----------------+"#.to_string(),
        ),
        (
            &[
                "domainMetadata.domain", "domainMetadata.configuration", "domainMetadata.removed",
            ][..],
            r#"+-----------------------+------------------------------+------------------------+
| domainMetadata.domain | domainMetadata.configuration | domainMetadata.removed |
+-----------------------+------------------------------+------------------------+
| test                  | deleted                      | true                   |
| other                 | keep                         | false                  |
+-----------------------+------------------------------+------------------------+"#.to_string(),
        ),
    ];
    let mut checked_rows = 2; // Protocol and metadata were compared in full above.
    for (fields, expected) in expected {
        let columns = fields
            .iter()
            .map(|field| actual.schema_ref().index_of(field))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(KernelError::from)?;
        let batch = filter_record_batch(
            &actual.project(&columns).map_err(KernelError::from)?,
            &is_not_null(actual.column_by_name(fields[0]).unwrap()).map_err(KernelError::from)?,
        )
        .map_err(KernelError::from)?;
        checked_rows += batch.num_rows();
        let mut expected = expected
            .lines()
            .filter(|line| start <= 1 || !line.starts_with("| other "))
            .collect::<Vec<_>>();
        sort_lines!(expected);
        assert_batches_sorted_eq!(expected, &[batch]);
    }
    assert_eq!(checked_rows, actual.num_rows());
    Ok(())
}

#[rstest]
#[case::raw_stats(
    r#"{"add":{"path":"data.parquet","partitionValues":{"long_col":null},
    "size":10,"modificationTime":1,"dataChange":true,"tags":{"keep":"yes","missing":null},
    "stats":" { \"numRecords\": 2, \"unindexed\": 7 }"}}"#
)]
#[case::remove(r#"{"remove":{"path":"data.parquet","deletionTimestamp":1,"dataChange":true}}"#)]
#[case::domain_deletion(
    r#"{"domainMetadata":{"domain":"test","configuration":"old","removed":true}}"#
)]
#[case::transaction(r#"{"txn":{"appId":"app","version":2,"lastUpdated":1}}"#)]
fn compaction_preserves_action_payloads(
    #[case] action: &str,
    #[values(false, true)] checkpoint: bool,
) -> Result<()> {
    let mut action: Value = serde_json::from_str(action).map_err(KernelError::from)?;
    let table = TestTableBuilder::new()
        .with_features(FeatureSet::new().domain_metadata().with_property(
            "delta.setTransactionRetentionDuration",
            "interval 1 milliseconds",
        ))
        .with_partition_columns(["long_col"])
        .with_data(0, 0)
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    if checkpoint {
        build_snapshot!(VersionTarget::Latest, table.table_root(), &engine)
            .checkpoint(&engine, None)?;
    }
    for version in 1..=2 {
        futures::executor::block_on(add_commit(
            table.table_root(),
            table.store().as_ref(),
            version,
            action.to_string(),
        ))
        .unwrap();
    }
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), &engine);
    let meta = snapshot.compact_log(&engine, 1, 2)?;
    let path = Path::from_url_path(meta.location.path()).map_err(KernelError::from)?;
    let bytes =
        futures::executor::block_on(async { table.store().get(&path).await?.bytes().await })
            .map_err(KernelError::from)?;
    let rows = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<Value>()
        .collect::<serde_json::Result<Vec<_>>>()
        .map_err(KernelError::from)?;
    for name in [ADD_NAME, REMOVE_NAME] {
        if let Some(file) = action.get_mut(name) {
            file["dataChange"] = Value::Bool(false);
        }
    }
    assert_eq!(rows, [action]);
    Ok(())
}

#[rstest]
#[case::unknown_writer(MockTableConfigurationBuilder::new().with_protocol(
    MockProtocolBuilder::new().with_writer_features([TableFeature::Unknown("unsupported".into())]).build()
).build())]
#[cfg_attr(feature = "adaptive-metadata-in-dev", case::adaptive_metadata(
    adaptive_metadata_table_configuration(test_schema_flat_with_column_mapping(), &[])
))]
fn compaction_rejects_unsupported_features(#[case] config: TableConfiguration) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), &engine);
    let config = TableConfiguration::try_new(
        config.metadata().clone(),
        config.protocol().clone(),
        snapshot.table_root().clone(),
        snapshot.version(),
    )?;
    let snapshot = Snapshot::new(snapshot.log_segment().clone(), config)?;
    assert!(matches!(
        snapshot.compact_log(&engine, 0, 1),
        Err(Error::Kernel(KernelError::Unsupported(_)))
    ));
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), 0, 1)?.location;
    assert!(engine.storage_handler().head(&path).is_err());
    Ok(())
}

#[rstest]
fn compaction_input_failure_does_not_publish(#[values(false, true)] corrupt: bool) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), &engine);
    let actions = snapshot.log_compaction_actions(engine.storage_handler().as_ref(), 0, 3)?;
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), 0, 3)?.location;
    let plan = PlanBuilder::write_json([actions], path.clone(), false, false)?.build()?;
    // Replay reads newest first, so this input fails after the other commits have been read.
    let commit = Path::from("_delta_log/00000000000000000000.json");
    futures::executor::block_on(async {
        if corrupt {
            let mut bytes = table.store().get(&commit).await?.bytes().await?.to_vec();
            bytes.extend_from_slice(b"\n{");
            table.store().put(&commit, bytes.into()).await?;
        } else {
            table.store().delete(&commit).await?;
        }
        Ok::<_, KernelError>(())
    })?;
    assert!(execute_plan(&engine, plan).is_err());
    assert!(engine.storage_handler().head(&path).is_err());
    Ok(())
}

#[rstest]
fn compaction_reuses_published_commits_after_checkpoint(
    #[values(0, 2, 3)] start: Version,
) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(4).with_checkpoint_at([2]))
        .build()
        .unwrap();
    let engine = Arc::new(SyncEngine::new_with_store(table.store().clone()));
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), engine.as_ref());
    let mut reads = RecordingReads::new(None);
    let recorder = Arc::get_mut(&mut reads).unwrap();
    recorder.inner = engine.clone();
    recorder.expected_token = None;
    let result = snapshot.log_compaction_actions(reads.as_ref(), start, 4);
    assert_eq!(result.is_ok(), start > 2);
    assert!(reads.events.lock().unwrap().is_empty());
    assert_eq!(
        snapshot.compact_log(engine.as_ref(), start, 4).is_ok(),
        start > 2
    );
    Ok(())
}

#[rstest]
#[case::published_prefix(0, 3, &[], true)]
#[case::newly_published_end(0, 4, &[], true)]
#[case::ignored_actions(1, 3, &[], false)]
#[case::reversed(3, 2, &[], false)]
#[case::single_version(2, 2, &[], false)]
#[case::beyond_snapshot(0, 5, &[], false)]
#[case::missing_start(0, 4, &[0], false)]
#[case::missing_middle(0, 4, &[1], false)]
#[case::staged_only_end(0, 4, &[4], false)]
#[case::empty(0, 4, &[0, 1, 2, 3, 4], false)]
#[tokio::test]
async fn compaction_validates_published_interval(
    #[case] start: Version,
    #[case] end: Version,
    #[case] missing: &[Version],
    #[case] valid: bool,
) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_data(0, 0)
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let staged = add_staged_commit(table.table_root(), table.store().as_ref(), 4, "{}".into())
        .await
        .unwrap();
    let staged = Url::parse(table.table_root())
        .map_err(KernelError::from)?
        .join(staged.as_ref())
        .map_err(KernelError::from)?;
    let snapshot = Snapshot::builder_for(table.table_root())
        .with_log_tail(vec![LogPath::try_new(
            engine.storage_handler().head(&staged)?,
        )?])
        .with_max_catalog_version(4)
        .build(&engine)
        .unwrap();
    // The snapshot still has a staged path, so the constructor must discover its published path.
    add_commit(table.table_root(), table.store().as_ref(), 4, "{}".into())
        .await
        .unwrap();
    for &version in missing {
        add_staged_commit(
            table.table_root(),
            table.store().as_ref(),
            version,
            "{}".into(),
        )
        .await
        .unwrap();
        table
            .store()
            .delete(&Path::from(format!("_delta_log/{version:020}.json")))
            .await
            .map_err(KernelError::from)?;
    }
    let result = snapshot.compact_log(&engine, start, end);
    if valid {
        let meta = result?;
        assert_eq!(engine.storage_handler().head(&meta.location)?, meta);
        return Ok(());
    }
    let error = result.unwrap_err();
    if missing.is_empty() {
        assert!(matches!(error, Error::Kernel(_)));
    } else {
        let Error::Delta(error) = error else {
            panic!("expected DELTA_VERSIONS_NOT_CONTIGUOUS: {error:?}");
        };
        assert_eq!(error.name(), "DELTA_VERSIONS_NOT_CONTIGUOUS");
        assert_eq!(error.sql_state(), Some("KD00C"));
        assert_eq!(
            error
                .parameters()
                .iter()
                .map(|p| p.value())
                .collect::<Vec<_>>(),
            [
                (start..=end).filter(|v| !missing.contains(v)).join(","),
                start.to_string(),
                end.to_string(),
                snapshot.version().to_string(),
            ]
        );
    }
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), start, end)?.location;
    assert!(engine.storage_handler().head(&path).is_err());
    Ok(())
}
