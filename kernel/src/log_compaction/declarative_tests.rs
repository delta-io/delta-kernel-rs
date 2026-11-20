use std::error::Error as _;
use std::sync::Arc;

use ::test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder, VersionTarget};
use ::test_utils::{add_commit, add_staged_commit, assert_batches_sorted_eq, build_snapshot};
use rstest::rstest;
use serde_json::Value;
use url::Url;

use super::*;
use crate::actions::{ADD_NAME, LOG_ADD_SCHEMA, REMOVE_NAME};
use crate::arrow::error::ArrowError;
use crate::committer::FileSystemCommitter;
use crate::engine::DelegatingEngine;
use crate::engine::sync::SyncEngine;
use crate::expressions::{Expression as Expr, col};
use crate::log_path::LogPath;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::path::Path;
use crate::path::ParsedLogPath;
use crate::plans::{Operation, PlanBuilder, PlanExecutor, PlanResult};
use crate::schema::schema_ref;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transaction::UpdateTableOperation;
use crate::unit_test_utils::plans::execute_plan;
use crate::unit_test_utils::{MockProtocolBuilder, MockTableConfigurationBuilder};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::unit_test_utils::{
    adaptive_metadata_table_configuration, test_schema_flat_with_column_mapping,
};
use crate::{Engine as _, Error, KernelResult};

#[rstest]
#[case::raw_stats(
    r#"{"add":{"path":"data.parquet","partitionValues":{"long_col":null},
    "size":10,"modificationTime":1,"dataChange":false,"tags":{"keep":"yes","missing":null},
    "stats":" { \"numRecords\": 2, \"unindexed\": 7 }"}}"#
)]
#[case::remove(r#"{"remove":{"path":"data.parquet","deletionTimestamp":1,"dataChange":false}}"#)]
#[case::full_add(
    r#"{"add":{"path":"data.parquet","partitionValues":{"long_col":"1"},
    "size":10,"modificationTime":1,"dataChange":false,"stats":"{\"numRecords\":2}",
    "tags":{"keep":"yes","missing":null},"deletionVector":{"storageType":"p",
    "pathOrInlineDv":"file:///dv.bin","offset":4,"sizeInBytes":8,"cardinality":1},
    "baseRowId":10,"defaultRowCommitVersion":1,"clusteringProvider":"liquid"}}"#
)]
#[case::full_remove(
    r#"{"remove":{"path":"data.parquet","deletionTimestamp":1,"dataChange":false,
    "extendedFileMetadata":true,"partitionValues":{"long_col":"1"},"size":10,
    "stats":"{\"numRecords\":2}","tags":{"keep":"yes"},"deletionVector":{"storageType":"p",
    "pathOrInlineDv":"file:///dv.bin","offset":4,"sizeInBytes":8,"cardinality":1},
    "baseRowId":10,"defaultRowCommitVersion":1}}"#
)]
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
        .with_features(
            FeatureSet::new()
                .deletion_vectors()
                .row_tracking()
                .domain_metadata()
                .with_property(
                    "delta.setTransactionRetentionDuration",
                    "interval 1 milliseconds",
                ),
        )
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
            file["dataChange"] = Value::Bool(true);
        }
    }
    assert_eq!(rows, [action]);
    Ok(())
}

#[rstest]
#[case::latest(VersionTarget::Latest, false, true)]
#[case::at_version(VersionTarget::AtVersion(3), false, false)]
#[case::post_commit(VersionTarget::AtVersion(3), true, true)]
fn compaction_requires_snapshot_built_as_latest(
    #[case] target: VersionTarget,
    #[case] post_commit: bool,
    #[case] latest: bool,
) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = build_snapshot!(target, table.table_root(), &engine);
    assert_eq!(snapshot.version(), 3);
    let snapshot = if post_commit {
        assert!(!snapshot.is_built_as_latest());
        snapshot
            .update_table(UpdateTableOperation::Write, "test", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_post_commit_snapshot()
    } else {
        snapshot
    };
    assert_eq!(snapshot.version(), 3 + u64::from(post_commit));
    assert_eq!(snapshot.is_built_as_latest(), latest);
    let path =
        ParsedLogPath::new_log_compaction(snapshot.table_root(), 0, snapshot.version())?.location;
    let result = snapshot.compact_log(&engine, 0, snapshot.version());
    if latest {
        assert_eq!(result?, engine.storage_handler().head(&path)?);
    } else {
        assert!(result.unwrap_err().to_string().contains(&format!(
            "Log compaction requires a snapshot built as latest (snapshot version {})",
            snapshot.version()
        )));
        assert!(engine.storage_handler().head(&path).is_err());
    }
    Ok(())
}

#[rstest]
#[case::inside(&[(2, 3)], &[2, 3])]
#[case::crosses_left(&[(0, 2)], &[])]
#[case::crosses_right(&[(3, 5)], &[])]
#[case::overlapping(&[(1, 2), (2, 3)], &[1, 2])]
#[tokio::test]
async fn compaction_reuses_only_contained_ranges(
    #[case] ranges: &[(Version, Version)],
    #[case] delete_commits: &[Version],
) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(
            LogState::with_latest_version(5).with_compaction_ranges(ranges.iter().copied()),
        )
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), &engine);
    for version in delete_commits {
        table
            .store()
            .delete(&Path::from(format!("_delta_log/{version:020}.json")))
            .await
            .map_err(KernelError::from)?;
    }
    let meta = snapshot.compact_log(&engine, 1, 4)?;
    let plan = PlanBuilder::scan_json([meta], &[], LOG_ADD_SCHEMA.clone())?
        .project(
            Expr::struct_from([col!("add.dataChange")]),
            schema_ref! { nullable "dataChange": BOOLEAN },
        )?
        .build()?;
    assert_batches_sorted_eq!(
        [
            "+------------+",
            "| dataChange |",
            "+------------+",
            "| true       |",
            "| true       |",
            "| true       |",
            "| true       |",
            "+------------+",
        ],
        &execute_plan(&engine, plan)?
    );
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
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), 0, 3)?.location;
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
    match snapshot.compact_log(&engine, 0, 3).unwrap_err() {
        Error::Kernel(error) if corrupt => assert!(matches!(
            error.without_backtrace(),
            KernelError::Arrow(ArrowError::JsonError(_))
        )),
        Error::Delta(error) if !corrupt => {
            assert_eq!(error.name(), "DELTA_TRUNCATED_TRANSACTION_LOG");
            assert!(error.parameters()[0].value().ends_with(commit.as_ref()));
        }
        error => panic!("{error:?}"),
    }
    assert!(engine.storage_handler().head(&path).is_err());
    Ok(())
}

#[rstest]
fn compaction_maps_missing_file_errors(
    #[values(false, true)] source_read: bool,
    #[values(false, true)] lazy: bool,
) -> Result<()> {
    struct MissingFileExecutor {
        path: String,
        lazy: bool,
    }
    impl PlanExecutor for MissingFileExecutor {
        fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
            assert!(matches!(op, Operation::QueryPlan(_)));
            let error = KernelError::FileNotFound(self.path.clone()).with_backtrace();
            if self.lazy {
                Ok(PlanResult::Data(Box::new(std::iter::once(Err(error)))))
            } else {
                Err(error)
            }
        }
    }
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .build()
        .unwrap();
    let engine = Arc::new(SyncEngine::new_with_store(table.store().clone()));
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), engine.as_ref());
    let destination = ParsedLogPath::new_log_compaction(snapshot.table_root(), 0, 3)?.location;
    let path = if source_read {
        snapshot
            .log_segment()
            .log_root
            .join("00000000000000000001.json")
            .map_err(KernelError::from)?
    } else {
        destination.clone()
    }
    .to_string();
    let executor = Arc::new(MissingFileExecutor {
        path: path.clone(),
        lazy,
    });
    let engine = DelegatingEngine::new(engine).with_plan_executor(executor);
    let error = snapshot.compact_log(&engine, 0, 3).unwrap_err();
    let Error::Delta(error) = error else {
        panic!("{error:?}")
    };
    assert_eq!(error.name(), "DELTA_TRUNCATED_TRANSACTION_LOG");
    assert_eq!(error.parameters()[0].value(), path);
    assert_eq!(error.parameters()[1].value(), "3");
    let original = error
        .source()
        .unwrap()
        .downcast_ref::<KernelError>()
        .unwrap();
    assert!(matches!(original.without_backtrace(), KernelError::FileNotFound(p) if p == &path));
    assert!(
        original
            .to_string()
            .starts_with(&format!("File not found: {path}"))
    );
    assert!(engine.storage_handler().head(&destination).is_err());
    Ok(())
}

#[rstest]
#[case::before_checkpoint(1, 5, 5, false)]
#[case::at_checkpoint(3, 5, 5, false)]
#[case::after_checkpoint(4, 5, 5, true)]
#[case::commits_after_interval(4, 5, 7, true)]
#[case::checkpoint_after_interval(1, 2, 5, false)]
#[case::checkpoint_at_snapshot(1, 2, 3, false)]
#[case::retained_checkpoint(0, 2, 2, false)]
fn compaction_requires_interval_after_checkpoint(
    #[case] start: Version,
    #[case] end: Version,
    #[case] snapshot_version: Version,
    #[case] valid: bool,
    #[values(false, true)] skip_checkpoints: bool,
) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(
            LogState::with_latest_version(snapshot_version)
                .with_checkpoint_at([0, 3].into_iter().filter(|v| *v <= snapshot_version))
                .with_crc_at([snapshot_version]),
        )
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root())
        .at_version(1)
        .build(&engine)?;
    let builder = Snapshot::builder_from(snapshot);
    let snapshot = if skip_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    }
    .build(&engine)?;
    assert_eq!(snapshot.version(), snapshot_version);
    assert_eq!(
        snapshot.log_segment().checkpoint_version,
        Some(if skip_checkpoints || snapshot_version < 3 {
            0
        } else {
            3
        }),
    );
    let result = snapshot.compact_log(&engine, start, end);
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), start, end)?.location;
    if valid && !skip_checkpoints {
        assert_eq!(result?, engine.storage_handler().head(&path)?);
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains(if skip_checkpoints {
                    "Log compaction requires checkpoint discovery"
                } else {
                    "Log compaction must start after the latest checkpoint"
                })
        );
        assert!(engine.storage_handler().head(&path).is_err());
    }
    Ok(())
}

#[rstest]
#[case::published_prefix(0, 3, &[], true)]
#[case::published_end(0, 4, &[], true)]
#[case::reversed(3, 2, &[], false)]
#[case::single_version(2, 2, &[], false)]
#[case::beyond_snapshot(0, 5, &[], false)]
#[case::missing_start(0, 4, &[0], false)]
#[case::missing_middle(0, 4, &[1], false)]
#[case::missing_selected_commit(0, 3, &[1], false)]
#[case::missing_end(0, 4, &[4], false)]
#[case::empty(0, 4, &[0, 1, 2, 3, 4], false)]
#[tokio::test]
async fn compaction_validates_published_interval(
    #[case] start: Version,
    #[case] end: Version,
    #[case] missing: &[Version],
    #[case] valid: bool,
    #[values(false, true)] staged_tail: bool,
) -> Result<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(4))
        .with_data(0, 0)
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let mut builder = Snapshot::builder_for(table.table_root());
    if staged_tail {
        let staged = add_staged_commit(table.table_root(), table.store().as_ref(), 4, "{}".into())
            .await
            .unwrap();
        let staged = Url::parse(table.table_root())
            .map_err(KernelError::from)?
            .join(staged.as_ref())
            .map_err(KernelError::from)?;
        builder = builder
            .with_log_tail(vec![LogPath::try_new(
                engine.storage_handler().head(&staged)?,
            )?])
            .with_max_catalog_version(4);
    }
    let snapshot = builder.build(&engine).unwrap();
    for &version in missing {
        table
            .store()
            .delete(&Path::from(format!("_delta_log/{version:020}.json")))
            .await
            .map_err(KernelError::from)?;
    }
    let result = snapshot.compact_log(&engine, start, end);
    let staged_interval = staged_tail && end == 4;
    if valid && !staged_interval {
        let meta = result?;
        assert_eq!(engine.storage_handler().head(&meta.location)?, meta);
        return Ok(());
    }
    let error = result.unwrap_err();
    if staged_interval {
        assert!(matches!(error, Error::Kernel(KernelError::Unsupported(_))));
    } else if missing.is_empty() {
        assert!(matches!(error, Error::Kernel(_)));
    } else {
        let Error::Delta(error) = error else {
            panic!("expected DELTA_TRUNCATED_TRANSACTION_LOG: {error:?}");
        };
        assert_eq!(error.name(), "DELTA_TRUNCATED_TRANSACTION_LOG");
        assert!(missing.iter().any(|version| {
            error.parameters()[0]
                .value()
                .ends_with(&format!("/{version:020}.json"))
        }));
        assert_eq!(error.parameters()[1].value(), end.to_string());
    }
    let path = ParsedLogPath::new_log_compaction(snapshot.table_root(), start, end)?.location;
    assert!(engine.storage_handler().head(&path).is_err());
    Ok(())
}
