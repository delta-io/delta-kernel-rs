use std::num::NonZero;
use std::sync::{Arc, Mutex};

use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::commit_range::{CommitRange, DeltaAction};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::expressions::{Predicate, PredicateRef, expr};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt};
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::schema::{DataType, MetadataColumnSpec, SchemaRef, StructField, schema_ref};
use delta_kernel::snapshot::IncrementalReplay;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, JsonHandler,
    KernelResult, KernelResultIterator, ParquetHandler, Snapshot, StorageHandler,
};
use futures::TryStreamExt;
use rstest::rstest;
use serde_json::json;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::log_compaction::write_compaction_fixture;
use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder, all_features_cm_name};
use test_utils::{
    add_commit, assert_result_error_with_message, begin_transaction, into_record_batch,
    load_test_data, read_scan,
};
use url::Url;

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn partial_compaction_preserves_suppression(
    #[values(false, true)] checkpoint_prefix: bool,
    #[values("domain", "file", "file_without_timestamp", "transaction")] action: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_features(
            FeatureSet::new()
                .domain_metadata()
                .with_property("delta.setTransactionRetentionDuration", "interval 1 day"),
        )
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let files = table.store().list(None).try_collect::<Vec<_>>().await?;
    let data_path = &files
        .iter()
        .find(|f| f.location.as_ref().ends_with(".parquet"))
        .unwrap()
        .location;
    add_commit(
        table.table_root(),
        table.store().as_ref(),
        2,
        [
            json!({"domainMetadata": {"domain": "test", "configuration": "old", "removed": false}}),
            json!({"txn": {"appId": "app", "version": 1}}),
        ]
        .map(|action| action.to_string())
        .join("\n"),
    )
    .await?;
    if checkpoint_prefix {
        Snapshot::builder_for(table.table_root())
            .build(engine.as_ref())?
            .checkpoint(engine.as_ref(), None)?;
    }
    // The builder cannot express historical or missing deletion/transaction timestamps.
    let mut remove = json!({"path": data_path.as_ref(), "dataChange": true});
    if action != "file_without_timestamp" {
        remove["deletionTimestamp"] = json!(1);
    }
    add_commit(
        table.table_root(),
        table.store().as_ref(),
        3,
        [
            json!({"remove": remove}),
            json!({"domainMetadata": {"domain": "test", "configuration": "", "removed": true}}),
            json!({"txn": {"appId": "app", "version": 2, "lastUpdated": 1}}),
        ]
        .map(|action| action.to_string())
        .join("\n"),
    )
    .await?;
    add_commit(
        table.table_root(),
        table.store().as_ref(),
        4,
        json!({"domainMetadata": {"domain": "keep", "configuration": "value", "removed": false}})
            .to_string(),
    )
    .await?;
    let baseline = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    assert_eq!(
        baseline
            .reader()
            .get_domain_metadata("test", engine.as_ref())?,
        None
    );
    assert_eq!(
        baseline
            .reader()
            .get_app_id_version("app", engine.as_ref())?,
        None
    );
    assert!(read_scan(&baseline.clone().scan_builder().build()?, engine.clone())?.is_empty());

    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 3, 4).await?;
    engine.json.reads.lock().unwrap().clear();
    let compacted = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    match action {
        "domain" => assert_eq!(
            compacted
                .reader()
                .get_domain_metadata("test", engine.as_ref())?,
            None
        ),
        "transaction" => assert_eq!(
            compacted
                .reader()
                .get_app_id_version("app", engine.as_ref())?,
            None
        ),
        "file" | "file_without_timestamp" => {
            assert!(read_scan(&compacted.scan_builder().build()?, engine.clone())?.is_empty())
        }
        _ => panic!("unknown action"),
    }
    assert!(engine.json.reads.lock().unwrap().contains(&path));
    Ok(())
}

#[rstest]
#[case::prefix(vec![(0, 4)])]
#[case::interior(vec![(2, 4)])]
#[case::adjacent(vec![(0, 2), (3, 4), (5, 6)])]
#[case::overlapping(vec![(0, 3), (0, 4), (2, 5), (5, 6)])]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_reads_match_commits(
    #[case] ranges: Vec<(u64, u64)>,
    #[values(false, true)] checkpoint: bool,
    #[values(None, Some(2), Some(6))] crc: Option<u64>,
    #[values(false, true)] mapped_partitioned: bool,
    #[values(1, 1024)] batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state =
        LogState::with_latest_version(6).maybe_with_checkpoint_at(checkpoint.then_some(2));
    if let Some(v) = crc {
        state = state.with_crc_at([v]);
    }
    let mut builder = TestTableBuilder::new()
        .with_log_state(state)
        .with_schema(schema_ref! { nullable "id": INTEGER, nullable "part": STRING })
        .with_data(1, 3);
    if mapped_partitioned {
        builder = builder
            .with_features(all_features_cm_name())
            .with_partition_columns(["part"]);
    }
    let table = builder.build()?;
    let engine = recording_engine(table.store().clone(), batch_size);
    let mut baselines = vec![];
    for version in [1, 3, 4, 6] {
        let snapshot = Snapshot::builder_for(table.table_root())
            .at_version(version)
            .build(engine.as_ref())?;
        baselines.push((snapshot.clone(), scan_results(snapshot, engine.clone())?));
    }
    let (_, expected) = baselines.last().unwrap();
    let all_files = expected[0].files.len();
    assert!(!expected[1].files.is_empty());
    assert!(expected[1].files.len() < all_files);
    if mapped_partitioned {
        assert!(!expected[2].files.is_empty());
        assert!(expected[2].files.len() < all_files);
    }
    let mut paths = vec![];
    for &(start, end) in &ranges {
        paths.push(
            write_compaction_fixture(table.table_root(), table.store().as_ref(), start, end)
                .await?,
        );
    }
    for (baseline, expected) in &baselines {
        engine.json.reads.lock().unwrap().clear();
        let fresh = Snapshot::builder_for(table.table_root())
            .at_version(baseline.version())
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .build(engine.as_ref())?;
        assert_eq!(fresh.schema(), baseline.schema());
        assert_eq!(
            fresh.table_configuration().protocol(),
            baseline.table_configuration().protocol()
        );
        assert_eq!(fresh.table_properties(), baseline.table_properties());
        assert_eq!(scan_results(fresh.clone(), engine.clone())?, *expected);
        let eligible = ranges.iter().any(|&(start, end)| {
            end <= baseline.version() && (!checkpoint || baseline.version() < 2 || start > 2)
        });
        let reads = engine.json.reads.lock().unwrap().clone();
        if eligible {
            assert!(paths.iter().any(|p| reads.contains(p)));
        }
        for (path, &(start, end)) in paths.iter().zip(&ranges) {
            if end > baseline.version() || (checkpoint && baseline.version() >= 2 && start <= 2) {
                assert!(
                    !reads.contains(path),
                    "out-of-range compaction was consumed: {path}"
                );
            }
        }
        let updated = Snapshot::builder_from(baselines[0].0.clone())
            .at_version(baseline.version())
            .build(engine.as_ref())?;
        assert_eq!(scan_results(updated, engine.clone())?, *expected);
    }
    let compacted = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    compacted.checkpoint(engine.as_ref(), None)?;
    let reloaded = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    assert_eq!(
        scan_results(reloaded.clone(), engine.clone())?,
        baselines.last().unwrap().1
    );
    let committed = begin_transaction(reloaded, engine.as_ref())?
        .commit(engine.as_ref())?
        .unwrap_committed();
    assert_eq!(
        committed.post_commit_stats().commits_since_log_compaction,
        1
    );
    Ok(())
}

#[rstest]
#[case::empty("", false)]
#[case::malformed("{", true)]
#[tokio::test(flavor = "multi_thread")]
async fn unusable_compaction_does_not_return_partial_state(
    #[case] contents: &str,
    #[case] fails: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2))
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let baseline = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 0, 2).await?;
    table
        .store()
        .put(
            &Path::from_url_path(path.path())?,
            contents.to_string().into(),
        )
        .await?;
    let result = Snapshot::builder_for(table.table_root()).build(engine.as_ref());
    if fails {
        assert!(result.is_err());
    } else {
        assert_eq!(result?.schema(), baseline.schema());
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_requires_original_commit_versions(
    #[values(false, true)] existing_compaction: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    if existing_compaction {
        write_compaction_fixture(table.table_root(), table.store().as_ref(), 1, 3).await?;
    }
    table
        .store()
        .delete(&Path::from("_delta_log/00000000000000000002.json"))
        .await?;
    assert!(
        Snapshot::builder_for(table.table_root())
            .build(engine.as_ref())
            .is_err()
    );
    Ok(())
}

#[rstest]
#[case::missing(None, false)]
#[case::behind(Some(1), false)]
#[case::missing_newer_checkpoint_rejected(Some(9), true)]
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_hint_behavior_with_compaction(
    #[case] hint_version: Option<u64>,
    #[case] fails: bool,
    #[values(false, true)] compact: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(5).with_checkpoint_at([1, 3]))
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let baseline = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let expected = scan_results(baseline.clone(), engine.clone())?;
    let compaction = if compact {
        Some(write_compaction_fixture(table.table_root(), table.store().as_ref(), 4, 5).await?)
    } else {
        None
    };
    let hint = Path::from("_delta_log/_last_checkpoint");
    if let Some(version) = hint_version {
        table
            .store()
            .put(
                &hint,
                json!({"version": version, "size": 1}).to_string().into(),
            )
            .await?;
    } else {
        table.store().delete(&hint).await?;
    }
    engine.json.reads.lock().unwrap().clear();
    let result = Snapshot::builder_for(table.table_root()).build(engine.as_ref());
    if fails {
        assert_result_error_with_message(
            result,
            "Had a _last_checkpoint hint but didn't find any checkpoints",
        );
    } else {
        let snapshot = result?;
        assert_eq!(snapshot.version(), baseline.version());
        assert_eq!(scan_results(snapshot, engine.clone())?, expected);
        if let Some(path) = compaction {
            assert!(engine.json.reads.lock().unwrap().contains(&path));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_cannot_advance_latest_version() -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let baseline = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let expected = scan_results(baseline.clone(), engine.clone())?;
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 1, 3).await?;
    let source = Path::from_url_path(path.path())?;
    let future = Path::from("_delta_log/00000000000000000001.00000000000000000004.compacted.json");
    let contents = table.store().get(&source).await?.bytes().await?;
    table.store().put(&future, contents.into()).await?;
    table.store().delete(&source).await?;
    engine.json.reads.lock().unwrap().clear();
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 3);
    assert_eq!(scan_results(snapshot, engine.clone())?, expected);
    assert!(
        !engine
            .json
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.path().ends_with(".compacted.json"))
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_without_adds_preserves_state(
    #[values("domain", "transaction", "removal", "empty")] action: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .with_features(FeatureSet::new().domain_metadata())
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let mut snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    for version in 1..=3 {
        let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?;
        match action {
            "transaction" => txn = txn.with_transaction_id("app".into(), version),
            "domain" => txn = txn.with_domain_metadata("app.config".into(), version.to_string()),
            "removal" => {
                for batch in snapshot
                    .scan_builder()
                    .build()?
                    .scan_metadata(engine.as_ref())?
                {
                    txn.remove_files(batch?.scan_files);
                }
            }
            "empty" => (),
            _ => panic!("unknown action"),
        }
        snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    }
    let expected_rows = scan_results(snapshot.clone(), engine.clone())?;
    if action == "removal" {
        assert!(expected_rows[0].files.is_empty());
    }
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 2, 4).await?;
    engine.json.reads.lock().unwrap().clear();
    let fresh = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    assert_eq!(fresh.version(), 4);
    if action == "transaction" {
        assert_eq!(
            fresh.reader().get_app_id_version("app", engine.as_ref())?,
            Some(3)
        );
    } else if action == "domain" {
        assert_eq!(
            fresh
                .reader()
                .get_domain_metadata("app.config", engine.as_ref())?
                .as_deref(),
            Some("3")
        );
    }
    assert_eq!(scan_results(fresh, engine.clone())?, expected_rows);
    if action == "empty" {
        assert_eq!(
            table
                .store()
                .head(&Path::from_url_path(path.path())?)
                .await?
                .size,
            0
        );
    }
    assert_eq!(
        engine.json.reads.lock().unwrap().contains(&path),
        action != "empty"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_preserves_nested_variant_data() -> Result<(), Box<dyn std::error::Error>> {
    let directory = load_test_data("./tests/data", "unshredded-variant")?;
    let table_root = Url::from_directory_path(directory.path().join("unshredded-variant"))
        .unwrap()
        .to_string();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = recording_engine(store.clone(), 1);
    // Version 1 precedes the fixture's checkpoint, so the scan must replay the compaction.
    let baseline = Snapshot::builder_for(&table_root)
        .at_version(1)
        .build(engine.as_ref())?;
    let predicates = vec![
        None,
        Some(Arc::new(Predicate::from_expr(expr!(col("id") >= 50i64)))),
    ];
    let expected =
        scan_results_with_predicates(baseline.clone(), engine.clone(), predicates.clone())?;
    assert!(!expected[0].files.is_empty());
    assert!(!expected[1].files.is_empty());
    assert!(expected[1].files.len() < expected[0].files.len());
    let path = write_compaction_fixture(&table_root, store.as_ref(), 0, 1).await?;
    engine.json.reads.lock().unwrap().clear();
    let fresh = Snapshot::builder_for(&table_root)
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(fresh.schema(), baseline.schema());
    assert_eq!(
        scan_results_with_predicates(fresh, engine.clone(), predicates)?,
        expected
    );
    assert!(engine.json.reads.lock().unwrap().contains(&path));
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn existing_compaction_fixture_matches_original_commits(
    #[values(5, 15)] version: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = load_test_data("./tests/data", "compacted-log-files-table")?;
    let table_root = Url::from_directory_path(directory.path().join("compacted-log-files-table"))
        .unwrap()
        .to_string();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = recording_engine(store.clone(), 1);
    let compacted = Snapshot::builder_for(&table_root)
        .at_version(version)
        .build(engine.as_ref())?;
    let actual = scan_results_with_predicates(compacted.clone(), engine.clone(), vec![None])?;
    assert!(!actual[0].files.is_empty());
    let paths = engine.json.reads.lock().unwrap().clone();
    assert!(paths.iter().any(|p| p.path().ends_with(".compacted.json")));

    let log_root = Url::parse(&table_root)?.join("_delta_log/")?;
    let prefix = Path::from_url_path(log_root.path())?;
    for file in store.list(Some(&prefix)).try_collect::<Vec<_>>().await? {
        if file.location.as_ref().ends_with(".compacted.json") {
            store.delete(&file.location).await?;
        }
    }
    engine.json.reads.lock().unwrap().clear();
    let original = Snapshot::builder_for(&table_root)
        .at_version(version)
        .build(engine.as_ref())?;
    assert_eq!(original.schema(), compacted.schema());
    assert_eq!(
        original.table_configuration().protocol(),
        compacted.table_configuration().protocol()
    );
    assert_eq!(
        scan_results_with_predicates(original, engine.clone(), vec![None])?,
        actual
    );
    assert!(
        !engine
            .json
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.path().ends_with(".compacted.json"))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_consumers_preserve_history_with_compaction()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_features(FeatureSet::new().change_data_feed().ict())
        .with_data(1, 2)
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let baseline = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let timestamp = baseline.reader().get_timestamp(engine.as_ref())?;
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 1, 3).await?;
    engine.json.reads.lock().unwrap().clear();
    let range = CommitRange::builder_for(table.table_root(), 1)
        .with_end_version(3)
        .build(engine.as_ref())?;
    let mut versions = vec![];
    for commit in range.commits(engine.clone(), None, &[DeltaAction::CommitInfo])? {
        let commit = commit?;
        versions.push(commit.version());
        let batches = commit
            .get_actions(engine.as_ref())?
            .collect::<KernelResult<Vec<_>>>()?;
        assert!(
            batches
                .into_iter()
                .map(into_record_batch)
                .collect::<Vec<_>>()
                .iter()
                .any(|b| b.column(0).null_count() < b.num_rows())
        );
    }
    assert_eq!(versions, vec![1, 2, 3]);
    let changes =
        TableChanges::try_new(Url::parse(table.table_root())?, engine.as_ref(), 1, Some(3))?;
    let scan = changes.into_scan_builder().build()?;
    let batches = scan
        .execute(engine.clone())?
        .collect::<KernelResult<Vec<_>>>()?;
    assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), 6);
    assert!(!engine.json.reads.lock().unwrap().contains(&path));
    let compacted = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    assert_eq!(
        compacted.reader().get_timestamp(engine.as_ref())?,
        timestamp
    );
    let resolved = delta_kernel::history_manager::latest_version_as_of(
        &compacted,
        engine.as_ref(),
        timestamp,
        delta_kernel::history_manager::HistoryCommitType::Recreatable,
    )?;
    assert_eq!(resolved.version, 3);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_preserves_schema_changes(
    #[values("none", "name", "id")] column_mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_features(FeatureSet::new().column_mapping(column_mapping))
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let old = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let mut latest = old.clone();
    for field in [
        StructField::nullable("sequence", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ] {
        latest = latest
            .update_table(UpdateTableOperation::AlterTable, "test", false)
            .add_column(field)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
    }
    let expected = scan_results(latest.clone(), engine.clone())?;
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 2, 3).await?;
    engine.json.reads.lock().unwrap().clear();
    let fresh = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let incremental = Snapshot::builder_from(old).build(engine.as_ref())?;
    for snapshot in [fresh, incremental] {
        assert_eq!(snapshot.schema(), latest.schema());
        assert_eq!(scan_results(snapshot, engine.clone())?, expected);
    }
    assert!(engine.json.reads.lock().unwrap().contains(&path));
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_preserves_protocol_upgrades(
    #[values(false, true)] stale_crc: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state = LogState::with_latest_version(1);
    if stale_crc {
        state = state.with_crc_at([1]);
    }
    let table = TestTableBuilder::new()
        .with_log_state(state)
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let old = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    // The update-table API cannot enable protocol features on an existing table.
    for (version, protocol) in [
        (
            2,
            json!({"minReaderVersion": 1, "minWriterVersion": 7,
            "writerFeatures": ["appendOnly", "invariants"]}),
        ),
        (
            3,
            json!({"minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["timestampNtz"],
            "writerFeatures": ["appendOnly", "invariants", "timestampNtz"]}),
        ),
    ] {
        add_commit(
            table.table_root(),
            table.store().as_ref(),
            version,
            json!({"protocol": protocol}).to_string(),
        )
        .await?;
    }
    let latest = Snapshot::builder_for(table.table_root())
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .build(engine.as_ref())?;
    let expected = latest.table_configuration().protocol();
    assert_ne!(expected, old.table_configuration().protocol());
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 2, 3).await?;
    engine.json.reads.lock().unwrap().clear();
    let fresh = Snapshot::builder_for(table.table_root())
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .build(engine.as_ref())?;
    let incremental = Snapshot::builder_from(old).build(engine.as_ref())?;
    for snapshot in [fresh, incremental] {
        let config = snapshot.table_configuration();
        assert_eq!(config.protocol(), expected);
        assert!(config.is_feature_supported(&TableFeature::TimestampWithoutTimezone));
        assert_eq!(
            scan_results(snapshot, engine.clone())?,
            scan_results(latest.clone(), engine.clone())?
        );
    }
    assert!(engine.json.reads.lock().unwrap().contains(&path));
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn parallel_metadata_uses_compaction(
    #[values(false, true)] checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(
            LogState::with_latest_version(6)
                .maybe_with_checkpoint_at(checkpoint.then_some(2))
                .with_compaction_ranges([(3, 5)]),
        )
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    let snapshot = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let mut expected = vec![];
    for batch in scan.scan_metadata(engine.as_ref())? {
        expected =
            batch?.visit_scan_files(expected, |files: &mut Vec<String>, f| files.push(f.path))?;
    }
    let mut actual = vec![];
    engine.json.reads.lock().unwrap().clear();
    let mut sequential = scan.parallel_scan_metadata(engine.clone())?;
    for batch in sequential.by_ref() {
        actual =
            batch?.visit_scan_files(actual, |files: &mut Vec<String>, f| files.push(f.path))?;
    }
    if let AfterSequentialScanMetadata::Parallel { state, files } = sequential.finish()? {
        let state = Arc::new(*state);
        for file in files {
            for batch in ParallelScanMetadata::try_new(engine.clone(), state.clone(), vec![file])? {
                actual = batch?
                    .visit_scan_files(actual, |files: &mut Vec<String>, f| files.push(f.path))?;
            }
        }
    }
    expected.sort();
    actual.sort();
    assert_eq!(actual.len(), 6);
    assert_eq!(actual, expected);
    assert!(
        engine
            .json
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.path().ends_with(".compacted.json"))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_preserves_unsupported_reader_feature_error()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_schema(schema_ref! { nullable "id": INTEGER })
        .build()?;
    let engine = recording_engine(table.store().clone(), 1);
    add_commit(
        table.table_root(),
        table.store().as_ref(),
        2,
        json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["unknownReaderFeature"],
            "writerFeatures": ["unknownReaderFeature"]}})
        .to_string(),
    )
    .await?;
    add_commit(
        table.table_root(),
        table.store().as_ref(),
        3,
        json!({"commitInfo": {"timestamp": 1}}).to_string(),
    )
    .await?;
    let expected = Snapshot::builder_for(table.table_root())
        .build(engine.as_ref())
        .unwrap_err()
        .to_string();
    assert!(expected.contains("unknownReaderFeature"), "{expected}");
    let path = write_compaction_fixture(table.table_root(), table.store().as_ref(), 2, 3).await?;
    engine.json.reads.lock().unwrap().clear();
    let actual = Snapshot::builder_for(table.table_root())
        .build(engine.as_ref())
        .unwrap_err()
        .to_string();
    assert_eq!(actual, expected);
    assert!(engine.json.reads.lock().unwrap().contains(&path));
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct ScanResult {
    files: Vec<String>,
    rows: Vec<String>,
}

fn scan_results(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
) -> Result<Vec<ScanResult>, Box<dyn std::error::Error>> {
    let mut predicates = vec![
        None,
        Some(Arc::new(Predicate::from_expr(expr!(col("id") >= 3000)))),
    ];
    if snapshot
        .schema()
        .fields()
        .any(|field| field.name() == "part")
    {
        predicates.push(Some(Arc::new(Predicate::from_expr(expr!(
            col("part") == "part_1000"
        )))));
    }
    scan_results_with_predicates(snapshot, engine, predicates)
}

fn scan_results_with_predicates(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    predicates: Vec<Option<PredicateRef>>,
) -> Result<Vec<ScanResult>, Box<dyn std::error::Error>> {
    let mut schema = snapshot.schema().as_ref().clone();
    if snapshot.table_properties().enable_row_tracking == Some(true) {
        schema = schema
            .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
            .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?;
    }
    let mut results = vec![];
    for predicate in predicates {
        let mut builder = snapshot
            .clone()
            .scan_builder()
            .with_schema(Arc::new(schema.clone()));
        if let Some(p) = predicate {
            builder = builder.with_predicate(p);
        }
        let scan = builder.build()?;
        let mut files = vec![];
        for batch in scan.scan_metadata(engine.as_ref())? {
            files =
                batch?.visit_scan_files(files, |paths: &mut Vec<String>, f| paths.push(f.path))?;
        }
        files.sort();
        let batches = read_scan(&scan, engine.clone())?;
        let mut rows = pretty_format_batches(&batches)?
            .to_string()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        results.push(ScanResult { files, rows });
    }
    Ok(results)
}

fn recording_engine(store: Arc<DynObjectStore>, batch_size: usize) -> Arc<RecordingEngine> {
    let inner = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_batch_size(NonZero::new(batch_size).unwrap())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );
    let json = Arc::new(RecordingJson {
        inner: inner.json_handler(),
        reads: Mutex::new(vec![]),
    });
    Arc::new(RecordingEngine { inner, json })
}

struct RecordingEngine {
    inner: Arc<dyn Engine>,
    json: Arc<RecordingJson>,
}

impl Engine for RecordingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.inner.parquet_handler()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }
}

struct RecordingJson {
    inner: Arc<dyn JsonHandler>,
    reads: Mutex<Vec<Url>>,
}

impl JsonHandler for RecordingJson {
    fn parse_json(
        &self,
        data: Box<dyn EngineData>,
        schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.parse_json(data, schema)
    }
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let batches = self
            .inner
            .read_json_files(files, schema, predicate)?
            .collect::<KernelResult<Vec<_>>>()?;
        self.reads
            .lock()
            .unwrap()
            .extend(files.iter().map(|f| f.location.clone()));
        Ok(Box::new(batches.into_iter().map(Ok)))
    }
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        overwrite: bool,
    ) -> KernelResult<u64> {
        self.inner.write_json_file(path, data, overwrite)
    }
}
