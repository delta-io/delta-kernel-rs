use std::sync::Arc;

use delta_kernel::commit_range::CommitRange;
use delta_kernel::history_manager::{first_version_after, latest_version_as_of, HistoryCommitType};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::snapshot::IncrementalReplay;
use delta_kernel::{KernelError, Snapshot};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::{
    actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit,
    create_log_path, delta_path_for_version, install_thread_local_metrics_reporter,
    CountingReporter, TestAction,
};
use url::Url;

fn setup_test() -> (
    Arc<InMemory>,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Url,
) {
    let storage = Arc::new(InMemory::new());
    let table_root = Url::parse("memory:///").unwrap();
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    (storage, engine, table_root)
}

fn setup_test_mt() -> (
    Arc<InMemory>,
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    Url,
) {
    let storage = Arc::new(InMemory::new());
    let table_root = Url::parse("memory:///").unwrap();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = Arc::new(
        DefaultEngineBuilder::new(storage.clone())
            .with_task_executor(executor)
            .build(),
    );
    (storage, engine, table_root)
}

#[tokio::test]
async fn basic_snapshot_with_log_tail_staged_commits() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // with staged commits:
    // _delta_log/0.json (PM in here, catalog-managed)
    // _delta_log/_staged_commits/1.uuid.json
    // _delta_log/_staged_commits/1.uuid.json // add an unused staged commit at version 1
    // _delta_log/_staged_commits/2.uuid.json
    let actions = vec![TestAction::Metadata];
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;
    let path1 = add_staged_commit(table_root, storage.as_ref(), 1, String::from("{}")).await?;
    let _ = add_staged_commit(table_root, storage.as_ref(), 1, String::from("{}")).await?;
    let path2 = add_staged_commit(table_root, storage.as_ref(), 2, String::from("{}")).await?;

    // 1. Create log_tail for commits 1, 2
    let log_tail = vec![
        create_log_path(&table_url, path1.clone()),
        create_log_path(&table_url, path2.clone()),
    ];
    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail.clone())
        .with_max_catalog_version(2)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 2);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.listed.ascending_commit_files.len(), 3);
    // version 0 is commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[0]
            .location
            .location,
        table_url.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[1]
            .location
            .location,
        table_url.join(path1.as_ref())?
    );
    // version 2 is staged commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[2]
            .location
            .location,
        table_url.join(path2.as_ref())?
    );

    // 2. Now check for time-travel to 1
    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail)
        .at_version(1)
        .with_max_catalog_version(2)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.listed.ascending_commit_files.len(), 2);
    // version 0 is commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[0]
            .location
            .location,
        table_url.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[1]
            .location
            .location,
        table_url.join(path1.as_ref())?
    );

    // 3. Check case for log_tail is only 1 staged commit
    let log_tail = vec![create_log_path(&table_url, path1.clone())];
    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail)
        .with_max_catalog_version(1)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.listed.ascending_commit_files.len(), 2);
    // version 0 is commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[0]
            .location
            .location,
        table_url.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[1]
            .location
            .location,
        table_url.join(path1.as_ref())?
    );

    // 4. Check if we don't pass log tail
    let snapshot = Snapshot::builder_for(table_root)
        .with_max_catalog_version(0)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.listed.ascending_commit_files.len(), 1);
    // version 0 is commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[0]
            .location
            .location,
        table_url.join(delta_path_for_version(0, "json").as_ref())?
    );

    // 5. Check duplicating log_tail with normal listed commit
    let log_tail = vec![create_log_path(
        &table_url,
        delta_path_for_version(0, "json"),
    )];
    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail)
        .with_max_catalog_version(0)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.listed.ascending_commit_files.len(), 1);
    // version 0 is commit
    assert_eq!(
        log_segment.listed.ascending_commit_files[0]
            .location
            .location,
        table_url.join(delta_path_for_version(0, "json").as_ref())?
    );

    Ok(())
}

/// Timestamp-to-version resolution must see catalog-managed staged commits (issue #2443). Staged
/// commits carry in-commit timestamps, so resolution feeds them in as the log_tail rather than
/// erroring on snapshots that contain staged commits.
#[tokio::test]
async fn timestamp_resolution_with_staged_commits() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    let ict_v0: i64 = 1587968586154;
    let ict_v1: i64 = ict_v0 + 100;
    let ict_v2: i64 = ict_v0 + 200;
    let staged_ict_body = |ict: i64| {
        format!(
            r#"{{"commitInfo":{{"timestamp":{ict},"inCommitTimestamp":{ict},"operation":"WRITE","isBlindAppend":true}}}}"#
        )
    };

    // publish v0; v1, v2 are ratified staged commits. Also include the corresponding ICT to each
    // version.
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(vec![TestAction::Metadata]),
    )
    .await?;
    let path1 = add_staged_commit(table_root, storage.as_ref(), 1, staged_ict_body(ict_v1)).await?;
    let path2 = add_staged_commit(table_root, storage.as_ref(), 2, staged_ict_body(ict_v2)).await?;

    let log_tail = vec![
        create_log_path(&table_url, path1),
        create_log_path(&table_url, path2),
    ];
    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail)
        .with_max_catalog_version(2)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 2);

    // latest_version_as_of rounds down; ICT between v1 and v2 rounds to v1.
    let e = engine.as_ref();
    let ct = HistoryCommitType::Published;
    assert_eq!(latest_version_as_of(&snapshot, e, ict_v1, ct)?.version, 1);
    assert_eq!(
        latest_version_as_of(&snapshot, e, ict_v1 + 50, ct)?.version,
        1
    );
    assert_eq!(latest_version_as_of(&snapshot, e, ict_v2, ct)?.version, 2);
    // timestamps before v0 is out of range
    assert!(latest_version_as_of(&snapshot, e, ict_v0 - 1, ct).is_err());

    // first_version_after rounds up; ICT between v0 and v1 rounds to v1.
    assert_eq!(
        first_version_after(&snapshot, e, ict_v0 + 1, ct)?.version,
        1
    );
    assert_eq!(first_version_after(&snapshot, e, ict_v2, ct)?.version, 2);

    Ok(())
}

#[tokio::test]
async fn basic_snapshot_with_log_tail() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // with normal commits:
    // _delta_log/0.json
    // _delta_log/1.json
    // _delta_log/2.json
    let actions = vec![TestAction::Metadata];
    add_commit(table_root, storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 2, actions_to_string(actions)).await?;

    // Create log_tail for commits 1, 2
    let log_tail = vec![
        create_log_path(&table_url, delta_path_for_version(1, "json")),
        create_log_path(&table_url, delta_path_for_version(2, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 2);
    Ok(())
}

#[tokio::test]
async fn log_tail_behind_filesystem() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // Create commits 0, 1, 2 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(table_root, storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 2, actions_to_string(actions)).await?;

    // log_tail BEHIND file system => must respect log_tail
    let log_tail = vec![
        create_log_path(&table_url, delta_path_for_version(0, "json")),
        create_log_path(&table_url, delta_path_for_version(1, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // snapshot stops at version 1, not 2
    assert_eq!(
        snapshot.version(),
        1,
        "Log tail should define the latest version"
    );
    Ok(())
}

#[rstest]
#[case::without_checkpoint(false)]
#[case::with_checkpoint(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_snapshot_skip_new_checkpoints_with_log_tail(
    #[case] with_checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // ===== GIVEN =====
    let (storage, engine, table_url) = setup_test_mt();
    let table_root = table_url.as_str();

    // commits 0, 1, 2 in storage (catalog-managed)
    let actions = vec![TestAction::Metadata];
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 2, actions_to_string(actions)).await?;

    let mut initial_snapshot = Snapshot::builder_for(table_root)
        .at_version(1)
        .with_max_catalog_version(2)
        .build(engine.as_ref())?;
    assert_eq!(initial_snapshot.version(), 1);
    if with_checkpoint {
        initial_snapshot.clone().checkpoint(engine.as_ref(), None)?;
        initial_snapshot = Snapshot::builder_for(table_root)
            .at_version(1)
            .with_max_catalog_version(2)
            .build(engine.as_ref())?;
        Snapshot::builder_for(table_root)
            .at_version(2)
            .with_max_catalog_version(2)
            .build(engine.as_ref())?
            .checkpoint(engine.as_ref(), None)?;
    }

    // Add a published version 3 that the staged catalog commit will supersede.
    let actions = vec![TestAction::Add("published_file_3.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 3, actions_to_string(actions)).await?;

    // The catalog tail overrides the published version 3.
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    let path3 =
        add_staged_commit(table_root, storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    let path4 =
        add_staged_commit(table_root, storage.as_ref(), 4, actions_to_string(actions)).await?;

    let log_tail = vec![
        create_log_path(&table_url, delta_path_for_version(2, "json")),
        create_log_path(&table_url, path3.clone()),
        create_log_path(&table_url, path4.clone()),
    ];

    let reporter = Arc::new(CountingReporter::new());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    // ===== WHEN =====
    // Build from version 1 using the catalog tail while retaining every intervening commit.
    let new_snapshot = Snapshot::builder_from(initial_snapshot)
        .with_log_tail(log_tail)
        .with_max_catalog_version(4)
        .skip_new_checkpoints()
        .build(engine.as_ref())?;

    // ===== THEN =====
    // The staged tail wins, all required commits remain available, and listing occurs only once.
    assert_eq!(new_snapshot.version(), 4);
    assert_eq!(reporter.list_calls.get(), 1);
    assert_eq!(
        new_snapshot.log_segment().checkpoint_version,
        with_checkpoint.then_some(1)
    );
    assert_eq!(
        new_snapshot.log_segment().listed.max_published_version,
        Some(3)
    );
    assert_eq!(
        new_snapshot
            .log_segment()
            .listed
            .latest_commit_file
            .as_ref()
            .map(|file| &file.location.location),
        Some(&table_url.join(path4.as_ref())?)
    );
    let mut expected_commit_paths = if with_checkpoint {
        Vec::new()
    } else {
        vec![
            table_url.join(delta_path_for_version(0, "json").as_ref())?,
            table_url.join(delta_path_for_version(1, "json").as_ref())?,
        ]
    };
    expected_commit_paths.extend([
        table_url.join(delta_path_for_version(2, "json").as_ref())?,
        table_url.join(path3.as_ref())?,
        table_url.join(path4.as_ref())?,
    ]);
    assert_eq!(
        new_snapshot
            .log_segment()
            .listed
            .ascending_commit_files
            .iter()
            .map(|file| file.location.location.clone())
            .collect::<Vec<_>>(),
        expected_commit_paths
    );

    // ===== WHEN =====
    let range = CommitRange::builder_from(new_snapshot, 2).build(engine.as_ref())?;

    // ===== THEN =====
    // Building the range reuses the retained commit metadata without another listing.
    assert_eq!(range.start_version(), 2);
    assert_eq!(range.end_version(), 4);
    assert_eq!(reporter.list_calls.get(), 1);

    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_snapshot_retains_cached_staged_commits(
    #[values(false, true)] with_checkpoint: bool,
    #[values(false, true)] full_tail: bool,
    #[values(false, true)] ignore_checkpoints: bool,
    #[values(IncrementalReplay::Disabled, IncrementalReplay::Unlimited)] replay: IncrementalReplay,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_incremental_snapshot_retains_staged_commits(
        with_checkpoint,
        full_tail,
        ignore_checkpoints,
        replay,
        false,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_snapshot_advances_checkpoint_retains_staged_predecessor(
) -> Result<(), Box<dyn std::error::Error>> {
    assert_incremental_snapshot_retains_staged_commits(
        true,
        false,
        false,
        IncrementalReplay::Disabled,
        true,
    )
    .await
}

async fn assert_incremental_snapshot_retains_staged_commits(
    with_checkpoint: bool,
    full_tail: bool,
    ignore_checkpoints: bool,
    replay: IncrementalReplay,
    advance_checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test_mt();
    let table_root = table_url.as_str();
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(vec![TestAction::Metadata]),
    )
    .await?;
    if with_checkpoint {
        Snapshot::builder_for(table_root)
            .with_max_catalog_version(0)
            .build(engine.as_ref())?
            .checkpoint(engine.as_ref(), None)?;
    }
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("file_1.parquet".into())]),
    )
    .await?;
    let staged_v2 = add_staged_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("file_2.parquet".into())]),
    )
    .await?;
    let cached_snapshot = Snapshot::builder_for(table_root)
        .with_max_catalog_version(2)
        .with_log_tail(vec![create_log_path(&table_url, staged_v2.clone())])
        .build(engine.as_ref())?;
    assert_eq!(cached_snapshot.version(), 2);
    assert_eq!(
        cached_snapshot.log_segment().checkpoint_version,
        with_checkpoint.then_some(0)
    );
    if advance_checkpoint {
        Snapshot::builder_for(table_root)
            .at_version(1)
            .with_max_catalog_version(1)
            .build(engine.as_ref())?
            .checkpoint(engine.as_ref(), None)?;
    }
    let staged_v3 = add_staged_commit(
        table_root,
        storage.as_ref(),
        3,
        actions_to_string(vec![TestAction::Add("file_3.parquet".into())]),
    )
    .await?;
    let full_log_tail = vec![
        create_log_path(&table_url, staged_v2.clone()),
        create_log_path(&table_url, staged_v3.clone()),
    ];
    let cold_snapshot = Snapshot::builder_for(table_root)
        .with_max_catalog_version(3)
        .with_log_tail(full_log_tail.clone())
        .build(engine.as_ref())?;
    let reporter = Arc::new(CountingReporter::new());
    let guard = install_thread_local_metrics_reporter(reporter.clone());
    let mut builder = Snapshot::builder_from(cached_snapshot)
        .with_max_catalog_version(3)
        .with_log_tail(if full_tail {
            full_log_tail
        } else {
            vec![create_log_path(&table_url, staged_v3.clone())]
        })
        .with_incremental_crc_replay(replay);
    if ignore_checkpoints {
        builder = builder.skip_new_checkpoints();
    }
    let refreshed = builder.build(engine.as_ref())?;
    assert_eq!(reporter.list_calls.get(), 1);
    drop(guard);

    assert_eq!(refreshed.version(), 3);
    assert_eq!(
        refreshed.table_configuration(),
        cold_snapshot.table_configuration()
    );
    assert_eq!(refreshed.schema(), cold_snapshot.schema());
    let segment = refreshed.log_segment();
    let expected_checkpoint = if advance_checkpoint {
        Some(1)
    } else {
        with_checkpoint.then_some(0)
    };
    assert_eq!(segment.checkpoint_version, expected_checkpoint);
    assert_eq!(segment.listed.max_published_version, Some(1));
    let mut expected_paths = Vec::new();
    if expected_checkpoint.is_none() {
        expected_paths.push(table_url.join(delta_path_for_version(0, "json").as_ref())?);
    }
    if expected_checkpoint.is_none_or(|checkpoint| checkpoint < 1) {
        expected_paths.push(table_url.join(delta_path_for_version(1, "json").as_ref())?);
    }
    expected_paths.extend([
        table_url.join(staged_v2.as_ref())?,
        table_url.join(staged_v3.as_ref())?,
    ]);
    assert_eq!(
        segment
            .listed
            .ascending_commit_files
            .iter()
            .map(|file| file.location.location.clone())
            .collect::<Vec<_>>(),
        expected_paths
    );
    assert_eq!(
        segment
            .listed
            .latest_commit_file
            .as_ref()
            .map(|file| &file.location.location),
        Some(&table_url.join(staged_v3.as_ref())?)
    );
    for version in [2, 3] {
        assert!(storage
            .head(&delta_path_for_version(version, "json"))
            .await
            .is_err());
    }
    for snapshot in [refreshed, cold_snapshot] {
        let scan = snapshot.scan_builder().build()?;
        let mut paths = Vec::new();
        for metadata in scan.scan_metadata(engine.as_ref())? {
            paths = metadata?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
        }
        paths.sort();
        assert_eq!(
            paths,
            ["file_1.parquet", "file_2.parquet", "file_3.parquet"]
        );
    }
    Ok(())
}

#[rstest]
#[case::staged_suffix(false, false)]
#[case::published_predecessors(true, false)]
#[case::empty_catalog_tail(true, true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_snapshot_retains_staged_history_across_updates(
    #[case] publish_predecessors: bool,
    #[case] empty_tail: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test_mt();
    let table_root = table_url.as_str();
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(vec![TestAction::Metadata]),
    )
    .await?;
    Snapshot::builder_for(table_root)
        .with_max_catalog_version(0)
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), None)?;
    add_commit(
        table_root,
        storage.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("file_1.parquet".into())]),
    )
    .await?;
    let staged_v2 = add_staged_commit(
        table_root,
        storage.as_ref(),
        2,
        actions_to_string(vec![TestAction::Add("file_2.parquet".into())]),
    )
    .await?;
    let mut snapshot = Snapshot::builder_for(table_root)
        .with_max_catalog_version(2)
        .with_log_tail(vec![create_log_path(&table_url, staged_v2.clone())])
        .build(engine.as_ref())?;
    let mut expected_paths = vec![
        table_url.join(delta_path_for_version(1, "json").as_ref())?,
        table_url.join(staged_v2.as_ref())?,
    ];

    for target in [3, 4, 6] {
        let cached_version = snapshot.version();
        let mut log_tail = Vec::new();
        if publish_predecessors {
            for version in 2..=cached_version {
                add_commit(
                    table_root,
                    storage.as_ref(),
                    version,
                    actions_to_string(vec![TestAction::Add(format!("file_{version}.parquet"))]),
                )
                .await?;
            }
        }
        for version in cached_version + 1..=target {
            let actions =
                actions_to_string(vec![TestAction::Add(format!("file_{version}.parquet"))]);
            let path = if empty_tail {
                add_commit(table_root, storage.as_ref(), version, actions).await?;
                delta_path_for_version(version, "json")
            } else {
                let staged =
                    add_staged_commit(table_root, storage.as_ref(), version, actions).await?;
                log_tail.push(create_log_path(&table_url, staged.clone()));
                staged
            };
            expected_paths.push(table_url.join(path.as_ref())?);
        }
        let reporter = Arc::new(CountingReporter::new());
        let guard = install_thread_local_metrics_reporter(reporter.clone());
        snapshot = Snapshot::builder_from(snapshot)
            .with_max_catalog_version(target)
            .with_log_tail(log_tail)
            .build(engine.as_ref())?;
        assert_eq!(reporter.list_calls.get(), 1);
        drop(guard);
        assert_eq!(snapshot.version(), target);
        assert_eq!(snapshot.log_segment().checkpoint_version, Some(0));
        assert_eq!(
            snapshot.log_segment().listed.max_published_version,
            Some(if empty_tail {
                target
            } else if publish_predecessors {
                cached_version
            } else {
                1
            })
        );
        assert_eq!(
            snapshot
                .log_segment()
                .listed
                .ascending_commit_files
                .iter()
                .map(|file| file.version)
                .collect::<Vec<_>>(),
            (1..=target).collect::<Vec<_>>()
        );
        assert_eq!(
            snapshot
                .log_segment()
                .listed
                .ascending_commit_files
                .iter()
                .map(|file| file.location.location.clone())
                .collect::<Vec<_>>(),
            expected_paths
        );
    }
    Ok(())
}

/// Verify that `builder_from` with `max_catalog_version` stops at the catalog-ratified version
/// even when later commits exist on the filesystem.
#[tokio::test]
async fn incremental_snapshot_caps_at_max_catalog_version() -> Result<(), Box<dyn std::error::Error>>
{
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // commits 0, 1, 2 in storage (catalog-managed)
    let actions = vec![TestAction::Metadata];
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 2, actions_to_string(actions)).await?;

    // Simulate a request to the catalog which reports max_catalog_version = 2
    let mcv = 2;

    // Build initial snapshot at version 1, catalog knows about version 2
    let initial_snapshot = Snapshot::builder_for(table_root)
        .at_version(1)
        .with_max_catalog_version(mcv)
        .build(engine.as_ref())?;
    assert_eq!(initial_snapshot.version(), 1);

    // Catalog reported v2 as the max ratified version. A moment later, v3 was
    // ratified and published to the log -- but the client is unaware of v3.
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 3, actions_to_string(actions)).await?;

    // Incremental update: catalog reported v2 as max, log_tail includes v2
    let log_tail = vec![create_log_path(
        &table_url,
        delta_path_for_version(mcv, "json"),
    )];
    let new_snapshot = Snapshot::builder_from(initial_snapshot)
        .with_log_tail(log_tail)
        .with_max_catalog_version(mcv)
        .build(engine.as_ref())?;

    // Snapshot respects the catalog's reported max version (v2)
    assert_eq!(new_snapshot.version(), 2);

    Ok(())
}

#[tokio::test]
async fn log_tail_exceeds_requested_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // commits 0, 1, 2, 3, 4 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(table_root, storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 4, actions_to_string(actions)).await?;

    // log tail goes up to version 4
    let log_tail = vec![
        create_log_path(&table_url, delta_path_for_version(1, "json")),
        create_log_path(&table_url, delta_path_for_version(2, "json")),
        create_log_path(&table_url, delta_path_for_version(3, "json")),
        create_log_path(&table_url, delta_path_for_version(4, "json")),
    ];

    // user asks for version 3 (or catalog says latest is 3)
    let snapshot = Snapshot::builder_for(table_root)
        .at_version(3)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // Should stop at version 3 even though log tail has version 4
    assert_eq!(snapshot.version(), 3);
    Ok(())
}

#[tokio::test]
async fn log_tail_behind_requested_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_url) = setup_test();
    let table_root = table_url.as_str();

    // create commits 0, 1, 2, 3, 4 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(table_root, storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(table_root, storage.as_ref(), 4, actions_to_string(actions)).await?;

    // Log tail only goes up to version 2
    let log_tail = vec![
        create_log_path(&table_url, delta_path_for_version(1, "json")),
        create_log_path(&table_url, delta_path_for_version(2, "json")),
    ];

    // User asks for version 4, but versions 3 and 4 are unavailable through the log tail.
    let result = Snapshot::builder_for(table_root)
        .at_version(4)
        .with_log_tail(log_tail)
        .build(engine.as_ref());

    assert!(matches!(result, Err(KernelError::MissingVersion(3))));

    Ok(())
}
