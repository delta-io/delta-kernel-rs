use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{FileMeta, LogPath, Snapshot};

use test_utils::{
    actions_to_string, add_commit, add_staged_commit, delta_path_for_version, TestAction,
};

/// Helper function to create a LogPath for a commit at the given version
fn create_log_path(table_root: &Url, commit_path: Path) -> LogPath {
    let file_meta = create_file_meta(table_root, commit_path);
    LogPath::try_new(file_meta).expect("Failed to create LogPath")
}

fn create_file_meta(table_root: &Url, commit_path: Path) -> FileMeta {
    let commit_url = table_root.join(commit_path.as_ref()).unwrap();
    FileMeta {
        location: commit_url,
        last_modified: 123,
        size: 100, // arbitrary size
    }
}

fn setup_test() -> (
    Arc<InMemory>,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Url,
) {
    let storage = Arc::new(InMemory::new());
    let table_root = Url::parse("memory:///").unwrap();
    let engine = Arc::new(DefaultEngine::new(
        storage.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));
    (storage, engine, table_root)
}

#[tokio::test]
async fn basic_snapshot_with_log_tail_staged_commits() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // with staged commits:
    // _delta_log/0.json (PM in here)
    // _delta_log/_staged_commits/1.uuid.json
    // _delta_log/_staged_commits/1.uuid.json // add an unused staged commit at version 1
    // _delta_log/_staged_commits/2.uuid.json
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let path1 = add_staged_commit(storage.as_ref(), 1, String::from("{}")).await?;
    let _ = add_staged_commit(storage.as_ref(), 1, String::from("{}")).await?;
    let path2 = add_staged_commit(storage.as_ref(), 2, String::from("{}")).await?;

    // 1. Create log_tail for commits 1, 2
    let log_tail = vec![
        create_log_path(&table_root, path1.clone()),
        create_log_path(&table_root, path2.clone()),
    ];
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail.clone())
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 2);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 3);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.ascending_commit_files[1].location.location,
        table_root.join(path1.as_ref())?
    );
    // version 2 is staged commit
    assert_eq!(
        log_segment.ascending_commit_files[2].location.location,
        table_root.join(path2.as_ref())?
    );

    // 2. Now check for time-travel to 1
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 2);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.ascending_commit_files[1].location.location,
        table_root.join(path1.as_ref())?
    );

    // 3. Check case for log_tail is only 1 staged commit
    let log_tail = vec![create_log_path(&table_root, path1.clone())];
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 2);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.ascending_commit_files[1].location.location,
        table_root.join(path1.as_ref())?
    );

    // 4. Check if we don't pass log tail
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );

    // 5. Check duplicating log_tail with normal listed commit
    let log_tail = vec![create_log_path(
        &table_root,
        delta_path_for_version(0, "json"),
    )];
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );

    Ok(())
}

#[tokio::test]
async fn basic_snapshot_with_log_tail() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // with normal commits:
    // _delta_log/0.json
    // _delta_log/1.json
    // _delta_log/2.json
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;

    // Create log_tail for commits 1, 2
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(1, "json")),
        create_log_path(&table_root, delta_path_for_version(2, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 2);
    Ok(())
}

#[tokio::test]
async fn log_tail_behind_filesystem() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // Create commits 0, 1, 2 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;

    // log_tail BEHIND file system => must respect log_tail
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(0, "json")),
        create_log_path(&table_root, delta_path_for_version(1, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root.clone())
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

#[tokio::test]
async fn incremental_snapshot_with_log_tail() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // commits 0, 1, 2 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;

    // initial snapshot at version 1
    let initial_snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(initial_snapshot.version(), 1);

    // add commit 3, 4
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    let path3 = add_staged_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    let path4 = add_staged_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // log_tail with commits 2, 3, 4
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, path3),
        create_log_path(&table_root, path4),
    ];

    // Build incremental snapshot with log_tail
    let new_snapshot = Snapshot::builder_from(initial_snapshot)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // Verify we advanced to version 4
    assert_eq!(new_snapshot.version(), 4);

    Ok(())
}

#[tokio::test]
async fn log_tail_exceeds_requested_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // commits 0, 1, 2, 3, 4 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // log tail goes up to version 4
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(1, "json")),
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, delta_path_for_version(3, "json")),
        create_log_path(&table_root, delta_path_for_version(4, "json")),
    ];

    // user asks for version 3 (or catalog says latest is 3)
    let snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(3)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // Should stop at version 3 even though log tail has version 4
    assert_eq!(snapshot.version(), 3);
    Ok(())
}

#[tokio::test]
async fn log_tail_behind_requested_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // create commits 0, 1, 2, 3, 4 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // Log tail only goes up to version 3
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(1, "json")),
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, delta_path_for_version(3, "json")),
    ];

    // User asks for version 4, but log tail only has up to version 3
    // This should fail with an error
    let result = Snapshot::builder_for(table_root.clone())
        .at_version(4)
        .with_log_tail(log_tail)
        .build(engine.as_ref());

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("LogSegment end version 3 not the same as the specified end version 4"));

    Ok(())
}
