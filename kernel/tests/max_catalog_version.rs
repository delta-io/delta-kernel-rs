use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::Snapshot;
use object_store::memory::InMemory;
use url::Url;

use test_utils::{
    actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit, TestAction,
};

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

#[tokio::test]
async fn test_max_catalog_version_latest_query() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, _table_root) = setup_test();

    // Create catalog-managed table with commits 0, 1
    let actions = vec![TestAction::Metadata];
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;

    // max_catalog_version=0 -> snapshot at v0 (even though v1 exists)
    let snapshot = Snapshot::builder_for(_table_root.clone())
        .with_max_catalog_version(0)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    Ok(())
}

#[tokio::test]
async fn test_max_catalog_version_time_travel_within_range(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, _table_root) = setup_test();

    // Create catalog-managed table with commits 0, 1
    let actions = vec![TestAction::Metadata];
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;

    // at_version(0) + max_catalog_version=1 -> snapshot at v0
    let snapshot = Snapshot::builder_for(_table_root.clone())
        .at_version(0)
        .with_max_catalog_version(1)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    Ok(())
}

#[tokio::test]
async fn test_ccv2_table_requires_max_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // Create catalog-managed table
    let actions = vec![TestAction::Metadata];
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;

    // No max_catalog_version -> error
    let result = Snapshot::builder_for(table_root.clone()).build(engine.as_ref());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Catalog-managed table requires max_catalog_version"));

    Ok(())
}

#[tokio::test]
async fn test_non_ccv2_table_rejects_max_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // Create non-catalog-managed table
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;

    // max_catalog_version set -> error
    let result = Snapshot::builder_for(table_root.clone())
        .with_max_catalog_version(0)
        .build(engine.as_ref());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("max_catalog_version must not be set for non-catalog-managed tables"));

    Ok(())
}

#[tokio::test]
async fn test_staged_commits_with_max_catalog_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // Create catalog-managed table with staged commits
    let actions = vec![TestAction::Metadata];
    add_commit(
        storage.as_ref(),
        0,
        actions_to_string_catalog_managed(actions),
    )
    .await?;
    let path1 = add_staged_commit(storage.as_ref(), 1, String::from("{}")).await?;

    let commit_url = table_root.join(path1.as_ref()).unwrap();
    let file_meta = delta_kernel::FileMeta {
        location: commit_url,
        last_modified: 123,
        size: 100,
    };
    let log_path = delta_kernel::LogPath::try_new(file_meta)?;

    // With max_catalog_version -> succeeds
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(vec![log_path])
        .with_max_catalog_version(1)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);

    Ok(())
}
