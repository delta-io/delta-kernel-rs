use std::sync::Arc;

use object_store::memory::InMemory;
use url::Url;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;

use test_utils::{actions_to_string, add_commit, create_table, TestAction};

fn simple_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("val", DataType::STRING),
        ])
        .unwrap(),
    )
}

#[test_log::test(tokio::test)]
async fn test_max_catalog_version_latest_query() -> Result<(), Box<dyn std::error::Error>> {
    // No time-travel + max_catalog_version=0 → snapshot at version 0 (not 1)
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///")?;
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    create_table(
        store.clone(),
        table_url.clone(),
        simple_schema(),
        &[],
        true,
        vec!["catalogManaged"],
        vec!["catalogManaged"],
    )
    .await?;

    // Add version 1
    let actions = actions_to_string(vec![TestAction::Add("file1.parquet".to_string())]);
    add_commit(store.as_ref(), 1, actions).await?;

    let snapshot = Snapshot::builder_for(table_url)
        .with_max_catalog_version(0)
        .build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_max_catalog_version_time_travel_within_range(
) -> Result<(), Box<dyn std::error::Error>> {
    // time_travel_version=0, max_catalog_version=1 → success at version 0
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///")?;
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    create_table(
        store.clone(),
        table_url.clone(),
        simple_schema(),
        &[],
        true,
        vec!["catalogManaged"],
        vec!["catalogManaged"],
    )
    .await?;

    // Add version 1
    let actions = actions_to_string(vec![TestAction::Add("file1.parquet".to_string())]);
    add_commit(store.as_ref(), 1, actions).await?;

    let snapshot = Snapshot::builder_for(table_url)
        .at_version(0)
        .with_max_catalog_version(1)
        .build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_ccv2_table_requires_max_version() -> Result<(), Box<dyn std::error::Error>> {
    // ccv2 table without max_catalog_version → error (post-build)
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///")?;
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    create_table(
        store.clone(),
        table_url.clone(),
        simple_schema(),
        &[],
        true,
        vec!["catalogManaged"],
        vec!["catalogManaged"],
    )
    .await?;

    let result = Snapshot::builder_for(table_url).build(&engine);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Must provide max_catalog_version for catalog-managed tables"),
        "Unexpected error: {err_msg}"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_non_ccv2_table_rejects_max_version() -> Result<(), Box<dyn std::error::Error>> {
    // non-ccv2 table with max_catalog_version → error (post-build)
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///")?;
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    create_table(
        store.clone(),
        table_url.clone(),
        simple_schema(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    // Add version 1
    let actions = actions_to_string(vec![TestAction::Add("file1.parquet".to_string())]);
    add_commit(store.as_ref(), 1, actions).await?;

    let result = Snapshot::builder_for(table_url)
        .with_max_catalog_version(1)
        .build(&engine);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Must not provide max_catalog_version for non-catalog-managed tables"),
        "Unexpected error: {err_msg}"
    );

    Ok(())
}
