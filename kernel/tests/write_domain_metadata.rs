//! Integration tests for domain metadata set/remove flows.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::Snapshot;
use itertools::Itertools;
use serde_json::Deserializer;
use test_utils::{assert_result_error_with_message, create_table, engine_store_setup};

mod common;

use common::write_utils::get_simple_int_schema;

#[tokio::test]
async fn test_set_domain_metadata_basic() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_domain_metadata_basic";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;

    // write context does not conflict with domain metadata
    let _write_context = txn.unpartitioned_write_context().unwrap();

    // set multiple domain metadata
    let domain1 = "app.config";
    let config1 = r#"{"version": 1}"#;
    let domain2 = "spark.settings";
    let config2 = r#"{"cores": 4}"#;

    assert!(txn
        .with_domain_metadata(domain1.to_string(), config1.to_string())
        .with_domain_metadata(domain2.to_string(), config2.to_string())
        .commit(&engine)?
        .is_committed());

    let commit_data = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000001.json"
        )))
        .await?
        .bytes()
        .await?;

    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_data)
        .into_iter()
        .try_collect()?;

    let domain_actions: Vec<_> = actions
        .iter()
        .filter(|v| v.get("domainMetadata").is_some())
        .collect();

    for action in &domain_actions {
        let domain = action["domainMetadata"]["domain"].as_str().unwrap();
        let config = action["domainMetadata"]["configuration"].as_str().unwrap();
        assert!(!action["domainMetadata"]["removed"].as_bool().unwrap());

        match domain {
            d if d == domain1 => assert_eq!(config, config1),
            d if d == domain2 => assert_eq!(config, config2),
            _ => panic!("Unexpected domain: {domain}"),
        }
    }

    let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let domain1_config = final_snapshot.get_domain_metadata(domain1, &engine)?;
    assert_eq!(domain1_config, Some(config1.to_string()));
    let domain2_config = final_snapshot.get_domain_metadata(domain2, &engine)?;
    assert_eq!(domain2_config, Some(config2.to_string()));
    Ok(())
}

#[tokio::test]
async fn test_set_domain_metadata_errors() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_domain_metadata_errors";
    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // System domain rejection
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let res = txn
        .with_domain_metadata("delta.system".to_string(), "config".to_string())
        .commit(&engine);
    assert_result_error_with_message(
        res,
        "Cannot modify domains that start with 'delta.' as those are system controlled",
    );

    // Duplicate domain rejection
    let txn2 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let res = txn2
        .with_domain_metadata("app.config".to_string(), "v1".to_string())
        .with_domain_metadata("app.config".to_string(), "v2".to_string())
        .commit(&engine);
    assert_result_error_with_message(
        res,
        "Metadata for domain app.config already specified in this transaction",
    );

    Ok(())
}

#[tokio::test]
async fn test_set_domain_metadata_unsupported_writer_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_domain_metadata_unsupported";

    // Create table WITHOUT domain metadata writer feature support
    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec![],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let res = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_domain_metadata("app.config".to_string(), "test_config".to_string())
        .commit(&engine);

    assert_result_error_with_message(res, "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature");

    Ok(())
}

#[tokio::test]
async fn test_remove_domain_metadata_unsupported_writer_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_remove_domain_metadata_unsupported";

    // Create table WITHOUT domain metadata writer feature support
    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec![],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let res = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_domain_metadata_removed("app.config".to_string())
        .commit(&engine);

    assert_result_error_with_message(res, "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature");

    Ok(())
}

#[tokio::test]
async fn test_remove_domain_metadata_non_existent_domain() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_domain_metadata_unsupported";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;

    let domain = "app.deprecated";

    // removing domain metadata that doesn't exist should NOT write a tombstone
    let _ = txn
        .with_domain_metadata_removed(domain.to_string())
        .commit(&engine)?;

    let commit_data = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000001.json"
        )))
        .await?
        .bytes()
        .await?;
    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_data)
        .into_iter()
        .try_collect()?;

    let domain_action = actions.iter().find(|v| v.get("domainMetadata").is_some());
    assert!(
        domain_action.is_none(),
        "No tombstone should be written for non-existent domain"
    );

    let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let config = final_snapshot.get_domain_metadata(domain, &engine)?;
    assert_eq!(config, None);

    Ok(())
}

#[tokio::test]
async fn test_domain_metadata_set_remove_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_domain_metadata_unsupported";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // set then remove same domain
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let err = txn
        .with_domain_metadata("app.config".to_string(), "v1".to_string())
        .with_domain_metadata_removed("app.config".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("already specified in this transaction"));

    // remove then set same domain
    let txn2 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let err = txn2
        .with_domain_metadata_removed("test.domain".to_string())
        .with_domain_metadata("test.domain".to_string(), "v1".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("already specified in this transaction"));

    // remove same domain twice
    let txn3 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let err = txn3
        .with_domain_metadata_removed("another.domain".to_string())
        .with_domain_metadata_removed("another.domain".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("already specified in this transaction"));

    // remove system domain
    let txn4 = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let err = txn4
        .with_domain_metadata_removed("delta.system".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Cannot modify domains that start with 'delta.' as those are system controlled"));

    Ok(())
}

#[tokio::test]
async fn test_domain_metadata_set_then_remove() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let table_name = "test_domain_metadata_unsupported";

    let (store, engine, table_location) = engine_store_setup(table_name, None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["domainMetadata"],
    )
    .await?;

    let domain = "app.config";
    let configuration = r#"{"version": 1}"#;

    // txn 1: set domain metadata
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let _ = txn
        .with_domain_metadata(domain.to_string(), configuration.to_string())
        .commit(&engine)?;

    // txn 2: remove the same domain metadata
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let _ = txn
        .with_domain_metadata_removed(domain.to_string())
        .commit(&engine)?;

    // verify removal commit preserves the previous configuration
    let commit_data = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000002.json"
        )))
        .await?
        .bytes()
        .await?;
    let actions: Vec<serde_json::Value> = Deserializer::from_slice(&commit_data)
        .into_iter()
        .try_collect()?;

    let domain_action = actions
        .iter()
        .find(|v| v.get("domainMetadata").is_some())
        .unwrap();
    assert_eq!(domain_action["domainMetadata"]["domain"], domain);
    assert_eq!(
        domain_action["domainMetadata"]["configuration"],
        configuration
    );
    assert_eq!(domain_action["domainMetadata"]["removed"], true);

    // verify reads see the metadata removal
    let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let domain_config = final_snapshot.get_domain_metadata(domain, &engine)?;
    assert_eq!(domain_config, None);

    Ok(())
}
