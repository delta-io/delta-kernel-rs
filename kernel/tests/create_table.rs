//! Integration tests for the CreateTable API

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use serde_json::Value;
use tempfile::tempdir;
use test_utils::create_default_engine;

#[tokio::test]
async fn test_create_simple_table() -> DeltaResult<()> {
    // Setup
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    // Create schema for an events table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("event_id", DataType::LONG, false),
        StructField::new("user_id", DataType::LONG, false),
        StructField::new("event_type", DataType::STRING, false),
        StructField::new("timestamp", DataType::TIMESTAMP, false),
        StructField::new("properties", DataType::STRING, true),
    ])?);

    // Create table using new API
    let _result = create_table(&table_path, schema.clone(), "DeltaKernel-RS/0.17.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Verify table was created
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.schema().fields().len(), 5);

    // Verify schema field names
    let field_names: Vec<_> = snapshot
        .schema()
        .fields()
        .map(|f| f.name().to_string())
        .collect();
    assert!(field_names.contains(&"event_id".to_string()));
    assert!(field_names.contains(&"user_id".to_string()));
    assert!(field_names.contains(&"event_type".to_string()));
    assert!(field_names.contains(&"timestamp".to_string()));
    assert!(field_names.contains(&"properties".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_create_table_with_properties() -> DeltaResult<()> {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("name", DataType::STRING, false),
    ])?);

    // Test multiple with_table_properties calls (builder pattern - properties merge)
    let _result = create_table(&table_path, schema.clone(), "TestApp/1.0")
        .with_table_properties([
            // Delta properties (parsed into typed fields)
            ("delta.appendOnly", "true"),
            ("delta.checkpointInterval", "100"),
        ])
        .with_table_properties([
            ("delta.deletedFileRetentionDuration", "interval 14 days"),
            ("delta.logRetentionDuration", "interval 30 days"),
            ("delta.autoOptimize.optimizeWrite", "true"),
            // Custom properties (stored in unknown_properties)
            ("myapp.version", "2.1.0"),
            ("owner", "data-platform-team"),
            ("data.classification", "internal"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Verify properties are parsed back correctly
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);

    let props = snapshot
        .table_configuration()
        .metadata()
        .parse_table_properties();

    assert_eq!(props.append_only, Some(true));
    assert_eq!(
        props.checkpoint_interval,
        Some(std::num::NonZero::new(100).unwrap())
    );
    assert_eq!(
        props.deleted_file_retention_duration,
        Some(std::time::Duration::from_secs(14 * 24 * 60 * 60))
    );
    assert_eq!(
        props.log_retention_duration,
        Some(std::time::Duration::from_secs(30 * 24 * 60 * 60))
    );
    assert_eq!(props.optimize_write, Some(true));

    // Verify custom properties are in unknown_properties
    assert_eq!(
        props.unknown_properties.get("myapp.version"),
        Some(&"2.1.0".to_string())
    );
    assert_eq!(
        props.unknown_properties.get("owner"),
        Some(&"data-platform-team".to_string())
    );
    assert_eq!(
        props.unknown_properties.get("data.classification"),
        Some(&"internal".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_create_table_already_exists() -> DeltaResult<()> {
    // Setup
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    // Create schema for a user profiles table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("user_id", DataType::LONG, false),
        StructField::new("username", DataType::STRING, false),
        StructField::new("email", DataType::STRING, false),
        StructField::new("created_at", DataType::TIMESTAMP, false),
        StructField::new("is_active", DataType::BOOLEAN, false),
    ])?);

    // Create table first time
    let _result = create_table(&table_path, schema.clone(), "UserManagementService/1.2.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Try to create again - should fail at build time (table already exists)
    let result = create_table(&table_path, schema.clone(), "UserManagementService/1.2.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("already exists"));

    Ok(())
}

#[tokio::test]
async fn test_create_table_empty_schema() -> DeltaResult<()> {
    // Setup
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    // Create empty schema
    let schema = Arc::new(StructType::try_new(vec![])?);

    // Try to create table with empty schema - should fail at build time
    let result = create_table(&table_path, schema, "InvalidApp/0.1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));

    Ok(())
}

#[tokio::test]
async fn test_create_table_log_actions() -> DeltaResult<()> {
    // Setup
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    // Create schema
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("user_id", DataType::LONG, false),
        StructField::new("action", DataType::STRING, false),
    ])?);

    let engine_info = "AuditService/2.1.0";

    // Create table
    let _ = create_table(&table_path, schema, engine_info)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the actual Delta log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");

    // Parse each line (each line is a separate JSON action)
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    // Verify we have exactly 3 actions: CommitInfo, Protocol, Metadata
    // CommitInfo is first to comply with ICT (In-Commit Timestamps) protocol requirements
    assert_eq!(
        actions.len(),
        3,
        "Expected 3 actions (commitInfo, protocol, metaData), found {}",
        actions.len()
    );

    // Verify CommitInfo action (first for ICT compliance)
    let commit_info_action = &actions[0];
    assert!(
        commit_info_action.get("commitInfo").is_some(),
        "First action should be commitInfo"
    );
    let commit_info = commit_info_action.get("commitInfo").unwrap();
    assert!(
        commit_info.get("timestamp").is_some(),
        "CommitInfo should have timestamp"
    );
    assert!(
        commit_info.get("engineInfo").is_some(),
        "CommitInfo should have engineInfo"
    );
    assert!(
        commit_info.get("operation").is_some(),
        "CommitInfo should have operation"
    );
    assert_eq!(
        commit_info["operation"], "CREATE TABLE",
        "Operation should be CREATE TABLE"
    );

    // Verify Protocol action
    let protocol_action = &actions[1];
    assert!(
        protocol_action.get("protocol").is_some(),
        "Second action should be protocol"
    );
    let protocol = protocol_action.get("protocol").unwrap();
    assert_eq!(protocol["minReaderVersion"], 3);
    assert_eq!(protocol["minWriterVersion"], 7);

    // Verify Metadata action
    let metadata_action = &actions[2];
    assert!(
        metadata_action.get("metaData").is_some(),
        "Third action should be metaData"
    );
    let metadata = metadata_action.get("metaData").unwrap();
    assert!(metadata.get("id").is_some(), "Metadata should have id");
    assert!(
        metadata.get("schemaString").is_some(),
        "Metadata should have schemaString"
    );
    assert!(
        metadata.get("createdTime").is_some(),
        "Metadata should have createdTime"
    );

    // Additional CommitInfo verification (commit_info was already extracted from actions[0] above)
    assert_eq!(
        commit_info["engineInfo"], engine_info,
        "CommitInfo should contain the engine info we provided"
    );

    assert!(
        commit_info.get("txnId").is_some(),
        "CommitInfo should have txnId"
    );

    // Verify kernelVersion is present
    let kernel_version = commit_info.get("kernelVersion");
    assert!(
        kernel_version.is_some(),
        "CommitInfo should have kernelVersion"
    );
    assert!(
        kernel_version.unwrap().as_str().unwrap().starts_with("v"),
        "Kernel version should start with 'v'"
    );

    Ok(())
}
