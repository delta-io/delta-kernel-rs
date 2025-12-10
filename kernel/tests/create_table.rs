//! Integration tests for the CreateTable API

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_manager::TableManager;
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
    let _result = TableManager::create_table(&table_path, schema.clone(), "DeltaKernel-RS/0.17.0")
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
    // Setup
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    // Create schema for a financial transactions table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("transaction_id", DataType::STRING, false),
        StructField::new("account_id", DataType::LONG, false),
        StructField::new("amount", DataType::decimal(18, 2)?, false),
        StructField::new("currency", DataType::STRING, false),
        StructField::new("transaction_date", DataType::DATE, false),
        StructField::new("description", DataType::STRING, true),
    ])?);

    // Create table with realistic Delta table properties
    let mut properties = HashMap::new();
    properties.insert("delta.appendOnly".to_string(), "true".to_string());
    properties.insert(
        "delta.deletedFileRetentionDuration".to_string(),
        "interval 7 days".to_string(),
    );
    properties.insert("delta.checkpointInterval".to_string(), "10".to_string());
    properties.insert(
        "delta.logRetentionDuration".to_string(),
        "interval 30 days".to_string(),
    );

    let _result = TableManager::create_table(&table_path, schema.clone(), "FinanceApp/2.5.1")
        .with_table_properties(properties.clone())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Verify table was created with properties
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);

    // Check table properties
    let table_properties = snapshot
        .table_configuration()
        .metadata()
        .parse_table_properties();
    assert_eq!(table_properties.append_only, Some(true));

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
    let _result =
        TableManager::create_table(&table_path, schema.clone(), "UserManagementService/1.2.0")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;

    // Try to create again - should fail at build time (table already exists)
    let result =
        TableManager::create_table(&table_path, schema.clone(), "UserManagementService/1.2.0")
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
    let result =
        TableManager::create_table(&table_path, schema, "InvalidApp/0.1.0")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));

    Ok(())
}

#[tokio::test]
async fn test_create_table_multiple_properties() -> DeltaResult<()> {
    // Setup
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    // Create schema for a product catalog table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("product_id", DataType::STRING, false),
        StructField::new("product_name", DataType::STRING, false),
        StructField::new("category", DataType::STRING, false),
        StructField::new("price", DataType::decimal(10, 2)?, false),
        StructField::new("inventory_count", DataType::INTEGER, false),
        StructField::new("last_updated", DataType::TIMESTAMP, false),
        StructField::new("is_available", DataType::BOOLEAN, false),
    ])?);

    // Create table with multiple property calls to test builder pattern
    let mut props1 = HashMap::new();
    props1.insert("delta.checkpointInterval".to_string(), "100".to_string());
    props1.insert(
        "delta.deletedFileRetentionDuration".to_string(),
        "interval 14 days".to_string(),
    );

    let mut props2 = HashMap::new();
    props2.insert(
        "delta.logRetentionDuration".to_string(),
        "interval 30 days".to_string(),
    );
    props2.insert(
        "delta.autoOptimize.optimizeWrite".to_string(),
        "true".to_string(),
    );

    let _result =
        TableManager::create_table(&table_path, schema.clone(), "InventorySystem/3.0.0")
            .with_table_properties(props1)
            .with_table_properties(props2)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;

    // Verify table was created
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);

    Ok(())
}

#[tokio::test]
async fn test_commit_info_is_written_to_log() -> DeltaResult<()> {
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
    let _ = TableManager::create_table(&table_path, schema, engine_info)
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

#[tokio::test]
async fn test_log_action_order() -> DeltaResult<()> {
    // This test verifies that actions are written in the correct order:
    // 1. Protocol
    // 2. Metadata
    // 3. CommitInfo
    //
    // The Delta protocol doesn't strictly require this order for initial commits,
    // but it's a best practice and required for commits with in-commit timestamps.

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![StructField::new(
        "id",
        DataType::LONG,
        false,
    )])?);

    let _ = TableManager::create_table(&table_path, schema, "OrderTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path)?;

    let lines: Vec<&str> = log_contents.lines().collect();

    // Verify order: CommitInfo first (for ICT compliance), then Protocol, then Metadata
    assert!(
        lines[0].contains("\"commitInfo\""),
        "First action should be commitInfo (for ICT compliance)"
    );
    assert!(
        lines[1].contains("\"protocol\""),
        "Second action should be protocol"
    );
    assert!(
        lines[2].contains("\"metaData\""),
        "Third action should be metaData"
    );

    Ok(())
}
