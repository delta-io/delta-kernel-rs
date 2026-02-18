//! Integration tests for the CreateTable API

#[path = "create_table/clustering.rs"]
mod clustering;
#[path = "create_table/column_mapping.rs"]
mod column_mapping;

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::table_properties::TableProperties;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use serde_json::Value;
use test_utils::{assert_result_error_with_message, test_table_setup};

/// Helper to create a simple two-column schema for tests.
/// Shared with sub-modules.
pub(crate) fn simple_schema() -> DeltaResult<Arc<StructType>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("value", DataType::STRING, true),
    ])?))
}

#[tokio::test]
async fn test_create_simple_table() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create schema for an events table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("event_id", DataType::LONG, false),
        StructField::new("user_id", DataType::LONG, false),
        StructField::new("event_type", DataType::STRING, false),
        StructField::new("timestamp", DataType::TIMESTAMP, false),
        StructField::new("properties", DataType::STRING, true),
    ])?);

    // Create table using new API
    let _ = create_table(&table_path, schema.clone(), "DeltaKernel-RS/0.17.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Verify table was created
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.schema().fields().len(), 5);

    // Verify protocol versions via snapshot
    let protocol = snapshot.table_configuration().protocol();
    assert_eq!(
        protocol.min_reader_version(),
        TABLE_FEATURES_MIN_READER_VERSION
    );
    assert_eq!(
        protocol.min_writer_version(),
        TABLE_FEATURES_MIN_WRITER_VERSION
    );
    // Verify no reader/writer features are set (empty for table features mode)
    assert!(protocol.reader_features().is_some_and(|f| f.is_empty()));
    assert!(protocol.writer_features().is_some_and(|f| f.is_empty()));

    // Verify no table properties are set via public API
    assert_eq!(snapshot.table_properties(), &TableProperties::default());

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
async fn test_create_table_with_user_domain_metadata() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create table with domainMetadata feature enabled
    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Add user domain metadata during table creation
    let domain = "app.settings";
    let config = r#"{"version": 1, "enabled": true}"#;

    let _ = txn
        .with_domain_metadata(domain.to_string(), config.to_string())
        .commit(engine.as_ref())?;

    // Load snapshot and verify domain metadata was persisted
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Verify domainMetadata feature is enabled in protocol
    assert!(
        snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::DomainMetadata),
        "DomainMetadata feature should be enabled"
    );

    // Verify domain metadata string was persisted correctly
    let retrieved_config = snapshot.get_domain_metadata(domain, engine.as_ref())?;
    assert_eq!(
        retrieved_config,
        Some(config.to_string()),
        "Domain metadata should be persisted and retrievable"
    );

    // Parse and verify the JSON contents
    let parsed: Value = serde_json::from_str(retrieved_config.as_ref().unwrap())?;
    assert_eq!(parsed["version"], 1);
    assert_eq!(parsed["enabled"], true);

    // Verify non-existent domain returns None
    let missing = snapshot.get_domain_metadata("nonexistent.domain", engine.as_ref())?;
    assert!(missing.is_none(), "Non-existent domain should return None");

    Ok(())
}

#[tokio::test]
async fn test_create_table_already_exists() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create schema for a user profiles table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("user_id", DataType::LONG, false),
        StructField::new("username", DataType::STRING, false),
        StructField::new("email", DataType::STRING, false),
        StructField::new("created_at", DataType::TIMESTAMP, false),
        StructField::new("is_active", DataType::BOOLEAN, false),
    ])?);

    // Create table first time
    let _ = create_table(&table_path, schema.clone(), "UserManagementService/1.2.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Try to create again - should fail at build time (table already exists)
    let result = create_table(&table_path, schema.clone(), "UserManagementService/1.2.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "already exists");

    Ok(())
}

#[tokio::test]
async fn test_create_table_empty_schema_not_supported() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create empty schema
    let schema = Arc::new(StructType::try_new(vec![])?);

    // Try to create table with empty schema - should fail at build time
    let result = create_table(&table_path, schema, "InvalidApp/0.1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "cannot be empty");

    Ok(())
}

#[tokio::test]
async fn test_create_table_log_actions() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

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
    assert_eq!(
        protocol["minReaderVersion"],
        TABLE_FEATURES_MIN_READER_VERSION
    );
    assert_eq!(
        protocol["minWriterVersion"],
        TABLE_FEATURES_MIN_WRITER_VERSION
    );

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
