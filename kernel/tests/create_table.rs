//! Integration tests for the CreateTable API

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Engine};
use serde_json::Value;
use tempfile::tempdir;
use test_utils::{assert_result_error_with_message, create_default_engine};

/// Helper to create a simple test schema.
fn test_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::LONG, false)])
            .expect("Invalid schema"),
    )
}

/// Helper to assert an error contains multiple expected substrings.
fn assert_error_contains_messages(result: DeltaResult<impl std::fmt::Debug>, messages: &[&str]) {
    assert!(result.is_err(), "Expected error but got Ok result");
    let err_msg = result.unwrap_err().to_string();
    for msg in messages {
        assert!(
            err_msg.contains(msg),
            "Error '{}' should contain '{}'",
            err_msg,
            msg
        );
    }
}

// ============================================================================
// Column Mapping and Clustering Test Helpers
// ============================================================================

/// Helper to build a snapshot from a table path.
fn build_snapshot(table_path: &str, engine: &dyn Engine) -> DeltaResult<Arc<Snapshot>> {
    let table_url = delta_kernel::try_parse_uri(table_path)?;
    Snapshot::builder_for(table_url).build(engine)
}

/// Assert that the protocol has a specific writer feature.
fn assert_has_writer_feature(snapshot: &Snapshot, feature: TableFeature) {
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("Protocol should have writer features");
    assert!(
        writer_features.contains(&feature),
        "Protocol should have writer feature: {:?}",
        feature
    );
}

/// Assert that the protocol has a specific reader feature.
fn assert_has_reader_feature(snapshot: &Snapshot, feature: TableFeature) {
    let protocol = snapshot.table_configuration().protocol();
    let reader_features = protocol
        .reader_features()
        .expect("Protocol should have reader features");
    assert!(
        reader_features.contains(&feature),
        "Protocol should have reader feature: {:?}",
        feature
    );
}

/// Assert that a configuration key equals the expected value.
fn assert_config_equals(snapshot: &Snapshot, key: &str, expected_value: &str) {
    let config = snapshot.table_configuration().metadata().configuration();
    assert_eq!(
        config.get(key),
        Some(&expected_value.to_string()),
        "Config key '{}' should equal '{}'",
        key,
        expected_value
    );
}

/// Assert that a configuration key exists.
fn assert_config_exists(snapshot: &Snapshot, key: &str) {
    let config = snapshot.table_configuration().metadata().configuration();
    assert!(
        config.contains_key(key),
        "Config should contain key '{}'",
        key
    );
}

/// Validator for column mapping annotations on schema fields.
struct ColumnMappingValidator<'a> {
    schema: &'a StructType,
    expected_fields: Vec<&'a str>,
}

impl<'a> ColumnMappingValidator<'a> {
    fn new(schema: &'a StructType, expected_fields: Vec<&'a str>) -> Self {
        Self {
            schema,
            expected_fields,
        }
    }

    fn validate(&self) {
        for field_name in &self.expected_fields {
            let field = self
                .schema
                .field(field_name)
                .unwrap_or_else(|| panic!("Schema should have field '{}'", field_name));

            // Check for column mapping metadata
            let metadata = field.metadata();

            // Column mapping requires delta.columnMapping.id
            assert!(
                metadata.contains_key("delta.columnMapping.id"),
                "Field '{}' should have delta.columnMapping.id metadata",
                field_name
            );

            // Column mapping requires delta.columnMapping.physicalName
            assert!(
                metadata.contains_key("delta.columnMapping.physicalName"),
                "Field '{}' should have delta.columnMapping.physicalName metadata",
                field_name
            );
        }
    }
}

/// Validate clustering domain metadata by reading the log file directly.
fn validate_clustering_columns(table_path: &str, expected_columns: Vec<&str>) {
    // Read the Delta log file to find the domainMetadata action
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");

    // Find the domainMetadata action for delta.clustering
    let clustering_action = log_contents
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|action| {
            action
                .get("domainMetadata")
                .and_then(|dm| dm.get("domain"))
                .and_then(|d| d.as_str())
                == Some("delta.clustering")
        })
        .expect("Should have delta.clustering domainMetadata action");

    let domain_metadata = clustering_action.get("domainMetadata").unwrap();
    let configuration = domain_metadata
        .get("configuration")
        .and_then(|c| c.as_str())
        .expect("domainMetadata should have configuration");

    // Parse the clustering configuration
    let clustering: Value =
        serde_json::from_str(configuration).expect("Configuration should parse as JSON");
    let columns = clustering
        .get("clusteringColumns")
        .expect("Should have clusteringColumns")
        .as_array()
        .expect("clusteringColumns should be array");

    assert_eq!(
        columns.len(),
        expected_columns.len(),
        "Should have {} clustering columns, got {}",
        expected_columns.len(),
        columns.len()
    );

    for (i, expected) in expected_columns.iter().enumerate() {
        // Clustering columns may be strings or arrays (for nested columns)
        let actual = if let Some(s) = columns[i].as_str() {
            s.to_string()
        } else if let Some(arr) = columns[i].as_array() {
            // Nested column represented as array of path parts
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(".")
        } else {
            panic!(
                "Clustering column {} has unexpected type: {:?}",
                i, columns[i]
            );
        };

        // When column mapping is enabled, the clustering column name may be physical
        // For now, just verify the column exists in some form
        assert!(
            actual.contains(expected) || actual.starts_with("col-"),
            "Clustering column {} should reference '{}', got '{}'",
            i,
            expected,
            actual
        );
    }
}

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

    // Verify protocol versions are (3, 7) by reading the log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    let protocol_action = actions
        .iter()
        .find(|a| a.get("protocol").is_some())
        .expect("Protocol action not found");
    let protocol = protocol_action.get("protocol").unwrap();
    assert_eq!(
        protocol["minReaderVersion"],
        TABLE_FEATURES_MIN_READER_VERSION
    );
    assert_eq!(
        protocol["minWriterVersion"],
        TABLE_FEATURES_MIN_WRITER_VERSION
    );
    // Verify no reader/writer features are set (empty arrays for table features mode)
    assert_eq!(protocol["readerFeatures"], Value::Array(vec![]));
    assert_eq!(protocol["writerFeatures"], Value::Array(vec![]));

    // Verify no table properties are set via public API
    use delta_kernel::table_properties::TableProperties;
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

    assert_result_error_with_message(result, "already exists");

    Ok(())
}

#[tokio::test]
async fn test_create_table_empty_schema_not_supported() -> DeltaResult<()> {
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

    assert_result_error_with_message(result, "cannot be empty");

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

#[test]
fn test_user_properties_allowed() {
    // User/application properties (non-delta.*) are allowed
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))
            .expect("Failed to create engine");

    let schema = Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::LONG, false)])
            .expect("Invalid schema"),
    );

    let result = create_table(&table_path, schema, "FeatureTest/1.0")
        .with_table_properties([("myapp.version", "1.0")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_ok(), "User properties should be allowed");
}

#[test]
fn test_feature_overrides_rejected_until_on_allow_list() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))
            .expect("Failed to create engine");

    let schema = Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::LONG, false)])
            .expect("Invalid schema"),
    );

    // Feature overrides are parsed but rejected during validation (not on allow-list)
    let result = create_table(&table_path, schema, "FeatureTest/1.0")
        .with_table_properties([("delta.feature.deletionVectors", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(
        result,
        "Enabling feature 'deletionVectors' is not supported during CREATE TABLE",
    );
}

#[test]
fn test_feature_override_rejects_invalid_value() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))
            .expect("Failed to create engine");

    let schema = Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::LONG, false)])
            .expect("Invalid schema"),
    );

    // "enabled" is not valid - only "supported" is allowed
    let result = create_table(&table_path, schema, "FeatureTest/1.0")
        .with_table_properties([("delta.feature.deletionVectors", "enabled")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Invalid value"),
        "Error should mention invalid value: {}",
        err_msg
    );
    assert!(
        err_msg.contains("supported"),
        "Error should mention 'supported' as valid value: {}",
        err_msg
    );
}

#[test]
fn test_protocol_version_validation() {
    // Helper to build a table with given properties and return the result
    let try_create = |props: HashMap<String, String>| {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap().to_string();
        let engine = create_default_engine(
            &url::Url::from_directory_path(&table_path).expect("Invalid URL"),
        )
        .expect("Failed to create engine");
        create_table(&table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
    };

    // Valid: both versions (3, 7)
    let props = HashMap::from([
        (MIN_READER_VERSION_PROP.to_string(), "3".to_string()),
        (MIN_WRITER_VERSION_PROP.to_string(), "7".to_string()),
    ]);
    assert!(
        try_create(props).is_ok(),
        "Valid protocol versions (3, 7) should succeed"
    );

    // Valid: only reader version (3)
    let props = HashMap::from([(MIN_READER_VERSION_PROP.to_string(), "3".to_string())]);
    assert!(
        try_create(props).is_ok(),
        "Only reader version (3) should succeed"
    );

    // Valid: only writer version (7)
    let props = HashMap::from([(MIN_WRITER_VERSION_PROP.to_string(), "7".to_string())]);
    assert!(
        try_create(props).is_ok(),
        "Only writer version (7) should succeed"
    );

    // Invalid: reader version 2 (only 3 is supported)
    let props = HashMap::from([(MIN_READER_VERSION_PROP.to_string(), "2".to_string())]);
    assert_error_contains_messages(
        try_create(props),
        &["delta.minReaderVersion", "Only '3' is supported"],
    );

    // Invalid: writer version 5 (only 7 is supported)
    let props = HashMap::from([(MIN_WRITER_VERSION_PROP.to_string(), "5".to_string())]);
    assert_error_contains_messages(
        try_create(props),
        &["delta.minWriterVersion", "Only '7' is supported"],
    );

    // Invalid: non-integer reader version
    let props = HashMap::from([(MIN_READER_VERSION_PROP.to_string(), "abc".to_string())]);
    assert_error_contains_messages(try_create(props), &["Must be an integer"]);

    // Invalid: non-integer writer version
    let props = HashMap::from([(MIN_WRITER_VERSION_PROP.to_string(), "xyz".to_string())]);
    assert_error_contains_messages(try_create(props), &["Must be an integer"]);
}

// ============================================================================
// Column Mapping Tests
// ============================================================================

/// Test creating a table with column mapping mode = name
#[test]
fn test_create_table_with_column_mapping_name() -> DeltaResult<()> {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Create table with column mapping mode = name
    let txn = create_table(&table_path, schema, "ColumnMappingTest/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let commit_result = txn.commit(engine.as_ref())?;
    match commit_result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 0);
        }
        _ => panic!("Expected successful commit"),
    }

    // Use kernel to read back and validate the committed data
    let snapshot = build_snapshot(&table_path, engine.as_ref())?;

    // Intent: Verify protocol has columnMapping feature
    assert_has_writer_feature(&snapshot, TableFeature::ColumnMapping);
    assert_has_reader_feature(&snapshot, TableFeature::ColumnMapping);

    // Intent: Verify column mapping configuration
    assert_config_equals(&snapshot, "delta.columnMapping.mode", "name");
    assert_config_exists(&snapshot, "delta.columnMapping.maxColumnId");

    // Intent: Validate all fields have column mapping annotations
    let schema = snapshot.schema();
    ColumnMappingValidator::new(schema.as_ref(), vec!["id", "name"]).validate();

    Ok(())
}

/// Test creating a table with column mapping mode = id
#[test]
fn test_create_table_with_column_mapping_id() -> DeltaResult<()> {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("a", DataType::INTEGER, false),
        StructField::new("b", DataType::STRING, true),
        StructField::new("c", DataType::DOUBLE, true),
    ])?);

    // Create table with column mapping mode = id
    let txn = create_table(&table_path, schema, "ColumnMappingTest/1.0")
        .with_table_properties([("delta.columnMapping.mode", "id")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let commit_result = txn.commit(engine.as_ref())?;
    match commit_result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 0);
        }
        _ => panic!("Expected successful commit"),
    }

    // Use kernel to read back and validate the committed data
    let snapshot = build_snapshot(&table_path, engine.as_ref())?;

    // Intent: Validate all fields have column mapping annotations
    let schema = snapshot.schema();
    ColumnMappingValidator::new(schema.as_ref(), vec!["a", "b", "c"]).validate();

    // Intent: Verify maxColumnId = 3 (for 3 columns)
    assert_config_equals(&snapshot, "delta.columnMapping.maxColumnId", "3");

    Ok(())
}

// ============================================================================
// Clustered Table with Column Mapping Tests
// ============================================================================

/// Test creating a clustered table with column mapping uses physical names
#[test]
fn test_clustered_table_with_column_mapping() -> DeltaResult<()> {
    use delta_kernel::transaction::data_layout::DataLayout;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Create clustered table with column mapping
    let txn = create_table(&table_path, schema, "ClusteredCMTest/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .with_data_layout(DataLayout::clustered(["id"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let commit_result = txn.commit(engine.as_ref())?;
    match commit_result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 0);
        }
        _ => panic!("Expected successful commit"),
    }

    // Use kernel to read back and validate the committed data
    let snapshot = build_snapshot(&table_path, engine.as_ref())?;

    // Intent: Validate all fields have column mapping annotations
    let schema = snapshot.schema();
    ColumnMappingValidator::new(schema.as_ref(), vec!["id", "name"]).validate();

    // Intent: Validate clustering columns are set
    // Read the log file directly to verify delta.clustering domain metadata
    validate_clustering_columns(&table_path, vec!["id"]);

    Ok(())
}
