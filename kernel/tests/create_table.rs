//! Integration tests for the CreateTable API
//!
//! These tests use the kernel's own types to validate that the kernel can correctly
//! read back what it writes - a true round-trip integration test.

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::actions::DomainMetadata;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, MetadataValue, SchemaRef, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::{create_table, CommitResult};
use delta_kernel::{DeltaResult, Engine};
use serde_json::Value;
use tempfile::tempdir;
use test_utils::create_default_engine;

// ============================================================================
// Kernel-Based Validation Infrastructure
// ============================================================================

/// Helper to build a Snapshot from a table path after commit.
fn build_snapshot(table_path: &str, engine: &dyn Engine) -> DeltaResult<Arc<Snapshot>> {
    let table_url = delta_kernel::try_parse_uri(table_path)?;
    Snapshot::builder_for(table_url).build(engine)
}

// ============================================================================
// Protocol Feature Validation
// ============================================================================

/// Validates that the protocol has a specific writer feature.
fn assert_has_writer_feature(snapshot: &Snapshot, feature: TableFeature) {
    let protocol = snapshot.table_configuration().protocol();
    let features = protocol
        .writer_features()
        .expect("Protocol should have writer features");
    assert!(
        features.contains(&feature),
        "Protocol should have writer feature '{:?}'. Found: {:?}",
        feature,
        features
    );
}

/// Validates that the protocol has a specific reader feature.
fn assert_has_reader_feature(snapshot: &Snapshot, feature: TableFeature) {
    let protocol = snapshot.table_configuration().protocol();
    let features = protocol
        .reader_features()
        .expect("Protocol should have reader features");
    assert!(
        features.contains(&feature),
        "Protocol should have reader feature '{:?}'. Found: {:?}",
        feature,
        features
    );
}

// ============================================================================
// Configuration Validation
// ============================================================================

/// Validates that a configuration property exists with expected value.
fn assert_config_equals(snapshot: &Snapshot, key: &str, expected: &str) {
    let config = snapshot.table_configuration().metadata().configuration();
    let actual = config.get(key);
    assert_eq!(
        actual,
        Some(&expected.to_string()),
        "Configuration '{}' should be '{}', but was {:?}",
        key,
        expected,
        actual
    );
}

/// Validates that a configuration property exists.
fn assert_config_exists(snapshot: &Snapshot, key: &str) {
    let config = snapshot.table_configuration().metadata().configuration();
    assert!(
        config.contains_key(key),
        "Configuration should have key '{}'. Available keys: {:?}",
        key,
        config.keys().collect::<Vec<_>>()
    );
}

// ============================================================================
// Column Mapping Validation
// ============================================================================

/// Validates column mapping annotations on all schema fields using kernel types.
struct ColumnMappingValidator<'a> {
    schema: &'a SchemaRef,
    expected_fields: Vec<&'a str>,
}

impl<'a> ColumnMappingValidator<'a> {
    fn new(schema: &'a SchemaRef, expected_fields: Vec<&'a str>) -> Self {
        Self {
            schema,
            expected_fields,
        }
    }

    fn validate(&self) {
        let mut seen_ids: HashSet<i64> = HashSet::new();
        let mut seen_physical_names: HashSet<String> = HashSet::new();
        let mut seen_field_names: HashSet<String> = HashSet::new();

        for field in self.schema.fields() {
            let metadata = field.metadata();
            let field_name = field.name();

            // Validate column ID exists and is unique
            let column_id = match metadata.get("delta.columnMapping.id") {
                Some(MetadataValue::Number(id)) => *id,
                other => panic!(
                    "Field '{}' should have delta.columnMapping.id as Number, got {:?}",
                    field_name, other
                ),
            };
            assert!(
                seen_ids.insert(column_id),
                "Duplicate column mapping ID {} for field '{}'",
                column_id,
                field_name
            );

            // Validate physical name exists, has correct format, and is unique
            let physical_name = match metadata.get("delta.columnMapping.physicalName") {
                Some(MetadataValue::String(name)) => name.clone(),
                other => panic!(
                    "Field '{}' should have delta.columnMapping.physicalName as String, got {:?}",
                    field_name, other
                ),
            };
            assert!(
                physical_name.starts_with("col-"),
                "Field '{}' physical name '{}' should start with 'col-'",
                field_name,
                physical_name
            );
            assert!(
                seen_physical_names.insert(physical_name.clone()),
                "Duplicate physical name '{}' for field '{}'",
                physical_name,
                field_name
            );

            seen_field_names.insert(field_name.to_string());
        }

        // Validate all expected fields are present
        for expected in &self.expected_fields {
            assert!(
                seen_field_names.contains(*expected),
                "Expected field '{}' not found in schema",
                expected
            );
        }

        // Validate IDs are sequential: {1, 2, ..., N}
        let expected_ids: HashSet<i64> = (1..=self.expected_fields.len() as i64).collect();
        assert_eq!(
            seen_ids,
            expected_ids,
            "Column IDs should be sequential from 1 to {}. Found: {:?}",
            self.expected_fields.len(),
            seen_ids
        );
    }
}

// ============================================================================
// Clustering Domain Metadata Validation
// ============================================================================

/// Validates that clustering uses physical column names using kernel types.
struct ClusteringValidator<'a> {
    domain_metadata: &'a [DomainMetadata],
    logical_column_names: Vec<&'a str>,
}

impl<'a> ClusteringValidator<'a> {
    fn new(domain_metadata: &'a [DomainMetadata], logical_column_names: Vec<&'a str>) -> Self {
        Self {
            domain_metadata,
            logical_column_names,
        }
    }

    fn validate(&self) {
        // Find delta.clustering domain
        let clustering_domain = self
            .domain_metadata
            .iter()
            .find(|dm| dm.domain() == "delta.clustering")
            .expect("Should have delta.clustering domain metadata");

        // Parse the configuration JSON
        let config: Value = serde_json::from_str(clustering_domain.configuration())
            .expect("Invalid clustering configuration JSON");

        let clustering_columns = config
            .get("clusteringColumns")
            .and_then(|v| v.as_array())
            .expect("Clustering config should have clusteringColumns array");

        assert!(
            !clustering_columns.is_empty(),
            "Should have clustering columns defined"
        );

        for column_path in clustering_columns {
            let segments = column_path
                .as_array()
                .expect("Each clustering column should be an array of path segments");

            for segment in segments {
                let segment_str = segment.as_str().expect("Path segment should be a string");

                // Verify uses physical name format
                assert!(
                    segment_str.starts_with("col-"),
                    "Clustering column should use physical name (col-*), but found: '{}'",
                    segment_str
                );

                // Verify logical name is NOT present
                for logical_name in &self.logical_column_names {
                    assert_ne!(
                        segment_str, *logical_name,
                        "Clustering should use physical name, not logical name '{}'",
                        logical_name
                    );
                }
            }
        }
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

/// Test that delta.feature.X = supported adds features to the protocol
#[test]
fn test_feature_overrides_add_to_protocol() -> DeltaResult<()> {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![StructField::new(
        "id",
        DataType::LONG,
        false,
    )])?);

    // Use feature overrides to enable deletionVectors (ReaderWriter) and changeDataFeed (Writer)
    let _ = create_table(&table_path, schema, "FeatureTest/1.0")
        .with_table_properties([
            // Feature overrides (should be consumed, not stored)
            ("delta.feature.deletionVectors", "supported"),
            ("delta.feature.changeDataFeed", "supported"),
            // Regular table properties (should be stored)
            ("delta.enableDeletionVectors", "true"),
            ("delta.enableChangeDataFeed", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path)?;

    // Find the protocol line
    let protocol_line = log_contents
        .lines()
        .find(|line| line.contains("\"protocol\""))
        .expect("Protocol action not found");

    // Verify deletionVectors is in BOTH readerFeatures and writerFeatures (ReaderWriter feature)
    assert!(
        protocol_line.contains("\"readerFeatures\""),
        "Protocol should have readerFeatures"
    );
    assert!(
        protocol_line.contains("\"writerFeatures\""),
        "Protocol should have writerFeatures"
    );
    assert!(
        protocol_line.contains("deletionVectors"),
        "deletionVectors should be in protocol features"
    );

    // Verify changeDataFeed is in writerFeatures only (Writer feature)
    assert!(
        protocol_line.contains("changeDataFeed"),
        "changeDataFeed should be in protocol features"
    );

    // Find the metadata line
    let metadata_line = log_contents
        .lines()
        .find(|line| line.contains("\"metaData\""))
        .expect("Metadata action not found");

    // Verify feature override properties are NOT in metadata configuration
    assert!(
        !metadata_line.contains("delta.feature.deletionVectors"),
        "Feature override should not be stored in metadata"
    );
    assert!(
        !metadata_line.contains("delta.feature.changeDataFeed"),
        "Feature override should not be stored in metadata"
    );

    // Verify regular properties ARE in metadata configuration
    assert!(
        metadata_line.contains("delta.enableDeletionVectors"),
        "Regular property should be stored in metadata"
    );
    assert!(
        metadata_line.contains("delta.enableChangeDataFeed"),
        "Regular property should be stored in metadata"
    );

    Ok(())
}

/// Test that invalid feature override values are rejected
#[test]
fn test_feature_override_invalid_value() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))
            .expect("Failed to create engine");

    let schema = Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::LONG, false)])
            .expect("Invalid schema"),
    );

    // Try to use an invalid value for feature override
    let result = create_table(&table_path, schema, "FeatureTest/1.0")
        .with_table_properties([("delta.feature.deletionVectors", "enabled")]) // Wrong! Should be "supported"
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Invalid value"),
        "Error should mention invalid value"
    );
    assert!(
        err_msg.contains("supported"),
        "Error should mention 'supported' as valid value"
    );
}

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
    ColumnMappingValidator::new(&schema, vec!["id", "name"]).validate();

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
    ColumnMappingValidator::new(&schema, vec!["a", "b", "c"]).validate();

    // Intent: Verify maxColumnId = 3 (for 3 columns)
    assert_config_equals(&snapshot, "delta.columnMapping.maxColumnId", "3");

    Ok(())
}

/// Test creating a clustered table with column mapping uses physical names
#[test]
fn test_clustered_table_with_column_mapping() -> DeltaResult<()> {
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::transaction::DataLayout;

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
        .with_data_layout(DataLayout::Clustered(vec![ColumnName::new(["id"])]))
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
    ColumnMappingValidator::new(&schema, vec!["id", "name"]).validate();

    // Intent: Validate clustering uses physical names (not logical "id")
    // Use the new unfiltered API to access system domain metadata
    let all_domain_metadata = snapshot.get_all_domain_metadata_unfiltered(engine.as_ref())?;
    ClusteringValidator::new(&all_domain_metadata, vec!["id"]).validate();

    Ok(())
}

// ============================================================================
// In-Commit Timestamp (ICT) Tests
// ============================================================================

#[tokio::test]
async fn test_create_table_with_in_commit_timestamp() -> DeltaResult<()> {
    // This test verifies that when the InCommitTimestamp feature is enabled,
    // the create table commit includes inCommitTimestamp in the commitInfo.

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let table_path = temp_dir.path().to_str().expect("Invalid path").to_string();

    let engine =
        create_default_engine(&url::Url::from_directory_path(&table_path).expect("Invalid URL"))?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("value", DataType::STRING, true),
    ])?);

    // Enable InCommitTimestamp feature via table property
    let _ = create_table(&table_path, schema, "ICTTest/1.0")
        .with_table_properties([
            ("delta.feature.inCommitTimestamp", "supported"),
            ("delta.enableInCommitTimestamps", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the Delta log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");

    // Parse each line to find commitInfo
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    // Find commitInfo action (should be first)
    let commit_info_action = &actions[0];
    assert!(
        commit_info_action.get("commitInfo").is_some(),
        "First action should be commitInfo"
    );

    let commit_info = commit_info_action.get("commitInfo").unwrap();

    // Verify inCommitTimestamp is present
    assert!(
        commit_info.get("inCommitTimestamp").is_some(),
        "CommitInfo should have inCommitTimestamp when ICT feature is enabled"
    );

    let ict = commit_info
        .get("inCommitTimestamp")
        .unwrap()
        .as_i64()
        .expect("inCommitTimestamp should be an integer");

    // ICT should be a reasonable timestamp (after year 2020)
    let year_2020_ms = 1577836800000i64; // Jan 1, 2020 00:00:00 UTC
    assert!(
        ict > year_2020_ms,
        "inCommitTimestamp ({}) should be greater than year 2020 ({})",
        ict,
        year_2020_ms
    );

    // Verify the timestamp field is also present and matches or is close to ICT
    let timestamp = commit_info
        .get("timestamp")
        .unwrap()
        .as_i64()
        .expect("timestamp should be an integer");

    // For create table, ICT should equal the commit timestamp
    assert_eq!(
        ict, timestamp,
        "For create table, inCommitTimestamp should equal timestamp"
    );

    // Verify protocol has inCommitTimestamp in writerFeatures
    let protocol_action = &actions[1];
    let protocol = protocol_action.get("protocol").unwrap();
    let writer_features = protocol
        .get("writerFeatures")
        .expect("Protocol should have writerFeatures");
    let writer_features_arr = writer_features.as_array().unwrap();
    assert!(
        writer_features_arr
            .iter()
            .any(|f| f.as_str() == Some("inCommitTimestamp")),
        "writerFeatures should contain inCommitTimestamp"
    );

    Ok(())
}
