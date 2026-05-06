//! Integration tests for the CreateTable API

mod clustering;
mod column_mapping;
mod ctas;
mod ict;
mod partitioned;
mod row_tracking;
mod timestamp_ntz;
mod variant;

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::table_properties::TableProperties;
use delta_kernel::test_utils::{assert_result_error_with_message, test_table_setup};
use delta_kernel::transaction::create_table::{create_table, CreateTableTransaction};
use delta_kernel::DeltaResult;
use rstest::rstest;
use serde_json::Value;

/// Helper to create a simple two-column schema for tests.
/// Shared with sub-modules.
pub(crate) fn simple_schema() -> DeltaResult<Arc<StructType>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ])?))
}

/// Helper to create a three-column schema for partition tests (id, date, value).
/// Shared with sub-modules.
pub(crate) fn partition_test_schema() -> DeltaResult<Arc<StructType>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("date", DataType::DATE),
        StructField::nullable("value", DataType::STRING),
    ])?))
}

#[tokio::test]
async fn test_create_simple_table() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create schema for an events table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("event_id", DataType::LONG),
        StructField::nullable("user_id", DataType::LONG),
        StructField::nullable("event_type", DataType::STRING),
        StructField::nullable("timestamp", DataType::TIMESTAMP),
        StructField::nullable("properties", DataType::STRING),
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
        StructField::nullable("user_id", DataType::LONG),
        StructField::nullable("username", DataType::STRING),
        StructField::nullable("email", DataType::STRING),
        StructField::nullable("created_at", DataType::TIMESTAMP),
        StructField::nullable("is_active", DataType::BOOLEAN),
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

fn top_level_non_null_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ])
        .expect("non-null top-level schema should be valid"),
    )
}

fn nested_non_null_schema() -> Arc<StructType> {
    let nested = StructType::try_new(vec![StructField::not_null("child", DataType::INTEGER)])
        .expect("nested non-null schema should be valid");
    Arc::new(
        StructType::try_new(vec![StructField::nullable(
            "nested",
            DataType::Struct(Box::new(nested)),
        )])
        .expect("top-level nested schema should be valid"),
    )
}

/// CREATE TABLE with non-null columns succeeds and auto-enables the `invariants`
/// writer feature in the protocol. Delta-Spark treats non-null schema columns as
/// implicit invariants and requires the feature to be declared.
#[rstest]
#[case::top_level_non_null(top_level_non_null_schema())]
#[case::nested_non_null(nested_non_null_schema())]
fn test_create_table_with_non_null_columns_auto_enables_invariants(
    #[case] schema: Arc<StructType>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let protocol = snapshot.table_configuration().protocol();
    assert!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::Invariants)),
        "Invariants feature should be auto-added to writer features for non-null schema"
    );
    // Invariants is writer-only; reader features should not contain it.
    assert!(
        !protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::Invariants)),
        "Invariants should not appear in reader features"
    );

    Ok(())
}

/// CREATE TABLE with `delta.feature.invariants=supported` + a non-null schema succeeds
/// and lands the feature in writerFeatures exactly once. Non-null columns auto-enable
/// the feature and the explicit signal also tries to enable it; both paths firing
/// together must not duplicate the entry. This also verifies users can pre-enable the
/// feature so a later ALTER TABLE ADD COLUMN NOT NULL does not need a protocol upgrade.
#[tokio::test]
async fn test_create_table_with_invariants_feature_signal_allowed() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    // Non-null schema auto-enables Invariants; the feature signal also tries to add it.
    // Both code paths firing together must produce exactly one Invariants entry.
    let schema = top_level_non_null_schema();

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.invariants", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("writer features should be present");
    let invariants_count = writer_features
        .iter()
        .filter(|f| **f == TableFeature::Invariants)
        .count();
    assert_eq!(
        invariants_count, 1,
        "Invariants should appear exactly once in writerFeatures; got {invariants_count}"
    );
    assert!(
        !protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::Invariants)),
        "Invariants should not appear in readerFeatures (writer-only feature)"
    );

    Ok(())
}

/// CREATE TABLE rejects any schema with `delta.invariants` metadata annotations
/// because kernel cannot evaluate SQL expression invariants.
#[tokio::test]
async fn test_create_table_rejects_delta_invariants_metadata() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let mut field = StructField::nullable("x", DataType::INTEGER);
    field.metadata.insert(
        ColumnMetadataKey::Invariants.as_ref().to_string(),
        MetadataValue::String(r#"{"expression": {"expression": "x > 0"}}"#.to_string()),
    );
    let schema = Arc::new(StructType::try_new(vec![field])?);

    let result = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "delta.invariants");

    Ok(())
}

#[tokio::test]
async fn test_create_table_log_actions() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create schema
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("user_id", DataType::LONG),
        StructField::nullable("action", DataType::STRING),
    ])?);

    let engine_info = "AuditService/2.1.0";

    // Create table
    let _ = create_table(&table_path, schema, engine_info)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the actual Delta log file
    let log_file_path = format!("{table_path}/_delta_log/00000000000000000000.json");
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

/// Helper to create a `CreateTableTransaction` for tests.
fn create_test_create_table_txn() -> DeltaResult<(
    Arc<impl delta_kernel::Engine>,
    CreateTableTransaction,
    tempfile::TempDir,
)> {
    let (tempdir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(
        StructType::try_new(vec![
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .expect("valid schema"),
    );
    let txn = create_table(&table_path, schema, "test_engine")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    Ok((engine, txn, tempdir))
}

#[tokio::test]
async fn test_create_table_txn_debug() -> DeltaResult<()> {
    let (_engine, txn, _tempdir) = create_test_create_table_txn()?;
    let debug_str = format!("{txn:?}");
    assert!(
        debug_str.contains("Transaction") && debug_str.contains("create_table"),
        "Debug output should contain Transaction info: {debug_str}"
    );
    Ok(())
}

#[rstest]
// ReaderWriter features (AlwaysIfSupported)
#[case("vacuumProtocolCheck", TableFeature::VacuumProtocolCheck, true, true)]
#[case("v2Checkpoint", TableFeature::V2Checkpoint, true, true)]
// ReaderWriter features (EnabledIf -- feature signal alone does not enable)
#[case("deletionVectors", TableFeature::DeletionVectors, true, false)]
#[case("typeWidening", TableFeature::TypeWidening, true, false)]
// WriterOnly features (EnabledIf -- feature signal alone does not enable)
#[case("appendOnly", TableFeature::AppendOnly, false, false)]
#[case("changeDataFeed", TableFeature::ChangeDataFeed, false, false)]
#[case("rowTracking", TableFeature::RowTracking, false, false)]
// WriterOnly features (AlwaysIfSupported)
#[case(
    "materializePartitionColumns",
    TableFeature::MaterializePartitionColumns,
    false,
    true
)]
fn test_create_table_with_feature_signal(
    #[case] feature_name: &str,
    #[case] feature: TableFeature,
    #[case] is_reader_writer: bool,
    #[case] enabled_when_supported: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let property_key = format!("delta.feature.{feature_name}");
    let _ = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_table_properties([(property_key.as_str(), "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let table_config = snapshot.table_configuration();

    assert!(
        table_config.is_feature_supported(&feature),
        "{feature_name} should be supported"
    );
    assert_eq!(
        table_config.is_feature_enabled(&feature),
        enabled_when_supported,
        "{feature_name}: is_feature_enabled should be {enabled_when_supported}"
    );
    let protocol = table_config.protocol();
    assert!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&feature)),
        "{feature_name} should be in writer features"
    );
    if is_reader_writer {
        assert!(
            protocol
                .reader_features()
                .is_some_and(|f| f.contains(&feature)),
            "{feature_name} should be in reader features"
        );
    }

    Ok(())
}

#[rstest]
fn test_create_table_with_checkpoint_stats_properties(
    #[values(true, false)] write_stats_as_json: bool,
    #[values(true, false)] write_stats_as_struct: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let json_val = write_stats_as_json.to_string();
    let struct_val = write_stats_as_struct.to_string();

    let _ = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_table_properties([
            ("delta.checkpoint.writeStatsAsJson", json_val.as_str()),
            ("delta.checkpoint.writeStatsAsStruct", struct_val.as_str()),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let tp = snapshot.table_properties();
    assert_eq!(tp.checkpoint_write_stats_as_json, Some(write_stats_as_json));
    assert_eq!(
        tp.checkpoint_write_stats_as_struct,
        Some(write_stats_as_struct)
    );

    Ok(())
}

#[rstest]
// ReaderWriter features
#[case("delta.enableDeletionVectors", TableFeature::DeletionVectors, true)]
#[case("delta.enableTypeWidening", TableFeature::TypeWidening, true)]
// WriterOnly features
#[case("delta.enableChangeDataFeed", TableFeature::ChangeDataFeed, false)]
#[case("delta.appendOnly", TableFeature::AppendOnly, false)]
#[case("delta.enableRowTracking", TableFeature::RowTracking, false)]
fn test_create_table_with_enablement_property(
    #[case] property: &str,
    #[case] feature: TableFeature,
    #[case] is_reader_writer: bool,
    #[values(true, false)] expect_enabled: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let value = expect_enabled.to_string();

    let _ = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_table_properties([(property, value.as_str())])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let table_config = snapshot.table_configuration();

    assert_eq!(
        table_config.is_feature_supported(&feature),
        expect_enabled,
        "{property}={value}: feature supported should be {expect_enabled}"
    );
    assert_eq!(
        table_config.is_feature_enabled(&feature),
        expect_enabled,
        "{property}={value}: feature enabled should be {expect_enabled}"
    );
    let protocol = table_config.protocol();
    assert_eq!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&feature)),
        expect_enabled,
        "{property}={value}: in writer features should be {expect_enabled}"
    );
    if is_reader_writer {
        assert_eq!(
            protocol
                .reader_features()
                .is_some_and(|f| f.contains(&feature)),
            expect_enabled,
            "{property}={value}: in reader features should be {expect_enabled}"
        );
    }

    Ok(())
}

#[rstest]
#[case::without_cm(false)]
#[case::with_cm(true)]
fn test_create_table_special_char_column_name(#[case] cm_enabled: bool) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("valid_col", DataType::INTEGER),
        StructField::nullable("bad column", DataType::STRING),
    ])?);

    let mut builder = create_table(&table_path, schema, "Test/1.0");
    if cm_enabled {
        builder = builder.with_table_properties([("delta.columnMapping.mode", "name")]);
    }
    let result = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    if cm_enabled {
        let txn = result?;
        let _ = txn.commit(engine.as_ref())?;

        let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        assert_eq!(snapshot.version(), 0);
        let field_names: Vec<_> = snapshot
            .schema()
            .fields()
            .map(|f| f.name().clone())
            .collect();
        assert!(
            field_names.contains(&"bad column".to_string()),
            "Schema should contain field 'bad column', got: {field_names:?}"
        );
    } else {
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid character"),
            "Expected invalid character error, got: {err}"
        );
    }

    Ok(())
}
