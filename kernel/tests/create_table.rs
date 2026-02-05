//! Integration tests for the CreateTable API

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use serde_json::Value;
use test_utils::{assert_result_error_with_message, test_table_setup};

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
    let _result = create_table(&table_path, schema.clone(), "DeltaKernel-RS/0.17.0")
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
    assert!(protocol.reader_features().map_or(false, |f| f.is_empty()));
    assert!(protocol.writer_features().map_or(false, |f| f.is_empty()));

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

#[tokio::test]
async fn test_create_clustered_table() -> DeltaResult<()> {
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::table_features::TableFeature;
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create schema for a clustered table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
        StructField::new("timestamp", DataType::TIMESTAMP, false),
    ])?);

    // Create clustered table on "id" column
    let txn = create_table(&table_path, schema.clone(), "DeltaKernel-RS/Test")
        .with_data_layout(DataLayout::clustered(["id"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols.iter().any(|c| c.to_string() == "id"),
        "Clustering column 'id' should be in stats columns"
    );

    // Commit the table
    let _result = txn.commit(engine.as_ref())?;

    // Verify clustering columns via snapshot read path
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let clustering_columns = snapshot.get_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_columns, Some(vec![ColumnName::new(["id"])]));

    // Verify protocol has required features
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("Writer features should exist");
    assert!(
        writer_features.contains(&TableFeature::DomainMetadata),
        "Protocol should have domainMetadata writer feature"
    );
    assert!(
        writer_features.contains(&TableFeature::ClusteredTable),
        "Protocol should have clustering writer feature"
    );

    Ok(())
}

/// Test that combining explicit feature signals with auto-enabled features doesn't create duplicates.
///
/// This tests the edge case where a user provides `delta.feature.domainMetadata=supported`
/// AND uses `DataLayout::Clustered`. Both would try to add DomainMetadata, but we should
/// only have it once in the feature lists.
#[tokio::test]
async fn test_clustering_with_explicit_feature_signal_no_duplicates() -> DeltaResult<()> {
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::table_features::TableFeature;
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![StructField::new(
        "id",
        DataType::INTEGER,
        false,
    )])?);

    // Combine BOTH: explicit feature signal AND clustering (which auto-adds domainMetadata)
    let _result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .with_data_layout(DataLayout::clustered(["id"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read back using kernel APIs and verify no duplicate features
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("Writer features should exist");

    // Count occurrences of DomainMetadata - should be exactly 1, not 2
    let domain_metadata_count = writer_features
        .iter()
        .filter(|f| **f == TableFeature::DomainMetadata)
        .count();

    assert_eq!(
        domain_metadata_count, 1,
        "domainMetadata should appear exactly once, not {} times (duplicate detected!)",
        domain_metadata_count
    );

    // Verify clustering columns via snapshot read path
    let clustering_columns = snapshot.get_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_columns, Some(vec![ColumnName::new(["id"])]));

    Ok(())
}

#[tokio::test]
async fn test_clustering_stats_columns_within_limit() -> DeltaResult<()> {
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Build schema with 10 columns (cluster on column 5, within default 32 limit)
    let fields: Vec<StructField> = (0..10)
        .map(|i| StructField::new(format!("col{}", i), DataType::INTEGER, true))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table on col5
    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["col5"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols.iter().any(|c| c.to_string() == "col5"),
        "Clustering column col5 should be in stats columns"
    );

    Ok(())
}

#[tokio::test]
async fn test_clustering_stats_columns_beyond_limit() -> DeltaResult<()> {
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Build schema with 40 columns (cluster on column 35, beyond default 32 limit)
    let fields: Vec<StructField> = (0..40)
        .map(|i| StructField::new(format!("col{}", i), DataType::INTEGER, true))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table on col35 (position > 32)
    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["col35"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column even beyond limit
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols.iter().any(|c| c.to_string() == "col35"),
        "Clustering column col35 should be in stats columns even beyond DEFAULT_NUM_INDEXED_COLS"
    );

    // Verify we have exactly 33 stats columns: first 32 + col35
    // (col35 is added in Pass 2 of collect_columns)
    assert_eq!(
        stats_cols.len(),
        33,
        "Should have 32 indexed cols + 1 clustering col"
    );

    Ok(())
}

#[tokio::test]
async fn test_clustering_column_not_in_schema() -> DeltaResult<()> {
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Try to create clustered table on non-existent column
    let result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["nonexistent"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(
        result,
        "Clustering column 'nonexistent' not found in schema",
    );

    Ok(())
}

// =============================================================================
// Column Mapping Integration Tests
// =============================================================================

#[tokio::test]
async fn test_create_table_with_column_mapping_name_mode() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Create table with column mapping mode = name
    let _result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the log file and verify
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    // Verify protocol has columnMapping feature in BOTH reader and writer features
    let protocol_action = actions
        .iter()
        .find(|a| a.get("protocol").is_some())
        .unwrap();
    let protocol = protocol_action.get("protocol").unwrap();

    let reader_features = protocol["readerFeatures"].as_array().unwrap();
    assert!(
        reader_features.iter().any(|f| f == "columnMapping"),
        "Protocol should have columnMapping in reader features"
    );

    let writer_features = protocol["writerFeatures"].as_array().unwrap();
    assert!(
        writer_features.iter().any(|f| f == "columnMapping"),
        "Protocol should have columnMapping in writer features"
    );

    // Verify metadata has the column mapping properties
    let metadata_action = actions
        .iter()
        .find(|a| a.get("metaData").is_some())
        .unwrap();
    let metadata = metadata_action.get("metaData").unwrap();
    let configuration = metadata["configuration"].as_object().unwrap();

    // Check maxColumnId is set correctly (2 fields)
    assert!(
        configuration.contains_key("delta.columnMapping.maxColumnId"),
        "Configuration should have maxColumnId"
    );
    let max_id: i64 = configuration["delta.columnMapping.maxColumnId"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(max_id, 2, "maxColumnId should equal total field count");

    // Check mode is set
    assert_eq!(
        configuration["delta.columnMapping.mode"].as_str().unwrap(),
        "name"
    );

    // Verify schema has column mapping annotations on ALL fields
    let schema_str = metadata["schemaString"].as_str().unwrap();
    let schema_value: Value = serde_json::from_str(schema_str).unwrap();
    let fields = schema_value["fields"].as_array().unwrap();

    for field in fields {
        let field_name = field["name"].as_str().unwrap();
        let field_metadata = field["metadata"].as_object().unwrap();

        assert!(
            field_metadata.contains_key("delta.columnMapping.id"),
            "Field '{}' should have column mapping id",
            field_name
        );
        assert!(
            field_metadata.contains_key("delta.columnMapping.physicalName"),
            "Field '{}' should have physical name",
            field_name
        );

        // Verify physical name format
        let physical_name = field_metadata["delta.columnMapping.physicalName"]
            .as_str()
            .unwrap();
        assert!(
            physical_name.starts_with("col-"),
            "Physical name '{}' should start with 'col-'",
            physical_name
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_create_table_with_column_mapping_id_mode() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![StructField::new(
        "id",
        DataType::INTEGER,
        false,
    )])?);

    // Create table with column mapping mode = id
    let _result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "id")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Verify table was created with column mapping
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    let protocol_action = actions
        .iter()
        .find(|a| a.get("protocol").is_some())
        .unwrap();
    let protocol = protocol_action.get("protocol").unwrap();

    // Verify columnMapping feature in both reader and writer features
    let reader_features = protocol["readerFeatures"].as_array().unwrap();
    assert!(reader_features.iter().any(|f| f == "columnMapping"));

    let writer_features = protocol["writerFeatures"].as_array().unwrap();
    assert!(writer_features.iter().any(|f| f == "columnMapping"));

    Ok(())
}

#[tokio::test]
async fn test_column_mapping_mode_none_no_annotations() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Create table WITHOUT column mapping
    let _result = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    // Verify protocol does NOT have columnMapping feature
    let protocol_action = actions
        .iter()
        .find(|a| a.get("protocol").is_some())
        .unwrap();
    let protocol = protocol_action.get("protocol").unwrap();

    let reader_features = protocol["readerFeatures"].as_array().unwrap();
    assert!(
        !reader_features.iter().any(|f| f == "columnMapping"),
        "Protocol should NOT have columnMapping feature when mode is not set"
    );

    // Verify configuration does NOT have maxColumnId
    let metadata_action = actions
        .iter()
        .find(|a| a.get("metaData").is_some())
        .unwrap();
    let metadata = metadata_action.get("metaData").unwrap();
    let configuration = metadata["configuration"].as_object().unwrap();

    assert!(
        !configuration.contains_key("delta.columnMapping.maxColumnId"),
        "Configuration should NOT have maxColumnId when mode is not set"
    );

    // Verify schema does NOT have column mapping annotations
    let schema_str = metadata["schemaString"].as_str().unwrap();
    let schema_value: Value = serde_json::from_str(schema_str).unwrap();
    let fields = schema_value["fields"].as_array().unwrap();

    for field in fields {
        let field_metadata = field["metadata"].as_object().unwrap();
        assert!(
            !field_metadata.contains_key("delta.columnMapping.id"),
            "Fields should NOT have column mapping id"
        );
        assert!(
            !field_metadata.contains_key("delta.columnMapping.physicalName"),
            "Fields should NOT have physical name"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_column_mapping_invalid_mode_rejected() {
    let (_temp_dir, table_path, engine) = test_table_setup().unwrap();

    let schema = Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap(),
    );

    // Try to create table with invalid column mapping mode
    let result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "invalid")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid column mapping mode"));
}

#[tokio::test]
async fn test_create_clustered_table_with_column_mapping() -> DeltaResult<()> {
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Create clustered table with column mapping enabled
    let _result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .with_data_layout(DataLayout::clustered(["id"])?)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the log file and verify
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    // Verify protocol has all three features
    let protocol_action = actions
        .iter()
        .find(|a| a.get("protocol").is_some())
        .unwrap();
    let protocol = protocol_action.get("protocol").unwrap();
    let writer_features = protocol["writerFeatures"].as_array().unwrap();
    let reader_features = protocol["readerFeatures"].as_array().unwrap();

    assert!(
        writer_features.iter().any(|f| f == "columnMapping"),
        "Should have columnMapping writer feature"
    );
    assert!(
        reader_features.iter().any(|f| f == "columnMapping"),
        "Should have columnMapping reader feature"
    );
    assert!(
        writer_features.iter().any(|f| f == "clustering"),
        "Should have clustering writer feature"
    );
    assert!(
        writer_features.iter().any(|f| f == "domainMetadata"),
        "Should have domainMetadata writer feature"
    );

    // Verify clustering domain metadata uses PHYSICAL column names (not logical)
    let dm_action = actions.iter().find(|a| {
        a.get("domainMetadata")
            .and_then(|dm| dm.get("domain"))
            .map(|d| d == "delta.clustering")
            .unwrap_or(false)
    });

    assert!(
        dm_action.is_some(),
        "Should have domainMetadata action for clustering"
    );
    let clustering_dm = dm_action.unwrap().get("domainMetadata").unwrap();
    let config: Value =
        serde_json::from_str(clustering_dm["configuration"].as_str().unwrap()).unwrap();
    let cols = config["clusteringColumns"].as_array().unwrap();

    // The clustering column should be the PHYSICAL name (col-*), not "id"
    let clustering_col_name = cols[0][0].as_str().unwrap();
    assert!(
        clustering_col_name.starts_with("col-"),
        "Clustering domain metadata should use physical name '{}' not logical name 'id'",
        clustering_col_name
    );

    // Verify maxColumnId is set correctly in configuration
    let metadata_action = actions
        .iter()
        .find(|a| a.get("metaData").is_some())
        .unwrap();
    let metadata = metadata_action.get("metaData").unwrap();
    let configuration = metadata["configuration"].as_object().unwrap();

    assert!(
        configuration.contains_key("delta.columnMapping.maxColumnId"),
        "Configuration should have maxColumnId"
    );

    Ok(())
}

#[tokio::test]
async fn test_column_mapping_nested_schema() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create nested schema
    let address_type = StructType::try_new(vec![
        StructField::new("street", DataType::STRING, true),
        StructField::new("city", DataType::STRING, true),
    ])?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("address", DataType::Struct(Box::new(address_type)), true),
    ])?);

    // Create table with column mapping
    let _result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read the log file
    let log_file_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    // Verify maxColumnId accounts for all fields (including nested)
    // id=1, address=2, street=3, city=4 => total 4 fields
    let metadata_action = actions
        .iter()
        .find(|a| a.get("metaData").is_some())
        .unwrap();
    let metadata = metadata_action.get("metaData").unwrap();
    let configuration = metadata["configuration"].as_object().unwrap();

    let max_id: i64 = configuration["delta.columnMapping.maxColumnId"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        max_id, 4,
        "maxColumnId should account for nested fields too"
    );

    // Verify nested fields have column mapping annotations
    let schema_str = metadata["schemaString"].as_str().unwrap();
    let schema_value: Value = serde_json::from_str(schema_str).unwrap();
    let fields = schema_value["fields"].as_array().unwrap();

    // Find the address field and check its nested fields
    let address_field = fields.iter().find(|f| f["name"] == "address").unwrap();
    let nested_fields = address_field["type"]["fields"].as_array().unwrap();

    for field in nested_fields {
        let field_name = field["name"].as_str().unwrap();
        let field_metadata = field["metadata"].as_object().unwrap();

        assert!(
            field_metadata.contains_key("delta.columnMapping.id"),
            "Nested field '{}' should have column mapping id",
            field_name
        );
        assert!(
            field_metadata.contains_key("delta.columnMapping.physicalName"),
            "Nested field '{}' should have physical name",
            field_name
        );
    }

    Ok(())
}
