//! Clustering integration tests for the CreateTable API.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::{assert_result_error_with_message, test_table_setup};

use super::simple_schema;

#[tokio::test]
async fn test_create_clustered_table() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create schema for a clustered table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
        StructField::new("timestamp", DataType::TIMESTAMP, false),
    ])?);

    // Create clustered table on "id" column
    let txn = create_table(&table_path, schema.clone(), "DeltaKernel-RS/Test")
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols.iter().any(|c| c.to_string() == "id"),
        "Clustering column 'id' should be in stats columns"
    );

    // Commit the table
    let _ = txn.commit(engine.as_ref())?;

    // Verify clustering columns via snapshot read path
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let clustering_columns = snapshot.get_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_columns, Some(vec![ColumnName::new(["id"])]));

    // Verify protocol has required features
    let table_configuration = snapshot.table_configuration();
    assert!(
        table_configuration.is_feature_supported(&TableFeature::DomainMetadata),
        "Protocol should support domainMetadata feature"
    );
    assert!(
        table_configuration.is_feature_supported(&TableFeature::ClusteredTable),
        "Protocol should support clustering feature"
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
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Combine BOTH: explicit feature signal AND clustering (which auto-adds domainMetadata)
    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .with_data_layout(DataLayout::clustered(["id"]))
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
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Build schema with 10 columns (cluster on column 5, within default 32 limit)
    let fields: Vec<StructField> = (0..10)
        .map(|i| StructField::new(format!("col{}", i), DataType::INTEGER, true))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table on col5
    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["col5"]))
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
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Build schema with 40 columns (cluster on column 35, beyond default 32 limit)
    let fields: Vec<StructField> = (0..40)
        .map(|i| StructField::new(format!("col{}", i), DataType::INTEGER, true))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table on col35 (position > 32)
    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["col35"]))
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
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Try to create clustered table on non-existent column
    let result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["nonexistent"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(
        result,
        "Clustering column 'nonexistent' not found in schema",
    );

    Ok(())
}
