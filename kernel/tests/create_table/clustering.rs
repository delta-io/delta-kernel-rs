//! Clustering integration tests for the CreateTable API.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::table_properties::DEFAULT_NUM_INDEXED_COLS;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use rstest::rstest;
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

#[derive(Debug)]
struct ClusteringStatsCase {
    description: &'static str,
    num_schema_columns: usize,
    clustering_column: &'static str,
    expected_stats_cols_len: usize,
}

#[rstest]
#[case(ClusteringStatsCase {
    description: "clustering column within the default indexed-column range",
    num_schema_columns: 10,
    clustering_column: "col5",
    expected_stats_cols_len: 10,
})]
#[case(ClusteringStatsCase {
    description: "clustering column just beyond default indexed-column range",
    num_schema_columns: 40,
    clustering_column: "col32",
    expected_stats_cols_len: DEFAULT_NUM_INDEXED_COLS as usize + 1,
})]
#[case(ClusteringStatsCase {
    description: "clustering column far beyond default indexed-column range",
    num_schema_columns: 40,
    clustering_column: "col35",
    // Default stats include first 32 leaf columns; clustering column must be appended.
    expected_stats_cols_len: DEFAULT_NUM_INDEXED_COLS as usize + 1,
})]
#[tokio::test]
async fn test_stats_columns_selected_when_adding_clustering_columns(
    #[case] case: ClusteringStatsCase,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let fields: Vec<StructField> = (0..case.num_schema_columns)
        .map(|i| StructField::new(format!("col{}", i), DataType::INTEGER, true))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table.
    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered([case.clustering_column]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column.
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols
            .iter()
            .any(|c| c.to_string() == case.clustering_column),
        "case={}: clustering column {} should be in stats columns",
        case.description,
        case.clustering_column,
    );

    assert_eq!(
        stats_cols.len(),
        case.expected_stats_cols_len,
        "case={}: expected stats column count mismatch",
        case.description,
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
