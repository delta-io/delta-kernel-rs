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
use rstest::rstest;
use test_utils::{assert_result_error_with_message, test_table_setup};

use super::simple_schema;

/// Builds a schema that supports clustering at depths 1, 2, and 5:
///   { id: int, name: string, address: { city: string, zip: string },
///     l1: { l2: { l3: { l4: { value: double } } } } }
fn clustering_test_schema() -> DeltaResult<Arc<StructType>> {
    let address = StructType::try_new(vec![
        StructField::new("city", DataType::STRING, true),
        StructField::new("zip", DataType::STRING, true),
    ])?;
    let l4 = StructType::try_new(vec![StructField::new("value", DataType::DOUBLE, true)])?;
    let l3 = StructType::try_new(vec![StructField::new(
        "l4",
        DataType::Struct(Box::new(l4)),
        true,
    )])?;
    let l2 = StructType::try_new(vec![StructField::new(
        "l3",
        DataType::Struct(Box::new(l3)),
        true,
    )])?;
    let l1 = StructType::try_new(vec![StructField::new(
        "l2",
        DataType::Struct(Box::new(l2)),
        true,
    )])?;
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
        StructField::new("address", DataType::Struct(Box::new(address)), true),
        StructField::new("l1", DataType::Struct(Box::new(l1)), true),
    ])?))
}

#[rstest]
#[case::top_level(vec![vec!["id"]])]
#[case::nested_2(vec![vec!["address", "city"]])]
#[case::mixed(vec![vec!["id"], vec!["name"], vec!["address", "city"], vec!["address", "zip"], vec!["l1", "l2", "l3", "l4", "value"]])]
#[tokio::test]
async fn test_create_clustered_table(#[case] col_paths: Vec<Vec<&str>>) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = clustering_test_schema()?;
    let expected_cols: Vec<ColumnName> = col_paths
        .iter()
        .map(|p| ColumnName::new(p.iter().copied()))
        .collect();

    let txn = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::Clustered {
            columns: expected_cols.clone(),
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let stats_cols = txn.stats_columns();
    for col in &expected_cols {
        assert!(
            stats_cols.contains(col),
            "Clustering column '{col}' should be in stats columns"
        );
    }

    let _ = txn.commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let clustering_columns = snapshot.get_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_columns, Some(expected_cols));

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

#[rstest]
#[case::not_in_schema(vec!["nonexistent"], "not found in schema")]
#[case::nested_not_found(vec!["l1", "l2", "l3", "l4", "missing"], "not found in schema")]
#[case::struct_as_leaf(vec!["address"], "unsupported type")]
#[tokio::test]
async fn test_clustering_column_error(
    #[case] col_path: Vec<&str>,
    #[case] expected_error: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = clustering_test_schema()?;

    let result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::Clustered {
            columns: vec![ColumnName::new(col_path.iter().copied())],
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, expected_error);

    Ok(())
}
