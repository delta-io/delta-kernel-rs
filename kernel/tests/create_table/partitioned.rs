//! Partition integration tests for the CreateTable API.
//!
//! TODO(#2201): Add end-to-end tests for insert + scan + checkpoint on partitioned tables.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::test_table_setup;

use super::partition_test_schema;

#[rstest]
#[case::exact_casing("date")]
#[case::mismatched_casing("DATE")]
fn test_create_table_partitioned_basic(#[case] partition_col: &str) -> DeltaResult<()> {
    let schema = partition_test_schema()?;
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::partitioned([partition_col]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    // Partition column should be stored matching the casing in the schema
    let partition_cols = snapshot.table_configuration().partition_columns();
    assert_eq!(partition_cols, &["date"]);

    let clustering = snapshot.get_physical_clustering_columns(engine.as_ref())?;
    assert!(
        clustering.is_none(),
        "Partitioned table should not have clustering columns"
    );

    Ok(())
}

/// CREATE TABLE with `delta.feature.materializePartitionColumns=supported` lands the feature
/// in `writerFeatures` exactly once and not in `readerFeatures` (writer-only feature). The
/// feature has no `delta.*` enablement property and is not auto-enabled by any schema or
/// metadata, so the explicit feature signal is the only opt-in path at create time.
#[test]
fn test_create_table_with_materialize_partition_columns_feature_signal_allowed() -> DeltaResult<()>
{
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = partition_test_schema()?;

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::partitioned(["date"]))
        .with_table_properties([("delta.feature.materializePartitionColumns", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("writer features should be present");
    let count = writer_features
        .iter()
        .filter(|f| **f == TableFeature::MaterializePartitionColumns)
        .count();
    assert_eq!(
        count, 1,
        "MaterializePartitionColumns should appear exactly once in writerFeatures; got {count}"
    );
    assert!(
        !protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::MaterializePartitionColumns)),
        "MaterializePartitionColumns is writer-only and must not appear in readerFeatures"
    );
    assert!(
        snapshot
            .table_configuration()
            .is_feature_enabled(&TableFeature::MaterializePartitionColumns),
        "MaterializePartitionColumns should be enabled when listed in writerFeatures"
    );

    Ok(())
}
