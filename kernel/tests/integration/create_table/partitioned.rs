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

use super::{partition_test_schema, simple_schema};

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

/// CREATE TABLE accepts `delta.feature.materializePartitionColumns=supported` regardless of
/// whether the table is partitioned. The feature is harmless on a non-partitioned table
/// (nothing to materialize) and Delta-Spark also does not error in that case, so kernel
/// matches that behavior.
#[rstest]
#[case::partitioned(true)]
#[case::non_partitioned(false)]
fn test_create_table_with_materialize_partition_columns_partitioned_and_not(
    #[case] partitioned: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = if partitioned {
        partition_test_schema()?
    } else {
        simple_schema()?
    };

    let mut builder = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.materializePartitionColumns", "supported")]);
    if partitioned {
        builder = builder.with_data_layout(DataLayout::partitioned(["date"]));
    }
    let _ = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert!(
        snapshot
            .table_configuration()
            .is_feature_enabled(&TableFeature::MaterializePartitionColumns),
        "MaterializePartitionColumns should be enabled (partitioned={partitioned})"
    );

    Ok(())
}
