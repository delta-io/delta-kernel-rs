//! Partition integration tests for the CreateTable API.
//!
//! TODO(#2201): Add end-to-end tests for insert + scan + checkpoint on partitioned tables.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::test_table_setup;

use super::partition_test_schema;

#[test]
fn test_create_table_partitioned_basic() -> DeltaResult<()> {
    let schema = partition_test_schema()?;
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::partitioned(["date"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    let partition_cols = snapshot.table_configuration().partition_columns();
    assert_eq!(partition_cols, &["date"]);

    let clustering = snapshot.get_physical_clustering_columns(engine.as_ref())?;
    assert!(
        clustering.is_none(),
        "Partitioned table should not have clustering columns"
    );

    Ok(())
}
