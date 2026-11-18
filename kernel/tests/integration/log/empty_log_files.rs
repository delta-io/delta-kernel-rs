//! Integration tests for snapshot loading with empty (0-byte) log files.
//!
//! Empty compactions and checkpoints fall back to individual commits. Empty commits contribute no
//! actions, and empty CRC files do not prevent snapshot loading.

use delta_kernel::Snapshot;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{SchemaRef, schema_ref};
use test_utils::table_builder::{LogState, TestTableBuilder};
use test_utils::{compacted_log_path_for_versions, delta_path_for_version};

fn simple_schema() -> SchemaRef {
    schema_ref! { nullable "id": INTEGER }
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2))
        .with_schema(simple_schema())
        .with_data(1, 1)
        .build()?;

    table
        .store()
        .put(
            &compacted_log_path_for_versions(0, 2, "json"),
            bytes::Bytes::new().into(),
        )
        .await?;

    let engine = table.engine();
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    assert_eq!(snapshot.version(), 2);

    Ok(())
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_commit() -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_schema(simple_schema())
        .with_data(1, 1)
        .build()?;

    table
        .store()
        .put(
            &delta_path_for_version(1, "json"),
            bytes::Bytes::new().into(),
        )
        .await?;

    let engine = table.engine();
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    assert_eq!(snapshot.version(), 1);

    Ok(())
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2))
        .with_schema(simple_schema())
        .with_data(1, 1)
        .build()?;

    table
        .store()
        .put(
            &delta_path_for_version(2, "checkpoint.parquet"),
            bytes::Bytes::new().into(),
        )
        .await?;

    let engine = table.engine();
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    assert_eq!(snapshot.version(), 2);

    Ok(())
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_crc() -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_schema(simple_schema())
        .with_data(1, 1)
        .build()?;

    table
        .store()
        .put(
            &delta_path_for_version(1, "crc"),
            bytes::Bytes::new().into(),
        )
        .await?;

    let engine = table.engine();
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    assert_eq!(snapshot.version(), 1);

    Ok(())
}
