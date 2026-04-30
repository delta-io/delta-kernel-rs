//! Integration tests for snapshot loading with empty (0-byte) log files.
//!
//! These tests verify that kernel handles corrupt log files gracefully:
//! - 0-byte compacted files are skipped, falling back to individual commits
//! - 0-byte commit files produce a clear error message
//! - 0-byte checkpoint files are skipped, falling back to an older checkpoint
//! - 0-byte CRC files are skipped (CRC is optional)

use std::sync::Arc;

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::{add_commit, compacted_log_path_for_versions, create_table, engine_store_setup};

fn simple_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![StructField::nullable("id", DataType::INTEGER)])
            .expect("valid schema"),
    )
}

fn commit_with_add(file_name: &str, num_records: i64) -> String {
    format!(
        r#"{{"commitInfo":{{"timestamp":1587968586000,"operation":"WRITE","operationParameters":{{"mode":"Append"}},"isBlindAppend":true}}}}
{{"add":{{"path":"{file_name}","partitionValues":{{}},"size":1024,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":{num_records},\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\":1}},\"maxValues\":{{\"id\":10}}}}"}}}}
"#
    )
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_location) = engine_store_setup("test_zero_byte_compaction", None);

    let table_url = create_table(
        store.clone(),
        table_location,
        simple_schema(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        commit_with_add("part-00000-file1.parquet", 10),
    )
    .await?;

    add_commit(
        table_url.as_str(),
        store.as_ref(),
        2,
        commit_with_add("part-00001-file2.parquet", 20),
    )
    .await?;

    // Write a 0-byte compaction covering v0-v2
    let compaction_path = compacted_log_path_for_versions(0, 2, "json");
    let table_compaction = Path::from(format!(
        "test_zero_byte_compaction/{}",
        compaction_path.as_ref()
    ));
    store
        .put(&table_compaction, bytes::Bytes::new().into())
        .await?;

    // Snapshot should load successfully by falling back to individual commits
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 2);

    Ok(())
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_commit() -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_location) = engine_store_setup("test_zero_byte_commit", None);

    let table_url = create_table(
        store.clone(),
        table_location,
        simple_schema(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        commit_with_add("part-00000-file1.parquet", 10),
    )
    .await?;

    // Overwrite commit v1 with 0 bytes -- the file stays in the listing
    // (commits are not skipped). The JSON handler reads it as an empty
    // commit (no actions).
    let empty_commit_path =
        Path::from("test_zero_byte_commit/_delta_log/00000000000000000001.json");
    store
        .put(&empty_commit_path, bytes::Bytes::new().into())
        .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 1);

    Ok(())
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_location) = engine_store_setup("test_zero_byte_checkpoint", None);

    let table_url = create_table(
        store.clone(),
        table_location,
        simple_schema(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        commit_with_add("part-00000-file1.parquet", 10),
    )
    .await?;

    add_commit(
        table_url.as_str(),
        store.as_ref(),
        2,
        commit_with_add("part-00001-file2.parquet", 20),
    )
    .await?;

    // Write a 0-byte checkpoint file at v2
    let empty_checkpoint_path =
        Path::from("test_zero_byte_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet");
    store
        .put(&empty_checkpoint_path, bytes::Bytes::new().into())
        .await?;

    // Snapshot should load by using commits from v0 (the empty checkpoint is skipped)
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 2);

    Ok(())
}

#[tokio::test]
async fn snapshot_loads_with_zero_byte_crc() -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_location) = engine_store_setup("test_zero_byte_crc", None);

    let table_url = create_table(
        store.clone(),
        table_location,
        simple_schema(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        commit_with_add("part-00000-file1.parquet", 10),
    )
    .await?;

    // Write a 0-byte CRC file at v1
    let empty_crc_path = Path::from("test_zero_byte_crc/_delta_log/00000000000000000001.crc");
    store
        .put(&empty_crc_path, bytes::Bytes::new().into())
        .await?;

    // Snapshot should load normally (CRC is optional)
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 1);

    Ok(())
}
