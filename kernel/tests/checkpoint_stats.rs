//! Integration tests for checkpoint stats and partition values across all
//! writeStatsAsJson / writeStatsAsStruct configuration combinations.
//!
//! These tests write real parquet data through the transaction API, create checkpoints,
//! change stats configuration, create new checkpoints, and read all data back to verify
//! the full write → checkpoint → config change → checkpoint → read pipeline.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::DeltaResult;
use delta_kernel::Snapshot;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::json;
use test_utils::{insert_data, read_scan};
use url::Url;

/// Creates an in-memory store and the table root URL.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (Arc::new(InMemory::new()), Url::parse("memory:///").unwrap())
}

/// Writes a JSON commit file to the store.
async fn write_commit(store: &Arc<InMemory>, content: &str, version: u64) -> DeltaResult<()> {
    let path = Path::from(format!("_delta_log/{version:020}.json", version = version));
    store.put(&path, content.to_string().into()).await?;
    Ok(())
}

/// Builds a JSON commit string with protocol + metadata for a non-partitioned table.
fn build_initial_commit(write_stats_as_json: bool, write_stats_as_struct: bool) -> String {
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": [],
            "writerFeatures": []
        }
    });
    let metadata = json!({
        "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {
                "delta.checkpoint.writeStatsAsJson": write_stats_as_json.to_string(),
                "delta.checkpoint.writeStatsAsStruct": write_stats_as_struct.to_string()
            },
            "createdTime": 1587968585495i64
        }
    });
    format!("{}\n{}", protocol, metadata)
}

/// Builds a JSON commit string with only metadata (for config changes).
fn build_metadata_commit(write_stats_as_json: bool, write_stats_as_struct: bool) -> String {
    let metadata = json!({
        "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {
                "delta.checkpoint.writeStatsAsJson": write_stats_as_json.to_string(),
                "delta.checkpoint.writeStatsAsStruct": write_stats_as_struct.to_string()
            },
            "createdTime": 1587968585495i64
        }
    });
    metadata.to_string()
}

/// Builds a JSON commit string with protocol + metadata for a partitioned table.
/// The table has two partition columns: `created_at` (timestamp) and `tag` (binary),
/// exercising MapToStruct for non-trivial partition value parsing.
fn build_partitioned_initial_commit(
    write_stats_as_json: bool,
    write_stats_as_struct: bool,
) -> String {
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": [],
            "writerFeatures": []
        }
    });
    let metadata = json!({
        "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{}},{"name":"tag","type":"binary","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": ["created_at", "tag"],
            "configuration": {
                "delta.checkpoint.writeStatsAsJson": write_stats_as_json.to_string(),
                "delta.checkpoint.writeStatsAsStruct": write_stats_as_struct.to_string()
            },
            "createdTime": 1587968585495i64
        }
    });
    format!("{}\n{}", protocol, metadata)
}

/// Builds a JSON commit string with only metadata for a partitioned table config change.
fn build_partitioned_metadata_commit(
    write_stats_as_json: bool,
    write_stats_as_struct: bool,
) -> String {
    let metadata = json!({
        "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{}},{"name":"tag","type":"binary","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": ["created_at", "tag"],
            "configuration": {
                "delta.checkpoint.writeStatsAsJson": write_stats_as_json.to_string(),
                "delta.checkpoint.writeStatsAsStruct": write_stats_as_struct.to_string()
            },
            "createdTime": 1587968585495i64
        }
    });
    metadata.to_string()
}

/// Tests all 16 combinations of writeStatsAsJson/writeStatsAsStruct settings with real
/// parquet data written through the transaction API on a non-partitioned table.
///
/// For each combination (json1, struct1, json2, struct2):
/// 1. Creates a table with (json1, struct1) settings
/// 2. Writes real data through transactions (generating actual parquet files with stats)
/// 3. Creates checkpoint 1 with (json1, struct1) settings
/// 4. Writes more data, then changes config to (json2, struct2)
/// 5. Creates checkpoint 2 (reads from checkpoint 1 + new commits, exercises COALESCE)
/// 6. Reads all data back and verifies correctness
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_stats_config_with_real_data(
    #[values(true, false)] json1: bool,
    #[values(true, false)] struct1: bool,
    #[values(true, false)] json2: bool,
    #[values(true, false)] struct2: bool,
) -> DeltaResult<()> {
    let (store, table_root) = new_in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(executor)
            .build(),
    );

    // Version 0: protocol + metadata with initial stats config
    write_commit(&store, &build_initial_commit(json1, struct1), 0).await?;

    // Version 1: write real data (generates actual parquet files with stats)
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let result = insert_data(
        snapshot,
        &engine,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .await?;
    assert!(result.is_committed());

    // Checkpoint 1 with (json1, struct1) settings
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref())?;

    // Version 2: write more data
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let result = insert_data(
        snapshot,
        &engine,
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Diana", "Eve", "Frank"])),
        ],
    )
    .await?;
    assert!(result.is_committed());

    // Version 3: change stats config
    write_commit(&store, &build_metadata_commit(json2, struct2), 3).await?;

    // Checkpoint 2 with (json2, struct2) settings (reads from checkpoint 1 + commits 2-3)
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref())?;

    // Read all data back and verify correctness
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 6,
        "All 6 rows should be readable after checkpoints"
    );

    Ok(())
}

/// Tests all 16 combinations of writeStatsAsJson/writeStatsAsStruct settings with a
/// partitioned table containing real parquet data.
///
/// When writeStatsAsStruct=true, the checkpoint includes `partitionValues_parsed` which
/// is a native typed struct derived from the string-valued `partitionValues` map using
/// `COALESCE(partitionValues_parsed, MAP_TO_STRUCT(partitionValues, partition_schema))`.
///
/// Two partition columns exercise different parsing paths:
///   - `created_at` (timestamp): "2024-01-15 10:30:00" → microseconds-since-epoch
///   - `tag` (binary): "hello" → raw bytes
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_partitioned_with_real_data(
    #[values(true, false)] json1: bool,
    #[values(true, false)] struct1: bool,
    #[values(true, false)] json2: bool,
    #[values(true, false)] struct2: bool,
) -> DeltaResult<()> {
    let (store, table_root) = new_in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(executor)
            .build(),
    );

    // Version 0: protocol + partitioned metadata with initial stats config
    write_commit(&store, &build_partitioned_initial_commit(json1, struct1), 0).await?;

    // Version 1: write data for partition created_at=2024-01-15 10:30:00, tag=hello
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, true),
            Field::new("name", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);
    let write_context = txn.get_write_context();
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(batch),
            &write_context,
            HashMap::from([
                ("created_at".to_string(), "2024-01-15 10:30:00".to_string()),
                ("tag".to_string(), "hello".to_string()),
            ]),
        )
        .await?;
    txn.add_files(add_meta);
    let result = txn.commit(engine.as_ref())?;
    assert!(result.is_committed());

    // Checkpoint 1 with (json1, struct1) settings
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref())?;

    // Version 2: write data for partition created_at=2025-03-01 09:15:30.123456, tag=world
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, true),
            Field::new("name", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Charlie", "Diana"])),
        ],
    )
    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);
    let write_context = txn.get_write_context();
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(batch),
            &write_context,
            HashMap::from([
                (
                    "created_at".to_string(),
                    "2025-03-01 09:15:30.123456".to_string(),
                ),
                ("tag".to_string(), "world".to_string()),
            ]),
        )
        .await?;
    txn.add_files(add_meta);
    let result = txn.commit(engine.as_ref())?;
    assert!(result.is_committed());

    // Version 3: change stats config
    write_commit(
        &store,
        &build_partitioned_metadata_commit(json2, struct2),
        3,
    )
    .await?;

    // Checkpoint 2 with (json2, struct2) settings (reads from checkpoint 1 + commits 2-3)
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref())?;

    // Read all data back and verify correctness
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "All 4 rows should be readable after checkpoints"
    );

    Ok(())
}
