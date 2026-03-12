//! Integration tests for checkpoint stats and partition values across all
//! writeStatsAsJson / writeStatsAsStruct configuration combinations.
//!
//! These tests write real parquet data through the transaction API, create checkpoints,
//! change stats configuration, create new checkpoints, and read all data back to verify
//! the full write → checkpoint → config change → checkpoint → read pipeline.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::compute::{concat_batches, sort_to_indices, take};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field, Int64Type, Schema as ArrowSchema, TimestampMicrosecondType,
};
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::DeltaResult;
use delta_kernel::Snapshot;

use serde_json::json;
use test_utils::{insert_data, read_scan, write_batch_to_table};
use url::Url;

/// Creates an in-memory store and the table root URL.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (Arc::new(InMemory::new()), Url::parse("memory:///").unwrap())
}

/// Writes a JSON commit file to the store.
async fn write_commit(store: &Arc<InMemory>, content: &str, version: u64) -> DeltaResult<()> {
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    store.put(&path, content.to_string().into()).await?;
    Ok(())
}

const NON_PARTITIONED_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#;

const PARTITIONED_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{}},{"name":"tag","type":"binary","nullable":true,"metadata":{}}]}"#;

/// Builds a JSON commit string with optional protocol, metadata, and stats config.
/// When `include_protocol` is true, includes the protocol action (for version 0 commits).
fn build_commit(
    schema_string: &str,
    partition_columns: &[&str],
    write_stats_as_json: bool,
    write_stats_as_struct: bool,
    include_protocol: bool,
) -> String {
    let metadata = json!({
        "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": partition_columns,
            "configuration": {
                "delta.checkpoint.writeStatsAsJson": write_stats_as_json.to_string(),
                "delta.checkpoint.writeStatsAsStruct": write_stats_as_struct.to_string()
            },
            "createdTime": 1587968585495i64
        }
    });
    if include_protocol {
        let protocol = json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": []
            }
        });
        format!("{protocol}\n{metadata}")
    } else {
        metadata.to_string()
    }
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
) -> Result<(), Box<dyn std::error::Error>> {
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
    write_commit(
        &store,
        &build_commit(NON_PARTITIONED_SCHEMA, &[], json1, struct1, true),
        0,
    )
    .await?;

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
    write_commit(
        &store,
        &build_commit(NON_PARTITIONED_SCHEMA, &[], json2, struct2, false),
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

    let schema = batches[0].schema();
    let merged = concat_batches(&schema, &batches)?;
    let id_col = merged.column_by_name("id").unwrap();
    let sort_indices = sort_to_indices(id_col, None, None)?;
    let sorted_columns: Vec<ArrayRef> = merged
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &sort_indices, None).unwrap())
        .collect();
    let sorted = RecordBatch::try_new(schema, sorted_columns)?;

    assert_eq!(sorted.num_rows(), 6, "All 6 rows should be readable");

    let ids: Vec<i64> = sorted
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);

    let names: Vec<&str> = (0..6)
        .map(|i| {
            sorted
                .column_by_name("name")
                .unwrap()
                .as_string::<i32>()
                .value(i)
        })
        .collect();
    assert_eq!(
        names,
        vec!["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]
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
) -> Result<(), Box<dyn std::error::Error>> {
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
    write_commit(
        &store,
        &build_commit(
            PARTITIONED_SCHEMA,
            &["created_at", "tag"],
            json1,
            struct1,
            true,
        ),
        0,
    )
    .await?;

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
    )?;
    write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        batch,
        HashMap::from([
            ("created_at".to_string(), "2024-01-15 10:30:00".to_string()),
            ("tag".to_string(), "hello".to_string()),
        ]),
    )
    .await?;

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
    )?;
    write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        batch,
        HashMap::from([
            (
                "created_at".to_string(),
                "2025-03-01 09:15:30.123456".to_string(),
            ),
            ("tag".to_string(), "world".to_string()),
        ]),
    )
    .await?;

    // Version 3: change stats config
    write_commit(
        &store,
        &build_commit(
            PARTITIONED_SCHEMA,
            &["created_at", "tag"],
            json2,
            struct2,
            false,
        ),
        3,
    )
    .await?;

    // Checkpoint 2 with (json2, struct2) settings (reads from checkpoint 1 + commits 2-3)
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref())?;

    // Verify partitionValues_parsed content directly in the checkpoint
    if struct2 {
        let checkpoint_path = Path::from("_delta_log/00000000000000000003.checkpoint.parquet");
        let checkpoint_bytes = store.get(&checkpoint_path).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(checkpoint_bytes)?.build()?;
        let ckpt_batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let ckpt_schema = ckpt_batches[0].schema();
        let ckpt_merged = concat_batches(&ckpt_schema, &ckpt_batches)?;

        let add_col = ckpt_merged
            .column_by_name("add")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Verify partitionValues_parsed has correctly typed partition values
        let pv_parsed = add_col
            .column_by_name("partitionValues_parsed")
            .expect("checkpoint should have partitionValues_parsed when writeStatsAsStruct=true")
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let add_rows: Vec<usize> = (0..ckpt_merged.num_rows())
            .filter(|&i| add_col.is_valid(i))
            .collect();
        assert_eq!(add_rows.len(), 2, "should have 2 add actions");

        // Verify tag (binary) partition values
        let tag_col = pv_parsed
            .column_by_name("tag")
            .expect("partitionValues_parsed should have tag");
        let mut tag_values: Vec<&[u8]> = add_rows
            .iter()
            .map(|&i| tag_col.as_binary::<i32>().value(i))
            .collect();
        tag_values.sort();
        assert_eq!(tag_values, vec![b"hello", b"world"]);

        // Verify created_at (timestamp) partition values
        let created_at_col = pv_parsed
            .column_by_name("created_at")
            .expect("partitionValues_parsed should have created_at");
        let mut ts_values: Vec<i64> = add_rows
            .iter()
            .map(|&i| {
                created_at_col
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(i)
            })
            .collect();
        ts_values.sort();
        // 2024-01-15 10:30:00 UTC and 2025-03-01 09:15:30.123456 UTC in microseconds
        assert_eq!(ts_values, vec![1705314600000000, 1740820530123456]);
    }

    // Read all data back and verify correctness
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;

    // Merge all batches and sort by id to get deterministic ordering
    let schema = batches[0].schema();
    let merged = concat_batches(&schema, &batches)?;
    let id_col = merged.column_by_name("id").unwrap();
    let sort_indices = sort_to_indices(id_col, None, None)?;
    let sorted_columns: Vec<ArrayRef> = merged
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &sort_indices, None).unwrap())
        .collect();
    let sorted = RecordBatch::try_new(schema, sorted_columns)?;

    assert_eq!(sorted.num_rows(), 4, "All 4 rows should be readable");

    // Verify partition values are correctly round-tripped
    let ids: Vec<i64> = sorted
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    let tags = sorted.column_by_name("tag").unwrap();
    let tags: Vec<&[u8]> = (0..4).map(|i| tags.as_binary::<i32>().value(i)).collect();
    // Rows 1,2 have tag=hello; rows 3,4 have tag=world
    assert_eq!(tags, vec![b"hello", b"hello", b"world", b"world"]);

    Ok(())
}

/// Schema with column mapping metadata: logical names differ from physical names.
const COLUMN_MAPPING_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-id-phys"}},{"name":"name","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-name-phys"}},{"name":"category","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-category-phys"}}]}"#;

/// Verifies that `partitionValues_parsed` uses physical column names when column mapping
/// is enabled. The checkpoint should contain `col-category-phys` (physical) not `category`
/// (logical) as the field name inside `partitionValues_parsed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_partition_values_parsed_with_column_mapping(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, table_root) = new_in_memory_store();
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(executor)
            .build(),
    );

    // Version 0: protocol + metadata with column mapping and writeStatsAsStruct=true
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    });
    let metadata = json!({
        "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": COLUMN_MAPPING_SCHEMA,
            "partitionColumns": ["category"],
            "configuration": {
                "delta.checkpoint.writeStatsAsJson": "true",
                "delta.checkpoint.writeStatsAsStruct": "true",
                "delta.columnMapping.mode": "name",
                "delta.columnMapping.maxColumnId": "3"
            },
            "createdTime": 1587968585495i64
        }
    });
    write_commit(&store, &format!("{protocol}\n{metadata}"), 0).await?;

    // Version 1: write data for partition category=books
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("col-id-phys", ArrowDataType::Int64, true),
            Field::new("col-name-phys", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;
    write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        batch,
        HashMap::from([("category".to_string(), "books".to_string())]),
    )
    .await?;

    // Create checkpoint with writeStatsAsStruct=true
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref())?;

    // Read checkpoint and verify partitionValues_parsed uses physical name
    let checkpoint_path = Path::from("_delta_log/00000000000000000001.checkpoint.parquet");
    let checkpoint_bytes = store.get(&checkpoint_path).await?.bytes().await?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(checkpoint_bytes)?.build()?;
    let ckpt_batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let ckpt_schema = ckpt_batches[0].schema();
    let ckpt_merged = concat_batches(&ckpt_schema, &ckpt_batches)?;

    let add_col = ckpt_merged
        .column_by_name("add")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let pv_parsed = add_col
        .column_by_name("partitionValues_parsed")
        .expect("checkpoint should have partitionValues_parsed")
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    // Should use physical name "col-category-phys", NOT logical name "category"
    assert!(
        pv_parsed.column_by_name("col-category-phys").is_some(),
        "partitionValues_parsed should use physical column name"
    );
    assert!(
        pv_parsed.column_by_name("category").is_none(),
        "partitionValues_parsed should not use logical column name"
    );

    // Verify the value is correct
    let add_row = (0..ckpt_merged.num_rows())
        .find(|&i| add_col.is_valid(i))
        .expect("should have an add action");
    let category_col = pv_parsed.column_by_name("col-category-phys").unwrap();
    let category_value = category_col.as_string::<i32>().value(add_row);
    assert_eq!(category_value, "books");

    // Also verify data round-trips correctly through scan
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    Ok(())
}
