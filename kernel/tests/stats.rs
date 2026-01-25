//! Integration tests for Delta Lake statistics collection.
//!
//! These tests verify that statistics (numRecords, nullCount, minValues, maxValues)
//! are correctly collected and written to the Delta log when writing parquet files.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::Snapshot;

use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::{json, Deserializer};
use url::Url;

use test_utils::{set_json_value, setup_test_tables};

mod common;

const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

/// Extract table name from URL (e.g., "memory:///my_table/" -> "my_table")
fn get_table_name_from_url(url: &Url) -> String {
    url.path()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string()
}

/// Helper to write data and return the commit log entries
async fn write_and_get_commit_log(
    table_url: &Url,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    batches: Vec<RecordBatch>,
    store: Arc<dyn ObjectStore>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot.transaction(committer)?.with_data_change(true);

    let write_context = Arc::new(txn.get_write_context());

    for batch in batches {
        let data = Box::new(ArrowEngineData::new(batch));
        let file_metadata = engine
            .write_parquet(&data, write_context.as_ref(), HashMap::new())
            .await?;
        txn.add_files(file_metadata);
    }

    match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(_) => {}
        _ => panic!("Commit should have succeeded"),
    };

    // Extract actual table name from URL
    let table_name = get_table_name_from_url(table_url);

    // Read the commit log
    let commit = store
        .get(&Path::from(format!(
            "/{table_name}/_delta_log/00000000000000000001.json"
        )))
        .await?;

    let parsed_commits: Vec<serde_json::Value> = Deserializer::from_slice(&commit.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    Ok(parsed_commits)
}

/// Test comprehensive statistics collection with multiple data types.
///
/// This test verifies stats for:
/// - Integer types: byte (int8), short (int16), int (int32), long (int64)
/// - Floating point: float (float32), double (float64)
/// - String and boolean
/// - Date
/// - Null value handling
#[tokio::test]
async fn test_stats_multiple_data_types() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Schema with multiple types (excluding timestamp to avoid feature requirements)
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("byte_val", DataType::BYTE, true),
        StructField::new("short_val", DataType::SHORT, true),
        StructField::new("int_val", DataType::INTEGER, true),
        StructField::new("float_val", DataType::FLOAT, true),
        StructField::new("double_val", DataType::DOUBLE, true),
        StructField::new("string_val", DataType::STRING, true),
        StructField::new("bool_val", DataType::BOOLEAN, true),
        StructField::new("date_val", DataType::DATE, true),
    ])?);

    for (table_url, engine, store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "stats_multi_types").await?
    {
        let engine = Arc::new(engine);

        // Create test data with nulls in various columns
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(Int8Array::from(vec![
                    Some(-10),
                    Some(0),
                    None,
                    Some(50),
                    Some(127),
                ])),
                Arc::new(Int16Array::from(vec![
                    Some(-1000),
                    Some(0),
                    Some(1000),
                    None,
                    Some(32000),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(100),
                    Some(200),
                    Some(300),
                    Some(400),
                    None,
                ])),
                Arc::new(Float32Array::from(vec![
                    Some(1.5f32),
                    None,
                    Some(3.5f32),
                    Some(4.5f32),
                    Some(5.5f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.1),
                    Some(20.2),
                    Some(30.3),
                    Some(40.4),
                    Some(50.5),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("apple"),
                    Some("banana"),
                    None,
                    Some("date"),
                    Some("elderberry"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                    Some(false),
                ])),
                Arc::new(Date32Array::from(vec![
                    Some(19000),
                    Some(19001),
                    Some(19002),
                    None,
                    Some(19004),
                ])),
            ],
        )?;

        let mut commits =
            write_and_get_commit_log(&table_url, engine, vec![batch], store.clone()).await?;

        // Normalize non-deterministic values
        set_json_value(&mut commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        set_json_value(&mut commits[1], "add.modificationTime", json!(0))?;
        set_json_value(&mut commits[1], "add.path", json!("data.parquet"))?;
        set_json_value(&mut commits[1], "add.size", json!(0))?;

        // Parse the stats JSON from the add action
        let stats_str = commits[1]["add"]["stats"].as_str().unwrap();
        let stats: serde_json::Value = serde_json::from_str(stats_str)?;

        // Verify numRecords
        assert_eq!(stats["numRecords"], 5, "Should have 5 records");

        // Verify nullCount for each column
        assert_eq!(stats["nullCount"]["id"], 0, "id has no nulls");
        assert_eq!(stats["nullCount"]["byte_val"], 1, "byte_val has 1 null");
        assert_eq!(stats["nullCount"]["short_val"], 1, "short_val has 1 null");
        assert_eq!(stats["nullCount"]["int_val"], 1, "int_val has 1 null");
        assert_eq!(stats["nullCount"]["float_val"], 1, "float_val has 1 null");
        assert_eq!(
            stats["nullCount"]["double_val"], 0,
            "double_val has no nulls"
        );
        assert_eq!(stats["nullCount"]["string_val"], 1, "string_val has 1 null");
        assert_eq!(stats["nullCount"]["bool_val"], 1, "bool_val has 1 null");
        assert_eq!(stats["nullCount"]["date_val"], 1, "date_val has 1 null");

        // Verify minValues
        assert_eq!(stats["minValues"]["id"], 1);
        assert_eq!(stats["minValues"]["byte_val"], -10);
        assert_eq!(stats["minValues"]["short_val"], -1000);
        assert_eq!(stats["minValues"]["int_val"], 100);
        assert_eq!(stats["minValues"]["string_val"], "apple");

        // Verify maxValues
        assert_eq!(stats["maxValues"]["id"], 5);
        assert_eq!(stats["maxValues"]["byte_val"], 127);
        assert_eq!(stats["maxValues"]["short_val"], 32000);
        assert_eq!(stats["maxValues"]["int_val"], 400);
        assert_eq!(stats["maxValues"]["string_val"], "elderberry");

        // Verify tightBounds
        assert_eq!(stats["tightBounds"], true);

        println!("✓ Multi-type stats test passed for {}", table_url);
    }

    Ok(())
}

/// Test statistics collection with nested structs.
///
/// Schema: { id: long, address: { city: string, location: { lat: double, lon: double } } }
#[tokio::test]
async fn test_stats_nested_structs() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Nested schema: address.location.{lat, lon}
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new(
            "address",
            DataType::try_struct_type(vec![
                StructField::new("city", DataType::STRING, true),
                StructField::new(
                    "location",
                    DataType::try_struct_type(vec![
                        StructField::new("lat", DataType::DOUBLE, true),
                        StructField::new("lon", DataType::DOUBLE, true),
                    ])?,
                    true,
                ),
            ])?,
            true,
        ),
    ])?);

    for (table_url, engine, store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "stats_nested").await?
    {
        let engine = Arc::new(engine);

        // Build nested Arrow arrays
        let location_fields = vec![
            Field::new("lat", ArrowDataType::Float64, true),
            Field::new("lon", ArrowDataType::Float64, true),
        ];
        let address_fields = vec![
            Field::new("city", ArrowDataType::Utf8, true),
            Field::new(
                "location",
                ArrowDataType::Struct(location_fields.clone().into()),
                true,
            ),
        ];

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new(
                "address",
                ArrowDataType::Struct(address_fields.clone().into()),
                true,
            ),
        ]));

        // Create location struct
        let lat_array = Arc::new(Float64Array::from(vec![
            Some(40.7128),
            Some(34.0522),
            Some(41.8781),
            None,
        ]));
        let lon_array = Arc::new(Float64Array::from(vec![
            Some(-74.0060),
            Some(-118.2437),
            None,
            Some(-95.3698),
        ]));
        let location_array = Arc::new(
            StructArray::try_new(
                location_fields.into(),
                vec![lat_array as ArrayRef, lon_array as ArrayRef],
                None,
            )
            .unwrap(),
        );

        // Create address struct
        let city_array = Arc::new(StringArray::from(vec![
            Some("New York"),
            Some("Los Angeles"),
            None,
            Some("Houston"),
        ]));
        let address_array = Arc::new(
            StructArray::try_new(
                address_fields.into(),
                vec![city_array as ArrayRef, location_array as ArrayRef],
                None,
            )
            .unwrap(),
        );

        let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![id_array as ArrayRef, address_array as ArrayRef],
        )?;

        let mut commits =
            write_and_get_commit_log(&table_url, engine, vec![batch], store.clone()).await?;

        // Normalize non-deterministic values
        set_json_value(&mut commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        set_json_value(&mut commits[1], "add.modificationTime", json!(0))?;
        set_json_value(&mut commits[1], "add.path", json!("data.parquet"))?;
        set_json_value(&mut commits[1], "add.size", json!(0))?;

        // Parse the stats JSON
        let stats_str = commits[1]["add"]["stats"].as_str().unwrap();
        let stats: serde_json::Value = serde_json::from_str(stats_str)?;

        // Verify numRecords
        assert_eq!(stats["numRecords"], 4);

        // Verify nested nullCount
        assert_eq!(stats["nullCount"]["id"], 0);
        assert_eq!(stats["nullCount"]["address"]["city"], 1);
        assert_eq!(stats["nullCount"]["address"]["location"]["lat"], 1);
        assert_eq!(stats["nullCount"]["address"]["location"]["lon"], 1);

        // Verify nested minValues - lat min should be ~34.0522 (Los Angeles)
        let lat_min = stats["minValues"]["address"]["location"]["lat"]
            .as_f64()
            .unwrap();
        assert!(
            (lat_min - 34.0522).abs() < 0.0001,
            "lat min should be ~34.0522"
        );

        // city min should be "Houston" (alphabetically first)
        assert_eq!(stats["minValues"]["address"]["city"], "Houston");

        // Verify nested maxValues - lat max should be ~41.8781 (Chicago)
        let lat_max = stats["maxValues"]["address"]["location"]["lat"]
            .as_f64()
            .unwrap();
        assert!(
            (lat_max - 41.8781).abs() < 0.0001,
            "lat max should be ~41.8781"
        );

        // city max should be "New York"
        assert_eq!(stats["maxValues"]["address"]["city"], "New York");

        println!("✓ Nested struct stats test passed for {}", table_url);
    }

    Ok(())
}

/// Test that all-null columns don't produce min/max stats (only nullCount).
#[tokio::test]
async fn test_stats_all_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("all_null_string", DataType::STRING, true),
        StructField::new("all_null_int", DataType::INTEGER, true),
    ])?);

    for (table_url, engine, store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "stats_all_nulls").await?
    {
        let engine = Arc::new(engine);
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>, None, None])),
                Arc::new(Int32Array::from(vec![None, None, None])),
            ],
        )?;

        let mut commits =
            write_and_get_commit_log(&table_url, engine, vec![batch], store.clone()).await?;

        set_json_value(&mut commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        set_json_value(&mut commits[1], "add.modificationTime", json!(0))?;
        set_json_value(&mut commits[1], "add.path", json!("data.parquet"))?;
        set_json_value(&mut commits[1], "add.size", json!(0))?;

        let stats_str = commits[1]["add"]["stats"].as_str().unwrap();
        let stats: serde_json::Value = serde_json::from_str(stats_str)?;

        // All-null columns should have nullCount = 3
        assert_eq!(stats["nullCount"]["all_null_string"], 3);
        assert_eq!(stats["nullCount"]["all_null_int"], 3);

        // All-null columns should not have min/max values
        // (they may be absent or null in the stats JSON)
        let has_string_min = stats["minValues"]
            .get("all_null_string")
            .map_or(true, |v| v.is_null());
        let has_int_min = stats["minValues"]
            .get("all_null_int")
            .map_or(true, |v| v.is_null());

        assert!(
            has_string_min || stats["minValues"].get("all_null_string").is_none(),
            "all_null_string should not have minValue"
        );
        assert!(
            has_int_min || stats["minValues"].get("all_null_int").is_none(),
            "all_null_int should not have minValue"
        );

        println!("✓ All-nulls stats test passed for {}", table_url);
    }

    Ok(())
}

/// Test string truncation for long strings (Delta protocol requires 32 char max).
#[tokio::test]
async fn test_stats_string_truncation() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("long_string", DataType::STRING, true),
    ])?);

    for (table_url, engine, store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "stats_string_truncation").await?
    {
        let engine = Arc::new(engine);
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);

        // Create strings longer than 32 characters
        let long_min = "a".repeat(50); // Should truncate to 32 'a's
        let long_max = "z".repeat(50); // Should truncate to 32 chars with increment

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    long_min.as_str(),
                    long_max.as_str(),
                ])),
            ],
        )?;

        let mut commits =
            write_and_get_commit_log(&table_url, engine, vec![batch], store.clone()).await?;

        set_json_value(&mut commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        set_json_value(&mut commits[1], "add.modificationTime", json!(0))?;
        set_json_value(&mut commits[1], "add.path", json!("data.parquet"))?;
        set_json_value(&mut commits[1], "add.size", json!(0))?;

        let stats_str = commits[1]["add"]["stats"].as_str().unwrap();
        let stats: serde_json::Value = serde_json::from_str(stats_str)?;

        // Verify min is truncated to 32 characters
        let min_val = stats["minValues"]["long_string"].as_str().unwrap();
        assert!(
            min_val.len() <= 32,
            "min string should be truncated to 32 chars, got {}",
            min_val.len()
        );
        assert_eq!(min_val, "a".repeat(32));

        // Verify max is truncated (may have increment applied)
        let max_val = stats["maxValues"]["long_string"].as_str().unwrap();
        assert!(
            max_val.len() <= 32,
            "max string should be truncated to 32 chars, got {}",
            max_val.len()
        );

        println!("✓ String truncation stats test passed for {}", table_url);
    }

    Ok(())
}
