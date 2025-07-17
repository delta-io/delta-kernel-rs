use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Int32Array, MapBuilder, MapFieldNames, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::record_batch::RecordBatch;

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStore;
use itertools::Itertools;
use serde_json::json;
use serde_json::Deserializer;

use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::DeltaResult;
use delta_kernel::Error as KernelError;
use delta_kernel::Snapshot;

use test_utils::{create_table, engine_store_setup, setup_test_tables};

mod common;
use test_utils::test_read;

// create commit info in arrow of the form {engineInfo: "default engine"}
fn new_commit_info() -> DeltaResult<Box<ArrowEngineData>> {
    // create commit info of the form {engineCommitInfo: Map { "engineInfo": "default engine" } }
    let commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "engineCommitInfo",
        ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", ArrowDataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        false,
    )]));

    let key_builder = StringBuilder::new();
    let val_builder = StringBuilder::new();
    let names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut builder = MapBuilder::new(Some(names), key_builder, val_builder);
    builder.keys().append_value("engineInfo");
    builder.values().append_value("default engine");
    builder.append(true).unwrap();
    let array = builder.finish();

    let commit_info_batch =
        RecordBatch::try_new(commit_info_schema.clone(), vec![Arc::new(array)])?;
    Ok(Box::new(ArrowEngineData::new(commit_info_batch)))
}

#[tokio::test]
async fn test_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    for (table_url, engine, store, table_name) in setup_test_tables(schema, &[]).await? {
        let commit_info = new_commit_info()?;

        // create a transaction
        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let txn = snapshot.transaction()?.with_commit_info(commit_info);

        // commit!
        txn.commit(&engine)?;

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commit: serde_json::Value = serde_json::from_slice(&commit1.bytes().await?)?;
        *parsed_commit
            .get_mut("commitInfo")
            .unwrap()
            .get_mut("timestamp")
            .unwrap() = serde_json::Value::Number(0.into());

        let expected_commit = json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineCommitInfo": {
                    "engineInfo": "default engine"
                }
            }
        });

        assert_eq!(parsed_commit, expected_commit);
    }
    Ok(())
}

#[tokio::test]
async fn test_empty_commit() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    for (table_url, engine, _store, _table_name) in setup_test_tables(schema, &[]).await? {
        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        assert!(matches!(
            snapshot.transaction()?.commit(&engine).unwrap_err(),
            KernelError::MissingCommitInfo
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_invalid_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));
    for (table_url, engine, _store, _table_name) in setup_test_tables(schema, &[]).await? {
        // empty commit info test
        let commit_info_schema = Arc::new(ArrowSchema::empty());
        let commit_info_batch = RecordBatch::new_empty(commit_info_schema.clone());
        assert!(commit_info_batch.num_rows() == 0);
        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let txn = snapshot
            .transaction()?
            .with_commit_info(Box::new(ArrowEngineData::new(commit_info_batch)));

        // commit!
        assert!(matches!(
            txn.commit(&engine),
            Err(KernelError::InvalidCommitInfo(_))
        ));

        // two-row commit info test
        let commit_info_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "engineInfo",
            ArrowDataType::Utf8,
            true,
        )]));
        let commit_info_batch = RecordBatch::try_new(
            commit_info_schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "row1: default engine",
                "row2: default engine",
            ]))],
        )?;

        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let txn = snapshot
            .transaction()?
            .with_commit_info(Box::new(ArrowEngineData::new(commit_info_batch)));

        // commit!
        assert!(matches!(
            txn.commit(&engine),
            Err(KernelError::InvalidCommitInfo(_))
        ));
    }
    Ok(())
}

// check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
fn check_action_timestamps<'a>(
    parsed_commits: impl Iterator<Item = &'a serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()
        .unwrap();

    parsed_commits.for_each(|commit| {
        if let Some(commit_info_ts) = &commit.pointer("/commitInfo/timestamp") {
            assert!((now - commit_info_ts.as_i64().unwrap()).abs() < 10_000);
        }
        if let Some(add_ts) = &commit.pointer("/add/modificationTime") {
            assert!((now - add_ts.as_i64().unwrap()).abs() < 10_000);
        }
    });

    Ok(())
}

// update `value` at (.-separated) `path` to `new_value`
fn set_value(
    value: &mut serde_json::Value,
    path: &str,
    new_value: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut path_string = path.replace(".", "/");
    path_string.insert(0, '/');
    let v = value
        .pointer_mut(&path_string)
        .ok_or_else(|| format!("key '{path}' not found"))?;
    *v = new_value;
    Ok(())
}

// list all the files at `path` and check that all parquet files have the same size, and return
// that size
async fn get_and_check_all_parquet_sizes(store: Arc<dyn ObjectStore>, path: &str) -> u64 {
    use futures::stream::StreamExt;
    let files: Vec<_> = store.list(Some(&Path::from(path))).collect().await;
    let parquet_files = files
        .into_iter()
        .filter(|f| match f {
            Ok(f) => f.location.extension() == Some("parquet"),
            Err(_) => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(parquet_files.len(), 2);
    let size = parquet_files.first().unwrap().as_ref().unwrap().size;
    assert!(parquet_files
        .iter()
        .all(|f| f.as_ref().unwrap().size == size));
    size
}

#[tokio::test]
async fn test_append() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    for (table_url, engine, store, table_name) in setup_test_tables(schema.clone(), &[]).await? {
        let commit_info = new_commit_info()?;

        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let mut txn = snapshot.transaction()?.with_commit_info(commit_info);

        // create two new arrow record batches to append
        let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let write_context = Arc::new(txn.get_write_context());
        let tasks = append_data.into_iter().map(|data| {
            // arc clones
            let engine = engine.clone();
            let write_context = write_context.clone();
            tokio::task::spawn(async move {
                engine
                    .write_parquet(
                        data.as_ref().unwrap(),
                        write_context.as_ref(),
                        HashMap::new(),
                        true,
                    )
                    .await
            })
        });

        let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
        for meta in add_files_metadata {
            txn.add_files(meta?);
        }

        // commit!
        txn.commit(engine.as_ref())?;

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        let size =
            get_and_check_all_parquet_sizes(store.clone(), format!("/{table_name}/").as_str())
                .await;
        // check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
        // before we clear them for comparison
        check_action_timestamps(parsed_commits.iter())?;

        // set timestamps to 0 and paths to known string values for comparison
        // (otherwise timestamps are non-deterministic and paths are random UUIDs)
        set_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
        set_value(&mut parsed_commits[1], "add.path", json!("first.parquet"))?;
        set_value(&mut parsed_commits[2], "add.modificationTime", json!(0))?;
        set_value(&mut parsed_commits[2], "add.path", json!("second.parquet"))?;

        let expected_commit = vec![
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "UNKNOWN",
                    "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                    "operationParameters": {},
                    "engineCommitInfo": {
                        "engineInfo": "default engine"
                    }
                }
            }),
            json!({
                "add": {
                    "path": "first.parquet",
                    "partitionValues": {},
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true
                }
            }),
            json!({
                "add": {
                    "path": "second.parquet",
                    "partitionValues": {},
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true
                }
            }),
        ];

        assert_eq!(parsed_commits, expected_commit);

        test_read(
            &ArrowEngineData::new(RecordBatch::try_new(
                Arc::new(schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            )?),
            &table_url,
            engine,
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn test_append_partitioned() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    let partition_col = "partition";

    // create a simple partitioned table: one int column named 'number', partitioned by string
    // column named 'partition'
    let table_schema = Arc::new(StructType::new(vec![
        StructField::nullable("number", DataType::INTEGER),
        StructField::nullable("partition", DataType::STRING),
    ]));
    let data_schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    for (table_url, engine, store, table_name) in
        setup_test_tables(table_schema.clone(), &[partition_col]).await?
    {
        let commit_info = new_commit_info()?;

        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let mut txn = snapshot.transaction()?.with_commit_info(commit_info);

        // create two new arrow record batches to append
        let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(data_schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });
        let partition_vals = vec!["a", "b"];

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let write_context = Arc::new(txn.get_write_context());
        let tasks = append_data
            .into_iter()
            .zip(partition_vals)
            .map(|(data, partition_val)| {
                // arc clones
                let engine = engine.clone();
                let write_context = write_context.clone();
                tokio::task::spawn(async move {
                    engine
                        .write_parquet(
                            data.as_ref().unwrap(),
                            write_context.as_ref(),
                            HashMap::from([(partition_col.to_string(), partition_val.to_string())]),
                            true,
                        )
                        .await
                })
            });

        let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
        for meta in add_files_metadata {
            txn.add_files(meta?);
        }

        // commit!
        txn.commit(engine.as_ref())?;

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        let size =
            get_and_check_all_parquet_sizes(store.clone(), format!("/{table_name}/").as_str())
                .await;
        // check that the timestamps in commit_info and add actions are within 10s of SystemTime::now()
        // before we clear them for comparison
        check_action_timestamps(parsed_commits.iter())?;

        // set timestamps to 0 and paths to known string values for comparison
        // (otherwise timestamps are non-deterministic and paths are random UUIDs)
        set_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
        set_value(&mut parsed_commits[1], "add.path", json!("first.parquet"))?;
        set_value(&mut parsed_commits[2], "add.modificationTime", json!(0))?;
        set_value(&mut parsed_commits[2], "add.path", json!("second.parquet"))?;

        let expected_commit = vec![
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "UNKNOWN",
                    "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                    "operationParameters": {},
                    "engineCommitInfo": {
                        "engineInfo": "default engine"
                    }
                }
            }),
            json!({
                "add": {
                    "path": "first.parquet",
                    "partitionValues": {
                        "partition": "a"
                    },
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true
                }
            }),
            json!({
                "add": {
                    "path": "second.parquet",
                    "partitionValues": {
                        "partition": "b"
                    },
                    "size": size,
                    "modificationTime": 0,
                    "dataChange": true
                }
            }),
        ];

        assert_eq!(parsed_commits, expected_commit);

        test_read(
            &ArrowEngineData::new(RecordBatch::try_new(
                Arc::new(table_schema.as_ref().try_into_arrow()?),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"])),
                ],
            )?),
            &table_url,
            engine,
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn test_append_invalid_schema() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let table_schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));
    // incompatible data schema: one string column named 'string'
    let data_schema = Arc::new(StructType::new(vec![StructField::nullable(
        "string",
        DataType::STRING,
    )]));

    for (table_url, engine, _store, _table_name) in setup_test_tables(table_schema, &[]).await? {
        let commit_info = new_commit_info()?;

        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let txn = snapshot.transaction()?.with_commit_info(commit_info);

        // create two new arrow record batches to append
        let append_data = [["a", "b"], ["c", "d"]].map(|data| -> DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(data_schema.as_ref().try_into_arrow()?),
                vec![Arc::new(StringArray::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });

        // write data out by spawning async tasks to simulate executors
        let engine = Arc::new(engine);
        let write_context = Arc::new(txn.get_write_context());
        let tasks = append_data.into_iter().map(|data| {
            // arc clones
            let engine = engine.clone();
            let write_context = write_context.clone();
            tokio::task::spawn(async move {
                engine
                    .write_parquet(
                        data.as_ref().unwrap(),
                        write_context.as_ref(),
                        HashMap::new(),
                        true,
                    )
                    .await
            })
        });

        let mut add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
        assert!(add_files_metadata.all(|res| match res {
            Err(KernelError::Arrow(ArrowError::SchemaError(_))) => true,
            Err(KernelError::Backtraced { source, .. })
                if matches!(&*source, KernelError::Arrow(ArrowError::SchemaError(_))) =>
                true,
            _ => false,
        }));
    }
    Ok(())
}

#[tokio::test]
async fn test_write_txn_actions() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )]));

    for (table_url, engine, store, table_name) in setup_test_tables(schema, &[]).await? {
        let commit_info = new_commit_info()?;

        // can't have duplicate app_id in same transaction
        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        assert!(matches!(
            snapshot
                .transaction()?
                .with_transaction_id("app_id1".to_string(), 0)
                .with_transaction_id("app_id1".to_string(), 1)
                .commit(&engine),
            Err(KernelError::Generic(msg)) if msg == "app_id app_id1 already exists in transaction"
        ));

        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
        let txn = snapshot
            .transaction()?
            .with_commit_info(commit_info)
            .with_transaction_id("app_id1".to_string(), 1)
            .with_transaction_id("app_id2".to_string(), 2);

        // commit!
        txn.commit(&engine)?;

        let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, Some(1))?);
        assert_eq!(
            snapshot.clone().get_app_id_version("app_id1", &engine)?,
            Some(1)
        );
        assert_eq!(
            snapshot.clone().get_app_id_version("app_id2", &engine)?,
            Some(2)
        );
        assert_eq!(snapshot.get_app_id_version("app_id3", &engine)?, None);

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        *parsed_commits[0]
            .get_mut("commitInfo")
            .unwrap()
            .get_mut("timestamp")
            .unwrap() = serde_json::Value::Number(0.into());

        let time_ms: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()
            .unwrap();

        // check that last_updated times are identical
        let last_updated1 = parsed_commits[1]
            .get("txn")
            .unwrap()
            .get("lastUpdated")
            .unwrap();
        let last_updated2 = parsed_commits[2]
            .get("txn")
            .unwrap()
            .get("lastUpdated")
            .unwrap();
        assert_eq!(last_updated1, last_updated2);

        let last_updated = parsed_commits[1]
            .get_mut("txn")
            .unwrap()
            .get_mut("lastUpdated")
            .unwrap();
        // sanity check that last_updated time is within 10s of now
        assert!((last_updated.as_i64().unwrap() - time_ms).abs() < 10_000);
        *last_updated = serde_json::Value::Number(1.into());

        let last_updated = parsed_commits[2]
            .get_mut("txn")
            .unwrap()
            .get_mut("lastUpdated")
            .unwrap();
        // sanity check that last_updated time is within 10s of now
        assert!((last_updated.as_i64().unwrap() - time_ms).abs() < 10_000);
        *last_updated = serde_json::Value::Number(2.into());

        let expected_commit = vec![
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "UNKNOWN",
                    "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                    "operationParameters": {},
                    "engineCommitInfo": {
                        "engineInfo": "default engine"
                    }
                }
            }),
            json!({
                "txn": {
                    "appId": "app_id1",
                    "version": 1,
                    "lastUpdated": 1
                }
            }),
            json!({
                "txn": {
                    "appId": "app_id2",
                    "version": 2,
                    "lastUpdated": 2
                }
            }),
        ];

        assert_eq!(parsed_commits, expected_commit);
    }
    Ok(())
}

#[tokio::test]
async fn test_append_timestamp_ntz() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a table with TIMESTAMP_NTZ column
    let schema = Arc::new(StructType::new(vec![StructField::nullable(
        "ts_ntz",
        DataType::TIMESTAMP_NTZ,
    )]));

    let (store, engine, table_location) = engine_store_setup("test_table_timestamp_ntz", true);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        true, // enable "timestamp without timezone" feature
    )
    .await?;

    let commit_info = new_commit_info()?;

    let snapshot = Arc::new(Snapshot::try_new(table_url.clone(), &engine, None)?);
    let mut txn = snapshot.transaction()?.with_commit_info(commit_info);

    // Create Arrow data with TIMESTAMP_NTZ values including edge cases
    // These are microseconds since Unix epoch
    let timestamp_values = vec![
        0i64,                  // Unix epoch (1970-01-01T00:00:00.000000)
        1634567890123456i64,   // 2021-10-18T12:31:30.123456
        1634567950654321i64,   // 2021-10-18T12:32:30.654321
        1672531200000000i64,   // 2023-01-01T00:00:00.000000
        253402300799999999i64, // 9999-12-31T23:59:59.999999 (near max valid timestamp)
        -62135596800000000i64, // 0001-01-01T00:00:00.000000 (near min valid timestamp)
    ];

    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(TimestampMicrosecondArray::from(timestamp_values))],
    )?;

    // Write data
    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());

    let add_files_metadata = engine
        .write_parquet(
            &ArrowEngineData::new(data.clone()),
            write_context.as_ref(),
            HashMap::new(),
            true,
        )
        .await?;

    txn.add_files(add_files_metadata);

    // Commit the transaction
    txn.commit(engine.as_ref())?;

    // Verify the commit was written correctly
    let commit1 = store
        .get(&Path::from(
            "/test_table_timestamp_ntz/_delta_log/00000000000000000001.json",
        ))
        .await?;

    let parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
        .into_iter::<serde_json::Value>()
        .try_collect()?;

    // Check that we have the expected number of commits (commitInfo + add)
    assert_eq!(parsed_commits.len(), 2);

    // Check that the add action exists
    assert!(parsed_commits[1].get("add").is_some());

    // Verify the data can be read back correctly
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}
