//! Integration tests that exercise CommitInfo generation for kernel-authored commits.

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, RecordBatch, StringArray, StructArray};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema,
};
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::Schema;
use delta_kernel::transaction::CommitInfoClientOptions;
use itertools::Itertools;
use serde_json::{json, Deserializer};
use test_utils::{load_and_begin_transaction, set_json_value, setup_test_tables};

use crate::common::write_utils::{get_simple_int_schema, validate_txn_id, ZERO_UUID};

#[tokio::test]
async fn test_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // create a transaction
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_engine_info("default engine");

        // commit!
        let _ = txn.commit(&engine)?;

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commit: serde_json::Value = serde_json::from_slice(&commit1.bytes().await?)?;

        validate_txn_id(&parsed_commit["commitInfo"]);

        set_json_value(&mut parsed_commit, "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commit, "commitInfo.txnId", json!(ZERO_UUID))?;

        let expected_commit = json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineInfo": "default engine",
                "txnId": ZERO_UUID,
            }
        });

        assert_eq!(parsed_commit, expected_commit);
    }
    Ok(())
}

#[tokio::test]
async fn test_commit_info_action() -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();
    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_engine_info("default engine");

        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        validate_txn_id(&parsed_commits[0]["commitInfo"]);

        // set timestamps to 0, paths and txn_id to known string values for comparison
        // (otherwise timestamps are non-deterministic, paths and txn_id are random UUIDs)
        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;

        let expected_commit = vec![json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "UNKNOWN",
                "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                "operationParameters": {},
                "engineInfo": "default engine",
                "txnId": ZERO_UUID
            }
        })];

        assert_eq!(parsed_commits, expected_commit);
    }
    Ok(())
}

/// Engine-set `operationParameters` and `operationMetrics` are written to the commit's CommitInfo
/// with their exact entries.
#[tokio::test]
async fn test_commit_info_with_operation_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_operation("WRITE".to_string())
            .with_commit_info_options(
                CommitInfoClientOptions::new()
                    .with_operation_parameters([("mode", "Append"), ("partitionBy", "[]")])
                    .with_operation_metrics([("numFiles", "1"), ("numOutputRows", "10")]),
            );

        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let parsed: serde_json::Value = serde_json::from_slice(&commit.bytes().await?)?;
        let ci = &parsed["commitInfo"];

        assert_eq!(ci["operation"], "WRITE");
        assert_eq!(
            ci["operationParameters"],
            json!({"mode": "Append", "partitionBy": "[]"})
        );
        assert_eq!(
            ci["operationMetrics"],
            json!({"numFiles": "1", "numOutputRows": "10"})
        );
    }
    Ok(())
}

/// An empty `operationMetrics` map is written as `{}`, unlike the unset case, which is omitted
/// (see `test_commit_info`).
#[tokio::test]
async fn test_commit_info_empty_operation_metrics_written_as_empty_map(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_operation("WRITE".to_string())
            .with_commit_info_options(
                CommitInfoClientOptions::new().with_operation_metrics(Vec::<(&str, &str)>::new()),
            );
        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;
        let parsed: serde_json::Value = serde_json::from_slice(&commit.bytes().await?)?;
        let ci = &parsed["commitInfo"];

        assert_eq!(ci["operationMetrics"], json!({}));
        // operationParameters is always present; unset serializes as an empty map too.
        assert_eq!(ci["operationParameters"], json!({}));
    }
    Ok(())
}

/// `with_additional_commit_info` merges caller fields into the on-disk `commitInfo`: engine-only
/// fields stay (a nested struct stays a nested object), a colliding field takes kernel's value,
/// and kernel-managed fields are present.
#[tokio::test]
async fn test_commit_info_with_engine_commit_info() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // A nested "notebook" struct, a flat "clusterId", and an "operation" field that collides
        // with the kernel-managed one (kernel must win).
        let notebook_fields = Fields::from(vec![
            Field::new("notebookId", ArrowDataType::Utf8, false),
            Field::new("notebookPath", ArrowDataType::Utf8, false),
        ]);
        let notebook = StructArray::new(
            notebook_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["4443029"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["/Users/me/nb"])) as ArrayRef,
            ],
            None,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("clusterId", ArrowDataType::Utf8, false),
            Field::new("operation", ArrowDataType::Utf8, true),
            Field::new("notebook", ArrowDataType::Struct(notebook_fields), false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1027-202406-pooh991"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["STALE_OP"])) as ArrayRef,
                Arc::new(notebook) as ArrayRef,
            ],
        )?;
        let kernel_schema: Schema = arrow_schema.as_ref().try_into_kernel()?;

        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_operation("INSERT".to_string())
            .with_additional_commit_info(
                Box::new(ArrowEngineData::new(batch)),
                Arc::new(kernel_schema),
            );
        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;
        let parsed: serde_json::Value = serde_json::from_slice(&commit.bytes().await?)?;
        let ci = &parsed["commitInfo"];

        // Kernel wins on the colliding field; engine-only fields pass through.
        assert_eq!(ci["operation"], "INSERT");
        assert_eq!(ci["clusterId"], "1027-202406-pooh991");
        // The nested struct round-trips as a nested object, not a stringified blob.
        assert_eq!(
            ci["notebook"],
            json!({"notebookId": "4443029", "notebookPath": "/Users/me/nb"})
        );
        // A kernel-managed field is present.
        assert_eq!(ci["operationParameters"], json!({}));
    }
    Ok(())
}
