//! Integration tests that exercise CommitInfo generation for kernel-authored commits.

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, MapBuilder, RecordBatch, StringArray, StringBuilder};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::schema_ref;
use delta_kernel::transaction::{TransactionOptions, UpdateTableOperation};
use delta_kernel::Snapshot;
use itertools::Itertools;
use rstest::rstest;
use serde_json::{json, Deserializer};
use test_utils::{load_and_begin_transaction, set_json_value, setup_test_tables};

use crate::common::write_utils::{
    get_simple_int_schema, validate_timestamp, validate_txn_id, ZERO_UUID,
};

#[tokio::test]
async fn test_commit_info_defaults_to_empty_parameters_and_omitted_metrics(
) -> Result<(), Box<dyn std::error::Error>> {
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

        assert_eq!(
            parsed_commit["commitInfo"]["operationParameters"],
            json!({})
        );
        assert!(parsed_commit["commitInfo"]
            .get("operationMetrics")
            .is_none());

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
async fn transaction_builder_writes_operation_parameters_and_metrics(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_options(
                TransactionOptions::new()
                    .with_operation_parameters([("mode", "Append")])?
                    .with_operation_metrics([("numFiles", "3")])?,
            )
            .build(&engine, Box::new(FileSystemCommitter::new()))?;

        transaction.commit(&engine)?.unwrap_committed();

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;
        let parsed_commit: serde_json::Value = serde_json::from_slice(&commit.bytes().await?)?;

        assert_eq!(
            parsed_commit["commitInfo"]["operationParameters"],
            json!({"mode": "Append"})
        );
        assert_eq!(
            parsed_commit["commitInfo"]["operationMetrics"],
            json!({"numFiles": "3"})
        );
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

#[tokio::test]
async fn test_commit_info_with_operation_maps() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_operation("WRITE".to_string())
            .with_operation_parameters([("stale", Some("value"))])
            .with_operation_parameters([
                ("mode", Some("Append")),
                ("mode", Some("Overwrite")),
                ("partitionBy", Some("[]")),
                ("description", None),
            ])
            .with_operation_metrics([("numFiles", Some("1")), ("numOutputRows", Some("10"))]);

        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;
        let parsed: serde_json::Value = serde_json::from_slice(&commit.bytes().await?)?;
        let commit_info = &parsed["commitInfo"];

        assert_eq!(
            commit_info["operationParameters"],
            json!({"description": null, "mode": "Overwrite", "partitionBy": "[]"})
        );
        assert_eq!(
            commit_info["operationMetrics"],
            json!({"numFiles": "1", "numOutputRows": "10"})
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum EmptyOperationMap {
    Parameters,
    Metrics,
}

#[rstest]
#[case::parameters(EmptyOperationMap::Parameters, "operationParameters")]
#[case::metrics(EmptyOperationMap::Metrics, "operationMetrics")]
#[tokio::test]
async fn test_commit_info_with_empty_operation_map(
    #[case] operation_map: EmptyOperationMap,
    #[case] field_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        let txn = load_and_begin_transaction(table_url.clone(), &engine)?;
        let txn = match operation_map {
            EmptyOperationMap::Parameters => {
                txn.with_operation_parameters(std::iter::empty::<(&str, Option<&str>)>())
            }
            EmptyOperationMap::Metrics => {
                txn.with_operation_metrics(std::iter::empty::<(&str, Option<&str>)>())
            }
        };

        let _ = txn.commit(&engine)?;

        let commit = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;
        let parsed: serde_json::Value = serde_json::from_slice(&commit.bytes().await?)?;

        assert_eq!(parsed["commitInfo"][field_name], json!({}));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum OperationMapSetters {
    Unset,
    BeforeCommitInfo,
    AfterCommitInfo,
}

/// Verifies that Kernel ignores reserved fields from `with_commit_info`, preserves custom fields,
/// and applies typed operation maps independently of setter order.
#[rstest]
#[case::unset(OperationMapSetters::Unset)]
#[case::before_commit_info(OperationMapSetters::BeforeCommitInfo)]
#[case::after_commit_info(OperationMapSetters::AfterCommitInfo)]
#[tokio::test]
async fn test_commit_info_merges_custom_fields_and_ignores_reserved_fields(
    #[case] setters: OperationMapSetters,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        let mut parameters_builder =
            MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        parameters_builder.keys().append_value("stale_parameter");
        parameters_builder.values().append_value("value");
        parameters_builder.append(true)?;
        let stale_operation_parameters = Arc::new(parameters_builder.finish()) as ArrayRef;

        let mut metrics_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        metrics_builder.keys().append_value("stale_metric");
        metrics_builder.values().append_value("1");
        metrics_builder.append(true)?;
        let stale_operation_metrics = Arc::new(metrics_builder.finish()) as ArrayRef;

        // Build engine_commit_info with:
        //   - "myApp"           : engine-only field, must pass through unchanged.
        //   - "myVersion"       : engine-only field, must pass through unchanged.
        //   - "operation"       : reserved field that Kernel ignores in favor of "WRITE".
        //   - "operationParameters": reserved field controlled by the typed setter.
        //   - "operationMetrics": reserved field controlled by the typed setter.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("myApp", ArrowDataType::Utf8, false),
            Field::new("myVersion", ArrowDataType::Utf8, false),
            Field::new("operation", ArrowDataType::Utf8, false),
            Field::new(
                "operationParameters",
                stale_operation_parameters.data_type().clone(),
                true,
            ),
            Field::new(
                "operationMetrics",
                stale_operation_metrics.data_type().clone(),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["spark"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["3.5.0"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["STALE_OP"])) as ArrayRef,
                stale_operation_parameters,
                stale_operation_metrics,
            ],
        )?;
        let engine_schema = schema_ref! {
            not_null "myApp": STRING,
            not_null "myVersion": STRING,
            nullable "operation": STRING,
            nullable "operationParameters": { STRING => nullable STRING },
            nullable "operationMetrics": { STRING => nullable STRING },
        };

        let txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_operation("WRITE".to_string());
        let txn = match setters {
            OperationMapSetters::Unset => {
                txn.with_commit_info(Box::new(ArrowEngineData::new(batch)), engine_schema)
            }
            OperationMapSetters::BeforeCommitInfo => txn
                .with_operation_parameters([("mode", Some("Append"))])
                .with_operation_metrics([("numFiles", Some("3"))])
                .with_commit_info(Box::new(ArrowEngineData::new(batch)), engine_schema),
            OperationMapSetters::AfterCommitInfo => txn
                .with_commit_info(Box::new(ArrowEngineData::new(batch)), engine_schema)
                .with_operation_parameters([("mode", Some("Append"))])
                .with_operation_metrics([("numFiles", Some("3"))]),
        };

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
        validate_timestamp(&parsed_commits[0]["commitInfo"]);

        // Zero out non-deterministic fields for stable comparison.
        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
        let (operation_parameters, operation_metrics) = match setters {
            OperationMapSetters::Unset => (json!({}), None),
            OperationMapSetters::BeforeCommitInfo | OperationMapSetters::AfterCommitInfo => {
                (json!({"mode": "Append"}), Some(json!({"numFiles": "3"})))
            }
        };
        // Null-valued CommitInfo fields (inCommitTimestamp, isBlindAppend, engineInfo) are
        // omitted from the JSON, consistent with how the Delta log serializes optional fields.
        let mut expected_commit_info = json!({
            "myApp": "spark",
            "myVersion": "3.5.0",
            "operation": "WRITE",
            "operationParameters": operation_parameters,
            "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
            "txnId": ZERO_UUID,
            "timestamp": 0,
        });
        if let Some(operation_metrics) = operation_metrics {
            expected_commit_info["operationMetrics"] = operation_metrics;
        }
        let expected_commits = vec![json!({"commitInfo": expected_commit_info})];

        assert_eq!(parsed_commits, expected_commits);
    }
    Ok(())
}
