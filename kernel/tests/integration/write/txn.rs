//! Integration tests for transaction-identifier write paths.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::object_store::path::Path;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelError, Snapshot};
use itertools::Itertools;
use rstest::rstest;
use serde_json::{Deserializer, json};
use test_utils::{assert_result_error_with_message, set_json_value, setup_test_tables};

use crate::common::write_utils::{ZERO_UUID, get_simple_int_schema, validate_txn_id};

#[rstest]
#[case::builder(false)]
#[case::transaction(true)]
#[tokio::test]
async fn test_write_txn_actions(
    #[case] after_build: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, store, table_name) in
        setup_test_tables(schema, &[], None, "test_table").await?
    {
        // can't have duplicate app_id in same transaction
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert!(matches!(
            snapshot.clone().update_table(UpdateTableOperation::Write, "test-engine", true)
                .with_transaction_id("app_id1".to_string(), 0)
                .with_transaction_id("app_id1".to_string(), 1)
                .build(&engine, Box::new(FileSystemCommitter::new())),
            Err(KernelError::InvalidTransactionState(msg)) if msg.contains("app_id app_id1")
        ));

        if after_build {
            let txn = snapshot
                .clone()
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .with_transaction_id("app_id1".to_string(), 0)
                .build(&engine, Box::new(FileSystemCommitter::new()))?
                .with_transaction_id("app_id1".to_string(), 1);
            assert_result_error_with_message(txn.commit(&engine), "app_id app_id1");
        }

        let builder = snapshot.update_table(UpdateTableOperation::Write, "default engine", true);
        let txn = if after_build {
            builder
                .build(&engine, Box::new(FileSystemCommitter::new()))?
                .with_transaction_id("app_id1".to_string(), 1)
                .with_transaction_id("app_id2".to_string(), 2)
        } else {
            builder
                .with_transaction_id("app_id1".to_string(), 1)
                .with_transaction_id("app_id2".to_string(), 2)
                .build(&engine, Box::new(FileSystemCommitter::new()))?
        };

        // commit!
        txn.commit(&engine)?.unwrap_committed();

        let snapshot = Snapshot::builder_for(table_url.clone())
            .at_version(1)
            .build(&engine)?;
        assert_eq!(snapshot.get_app_id_version("app_id1", &engine)?, Some(1));
        assert_eq!(snapshot.get_app_id_version("app_id2", &engine)?, Some(2));
        assert_eq!(snapshot.get_app_id_version("app_id3", &engine)?, None);

        let commit1 = store
            .get(&Path::from(format!(
                "/{table_name}/_delta_log/00000000000000000001.json"
            )))
            .await?;

        let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;

        set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0)).unwrap();

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

        validate_txn_id(&parsed_commits[0]["commitInfo"]);

        set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;

        let expected_commit = vec![
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "WRITE",
                    "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                    "operationParameters": {},
                    "engineInfo": "default engine",
                    "txnId": ZERO_UUID
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
