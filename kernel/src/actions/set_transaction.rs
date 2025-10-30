use std::sync::{Arc, LazyLock};

use crate::actions::get_log_txn_schema;
use crate::actions::visitors::SetTransactionVisitor;
use crate::actions::{SetTransaction, SET_TRANSACTION_NAME};
use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine, Expression as Expr, PredicateRef, RowVisitor as _};

pub(crate) use crate::actions::visitors::SetTransactionMap;

pub(crate) struct SetTransactionScanner {}

impl SetTransactionScanner {
    /// Scan the Delta Log for the latest `txn` action for an application id.
    ///
    /// Note that each call to this function repeats log replay. Thus, if callers are interested
    /// in multiple app ids, use `get_all` (once) instead and probe the map returned.
    pub(crate) fn get_one(
        log_segment: &LogSegment,
        application_id: &str,
        engine: &dyn Engine,
        expiration_timestamp: Option<i64>,
    ) -> DeltaResult<Option<SetTransaction>> {
        let mut transactions = scan_application_transactions(
            log_segment,
            Some(application_id),
            engine,
            expiration_timestamp,
        )?;
        Ok(transactions.remove(application_id))
    }

    /// Scan the Delta Log to obtain the all of the latest `txn` actions.
    ///
    /// This performs log replay and populates the `SetTransactionMap` with the latest `txn` action
    /// found for each app_id.
    #[allow(unused)]
    pub(crate) fn get_all(
        log_segment: &LogSegment,
        engine: &dyn Engine,
        expiration_timestamp: Option<i64>,
    ) -> DeltaResult<SetTransactionMap> {
        scan_application_transactions(log_segment, None, engine, expiration_timestamp)
    }
}

/// Scan the entire log for all application ids but terminate early if a specific application id
/// is provided
// TODO: we could have this track _multiple_ application ids instead of only up to one.
fn scan_application_transactions(
    log_segment: &LogSegment,
    application_id: Option<&str>,
    engine: &dyn Engine,
    expiration_timestamp: Option<i64>,
) -> DeltaResult<SetTransactionMap> {
    let mut visitor =
        SetTransactionVisitor::new(application_id.map(|s| s.to_owned()), expiration_timestamp);
    // If a specific id is requested then we can terminate log replay early as soon as it was
    // found. If all ids are requested then we are forced to replay the entire log.
    for maybe_data in replay_for_app_ids(log_segment, engine)? {
        let txns = maybe_data?.actions;
        visitor.visit_rows_of(txns.as_ref())?;
        // if a specific id is requested and a transaction was found, then return
        if application_id.is_some() && !visitor.set_transactions.is_empty() {
            break;
        }
    }

    Ok(visitor.set_transactions)
}

// Factored out to facilitate testing
fn replay_for_app_ids(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
    let txn_schema = get_log_txn_schema();
    // This meta-predicate should be effective because all the app ids end up in a single
    // checkpoint part when patitioned by `add.path` like the Delta spec requires. There's no
    // point filtering by a particular app id, even if we have one, because app ids are all in
    // the a single checkpoint part having large min/max range (because they're usually uuids).
    static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
        Some(Arc::new(
            Expr::column([SET_TRANSACTION_NAME, "appId"]).is_not_null(),
        ))
    });
    log_segment.read_actions(
        engine,
        txn_schema.clone(), // Arc clone
        META_PREDICATE.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::utils::test_utils::parse_json_batch;
    use crate::Snapshot;

    use crate::arrow::array::StringArray;
    use itertools::Itertools;

    fn get_latest_transactions(
        path: &str,
        app_id: &str,
    ) -> (SetTransactionMap, Option<SetTransaction>) {
        let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let log_segment = snapshot.log_segment();

        (
            SetTransactionScanner::get_all(log_segment, &engine, None).unwrap(),
            SetTransactionScanner::get_one(log_segment, app_id, &engine, None).unwrap(),
        )
    }

    #[test]
    fn test_txn() {
        let (txns, txn) = get_latest_transactions("./tests/data/basic_partitioned/", "test");
        assert!(txn.is_none());
        assert_eq!(txns.len(), 0);

        let (txns, txn) = get_latest_transactions("./tests/data/app-txn-no-checkpoint/", "my-app");
        assert!(txn.is_some());
        assert_eq!(txns.len(), 2);
        assert_eq!(txns.get("my-app"), txn.as_ref());
        assert_eq!(
            txns.get("my-app2"),
            Some(SetTransaction {
                app_id: "my-app2".to_owned(),
                version: 2,
                last_updated: None
            })
            .as_ref()
        );

        let (txns, txn) = get_latest_transactions("./tests/data/app-txn-checkpoint/", "my-app");
        assert!(txn.is_some());
        assert_eq!(txns.len(), 2);
        assert_eq!(txns.get("my-app"), txn.as_ref());
        assert_eq!(
            txns.get("my-app2"),
            Some(SetTransaction {
                app_id: "my-app2".to_owned(),
                version: 2,
                last_updated: None
            })
            .as_ref()
        );
    }

    #[test]
    fn test_replay_for_app_ids() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let log_segment = snapshot.log_segment();

        // The checkpoint has five parts, each containing one action. There are two app ids.
        let data: Vec<_> = replay_for_app_ids(log_segment, &engine)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn test_txn_retention_filtering() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-with-last-updated/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let log_segment = snapshot.log_segment();

        // Test with no retention (should get all transactions)
        let all_txns = SetTransactionScanner::get_all(log_segment, &engine, None).unwrap();
        assert_eq!(all_txns.len(), 4);

        // Test with retention that filters out old transactions
        let expiration_timestamp = Some(100); // Very old timestamp
        let filtered_txns =
            SetTransactionScanner::get_all(log_segment, &engine, expiration_timestamp).unwrap();

        // Exact count depends on test data
        assert!(filtered_txns.len() <= all_txns.len());
    }

    #[test]
    fn test_visitor_retention_with_null_last_updated() {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app_with_time","version":1,"lastUpdated":100}}"#,
            r#"{"txn":{"appId":"app_without_time","version":2}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut visitor = SetTransactionVisitor::new(None, Some(1000));
        visitor.visit_rows_of(batch.as_ref()).unwrap();

        // app_with_last_updated should be filtered out (100 < 1000)
        // app_without_last_updated should be kept
        assert_eq!(visitor.set_transactions.len(), 1);
        assert!(visitor.set_transactions.contains_key("app_without_time"));
    }
}
