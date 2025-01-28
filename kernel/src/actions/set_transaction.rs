use std::sync::{Arc, LazyLock};

use crate::actions::visitors::SetTransactionVisitor;
use crate::actions::{get_log_schema, SetTransaction, SET_TRANSACTION_NAME};
use crate::snapshot::Snapshot;
use crate::{
    DeltaResult, Engine, EngineData, Expression as Expr, ExpressionRef, RowVisitor as _, SchemaRef,
};

pub use crate::actions::visitors::SetTransactionMap;
pub struct SetTransactionScanner {
    snapshot: Arc<Snapshot>,
}

impl SetTransactionScanner {
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        SetTransactionScanner { snapshot }
    }

    /// Scan the entire log for all application ids but terminate early if a specific application id is provided
    fn scan_application_transactions(
        &self,
        engine: &dyn Engine,
        application_id: Option<&str>,
    ) -> DeltaResult<SetTransactionMap> {
        let schema = Self::get_txn_schema()?;
        let mut visitor = SetTransactionVisitor::new(application_id.map(|s| s.to_owned()));
        // If a specific id is requested then we can terminate log replay early as soon as it was
        // found. If all ids are requested then we are forced to replay the entire log.
        for maybe_data in self.replay_for_app_ids(engine, schema.clone())? {
            let (txns, _) = maybe_data?;
            visitor.visit_rows_of(txns.as_ref())?;
            // if a specific id is requested and a transaction was found, then return
            if application_id.is_some() && !visitor.set_transactions.is_empty() {
                break;
            }
        }

        Ok(visitor.set_transactions)
    }

    // Factored out to facilitate testing
    fn get_txn_schema() -> DeltaResult<SchemaRef> {
        get_log_schema().project(&[SET_TRANSACTION_NAME])
    }

    // Factored out to facilitate testing
    fn replay_for_app_ids(
        &self,
        engine: &dyn Engine,
        schema: SchemaRef,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        // This meta-predicate should be effective because all the app ids end up in a single
        // checkpoint part when patitioned by `add.path` like the Delta spec requires. There's no
        // point filtering by a particular app id, even if we have one, because app ids are all in
        // the a single checkpoint part having large min/max range (because they're usually uuids).
        static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
            Some(Arc::new(
                Expr::column([SET_TRANSACTION_NAME, "appId"]).is_not_null(),
            ))
        });
        self.snapshot
            .log_segment()
            .replay(engine, schema.clone(), schema, META_PREDICATE.clone())
    }

    /// Scan the Delta Log for the latest transaction entry of an application
    pub fn application_transaction(
        &self,
        engine: &dyn Engine,
        application_id: &str,
    ) -> DeltaResult<Option<SetTransaction>> {
        let mut transactions = self.scan_application_transactions(engine, Some(application_id))?;
        Ok(transactions.remove(application_id))
    }

    /// Scan the Delta Log to obtain the latest transaction for all applications
    pub fn application_transactions(&self, engine: &dyn Engine) -> DeltaResult<SetTransactionMap> {
        self.scan_application_transactions(engine, None)
    }
}

#[cfg(all(test, feature = "sync-engine"))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::Table;
    use itertools::Itertools;

    fn get_latest_transactions(
        path: &str,
        app_id: &str,
    ) -> (SetTransactionMap, Option<SetTransaction>) {
        let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        let txn_scan = SetTransactionScanner::new(snapshot.into());

        (
            txn_scan.application_transactions(&engine).unwrap(),
            txn_scan.application_transaction(&engine, app_id).unwrap(),
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

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        let txn = SetTransactionScanner::new(snapshot.into());
        let txn_schema = SetTransactionScanner::get_txn_schema().unwrap();

        // The checkpoint has five parts, each containing one action. There are two app ids.
        let data: Vec<_> = txn
            .replay_for_app_ids(&engine, txn_schema.clone())
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(data.len(), 2);
    }
}
