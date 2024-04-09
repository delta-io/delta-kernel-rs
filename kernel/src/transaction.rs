use std::sync::Arc;

use crate::actions::visitors::TransactionVisitor;
use crate::actions::{get_log_schema, TRANSACTION_NAME};
use crate::snapshot::Snapshot;
use crate::EngineInterface;
use crate::{actions::Transaction, DeltaResult};

pub use crate::actions::visitors::TransactionMap;
pub struct TransactionScanner {
    snapshot: Arc<Snapshot>,
}

impl TransactionScanner {
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        TransactionScanner { snapshot }
    }

    /// Scan the entire log for all application ids but terminate early if a specific application id is provided
    fn scan_application_transactions(
        &self,
        engine: &dyn EngineInterface,
        application_id: Option<&str>,
    ) -> DeltaResult<TransactionMap> {
        let schema = get_log_schema().project(&[TRANSACTION_NAME])?;

        let mut visitor = TransactionVisitor::new(application_id.map(|s| s.to_owned()));

        // when all ids are requested then a full scan of the log to the latest checkpoint is required
        let iter =
            self.snapshot
                .log_segment
                .replay(engine, schema.clone(), schema.clone(), None)?;

        for maybe_data in iter {
            let (txns, _) = maybe_data?;
            txns.extract(schema.clone(), &mut visitor)?;
            // if a specific id is requested and a transaction was found, then return
            if application_id.is_some() && !visitor.transactions.is_empty() {
                break;
            }
        }

        Ok(visitor.transactions)
    }

    /// Scan the Delta Log for the latest transaction entry of an application
    pub fn application_transaction(
        &self,
        engine: &dyn EngineInterface,
        application_id: &str,
    ) -> DeltaResult<Option<Transaction>> {
        let mut transactions = self.scan_application_transactions(engine, Some(application_id))?;
        Ok(transactions.remove(application_id))
    }

    /// Scan the Delta Log to obtain the latest transaction for all applications
    pub fn application_transactions(
        &self,
        engine: &dyn EngineInterface,
    ) -> DeltaResult<TransactionMap> {
        self.scan_application_transactions(engine, None)
    }
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::client::sync::SyncEngineInterface;
    use crate::Table;

    fn get_latest_transactions(path: &str, app_id: &str) -> (TransactionMap, Option<Transaction>) {
        let path = std::fs::canonicalize(PathBuf::from(path)).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_interface = SyncEngineInterface::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_interface, None).unwrap();
        let txn_scan = TransactionScanner::new(snapshot.clone());

        (
            txn_scan
                .application_transactions(&engine_interface)
                .unwrap(),
            txn_scan
                .application_transaction(&engine_interface, app_id)
                .unwrap(),
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
            Some(Transaction {
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
            Some(Transaction {
                app_id: "my-app2".to_owned(),
                version: 2,
                last_updated: None
            })
            .as_ref()
        );
    }
}
