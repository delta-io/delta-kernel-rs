//! Builder for transactions against an existing table.
//!
//! This builder owns configurable transaction and `commitInfo` intent. Calling
//! [`TransactionBuilder::build`] validates and freezes that state into a [`Transaction`]. New
//! configuration APIs belong here rather than on `Transaction`.

use super::super::update::ExistingTransactionConfig;
use super::super::{Operation, Transaction, TransactionConfig, TransactionOptions};
use crate::expressions::ColumnName;
use crate::snapshot::SnapshotRef;
use crate::table_features::{
    iceberg_compat_v3_column_defaults_validation, Operation as TableOperation, TableFeature,
};
use crate::{DeltaResult, Engine};

/// Accumulates the complete intent for a transaction against an existing table.
///
/// Once [`build`](Self::build) is called, the returned [`Transaction`] represents the frozen
/// configuration and provenance intent. Late-produced data is supplied separately at commit.
#[derive(Clone)]
pub struct TransactionBuilder {
    snapshot: SnapshotRef,
    config: TransactionConfig,
    operation: Option<Operation>,
    domain_metadata_removals: Vec<String>,
    is_blind_append: bool,
}

impl std::fmt::Debug for TransactionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBuilder")
            .field("snapshot_version", &self.snapshot.version())
            .field("operation", &self.operation)
            .finish()
    }
}

impl TransactionBuilder {
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        TransactionBuilder {
            snapshot,
            config: TransactionConfig::default(),
            operation: None,
            domain_metadata_removals: Vec::new(),
            is_blind_append: false,
        }
    }

    fn validate_table(&self, engine: &dyn Engine) -> DeltaResult<Option<Vec<ColumnName>>> {
        self.snapshot
            .table_configuration()
            .ensure_operation_supported(TableOperation::Write)?;
        let physical_clustering_columns = self.snapshot.get_physical_clustering_columns(engine)?;
        let table_config = self.snapshot.table_configuration();
        if table_config.is_feature_enabled(&TableFeature::IcebergCompatV3) {
            iceberg_compat_v3_column_defaults_validation(table_config)?;
        }
        Ok(physical_clustering_columns)
    }

    /// Validate and freeze the staged intent into a transaction.
    ///
    /// For clustered tables, this reads clustering columns from domain metadata through `engine`.
    ///
    /// # Errors
    ///
    /// Returns an error if deterministic transaction intent is invalid, including duplicate
    /// application transaction identifiers or conflicting domain operations.
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Transaction> {
        let physical_clustering_columns = self.validate_table(engine)?;
        Transaction::try_new_existing_table(
            self.snapshot,
            physical_clustering_columns,
            ExistingTransactionConfig {
                common: self.config,
                operation: self.operation,
                domain_metadata_removals: self.domain_metadata_removals,
                is_blind_append: self.is_blind_append,
            },
        )
    }

    /// Replaces the options that are valid for every transaction variant.
    pub fn with_options(mut self, options: TransactionOptions) -> Self {
        self.config.set_options(options);
        self
    }

    /// Set whether file actions supplied at commit represent a logical data change.
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.config.set_data_change(data_change);
        self
    }

    /// Acknowledge that the connector applies column defaults before writing.
    pub fn ack_column_defaults(mut self) -> Self {
        self.config.acknowledge_column_defaults();
        self
    }

    /// Mark the transaction as a blind append assertion.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
        self
    }

    /// Set the operation recorded in `commitInfo`.
    pub fn with_operation(mut self, operation: impl Into<Operation>) -> Self {
        self.operation = Some(operation.into());
        self
    }

    /// Remove user-controlled domain metadata in the commit.
    pub fn with_domain_metadata_removed(mut self, domain: String) -> Self {
        self.domain_metadata_removals.push(domain);
        self
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::transaction::builder::TransactionBuilder;
    use crate::transaction::{Operation, TransactionOptions};
    use crate::unit_test_utils::load_test_table;
    use crate::DeltaResult;

    #[test]
    fn build_freezes_configured_commit_intent() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = TransactionBuilder::new(snapshot)
            .with_operation("WRITE".to_string())
            .with_options(
                TransactionOptions::new()
                    .with_engine_info("test-engine")
                    .with_operation_parameters([("mode", "Append")])?
                    .with_operation_metrics([("numFiles", "3")])?,
            )
            .with_blind_append()
            .build(engine.as_ref())?;

        assert_eq!(
            transaction.operation.as_ref().map(Operation::as_str),
            Some("WRITE")
        );
        assert_eq!(transaction.engine_info.as_deref(), Some("test-engine"));
        assert_eq!(
            transaction.operation_parameters.get("mode").unwrap(),
            "Append"
        );
        assert_eq!(transaction.operation_metrics.get("numFiles").unwrap(), "3");
        assert!(transaction.is_blind_append);
        Ok(())
    }

    #[test]
    fn build_rejects_duplicate_application_transaction_ids() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = TransactionBuilder::new(snapshot)
            .with_options(
                TransactionOptions::new()
                    .with_transaction_id("app".to_string(), 1)
                    .with_transaction_id("app".to_string(), 2),
            )
            .build(engine.as_ref())
            .unwrap_err();

        assert!(error.to_string().contains("app_id app already exists"));
        Ok(())
    }

    #[test]
    fn build_rejects_blind_append_without_data_change() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = TransactionBuilder::new(snapshot)
            .with_blind_append()
            .with_data_change(false)
            .build(engine.as_ref())
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("Blind append requires data_change to be true"));
        Ok(())
    }

    #[rstest]
    #[case(Operation::CreateTable)]
    #[case(Operation::ReplaceTable)]
    #[case(Operation::AlterTable)]
    fn existing_table_builder_rejects_ddl_operations(
        #[case] operation: Operation,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = TransactionBuilder::new(snapshot)
            .with_operation(operation)
            .build(engine.as_ref())
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("require their dedicated transaction builders"));
        Ok(())
    }

    #[test]
    fn repeated_with_options_replaces_the_complete_options_value() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = TransactionBuilder::new(snapshot)
            .with_options(
                TransactionOptions::new()
                    .with_engine_info("first-engine")
                    .with_operation_metrics([("numFiles", "1")])?
                    .with_transaction_id("first-app".to_string(), 1)
                    .with_domain_metadata("first-domain".to_string(), "{}".to_string()),
            )
            .with_options(TransactionOptions::new().with_engine_info("second-engine"))
            .build(engine.as_ref())?;

        assert_eq!(transaction.engine_info.as_deref(), Some("second-engine"));
        assert!(transaction.operation_metrics.is_empty());
        assert!(transaction.transaction_ids.is_empty());
        assert!(transaction.user_domain_metadata_additions.is_empty());
        Ok(())
    }

    #[rstest]
    #[case::parameters(true)]
    #[case::metrics(false)]
    fn structured_operation_metadata_rejects_duplicate_keys(
        #[case] parameters: bool,
    ) -> DeltaResult<()> {
        let result = if parameters {
            TransactionOptions::new()
                .with_operation_parameters([("duplicate", "first"), ("duplicate", "second")])
        } else {
            TransactionOptions::new()
                .with_operation_metrics([("duplicate", "first"), ("duplicate", "second")])
        };

        let error = result.unwrap_err();
        assert!(error.to_string().contains("appears more than once"));
        Ok(())
    }

    #[rstest]
    #[case::parameters(true)]
    #[case::metrics(false)]
    fn structured_operation_metadata_rejects_empty_keys(
        #[case] parameters: bool,
    ) -> DeltaResult<()> {
        let result = if parameters {
            TransactionOptions::new().with_operation_parameters([("", "value")])
        } else {
            TransactionOptions::new().with_operation_metrics([("", "value")])
        };

        let error = result.unwrap_err();
        assert!(error.to_string().contains("key cannot be empty"));
        Ok(())
    }
}
