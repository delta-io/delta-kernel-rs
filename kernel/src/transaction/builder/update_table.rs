//! Builder for transactions against an existing table.
//!
//! This builder owns configurable transaction and `commitInfo` intent. Calling
//! [`ExistingTableTransactionBuilder::build`] validates and freezes that state into a
//! [`Transaction`]. New
//! configuration APIs belong here rather than on `Transaction`.

use super::super::update_table::ExistingTransactionConfig;
use super::super::{Operation, Transaction, TransactionConfig, TransactionOptions};
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_features::{
    validate_iceberg_compat_if_needed, IcebergCompatValidationContext, Operation as TableOperation,
    TableFeature, V3_VALIDATOR,
};
use crate::transaction::schema_evolution::{evolve_table_config, SchemaOperation};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::FileMeta;
use crate::{DeltaResult, Engine, Error};

/// Accumulates the complete intent for a transaction against an existing table.
///
/// Once [`build`](Self::build) is called, the returned [`Transaction`] represents the frozen
/// configuration and provenance intent. Late-produced data is supplied separately at commit.
#[derive(Clone)]
pub struct ExistingTableTransactionBuilder {
    snapshot: SnapshotRef,
    config: TransactionConfig,
    operation: Option<Operation>,
    schema_changes: Vec<SchemaOperation>,
    domain_metadata_removals: Vec<String>,
    is_blind_append: bool,
    row_tracking_high_water_mark: Option<i64>,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    root_manifest_file: Option<FileMeta>,
}

impl std::fmt::Debug for ExistingTableTransactionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExistingTableTransactionBuilder")
            .field("snapshot_version", &self.snapshot.version())
            .field("operation", &self.operation)
            .finish()
    }
}

impl ExistingTableTransactionBuilder {
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        ExistingTableTransactionBuilder {
            snapshot,
            config: TransactionConfig::default(),
            operation: None,
            schema_changes: Vec::new(),
            domain_metadata_removals: Vec::new(),
            is_blind_append: false,
            row_tracking_high_water_mark: None,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            root_manifest_file: None,
        }
    }

    pub(crate) fn new_alter_table(snapshot: SnapshotRef) -> Self {
        Self::new(snapshot)
            .with_operation(Operation::AlterTable)
            .with_data_change(false)
    }

    fn validate_table(&self, engine: &dyn Engine) -> DeltaResult<Option<Vec<ColumnName>>> {
        self.snapshot
            .table_configuration()
            .ensure_operation_supported(TableOperation::Write)?;
        let physical_clustering_columns = self.snapshot.get_physical_clustering_columns(engine)?;
        let table_config = self.snapshot.table_configuration();
        validate_iceberg_compat_if_needed(
            table_config,
            &V3_VALIDATOR,
            IcebergCompatValidationContext::Write,
        )?;
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
        self.validate()?;
        let physical_clustering_columns = self.validate_table(engine)?;
        let effective_table_config = if self.schema_changes.is_empty() {
            None
        } else {
            Some(evolve_table_config(
                self.snapshot.table_configuration(),
                self.schema_changes.clone(),
            )?)
        };
        Transaction::try_new_existing_table(
            self.snapshot,
            physical_clustering_columns,
            ExistingTransactionConfig {
                common: self.config,
                operation: self.operation,
                effective_table_config,
                domain_metadata_removals: self.domain_metadata_removals,
                is_blind_append: self.is_blind_append,
                provided_row_tracking_high_water_mark: self.row_tracking_high_water_mark,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                root_manifest_file: self.root_manifest_file,
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

    /// Acknowledges the connector's row-tracking preservation responsibilities.
    pub fn ack_row_tracking_preservation(mut self) -> Self {
        self.config.acknowledge_row_tracking_preservation();
        self
    }

    /// Sets an explicit row-tracking high-water mark.
    pub fn with_row_tracking_high_water_mark(mut self, high_water_mark: i64) -> Self {
        self.row_tracking_high_water_mark = Some(high_water_mark);
        self
    }

    /// Stages the table's root manifest file.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub fn with_root_manifest_file(mut self, file: FileMeta) -> Self {
        self.root_manifest_file = Some(file);
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

    /// Adds schema changes to validate and apply when the transaction is built.
    pub fn with_schema_changes(
        mut self,
        changes: impl IntoIterator<Item = SchemaOperation>,
    ) -> Self {
        self.schema_changes.extend(changes);
        self
    }

    /// Adds a nullable top-level column to the table schema.
    pub fn add_column(mut self, field: StructField) -> Self {
        self.schema_changes
            .push(SchemaOperation::add_column(None, field));
        self
    }

    /// Adds a nullable field under `parent` in the table schema.
    pub fn add_column_at(mut self, parent: ColumnName, field: StructField) -> Self {
        self.schema_changes
            .push(SchemaOperation::add_column(parent, field));
        self
    }

    /// Changes a column from non-nullable to nullable.
    pub fn set_nullable(mut self, column: ColumnName) -> Self {
        self.schema_changes
            .push(SchemaOperation::SetNullable { column });
        self
    }

    /// Remove user-controlled domain metadata in the commit.
    pub fn with_domain_metadata_removed(mut self, domain: String) -> Self {
        self.domain_metadata_removals.push(domain);
        self
    }

    fn validate(&self) -> DeltaResult<()> {
        if let Some(operation) = &self.operation {
            operation
                .validate()
                .map_err(Error::invalid_transaction_state)?;
        }
        match self.operation.as_ref() {
            Some(Operation::CreateTable | Operation::ReplaceTable) => {
                return Err(Error::invalid_transaction_state(
                    "CREATE TABLE and REPLACE TABLE cannot use an existing-table transaction",
                ));
            }
            Some(Operation::AlterTable) => {
                if self.schema_changes.is_empty() {
                    return Err(Error::invalid_transaction_state(
                        "ALTER TABLE requires at least one schema change",
                    ));
                }
                if self.is_blind_append {
                    return Err(Error::invalid_transaction_state(
                        "ALTER TABLE cannot be marked as a blind append",
                    ));
                }
                if self.config.data_change {
                    return Err(Error::invalid_transaction_state(
                        "ALTER TABLE cannot be marked as a data-changing transaction",
                    ));
                }
            }
            _ => {}
        }
        if self.is_blind_append && !self.config.data_change {
            return Err(Error::invalid_transaction_state(
                "blind append requires data_change to be true",
            ));
        }
        if self.is_blind_append && !self.schema_changes.is_empty() {
            return Err(Error::invalid_transaction_state(
                "blind append cannot include schema changes",
            ));
        }
        if !self.schema_changes.is_empty()
            && self
                .snapshot
                .table_configuration()
                .is_feature_enabled(&TableFeature::IcebergCompatV3)
        {
            return Err(Error::unsupported(
                "ALTER TABLE is not yet supported on tables with icebergCompatV3 enabled",
            ));
        }
        if !self.schema_changes.is_empty()
            && self
                .snapshot
                .table_configuration()
                .is_feature_enabled(&TableFeature::AllowColumnDefaults)
        {
            return Err(Error::unsupported(
                "ALTER TABLE is not yet supported on tables with allowColumnDefaults enabled",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::transaction::builder::ExistingTableTransactionBuilder;
    use crate::transaction::{Operation, TransactionOptions};
    use crate::unit_test_utils::load_test_table;
    use crate::DeltaResult;

    #[test]
    fn build_freezes_configured_commit_intent() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = ExistingTableTransactionBuilder::new(snapshot)
            .with_operation(crate::transaction::Operation::Write)
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
        let error = ExistingTableTransactionBuilder::new(snapshot)
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
        let error = ExistingTableTransactionBuilder::new(snapshot)
            .with_blind_append()
            .with_data_change(false)
            .build(engine.as_ref())
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("blind append requires data_change to be true"));
        Ok(())
    }

    #[rstest]
    #[case(Operation::CreateTable, "cannot use an existing-table transaction")]
    #[case(Operation::ReplaceTable, "cannot use an existing-table transaction")]
    #[case(Operation::AlterTable, "requires at least one schema change")]
    fn existing_table_builder_rejects_ddl_operations(
        #[case] operation: Operation,
        #[case] expected: &str,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = ExistingTableTransactionBuilder::new(snapshot)
            .with_operation(operation)
            .build(engine.as_ref())
            .unwrap_err();

        assert!(error.to_string().contains(expected));
        Ok(())
    }

    #[test]
    fn repeated_with_options_replaces_the_complete_options_value() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = ExistingTableTransactionBuilder::new(snapshot)
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
