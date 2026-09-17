//! Builder for transactions against an existing table.

use std::collections::HashSet;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::transaction::schema_evolution::SchemaOperation;
use crate::transaction::{Operation, Transaction, TransactionOptions};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::FileMeta;
use crate::{DeltaResult, Engine, Error};

/// Configures a transaction against an existing table.
///
/// The builder supports both data-changing operations and schema changes. Calling
/// [`build`](Self::build) validates the accumulated intent and constructs the transaction.
pub struct ExistingTableTransactionBuilder {
    snapshot: SnapshotRef,
    options: TransactionOptions,
    operation: Option<Operation>,
    schema_changes: Vec<SchemaOperation>,
    domain_metadata_removals: Vec<String>,
    data_change: Option<bool>,
    column_defaults_acknowledged: bool,
    row_tracking_preservation_acknowledged: bool,
    row_tracking_high_water_mark: Option<i64>,
    is_blind_append: bool,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    root_manifest_file: Option<FileMeta>,
}

impl std::fmt::Debug for ExistingTableTransactionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExistingTableTransactionBuilder")
            .field("snapshot_version", &self.snapshot.version())
            .field("operation", &self.operation)
            .field("schema_change_count", &self.schema_changes.len())
            .finish()
    }
}

impl ExistingTableTransactionBuilder {
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        Self {
            snapshot,
            options: TransactionOptions::new(),
            operation: None,
            schema_changes: Vec::new(),
            domain_metadata_removals: Vec::new(),
            data_change: None,
            column_defaults_acknowledged: false,
            row_tracking_preservation_acknowledged: false,
            row_tracking_high_water_mark: None,
            is_blind_append: false,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            root_manifest_file: None,
        }
    }

    pub(crate) fn new_alter_table(snapshot: SnapshotRef) -> Self {
        Self::new(snapshot)
            .with_operation(Operation::AlterTable)
            .with_data_change(false)
    }

    /// Validates the accumulated intent and builds a transaction.
    ///
    /// # Parameters
    ///
    /// - `engine`: Provides table-state reads needed during validation.
    /// - `committer`: Executes the eventual atomic commit.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is not writable, schema evolution fails, or the selected
    /// operation is incompatible with the configured capabilities.
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Transaction> {
        self.validate()?;
        let Self {
            snapshot,
            options,
            operation,
            schema_changes,
            domain_metadata_removals,
            data_change,
            column_defaults_acknowledged,
            row_tracking_preservation_acknowledged,
            row_tracking_high_water_mark,
            is_blind_append,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            root_manifest_file,
        } = self;

        let mut transaction = Transaction::try_new_existing_table(snapshot, committer, engine)?
            .with_transaction_options(options)?
            .with_data_change(data_change.unwrap_or(true));

        if let Some(operation) = operation {
            transaction = transaction.with_operation(operation.to_string());
        }
        if is_blind_append {
            transaction = transaction.with_blind_append();
        }
        if !schema_changes.is_empty() {
            transaction = transaction.with_schema_changes(schema_changes)?;
        }
        for domain in domain_metadata_removals {
            transaction = transaction.with_domain_metadata_removed(domain);
        }
        if column_defaults_acknowledged {
            transaction.ack_column_defaults();
        }
        if row_tracking_preservation_acknowledged {
            transaction.ack_row_tracking_preservation();
        }
        if let Some(high_water_mark) = row_tracking_high_water_mark {
            transaction = transaction.with_row_tracking_high_water_mark(high_water_mark)?;
        }
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some(root_manifest_file) = root_manifest_file {
            transaction = transaction.with_root_manifest_file(root_manifest_file)?;
        }
        Ok(transaction)
    }

    /// Replaces the options shared by transaction variants.
    pub fn with_options(mut self, options: TransactionOptions) -> Self {
        self.options = options;
        self
    }

    /// Attaches an opaque identifier to the transaction's metric events.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<std::sync::Arc<str>>) -> Self {
        self.options = self.options.with_correlation_id(correlation_id);
        self
    }

    /// Sets whether file actions represent a logical data change.
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.data_change = Some(data_change);
        self
    }

    /// Acknowledges that the connector applies column defaults before writing.
    pub fn ack_column_defaults(mut self) -> Self {
        self.column_defaults_acknowledged = true;
        self
    }

    /// Marks the transaction as a blind append assertion.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
        self
    }

    /// Sets the operation recorded in `commitInfo`.
    pub fn with_operation(mut self, operation: Operation) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Adds schema changes to validate and apply during [`build`](Self::build).
    ///
    /// Changes are applied in order, and each change observes the result of prior changes.
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

    /// Adds a user-controlled domain metadata tombstone to the transaction.
    pub fn with_domain_metadata_removed(mut self, domain: impl Into<String>) -> Self {
        self.domain_metadata_removals.push(domain.into());
        self
    }

    /// Acknowledges the connector's row-tracking preservation responsibilities.
    pub fn ack_row_tracking_preservation(mut self) -> Self {
        self.row_tracking_preservation_acknowledged = true;
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
                if self.data_change == Some(true) {
                    return Err(Error::invalid_transaction_state(
                        "ALTER TABLE cannot be marked as a data-changing transaction",
                    ));
                }
            }
            _ => {}
        }
        if self.is_blind_append && self.data_change == Some(false) {
            return Err(Error::invalid_transaction_state(
                "blind append requires data_change to be true",
            ));
        }
        if self.is_blind_append && !self.schema_changes.is_empty() {
            return Err(Error::invalid_transaction_state(
                "blind append cannot include schema changes",
            ));
        }

        let mut removals = HashSet::with_capacity(self.domain_metadata_removals.len());
        if let Some(domain) = self
            .domain_metadata_removals
            .iter()
            .find(|domain| !removals.insert(domain.as_str()))
        {
            return Err(Error::invalid_transaction_state(format!(
                "domain metadata '{domain}' is removed more than once"
            )));
        }
        if let Some(domain) = self
            .options
            .domain_metadata_additions
            .iter()
            .map(|metadata| metadata.domain())
            .find(|domain| removals.contains(domain))
        {
            return Err(Error::invalid_transaction_state(format!(
                "domain metadata '{domain}' cannot be added and removed in one transaction"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::committer::FileSystemCommitter;
    use crate::schema::{DataType, StructField};
    use crate::transaction::{Operation, TransactionOptions};
    use crate::unit_test_utils::load_test_table;
    use crate::DeltaResult;

    #[test]
    fn builder_collects_existing_table_transaction_intent() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(Operation::Write)
            .with_options(
                TransactionOptions::new()
                    .with_engine_info("test-engine")
                    .with_operation_parameters([("mode", "Append")])?
                    .with_operation_metrics([("numFiles", "3")])?
                    .with_transaction_id("app", 7),
            )
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.operation, Some(Operation::Write));
        assert_eq!(transaction.engine_info.as_deref(), Some("test-engine"));
        assert_eq!(transaction.operation_parameters["mode"], "Append");
        assert_eq!(transaction.operation_metrics["numFiles"], "3");
        assert_eq!(transaction.set_transactions[0].app_id, "app");
        Ok(())
    }

    #[test]
    fn alter_table_convenience_uses_unified_builder() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .alter_table()
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.operation, Some(Operation::AlterTable));
        assert!(transaction.should_emit_metadata);
        assert!(transaction
            .effective_table_config
            .logical_schema()
            .contains("new_column"));
        Ok(())
    }

    #[test]
    fn custom_operation_with_schema_change_uses_behavioral_validation() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(Operation::Custom("vendor schema write".to_string()))
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert!(transaction.should_emit_metadata);
        Ok(())
    }

    #[test]
    fn custom_operation_rejects_reserved_known_name() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction_builder()
            .with_operation(Operation::Custom("ALTER TABLE".to_string()))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("reserved"));
        Ok(())
    }

    #[test]
    fn alter_table_requires_schema_changes() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction_builder()
            .with_operation(Operation::AlterTable)
            .with_data_change(false)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("requires at least one schema change"));
        Ok(())
    }
}
