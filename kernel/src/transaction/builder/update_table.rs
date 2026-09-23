//! Builder for transactions against an existing table.

use std::collections::HashSet;

use delta_kernel_derive::internal_api;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::transaction::schema_evolution::SchemaOperation;
use crate::transaction::{
    Transaction, TransactionConfig, TransactionOptions, UpdateTableOperation,
};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::FileMeta;
use crate::{DeltaResult, Engine, Error};

/// Configures a transaction against an existing table.
///
/// The builder supports both data-changing operations and schema changes. Calling
/// [`build`](Self::build) validates the accumulated intent and constructs the transaction.
pub struct UpdateTableTransactionBuilder {
    snapshot: SnapshotRef,
    config: TransactionConfig,
    operation: Option<UpdateTableOperation>,
    schema_changes: Vec<SchemaOperation>,
    domain_metadata_removals: Vec<String>,
    row_tracking_high_water_mark: Option<i64>,
    is_blind_append: bool,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    root_manifest_file: Option<FileMeta>,
}

impl std::fmt::Debug for UpdateTableTransactionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateTableTransactionBuilder")
            .field("snapshot_version", &self.snapshot.version())
            .field("operation", &self.operation)
            .field("schema_change_count", &self.schema_changes.len())
            .finish()
    }
}

impl UpdateTableTransactionBuilder {
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        Self {
            snapshot,
            config: TransactionConfig::new(),
            operation: None,
            schema_changes: Vec::new(),
            domain_metadata_removals: Vec::new(),
            row_tracking_high_water_mark: None,
            is_blind_append: false,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            root_manifest_file: None,
        }
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
    /// Returns an error when the table is not writable, schema evolution fails, the evolved
    /// schema uses a CDF-reserved top-level column while CDF is enabled, or the selected operation
    /// is incompatible with the configured capabilities.
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Transaction> {
        self.validate()?;
        let Self {
            snapshot,
            config,
            operation,
            schema_changes,
            domain_metadata_removals,
            row_tracking_high_water_mark,
            is_blind_append,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            root_manifest_file,
        } = self;

        let mut transaction = Transaction::try_new_existing_table(snapshot, committer, engine)?
            .with_transaction_config(config)?;

        if let Some(operation) = operation {
            transaction = transaction.with_update_table_operation(operation);
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
    ///
    /// This replaces values previously set through option-specific builder methods such as
    /// [`Self::with_correlation_id`]. Call those methods after `with_options` to override an
    /// individual option.
    pub fn with_options(mut self, options: TransactionOptions) -> Self {
        self.config.set_options(options);
        self
    }

    /// Attaches an opaque identifier to the transaction's metric events.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<std::sync::Arc<str>>) -> Self {
        self.config.set_correlation_id(correlation_id);
        self
    }

    /// Sets whether file actions represent a logical data change.
    ///
    /// If not set, `ALTER TABLE` commits infer the value after file actions are staged:
    /// metadata-only commits use `false`, while commits containing file actions use `true`.
    /// Other operations default to `true`. Set this explicitly to `false` for a protocol-valid
    /// logical-preserving rewrite.
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.config.set_data_change(data_change);
        self
    }

    /// Acknowledges that the connector applies column defaults before writing.
    pub fn ack_column_defaults(mut self) -> Self {
        self.config.acknowledge_column_defaults();
        self
    }

    /// Marks the transaction as a blind append assertion.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
        self
    }

    /// Sets the operation recorded in `commitInfo`.
    pub fn with_operation(mut self, operation: UpdateTableOperation) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Adds schema changes to validate and apply during [`build`](Self::build).
    ///
    /// Changes are applied in order, and each change observes the result of prior changes.
    #[internal_api]
    pub(crate) fn with_schema_changes(
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
    #[internal_api]
    pub(crate) fn add_column_at(mut self, parent: ColumnName, field: StructField) -> Self {
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
        self.config.acknowledge_row_tracking_preservation();
        self
    }

    /// Sets an explicit row-tracking high-water mark.
    #[internal_api]
    pub(crate) fn with_row_tracking_high_water_mark(mut self, high_water_mark: i64) -> Self {
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
        if self.operation.as_ref() == Some(&UpdateTableOperation::AlterTable) {
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
        }
        if self.is_blind_append && self.config.data_change == Some(false) {
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
            .config
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
    use std::sync::Arc;

    use rstest::rstest;

    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::expressions::ColumnName;
    use crate::schema::{schema_ref, DataType, StructField};
    use crate::transaction::{SchemaOperation, TransactionOptions, UpdateTableOperation};
    use crate::unit_test_utils::load_test_table;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::FileMeta;
    use crate::{DeltaResult, Error};

    #[test]
    fn builder_collects_existing_table_transaction_intent() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_options(
                TransactionOptions::new()
                    .with_engine_info("test-engine")
                    .with_operation_parameters([("mode", "Append")])?
                    .with_operation_metrics([("numFiles", "3")])?
                    .with_transaction_id("app", 7),
            )
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            Some(UpdateTableOperation::Write.into())
        );
        assert_eq!(transaction.engine_info.as_deref(), Some("test-engine"));
        assert_eq!(transaction.operation_parameters["mode"], "Append");
        assert_eq!(transaction.operation_metrics["numFiles"], "3");
        assert_eq!(transaction.set_transactions[0].app_id, "app");
        assert!(transaction.data_change);
        Ok(())
    }

    #[test]
    fn builder_debug_summarizes_intent() -> DeltaResult<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let debug = format!(
            "{:?}",
            snapshot
                .transaction_builder()
                .with_operation(UpdateTableOperation::Write)
                .add_column(StructField::nullable("new_column", DataType::STRING))
        );

        assert!(debug.contains("UpdateTableTransactionBuilder"));
        assert!(debug.contains("operation: Some(Write)"));
        assert!(debug.contains("schema_change_count: 1"));
        Ok(())
    }

    #[test]
    fn builder_applies_optional_transaction_intent() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let commit_info_schema = schema_ref! { nullable "tag": STRING };
        let arrow_schema: ArrowSchema = commit_info_schema.as_ref().try_into_arrow()?;
        let commit_info = Box::new(ArrowEngineData::new(RecordBatch::new_empty(Arc::new(
            arrow_schema,
        ))));
        let options = TransactionOptions::new()
            .with_engine_info("test-engine")
            .with_commit_info(commit_info, commit_info_schema);
        assert!(format!("{options:?}").contains("engine_commit_info: true"));

        let transaction = snapshot
            .transaction_builder()
            .with_options(options)
            .with_correlation_id("correlation-id")
            .with_data_change(false)
            .with_schema_changes([SchemaOperation::add_column(
                None,
                StructField::nullable("new_column", DataType::STRING),
            )])
            .with_domain_metadata_removed("missing-domain")
            .ack_column_defaults()
            .ack_row_tracking_preservation()
            .with_row_tracking_high_water_mark(17)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.correlation_id.as_deref(),
            Some("correlation-id")
        );
        assert!(!transaction.data_change);
        assert!(transaction.column_defaults_acknowledged);
        assert!(transaction.row_tracking_preservation_acknowledged);
        assert_eq!(transaction.provided_row_tracking_high_water_mark, Some(17));
        assert!(transaction.engine_commit_info.is_some());
        assert_eq!(transaction.user_domain_removals.len(), 1);
        Ok(())
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn internal_builder_methods_collect_nested_schema_and_manifest_intent() -> DeltaResult<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let manifest = FileMeta {
            location: snapshot.table_root().join("manifest.json")?,
            last_modified: 0,
            size: 1,
        };
        let builder = snapshot
            .transaction_builder()
            .add_column_at(
                ColumnName::new(["parent"]),
                StructField::nullable("child", DataType::STRING),
            )
            .with_root_manifest_file(manifest);

        assert_eq!(builder.schema_changes.len(), 1);
        assert!(builder.root_manifest_file.is_some());
        Ok(())
    }

    #[test]
    fn alter_table_operation_uses_unified_builder() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::AlterTable)
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            Some(UpdateTableOperation::AlterTable.into())
        );
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
            .with_operation(UpdateTableOperation::Custom(
                "vendor schema write".to_string(),
            ))
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
            .with_operation(UpdateTableOperation::Custom("ALTER TABLE".to_string()))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("reserved"));
        Ok(())
    }

    #[rstest]
    #[case("CREATE TABLE", "cannot use an update-table transaction")]
    #[case("REPLACE TABLE", "reserved")]
    #[case("ALTER TABLE", "requires at least one schema change")]
    fn legacy_transaction_rejects_builder_specific_operations(
        #[case] operation: &str,
        #[case] expected: &str,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation(operation)
            .commit(engine.as_ref())
            .unwrap_err();

        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[test]
    fn alter_table_requires_schema_changes() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::AlterTable)
            .with_data_change(false)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("requires at least one schema change"));
        Ok(())
    }

    #[derive(Debug)]
    enum InvalidIntent {
        AlterTableBlindAppend,
        BlindAppendWithoutDataChange,
        BlindAppendWithSchemaChange,
    }

    #[rstest]
    #[case::alter_blind_append(
        InvalidIntent::AlterTableBlindAppend,
        "cannot be marked as a blind append"
    )]
    #[case::blind_append_without_data_change(
        InvalidIntent::BlindAppendWithoutDataChange,
        "requires data_change to be true"
    )]
    #[case::blind_append_with_schema_change(
        InvalidIntent::BlindAppendWithSchemaChange,
        "cannot include schema changes"
    )]
    fn invalid_intent_is_rejected(
        #[case] intent: InvalidIntent,
        #[case] expected: &str,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let builder = snapshot.transaction_builder();
        let builder = match intent {
            InvalidIntent::AlterTableBlindAppend => builder
                .with_operation(UpdateTableOperation::AlterTable)
                .add_column(StructField::nullable("new_column", DataType::STRING))
                .with_blind_append(),
            InvalidIntent::BlindAppendWithoutDataChange => {
                builder.with_blind_append().with_data_change(false)
            }
            InvalidIntent::BlindAppendWithSchemaChange => builder
                .with_operation(UpdateTableOperation::Write)
                .add_column(StructField::nullable("new_column", DataType::STRING))
                .with_blind_append(),
        };

        let error = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(matches!(&error, Error::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[test]
    fn alter_table_infers_data_change_after_actions_are_staged() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::AlterTable)
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert!(!transaction.data_change);

        let add_schema: ArrowSchema = transaction.add_files_schema().as_ref().try_into_arrow()?;
        transaction.add_files(Box::new(ArrowEngineData::new(RecordBatch::new_empty(
            Arc::new(add_schema),
        ))));
        transaction.resolve_data_change();
        assert!(transaction.data_change);
        Ok(())
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn alter_table_preserves_explicit_data_change(#[case] data_change: bool) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::AlterTable)
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .with_data_change(data_change)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert_eq!(transaction.data_change, data_change);
        Ok(())
    }

    #[rstest]
    #[case::duplicate_app_id(
        TransactionOptions::new()
            .with_transaction_id("app", 1)
            .with_transaction_id("app", 2),
        None,
        "app_id app already exists"
    )]
    #[case::duplicate_domain(
        TransactionOptions::new()
            .with_domain_metadata("domain", "first")
            .with_domain_metadata("domain", "second"),
        None,
        "domain metadata 'domain' appears more than once"
    )]
    #[case::added_and_removed_domain(
        TransactionOptions::new().with_domain_metadata("domain", "value"),
        Some("domain"),
        "cannot be added and removed"
    )]
    fn duplicate_transaction_options_are_rejected(
        #[case] options: TransactionOptions,
        #[case] removed_domain: Option<&str>,
        #[case] expected: &str,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut builder = snapshot.transaction_builder().with_options(options);
        if let Some(domain) = removed_domain {
            builder = builder.with_domain_metadata_removed(domain);
        }

        let error = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[test]
    fn duplicate_domain_metadata_removals_are_rejected() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction_builder()
            .with_domain_metadata_removed("domain")
            .with_domain_metadata_removed("domain")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(
            error.to_string().contains("removed more than once"),
            "{error}"
        );
        Ok(())
    }

    #[rstest]
    #[case::empty_parameter(
        TransactionOptions::new().with_operation_parameters([("", "value")]),
        "parameter key cannot be empty"
    )]
    #[case::duplicate_parameter(
        TransactionOptions::new().with_operation_parameters([("key", "first"), ("key", "second")]),
        "parameter key 'key' appears more than once"
    )]
    #[case::empty_metric(
        TransactionOptions::new().with_operation_metrics([("", "value")]),
        "metric key cannot be empty"
    )]
    #[case::duplicate_metric(
        TransactionOptions::new().with_operation_metrics([("key", "first"), ("key", "second")]),
        "metric key 'key' appears more than once"
    )]
    fn invalid_operation_metadata_is_rejected(
        #[case] result: DeltaResult<TransactionOptions>,
        #[case] expected: &str,
    ) {
        let error = result.unwrap_err();
        assert!(matches!(&error, Error::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
    }
}
