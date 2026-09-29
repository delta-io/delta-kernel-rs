//! Builder for transactions against an existing table.

use std::collections::HashSet;

use delta_kernel_derive::internal_api;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::{SchemaRef, StructField};
use crate::snapshot::SnapshotRef;
use crate::transaction::builder::TransactionBuilderState;
use crate::transaction::schema_evolution::SchemaOperation;
use crate::transaction::{Transaction, UpdateTableOperation};
use crate::{DeltaResult, Engine, EngineData, KernelError};

/// Configures a transaction against an existing table.
///
/// The builder supports both data-changing operations and schema changes. Calling
/// [`build`](Self::build) validates the accumulated intent and constructs the transaction.
pub struct UpdateTableTransactionBuilder {
    snapshot: SnapshotRef,
    state: TransactionBuilderState,
    operation: Option<UpdateTableOperation>,
    transaction_ids: Vec<(String, i64)>,
    schema_changes: Vec<SchemaOperation>,
    domain_metadata_removals: Vec<String>,
    is_blind_append: bool,
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
            state: TransactionBuilderState::new(),
            operation: None,
            transaction_ids: Vec::new(),
            schema_changes: Vec::new(),
            domain_metadata_removals: Vec::new(),
            is_blind_append: false,
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
            state,
            operation,
            transaction_ids,
            schema_changes,
            domain_metadata_removals,
            is_blind_append,
        } = self;

        let mut transaction = Transaction::try_new_existing_table(snapshot, committer, engine)?
            .with_builder_state(state)?
            .with_transaction_ids(transaction_ids);

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
        transaction.validate_domain_metadata_operations()?;
        Ok(transaction)
    }

    /// Sets the connector name and version recorded in `commitInfo`.
    ///
    /// Consecutive calls replace the previous value.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.state = self.state.with_engine_info(engine_info);
        self
    }

    /// Attaches an opaque identifier to the transaction's metric events.
    ///
    /// Consecutive calls replace the previous value. An empty identifier is treated as unset.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<std::sync::Arc<str>>) -> Self {
        self.state = self.state.with_correlation_id(correlation_id);
        self
    }

    /// Replaces the operation parameters recorded in `commitInfo`.
    ///
    /// This map replaces rather than merges with a map from an earlier call. Dedicated operation
    /// parameters take precedence over a same-named nested field supplied by
    /// [`with_commit_info`](Self::with_commit_info).
    ///
    /// # Errors
    ///
    /// Returns an error when a key is empty or occurs more than once.
    pub fn with_operation_parameters<I, K, V>(mut self, parameters: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state = self.state.with_operation_parameters(parameters)?;
        Ok(self)
    }

    /// Replaces the operation metrics recorded in `commitInfo`.
    ///
    /// This map replaces rather than merges with a map from an earlier call. Metrics supplied to
    /// the built [`Transaction`] replace these values. Dedicated operation metrics take
    /// precedence over a same-named nested field supplied by
    /// [`with_commit_info`](Self::with_commit_info).
    ///
    /// # Errors
    ///
    /// Returns an error when a key is empty or occurs more than once.
    pub fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state = self.state.with_operation_metrics(metrics)?;
        Ok(self)
    }

    /// Supplies one arbitrary connector-provided `commitInfo` row.
    ///
    /// Consecutive calls replace the previous row. Kernel-owned fields and dedicated operation
    /// parameter or metric maps take precedence over same-named fields in this row.
    pub fn with_commit_info(
        mut self,
        commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.state = self.state.with_commit_info(commit_info, commit_info_schema);
        self
    }

    /// Adds an application transaction identifier to emit as a `txn` action.
    ///
    /// An application id may occur only once; duplicate ids are rejected by [`build`](Self::build).
    pub fn with_transaction_id(mut self, app_id: impl Into<String>, version: i64) -> Self {
        self.transaction_ids.push((app_id.into(), version));
        self
    }

    /// Adds user-controlled domain metadata.
    ///
    /// Each domain may occur only once across additions and removals. Conflicts are rejected by
    /// [`build`](Self::build).
    pub fn with_domain_metadata(
        mut self,
        domain: impl Into<String>,
        configuration: impl Into<String>,
    ) -> Self {
        self.state = self.state.with_domain_metadata(domain, configuration);
        self
    }

    /// Sets whether file actions represent a logical data change.
    ///
    /// If not set, `ALTER TABLE` commits infer the value after file actions are staged:
    /// metadata-only commits use `false`, while commits containing file actions use `true`.
    /// Other operations default to `true`. Set this explicitly to `false` for a protocol-valid
    /// logical-preserving rewrite.
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.state.data_change = Some(data_change);
        self
    }

    /// Marks the transaction as a blind append assertion.
    ///
    /// Blind append is invalid for `ALTER TABLE` and for transactions that remove files.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
        self
    }

    /// Sets the operation recorded in `commitInfo`.
    ///
    /// Consecutive calls replace the previous operation. Invalid custom names and incompatible
    /// operation/schema-change combinations are rejected by [`build`](Self::build).
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
    ///
    /// Schema changes are applied in call order and validated by [`build`](Self::build).
    pub fn add_column(mut self, field: StructField) -> Self {
        self.schema_changes
            .push(SchemaOperation::add_column(None, field));
        self
    }

    /// Adds a nullable field under `parent` in the table schema.
    ///
    /// `parent` may identify a nested struct. Schema changes are applied in call order.
    pub fn add_column_at(mut self, parent: ColumnName, field: StructField) -> Self {
        self.schema_changes
            .push(SchemaOperation::add_column(parent, field));
        self
    }

    /// Changes a possibly nested column from non-nullable to nullable.
    ///
    /// The column path is resolved after all preceding schema changes.
    pub fn set_nullable(mut self, column: ColumnName) -> Self {
        self.schema_changes
            .push(SchemaOperation::SetNullable { column });
        self
    }

    /// Adds a user-controlled domain metadata tombstone to the transaction.
    ///
    /// Each domain may occur only once across additions and removals. Conflicts are rejected by
    /// [`build`](Self::build).
    pub fn with_domain_metadata_removed(mut self, domain: impl Into<String>) -> Self {
        self.domain_metadata_removals.push(domain.into());
        self
    }

    fn validate(&self) -> DeltaResult<()> {
        if let Some(operation) = &self.operation {
            operation
                .validate()
                .map_err(KernelError::invalid_transaction_state)?;
        }
        if self.operation.as_ref() == Some(&UpdateTableOperation::AlterTable) {
            if self.schema_changes.is_empty() {
                return Err(KernelError::invalid_transaction_state(
                    "ALTER TABLE requires at least one schema change",
                ));
            }
            if self.is_blind_append {
                return Err(KernelError::invalid_transaction_state(
                    "ALTER TABLE cannot be marked as a blind append",
                ));
            }
        }
        if self.is_blind_append && self.state.data_change == Some(false) {
            return Err(KernelError::invalid_transaction_state(
                "blind append requires data_change to be true",
            ));
        }
        if self.is_blind_append && !self.schema_changes.is_empty() {
            return Err(KernelError::invalid_transaction_state(
                "blind append cannot include schema changes",
            ));
        }

        let mut removals = HashSet::with_capacity(self.domain_metadata_removals.len());
        if let Some(domain) = self
            .domain_metadata_removals
            .iter()
            .find(|domain| !removals.insert(domain.as_str()))
        {
            return Err(KernelError::invalid_transaction_state(format!(
                "domain metadata '{domain}' is removed more than once"
            )));
        }
        if let Some(domain) = self
            .state
            .domain_metadata_additions
            .iter()
            .map(|metadata| metadata.domain())
            .find(|domain| removals.contains(domain))
        {
            return Err(KernelError::invalid_transaction_state(format!(
                "domain metadata '{domain}' cannot be added and removed in one transaction"
            )));
        }
        let mut app_ids = HashSet::with_capacity(self.transaction_ids.len());
        if let Some((app_id, _)) = self
            .transaction_ids
            .iter()
            .find(|(app_id, _)| !app_ids.insert(app_id.as_str()))
        {
            return Err(KernelError::invalid_transaction_state(format!(
                "app_id {app_id} appears more than once"
            )));
        }
        self.state.validate()?;
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
    use crate::transaction::builder::TransactionBuilderState;
    use crate::transaction::{SchemaOperation, UpdateTableOperation};
    use crate::unit_test_utils::load_test_table;
    use crate::{DeltaResult, KernelError};

    #[test]
    fn builder_collects_existing_table_transaction_intent() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_engine_info("test-engine")
            .with_operation_parameters([("mode", "Append")])?
            .with_operation_metrics([("numFiles", "3")])?
            .with_transaction_id("app", 7)
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
    fn replacement_setters_use_the_last_value_and_empty_correlation_clears() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_operation(UpdateTableOperation::Delete)
            .with_engine_info("first-engine")
            .with_engine_info("second-engine")
            .with_correlation_id("first-correlation")
            .with_correlation_id("")
            .with_operation_parameters([("first", "value")])?
            .with_operation_parameters([("second", "value")])?
            .with_operation_metrics([("first", "value")])?
            .with_operation_metrics([("second", "value")])?
            .with_data_change(false)
            .with_data_change(true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            Some(UpdateTableOperation::Delete.into())
        );
        assert_eq!(transaction.engine_info.as_deref(), Some("second-engine"));
        assert!(transaction.correlation_id.is_none());
        assert_eq!(transaction.operation_parameters.len(), 1);
        assert_eq!(transaction.operation_parameters["second"], "value");
        assert_eq!(transaction.operation_metrics.len(), 1);
        assert_eq!(transaction.operation_metrics["second"], "value");
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
        let mut transaction = snapshot
            .transaction_builder()
            .with_engine_info("test-engine")
            .with_commit_info(commit_info, commit_info_schema)
            .with_correlation_id("correlation-id")
            .with_data_change(false)
            .with_schema_changes([SchemaOperation::add_column(
                None,
                StructField::nullable("new_column", DataType::STRING),
            )])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        transaction.ack_column_defaults();
        transaction.ack_row_tracking_preservation();
        transaction = transaction.with_row_tracking_high_water_mark(17)?;

        assert_eq!(
            transaction.correlation_id.as_deref(),
            Some("correlation-id")
        );
        assert!(!transaction.data_change);
        assert!(transaction.column_defaults_acknowledged);
        assert!(transaction.row_tracking_preservation_acknowledged);
        assert_eq!(transaction.provided_row_tracking_high_water_mark, Some(17));
        assert!(transaction.engine_commit_info.is_some());
        assert!(transaction.user_domain_removals.is_empty());
        Ok(())
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn builder_collects_nested_schema_intent() -> DeltaResult<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let builder = snapshot.transaction_builder().add_column_at(
            ColumnName::new(["parent"]),
            StructField::nullable("child", DataType::STRING),
        );

        assert_eq!(builder.schema_changes.len(), 1);
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
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    enum StagedFileAction {
        Add,
        Remove,
        DeletionVectorUpdate,
    }

    #[rstest]
    fn alter_table_infers_data_change_after_actions_are_staged(
        #[values(
            StagedFileAction::Add,
            StagedFileAction::Remove,
            StagedFileAction::DeletionVectorUpdate
        )]
        action: StagedFileAction,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::AlterTable)
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert!(!transaction.data_change);

        let data = || {
            Box::new(ArrowEngineData::new(RecordBatch::new_empty(Arc::new(
                ArrowSchema::empty(),
            ))))
        };
        match action {
            StagedFileAction::Add => transaction.add_files_metadata.push(data()),
            StagedFileAction::Remove => transaction
                .remove_files_metadata
                .push(crate::FilteredEngineData::with_all_rows_selected(data())),
            StagedFileAction::DeletionVectorUpdate => transaction
                .dv_matched_files
                .push(crate::FilteredEngineData::with_all_rows_selected(data())),
        }
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
    #[case::duplicate_app_id(0, "app_id app appears more than once")]
    #[case::duplicate_domain(1, "domain metadata 'domain' appears more than once")]
    #[case::added_and_removed_domain(2, "cannot be added and removed")]
    fn duplicate_builder_values_are_rejected(
        #[case] kind: u8,
        #[case] expected: &str,
    ) -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let builder = snapshot.transaction_builder();
        let builder = match kind {
            0 => builder
                .with_transaction_id("app", 1)
                .with_transaction_id("app", 2),
            1 => builder
                .with_domain_metadata("domain", "first")
                .with_domain_metadata("domain", "second"),
            _ => builder
                .with_domain_metadata("domain", "value")
                .with_domain_metadata_removed("domain"),
        };

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

    #[test]
    fn domain_metadata_without_table_feature_is_rejected_during_build() -> DeltaResult<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction_builder()
            .with_domain_metadata("app.config", "{}")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("domainMetadata"), "{error}");
        Ok(())
    }

    #[rstest]
    #[case::empty_parameter(
        TransactionBuilderState::new().with_operation_parameters([("", "value")]),
        "parameter key cannot be empty"
    )]
    #[case::duplicate_parameter(
        TransactionBuilderState::new().with_operation_parameters([("key", "first"), ("key", "second")]),
        "parameter key 'key' appears more than once"
    )]
    #[case::empty_metric(
        TransactionBuilderState::new().with_operation_metrics([("", "value")]),
        "metric key cannot be empty"
    )]
    #[case::duplicate_metric(
        TransactionBuilderState::new().with_operation_metrics([("key", "first"), ("key", "second")]),
        "metric key 'key' appears more than once"
    )]
    fn invalid_operation_metadata_is_rejected(
        #[case] result: DeltaResult<TransactionBuilderState>,
        #[case] expected: &str,
    ) {
        let error = result.unwrap_err();
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
    }
}
