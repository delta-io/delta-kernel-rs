//! Builder for transactions against an existing table.

use delta_kernel_derive::internal_api;

use super::state::TransactionBuilderState;
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::metrics::MetricId;
use crate::schema::{SchemaRef, StructField};
use crate::snapshot::SnapshotRef;
use crate::table_features::{
    validate_iceberg_compat_if_needed, IcebergCompatValidationContext, Operation, V2_VALIDATOR,
    V3_VALIDATOR,
};
use crate::transaction::domain_metadata::validate_unique_domains;
use crate::transaction::schema_evolution::SchemaOperation;
use crate::transaction::{Transaction, UpdateTableOperation};
use crate::utils::{current_time_ms, PhantomType};
use crate::{Engine, EngineData, KernelError, KernelResult, Result};

/// Configures DML and schema-changing DDL transactions against an existing table.
///
/// This builder does not create or replace tables. Calling [`build`](Self::build) validates the
/// accumulated intent and constructs the transaction.
pub struct UpdateTableTransactionBuilder {
    snapshot: SnapshotRef,
    state: TransactionBuilderState,
    operation: Option<UpdateTableOperation>,
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
            state: TransactionBuilderState::for_update_table(),
            operation: None,
            schema_changes: Vec::new(),
            domain_metadata_removals: Vec::new(),
            is_blind_append: false,
        }
    }

    /// Sets the operation recorded in `commitInfo`.
    ///
    /// Consecutive calls replace the previous operation. Invalid custom names and incompatible
    /// operation/schema-change combinations are rejected by [`build`](Self::build).
    pub fn with_operation(mut self, operation: UpdateTableOperation) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Sets whether file actions represent a logical data change.
    ///
    /// `true` indicates that the commit changes the table's logical contents. Use `false` for
    /// metadata-only commits or rewrites that reorganize data without changing its contents.
    /// Connector-supplied values are preserved through commit; Kernel does not compare file
    /// contents to verify them.
    ///
    /// If not set, transactions with schema changes infer the value at commit, after file staging:
    /// `false` when no Add, Remove, or DV-update batches are staged and `true` otherwise, including
    /// empty or unselected batches. Transactions without schema changes default to `true`. Set
    /// this explicitly to `false` for a protocol-valid logical-preserving rewrite such as OPTIMIZE.
    pub fn with_data_change(mut self, data_change: bool) -> Self {
        self.state.data_change = Some(data_change);
        self
    }

    /// Marks the transaction as a blind append assertion.
    ///
    /// Blind appends add new files without depending on existing table state. Commit validation
    /// requires staged Add metadata and rejects staged Remove metadata or deletion-vector batches.
    /// Blind append is also invalid for `ALTER TABLE`, schema changes, or `dataChange = false`.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
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
    /// The field must not already exist in the schema, using a case-insensitive comparison, and
    /// must be nullable because existing data files do not contain it. On column-mapping tables,
    /// Kernel assigns or preserves column-mapping IDs and physical names.
    ///
    /// Schema changes are applied in call order and validated by [`build`](Self::build).
    pub fn add_column(mut self, field: StructField) -> Self {
        self.schema_changes
            .push(SchemaOperation::add_column(None, field));
        self
    }

    /// Adds a nullable field under `parent` in the table schema.
    ///
    /// An empty `parent` targets the root schema. Path segments may traverse nested structs, array
    /// elements, map keys, and map values, but the resolved parent must be a struct. The field must
    /// be nullable, must not be a metadata column, and must not collide case-insensitively with a
    /// sibling. On column-mapping tables, Kernel assigns or preserves column-mapping IDs and
    /// physical names.
    ///
    /// Schema changes are applied in call order and validated by [`build`](Self::build).
    pub fn add_column_at(mut self, parent: ColumnName, field: StructField) -> Self {
        self.schema_changes
            .push(SchemaOperation::add_column(parent, field));
        self
    }

    /// Changes a possibly nested column from non-nullable to nullable.
    ///
    /// The column path is resolved after all preceding schema changes. An already-nullable column
    /// is unchanged, but the transaction still emits its metadata action.
    pub fn set_nullable(mut self, column: ColumnName) -> Self {
        self.schema_changes
            .push(SchemaOperation::SetNullable { column });
        self
    }

    /// Adds an application transaction identifier to emit as a `txn` action.
    ///
    /// The action's `lastUpdated` value uses the transaction's commit timestamp.
    /// An application id may occur only once; duplicate ids are rejected by [`build`](Self::build).
    pub fn with_transaction_id(mut self, app_id: impl Into<String>, version: i64) -> Self {
        self.state = self.state.with_transaction_id(app_id, version);
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

    /// Adds a user-controlled domain metadata removal to the transaction.
    ///
    /// If the domain exists, commit emits a tombstone that preserves its previous configuration.
    /// Removing a domain that does not exist is a no-op.
    ///
    /// Each domain may occur only once across additions and removals. Conflicts are rejected by
    /// [`build`](Self::build).
    pub fn with_domain_metadata_removed(mut self, domain: impl Into<String>) -> Self {
        self.domain_metadata_removals.push(domain.into());
        self
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
    /// Common parameters include the write `mode`, `partitionBy` columns, and predicates used by
    /// update or delete operations. Values must already be stringified; `None` writes a null map
    /// value. This map replaces rather than merges with an earlier map, and the last value wins
    /// when a key occurs more than once.
    ///
    /// Dedicated parameters take precedence over a same-named nested field supplied by
    /// [`with_commit_info`](Self::with_commit_info).
    pub fn with_operation_parameters<I, K, V>(mut self, parameters: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state = self.state.with_operation_parameters(parameters);
        self
    }

    /// Replaces the operation metrics recorded in `commitInfo`.
    ///
    /// Common metrics include `numFiles`, `numOutputRows`, `numOutputBytes`, and
    /// `executionTimeMs`. Values must already be stringified; `None` writes a null map value. This
    /// map replaces rather than merges with an earlier map, and the last value wins when a key
    /// occurs more than once.
    ///
    /// Metrics supplied to the built [`Transaction`] replace these values. Dedicated operation
    /// metrics take precedence over a same-named nested field supplied by
    /// [`with_commit_info`](Self::with_commit_info).
    pub fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state = self.state.with_operation_metrics(metrics);
        self
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

    /// Validates the accumulated intent and builds a transaction.
    ///
    /// # Parameters
    ///
    /// - `engine`: Provides table-state reads needed during validation.
    /// - `committer`: Executes the eventual atomic commit.
    ///
    /// For clustered tables, building performs log replay to load clustering columns from domain
    /// metadata and may therefore incur additional I/O.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is not writable; operation or schema intent is invalid;
    /// schema evolution or CDF validation fails; application ids or domain metadata conflict;
    /// a domain is reserved or unsupported; connector commit information does not contain exactly
    /// one row; or blind append is incompatible with the configured transaction intent.
    pub fn build(self, engine: &dyn Engine, committer: Box<dyn Committer>) -> Result<Transaction> {
        self.validate()?;
        let Self {
            snapshot,
            state,
            operation,
            schema_changes,
            domain_metadata_removals,
            is_blind_append,
        } = self;

        let mut transaction = try_new_existing_table(snapshot, committer, engine, state)?;

        // TODO(#3149): Construct the transaction from complete validated builder intent.
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

    fn validate(&self) -> Result<()> {
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

        let removals = validate_unique_domains(
            self.domain_metadata_removals.iter().map(String::as_str),
            |domain| {
                KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' is removed more than once"
                ))
            },
        )?;
        for metadata in &self.state.domain_metadata_additions {
            let domain = metadata.domain();
            if removals.contains(domain) {
                return Err(KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' cannot be added and removed in one transaction"
                )));
            }
        }
        // Duplicate additions are validated by the shared builder state.
        self.state.validate()?;
        Ok(())
    }
}

fn try_new_existing_table(
    snapshot: impl Into<SnapshotRef>,
    committer: Box<dyn Committer>,
    engine: &dyn Engine,
    state: TransactionBuilderState,
) -> KernelResult<Transaction> {
    let read_snapshot = snapshot.into();

    // important! before writing to the table we must check it is supported
    read_snapshot
        .table_configuration()
        .ensure_operation_supported(Operation::Write)?;

    // TODO(#3240): Validate that delta.enableRowTracking=true has the required protocol support
    // and materialized column-name properties. Materialized names must be distinct and must not
    // collide with physical data columns.

    // Read clustering columns from snapshot (returns None if clustering not enabled)
    let clustering_columns = read_snapshot.get_physical_clustering_columns(engine)?;

    let commit_timestamp = current_time_ms()?;

    let span = tracing::info_span!(
        "txn",
        path = %read_snapshot.table_root(),
        read_version = read_snapshot.version(),
    );

    let effective_table_config = read_snapshot.table_configuration().clone();

    validate_iceberg_compat_if_needed(
        &effective_table_config,
        &V2_VALIDATOR,
        IcebergCompatValidationContext::Write,
    )?;

    validate_iceberg_compat_if_needed(
        &effective_table_config,
        &V3_VALIDATOR,
        IcebergCompatValidationContext::Write,
    )?;

    Ok(state.apply_to_transaction(Transaction {
        span,
        operation_id: MetricId::new(),
        correlation_id: None,
        read_snapshot_opt: Some(read_snapshot),
        effective_table_config,
        should_emit_protocol: false,
        should_emit_metadata: false,
        committer,
        operation: None,
        operation_parameters: None,
        operation_metrics: None,
        engine_info: None,
        add_files_metadata: vec![],
        remove_files_metadata: vec![],
        set_transactions: vec![],
        commit_timestamp,
        user_domain_metadata_additions: vec![],
        system_domain_metadata_additions: vec![],
        provided_row_tracking_high_water_mark: None,
        user_domain_removals: vec![],
        data_change: true,
        infer_data_change: false,
        column_defaults_acknowledged: false,
        row_tracking_preservation_acknowledged: false,
        engine_commit_info: None,
        is_blind_append: false,
        dv_matched_files: vec![],
        num_dv_updates: 0,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        manifest_write: None,
        physical_clustering_columns: clustering_columns,
        _state: PhantomType::default(),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use crate::arrow::array::{ArrayRef, Int32Array, StringArray};
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::expressions::ColumnName;
    use crate::schema::{schema_ref, DataType, StructField};
    use crate::transaction::{SchemaOperation, UpdateTableOperation};
    use crate::unit_test_utils::load_test_table;
    use crate::{KernelError, Result};

    #[test]
    fn builder_collects_existing_table_transaction_intent() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_engine_info("test-engine")
            .with_operation_parameters([("mode", Some("Append"))])
            .with_operation_metrics([("numFiles", Some("3"))])
            .with_transaction_id("app", 7)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            Some(UpdateTableOperation::Write.into())
        );
        assert_eq!(transaction.engine_info.as_deref(), Some("test-engine"));
        assert_eq!(
            transaction.operation_parameters.as_ref().unwrap()["mode"].as_deref(),
            Some("Append")
        );
        assert_eq!(
            transaction.operation_metrics.as_ref().unwrap()["numFiles"].as_deref(),
            Some("3")
        );
        assert_eq!(transaction.set_transactions[0].app_id, "app");
        assert!(transaction.data_change);
        Ok(())
    }

    #[test]
    fn replacement_setters_use_the_last_value_and_empty_correlation_clears() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Write)
            .with_operation(UpdateTableOperation::Delete)
            .with_engine_info("first-engine")
            .with_engine_info("second-engine")
            .with_correlation_id("first-correlation")
            .with_correlation_id("")
            .with_operation_parameters([("first", Some("value"))])
            .with_operation_parameters([("second", Some("value"))])
            .with_operation_metrics([("first", Some("value"))])
            .with_operation_metrics([("second", Some("value"))])
            .with_data_change(false)
            .with_data_change(true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            Some(UpdateTableOperation::Delete.into())
        );
        assert_eq!(transaction.engine_info.as_deref(), Some("second-engine"));
        assert!(transaction.correlation_id.is_none());
        let parameters = transaction.operation_parameters.as_ref().unwrap();
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters["second"].as_deref(), Some("value"));
        let metrics = transaction.operation_metrics.as_ref().unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics["second"].as_deref(), Some("value"));
        assert!(transaction.data_change);
        Ok(())
    }

    #[test]
    fn builder_debug_summarizes_intent() -> Result<()> {
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
    fn builder_applies_optional_transaction_intent() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let commit_info_schema = schema_ref! { nullable "tag": STRING };
        let arrow_schema: ArrowSchema = commit_info_schema.as_ref().try_into_arrow()?;
        let commit_info = Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(StringArray::from(vec![Some("value")]))],
        )?));
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

    #[rstest]
    #[case(0)]
    #[case(2)]
    fn builder_rejects_non_single_row_commit_info(#[case] row_count: usize) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let commit_info_schema = schema_ref! { nullable "tag": STRING };
        let arrow_schema: ArrowSchema = commit_info_schema.as_ref().try_into_arrow()?;
        let commit_info = Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(StringArray::from(vec![Some("value"); row_count]))],
        )?));

        let error = snapshot
            .transaction_builder()
            .with_commit_info(commit_info, commit_info_schema)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Connector commit info must contain exactly one row"),
            "{error}"
        );
        Ok(())
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn builder_collects_nested_schema_intent() -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let builder = snapshot.transaction_builder().add_column_at(
            ColumnName::new(["parent"]),
            StructField::nullable("child", DataType::STRING),
        );

        assert_eq!(builder.schema_changes.len(), 1);
        Ok(())
    }

    #[test]
    fn alter_table_operation_uses_unified_builder() -> Result<()> {
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
    fn custom_operation_with_schema_change_uses_behavioral_validation() -> Result<()> {
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
    fn custom_operation_rejects_reserved_known_name() -> Result<()> {
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
    fn alter_table_requires_schema_changes() -> Result<()> {
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
    ) -> Result<()> {
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
    fn alter_table_infers_data_change_from_staged_file_batches(
        #[values(
            StagedFileAction::Add,
            StagedFileAction::Remove,
            StagedFileAction::DeletionVectorUpdate
        )]
        action: StagedFileAction,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::AlterTable)
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert!(!transaction.data_change);

        let data = || {
            let values = Arc::new(Int32Array::from_iter_values([1])) as ArrayRef;
            Box::new(ArrowEngineData::new(
                RecordBatch::try_from_iter([("value", values)]).unwrap(),
            ))
        };
        match action {
            StagedFileAction::Add => transaction.add_files_metadata.push(data()),
            StagedFileAction::Remove | StagedFileAction::DeletionVectorUpdate => {
                let data = crate::FilteredEngineData::with_all_rows_selected(data());
                match action {
                    StagedFileAction::Remove => transaction.remove_files_metadata.push(data),
                    StagedFileAction::DeletionVectorUpdate => {
                        transaction.dv_matched_files.push(data)
                    }
                    StagedFileAction::Add => unreachable!(),
                }
            }
        }
        transaction.resolve_data_change();
        assert!(transaction.data_change);
        Ok(())
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn alter_table_preserves_explicit_data_change(#[case] data_change: bool) -> Result<()> {
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

    #[test]
    fn schema_change_data_change_inference_does_not_depend_on_operation_name() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .transaction_builder()
            .with_operation(UpdateTableOperation::Custom("CUSTOM".to_string()))
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        transaction.resolve_data_change();
        assert!(!transaction.data_change);

        let values = Arc::new(Int32Array::from_iter_values([1])) as ArrayRef;
        transaction
            .add_files_metadata
            .push(Box::new(ArrowEngineData::new(RecordBatch::try_from_iter(
                [("value", values)],
            )?)));
        transaction.resolve_data_change();
        assert!(transaction.data_change);
        Ok(())
    }

    #[rstest]
    #[case::duplicate_app_id(&[], &[], &["app", "app"], "app_id app appears more than once")]
    #[case::duplicate_domain(
        &["domain", "domain"], &[], &[], "domain metadata 'domain' appears more than once"
    )]
    #[case::added_and_removed_domain(
        &["domain"], &["domain"], &[], "cannot be added and removed"
    )]
    #[case::duplicate_removals(
        &[], &["domain", "domain"], &[], "removed more than once"
    )]
    #[case::duplicate_removal_precedes_add_remove_conflict(
        &["domain"], &["domain", "domain"], &[], "removed more than once"
    )]
    #[case::add_remove_conflict_precedes_duplicate_addition(
        &["domain", "domain"], &["domain"], &[], "cannot be added and removed"
    )]
    #[case::duplicate_app_id_precedes_duplicate_addition(
        &["domain", "domain"], &[], &["app", "app"], "app_id app appears more than once"
    )]
    fn duplicate_builder_values_are_rejected(
        #[case] additions: &[&str],
        #[case] removals: &[&str],
        #[case] app_ids: &[&str],
        #[case] expected: &str,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut builder = snapshot.transaction_builder();
        for domain in additions {
            builder = builder.with_domain_metadata(*domain, "value");
        }
        for domain in removals {
            builder = builder.with_domain_metadata_removed(*domain);
        }
        for app_id in app_ids {
            builder = builder.with_transaction_id(*app_id, 1);
        }

        let error = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[test]
    fn domain_metadata_without_table_feature_is_rejected_during_build() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .transaction_builder()
            .with_domain_metadata("app.config", "{}")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("domainMetadata"), "{error}");
        Ok(())
    }
}
