//! Builder for transactions against an existing table.

use delta_kernel_derive::internal_api;

use super::alter_table::Modifying;
use super::state::TransactionBuilderState;
use super::{TransactionBuilder, TransactionType};
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    validate_iceberg_compat_if_needed, IcebergCompatValidationContext, Operation, TableFeature,
    V2_VALIDATOR, V3_VALIDATOR,
};
use crate::transaction::domain_metadata::validate_unique_domains;
use crate::transaction::schema_evolution::{evolve_table_config, SchemaOperation};
use crate::transaction::{
    AlterTable, CommitOperation, ExecutionMode, Transaction, UpdateTable, UpdateTableOperation,
};
use crate::utils::{current_time_ms, require};
use crate::{Engine, KernelError, KernelResult, Result};

/// Configures data writes and schema evolution against an existing table.
///
/// This builder does not create or replace tables. Construct it with
/// [`Snapshot::update_table`](crate::Snapshot::update_table); operation, engine info, and
/// `data_change` cannot be changed afterward.
/// [`build`](UpdateTableTransactionBuilder::build) validates the accumulated intent and constructs
/// the transaction.
///
/// # Schema changes
///
/// Use [`Snapshot::alter_table`](crate::Snapshot::alter_table) for a standalone metadata-only
/// schema change, such as adding a nullable column without writing data files.
///
/// Use **schema evolution during a write** to change the schema while writing or rewriting data
/// files in the same transaction. Select the data operation, such as
/// [`UpdateTableOperation::Write`], and configure the schema changes on the same builder.
///
/// In both workflows, configure schema operations before `build()`, which validates and applies
/// them. For schema evolution during a write, stage data-file changes on the returned
/// [`Transaction`], using the evolved schema provided by [`Transaction::write_state`].
///
/// Set `data_change` when constructing the builder: `true` for logical data changes, or `false`
/// for metadata-only changes and data-preserving rewrites. Blind append requires `true`.
/// [`build`](UpdateTableTransactionBuilder::build) rejects incompatible intent; commit validates
/// the staged file actions.
///
/// cbindgen:ignore
pub type UpdateTableTransactionBuilder = TransactionBuilder<UpdateTable>;

/// Opaque state collected by existing-table transaction builders.
#[doc(hidden)]
pub struct ExistingTableBuilderState {
    pub(super) snapshot: SnapshotRef,
    pub(super) operation: CommitOperation,
    pub(super) schema_changes: Option<Vec<SchemaOperation>>,
    pub(super) domain_metadata_removals: Vec<String>,
    pub(super) is_blind_append: bool,
}

/// A transaction type that accepts schema changes against an existing table.
///
/// Schema setters share their implementation and preserve transaction metadata while selecting
/// the builder state in which a schema operation has been queued.
pub trait SupportsSchemaChanges: TransactionType<BuilderState = ExistingTableBuilderState> {
    /// Transaction type after adding at least one schema operation.
    type WithSchemaChanges: TransactionType<BuilderState = ExistingTableBuilderState>;
}

impl SupportsSchemaChanges for UpdateTable {
    type WithSchemaChanges = UpdateTable;
}

impl<S> SupportsSchemaChanges for AlterTable<S> {
    type WithSchemaChanges = AlterTable<Modifying>;
}

impl std::fmt::Debug for TransactionBuilder<UpdateTable> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateTableTransactionBuilder")
            .field("snapshot_version", &self.local_state.snapshot.version())
            .field("operation", &self.local_state.operation)
            .field(
                "schema_change_count",
                &self.local_state.schema_changes.as_ref().map_or(0, Vec::len),
            )
            .finish()
    }
}

impl TransactionBuilder<UpdateTable> {
    pub(crate) fn new(
        snapshot: SnapshotRef,
        operation: UpdateTableOperation,
        engine_info: String,
        data_change: bool,
    ) -> Self {
        Self {
            common_state: TransactionBuilderState::new(engine_info, data_change),
            local_state: ExistingTableBuilderState {
                snapshot,
                operation: operation.into(),
                schema_changes: None,
                domain_metadata_removals: Vec::new(),
                is_blind_append: false,
            },
        }
    }

    /// Marks the transaction as a blind append assertion.
    ///
    /// Blind appends add new files without depending on existing table state. Commit validation
    /// requires staged Add metadata and rejects staged Remove metadata or deletion-vector batches.
    /// Blind append is also invalid for schema changes or `dataChange = false`.
    pub fn with_blind_append(mut self) -> Self {
        self.local_state.is_blind_append = true;
        self
    }
}

impl<T: SupportsSchemaChanges> TransactionBuilder<T> {
    /// Adds schema changes to validate and apply when building the transaction.
    ///
    /// Appends `changes` in call order and returns the updated builder. Each change observes the
    /// result of prior changes. Empty input is rejected immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if `changes` is empty.
    #[internal_api]
    pub(crate) fn with_schema_changes(
        mut self,
        changes: impl IntoIterator<Item = SchemaOperation>,
    ) -> Result<TransactionBuilder<T::WithSchemaChanges>> {
        let mut changes = changes.into_iter().peekable();
        require!(
            changes.peek().is_some(),
            KernelError::generic("with_schema_changes requires at least one schema operation")
        );
        self.local_state
            .schema_changes
            .get_or_insert_with(Vec::new)
            .extend(changes);
        Ok(self.transition())
    }

    /// Add a new top-level column to the table schema.
    ///
    /// The field must not already exist in the schema (case-insensitive). The field must be
    /// nullable because existing data files do not contain this column and will read NULL for it.
    /// On column-mapping tables, Kernel assigns or preserves column-mapping IDs and physical names
    /// for the added field.
    ///
    /// Schema changes are applied in call order and validated when building the transaction.
    pub fn add_column(mut self, field: StructField) -> TransactionBuilder<T::WithSchemaChanges> {
        self.local_state
            .schema_changes
            .get_or_insert_with(Vec::new)
            .push(SchemaOperation::add_column(None, field));
        self.transition()
    }

    /// Add a new column or nested field to the table schema.
    ///
    /// `parent` identifies the struct that will contain `field`. An empty parent targets the
    /// table's root schema; segments may traverse nested structs, array elements, map keys, and
    /// map values.
    ///
    /// The added field must be nullable (existing data files lack the column and will read NULL),
    /// must not be a metadata column and must not collide case-insensitively with a
    /// sibling in the target struct. `parent` must resolve to a struct.
    ///
    /// With column mapping enabled, existing IDs and physical names are preserved and missing
    /// annotations are assigned.
    ///
    /// Schema changes are applied in call order and validated when building the transaction.
    pub fn add_column_at(
        mut self,
        parent: ColumnName,
        field: StructField,
    ) -> TransactionBuilder<T::WithSchemaChanges> {
        self.local_state
            .schema_changes
            .get_or_insert_with(Vec::new)
            .push(SchemaOperation::add_column(parent, field));
        self.transition()
    }

    /// Change a column's nullability from NOT NULL to nullable. If the column is already
    /// nullable, the op is a no-op but still generates a commit.
    ///
    /// Note: this matches Spark's behavior.
    pub fn set_nullable(mut self, column: ColumnName) -> TransactionBuilder<T::WithSchemaChanges> {
        self.local_state
            .schema_changes
            .get_or_insert_with(Vec::new)
            .push(SchemaOperation::SetNullable { column });
        self.transition()
    }

    // Reconstructs the builder with a different transaction type, changing which methods
    // are available at compile time (e.g. Ready -> Modifying enables `build()`). All real
    // fields are moved as-is; only the zero-sized type state changes.
    fn transition(self) -> TransactionBuilder<T::WithSchemaChanges> {
        TransactionBuilder {
            common_state: self.common_state,
            local_state: self.local_state,
        }
    }
}

impl TransactionBuilder<UpdateTable> {
    /// Adds a user-controlled domain metadata removal to the transaction.
    ///
    /// If the domain exists, commit emits a tombstone that preserves its previous configuration.
    /// Removing a domain that does not exist is a no-op.
    ///
    /// Each domain may occur only once across additions and removals. Conflicts are rejected by
    /// [`build`](Self::build).
    pub fn with_domain_metadata_removed(mut self, domain: impl Into<String>) -> Self {
        self.local_state
            .domain_metadata_removals
            .push(domain.into());
        self
    }

    /// Validates the accumulated intent and builds a transaction.
    ///
    /// The caller-supplied `data_change` is preserved, including when no files are staged.
    /// Blind append requires `true`. Commit validates file actions staged after building
    /// without inferring or overriding the configured value.
    ///
    /// # Parameters
    ///
    /// - `engine`: Provides table-state reads needed during validation.
    /// - `committer`: Executes the eventual atomic commit.
    ///
    /// Note: For tables with clustering enabled, this performs log replay to read clustering
    /// columns from domain metadata, which may have a performance cost.
    ///
    /// # Errors
    ///
    /// Returns an error for:
    /// - Invalid operation intent, including incompatible blind-append or schema intent.
    /// - Unsupported table writes, schema evolution, or CDF validation failures.
    /// - Conflicting application ids or domain metadata, or reserved or unsupported domains.
    /// - Connector commit information that does not contain exactly one row.
    ///
    /// Schema changes are rejected when `icebergCompatV2`, `icebergCompatV3`, or
    /// `allowColumnDefaults` is enabled. This applies to standalone ALTER TABLE transactions and
    /// schema evolution during writes.
    pub fn build(self, engine: &dyn Engine, committer: Box<dyn Committer>) -> Result<Transaction> {
        self.validate()?;
        let mut transaction = try_new_existing_table(self, committer)?;
        transaction.physical_clustering_columns = transaction
            .read_snapshot()?
            .get_physical_clustering_columns(engine)?;
        transaction.validate_domain_metadata_operations()?;
        Ok(transaction)
    }

    fn validate(&self) -> Result<()> {
        let operation = &self.local_state.operation;
        operation
            .validate()
            .map_err(KernelError::invalid_transaction_state)?;
        if self.local_state.is_blind_append && !self.common_state.data_change {
            return Err(KernelError::invalid_transaction_state(
                "blind append requires data_change to be true",
            ));
        }
        if self.local_state.is_blind_append && self.local_state.schema_changes.is_some() {
            return Err(KernelError::invalid_transaction_state(
                "blind append cannot include schema changes",
            ));
        }

        let removals = validate_unique_domains(
            self.local_state
                .domain_metadata_removals
                .iter()
                .map(String::as_str),
            |domain| {
                KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' is removed more than once"
                ))
            },
        )?;
        for metadata in &self.common_state.domain_metadata_additions {
            let domain = metadata.domain();
            if removals.contains(domain) {
                return Err(KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' cannot be added and removed in one transaction"
                )));
            }
        }
        // Duplicate additions are validated by the shared builder state.
        self.common_state.validate()?;
        Ok(())
    }
}

pub(super) fn try_new_existing_table<
    STATE: TransactionType<BuilderState = ExistingTableBuilderState>,
    MODE: ExecutionMode,
>(
    builder: TransactionBuilder<STATE>,
    committer: Box<dyn Committer>,
) -> KernelResult<Transaction<STATE, MODE>> {
    let TransactionBuilder {
        common_state,
        local_state,
    } = builder;
    let ExistingTableBuilderState {
        snapshot: read_snapshot,
        operation,
        schema_changes,
        domain_metadata_removals,
        is_blind_append,
    } = local_state;

    // important! before writing to the table we must check it is supported
    read_snapshot
        .table_configuration()
        .ensure_operation_supported(Operation::Write)?;

    // TODO(#3240): Validate that delta.enableRowTracking=true has the required protocol support
    // and materialized column-name properties. Materialized names must be distinct and must not
    // collide with physical data columns.

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

    let should_emit_metadata = schema_changes.is_some();
    let effective_table_config = match schema_changes {
        Some(changes) => apply_schema_changes(effective_table_config, changes)?,
        None => effective_table_config,
    };

    Ok(Transaction {
        read_snapshot_opt: Some(read_snapshot),
        should_emit_metadata,
        user_domain_removals: domain_metadata_removals,
        is_blind_append,
        ..Transaction::new(
            common_state,
            effective_table_config,
            committer,
            operation,
            span,
            commit_timestamp,
        )
    })
}

fn apply_schema_changes(
    table_config: TableConfiguration,
    changes: Vec<SchemaOperation>,
) -> KernelResult<TableConfiguration> {
    // kernel doesn't currently support altering tables with these features
    let unsupported_iceberg_compat = [TableFeature::IcebergCompatV2, TableFeature::IcebergCompatV3]
        .into_iter()
        .find(|feature| table_config.is_feature_enabled(feature));
    if let Some(feature) = unsupported_iceberg_compat {
        return Err(KernelError::unsupported(format!(
            "Schema changes are not yet supported on tables with {feature} enabled"
        )));
    }
    // TODO(#2630): Support ALTER TABLE on tables with column defaults.
    if table_config.is_feature_enabled(&TableFeature::AllowColumnDefaults) {
        return Err(KernelError::unsupported(
            "Schema changes are not yet supported on tables with allowColumnDefaults enabled",
        ));
    }
    evolve_table_config(&table_config, changes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use rstest_reuse::{apply, template};
    use tempfile::TempDir;

    use super::{TransactionBuilder, TransactionType};
    use crate::arrow::array::{ArrayRef, Int32Array, StringArray};
    use crate::arrow::datatypes::Schema as ArrowSchema;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::column_name;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::expressions::ColumnName;
    use crate::object_store::memory::InMemory;
    use crate::schema::{schema_ref, DataType, StructField};
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::table_properties::{ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3};
    use crate::transaction::create_table::create_table;
    use crate::transaction::data_layout::DataLayout;
    use crate::transaction::{SchemaOperation, UpdateTableOperation};
    use crate::unit_test_utils::{load_test_table, test_schema_flat};
    use crate::{Engine, KernelError, Result, SnapshotRef};

    #[rstest]
    #[case::alter(false)]
    #[case::update(true)]
    fn only_update_build_reads_clustering_metadata(
        #[case] update: bool,
        #[values(true, false)] commit_available: bool,
    ) -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let table_path = tempdir.path().join("table");
        std::fs::create_dir(&table_path)?;
        let engine = SyncEngine::new();
        create_table(
            table_path.to_string_lossy(),
            test_schema_flat(),
            "test-engine",
            false,
        )
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
        let snapshot = Snapshot::builder_for(table_path.to_string_lossy()).build(&engine)?;
        assert!(snapshot.crc_at_version().is_none());
        if !commit_available {
            std::fs::remove_file(table_path.join("_delta_log/00000000000000000000.json"))?;
        }

        let field = StructField::nullable("added", DataType::STRING);
        let result = if update {
            snapshot
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .add_column(field)
                .build(&engine, Box::new(FileSystemCommitter::new()))
                .map(|transaction| transaction.physical_clustering_columns)
        } else {
            snapshot
                .alter_table("test-engine")
                .add_column(field)
                .build(&engine, Box::new(FileSystemCommitter::new()))
                .map(|transaction| transaction.physical_clustering_columns)
        };
        if update && !commit_available {
            assert!(result.is_err());
        } else {
            assert_eq!(result?, update.then(|| vec![column_name!("id")]));
        }
        Ok(())
    }

    #[rstest]
    #[case::write(UpdateTableOperation::Write)]
    #[case::delete(UpdateTableOperation::Delete)]
    #[case::custom(UpdateTableOperation::Custom("vendor.write".to_string()))]
    fn builder_collects_existing_table_transaction_intent(
        #[case] operation: UpdateTableOperation,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(operation.clone(), "test-engine", true)
            .with_operation_parameters([("mode", Some("Append"))])
            .with_operation_metrics([("numFiles", Some("3"))])
            .with_transaction_id("app", 7)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.operation, operation.into());
        assert_eq!(transaction.engine_info, "test-engine");
        assert_eq!(
            transaction.operation_parameters.as_ref().unwrap()["mode"].as_deref(),
            Some("Append")
        );
        assert_eq!(
            transaction.operation_metrics.as_ref().unwrap()["numFiles"].as_deref(),
            Some("3")
        );
        assert_eq!(transaction.set_transactions[0].app_id, "app");
        assert_eq!(transaction.set_transactions[0].version, 7);
        assert_eq!(
            transaction.set_transactions[0].last_updated,
            Some(transaction.commit_timestamp)
        );
        assert!(transaction.data_change);
        Ok(())
    }

    #[test]
    fn replacement_setters_use_the_last_value_and_empty_correlation_clears() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(UpdateTableOperation::Delete, "second-engine", true)
            .with_correlation_id("first-correlation")
            .with_correlation_id("")
            .with_operation_parameters([("first", Some("value"))])
            .with_operation_parameters([("second", Some("value"))])
            .with_operation_metrics([("first", Some("value"))])
            .with_operation_metrics([("second", Some("value"))])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.operation, UpdateTableOperation::Delete.into());
        assert_eq!(transaction.engine_info, "second-engine");
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
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .add_column(StructField::nullable("new_column", DataType::STRING))
        );

        assert!(debug.contains("UpdateTableTransactionBuilder"));
        assert!(debug.contains("operation: UpdateTable(Write)"));
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
            .update_table(UpdateTableOperation::Write, "test-engine", false)
            .with_commit_info(commit_info, commit_info_schema)
            .with_correlation_id("correlation-id")
            .with_schema_changes([SchemaOperation::add_column(
                None,
                StructField::nullable("new_column", DataType::STRING),
            )])
            .expect("non-empty schema changes must be accepted by the setter")
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
    fn builder_rejects_non_single_row_commit_info(
        #[case] row_count: usize,
        #[values(false, true)] alter_table: bool,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let commit_info_schema = schema_ref! { nullable "tag": STRING };
        let arrow_schema: ArrowSchema = commit_info_schema.as_ref().try_into_arrow()?;
        let commit_info = Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(StringArray::from(vec![Some("value"); row_count]))],
        )?));

        let result = if alter_table {
            snapshot
                .alter_table("test-engine")
                .with_commit_info(commit_info, commit_info_schema)
                .add_column(StructField::nullable("added", DataType::STRING))
                .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
                .map(|_| ())
        } else {
            snapshot
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .with_commit_info(commit_info, commit_info_schema)
                .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
                .map(|_| ())
        };
        let error = result.unwrap_err();
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
        let builder = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .add_column_at(
                ColumnName::new(["parent"]),
                StructField::nullable("child", DataType::STRING),
            );

        assert_eq!(
            builder.local_state.schema_changes.as_ref().unwrap().len(),
            1
        );
        Ok(())
    }

    #[test]
    fn alter_table_operation_uses_unified_builder() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .alter_table("test-engine")
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(
            transaction.operation,
            crate::transaction::CommitOperation::AlterTable
        );
        assert!(!transaction.data_change);
        assert!(!transaction.is_blind_append);
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
            .update_table(
                UpdateTableOperation::Custom("vendor schema write".to_string()),
                "test-engine",
                true,
            )
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert!(transaction.should_emit_metadata);
        Ok(())
    }

    #[test]
    fn custom_operation_rejects_reserved_known_name() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .update_table(
                UpdateTableOperation::Custom("ALTER TABLE".to_string()),
                "test-engine",
                true,
            )
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();

        assert!(error.to_string().contains("reserved"));
        Ok(())
    }

    #[test]
    fn empty_alter_table_schema_changes_are_rejected() -> Result<()> {
        let (_engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let error = snapshot
            .alter_table("test-engine")
            .with_schema_changes([])
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("requires at least one schema operation"));
        Ok(())
    }

    #[test]
    fn empty_schema_changes_are_rejected() {
        let (_engine, snapshot, _tempdir) =
            load_test_table("table-without-dv-small").expect("load test table");
        let error = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .with_schema_changes(std::iter::from_fn(|| None::<SchemaOperation>))
            .unwrap_err();

        assert!(matches!(&error, KernelError::Generic(message)
            if message == "with_schema_changes requires at least one schema operation"));
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum NonemptySchemaInput {
        AddColumn,
        AlreadyNullable,
    }

    impl NonemptySchemaInput {
        fn changes(self) -> Vec<SchemaOperation> {
            match self {
                Self::AddColumn => vec![SchemaOperation::add_column(
                    None,
                    StructField::nullable("added", DataType::STRING),
                )],
                Self::AlreadyNullable => vec![SchemaOperation::SetNullable {
                    column: column_name!("id"),
                }],
            }
        }
    }

    type SchemaValidationTable = (Arc<dyn Engine>, SnapshotRef, Option<TempDir>);

    #[template]
    #[rstest]
    #[case::plain(None, false)]
    #[case::v2_compatibility_property_true(Some(TableFeature::IcebergCompatV2), true)]
    #[case::v2_compatibility_property_false(Some(TableFeature::IcebergCompatV2), false)]
    #[case::v3_compatibility_property_true(Some(TableFeature::IcebergCompatV3), true)]
    #[case::v3_compatibility_property_false(Some(TableFeature::IcebergCompatV3), false)]
    #[case::column_defaults(Some(TableFeature::AllowColumnDefaults), true)]
    fn schema_validation_tables(
        #[case] feature: Option<TableFeature>,
        #[case] feature_enabled: bool,
    ) {
    }

    #[template]
    #[apply(schema_validation_tables)]
    fn schema_validation_tables_and_operations(
        feature: Option<TableFeature>,
        feature_enabled: bool,
        #[values(
            UpdateTableOperation::Write,
            UpdateTableOperation::StreamingUpdate,
            UpdateTableOperation::Delete,
            UpdateTableOperation::Update,
            UpdateTableOperation::Merge,
            UpdateTableOperation::Optimize,
            UpdateTableOperation::Custom("UNKNOWN".to_string())
        )]
        operation: UpdateTableOperation,
    ) {
    }

    #[apply(schema_validation_tables_and_operations)]
    fn empty_schema_changes_are_rejected_for_all_table_operations(
        feature: Option<TableFeature>,
        feature_enabled: bool,
        operation: UpdateTableOperation,
    ) {
        let (_engine, snapshot, _tempdir) =
            create_schema_validation_table(feature.as_ref(), feature_enabled);
        let error = snapshot
            .update_table(operation, "test-engine", true)
            .with_schema_changes([])
            .unwrap_err();
        assert!(matches!(&error, KernelError::Generic(message)
            if message == "with_schema_changes requires at least one schema operation"));
    }

    #[apply(schema_validation_tables_and_operations)]
    fn omitted_schema_changes_allow_all_update_operations(
        feature: Option<TableFeature>,
        feature_enabled: bool,
        operation: UpdateTableOperation,
    ) {
        let (engine, snapshot, _tempdir) =
            create_schema_validation_table(feature.as_ref(), feature_enabled);
        let builder = snapshot
            .clone()
            .update_table(operation.clone(), "test-engine", true);
        assert!(builder.local_state.schema_changes.is_none());
        let result = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
        let transaction = result.expect("update transactions do not require schema changes");
        assert_eq!(transaction.operation, operation.into());
        assert!(!transaction.should_emit_metadata);
        assert!(!transaction.should_emit_protocol);
        assert_eq!(
            transaction.effective_table_config.logical_schema(),
            snapshot.schema()
        );
    }

    #[apply(schema_validation_tables_and_operations)]
    fn nonempty_schema_changes_respect_table_feature_restrictions(
        feature: Option<TableFeature>,
        feature_enabled: bool,
        operation: UpdateTableOperation,
        #[values(NonemptySchemaInput::AddColumn, NonemptySchemaInput::AlreadyNullable)]
        input: NonemptySchemaInput,
    ) {
        let (engine, snapshot, _tempdir) =
            create_schema_validation_table(feature.as_ref(), feature_enabled);
        let builder = snapshot
            .clone()
            .update_table(operation.clone(), "test-engine", true)
            .with_schema_changes(input.changes())
            .expect("non-empty schema changes must be accepted by the setter");
        assert_eq!(
            builder.local_state.schema_changes.as_ref().unwrap().len(),
            1
        );
        let result = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
        if let Some(feature) = feature.filter(|_| feature_enabled) {
            assert!(snapshot
                .table_configuration()
                .is_feature_supported(&feature));
            assert!(snapshot.table_configuration().is_feature_enabled(&feature));
            let error = result.unwrap_err();
            let expected =
                format!("Schema changes are not yet supported on tables with {feature} enabled");
            assert!(matches!(&error, KernelError::Unsupported(message) if message == &expected));
        } else {
            let transaction = result.expect("supported schema changes must build successfully");
            assert_eq!(transaction.operation, operation.into());
            assert!(transaction.should_emit_metadata);
            assert!(!transaction.should_emit_protocol);
            let schema = transaction.effective_table_config.logical_schema();
            if input == NonemptySchemaInput::AddColumn {
                assert_eq!(
                    schema.field("added").unwrap().data_type(),
                    &DataType::STRING
                );
                assert!(schema.field("added").unwrap().is_nullable());
                assert_eq!(schema.num_fields(), snapshot.schema().num_fields() + 1);
            } else {
                assert_eq!(schema, snapshot.schema());
            }
        }
    }

    #[apply(schema_validation_tables)]
    fn empty_schema_changes_are_rejected_after_queued_changes(
        feature: Option<TableFeature>,
        feature_enabled: bool,
        #[values(false, true)] individual_setter: bool,
        #[values(NonemptySchemaInput::AddColumn, NonemptySchemaInput::AlreadyNullable)]
        input: NonemptySchemaInput,
    ) {
        let (_engine, snapshot, _tempdir) =
            create_schema_validation_table(feature.as_ref(), feature_enabled);
        let builder = snapshot.update_table(UpdateTableOperation::Write, "test-engine", true);
        let builder = match (individual_setter, input) {
            (true, NonemptySchemaInput::AddColumn) => {
                builder.add_column(StructField::nullable("added", DataType::STRING))
            }
            (true, NonemptySchemaInput::AlreadyNullable) => {
                builder.set_nullable(column_name!("id"))
            }
            _ => builder
                .with_schema_changes(input.changes())
                .expect("non-empty schema changes must be accepted by the setter"),
        };
        assert_eq!(
            builder.local_state.schema_changes.as_ref().unwrap().len(),
            1
        );
        let error = builder.with_schema_changes([]).unwrap_err();
        assert!(matches!(&error, KernelError::Generic(message)
            if message == "with_schema_changes requires at least one schema operation"));
    }

    #[test]
    fn nonempty_schema_change_calls_preserve_iterator_items_and_call_order() {
        let (engine, snapshot, _tempdir) =
            load_test_table("table-without-dv-small").expect("load test table");
        let mut changes = [SchemaOperation::add_column(
            None,
            StructField::nullable("new_column", DataType::STRING),
        )]
        .into_iter();
        let builder = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .with_schema_changes(std::iter::from_fn(move || changes.next()))
            .expect("non-empty iterator must be accepted")
            .with_schema_changes([SchemaOperation::SetNullable {
                column: column_name!("new_column"),
            }])
            .expect("subsequent non-empty schema changes must be accepted")
            .add_column(StructField::nullable("another_column", DataType::INTEGER));

        let queued_changes = builder.local_state.schema_changes.as_deref().unwrap();
        assert!(matches!(
            queued_changes,
            [
                SchemaOperation::AddColumn { parent: None, field: first_column },
                SchemaOperation::SetNullable { column },
                SchemaOperation::AddColumn { parent: None, field: second_column },
            ] if first_column == &StructField::nullable("new_column", DataType::STRING)
                && column == &column_name!("new_column")
                && second_column == &StructField::nullable("another_column", DataType::INTEGER)
        ));
        let transaction = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .expect("schema changes must build in call order");
        assert!(transaction.should_emit_metadata);
        let schema = transaction.effective_table_config.logical_schema();
        assert!(schema.contains("new_column"));
        assert!(schema.contains("another_column"));
    }

    #[derive(Debug)]
    enum InvalidIntent {
        BlindAppendWithoutDataChange,
        BlindAppendWithSchemaChange,
    }

    #[rstest]
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
        let data_change = !matches!(intent, InvalidIntent::BlindAppendWithoutDataChange);
        let builder =
            snapshot.update_table(UpdateTableOperation::Write, "test-engine", data_change);
        let builder = match intent {
            InvalidIntent::BlindAppendWithoutDataChange => builder.with_blind_append(),
            InvalidIntent::BlindAppendWithSchemaChange => builder
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
    #[case::add(StagedFileAction::Add, &[true])]
    #[case::empty_add(StagedFileAction::Add, &[])]
    #[case::remove(StagedFileAction::Remove, &[true])]
    #[case::empty_remove(StagedFileAction::Remove, &[])]
    #[case::unselected_remove(StagedFileAction::Remove, &[false])]
    #[case::dv_update(StagedFileAction::DeletionVectorUpdate, &[true])]
    #[case::empty_dv_update(StagedFileAction::DeletionVectorUpdate, &[])]
    #[case::unselected_dv_update(StagedFileAction::DeletionVectorUpdate, &[false])]
    fn alter_table_commit_rejects_staged_file_batches(
        #[case] action: StagedFileAction,
        #[case] selection_vector: &[bool],
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .alter_table("test-engine")
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert!(!transaction.data_change);

        let data = || {
            let values = Arc::new(Int32Array::from(vec![1; selection_vector.len()])) as ArrayRef;
            Box::new(ArrowEngineData::new(
                RecordBatch::try_from_iter([("value", values)]).unwrap(),
            ))
        };
        match action {
            StagedFileAction::Add => transaction
                .staged_data_changes
                .add_files_metadata
                .push(data()),
            StagedFileAction::Remove | StagedFileAction::DeletionVectorUpdate => {
                let data = crate::FilteredEngineData::try_new(data(), selection_vector.to_vec())?;
                match action {
                    StagedFileAction::Remove => {
                        transaction
                            .staged_data_changes
                            .remove_files_metadata
                            .push(data);
                    }
                    StagedFileAction::DeletionVectorUpdate => {
                        transaction.staged_data_changes.dv_matched_files.push(data);
                        transaction.staged_data_changes.num_dv_updates = selection_vector
                            .iter()
                            .filter(|&&selected| selected)
                            .count();
                    }
                    StagedFileAction::Add => unreachable!(),
                }
            }
        }
        assert!(!transaction.data_change);
        let error = transaction.commit(engine.as_ref()).unwrap_err();
        assert!(
            matches!(&error, KernelError::InvalidTransactionState(message)
            if message == "ALTER TABLE cannot include data-file actions")
        );
        Ok(())
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn schema_evolution_preserves_explicit_data_change(#[case] data_change: bool) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let transaction = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", data_change)
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.data_change, data_change);
        Ok(())
    }

    #[test]
    fn alter_table_commit_rejects_invalid_internal_data_change() -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .alter_table("test-engine")
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        transaction.data_change = true;
        let error = transaction.commit(engine.as_ref()).unwrap_err();
        assert!(
            matches!(&error, KernelError::InvalidTransactionState(message)
            if message == "ALTER TABLE requires data_change to be false")
        );
        Ok(())
    }

    #[rstest]
    fn custom_schema_changes_preserve_explicit_data_change(
        #[values(false, true)] data_change: bool,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut transaction = snapshot
            .update_table(
                UpdateTableOperation::Custom("CUSTOM".to_string()),
                "test-engine",
                data_change,
            )
            .add_column(StructField::nullable("new_column", DataType::STRING))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

        assert_eq!(transaction.data_change, data_change);

        let values = Arc::new(Int32Array::from_iter_values([1])) as ArrayRef;
        transaction
            .staged_data_changes
            .add_files_metadata
            .push(Box::new(ArrowEngineData::new(RecordBatch::try_from_iter(
                [("value", values)],
            )?)));
        assert_eq!(transaction.data_change, data_change);
        Ok(())
    }

    #[rstest]
    #[case::duplicate_app_id(&[], &["app", "app"], "app_id app appears more than once")]
    #[case::duplicate_domain(
        &["domain", "domain"], &[], "domain metadata 'domain' appears more than once"
    )]
    #[case::duplicate_app_id_precedes_duplicate_addition(
        &["domain", "domain"], &["app", "app"], "app_id app appears more than once"
    )]
    fn duplicate_shared_builder_values_are_rejected(
        #[case] additions: &[&str],
        #[case] app_ids: &[&str],
        #[case] expected: &str,
        #[values(false, true)] alter_table: bool,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let result = if alter_table {
            configure_builder_metadata(snapshot.alter_table("test-engine"), additions, app_ids)
                .add_column(StructField::nullable("added", DataType::STRING))
                .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
                .map(|_| ())
        } else {
            configure_builder_metadata(
                snapshot.update_table(UpdateTableOperation::Write, "test-engine", true),
                additions,
                app_ids,
            )
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .map(|_| ())
        };
        let error = result.unwrap_err();
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[rstest]
    #[case::added_and_removed_domain(
        &["domain"], &["domain"], "cannot be added and removed"
    )]
    #[case::duplicate_removals(&[], &["domain", "domain"], "removed more than once")]
    #[case::duplicate_removal_precedes_add_remove_conflict(
        &["domain"], &["domain", "domain"], "removed more than once"
    )]
    #[case::add_remove_conflict_precedes_duplicate_addition(
        &["domain", "domain"], &["domain"], "cannot be added and removed"
    )]
    fn duplicate_builder_values_are_rejected(
        #[case] additions: &[&str],
        #[case] removals: &[&str],
        #[case] expected: &str,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let mut builder = configure_builder_metadata(
            snapshot.update_table(UpdateTableOperation::Write, "test-engine", true),
            additions,
            &[],
        );
        for domain in removals {
            builder = builder.with_domain_metadata_removed(*domain);
        }

        let error = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
            .unwrap_err();
        assert!(matches!(&error, KernelError::InvalidTransactionState(_)));
        assert!(error.to_string().contains(expected), "{error}");
        Ok(())
    }

    #[rstest]
    fn domain_metadata_without_table_feature_is_rejected_during_build(
        #[values(false, true)] alter_table: bool,
    ) -> Result<()> {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
        let result = if alter_table {
            snapshot
                .alter_table("test-engine")
                .with_domain_metadata("app.config", "{}")
                .add_column(StructField::nullable("added", DataType::STRING))
                .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
                .map(|_| ())
        } else {
            snapshot
                .update_table(UpdateTableOperation::Write, "test-engine", true)
                .with_domain_metadata("app.config", "{}")
                .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
                .map(|_| ())
        };
        let error = result.unwrap_err();

        assert!(error.to_string().contains("domainMetadata"), "{error}");
        Ok(())
    }

    fn configure_builder_metadata<T: TransactionType>(
        mut builder: TransactionBuilder<T>,
        additions: &[&str],
        app_ids: &[&str],
    ) -> TransactionBuilder<T> {
        for domain in additions {
            builder = builder.with_domain_metadata(*domain, "value");
        }
        for app_id in app_ids {
            builder = builder.with_transaction_id(*app_id, 1);
        }
        builder
    }

    fn create_schema_validation_table(
        feature: Option<&TableFeature>,
        feature_enabled: bool,
    ) -> SchemaValidationTable {
        let (engine, snapshot, tempdir): SchemaValidationTable =
            if feature == Some(&TableFeature::AllowColumnDefaults) {
                load_test_table("table-with-column-defaults").expect("load column-defaults table")
            } else {
                let property_value = if feature_enabled { "true" } else { "false" };
                let properties = match feature {
                    None => vec![],
                    Some(TableFeature::IcebergCompatV2) => vec![
                        ("delta.feature.icebergCompatV2", "supported"),
                        (ENABLE_ICEBERG_COMPAT_V2, property_value),
                        ("delta.columnMapping.mode", "name"),
                    ],
                    Some(TableFeature::IcebergCompatV3) => vec![
                        ("delta.feature.icebergCompatV3", "supported"),
                        (ENABLE_ICEBERG_COMPAT_V3, property_value),
                        ("delta.columnMapping.mode", "name"),
                        ("delta.enableRowTracking", "true"),
                    ],
                    Some(feature) => panic!("unsupported schema-validation fixture: {feature}"),
                };
                let engine = Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
                let snapshot = create_table("memory:///", test_schema_flat(), "test", true)
                    .with_table_properties(properties)
                    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
                    .expect("valid table properties")
                    .commit(engine.as_ref())
                    .expect("create schema-validation table")
                    .unwrap_post_commit_snapshot();
                (engine, snapshot, None)
            };
        if let Some(feature) = feature {
            assert!(snapshot.table_configuration().is_feature_supported(feature));
            assert_eq!(
                snapshot.table_configuration().is_feature_enabled(feature),
                feature_enabled
            );
        }
        (engine, snapshot, tempdir)
    }
}
