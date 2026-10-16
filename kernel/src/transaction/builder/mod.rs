//! Type-specific transaction construction with shared configuration setters.

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

pub mod alter_table;
pub mod create_table;
mod state;
pub mod update_table;

use std::sync::Arc;

use delta_kernel_derive::internal_api;

pub(super) use self::state::TransactionBuilderState;
use crate::actions::DomainMetadata;
use crate::expressions::ColumnName;
use crate::schema::{SchemaRef, StructField};
use crate::transaction::commit_info::collect_operation_metadata;
use crate::transaction::schema_evolution::SchemaOperation;
use crate::transaction::{AlterTable, CreateTable, SupportsSchemaChanges, UpdateTable};
use crate::utils::require;
use crate::{EngineData, KernelError, Result};

/// A transaction type supported by [`TransactionBuilder`].
///
/// Each type selects its builder state and specializes constructors, setters, and build
/// validation. This trait is sealed; Kernel provides the supported transaction types.
pub trait TransactionType: private::Sealed {
    /// Builder state specific to this transaction type. Use `()` when no extra state is needed.
    #[doc(hidden)]
    type BuilderState: Send;
}

/// Configures a transaction of type `T`.
///
/// Common configuration setters preserve the transaction type. Constructors, type-specific setters,
/// and `build` are provided by the specialized implementations for [`CreateTable`],
/// [`UpdateTable`], and [`AlterTable`].
/// Use [`create_table`](super::create_table::create_table) or
/// [`Snapshot::update_table`](crate::Snapshot::update_table), or
/// [`Snapshot::alter_table`](crate::Snapshot::alter_table) to obtain a builder.
///
/// The operation cannot be changed on a builder:
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{TransactionBuilder, UpdateTableOperation};
///
/// fn configure(builder: TransactionBuilder) {
///     let _ = builder.with_operation(UpdateTableOperation::Write);
/// }
/// ```
///
/// Update-table builders cannot configure new-table properties:
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{TransactionBuilder, UpdateTable};
///
/// fn configure(builder: TransactionBuilder<UpdateTable>) {
///     let _ = builder.with_table_properties([("app.version", "1")]);
/// }
/// ```
pub struct TransactionBuilder<T: TransactionType = UpdateTable> {
    common_state: TransactionBuilderState,
    local_state: T::BuilderState,
}

impl<T: TransactionType> TransactionBuilder<T> {
    /// Adds an application transaction identifier to emit as a `txn` action.
    ///
    /// Returns the updated builder. The action's `lastUpdated` value uses the transaction's commit
    /// timestamp. An application id may occur only once; duplicate ids are rejected during build.
    pub fn with_transaction_id(mut self, app_id: impl Into<String>, version: i64) -> Self {
        self.common_state
            .transaction_ids
            .push((app_id.into(), version));
        self
    }

    /// Adds user-controlled domain metadata and returns the updated builder.
    ///
    /// `domain` identifies the metadata and `configuration` supplies its serialized value. Each
    /// domain may occur only once; conflicts are rejected during build.
    pub fn with_domain_metadata(
        mut self,
        domain: impl Into<String>,
        configuration: impl Into<String>,
    ) -> Self {
        self.common_state
            .domain_metadata_additions
            .push(DomainMetadata::new(domain.into(), configuration.into()));
        self
    }

    /// Attaches an opaque identifier to the transaction's metric events.
    ///
    /// Returns the updated builder. Consecutive calls replace the previous value; an empty
    /// `correlation_id` clears it.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.common_state.correlation_id = Some(correlation_id.into()).filter(|id| !id.is_empty());
        self
    }

    /// Replaces the operation parameters recorded in `commitInfo`.
    ///
    /// Returns the updated builder. Values must already be stringified as expected in table
    /// history; `None` writes a null map value. The map replaces rather than merges with an earlier
    /// map, and the last value wins when a key occurs more than once. Dedicated parameters take
    /// precedence over a same-named nested field supplied by
    /// [`with_commit_info`](Self::with_commit_info).
    pub fn with_operation_parameters<I, K, V>(mut self, parameters: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.common_state.operation_parameters = Some(collect_operation_metadata(parameters));
        self
    }

    /// Replaces the operation metrics recorded in `commitInfo`.
    ///
    /// Returns the updated builder. Values must already be stringified as expected in table
    /// history; `None` writes a null map value. The map replaces rather than merges with an earlier
    /// map, and the last value wins when a key occurs more than once. Metrics supplied to the built
    /// transaction replace these values. Dedicated metrics take precedence over a same-named
    /// nested field supplied by [`with_commit_info`](Self::with_commit_info).
    pub fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.common_state.operation_metrics = Some(collect_operation_metadata(metrics));
        self
    }

    /// Supplies one arbitrary connector-provided `commitInfo` row and its schema.
    ///
    /// Returns the updated builder. Consecutive calls replace the previous row. Kernel-owned
    /// fields and dedicated parameter or metric maps take precedence over same-named fields in
    /// this row. Build rejects `commit_info` if it does not contain exactly one row.
    pub fn with_commit_info(
        mut self,
        commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.common_state.engine_commit_info = Some((commit_info, commit_info_schema));
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

impl TransactionType for CreateTable {
    type BuilderState = create_table::CreateTableBuilderState;
}

impl TransactionType for UpdateTable {
    type BuilderState = update_table::ExistingTableBuilderState;
}

impl<S> TransactionType for AlterTable<S> {
    type BuilderState = update_table::ExistingTableBuilderState;
}

impl private::Sealed for CreateTable {}
impl private::Sealed for UpdateTable {}
impl<S> private::Sealed for AlterTable<S> {}

mod private {
    pub trait Sealed {}
}

#[cfg(test)]
mod tests {
    use super::{private, TransactionBuilder, TransactionBuilderState, TransactionType};
    use crate::arrow::array::StringArray;
    use crate::schema::schema_ref;
    use crate::unit_test_utils::string_array_to_engine_data;

    struct CustomType;

    impl private::Sealed for CustomType {}

    impl TransactionType for CustomType {
        type BuilderState = ();
    }

    impl TransactionBuilder<CustomType> {
        fn new(engine_info: &str) -> Self {
            Self {
                common_state: TransactionBuilderState::new(engine_info.into(), false),
                local_state: (),
            }
        }
    }

    fn configure_shared_setters<T: TransactionType>(
        builder: TransactionBuilder<T>,
    ) -> TransactionBuilder<T> {
        builder
            .with_correlation_id("correlation")
            .with_operation_parameters([("mode", Some("Append"))])
            .with_operation_metrics([("numFiles", Some("3"))])
            .with_transaction_id("app", 7)
            .with_domain_metadata("app.domain", "{}")
            .with_commit_info(
                string_array_to_engine_data(StringArray::from(vec!["tag"])),
                schema_ref! { nullable "a": STRING },
            )
    }

    #[test]
    fn custom_type_supports_shared_setters_without_custom_state() {
        let builder = TransactionBuilder::<CustomType>::new("test-engine");
        let builder = configure_shared_setters(builder);

        assert_eq!(builder.common_state.engine_info, "test-engine");
        assert_eq!(
            builder.common_state.correlation_id.as_deref(),
            Some("correlation")
        );
        assert_eq!(
            builder.common_state.operation_parameters.as_ref().unwrap()["mode"].as_deref(),
            Some("Append")
        );
        assert_eq!(
            builder.common_state.operation_metrics.as_ref().unwrap()["numFiles"].as_deref(),
            Some("3")
        );
        assert_eq!(builder.common_state.transaction_ids, [("app".into(), 7)]);
        assert_eq!(
            builder.common_state.domain_metadata_additions[0].domain(),
            "app.domain"
        );
        assert!(!builder.common_state.data_change);
        let (commit_info, schema) = builder.common_state.engine_commit_info.as_ref().unwrap();
        assert_eq!(commit_info.len(), 1);
        assert_eq!(
            schema.as_ref(),
            schema_ref! { nullable "a": STRING }.as_ref()
        );
        builder.common_state.validate().unwrap();
    }
}
