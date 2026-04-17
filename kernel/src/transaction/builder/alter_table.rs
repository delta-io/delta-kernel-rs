//! Builder for ALTER TABLE (schema evolution) transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which uses a type-state pattern to
//! enforce valid combinations of schema operations at compile time.
//!
//! # Type States
//!
//! - [`Ready`]: Initial state. Operations are available, but `build()` is not (at least one
//!   operation is required).
//! - [`Modifying`]: After at least one operation (`add_column`, `set_nullable`). Can chain more
//!   operations, and `build()` is available.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::Operation;
use crate::transaction::alter_table::AlterTableTransaction;
use crate::transaction::schema_evolution::{
    apply_schema_operations, SchemaEvolutionResult, SchemaOperation,
};
use crate::{DeltaResult, Engine};

/// Initial state: `add_column` is available, but `build()` is not.
pub struct Ready;

/// State after at least one operation has been added. `add_column` and `build()` are available.
pub struct Modifying;

/// Builder for constructing an [`AlterTableTransaction`] with schema evolution operations.
///
/// Uses a type-state pattern to enforce valid operation chaining at compile time.
pub struct AlterTableTransactionBuilder<S = Ready> {
    snapshot: SnapshotRef,
    operations: Vec<SchemaOperation>,
    // PhantomData marker for builder state (Ready or Modifying).
    // Zero-sized; only affects which methods are available at compile time.
    _state: PhantomData<S>,
}

impl<S> AlterTableTransactionBuilder<S> {
    // Reconstructs the builder with a different PhantomData marker, changing which methods
    // are available at compile time (e.g. Ready -> Modifying enables `build()`). All real
    // fields are moved as-is; only the zero-sized type state changes.
    //
    // `T` (distinct from the struct's `S`) lets the caller pick the target state:
    // `self.transition::<Modifying>()` returns `AlterTableTransactionBuilder<Modifying>`.
    fn transition<T>(self) -> AlterTableTransactionBuilder<T> {
        AlterTableTransactionBuilder {
            snapshot: self.snapshot,
            operations: self.operations,
            _state: PhantomData,
        }
    }
}

impl AlterTableTransactionBuilder<Ready> {
    /// Create a new builder from a snapshot.
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        AlterTableTransactionBuilder {
            snapshot,
            operations: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Add a new top-level column to the table schema.
    ///
    /// The field must not already exist in the schema (case-insensitive). The field must be
    /// nullable because existing data files do not contain this column and will read NULL for it.
    /// If column mapping is enabled, the builder automatically assigns a new column ID and physical
    /// name at build time. These constraints are validated during
    /// [`build()`](AlterTableTransactionBuilder::build).
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::AddColumn { field });
        self.transition()
    }

    /// Change a column's nullability from NOT NULL to nullable.
    pub fn set_nullable(mut self, path: ColumnName) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::SetNullable { path });
        self.transition()
    }
}

impl AlterTableTransactionBuilder<Modifying> {
    /// Add a new top-level nullable column to the table schema.
    pub fn add_column(mut self, field: StructField) -> Self {
        self.operations.push(SchemaOperation::AddColumn { field });
        self
    }

    /// Change a column's nullability from NOT NULL to nullable.
    pub fn set_nullable(mut self, path: ColumnName) -> Self {
        self.operations.push(SchemaOperation::SetNullable { path });
        self
    }

    /// Validate and apply schema operations, then build the [`AlterTableTransaction`].
    ///
    /// This method:
    /// 1. Validates the table supports writes
    /// 2. Applies each operation sequentially against the evolving schema
    /// 3. Constructs new Metadata action with evolved schema
    /// 4. Builds the evolved table configuration
    /// 5. Creates the transaction
    ///
    /// # Errors
    ///
    /// - Any individual operation fails validation (see per-method errors above)
    /// - Table does not support writes (unsupported features)
    /// - The evolved schema requires protocol features not enabled on the table (e.g. adding a
    ///   `timestampNtz` column without the `timestampNtz` feature)
    pub fn build(
        self,
        _engine: &dyn Engine, // used by later operations (e.g., drop_column reads clustering columns)
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();
        table_config.ensure_operation_supported(Operation::Write)?;

        let schema = Arc::unwrap_or_clone(table_config.logical_schema());
        let column_mapping_mode = table_config.column_mapping_mode();
        let current_max_column_id = table_config
            .metadata()
            .configuration()
            .get(crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID)
            .map(|v| {
                v.parse::<i64>().map_err(|_| {
                    crate::Error::generic(format!(
                        "Invalid delta.columnMapping.maxColumnId value: '{v}'"
                    ))
                })
            })
            .transpose()?;
        let SchemaEvolutionResult {
            schema: evolved_schema,
            new_max_column_id,
        } = apply_schema_operations(
            schema,
            self.operations,
            column_mapping_mode,
            current_max_column_id,
        )?;

        let mut evolved_metadata = table_config
            .metadata()
            .clone()
            .with_schema(evolved_schema.clone())?;

        if let Some(new_id) = new_max_column_id {
            let mut config = table_config.metadata().configuration().clone();
            config.insert(
                crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID.to_string(),
                new_id.to_string(),
            );
            evolved_metadata = evolved_metadata.with_configuration(config);
        }

        // Validates the evolved metadata against the protocol.
        let evolved_table_config = TableConfiguration::try_new_with_schema(
            evolved_metadata,
            table_config.protocol().clone(),
            table_config.table_root().clone(),
            table_config.version(),
            evolved_schema,
        )?;

        AlterTableTransaction::try_new_alter_table(self.snapshot, evolved_table_config, committer)
    }
}
