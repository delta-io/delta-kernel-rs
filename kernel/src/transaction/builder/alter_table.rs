//! Builder for ALTER TABLE (schema evolution) transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which uses a type-state pattern to
//! enforce valid combinations of schema operations at compile time.
//!
//! # Type States
//!
//! - [`Ready`]: Initial state. Operations are available, but `build()` is not (at least one
//!   operation is required).
//! - [`Modifying`]: After `add_column`. Can chain more `add_column` calls, and `build()` is
//!   available.

#![allow(unreachable_pub)]

use std::marker::PhantomData;
use std::sync::Arc;

use crate::committer::Committer;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::Operation;
use crate::transaction::alter_table::AlterTableTransaction;
use crate::transaction::schema_evolution::{
    apply_schema_operations, SchemaEvolutionResult, SchemaOperation,
};
use crate::{DeltaResult, Engine};

/// Initial state: operations available, `build()` is not.
pub struct Ready;

/// State after at least one operation has been added. `build()` is available.
pub struct Modifying;

/// Builder for constructing an [`AlterTableTransaction`] with schema evolution operations.
///
/// Uses a type-state pattern to enforce valid operation chaining at compile time.
pub struct AlterTableTransactionBuilder<S = Ready> {
    snapshot: SnapshotRef,
    operations: Vec<SchemaOperation>,
    _state: PhantomData<S>,
}

impl<S> AlterTableTransactionBuilder<S> {
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
    /// The field must not already exist in the schema. The field must be nullable because existing
    /// data files do not contain this column and will read NULL for it.
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::AddColumn { field });
        self.transition()
    }
}

impl AlterTableTransactionBuilder<Modifying> {
    /// Add a new top-level nullable column to the table schema.
    pub fn add_column(mut self, field: StructField) -> Self {
        self.operations.push(SchemaOperation::AddColumn { field });
        self
    }

    /// Validate and apply schema operations, then build the [`AlterTableTransaction`].
    ///
    /// This method:
    /// 1. Validates the table supports writes
    /// 2. Applies each operation sequentially against the evolving schema
    /// 3. Constructs new Metadata action with evolved schema
    /// 4. Builds the new configuration via [`TableConfiguration::try_new`]
    /// 5. Creates the transaction
    ///
    /// # Errors
    ///
    /// - Any individual operation fails validation (see per-method errors above)
    /// - Table does not support writes (unsupported features)
    /// - TableConfiguration validation fails on the evolved state
    pub fn build(
        self,
        _engine: &dyn Engine, // used by later operations (e.g., drop_column reads clustering columns)
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();

        // Validate the table supports writes
        table_config.ensure_operation_supported(Operation::Write)?;

        let schema = Arc::unwrap_or_clone(table_config.logical_schema());

        // Apply schema operations
        let SchemaEvolutionResult {
            schema: evolved_schema,
        } = apply_schema_operations(schema, &self.operations)?;

        // Build evolved metadata with new schema
        let evolved_metadata = table_config
            .metadata()
            .clone()
            .with_schema(evolved_schema)?;

        // Build evolved table configuration
        let evolved_table_config = TableConfiguration::try_new(
            evolved_metadata,
            table_config.protocol().clone(),
            table_config.table_root().clone(),
            table_config.version(),
        )?;

        AlterTableTransaction::try_new_alter_table(self.snapshot, evolved_table_config, committer)
    }
}
