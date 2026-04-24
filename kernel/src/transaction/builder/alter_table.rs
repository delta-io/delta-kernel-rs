//! Builder for ALTER TABLE (schema evolution) transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which uses a type-state pattern to
//! enforce valid operation chaining at compile time.
//!
//! # Type States
//!
//! - [`Ready`]: Initial state. Operations are available, but `build()` is not (at least one
//!   operation is required).
//! - [`Modifying`]: After any chainable schema operation. More ops can be chained, and `build()` is
//!   available. See [`AlterTableTransactionBuilder<Modifying>`] for ops.
//!
//! # Transitions
//!
//! Each `impl` block below is gated by a state bound and documents which operations that
//! state enables. Chainable schema operations live on `impl<S: Chainable>` and transition
//! the builder to a chainable state; `build()` lives on states that are buildable.
//!
//! ```ignore
//! // Allowed: at least one op queued before build().
//! snapshot.alter_table().add_column(field).build(engine, committer)?;
//!
//! // Not allowed: build() is not defined on Ready (no ops queued).
//! snapshot.alter_table().build(engine, committer)?;  // compile error
//! ```

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

/// Initial state: `build()` is not yet available (at least one operation is required).
/// See [`Chainable`] for the operations available on this state.
pub struct Ready;

/// State after at least one operation has been added. `build()` is available.
/// See [`Chainable`] for the operations available on this state.
pub struct Modifying;

/// Marker trait for builder states that accept chainable schema operations. Grouping states
/// under one bound lets each op (like `add_column`) live on a single `impl<S: Chainable>`
/// block -- chainable states share the body rather than duplicating it per state.
///
/// Sealed: external types cannot implement this, keeping the set of chainable states closed.
pub trait Chainable: sealed::Sealed {}
impl Chainable for Ready {}
impl Chainable for Modifying {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Ready {}
    impl Sealed for super::Modifying {}
}

/// Builder for constructing an [`AlterTableTransaction`] with schema evolution operations.
///
/// Uses a type-state pattern (`S`) to enforce at compile time:
/// - At least one schema operation must be queued before `build()` is callable.
/// - Only operations valid for the current state can be chained. This will disallow incompatibel
///   chaining.
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
}

impl<S: Chainable> AlterTableTransactionBuilder<S> {
    /// Add a new top-level column to the table schema.
    ///
    /// The field must not already exist in the schema (case-insensitive). The field must be
    /// nullable because existing data files do not contain this column and will read NULL for it.
    /// These constraints are validated during [`build()`](AlterTableTransactionBuilder::build).
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::AddColumn { field });
        self.transition()
    }
}

impl AlterTableTransactionBuilder<Modifying> {
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
        _engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();
        // Rejects writes to tables kernel can't safely commit to: writer version out of
        // kernel's supported range, unsupported writer features, or schemas with SQL-expression
        // invariants. Runs on the pre-alter snapshot; future ALTER variants that change the
        // protocol must also re-check this on the evolved `TableConfiguration`.
        table_config.ensure_operation_supported(Operation::Write)?;

        let schema = Arc::unwrap_or_clone(table_config.logical_schema());
        let SchemaEvolutionResult {
            schema: evolved_schema,
        } = apply_schema_operations(schema, self.operations, table_config.column_mapping_mode())?;

        let evolved_metadata = table_config
            .metadata()
            .clone()
            .with_schema(evolved_schema.clone())?;

        // Validates the evolved metadata against the protocol.
        let evolved_table_config = TableConfiguration::try_new_with_schema(
            table_config,
            evolved_metadata,
            evolved_schema,
        )?;

        AlterTableTransaction::try_new_alter_table(self.snapshot, evolved_table_config, committer)
    }
}
