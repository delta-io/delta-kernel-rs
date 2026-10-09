//! Builder for ALTER TABLE transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which uses a type-state pattern to
//! enforce valid operation chaining at compile time.
//!
//! # Type States
//!
//! - [`Ready`]: Initial state. Operations are available, but `build()` is not (at least one
//!   operation is required).
//! - [`Modifying`]: After any chainable operation. More ops can be chained, and `build()` is
//!   available. See [`AlterTableTransactionBuilder<Modifying>`] for ops.
//!
//! # Transitions
//!
//! Each `impl` block below is gated by a state bound and documents which operations that
//! state enables. Chainable operations live on `impl<S: Chainable>` and transition
//! the builder to a chainable state; `build()` lives on states that are buildable.
//!
//! ```ignore
//! // Allowed: at least one op queued before build().
//! snapshot.alter_table().add_column(field).build(engine, committer)?;
//!
//! // Not allowed: build() is not defined on Ready (no ops queued).
//! snapshot.alter_table().build(engine, committer)?;  // compile error
//! ```

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_features::{Operation, TableFeature};
use crate::transaction::alter_table::AlterTableTransaction;
use crate::transaction::schema_evolution::{evolve_table_config, SchemaOperation};
use crate::utils::PhantomType;
#[cfg(feature = "check-constraints-in-dev")]
use crate::write_expressions::{apply_check_constraint_operations, CheckConstraintOperation};
use crate::{Engine, KernelError, Result};

/// Initial state: `build()` is not yet available (at least one operation is required).
/// See [`Chainable`] for the operations available on this state.
pub struct Ready;

/// State after at least one operation has been added. `build()` is available.
/// See [`Chainable`] for the operations available on this state.
pub struct Modifying;

/// Marker trait for builder states that accept chainable operations. Grouping states
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

/// Builder for constructing an [`AlterTableTransaction`].
///
/// Uses a type-state pattern (`S`) to enforce at compile time:
/// - At least one operation must be added before `build()` is callable.
/// - Only operations valid for the current state can be chained. This will disallow incompatible
///   chaining.
pub struct AlterTableTransactionBuilder<S = Ready> {
    snapshot: SnapshotRef,
    operations: Vec<SchemaOperation>,
    #[cfg(feature = "check-constraints-in-dev")]
    check_constraint_operations: Vec<CheckConstraintOperation>,
    correlation_id: Option<Arc<str>>,
    // PhantomType marker for builder state (Ready or Modifying).
    // Zero-sized; only affects which methods are available at compile time.
    _state: PhantomType<S>,
}

impl<S> AlterTableTransactionBuilder<S> {
    // Reconstructs the builder with a different PhantomType marker, changing which methods
    // are available at compile time (e.g. Ready -> Modifying enables `build()`). All real
    // fields are moved as-is; only the zero-sized type state changes.
    //
    // `T` (distinct from the struct's `S`) lets the caller pick the target state:
    // `self.transition::<Modifying>()` returns `AlterTableTransactionBuilder<Modifying>`.
    fn transition<T>(self) -> AlterTableTransactionBuilder<T> {
        AlterTableTransactionBuilder {
            snapshot: self.snapshot,
            operations: self.operations,
            #[cfg(feature = "check-constraints-in-dev")]
            check_constraint_operations: self.check_constraint_operations,
            correlation_id: self.correlation_id,
            _state: PhantomType::default(),
        }
    }

    /// Attach an opaque, caller-supplied correlation id for joining the alter-table commit's metric
    /// events to the caller's own request or operation id. An empty id is treated as unset.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.correlation_id = Some(correlation_id.into()).filter(|id| !id.is_empty());
        self
    }
}

impl AlterTableTransactionBuilder<Ready> {
    /// Create a new builder from a snapshot.
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        AlterTableTransactionBuilder {
            snapshot,
            operations: Vec::new(),
            #[cfg(feature = "check-constraints-in-dev")]
            check_constraint_operations: Vec::new(),
            correlation_id: None,
            _state: PhantomType::default(),
        }
    }
}

impl<S: Chainable> AlterTableTransactionBuilder<S> {
    /// Add a new top-level column to the table schema.
    ///
    /// The field must not already exist in the schema (case-insensitive). The field must be
    /// nullable because existing data files do not contain this column and will read NULL for it.
    /// On column-mapping tables, Kernel assigns or preserves column-mapping IDs and physical names
    /// for the added field.
    ///
    /// These constraints are validated during [`build()`](AlterTableTransactionBuilder::build).
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations
            .push(SchemaOperation::add_column(None, field));
        self.transition()
    }

    /// Change a column's nullability from NOT NULL to nullable. If the column is already
    /// nullable, the op is a no-op but still generates a commit.
    ///
    /// Note: this matches Spark's behavior.
    pub fn set_nullable(mut self, column: ColumnName) -> AlterTableTransactionBuilder<Modifying> {
        self.operations
            .push(SchemaOperation::SetNullable { column });
        self.transition()
    }

    /// Add a CHECK constraint, stored under `delta.constraints.<name>` with `raw_sql` as its
    /// expression. The name is stored lowercased. Kernel does not parse or evaluate `raw_sql`.
    ///
    /// The name must not match an existing constraint case-insensitively, and the expression must
    /// not be empty or whitespace-only. Adding the first constraint enables the `checkConstraints`
    /// writer feature. Committing requires
    /// [`ack_check_constraints`](crate::transaction::Transaction::ack_check_constraints), which
    /// lists what the connector must verify for the new constraint.
    ///
    /// These constraints are validated during [`build()`](AlterTableTransactionBuilder::build).
    #[cfg(feature = "check-constraints-in-dev")]
    pub fn add_check_constraint(
        mut self,
        name: impl Into<String>,
        raw_sql: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.check_constraint_operations
            .push(CheckConstraintOperation::Add {
                name: name.into(),
                raw_sql: raw_sql.into(),
            });
        self.transition()
    }

    /// Drop every CHECK constraint whose name matches `name` case-insensitively. The
    /// `checkConstraints` feature stays in the protocol. Committing requires
    /// [`ack_check_constraints`](crate::transaction::Transaction::ack_check_constraints) only when
    /// the table still declares other constraints afterwards.
    ///
    /// The constraint must exist. This is validated during
    /// [`build()`](AlterTableTransactionBuilder::build).
    #[cfg(feature = "check-constraints-in-dev")]
    pub fn drop_check_constraint(
        mut self,
        name: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.check_constraint_operations
            .push(CheckConstraintOperation::Drop {
                name: name.into(),
                if_exists: false,
            });
        self.transition()
    }

    /// Drop every CHECK constraint whose name matches `name` case-insensitively, if any exists.
    /// Behaves like [`drop_check_constraint`](Self::drop_check_constraint), except that a missing
    /// constraint is not an error. If the constraint is missing, the op is a no-op but still
    /// generates a commit.
    #[cfg(feature = "check-constraints-in-dev")]
    pub fn drop_check_constraint_if_exists(
        mut self,
        name: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.check_constraint_operations
            .push(CheckConstraintOperation::Drop {
                name: name.into(),
                if_exists: true,
            });
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
    /// These constraints are validated during [`build()`](AlterTableTransactionBuilder::build).
    #[internal_api]
    pub(crate) fn add_column_at(
        mut self,
        parent: ColumnName,
        field: StructField,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.operations
            .push(SchemaOperation::add_column(parent, field));
        self.transition()
    }
}

impl AlterTableTransactionBuilder<Modifying> {
    /// Validate and apply the operations, then build the [`AlterTableTransaction`].
    ///
    /// This method:
    /// 1. Validates the table supports writes
    /// 2. Applies each schema operation sequentially against the evolving schema
    /// 3. Constructs new Metadata action with evolved schema
    /// 4. Applies each CHECK-constraint operation, in order, after all schema operations
    /// 5. Builds the evolved table configuration
    /// 6. Creates the transaction
    ///
    /// # Errors
    ///
    /// - The table enables `icebergCompatV2`, `icebergCompatV3`, or `allowColumnDefaults`, which
    ///   ALTER TABLE does not yet support
    /// - Any individual operation fails validation (see per-method errors above)
    /// - CDF is enabled and the evolved schema contains a top-level column reserved for CDF
    /// - Table does not support writes (unsupported features)
    /// - The evolved schema requires protocol features not enabled on the table (e.g. adding a
    ///   `timestampNtz` column without the `timestampNtz` feature)
    pub fn build(
        self,
        _engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> Result<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();
        // kernel doesn't currently support altering tables with these features
        let unsupported_iceberg_compat =
            [TableFeature::IcebergCompatV2, TableFeature::IcebergCompatV3]
                .into_iter()
                .find(|feature| table_config.is_feature_enabled(feature));
        if let Some(feature) = unsupported_iceberg_compat {
            return Err(KernelError::unsupported(format!(
                "ALTER TABLE is not yet supported on tables with {feature} enabled"
            )));
        }
        // TODO(#2630): Support ALTER TABLE on tables with column defaults.
        if table_config.is_feature_enabled(&TableFeature::AllowColumnDefaults) {
            return Err(KernelError::unsupported(
                "ALTER TABLE is not yet supported on tables with allowColumnDefaults enabled",
            ));
        }
        // Rejects writes to tables kernel can't safely commit to: writer version out of
        // kernel's supported range, unsupported writer features, or schemas with SQL-expression
        // invariants. Runs on the pre-alter snapshot. Operations that change the protocol re-check
        // this on the altered configuration below.
        table_config.ensure_operation_supported(Operation::Write)?;

        let evolved_table_config = evolve_table_config(table_config, self.operations)?;
        #[cfg(feature = "check-constraints-in-dev")]
        let evolved_table_config = if self.check_constraint_operations.is_empty() {
            evolved_table_config
        } else {
            let evolved_table_config = apply_check_constraint_operations(
                &evolved_table_config,
                self.check_constraint_operations,
            )?;
            // Adding a constraint can enable `checkConstraints` and so change the protocol.
            evolved_table_config.ensure_operation_supported(Operation::Write)?;
            evolved_table_config
        };

        AlterTableTransaction::try_new_alter_table(
            self.snapshot,
            evolved_table_config,
            committer,
            self.correlation_id,
        )
    }
}
