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
//! - [`Renaming`]: After `rename_column`. Terminal state -- only `build()` is available.
//!
//! # Transitions
//!
//! Each `impl` block below is gated by a state bound and documents which operations that
//! state enables. Chainable schema operations live on `impl<S: Chainable>` and transition
//! the builder to a chainable state; `build()` lives on states that are buildable
//! (see [`Buildable`]).
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
use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{Operation, TableFeature};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::alter_table::AlterTableTransaction;
use crate::transaction::schema_evolution::{
    apply_schema_operations, SchemaEvolutionResult, SchemaOperation,
};
use crate::{DeltaResult, Engine};

/// Initial state: `build()` is not yet available (at least one operation is required).
/// See [`Chainable`] for the operations available on this state.
pub struct Ready;

/// State after at least one chainable operation. `build()` is available, more chainable ops
/// can be queued. See [`Chainable`] for the operations available on this state.
pub struct Modifying;

/// Terminal state after `rename_column`. Only `build()` is available; no further operations
/// can be chained. This mirrors delta-spark's per-statement semantics: a single ALTER TABLE
/// statement that renames a column is not also allowed to add, drop, or set-nullable other
/// columns.
pub struct Renaming;

/// Marker trait for builder states that accept chainable schema operations. Grouping states
/// under one bound lets each op (like `add_column`) live on a single `impl<S: Chainable>`
/// block -- chainable states share the body rather than duplicating it per state.
///
/// Sealed: external types cannot implement this, keeping the set of chainable states closed.
pub trait Chainable: sealed::Sealed {}
impl Chainable for Ready {}
impl Chainable for Modifying {}

/// Marker trait for builder states that support `build()`. Grouping the post-op states
/// (`Modifying` and `Renaming`) under one bound lets `build()` live on a single
/// `impl<S: Buildable>` block -- shared body rather than duplicating per state.
///
/// Sealed: external types cannot implement this, keeping the set of buildable states closed.
pub trait Buildable: sealed::Sealed {}
impl Buildable for Modifying {}
impl Buildable for Renaming {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Ready {}
    impl Sealed for super::Modifying {}
    impl Sealed for super::Renaming {}
}

/// Builder for constructing an [`AlterTableTransaction`] with schema evolution operations.
///
/// Uses a type-state pattern (`S`) to enforce at compile time:
/// - At least one schema operation must be queued before `build()` is callable.
/// - Only operations valid for the current state can be chained. This will disallow incompatible
///   chaining.
pub struct AlterTableTransactionBuilder<S = Ready> {
    snapshot: SnapshotRef,
    operations: Vec<SchemaOperation>,
    // PhantomData marker for builder state (Ready, Modifying, or Renaming).
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
    /// `field` and any of its nested fields must not carry `delta.columnMapping.id` or
    /// `delta.columnMapping.physicalName` annotations.
    ///
    /// These constraints are validated during [`build()`](AlterTableTransactionBuilder::build).
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::AddColumn { field });
        self.transition()
    }

    /// Drop a column from the table schema. Supports nested columns via [`ColumnName`] paths
    /// (e.g. `column_name!("address.city")`).
    ///
    /// Requires column mapping to be enabled (mode = name or id). The column is removed from the
    /// logical schema but physical data in existing Parquet files is untouched.
    ///
    /// # Errors (at build time)
    ///
    /// - Column does not exist in the current schema
    /// - Column mapping is not enabled on the table
    /// - Column is a partition column or clustering column
    /// - Column is the last remaining field at its struct level (would produce an empty struct)
    /// - Column is an ancestor struct of a clustering column
    /// - An intermediate component of the path is not a struct (e.g. `name.inner` where `name` is a
    ///   primitive)
    /// - Table has `delta.dataSkippingStatsColumns` set (kernel doesn't yet rewrite it; #2446)
    // TODO(#2446): rewrite `delta.dataSkippingStatsColumns` on drop to match delta-spark.
    pub fn drop_column(mut self, column: ColumnName) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::DropColumn { column });
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

    /// Rename a column in the table schema. Supports nested columns via [`ColumnName`] paths.
    ///
    /// Requires column mapping to be enabled (mode = name or id). Only the logical name
    /// changes; the physical name and column ID are preserved, so existing Parquet files
    /// continue to be readable without rewrites.
    ///
    /// Rename is a terminal operation: no further operations can be chained on the same
    /// builder. This matches delta-spark's per-statement semantics.
    ///
    /// Renaming a column to its current name (exact match) is a no-op; a case-only rename
    /// (e.g. `name` -> `Name`) updates the stored logical name. Both still produce a commit.
    ///
    /// # Errors (at build time)
    ///
    /// - Column path is empty or the column does not exist in the current schema
    /// - Column mapping is not enabled on the table
    /// - `new_name` is empty or contains invalid characters (e.g. newlines)
    /// - New name conflicts with an existing sibling column (case-insensitive)
    /// - Column is a partition column, clustering column, or ancestor struct of one
    /// - An intermediate component of the path is not a struct (e.g. `name.inner` where `name` is a
    ///   primitive)
    /// - Table has `delta.dataSkippingStatsColumns` set (kernel doesn't yet rewrite it; #2446)
    // TODO(#2446): rewrite `delta.dataSkippingStatsColumns` on rename to match delta-spark.
    pub fn rename_column(
        mut self,
        column: ColumnName,
        new_name: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Renaming> {
        self.operations.push(SchemaOperation::RenameColumn {
            column,
            new_name: new_name.into(),
        });
        self.transition()
    }
}

impl<S: Buildable> AlterTableTransactionBuilder<S> {
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
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();
        // Rejects writes to tables kernel can't safely commit to: writer version out of
        // kernel's supported range, unsupported writer features, or schemas with SQL-expression
        // invariants. Runs on the pre-alter snapshot; future ALTER variants that change the
        // protocol must also re-check this on the evolved `TableConfiguration`.
        table_config.ensure_operation_supported(Operation::Write)?;

        // drop_column and rename_column do not yet check dependent expressions (Spark's
        // checkDependentExpressions) or rewrite stats-columns metadata. Block these ops on
        // tables with the corresponding features so flipping any feature to Supported won't
        // silently orphan a generated/CHECK/identity reference, and so a rewrite gap on
        // `delta.dataSkippingStatsColumns` doesn't leave the property pointing at a missing
        // or renamed column.
        let op_label = self.operations.iter().find_map(|op| match op {
            SchemaOperation::DropColumn { .. } => Some("drop_column"),
            SchemaOperation::RenameColumn { .. } => Some("rename_column"),
            _ => None,
        });
        if let Some(op_label) = op_label {
            for feature in [
                TableFeature::GeneratedColumns,
                TableFeature::CheckConstraints,
                TableFeature::IdentityColumns,
            ] {
                if table_config.is_feature_supported(&feature) {
                    return Err(Error::unsupported(format!(
                        "{op_label} is not supported on tables with the '{feature}' feature: \
                         the column may be referenced by a generated-column expression, \
                         CHECK constraint, or identity column"
                    )));
                }
            }
            if table_config
                .table_properties()
                .data_skipping_stats_columns
                .is_some()
            {
                return Err(Error::unsupported(format!(
                    "{op_label} is not supported on tables with \
                     'delta.dataSkippingStatsColumns' set: the property may reference the \
                     column and kernel does not yet rewrite it on drop or rename"
                )));
            }
        }

        let schema = Arc::unwrap_or_clone(table_config.logical_schema());
        let column_mapping_mode = table_config.column_mapping_mode();
        let current_max_column_id = table_config.table_properties().column_mapping_max_column_id;
        let partition_columns = table_config.partition_columns();
        let clustering_columns = self.snapshot.get_logical_clustering_columns(engine)?;
        let SchemaEvolutionResult {
            schema: evolved_schema,
            new_max_column_id,
            operation,
            is_blind_append,
        } = apply_schema_operations(
            schema,
            self.operations,
            column_mapping_mode,
            current_max_column_id,
            partition_columns,
            clustering_columns.as_deref().unwrap_or(&[]),
        )?;

        let mut evolved_metadata = table_config
            .metadata()
            .clone()
            .with_schema(evolved_schema.clone())?;
        if let Some(id) = new_max_column_id {
            evolved_metadata = evolved_metadata
                .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, id.to_string());
        }

        // Validates the evolved metadata against the protocol.
        let evolved_table_config = TableConfiguration::try_new_with_schema(
            table_config,
            evolved_metadata,
            evolved_schema,
        )?;

        AlterTableTransaction::try_new_alter_table(
            self.snapshot,
            evolved_table_config,
            committer,
            operation,
            is_blind_append,
        )
    }
}
