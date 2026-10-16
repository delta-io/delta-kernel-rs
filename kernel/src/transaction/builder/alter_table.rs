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
//!   available. Schema operations are shared methods on [`TransactionBuilder`].
//!
//! # Transitions
//!
//! Chainable schema operations share their implementation with the update-table builder and
//! transition the builder to a chainable state; `build()` lives on states that are buildable.

use super::update_table::{try_new_existing_table, ExistingTableBuilderState};
use super::{TransactionBuilder, TransactionBuilderState};
use crate::committer::Committer;
use crate::snapshot::SnapshotRef;
use crate::transaction::{AlterTable, AlterTableTransaction, CommitOperation};
use crate::{Engine, Result};

/// Initial state: `build()` is not yet available (at least one operation is required).
#[derive(Debug)]
pub struct Ready;

/// State after at least one operation has been added. `build()` is available.
#[derive(Debug)]
pub struct Modifying;

/// Builder for constructing an [`AlterTableTransaction`] with schema evolution operations.
///
/// Uses a type-state pattern (`S`) to enforce at compile time:
/// - At least one schema operation must be queued before `build()` is callable.
/// - Only operations valid for the current state can be chained.
///
/// Obtain a builder with [`Snapshot::alter_table`](crate::Snapshot::alter_table), queue at least
/// one schema operation, then call [`build`](Self::build). The operation is fixed to `ALTER TABLE`
/// and `dataChange` is always `false`. File writes and schema evolution during writes belong on
/// [`UpdateTableTransactionBuilder`](super::update_table::UpdateTableTransactionBuilder).
///
/// Schema operations are shared with the update-table builder:
/// [`add_column`](TransactionBuilder::add_column),
/// [`add_column_at`](TransactionBuilder::add_column_at), and
/// [`set_nullable`](TransactionBuilder::set_nullable). Common setters, such as
/// [`with_correlation_id`](TransactionBuilder::with_correlation_id) and
/// [`with_operation_metrics`](TransactionBuilder::with_operation_metrics), are documented on
/// [`TransactionBuilder`] and preserve the builder's state.
///
/// ```
/// use delta_kernel::committer::FileSystemCommitter;
/// use delta_kernel::expressions::column_name;
/// use delta_kernel::schema::{DataType, StructField};
/// use delta_kernel::transaction::AlterTableTransaction;
/// use delta_kernel::{Engine, Result, SnapshotRef};
///
/// fn build(snapshot: SnapshotRef, engine: &dyn Engine) -> Result<AlterTableTransaction> {
///     snapshot
///         .alter_table("my-app/1.0")
///         .with_correlation_id("request")
///         .add_column(StructField::nullable("added", DataType::STRING))
///         .set_nullable(column_name!("added"))
///         .with_operation_metrics([("numAddedColumns", Some("1"))])
///         .build(engine, Box::new(FileSystemCommitter::new()))
/// }
/// ```
///
/// ALTER builders cannot configure data-change or blind-append intent:
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::AlterTableTransactionBuilder;
/// fn configure(builder: AlterTableTransactionBuilder) {
///     let _ = builder.with_data_change(true);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use delta_kernel::schema::{DataType, StructField};
/// use delta_kernel::transaction::AlterTableTransactionBuilder;
/// fn configure(builder: AlterTableTransactionBuilder) {
///     let _ = builder
///         .add_column(StructField::nullable("added", DataType::STRING))
///         .with_data_change(true);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::AlterTableTransactionBuilder;
/// fn configure(builder: AlterTableTransactionBuilder) {
///     let _ = builder.with_blind_append();
/// }
/// ```
///
/// ```compile_fail,E0599
/// use delta_kernel::schema::{DataType, StructField};
/// use delta_kernel::transaction::AlterTableTransactionBuilder;
/// fn configure(builder: AlterTableTransactionBuilder) {
///     let _ = builder
///         .add_column(StructField::nullable("added", DataType::STRING))
///         .with_blind_append();
/// }
/// ```
///
/// Domain removals are available only on the update-table builder:
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::AlterTableTransactionBuilder;
/// fn configure(builder: AlterTableTransactionBuilder) {
///     let _ = builder.with_domain_metadata_removed("connector.domain");
/// }
/// ```
///
/// ```compile_fail,E0599
/// use delta_kernel::schema::{DataType, StructField};
/// use delta_kernel::transaction::AlterTableTransactionBuilder;
/// fn configure(builder: AlterTableTransactionBuilder) {
///     let _ = builder
///         .add_column(StructField::nullable("added", DataType::STRING))
///         .with_domain_metadata_removed("connector.domain");
/// }
/// ```
///
/// Building before queuing a schema operation is a compile error:
///
/// ```compile_fail,E0599
/// use delta_kernel::{Engine, transaction::AlterTableTransactionBuilder};
/// use delta_kernel::committer::FileSystemCommitter;
/// fn build(builder: AlterTableTransactionBuilder, engine: &dyn Engine) {
///     let _ = builder.build(engine, Box::new(FileSystemCommitter::new()));
/// }
/// ```
///
/// cbindgen:ignore
pub type AlterTableTransactionBuilder<S = Ready> = TransactionBuilder<AlterTable<S>>;

impl<S> std::fmt::Debug for AlterTableTransactionBuilder<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterTableTransactionBuilder")
            .field("snapshot_version", &self.local_state.snapshot.version())
            .field(
                "schema_change_count",
                &self.local_state.schema_changes.as_ref().map_or(0, Vec::len),
            )
            .finish()
    }
}

impl AlterTableTransactionBuilder<Ready> {
    /// Create a new builder from a snapshot.
    pub(crate) fn new(snapshot: SnapshotRef, engine_info: String) -> Self {
        Self {
            common_state: TransactionBuilderState::new(engine_info, false),
            local_state: ExistingTableBuilderState {
                snapshot,
                operation: CommitOperation::AlterTable,
                schema_changes: None,
                domain_metadata_removals: Vec::new(),
                // TODO(#2446): match delta-spark's per-op isBlindAppend policy
                // (ADD/DROP/DROP NOT NULL -> true, SET NOT NULL -> false). Hardcoded false for
                // now: safe, but misses the true-case optimization delta-spark applies.
                is_blind_append: false,
            },
        }
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
    /// - The table enables `icebergCompatV2`, `icebergCompatV3`, or `allowColumnDefaults`, which
    ///   ALTER TABLE does not yet support
    /// - Any individual operation fails validation (see [`TransactionBuilder::add_column`],
    ///   [`TransactionBuilder::add_column_at`], and [`TransactionBuilder::set_nullable`])
    /// - CDF is enabled and the evolved schema contains a top-level column reserved for CDF
    /// - Table does not support writes (unsupported features)
    /// - The evolved schema requires protocol features not enabled on the table (e.g. adding a
    ///   `timestampNtz` column without the `timestampNtz` feature)
    /// - Shared transaction metadata is invalid
    pub fn build(
        self,
        _engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> Result<AlterTableTransaction> {
        self.common_state.validate()?;
        let transaction = try_new_existing_table(self, committer)?;
        transaction.validate_domain_metadata_operations()?;
        Ok(transaction)
    }
}
