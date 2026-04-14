//! Alter table transaction types and constructor.
//!
//! This module defines the [`AlterTableTransaction`] type alias and the
//! [`try_new_alter_table`](AlterTableTransaction::try_new_alter_table) constructor.
//! The builder logic lives in [`builder::alter_table`](super::builder::alter_table).

#![allow(unreachable_pub)]

use std::marker::PhantomData;

use crate::committer::Committer;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::transaction::{AlterTable, Transaction};
use crate::utils::current_time_ms;
use crate::DeltaResult;

/// A type alias for alter-table (schema evolution) transactions.
///
/// This provides a restricted API surface that only exposes operations valid during schema
/// evolution. Data file operations are not available at compile time because `AlterTable`
/// does not implement [`SupportsDataFiles`](super::SupportsDataFiles).
///
/// # Operations NOT available on alter-table transactions
///
/// - **`add_files()`** -- Cannot add data files in a metadata-only commit.
/// - **`remove_files()`** -- Cannot remove data files in a metadata-only commit.
/// - **`get_write_context()`** -- No data files to write.
/// - **`stats_schema()`** / **`stats_columns()`** -- No data files needing stats.
/// - **`update_deletion_vectors()`** -- Deletion vectors require data file operations.
/// - **`with_blind_append()`** -- Not a data append.
///
/// ```compile_fail,E0599
/// fn cannot_add_files(txn: &mut delta_kernel::transaction::alter_table::AlterTableTransaction) {
///     txn.add_files(todo!());
/// }
/// ```
///
/// ```compile_fail,E0599
/// fn cannot_get_write_context(txn: &delta_kernel::transaction::alter_table::AlterTableTransaction) {
///     let _ = txn.get_write_context();
/// }
/// ```
///
/// The builder enforces at least one operation before `build()` via the type-state pattern:
///
/// ```compile_fail,E0599
/// fn cannot_build_without_operations(
///     builder: delta_kernel::transaction::builder::alter_table::AlterTableTransactionBuilder,
/// ) {
///     let _ = builder.build(todo!(), todo!());
/// }
/// ```
pub type AlterTableTransaction = Transaction<AlterTable>;

impl AlterTableTransaction {
    /// Create a new transaction for altering a table's schema. Produces a metadata-only commit
    /// that emits an updated Metadata action with the evolved schema.
    ///
    /// The `effective_table_config` is the evolved table configuration (new schema, same
    /// protocol). The `read_snapshot` provides the pre-commit state for conflict detection.
    ///
    /// This is typically called via `AlterTableTransactionBuilder::build()` rather than directly.
    pub(crate) fn try_new_alter_table(
        read_snapshot: SnapshotRef,
        effective_table_config: TableConfiguration,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Self> {
        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
            operation = "ALTER TABLE",
        );

        Ok(Transaction {
            span,
            read_snapshot: Some(read_snapshot),
            effective_table_config,
            should_emit_protocol: false,
            should_emit_metadata: true,
            committer,
            operation: Some("ALTER TABLE".to_string()),
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp: current_time_ms()?,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: vec![],
            user_domain_removals: vec![],
            data_change: false,
            shared_write_state: std::sync::OnceLock::new(),
            engine_commit_info: None,
            is_blind_append: false,
            dv_matched_files: vec![],
            physical_clustering_columns: None,
            _state: PhantomData,
        })
    }
}
