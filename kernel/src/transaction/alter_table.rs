//! Alter table (schema evolution) transaction types.
//!
//! This module defines the [`AlterTableTransaction`] type alias and the [`AlterTable`] marker
//! type. The builder logic lives in [`builder::alter_table`](super::builder::alter_table).
//! The primary entry point is [`Snapshot::alter_table`](crate::snapshot::Snapshot::alter_table).
//!
//! # Example
//!
//! ```rust,no_run
//! use delta_kernel::schema::{StructField, DataType};
//! use delta_kernel::committer::FileSystemCommitter;
//! use delta_kernel::snapshot::SnapshotRef;
//! # use delta_kernel::Engine;
//! # fn example(engine: &dyn Engine, snapshot: SnapshotRef) -> delta_kernel::DeltaResult<()> {
//!
//! let result = snapshot
//!     .alter_table()
//!     .add_column(StructField::nullable("email", DataType::STRING))
//!     .build(engine, Box::new(FileSystemCommitter::new()))?
//!     .commit(engine)?;
//! # Ok(())
//! # }
//! ```

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

use std::marker::PhantomData;

use crate::committer::Committer;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::transaction::Transaction;
use crate::utils::current_time_ms;
use crate::DeltaResult;

// Re-export the builder so callers can access it from this module path.
pub use super::builder::alter_table::AlterTableTransactionBuilder;

/// Marker type for alter-table (schema evolution) transactions.
///
/// Transactions in this state have a restricted API surface -- data file operations
/// (add, remove, DV updates) are not available at compile time because schema evolution
/// commits are metadata-only.
#[derive(Debug)]
pub struct AlterTable;

/// A type alias for alter-table (schema evolution) transactions.
///
/// This provides a restricted API surface that only exposes operations valid during
/// schema evolution. Data file operations (add, remove, DV updates) are not available
/// at compile time.
///
/// # Operations NOT available on alter-table transactions
///
/// - **`add_files()`** — Schema evolution commits are metadata-only.
/// - **`get_write_context()`** — No data files are written.
/// - **`remove_files()`** — Cannot remove files in a schema evolution commit.
/// - **`update_deletion_vectors()`** — Not applicable to metadata-only commits.
/// - **`with_blind_append()`** — No data is being appended.
///
/// # Example
///
/// ```rust,no_run
/// use delta_kernel::schema::{StructField, DataType};
/// use delta_kernel::committer::FileSystemCommitter;
/// use delta_kernel::snapshot::SnapshotRef;
/// # use delta_kernel::Engine;
/// # fn example(engine: &dyn Engine, snapshot: SnapshotRef) -> delta_kernel::DeltaResult<()> {
///
/// let result = snapshot
///     .alter_table()
///     .add_column(StructField::nullable("email", DataType::STRING))
///     .build(engine, Box::new(FileSystemCommitter::new()))?
///     .commit(engine)?;
/// # Ok(())
/// # }
/// ```
pub type AlterTableTransaction = Transaction<AlterTable>;

impl AlterTableTransaction {
    /// Create a new transaction for altering a table's schema.
    ///
    /// Produces a metadata-only commit that updates the schema. The `effective_table_config`
    /// contains the evolved metadata, computed by the builder.
    ///
    /// # Arguments
    ///
    /// * `read_snapshot` - The snapshot this transaction is based on (for conflict detection)
    /// * `effective_table_config` - Table configuration with evolved metadata
    /// * `committer` - The committer to use
    pub(crate) fn try_new_alter_table(
        read_snapshot: SnapshotRef,
        effective_table_config: TableConfiguration,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Self> {
        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
            operation = "ALTER",
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
            engine_commit_info: None,
            is_blind_append: false,
            dv_matched_files: vec![],
            clustering_columns_physical: None,
            _state: PhantomData,
        })
    }
}
