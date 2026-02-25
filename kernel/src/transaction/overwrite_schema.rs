//! Overwrite-schema transaction types and entry point (internal API).
//!
//! This module defines the [`OverwriteSchemaTransaction`] type alias and the [`overwrite_schema`]
//! entry point function. The builder logic lives in
//! [`builder::overwrite_schema`](super::builder::overwrite_schema).

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

use std::marker::PhantomData;

use crate::actions::Metadata;
use crate::committer::Committer;
use crate::engine_data::FilteredEngineData;
use crate::expressions::ColumnName;
use crate::schema::SchemaRef;
use crate::snapshot::SnapshotRef;
use crate::transaction::{OverwriteSchema, Transaction};
use crate::utils::current_time_ms;
use crate::DeltaResult;

// Re-export the builder so callers can still access it from this module path.
pub use super::builder::overwrite_schema::OverwriteSchemaTransactionBuilder;

/// A type alias for overwrite-schema transactions.
///
/// This provides a restricted API surface that only exposes operations valid during
/// schema overwrite. Operations like removing files, removing domain metadata, updating
/// deletion vectors, setting blind append, and setting the operation are not available
/// at compile time.
///
/// # Operations NOT available on overwrite-schema transactions
///
/// - **`with_operation()`** — The operation is fixed to `"OVERWRITE"`.
/// - **`with_blind_append()`** — Blind append is incompatible with schema overwrite.
/// - **`remove_files()`** — Files to remove are collected automatically during build.
/// - **`update_deletion_vectors()`** — Not applicable during schema overwrite.
/// - **`with_domain_metadata_removed()`** — Not available during schema overwrite.
pub type OverwriteSchemaTransaction = Transaction<OverwriteSchema>;

/// Creates a builder for overwriting the schema of an existing Delta table.
///
/// This function returns an [`OverwriteSchemaTransactionBuilder`] that can be configured
/// before building an [`OverwriteSchemaTransaction`]. The `build()` method performs
/// all validation and I/O (scanning existing files for removal, handling column mapping).
///
/// # Arguments
///
/// * `snapshot` - The snapshot of the existing table to overwrite
/// * `new_schema` - The new logical schema for the table
/// * `partition_columns` - Partition columns for the new schema (empty vec for unpartitioned)
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use delta_kernel::transaction::overwrite_schema::overwrite_schema;
/// use delta_kernel::schema::{DataType, StructField, StructType};
/// use delta_kernel::committer::FileSystemCommitter;
/// use delta_kernel::snapshot::Snapshot;
///
/// # fn main() -> delta_kernel::DeltaResult<()> {
/// # let engine: &dyn delta_kernel::Engine = todo!();
/// # let table_url: url::Url = todo!();
/// let schema = Arc::new(StructType::new_unchecked(vec![
///     StructField::new("id", DataType::INTEGER, false),
///     StructField::new("name", DataType::STRING, true),
/// ]));
///
/// let snapshot = Snapshot::builder_for(table_url).build(engine)?;
/// let transaction = overwrite_schema(snapshot, schema, vec![])
///     .build(engine, Box::new(FileSystemCommitter::new()))?;
///
/// // Write data and commit
/// transaction.commit(engine)?;
/// # Ok(())
/// # }
/// ```
pub fn overwrite_schema(
    snapshot: impl Into<SnapshotRef>,
    new_schema: SchemaRef,
    partition_columns: Vec<String>,
) -> OverwriteSchemaTransactionBuilder {
    OverwriteSchemaTransactionBuilder::new(snapshot.into(), new_schema, partition_columns)
}

impl OverwriteSchemaTransaction {
    /// Create a new transaction for overwriting the schema of an existing table.
    ///
    /// This constructor takes pre-validated metadata and pre-collected files to remove.
    /// It is typically called via `OverwriteSchemaTransactionBuilder::build()` rather than
    /// directly.
    ///
    /// # Arguments
    ///
    /// * `read_snapshot` - The snapshot of the existing table
    /// * `committer` - The committer to use for the transaction
    /// * `new_metadata` - Pre-validated Metadata with the new schema
    /// * `remove_files` - Pre-collected files to remove (from scanning the table)
    /// * `clustering_columns` - Clustering columns from domain metadata (if any)
    pub(crate) fn try_new_overwrite_schema(
        read_snapshot: SnapshotRef,
        committer: Box<dyn Committer>,
        new_metadata: Metadata,
        remove_files: Vec<FilteredEngineData>,
        clustering_columns: Option<Vec<ColumnName>>,
    ) -> DeltaResult<Self> {
        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
            operation = "OVERWRITE",
        );

        Ok(Transaction {
            span,
            read_snapshot,
            committer,
            operation: Some("OVERWRITE".to_string()),
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata: remove_files,
            set_transactions: vec![],
            commit_timestamp: current_time_ms()?,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: vec![],
            user_domain_removals: vec![],
            data_change: true,
            is_blind_append: false,
            dv_matched_files: vec![],
            clustering_columns,
            override_metadata: Some(new_metadata),
            _state: PhantomData,
        })
    }
}
