//! Create table transaction types and entry point (internal API).
//!
//! This module defines the [`CreateTableTransaction`] type alias and the [`create_table`]
//! entry point function. The builder logic lives in
//! [`builder::create_table`](super::builder::create_table).
//!
//! # Example
//!
//! ```rust,no_run
//! use delta_kernel::transaction::create_table::create_table;
//! use delta_kernel::schema::{StructType, StructField, DataType};
//! use delta_kernel::committer::FileSystemCommitter;
//! use std::sync::Arc;
//! # use delta_kernel::Engine;
//! # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
//!
//! let schema = Arc::new(StructType::try_new(vec![
//!     StructField::new("id", DataType::INTEGER, false),
//! ])?);
//!
//! let result = create_table("/path/to/table", schema, "MyApp/1.0")
//!     .with_table_properties([("myapp.version", "1.0")])
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

use crate::actions::DomainMetadata;
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::SchemaRef;
use crate::snapshot::SnapshotRef;
use crate::transaction::{CreateTable, Transaction};
use crate::utils::current_time_ms;
use crate::DeltaResult;

// Re-export the builder so callers can still access it from this module path.
pub use super::builder::create_table::CreateTableTransactionBuilder;

/// A type alias for create-table transactions.
///
/// This provides a restricted API surface that only exposes operations valid during table
/// creation. Operations like removing files, removing domain metadata, updating deletion
/// vectors, and setting blind append are not available at compile time.
///
/// # Operations NOT available on create-table transactions
///
/// - **`with_domain_metadata_removed()`** — Cannot remove domain metadata from a table
///   that doesn't exist yet.
/// - **`remove_files()`** — Cannot remove files from a table that has no files.
/// - **`with_blind_append()`** — Blind append semantics don't apply to table creation.
/// - **`update_deletion_vectors()`** — Deletion vectors require an existing table.
/// - **`with_transaction_id()`** — Transaction ID (app_id) tracking is for existing tables.
/// - **`with_operation()`** — The operation is fixed to `"CREATE TABLE"`.
///
/// # Example
///
/// ```rust,no_run
/// use delta_kernel::transaction::create_table::create_table;
/// use delta_kernel::schema::{StructType, StructField, DataType};
/// use delta_kernel::committer::FileSystemCommitter;
/// use std::sync::Arc;
/// # use delta_kernel::Engine;
/// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
///
/// let schema = Arc::new(StructType::try_new(vec![
///     StructField::new("id", DataType::INTEGER, false),
/// ])?);
///
/// let result = create_table("/path/to/table", schema, "MyApp/1.0")
///     .build(engine, Box::new(FileSystemCommitter::new()))?
///     .commit(engine)?;
/// # Ok(())
/// # }
/// ```
pub type CreateTableTransaction = Transaction<CreateTable>;

/// Creates a builder for creating a new Delta table.
///
/// This function returns a [`CreateTableTransactionBuilder`] that can be configured with table
/// properties and other options before building a [`CreateTableTransaction`].
///
/// # Arguments
///
/// * `path` - The file system path where the Delta table will be created
/// * `schema` - The schema for the new table
/// * `engine_info` - Information about the engine creating the table (e.g., "MyApp/1.0")
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use delta_kernel::transaction::create_table::create_table;
/// use delta_kernel::schema::{DataType, StructField, StructType};
/// use delta_kernel::committer::FileSystemCommitter;
/// use delta_kernel::engine::default::DefaultEngineBuilder;
/// use delta_kernel::engine::default::storage::store_from_url;
///
/// # fn main() -> delta_kernel::DeltaResult<()> {
/// let schema = Arc::new(StructType::new_unchecked(vec![
///     StructField::new("id", DataType::INTEGER, false),
///     StructField::new("name", DataType::STRING, true),
/// ]));
///
/// let url = url::Url::parse("file:///tmp/my_table")?;
/// let engine = DefaultEngineBuilder::new(store_from_url(&url)?).build();
///
/// let transaction = create_table("/tmp/my_table", schema, "MyApp/1.0")
///     .build(&engine, Box::new(FileSystemCommitter::new()))?;
///
/// // Commit the transaction to create the table
/// transaction.commit(&engine)?;
/// # Ok(())
/// # }
/// ```
pub fn create_table(
    path: impl AsRef<str>,
    schema: SchemaRef,
    engine_info: impl Into<String>,
) -> CreateTableTransactionBuilder {
    CreateTableTransactionBuilder::new(path, schema, engine_info)
}

impl CreateTableTransaction {
    /// Create a new transaction for creating a new table. This is used when the table doesn't
    /// exist yet and we need to create it with Protocol and Metadata actions.
    ///
    /// The `pre_commit_snapshot` is a synthetic snapshot created from the protocol and metadata
    /// that will be committed. It uses `PRE_COMMIT_VERSION` as a sentinel to indicate no
    /// version exists yet on disk.
    ///
    /// This is typically called via `CreateTableTransactionBuilder::build()` rather than directly.
    pub(crate) fn try_new_create_table(
        pre_commit_snapshot: SnapshotRef,
        engine_info: String,
        committer: Box<dyn Committer>,
        system_domain_metadata: Vec<DomainMetadata>,
        clustering_columns: Option<Vec<ColumnName>>,
    ) -> DeltaResult<Self> {
        // TODO(sanuj) Today transactions expect a read snapshot to be passed in and we pass
        // in the pre_commit_snapshot for CREATE. To support other operations such as ALTERs
        // there might be cleaner alternatives which can clearly disambiguate b/w a snapshot
        // the was read vs the effective snapshot we will use for the commit.
        let span = tracing::info_span!(
            "txn",
            path = %pre_commit_snapshot.table_root(),
            operation = "CREATE",
        );

        Ok(Transaction {
            span,
            read_snapshot: pre_commit_snapshot,
            committer,
            operation: Some("CREATE TABLE".to_string()),
            engine_info: Some(engine_info),
            add_files_metadata: vec![],
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp: current_time_ms()?,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: system_domain_metadata,
            user_domain_removals: vec![],
            data_change: true,
            is_blind_append: false,
            dv_matched_files: vec![],
            clustering_columns_logical: clustering_columns,
            _state: PhantomData,
        })
    }
}
