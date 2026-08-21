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
//!     StructField::nullable("id", DataType::INTEGER),
//! ])?);
//!
//! let result = create_table("/path/to/table", schema, "MyApp/1.0")
//!     .with_table_properties([("myapp.version", "1.0")])
//!     .build(engine)?
//!     .commit(engine, &FileSystemCommitter::new(), delta_kernel::transaction::CommitActions::new())?;
//! # Ok(())
//! # }
//! ```

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

// Re-export the builder so callers can still access it from this module path.
pub use super::builder::create_table::CreateTableTransactionBuilder;
use crate::actions::DomainMetadata;
use crate::expressions::ColumnName;
use crate::schema::SchemaRef;
use crate::table_configuration::TableConfiguration;
use crate::transaction::{CreateTable, Operation, Transaction, TransactionConfig, TransactionInit};
use crate::DeltaResult;

/// A type alias for create-table transactions.
///
/// This provides a restricted API surface that only exposes operations valid during table
/// creation. Operations like removing files, removing domain metadata, updating deletion
/// vectors, and setting blind append are not available at compile time.
///
/// # Operations NOT available on create-table transactions
///
/// - **`with_domain_metadata_removed()`** — Cannot remove domain metadata from a table that doesn't
///   exist yet.
/// - **`remove_files()`** — Cannot remove files from a table that has no files.
/// - **`with_blind_append()`** — Blind append semantics don't apply to table creation.
/// - **`update_deletion_vectors()`** — Deletion vectors require an existing table.
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
///     StructField::nullable("id", DataType::INTEGER),
/// ])?);
///
/// let result = create_table("/path/to/table", schema, "MyApp/1.0")
///     .build(engine)?
///     .commit(engine, &FileSystemCommitter::new(), delta_kernel::transaction::CommitActions::new())?;
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
/// use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
/// use test_utils::delta_kernel_default_engine::storage::store_from_url;
///
/// # fn main() -> delta_kernel::DeltaResult<()> {
/// let schema = Arc::new(StructType::try_new([
///     StructField::nullable("id", DataType::INTEGER),
///     StructField::nullable("name", DataType::STRING),
/// ])?);
///
/// let url = url::Url::parse("file:///tmp/my_table")?;
/// let engine = DefaultEngineBuilder::new(store_from_url(&url)?).build();
///
/// let transaction = create_table("/tmp/my_table", schema, "MyApp/1.0")
///     .build(&engine)?;
///
/// // Commit the transaction to create the table
/// transaction.commit(&engine, &FileSystemCommitter::new(), delta_kernel::transaction::CommitActions::new())?;
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
    /// The `effective_table_config` is the table configuration that will be committed (protocol,
    /// metadata, schema).
    ///
    /// This is typically called via `CreateTableTransactionBuilder::build()` rather than directly.
    pub(crate) fn try_new_create_table(
        effective_table_config: TableConfiguration,
        system_domain_metadata: Vec<DomainMetadata>,
        clustering_columns: Option<Vec<ColumnName>>,
        config: TransactionConfig,
    ) -> DeltaResult<Self> {
        let span = tracing::info_span!(
            "txn",
            path = %effective_table_config.table_root(),
            operation = "CREATE",
        );
        Transaction::try_from_init(TransactionInit::<CreateTable> {
            span,
            read_snapshot_opt: None,
            effective_table_config,
            should_emit_protocol: true,
            should_emit_metadata: true,
            operation: Some(Operation::CreateTable),
            config,
            system_domain_metadata_additions: system_domain_metadata,
            user_domain_removals: vec![],
            is_blind_append: false,
            physical_clustering_columns: clustering_columns,
            _state: std::marker::PhantomData,
        })
    }
}
