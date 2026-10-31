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
//! use std::sync::Arc;
//! # use delta_kernel::Engine;
//! # fn example(engine: &dyn Engine) -> delta_kernel::KernelResult<()> {
//!
//! let schema = Arc::new(StructType::try_new(vec![
//!     StructField::nullable("id", DataType::INTEGER),
//! ])?);
//!
//! let result = create_table("/path/to/table", schema, "MyApp/1.0", /* data_change */ true)
//!     .with_table_properties([("myapp.version", "1.0")])
//!     .build_with_filesystem_committer(engine)?
//!     .commit(engine)?;
//! # Ok(())
//! # }
//! ```

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

// Re-export the builder so callers can still access it from this module path.
pub use super::builder::create_table::CreateTableTransactionBuilder;
use crate::schema::SchemaRef;
use crate::transaction::{CreateTable, Imperative, Transaction};

/// A type alias for create-table transactions.
///
/// This provides a restricted API surface that only exposes operations valid during table
/// creation. Operations like removing files, removing domain metadata, and updating deletion
/// vectors are not available at compile time.
///
/// Create-table transactions always record `"CREATE TABLE"` as their operation.
/// Create-table builders do not support blind append.
///
/// # Operations NOT available on create-table transactions
///
/// - **`with_domain_metadata_removed()`** — Cannot remove domain metadata from a table that doesn't
///   exist yet.
/// - **`remove_files()`** — Cannot remove files from a table that has no files.
/// - **`update_deletion_vectors()`** — Deletion vectors require an existing table.
///
/// # Example
///
/// ```rust,no_run
/// use delta_kernel::transaction::create_table::create_table;
/// use delta_kernel::schema::{StructType, StructField, DataType};
/// use std::sync::Arc;
/// # use delta_kernel::Engine;
/// # fn example(engine: &dyn Engine) -> delta_kernel::KernelResult<()> {
///
/// let schema = Arc::new(StructType::try_new(vec![
///     StructField::nullable("id", DataType::INTEGER),
/// ])?);
///
/// let result = create_table("/path/to/table", schema, "MyApp/1.0", /* data_change */ true)
///     .build_with_filesystem_committer(engine)?
///     .commit(engine)?;
/// # Ok(())
/// # }
/// ```
pub type CreateTableTransaction<MODE = Imperative> = Transaction<CreateTable, MODE>;

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
/// * `data_change` - Whether added files change logical data. Use `true` when inserting initial
///   rows, such as `CREATE TABLE AS SELECT`, and `false` for schema-only creation with no staged
///   files. The value cannot be changed later and is preserved even when no files are staged, so an
///   empty creation may also use `true`.
///
/// Engine info is required at construction:
///
/// ```compile_fail,E0061
/// # use delta_kernel::schema::SchemaRef;
/// # use delta_kernel::transaction::create_table::create_table;
/// fn configure(schema: SchemaRef) {
///     let _ = create_table("/path/to/table", schema);
/// }
/// ```
///
/// Data-change intent is also required:
///
/// ```compile_fail,E0061
/// # use delta_kernel::schema::SchemaRef;
/// # use delta_kernel::transaction::create_table::create_table;
/// fn configure(schema: SchemaRef) {
///     let _ = create_table("/path/to/table", schema, "my-app/1.0");
/// }
/// ```
///
/// ```compile_fail,E0599
/// # use delta_kernel::transaction::create_table::CreateTableTransactionBuilder;
/// fn configure(builder: CreateTableTransactionBuilder) {
///     let _ = builder.with_data_change(true);
/// }
/// ```
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use delta_kernel::transaction::create_table::create_table;
/// use delta_kernel::schema::{DataType, StructField, StructType};
/// use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
/// use test_utils::delta_kernel_default_engine::storage::store_from_url;
///
/// # fn main() -> delta_kernel::KernelResult<()> {
/// let schema = Arc::new(StructType::try_new([
///     StructField::nullable("id", DataType::INTEGER),
///     StructField::nullable("name", DataType::STRING),
/// ])?);
///
/// let url = url::Url::parse("file:///tmp/my_table")?;
/// let engine = DefaultEngineBuilder::new(store_from_url(&url)?).build();
///
/// let transaction = create_table("/tmp/my_table", schema, "MyApp/1.0", /* data_change */ true)
///     .build_with_filesystem_committer(&engine)?;
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
    data_change: bool,
) -> CreateTableTransactionBuilder {
    CreateTableTransactionBuilder::new(path, schema, engine_info, data_change)
}
