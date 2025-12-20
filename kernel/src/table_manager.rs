//! The `table_manager` module provides the entry point for creating and managing Delta tables.
//!
//! This module exposes the [`TableManager`] struct which provides static factory methods for
//! creating new Delta tables with a fluent builder API.

use crate::schema::SchemaRef;
use crate::transaction::create_table::CreateTableTransactionBuilder;

/// Entry point for creating and managing Delta tables.
///
/// `TableManager` provides static factory methods that return builders for configuring
/// and creating new Delta tables.
pub struct TableManager;

impl TableManager {
    /// Creates a builder for creating a new Delta table.
    ///
    /// This method returns a [`CreateTableTransactionBuilder`] that can be configured with table
    /// properties and other options before building the transaction.
    ///
    /// # Arguments
    ///
    /// * `path` - The file system path where the Delta table will be created
    /// * `schema` - The schema for the new table
    /// * `engine_info` - Information about the engine creating the table (e.g., "MyApp/1.0")
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use delta_kernel::table_manager::TableManager;
    /// use delta_kernel::schema::{StructType, StructField, DataType};
    /// use delta_kernel::committer::Committer;
    /// use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # fn example(engine: &dyn Engine, committer: Box<dyn Committer>) -> delta_kernel::DeltaResult<()> {
    ///
    /// let schema = Arc::new(StructType::try_new(vec![
    ///     StructField::new("id", DataType::INTEGER, false),
    ///     StructField::new("name", DataType::STRING, true),
    /// ])?);
    ///
    /// let create_txn = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .build(engine, committer)?;
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
}
