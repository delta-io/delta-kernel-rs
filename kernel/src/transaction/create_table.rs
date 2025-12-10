//! Create table transaction implementation.
//!
//! This module provides a type-safe API for creating Delta tables.
//! Use the [`create_table`] function to get a [`CreateTableTransactionBuilder`] that can be
//! configured with table properties and other options before building the [`Transaction`].
//!
//! # Example
//!
//! ```rust,no_run
//! use delta_kernel::transaction::create_table;
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
//!     .with_table_properties([("delta.enableChangeDataFeed", "true")])
//!     .build(engine, Box::new(FileSystemCommitter::new()))?
//!     .commit(engine)?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::actions::{
    Metadata, Protocol, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::committer::Committer;
use crate::schema::SchemaRef;
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error};

/// Creates a builder for creating a new Delta table.
///
/// This function returns a [`CreateTableTransactionBuilder`] that can be configured with table
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
/// use delta_kernel::transaction::create_table;
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
/// let create_txn = create_table("/path/to/table", schema, "MyApp/1.0")
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

/// Builder for configuring a new Delta table.
///
/// Use this to configure table properties before building a [`Transaction`].
/// If the table build fails, no transaction will be created.
///
/// Created via [`create_table`].
pub struct CreateTableTransactionBuilder {
    path: String,
    schema: SchemaRef,
    engine_info: String,
    table_properties: HashMap<String, String>,
    partition_columns: Vec<String>,
}

impl CreateTableTransactionBuilder {
    /// Creates a new CreateTableTransactionBuilder.
    ///
    /// This is typically called via [`create_table`] rather than directly.
    pub(crate) fn new(
        path: impl AsRef<str>,
        schema: SchemaRef,
        engine_info: impl Into<String>,
    ) -> Self {
        Self {
            path: path.as_ref().to_string(),
            schema,
            engine_info: engine_info.into(),
            table_properties: HashMap::new(),
            partition_columns: Vec::new(),
        }
    }

    /// Sets table properties for the new Delta table.
    ///
    /// Table properties can include both Delta properties (e.g., `delta.enableChangeDataFeed`)
    /// and custom application properties.
    ///
    /// This method can be called multiple times. If a property key already exists from a
    /// previous call, the new value will overwrite the old one.
    ///
    /// # Arguments
    ///
    /// * `properties` - A map of table property names to their values
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use delta_kernel::transaction::create_table;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap());
    /// let builder = create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_table_properties([
    ///         ("delta.enableChangeDataFeed", "true"),
    ///         ("myapp.version", "1.0"),
    ///     ]);
    /// ```
    pub fn with_table_properties<I, K, V>(mut self, properties: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.table_properties
            .extend(properties.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Builds a [`Transaction`] that can be committed to create the table.
    ///
    /// This method performs validation:
    /// - Checks that the table path is valid
    /// - Verifies the table doesn't already exist
    /// - Validates the schema is non-empty
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine instance to use for validation
    /// * `committer` - The committer to use for the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table path is invalid
    /// - A table already exists at the given path
    /// - The schema is empty
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use delta_kernel::transaction::create_table;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use delta_kernel::committer::FileSystemCommitter;
    /// # use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap());
    /// let txn = create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .build(engine, Box::new(FileSystemCommitter::new()))?;
    ///
    /// // Commit the transaction to create the table
    /// let result = txn.commit(engine)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Transaction> {
        // Validate path
        let table_url = try_parse_uri(&self.path)?;

        // Validate schema is non-empty
        if self.schema.fields().len() == 0 {
            return Err(Error::generic("Schema cannot be empty"));
        }

        // Check if table already exists by looking for _delta_log directory
        let delta_log_url = table_url.join("_delta_log/")?;
        let storage = engine.storage_handler();

        // Try to list the _delta_log directory - if it exists and has files, table exists
        match storage.list_from(&delta_log_url) {
            Ok(mut files) => {
                if files.next().is_some() {
                    return Err(Error::generic(format!(
                        "Table already exists at path: {}",
                        self.path
                    )));
                }
            }
            Err(_) => {
                // Directory doesn't exist, which is what we want for a new table
            }
        }

        // Create Protocol action with table features support
        let protocol = Protocol::try_new(
            TABLE_FEATURES_MIN_READER_VERSION,
            TABLE_FEATURES_MIN_WRITER_VERSION,
            Some(Vec::<String>::new()), // readerFeatures (empty for now)
            Some(Vec::<String>::new()), // writerFeatures (empty for now)
        )?;

        // Get current timestamp
        let created_time = current_time_ms()?;

        // Create Metadata action
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            (*self.schema).clone(),
            self.partition_columns,
            created_time,
            self.table_properties,
        )?;

        // Create Transaction with cached Protocol and Metadata
        Transaction::try_new_create_table(
            table_url,
            protocol,
            metadata,
            self.engine_info,
            committer,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};
    use std::sync::Arc;

    #[test]
    fn test_builder_creation() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]));

        let builder =
            CreateTableTransactionBuilder::new("/path/to/table", schema.clone(), "TestApp/1.0");

        assert_eq!(builder.path, "/path/to/table");
        assert_eq!(builder.engine_info, "TestApp/1.0");
        assert!(builder.table_properties.is_empty());
        assert!(builder.partition_columns.is_empty());
    }

    #[test]
    fn test_with_table_properties() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]));

        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_table_properties(props);

        assert_eq!(
            builder.table_properties.get("key1"),
            Some(&"value1".to_string())
        );
    }

    #[test]
    fn test_with_multiple_table_properties() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]));

        let mut props1 = HashMap::new();
        props1.insert("key1".to_string(), "value1".to_string());

        let mut props2 = HashMap::new();
        props2.insert("key2".to_string(), "value2".to_string());

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_table_properties(props1)
            .with_table_properties(props2);

        assert_eq!(
            builder.table_properties.get("key1"),
            Some(&"value1".to_string())
        );
        assert_eq!(
            builder.table_properties.get("key2"),
            Some(&"value2".to_string())
        );
    }
}
