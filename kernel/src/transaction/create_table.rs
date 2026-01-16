//! Create table transaction implementation (internal API).
//!
//! This module provides a type-safe API for creating Delta tables.
//! Use the [`create_table`] function to get a [`CreateTableTransactionBuilder`] that can be
//! configured with table properties and other options before building the [`Transaction`].
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
//!     .with_table_properties([("delta.enableChangeDataFeed", "true")])
//!     .build(engine, Box::new(FileSystemCommitter::new()))?
//!     .commit(engine)?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::actions::{
    Metadata, Protocol, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::committer::Committer;
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error, PRE_COMMIT_VERSION};

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
}

impl CreateTableTransactionBuilder {
    /// Creates a new CreateTableTransactionBuilder.
    ///
    /// This is typically called via [`create_table`] rather than directly.
    pub fn new(path: impl AsRef<str>, schema: SchemaRef, engine_info: impl Into<String>) -> Self {
        Self {
            path: path.as_ref().to_string(),
            schema,
            engine_info: engine_info.into(),
            table_properties: HashMap::new(),
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
    /// # use delta_kernel::transaction::create_table::create_table;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # fn example() -> delta_kernel::DeltaResult<()> {
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)])?);
    /// let builder = create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_table_properties([
    ///         ("delta.enableChangeDataFeed", "true"),
    ///         ("myapp.version", "1.0"),
    ///     ]);
    /// # Ok(())
    /// # }
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

        // Create Metadata action
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            (*self.schema).clone(),
            Vec::new(), // partition_columns - added with data layout support
            current_time_ms()?,
            self.table_properties,
        )?;

        // Create pre-commit snapshot from protocol/metadata
        let log_root = table_url.join("_delta_log/")?;
        let log_segment = LogSegment::for_pre_commit(log_root);
        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url, PRE_COMMIT_VERSION)?;

        // Create Transaction with pre-commit snapshot
        Transaction::try_new_create_table(
            Arc::new(Snapshot::new(log_segment, table_configuration)),
            self.engine_info,
            committer,
            None, // system_domain_metadata - not supported in base API
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
    }

    #[test]
    fn test_with_table_properties() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]));

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_table_properties([("key1", "value1")]);

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

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_table_properties([("key1", "value1")])
            .with_table_properties([("key2", "value2")]);

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
