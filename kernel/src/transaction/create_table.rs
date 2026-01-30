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

use std::collections::HashMap;
use std::sync::Arc;

use url::Url;

use crate::committer::Committer;
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;
use crate::table_transformation::TransformationPipeline;
use crate::transaction::Transaction;
use crate::utils::try_parse_uri;
use crate::{DeltaResult, Engine, Error, StorageHandler, PRE_COMMIT_VERSION};

/// Ensures that no Delta table exists at the given path.
///
/// This function checks the `_delta_log` directory to determine if a table already exists.
/// It handles various storage backend behaviors gracefully:
/// - If the directory doesn't exist (FileNotFound), returns Ok (new table can be created)
/// - If the directory exists but is empty, returns Ok (new table can be created)
/// - If the directory contains files, returns an error (table already exists)
/// - For other errors (permissions, network), propagates the error
///
/// # Arguments
/// * `storage` - The storage handler to use for listing
/// * `delta_log_url` - URL to the `_delta_log` directory
/// * `table_path` - Original table path (for error messages)
fn ensure_table_does_not_exist(
    storage: &dyn StorageHandler,
    delta_log_url: &Url,
    table_path: &str,
) -> DeltaResult<()> {
    match storage.list_from(delta_log_url) {
        Ok(mut files) => {
            // files.next() returns Option<DeltaResult<FileMeta>>
            // - Some(Ok(_)) means a file exists -> table exists
            // - Some(Err(FileNotFound)) means path doesn't exist -> OK for new table
            // - Some(Err(other)) means real error -> propagate
            // - None means empty iterator -> OK for new table
            match files.next() {
                Some(Ok(_)) => Err(Error::generic(format!(
                    "Table already exists at path: {}",
                    table_path
                ))),
                Some(Err(Error::FileNotFound(_))) | None => {
                    // Path doesn't exist or empty - OK for new table
                    Ok(())
                }
                Some(Err(e)) => {
                    // Real error (permissions, network, etc.) - propagate
                    Err(e)
                }
            }
        }
        Err(Error::FileNotFound(_)) => {
            // Directory doesn't exist - this is expected for a new table.
            // The storage layer will create the full path (including _delta_log/)
            // when the commit writes the first log file via write_json_file().
            Ok(())
        }
        Err(e) => {
            // Real error - propagate
            Err(e)
        }
    }
}

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
    data_layout: super::data_layout::DataLayout,
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
            data_layout: super::data_layout::DataLayout::None,
        }
    }

    /// Sets table properties for the new Delta table.
    ///
    /// Custom application properties (those not starting with `delta.`) are always allowed.
    /// Delta properties (`delta.*`) are validated against an allow list during [`build()`].
    /// Feature flags (`delta.feature.*`) are not supported during CREATE TABLE.
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
    ///         ("myapp.version", "1.0"),
    ///         ("myapp.author", "test"),
    ///     ]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`build()`]: CreateTableTransactionBuilder::build
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

    /// Sets the data layout for the new Delta table.
    ///
    /// The data layout determines how data files are organized within the table:
    ///
    /// - [`DataLayout::None`]: No special organization (default)
    /// - [`DataLayout::Partitioned`]: Data files are organized by partition column values
    /// - [`DataLayout::Clustered`]: Data files are optimized for queries on clustering columns
    ///
    /// Note: Partitioning and clustering are mutually exclusive. A table can have one or the
    /// other, but not both.
    ///
    /// [`DataLayout::None`]: super::data_layout::DataLayout::None
    /// [`DataLayout::Partitioned`]: super::data_layout::DataLayout::Partitioned
    /// [`DataLayout::Clustered`]: super::data_layout::DataLayout::Clustered
    pub fn with_data_layout(mut self, layout: super::data_layout::DataLayout) -> Self {
        self.data_layout = layout;
        self
    }

    /// Builds a [`Transaction`] that can be committed to create the table.
    ///
    /// This method performs validation:
    /// - Checks that the table path is valid
    /// - Verifies the table doesn't already exist
    /// - Validates the schema is non-empty
    /// - Validates table properties against the allow list
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
    /// - Unsupported delta properties or feature flags are specified
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Transaction> {
        // Validate path
        let table_url = try_parse_uri(&self.path)?;
        // Check if table already exists by looking for _delta_log directory
        let delta_log_url = table_url.join("_delta_log/")?;
        let storage = engine.storage_handler();
        ensure_table_does_not_exist(storage.as_ref(), &delta_log_url, &self.path)?;

        // Validate schema is non-empty
        if self.schema.fields().len() == 0 {
            return Err(Error::generic("Schema cannot be empty"));
        }

        // Create initial config with bare protocol and all properties
        let config = TableProtocolMetadataConfig::new(
            (*self.schema).clone(),
            Vec::new(), // partition_columns - set by PartitioningTransform if needed
            self.table_properties.clone(),
        )?;

        // Apply transforms based on properties and data layout (enables features, validates, etc.)
        let output = TransformationPipeline::apply_transforms(
            config,
            &self.table_properties,
            &self.data_layout,
        )?;

        // Extract protocol and metadata from final config
        let protocol = output.config.protocol;
        let metadata = output.config.metadata;

        // Create pre-commit snapshot from protocol/metadata
        let log_root = table_url.join("_delta_log/")?;
        let log_segment = LogSegment::for_pre_commit(log_root);
        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url, PRE_COMMIT_VERSION)?;

        // Create Transaction with pre-commit snapshot
        // Domain metadata from transforms (e.g., delta.clustering) is passed as system domain metadata
        Transaction::try_new_create_table(
            Arc::new(Snapshot::new(log_segment, table_configuration)),
            self.engine_info,
            committer,
            output.domain_metadata, // domain metadata from transforms (e.g., clustering)
        )
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]))
    }

    #[test]
    fn test_basic_builder_creation() {
        let schema = test_schema();
        let builder =
            CreateTableTransactionBuilder::new("/path/to/table", schema.clone(), "TestApp/1.0");

        assert_eq!(builder.path, "/path/to/table");
        assert_eq!(builder.engine_info, "TestApp/1.0");
        assert!(builder.table_properties.is_empty());
    }

    #[test]
    fn test_nested_path_builder_creation() {
        let schema = test_schema();
        let builder = CreateTableTransactionBuilder::new(
            "/path/to/table/nested",
            schema.clone(),
            "TestApp/1.0",
        );

        assert_eq!(builder.path, "/path/to/table/nested");
    }

    #[test]
    fn test_with_table_properties() {
        let schema = test_schema();

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_table_properties([("key1", "value1")]);

        assert_eq!(
            builder.table_properties.get("key1"),
            Some(&"value1".to_string())
        );
    }

    #[test]
    fn test_with_multiple_table_properties() {
        let schema = test_schema();

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

    #[test]
    fn test_table_creation_config_parsing() {
        let schema = StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]);

        // Empty properties are allowed - protocol is created with default features
        let properties = HashMap::new();
        let config =
            TableProtocolMetadataConfig::new(schema.clone(), vec![], properties).unwrap();
        assert!(config.metadata.configuration().is_empty());
        // Protocol always has features (even if empty) at version 3/7
        assert_eq!(config.protocol.min_reader_version(), 3);
        assert_eq!(config.protocol.min_writer_version(), 7);
        assert!(config.protocol.reader_features().unwrap().is_empty());
        assert!(config.protocol.writer_features().unwrap().is_empty());

        // User/application properties are passed through
        let properties = HashMap::from([
            ("myapp.version".to_string(), "1.0".to_string()),
            ("custom.setting".to_string(), "value".to_string()),
        ]);
        let config =
            TableProtocolMetadataConfig::new(schema.clone(), vec![], properties).unwrap();
        assert_eq!(
            config.metadata.configuration().get("myapp.version"),
            Some(&"1.0".to_string())
        );
        assert_eq!(
            config.metadata.configuration().get("custom.setting"),
            Some(&"value".to_string())
        );

        // try_from creates bare protocol and passes all properties through
        let properties = HashMap::from([
            (MIN_READER_VERSION_PROP.to_string(), "3".to_string()),
            (MIN_WRITER_VERSION_PROP.to_string(), "7".to_string()),
            ("myapp.version".to_string(), "1.0".to_string()),
        ]);
        let config = TableProtocolMetadataConfig::new(schema, vec![], properties).unwrap();
        // Protocol is bare v3/v7 (transforms would process version signals)
        assert_eq!(config.protocol.min_reader_version(), 3);
        assert_eq!(config.protocol.min_writer_version(), 7);
        // All properties pass through in try_from (including version props)
        assert!(config
            .metadata
            .configuration()
            .contains_key(MIN_READER_VERSION_PROP));
        assert!(config
            .metadata
            .configuration()
            .contains_key(MIN_WRITER_VERSION_PROP));
        assert_eq!(
            config.metadata.configuration().get("myapp.version"),
            Some(&"1.0".to_string())
        );
    }

    // Note: Protocol version validation tests are pending implementation of
    // ProtocolVersionTransform. Once implemented, add tests here for:
    // - Valid versions (3, 7) succeed
    // - Invalid reader version (not 3) fails
    // - Invalid writer version (not 7) fails
    // - Non-integer versions fail
}
