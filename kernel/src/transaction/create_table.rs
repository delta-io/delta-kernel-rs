//! Create table transaction implementation (internal API).
//!
//! This module provides a type-safe API for creating Delta tables.
//! Use the [`create_table`] function to get a [`CreateTableTransactionBuilder`] that can be
//! configured before building the [`Transaction`].

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::committer::Committer;
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION};
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
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
        ensure_table_does_not_exist(storage.as_ref(), &delta_log_url, &self.path)?;

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

        // We validate that the table properties are empty. Supported
        // table properties for create table will be allowlisted in the future.
        assert!(
            metadata.configuration().is_empty(),
            "Base create table API does not support table properties"
        );
        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url, PRE_COMMIT_VERSION)?;

        // Create Transaction with pre-commit snapshot
        Transaction::try_new_create_table(
            Arc::new(Snapshot::new(log_segment, table_configuration)),
            self.engine_info,
            committer,
            vec![], // system_domain_metadata - not supported in base API
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};
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
}
