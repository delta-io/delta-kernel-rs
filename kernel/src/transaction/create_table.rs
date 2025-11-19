//! Create table transaction implementation using the typestate pattern.
//!
//! This module provides a type-safe API for creating Delta tables that enforces
//! the following progression:
//! 1. [`CreateTableBuilder`] - Configure table properties
//! 2. [`CreateTableTransaction`] - Commit metadata
//! 3. [`Transaction`] - Perform data operations (future)
//!
//! # Example
//!
//! ```rust,no_run
//! use delta_kernel::table_manager::TableManager;
//! use delta_kernel::schema::{StructType, StructField, DataType};
//! use delta_kernel::committer::FileSystemCommitter;
//! use std::sync::Arc;
//! use std::collections::HashMap;
//! # use delta_kernel::Engine;
//! # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
//!
//! let schema = Arc::new(StructType::try_new(vec![
//!     StructField::new("id", DataType::INTEGER, false),
//! ])?);

//!
//! let create_txn = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
//!     .with_table_properties(HashMap::from([
//!         ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
//!     ]))
//!     .build(engine)?;
//!
//! let txn = create_txn.commit_metadata(engine, Box::new(FileSystemCommitter::new()))?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::actions::{
    get_commit_schema, CommitInfo, Metadata, Protocol, COMMIT_INFO_NAME, METADATA_NAME,
    PROTOCOL_NAME, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::committer::{CommitMetadata, CommitResponse, Committer};
use crate::engine_data::FilteredEngineData;
use crate::path::LogRoot;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error, IntoEngineData};

/// Builder for configuring a new Delta table.
/// This is the first stage in the typestate progression. Use this to configure
/// table properties before building a [`CreateTableTransaction`]. If the table
/// build fails the engine will not get a CreateTableTransaction instance to
/// perform a metadata commit on the table.
/// Created via [`TableManager::create_table`](crate::table_manager::TableManager::create_table).
pub struct CreateTableBuilder {
    path: String,
    schema: SchemaRef,
    engine_info: String,
    table_properties: HashMap<String, String>,
    partition_columns: Vec<String>,
}

impl CreateTableBuilder {
    /// Creates a new CreateTableBuilder.
    ///
    /// This is typically called via `TableManager::create_table()` rather than directly.
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
    /// # use delta_kernel::table_manager::TableManager;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # use std::collections::HashMap;
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap());
    /// let builder = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_table_properties(HashMap::from([
    ///         ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
    ///         ("myapp.version".to_string(), "1.0".to_string()),
    ///     ]));
    /// ```
    pub fn with_table_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.table_properties.extend(properties);
        self
    }

    /// Builds a [`CreateTableTransaction`] that can commit the table metadata.
    ///
    /// This method performs validation:
    /// - Checks that the table path is valid
    /// - Verifies the table doesn't already exist
    /// - Validates the schema is non-empty
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine instance to use for validation
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
    /// # use delta_kernel::table_manager::TableManager;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap());
    /// let create_txn = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .build(engine)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<CreateTableTransaction> {
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

        Ok(CreateTableTransaction {
            path: self.path,
            schema: self.schema,
            engine_info: self.engine_info,
            table_properties: self.table_properties,
            partition_columns: self.partition_columns,
        })
    }
}

/// Transaction for committing table metadata.
///
/// This is the second stage in the typestate progression. This struct can only commit
/// metadata; it cannot perform data operations. After committing metadata, it returns
/// a [`Transaction`] that can be used for data operations.
///
/// Created via [`CreateTableBuilder::build`].
#[derive(Debug)]
pub struct CreateTableTransaction {
    path: String,
    schema: SchemaRef,
    engine_info: String,
    table_properties: HashMap<String, String>,
    partition_columns: Vec<String>,
}

impl CreateTableTransaction {
    /// Commits the table metadata and returns a [`Transaction`] for data operations.
    ///
    /// This method:
    /// 1. Creates Protocol and Metadata actions
    /// 2. Writes them to `_delta_log/00000000000000000000.json`
    /// 3. Loads the resulting snapshot
    /// 4. Creates and returns a Transaction
    ///
    /// The returned Transaction can be used for future data operations (when implemented).
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine instance to use
    /// * `committer` - The committer to use for the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The metadata actions cannot be created
    /// - The commit file cannot be written
    /// - The snapshot cannot be loaded
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use delta_kernel::table_manager::TableManager;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use delta_kernel::committer::FileSystemCommitter;
    /// # use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap());
    /// let create_txn = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .build(engine)?;
    ///
    /// let txn = create_txn.commit_metadata(engine, Box::new(FileSystemCommitter::new()))?;
    /// // Future: txn.add_files(...); txn.commit(engine)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn commit_metadata(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Transaction> {
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

        // Create CommitInfo action
        let commit_info = CommitInfo::new(
            created_time,
            None, // in_commit_timestamp (not using in-commit timestamps)
            Some("CREATE TABLE".to_string()),
            Some(self.engine_info),
        );

        // Convert actions to EngineData by projecting the Protocol, Metadata, and CommitInfo
        // fields from the commit schema and using EngineData::into_engine_data
        let commit_schema = get_commit_schema();
        let protocol_schema = commit_schema.project(&[PROTOCOL_NAME])?;
        let metadata_schema = commit_schema.project(&[METADATA_NAME])?;
        let commit_info_schema = commit_schema.project(&[COMMIT_INFO_NAME])?;

        let protocol_data = protocol.into_engine_data(protocol_schema, engine)?;
        let metadata_data = metadata.into_engine_data(metadata_schema, engine)?;
        let commit_info_data = commit_info.into_engine_data(commit_info_schema, engine)?;

        // Create an iterator of actions as FilteredEngineData to be used by the
        // committer
        let actions = vec![
            Ok(FilteredEngineData::with_all_rows_selected(protocol_data)),
            Ok(FilteredEngineData::with_all_rows_selected(metadata_data)),
            Ok(FilteredEngineData::with_all_rows_selected(commit_info_data)),
        ];

        // Get table URL and create commit metadata
        let table_url = try_parse_uri(&self.path)?;
        let log_root = LogRoot::new(table_url.clone())?;
        let commit_metadata = CommitMetadata::new(log_root, 0, created_time);

        // Use the committer to write the initial commit. This ends up creating
        // the _delta_log/00000000000000000000.json file with the Protocol,
        // Metadata, and CommitInfo actions.
        match committer.commit(engine, Box::new(actions.into_iter()), commit_metadata)? {
            CommitResponse::Committed { version: _ } => {
                // Load the newly created table as a snapshot
                let snapshot = Snapshot::builder_for(table_url).build(engine)?;

                // Create a transaction from the snapshot
                snapshot.transaction(committer)
            }
            CommitResponse::Conflict { version } => Err(Error::generic(format!(
                "Unexpected conflict while creating table at version {}",
                version
            ))),
        }
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

        let builder = CreateTableBuilder::new("/path/to/table", schema.clone(), "TestApp/1.0");

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

        let builder = CreateTableBuilder::new("/path/to/table", schema, "TestApp/1.0")
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

        let builder = CreateTableBuilder::new("/path/to/table", schema, "TestApp/1.0")
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
