//! Create table transaction implementation.
//!
//! This module provides a type-safe API for creating Delta tables.
//! [`CreateTableTransactionBuilder`] configures table properties and builds a [`Transaction`]
//! that can be committed to create the table.
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
//! let result = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
//!     .with_table_properties(HashMap::from([
//!         ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
//!     ]))
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
use crate::table_features::extract_feature_overrides;
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error};

/// Table property key for specifying the minimum reader protocol version.
const MIN_READER_VERSION_PROP: &str = "delta.minReaderVersion";
/// Table property key for specifying the minimum writer protocol version.
const MIN_WRITER_VERSION_PROP: &str = "delta.minWriterVersion";

/// Result of extracting table features from properties.
struct ExtractedFeatures {
    /// Features that require reader support (ReaderWriter features).
    reader_features: Vec<String>,
    /// Features that require writer support (all features).
    writer_features: Vec<String>,
    /// Properties with feature override entries removed.
    cleaned_properties: HashMap<String, String>,
}

/// Builder for configuring a new Delta table.
///
/// Use this to configure table properties before building a [`Transaction`].
/// If the table build fails, no transaction will be created.
///
/// Created via [`TableManager::create_table`](crate::table_manager::TableManager::create_table).
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

    /// Validates protocol version properties and removes them from the properties map.
    ///
    /// Only protocol versions (3, 7) are supported. If `delta.minReaderVersion` or
    /// `delta.minWriterVersion` are provided with different values, an error is returned.
    /// These properties are stripped from the returned map as they are not stored in
    /// the table's metadata configuration.
    ///
    /// # Arguments
    ///
    /// * `properties` - The table properties to validate
    ///
    /// # Returns
    ///
    /// The properties map with protocol version properties removed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `delta.minReaderVersion` is not `3`
    /// - `delta.minWriterVersion` is not `7`
    /// - Either property value is not a valid integer
    fn validate_and_strip_protocol_properties(
        mut properties: HashMap<String, String>,
    ) -> DeltaResult<HashMap<String, String>> {
        // Validate delta.minReaderVersion if provided
        if let Some(reader_version) = properties.get(MIN_READER_VERSION_PROP) {
            let parsed: i32 = reader_version.parse().map_err(|_| {
                Error::generic(format!(
                    "Invalid value '{}' for '{}'. Must be an integer.",
                    reader_version, MIN_READER_VERSION_PROP
                ))
            })?;
            if parsed != TABLE_FEATURES_MIN_READER_VERSION {
                return Err(Error::generic(format!(
                    "Invalid value '{}' for '{}'. Only '{}' is supported.",
                    parsed, MIN_READER_VERSION_PROP, TABLE_FEATURES_MIN_READER_VERSION
                )));
            }
        }

        // Validate delta.minWriterVersion if provided
        if let Some(writer_version) = properties.get(MIN_WRITER_VERSION_PROP) {
            let parsed: i32 = writer_version.parse().map_err(|_| {
                Error::generic(format!(
                    "Invalid value '{}' for '{}'. Must be an integer.",
                    writer_version, MIN_WRITER_VERSION_PROP
                ))
            })?;
            if parsed != TABLE_FEATURES_MIN_WRITER_VERSION {
                return Err(Error::generic(format!(
                    "Invalid value '{}' for '{}'. Only '{}' is supported.",
                    parsed, MIN_WRITER_VERSION_PROP, TABLE_FEATURES_MIN_WRITER_VERSION
                )));
            }
        }

        // Remove protocol version properties - they are not stored in metadata.configuration
        properties.remove(MIN_READER_VERSION_PROP);
        properties.remove(MIN_WRITER_VERSION_PROP);

        Ok(properties)
    }

    /// Extracts table features from properties and builds reader/writer feature lists.
    ///
    /// Processes `delta.feature.X = supported` properties to determine which table features
    /// should be enabled. Returns separate lists for reader and writer features, along with
    /// the cleaned properties (with feature override properties removed).
    ///
    /// # Arguments
    ///
    /// * `properties` - The table properties to process
    ///
    /// # Returns
    ///
    /// An [`ExtractedFeatures`] containing reader features, writer features, and cleaned properties.
    ///
    /// # Errors
    ///
    /// Returns an error if feature override properties have invalid values.
    fn extract_table_features(
        properties: HashMap<String, String>,
    ) -> DeltaResult<ExtractedFeatures> {
        let feature_overrides = extract_feature_overrides(properties)?;

        let mut reader_features: Vec<String> = Vec::new();
        let mut writer_features: Vec<String> = Vec::new();

        for feature in &feature_overrides.features {
            let feature_name = feature.to_string();

            // All features go into writer_features
            writer_features.push(feature_name.clone());

            // ReaderWriter features also go into reader_features
            if feature.is_reader_writer() {
                reader_features.push(feature_name);
            }
        }

        Ok(ExtractedFeatures {
            reader_features,
            writer_features,
            cleaned_properties: feature_overrides.cleaned_properties,
        })
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
    /// # use delta_kernel::table_manager::TableManager;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use delta_kernel::committer::FileSystemCommitter;
    /// # use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap());
    /// let txn = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
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

        // Validate and strip protocol version properties (before engine access for fail-fast)
        let table_properties = Self::validate_and_strip_protocol_properties(self.table_properties)?;

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

        // Extract feature overrides and build reader/writer feature lists
        let extracted = Self::extract_table_features(table_properties)?;

        // Create Protocol action with table features support
        let protocol = Protocol::try_new(
            TABLE_FEATURES_MIN_READER_VERSION,
            TABLE_FEATURES_MIN_WRITER_VERSION,
            Some(extracted.reader_features),
            Some(extracted.writer_features),
        )?;

        // Get current timestamp
        let created_time = current_time_ms()?;

        // Create Metadata action with cleaned properties (feature overrides removed)
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            (*self.schema).clone(),
            self.partition_columns,
            created_time,
            extracted.cleaned_properties,
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
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
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

    #[test]
    fn test_allows_valid_protocol_versions() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([
            (MIN_READER_VERSION_PROP.to_string(), "3".to_string()),
            (MIN_WRITER_VERSION_PROP.to_string(), "7".to_string()),
        ]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        // Should succeed - (3, 7) is allowed
        assert!(result.is_ok());
    }

    #[test]
    fn test_rejects_invalid_reader_version() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([(MIN_READER_VERSION_PROP.to_string(), "2".to_string())]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("delta.minReaderVersion"));
        assert!(err.contains("Only '3' is supported"));
    }

    #[test]
    fn test_rejects_invalid_writer_version() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([(MIN_WRITER_VERSION_PROP.to_string(), "5".to_string())]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("delta.minWriterVersion"));
        assert!(err.contains("Only '7' is supported"));
    }

    #[test]
    fn test_rejects_non_integer_reader_version() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([(MIN_READER_VERSION_PROP.to_string(), "abc".to_string())]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Must be an integer"));
    }

    #[test]
    fn test_rejects_non_integer_writer_version() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([(MIN_WRITER_VERSION_PROP.to_string(), "xyz".to_string())]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Must be an integer"));
    }

    #[test]
    fn test_allows_only_reader_version() {
        // Providing only reader version (3) should succeed
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([(MIN_READER_VERSION_PROP.to_string(), "3".to_string())]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_ok());
    }

    #[test]
    fn test_allows_only_writer_version() {
        // Providing only writer version (7) should succeed
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let props = HashMap::from([(MIN_WRITER_VERSION_PROP.to_string(), "7".to_string())]);

        let result = CreateTableTransactionBuilder::new(table_path, test_schema(), "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_ok());
    }
}
