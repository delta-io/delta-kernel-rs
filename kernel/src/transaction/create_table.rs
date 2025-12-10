//! Create table transaction support.
//!
//! This module provides the [`CreateTableTransactionBuilder`] for creating new Delta tables
//! with optional features like partitioning, clustering, table properties, and table features.
//! Use the [`create_table`] function to get a builder that can be configured before building
//! the [`Transaction`].
//!
//! # Example
//!
//! ```rust,no_run
//! # use delta_kernel::DeltaResult;
//! # use delta_kernel::Engine;
//! # fn example(engine: &dyn Engine) -> DeltaResult<()> {
//! use delta_kernel::transaction::create_table;
//! use delta_kernel::schema::{StructType, DataType, StructField};
//! use delta_kernel::committer::FileSystemCommitter;
//! use std::sync::Arc;
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

use std::collections::HashMap;

use url::Url;

use crate::actions::{
    DomainMetadata, Metadata, Protocol, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::clustering::ClusteringMetadataDomain;
use crate::committer::Committer;
use crate::schema::{ColumnName, SchemaRef};
use crate::table_features::{extract_table_configuration, TableFeature};
use crate::transaction::data_layout::{DataLayout, MAX_CLUSTERING_COLUMNS};
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error};

/// Table property key for specifying the minimum reader protocol version.
const MIN_READER_VERSION_PROP: &str = "delta.minReaderVersion";
/// Table property key for specifying the minimum writer protocol version.
const MIN_WRITER_VERSION_PROP: &str = "delta.minWriterVersion";

/// Result of processing the data layout specification.
struct ProcessedDataLayout {
    /// Partition columns for the table (empty if not partitioned).
    partition_columns: Vec<String>,
    /// Domain metadata for clustering (None if not clustered).
    clustering_domain_metadata: Option<DomainMetadata>,
    /// Additional writer features required by the layout.
    additional_writer_features: Vec<TableFeature>,
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
    data_layout: DataLayout,
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
            data_layout: DataLayout::None,
        }
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
    /// # Arguments
    ///
    /// * `layout` - The data layout specification
    ///
    /// # Example
    ///
    /// ## Partitioned Table
    /// ```rust,no_run
    /// # use delta_kernel::table_manager::TableManager;
    /// # use delta_kernel::transaction::DataLayout;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # let schema = Arc::new(StructType::try_new(vec![
    /// #     StructField::new("id", DataType::INTEGER, false),
    /// #     StructField::new("date", DataType::STRING, false),
    /// # ]).unwrap());
    /// let builder = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_data_layout(DataLayout::partitioned(["date"]).unwrap());
    /// ```
    ///
    /// ## Clustered Table
    /// ```rust,ignore
    /// # use delta_kernel::table_manager::TableManager;
    /// # use delta_kernel::transaction::DataLayout;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # let schema = Arc::new(StructType::try_new(vec![
    /// #     StructField::new("id", DataType::INTEGER, false),
    /// #     StructField::new("category", DataType::STRING, false),
    /// # ]).unwrap());
    /// let builder = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_data_layout(DataLayout::clustered(["category"])?);
    /// ```
    pub fn with_data_layout(mut self, layout: DataLayout) -> Self {
        self.data_layout = layout;
        self
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

    /// Validates protocol version properties and removes them from the properties map.
    ///
    /// Only protocol versions (3, 7) are supported. If `delta.minReaderVersion` or
    /// `delta.minWriterVersion` are provided with different values, an error is returned.
    /// These properties are stripped from the returned map as they are signal flags to
    /// update the protocol and not stored in the table's metadata configuration.
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
    /// - `delta.minReaderVersion` is provided and is not "3"
    /// - `delta.minWriterVersion` is provided and is not "7"
    /// - Either property value is not a valid integer
    fn validate_and_strip_protocol_properties(
        mut properties: HashMap<String, String>,
    ) -> DeltaResult<HashMap<String, String>> {
        // Helper to validate a protocol version property
        let validate_version = |key: &str, value: &str, expected: i32| -> DeltaResult<()> {
            let version: i32 = value.parse().map_err(|_| {
                Error::generic(format!("Invalid {}: '{}'. Must be an integer.", key, value))
            })?;
            if version != expected {
                return Err(Error::generic(format!(
                    "Invalid {}: {}. Only '{}' is supported.",
                    key, version, expected
                )));
            }
            Ok(())
        };

        // Validate reader version if provided
        if let Some(reader_version) = properties.get(MIN_READER_VERSION_PROP) {
            validate_version(
                MIN_READER_VERSION_PROP,
                reader_version,
                TABLE_FEATURES_MIN_READER_VERSION,
            )?;
        }

        // Validate writer version if provided
        if let Some(writer_version) = properties.get(MIN_WRITER_VERSION_PROP) {
            validate_version(
                MIN_WRITER_VERSION_PROP,
                writer_version,
                TABLE_FEATURES_MIN_WRITER_VERSION,
            )?;
        }

        // Strip protocol properties from the map (they're not stored in metadata)
        properties.remove(MIN_READER_VERSION_PROP);
        properties.remove(MIN_WRITER_VERSION_PROP);

        Ok(properties)
    }

    /// Ensures no table exists at the given path by checking for the _delta_log directory.
    ///
    /// # Errors
    ///
    /// Returns an error if a Delta table already exists at the path.
    fn ensure_table_does_not_exist(
        table_url: &Url,
        path: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        let delta_log_url = table_url.join("_delta_log/")?;
        let storage = engine.storage_handler();

        // Try to list the _delta_log directory - if it exists and has files, table exists
        match storage.list_from(&delta_log_url) {
            Ok(mut files) => {
                if files.next().is_some() {
                    return Err(Error::generic(format!(
                        "Table already exists at path: {}",
                        path
                    )));
                }
            }
            Err(_) => {
                // Directory doesn't exist, which is what we want for a new table
            }
        }

        Ok(())
    }

    /// Validates clustering columns against the schema and constraints.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - More than 4 clustering columns are specified
    /// - A clustering column doesn't exist in the schema
    fn validate_clustering_columns(columns: &[ColumnName], schema: &SchemaRef) -> DeltaResult<()> {
        use crate::schema::DataType;

        // Check maximum clustering columns
        if columns.len() > MAX_CLUSTERING_COLUMNS {
            return Err(Error::generic(format!(
                "Cannot specify more than {} clustering columns. Found {}.",
                MAX_CLUSTERING_COLUMNS,
                columns.len()
            )));
        }

        // Validate each column exists in the schema by traversing the path
        for col in columns {
            let path = col.path();
            if path.is_empty() {
                return Err(Error::generic("Clustering column path cannot be empty"));
            }

            // Traverse the schema tree to validate the path
            let mut current_schema = schema.as_ref();
            for (i, field_name) in path.iter().enumerate() {
                match current_schema.field(field_name) {
                    Some(field) => {
                        // If not the last element, we need to descend into a struct
                        if i < path.len() - 1 {
                            match field.data_type() {
                                DataType::Struct(inner) => {
                                    current_schema = inner;
                                }
                                _ => {
                                    return Err(Error::generic(format!(
                                        "Clustering column '{}': field '{}' is not a struct and cannot contain nested fields",
                                        col, field_name
                                    )));
                                }
                            }
                        }
                        // If it's the last element, we found the column - validation passes
                    }
                    None => {
                        return Err(Error::generic(format!(
                            "Clustering column '{}' not found in schema: field '{}' does not exist",
                            col, field_name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes the data layout specification and returns partition columns,
    /// clustering metadata, and any additional features required.
    ///
    /// # Arguments
    ///
    /// * `data_layout` - The data layout specification
    /// * `schema` - The table schema (for validation)
    ///
    /// # Returns
    ///
    /// A [`ProcessedDataLayout`] containing:
    /// - Partition columns (empty if not partitioned)
    /// - Clustering domain metadata (None if not clustered)
    /// - Additional writer features required by the layout
    fn process_data_layout(
        data_layout: &DataLayout,
        schema: &SchemaRef,
    ) -> DeltaResult<ProcessedDataLayout> {
        match data_layout {
            DataLayout::None => Ok(ProcessedDataLayout {
                partition_columns: vec![],
                clustering_domain_metadata: None,
                additional_writer_features: vec![],
            }),
            DataLayout::Partitioned(cols) => Ok(ProcessedDataLayout {
                // Convert ColumnName to String for Metadata compatibility
                partition_columns: cols.iter().map(|c| c.to_string()).collect(),
                clustering_domain_metadata: None,
                additional_writer_features: vec![],
            }),
            DataLayout::Clustered(cols) => {
                // Validate clustering columns
                Self::validate_clustering_columns(cols, schema)?;

                // Create clustering domain metadata
                let clustering_metadata = ClusteringMetadataDomain::new(cols);
                let domain_metadata = clustering_metadata.to_domain_metadata()?;

                // Clustering requires this feature (DomainMetadata is added in build())
                let additional_features = vec![TableFeature::ClusteredTable];

                Ok(ProcessedDataLayout {
                    partition_columns: vec![],
                    clustering_domain_metadata: Some(domain_metadata),
                    additional_writer_features: additional_features,
                })
            }
        }
    }

    /// Builds a [`Transaction`] that can be committed to create the table.
    ///
    /// This method performs validation:
    /// - Checks that the table path is valid
    /// - Verifies the table doesn't already exist
    /// - Validates the schema is non-empty
    /// - For clustered tables: validates clustering columns exist and adds required features
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
    /// - Clustering columns don't exist in schema or exceed the limit (4)
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

        // Validate and strip protocol version properties (before engine access for fail-fast)
        let table_properties = Self::validate_and_strip_protocol_properties(self.table_properties)?;

        // Ensure no table exists at the path
        Self::ensure_table_does_not_exist(&table_url, &self.path, engine)?;

        // Extract table configuration (features + cleaned properties)
        let mut extracted = extract_table_configuration(table_properties)?;

        // Process data layout (partitioning or clustering)
        let layout = Self::process_data_layout(&self.data_layout, &self.schema)?;

        // Add any features required by the data layout
        for feature in layout.additional_writer_features {
            if !extracted.writer_features.contains(&feature) {
                extracted.writer_features.push(feature);
            }
        }

        // Add domainMetadata feature if any domain metadata (system or user) is present
        // This centralizes the domain metadata feature logic in build() rather than
        // scattering it across different layout processors. Clustering for instance
        // needs the domain metadata feature to be added.
        let has_domain_metadata = layout.clustering_domain_metadata.is_some();
        if has_domain_metadata
            && !extracted
                .writer_features
                .contains(&TableFeature::DomainMetadata)
        {
            extracted.writer_features.push(TableFeature::DomainMetadata);
        }

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
            layout.partition_columns,
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
            layout.clustering_domain_metadata,
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

    fn test_schema_with_columns() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("value", DataType::LONG, true),
        ]))
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
        assert!(builder.data_layout.is_none());
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

    #[test]
    fn test_with_data_layout_partitioned() {
        let schema = test_schema_with_columns();

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_data_layout(DataLayout::partitioned(["name"]).unwrap());

        assert!(matches!(builder.data_layout, DataLayout::Partitioned(_)));
    }

    #[test]
    fn test_with_data_layout_clustered() {
        let schema = test_schema_with_columns();

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_data_layout(DataLayout::clustered(["name"]).unwrap());

        assert!(matches!(builder.data_layout, DataLayout::Clustered(_)));
    }

    #[test]
    fn test_clustering_too_many_columns() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        // Create a schema with 5 columns
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("a", DataType::STRING, false),
            StructField::new("b", DataType::STRING, false),
            StructField::new("c", DataType::STRING, false),
            StructField::new("d", DataType::STRING, false),
            StructField::new("e", DataType::STRING, false),
        ]));

        // Try to cluster on 5 columns (max is 4)
        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::clustered(["a", "b", "c", "d", "e"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Cannot specify more than 4 clustering columns"));
    }

    #[test]
    fn test_clustering_column_not_in_schema() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let schema = test_schema_with_columns();

        // Try to cluster on a non-existent column
        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::clustered(["nonexistent"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found in schema"));
    }

    #[test]
    fn test_clustering_nested_column() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        // Create a schema with nested struct
        let inner_struct = StructType::new_unchecked(vec![
            StructField::new("city", DataType::STRING, false),
            StructField::new("zip", DataType::STRING, false),
        ]);
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("address", DataType::Struct(Box::new(inner_struct)), false),
        ]));

        // Cluster on nested column using dot notation
        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::clustered(["address.city"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_ok());
    }

    #[test]
    fn test_clustering_invalid_nested_path() {
        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let schema = test_schema_with_columns();

        // Try to access nested field on a non-struct column
        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::clustered(["name.nested"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("is not a struct"));
    }
}
