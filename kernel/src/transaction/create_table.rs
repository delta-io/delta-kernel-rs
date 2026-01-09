//! Create table transaction support.
//!
//! This module provides the [`CreateTableTransactionBuilder`] for creating new Delta tables
//! with optional features like partitioning, table properties, and table features.
//!
//! # Example
//!
//! ```rust,no_run
//! # use delta_kernel::DeltaResult;
//! # fn main() -> DeltaResult<()> {
//! use delta_kernel::table_manager::TableManager;
//! use delta_kernel::schema::{StructType, DataType, StructField};
//! use delta_kernel::committer::FileSystemCommitter;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! let schema = Arc::new(StructType::try_new(vec![
//!     StructField::new("id", DataType::INTEGER, false),
//! ])?);
//!
//! // let result = TableManager::create_table("/path/to/table", schema, "MyApp/1.0")
//! //     .with_table_properties(HashMap::from([
//! //         ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
//! //     ]))
//! //     .build(engine, Box::new(FileSystemCommitter::new()))?
//! //     .commit(engine)?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use url::Url;

use crate::actions::{
    DomainMetadata, Metadata, Protocol, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::clustering::ClusteringMetadataDomain;
use crate::committer::Committer;
use crate::schema::{
    validate_partition_columns, validate_schema_for_create, ColumnName, SchemaRef,
};
use crate::table_features::{
    assign_column_mapping_metadata, extract_table_configuration,
    get_column_mapping_mode_from_properties, ColumnMappingMode, ExtractedTableConfiguration,
    TableFeature, COLUMN_MAPPING_MAX_COLUMN_ID_KEY,
};
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

/// Result of processing column mapping configuration.
struct ProcessedColumnMapping {
    /// Updated schema with column mapping metadata (ID and physical name for each field).
    schema: SchemaRef,
    /// Updated properties including delta.columnMapping.maxColumnId.
    properties: HashMap<String, String>,
    /// Additional reader features required (columnMapping if mode is name or id).
    additional_reader_features: Vec<String>,
    /// Additional writer features required (columnMapping if mode is name or id).
    additional_writer_features: Vec<String>,
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
    data_layout: DataLayout,
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

    /// Processes column mapping configuration and updates schema/properties if needed.
    ///
    /// When column mapping mode is `name` or `id`, this function:
    /// 1. Assigns unique column IDs to each field
    /// 2. Assigns unique physical names (format: `col-{uuid}`) to each field
    /// 3. Updates the `delta.columnMapping.maxColumnId` table property
    /// 4. Adds `columnMapping` to the required features list
    ///
    /// # Arguments
    ///
    /// * `schema` - The table schema
    /// * `properties` - The table properties (must include `delta.columnMapping.mode` if enabled)
    ///
    /// # Returns
    ///
    /// A [`ProcessedColumnMapping`] with the updated schema, properties, and required features.
    fn process_column_mapping(
        schema: &SchemaRef,
        properties: &HashMap<String, String>,
    ) -> DeltaResult<ProcessedColumnMapping> {
        let mode = get_column_mapping_mode_from_properties(properties)?;

        match mode {
            ColumnMappingMode::None => Ok(ProcessedColumnMapping {
                schema: schema.clone(),
                properties: properties.clone(),
                additional_reader_features: vec![],
                additional_writer_features: vec![],
            }),
            ColumnMappingMode::Name | ColumnMappingMode::Id => {
                // For CREATE TABLE, start from 0. The maxColumnId property is an internal
                // property managed by the kernel, not user-specified.
                // assign_column_mapping_metadata will assign sequential IDs and track
                // the max ID, which we then store in the table properties.
                let mut max_id = 0i64;

                // Assign column mapping metadata to all fields
                let updated_schema = assign_column_mapping_metadata(schema, &mut max_id)?;

                // Update properties with maxColumnId
                let mut updated_props = properties.clone();
                updated_props.insert(
                    COLUMN_MAPPING_MAX_COLUMN_ID_KEY.to_string(),
                    max_id.to_string(),
                );

                // columnMapping is a ReaderWriter feature
                let feature_name = TableFeature::ColumnMapping.to_string();

                Ok(ProcessedColumnMapping {
                    schema: Arc::new(updated_schema),
                    properties: updated_props,
                    additional_reader_features: vec![feature_name.clone()],
                    additional_writer_features: vec![feature_name],
                })
            }
        }
    }

    /// Applies column mapping processing to the schema and updates extracted table configuration.
    ///
    /// This method:
    /// 1. Processes column mapping (assigns IDs and physical names if enabled)
    /// 2. Updates extracted configuration with column mapping reader/writer features
    /// 3. Updates properties with `delta.columnMapping.maxColumnId`
    ///
    /// # Arguments
    ///
    /// * `schema` - The table schema
    /// * `extracted` - The extracted table configuration (will be updated with column mapping results)
    ///
    /// # Returns
    ///
    /// A tuple of (updated schema, column mapping mode) for use in subsequent processing.
    fn apply_column_mapping(
        schema: &SchemaRef,
        extracted: &mut ExtractedTableConfiguration,
    ) -> DeltaResult<(SchemaRef, ColumnMappingMode)> {
        // Process column mapping (assigns IDs and physical names if enabled)
        let column_mapping = Self::process_column_mapping(schema, &extracted.cleaned_properties)?;

        // Get the column mapping mode for use in data layout processing
        let column_mapping_mode =
            get_column_mapping_mode_from_properties(&column_mapping.properties)?;

        // Update properties with column mapping results (includes maxColumnId if CM enabled)
        extracted.cleaned_properties = column_mapping.properties;

        // Add column mapping features if enabled
        let cm_feature = TableFeature::ColumnMapping;
        for feature_name in column_mapping.additional_reader_features {
            // Only add columnMapping feature if it's not already present
            if feature_name == cm_feature.to_string()
                && !extracted.reader_features.contains(&cm_feature)
            {
                extracted.reader_features.push(cm_feature.clone());
            }
        }
        for feature_name in column_mapping.additional_writer_features {
            if feature_name == cm_feature.to_string()
                && !extracted.writer_features.contains(&cm_feature)
            {
                extracted.writer_features.push(cm_feature.clone());
            }
        }

        Ok((column_mapping.schema, column_mapping_mode))
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
    /// * `schema` - The table schema (for validation, must have column mapping metadata if CM enabled)
    /// * `column_mapping_mode` - The column mapping mode for the table
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
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<ProcessedDataLayout> {
        match data_layout {
            DataLayout::None => Ok(ProcessedDataLayout {
                partition_columns: vec![],
                clustering_domain_metadata: None,
                additional_writer_features: vec![],
            }),
            DataLayout::Partitioned(cols) => {
                // Convert ColumnName to String for Metadata compatibility
                let partition_columns: Vec<String> = cols.iter().map(|c| c.to_string()).collect();

                // Validate partition columns exist and have valid types
                validate_partition_columns(schema, &partition_columns)?;

                Ok(ProcessedDataLayout {
                    partition_columns,
                    clustering_domain_metadata: None,
                    additional_writer_features: vec![],
                })
            }
            DataLayout::Clustered(cols) => {
                // Validate clustering columns
                Self::validate_clustering_columns(cols, schema)?;

                // Create clustering domain metadata
                // When column mapping is enabled, we need to store physical column names
                let clustering_metadata = ClusteringMetadataDomain::new_with_physical_names(
                    cols,
                    schema,
                    column_mapping_mode,
                )?;
                let domain_metadata = clustering_metadata.to_domain_metadata()?;

                // Clustering requires these features
                let additional_features =
                    vec![TableFeature::ClusteredTable, TableFeature::DomainMetadata];

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

        // Determine column mapping mode early for schema validation
        let column_mapping_mode = get_column_mapping_mode_from_properties(&table_properties)?;

        // Validate schema: duplicate columns, valid column names
        validate_schema_for_create(&self.schema, column_mapping_mode)?;

        // Ensure no table exists at the path
        Self::ensure_table_does_not_exist(&table_url, &self.path, engine)?;

        // Extract table configuration (features + cleaned properties)
        let mut extracted = extract_table_configuration(table_properties)?;

        // Process column mapping FIRST (before data layout, as clustering needs physical names)
        let (schema_with_cm, column_mapping_mode) =
            Self::apply_column_mapping(&self.schema, &mut extracted)?;

        // Process data layout (partitioning or clustering) using the updated schema
        let layout =
            Self::process_data_layout(&self.data_layout, &schema_with_cm, column_mapping_mode)?;

        // Add any features required by the data layout
        for feature in layout.additional_writer_features {
            if !extracted.writer_features.contains(&feature) {
                extracted.writer_features.push(feature);
            }
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

        // Create Metadata action with the updated schema (with column mapping metadata if enabled)
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            (*schema_with_cm).clone(),
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

    // ==================== Schema Validation Tests ====================

    #[test]
    fn test_rejects_duplicate_column_names_case_insensitive() {
        // Create schema with case-insensitive duplicates
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("Name", DataType::STRING, false),
            StructField::new("name", DataType::STRING, true),
        ]));

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn test_rejects_duplicate_nested_column_names() {
        // Create schema with duplicate nested paths
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new(
                "outer",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "Inner",
                    DataType::STRING,
                    true,
                )]))),
                true,
            ),
            StructField::new(
                "Outer",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "inner",
                    DataType::STRING,
                    true,
                )]))),
                true,
            ),
        ]));

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn test_rejects_invalid_column_name_without_column_mapping() {
        // Create schema with column name containing space (invalid for Parquet)
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "my column",
            DataType::STRING,
            false,
        )]));

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid character"));
    }

    #[test]
    fn test_accepts_special_column_name_with_column_mapping() {
        // Create schema with column name containing space (valid with column mapping)
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "my column",
            DataType::STRING,
            false,
        )]));

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let mut props = HashMap::new();
        props.insert("delta.columnMapping.mode".to_string(), "name".to_string());

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        // Should succeed - spaces are allowed with column mapping
        assert!(result.is_ok());
    }

    #[test]
    fn test_rejects_newline_in_column_name_even_with_column_mapping() {
        // Create schema with column name containing newline (always invalid)
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "my\ncolumn",
            DataType::STRING,
            false,
        )]));

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let mut props = HashMap::new();
        props.insert("delta.columnMapping.mode".to_string(), "name".to_string());

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_table_properties(props)
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("newline") || err.contains("\\n"));
    }

    #[test]
    fn test_rejects_partition_column_not_found() {
        let schema = test_schema_with_columns();

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::partitioned(["nonexistent"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"));
    }

    #[test]
    fn test_rejects_partition_column_complex_type() {
        use crate::schema::ArrayType;

        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
                true,
            ),
        ]));

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::partitioned(["tags"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsupported type"));
    }

    #[test]
    fn test_accepts_valid_partition_column() {
        let schema = test_schema_with_columns();

        let engine = SyncEngine::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let result = CreateTableTransactionBuilder::new(table_path, schema, "TestApp/1.0")
            .with_data_layout(DataLayout::partitioned(["name"]).unwrap())
            .build(&engine, Box::new(FileSystemCommitter::new()));

        assert!(result.is_ok());
    }
}
