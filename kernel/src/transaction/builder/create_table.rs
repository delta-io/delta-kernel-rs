//! Builder for creating new Delta tables.
//!
//! This module contains [`CreateTableTransactionBuilder`], which validates and constructs a
//! [`CreateTableTransaction`] from user-provided schema, properties, and data layout options.
//!
//! Use [`create_table()`](super::super::create_table::create_table) as the entry point rather
//! than constructing the builder directly.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use url::Url;

use crate::actions::{DomainMetadata, Metadata, Protocol};
use crate::clustering::{create_clustering_domain_metadata, validate_clustering_columns};
use crate::committer::Committer;
use crate::crc::LazyCrc;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::schema::variant_utils::UsesVariant;
use crate::schema::{SchemaRef, SchemaTransform};
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    assign_column_mapping_metadata, get_any_level_column_physical_name,
    get_column_mapping_mode_from_properties, ColumnMappingMode, FeatureType, TableFeature,
    SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
};
use crate::table_properties::{
    COLUMN_MAPPING_MAX_COLUMN_ID, COLUMN_MAPPING_MODE, DELTA_PROPERTY_PREFIX,
};
use crate::transaction::create_table::CreateTableTransaction;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error, StorageHandler, PRE_COMMIT_VERSION};

/// Table features allowed to be enabled via `delta.feature.*=supported` during CREATE TABLE.
///
/// Feature signals (`delta.feature.X=supported`) are validated against this list.
/// Only features in this list can be enabled via feature signals.
const ALLOWED_DELTA_FEATURES: &[TableFeature] = &[
    // DomainMetadata is required for clustering and other system domain operations
    TableFeature::DomainMetadata,
    // ColumnMapping enables column mapping (name/id mode)
    TableFeature::ColumnMapping,
    // Note: Clustering is NOT included here. Users should not enable clustering via
    // `delta.feature.clustering = supported`. Instead, clustering is enabled by
    // specifying clustering columns via `with_data_layout()`.
    // As features are supported, add them here:
    // TableFeature::DeletionVectors,
    TableFeature::V2Checkpoint,
];

/// Delta properties allowed to be set during CREATE TABLE.
///
/// This list will expand as more features are supported.
/// The allow list will be deprecated once auto feature enablement is implemented
/// like the Java Kernel.
const ALLOWED_DELTA_PROPERTIES: &[&str] = &[
    // ColumnMapping mode property: triggers column mapping transform
    COLUMN_MAPPING_MODE,
    // As features are supported, add them here:
    // "delta.enableDeletionVectors",
];

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

/// Result of validating and transforming table properties.
struct ValidatedTableProperties {
    /// Table properties with feature signals removed (to be stored in metadata)
    properties: HashMap<String, String>,
    /// Reader features extracted from feature signals (for ReaderWriter features)
    reader_features: Vec<TableFeature>,
    /// Writer features extracted from feature signals (for all features)
    writer_features: Vec<TableFeature>,
}

/// Adds a feature to the appropriate reader/writer feature lists based on its type.
///
/// - ReaderWriter features are added to both reader and writer lists
/// - Writer and Unknown features are added only to the writer list
///
/// This function is idempotent - it won't add duplicate features.
fn add_feature_to_lists(
    feature: TableFeature,
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
) {
    match feature.feature_type() {
        FeatureType::ReaderWriter => {
            if !reader_features.contains(&feature) {
                reader_features.push(feature.clone());
            }
            if !writer_features.contains(&feature) {
                writer_features.push(feature);
            }
        }
        FeatureType::WriterOnly | FeatureType::Unknown => {
            if !writer_features.contains(&feature) {
                writer_features.push(feature);
            }
        }
    }
}

/// Configures clustering support for table creation (used by unit tests).
///
/// Validates clustering columns, adds required features (DomainMetadata, ClusteredTable),
/// and creates the domain metadata action.
fn apply_clustering_for_table_create(
    logical_schema: &SchemaRef,
    logical_columns: &[ColumnName],
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
) -> DeltaResult<DomainMetadata> {
    validate_clustering_columns(logical_schema, logical_columns)?;

    // Add required features
    add_feature_to_lists(
        TableFeature::DomainMetadata,
        reader_features,
        writer_features,
    );
    add_feature_to_lists(
        TableFeature::ClusteredTable,
        reader_features,
        writer_features,
    );

    Ok(create_clustering_domain_metadata(logical_columns))
}

/// Conditionally enables clustering for table creation based on the data layout.
///
/// If clustering is specified in the data layout, this function:
/// 1. Validates clustering columns against the schema (using logical names)
/// 2. Resolves logical to physical column names (if column mapping is enabled)
/// 3. Adds DomainMetadata and ClusteredTable features to the protocol
/// 4. Creates the clustering domain metadata with physical column names
///
/// # Arguments
///
/// * `data_layout` - The data layout (may specify clustering columns)
/// * `effective_schema` - The schema to validate against
/// * `column_mapping_mode` - The column mapping mode (determines name resolution)
/// * `validated` - The validated table properties (features will be added)
///
/// # Returns
///
/// A tuple of (domain_metadata_list, clustering_columns_for_stats).
/// The clustering columns returned are logical names (for stats_columns).
fn maybe_enable_clustering(
    data_layout: &DataLayout,
    effective_schema: &SchemaRef,
    column_mapping_mode: ColumnMappingMode,
    validated: &mut ValidatedTableProperties,
) -> DeltaResult<(Vec<DomainMetadata>, Option<Vec<ColumnName>>)> {
    match data_layout {
        DataLayout::Clustered { columns } => {
            // Validate using logical names against the schema
            // (Schema field names are always logical, even with column mapping)
            validate_clustering_columns(effective_schema, columns)?;

            // Resolve logical to physical column names for domain metadata.
            // When column mapping is enabled, clustering stores physical names.
            // Supports both top-level and nested columns.
            let physical_columns: Vec<ColumnName> = columns
                .iter()
                .map(|c| {
                    get_any_level_column_physical_name(effective_schema, c, column_mapping_mode)
                })
                .try_collect()?;

            // Add required features
            add_feature_to_lists(
                TableFeature::DomainMetadata,
                &mut validated.reader_features,
                &mut validated.writer_features,
            );
            add_feature_to_lists(
                TableFeature::ClusteredTable,
                &mut validated.reader_features,
                &mut validated.writer_features,
            );

            // Create domain metadata with physical names
            let dm = create_clustering_domain_metadata(&physical_columns);

            // Return logical names for stats_columns
            Ok((vec![dm], Some(columns.clone())))
        }
        DataLayout::None => Ok((vec![], None)),
    }
}

/// Conditionally adds the `variantType` feature to the protocol when the schema contains
/// Variant columns. Uses the [`UsesVariant`] schema visitor to detect Variant data types
/// anywhere in the schema tree (top-level, nested structs, arrays, maps).
fn maybe_enable_variant_type(schema: &SchemaRef, validated: &mut ValidatedTableProperties) {
    let mut visitor = UsesVariant::default();
    let _ = visitor.transform_struct(schema);
    if visitor.found() {
        add_feature_to_lists(
            TableFeature::VariantType,
            &mut validated.reader_features,
            &mut validated.writer_features,
        );
    }
}

/// Conditionally applies column mapping for table creation based on the mode in properties.
///
/// If `delta.columnMapping.mode` is set to `name` or `id`, this function:
/// 1. Adds the ColumnMapping feature to the protocol
/// 2. Transforms the schema to assign IDs and physical names to all fields
/// 3. Sets `delta.columnMapping.maxColumnId` in properties
/// 4. Returns the transformed schema
///
/// If mode is `none` or not set, returns the original schema unchanged.
///
/// # Arguments
///
/// * `schema` - The table schema to potentially transform
/// * `validated` - The validated table properties (may be modified to add maxColumnId)
///
/// # Returns
///
/// A tuple of (effective_schema, column_mapping_mode).
fn maybe_apply_column_mapping_for_table_create(
    schema: &SchemaRef,
    validated: &mut ValidatedTableProperties,
) -> DeltaResult<(SchemaRef, ColumnMappingMode)> {
    let column_mapping_mode = get_column_mapping_mode_from_properties(&validated.properties)?;

    let effective_schema = match column_mapping_mode {
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            // Add ColumnMapping feature to protocol (it's a ReaderWriter feature)
            add_feature_to_lists(
                TableFeature::ColumnMapping,
                &mut validated.reader_features,
                &mut validated.writer_features,
            );

            // Transform schema: assign IDs and physical names to all fields
            let mut max_id = 0i64;
            let transformed_schema = assign_column_mapping_metadata(schema, &mut max_id)?;

            // Add maxColumnId to properties
            validated
                .properties
                .insert(COLUMN_MAPPING_MAX_COLUMN_ID.to_string(), max_id.to_string());

            Arc::new(transformed_schema)
        }
        ColumnMappingMode::None => schema.clone(),
    };

    Ok((effective_schema, column_mapping_mode))
}

/// Validates and transforms table properties for CREATE TABLE.
///
/// This function:
/// 1. Validates feature signals (`delta.feature.*`) against `ALLOWED_DELTA_FEATURES`
/// 2. Validates delta properties (`delta.*`) against `ALLOWED_DELTA_PROPERTIES`
/// 3. Removes feature signals from properties (they shouldn't be stored in metadata)
/// 4. Extracts reader/writer features from validated feature signals
///
/// Non-delta properties (user/application properties) are always allowed.
fn validate_extract_table_features_and_properties(
    properties: HashMap<String, String>,
) -> DeltaResult<ValidatedTableProperties> {
    let mut reader_features = Vec::new();
    let mut writer_features = Vec::new();

    // Partition properties into feature signals and regular properties
    // Feature signals (delta.feature.X=supported) are processed but not stored in metadata
    // Feature signals are removed from the properties map.
    let (feature_signals, properties): (HashMap<_, _>, HashMap<_, _>) = properties
        .into_iter()
        .partition(|(k, _)| k.starts_with(SET_TABLE_FEATURE_SUPPORTED_PREFIX));

    // Process and validate feature signals
    for (key, value) in &feature_signals {
        // Safe: we partitioned for keys starting with this prefix above
        let Some(feature_name) = key.strip_prefix(SET_TABLE_FEATURE_SUPPORTED_PREFIX) else {
            continue;
        };

        // Validate that the value is "supported"
        if value != SET_TABLE_FEATURE_SUPPORTED_VALUE {
            return Err(Error::generic(format!(
                "Invalid value '{}' for '{}'. Only '{}' is allowed.",
                value, key, SET_TABLE_FEATURE_SUPPORTED_VALUE
            )));
        }

        // Parse feature name to TableFeature (unknown features become TableFeature::Unknown)
        let feature: TableFeature = feature_name
            .parse()
            .unwrap_or_else(|_| TableFeature::Unknown(feature_name.to_string()));

        if !ALLOWED_DELTA_FEATURES.contains(&feature) {
            return Err(Error::generic(format!(
                "Enabling feature '{}' via '{}' is not supported during CREATE TABLE",
                feature_name, key
            )));
        }

        // Add to appropriate feature lists based on feature type
        add_feature_to_lists(feature, &mut reader_features, &mut writer_features);
    }

    // Validate remaining delta.* properties against allow list
    for key in properties.keys() {
        if key.starts_with(DELTA_PROPERTY_PREFIX)
            && !ALLOWED_DELTA_PROPERTIES.contains(&key.as_str())
        {
            return Err(Error::generic(format!(
                "Setting delta property '{}' is not supported during CREATE TABLE",
                key
            )));
        }
    }

    Ok(ValidatedTableProperties {
        properties,
        reader_features,
        writer_features,
    })
}

/// Builder for configuring a new Delta table.
///
/// Use this to configure table properties before building a [`CreateTableTransaction`].
/// If the table build fails, no transaction will be created.
///
/// Created via [`create_table()`](super::super::create_table::create_table).
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
    /// This is typically called via
    /// [`create_table()`](super::super::create_table::create_table) rather than directly.
    pub fn new(path: impl AsRef<str>, schema: SchemaRef, engine_info: impl Into<String>) -> Self {
        Self {
            path: path.as_ref().to_string(),
            schema,
            engine_info: engine_info.into(),
            table_properties: HashMap::new(),
            data_layout: DataLayout::None,
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
    /// - [`DataLayout::Clustered`]: Data files are optimized for queries on clustering columns
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use delta_kernel::transaction::create_table::create_table;
    /// # use delta_kernel::transaction::data_layout::DataLayout;
    /// # use delta_kernel::schema::{StructType, DataType, StructField};
    /// # use std::sync::Arc;
    /// # fn example() -> delta_kernel::DeltaResult<()> {
    /// # let schema = Arc::new(StructType::try_new(vec![
    /// #     StructField::new("id", DataType::INTEGER, false),
    /// #     StructField::new("date", DataType::STRING, false),
    /// # ])?);
    /// let builder = create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_data_layout(DataLayout::clustered(["id"]));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_data_layout(mut self, layout: DataLayout) -> Self {
        self.data_layout = layout;
        self
    }

    /// Builds a [`CreateTableTransaction`] that can be committed to create the table.
    ///
    /// The returned [`CreateTableTransaction`] only exposes operations that are valid for
    /// table creation. Operations like removing files, removing domain metadata, or updating
    /// deletion vectors are not available, preventing misuse at compile time.
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
    ) -> DeltaResult<CreateTableTransaction> {
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

        // Validate and transform table properties
        // - Extracts and validates feature signals
        // - Removes feature signals from properties (they shouldn't be stored in metadata)
        // - Returns reader/writer features to add to protocol
        let mut validated = validate_extract_table_features_and_properties(self.table_properties)?;

        // Apply column mapping if mode is name or id (must happen BEFORE clustering)
        let (effective_schema, column_mapping_mode) =
            maybe_apply_column_mapping_for_table_create(&self.schema, &mut validated)?;

        // Handle clustering (validates, resolves to physical names, adds features)
        let (system_domain_metadata, clustering_columns) = maybe_enable_clustering(
            &self.data_layout,
            &effective_schema,
            column_mapping_mode,
            &mut validated,
        )?;

        // Auto-enable variantType feature if schema contains Variant columns
        maybe_enable_variant_type(&effective_schema, &mut validated);

        // Create Protocol action with table features support
        let protocol =
            Protocol::try_new_modern(validated.reader_features, validated.writer_features)?;

        // Create Metadata action with filtered properties (feature signals removed)
        // Use effective_schema which includes column mapping annotations if enabled
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            effective_schema.clone(),
            Vec::new(), // partition_columns - added with data layout support
            current_time_ms()?,
            validated.properties,
        )?;

        // Create pre-commit snapshot from protocol/metadata
        let log_root = table_url.join("_delta_log/")?;
        let log_segment = LogSegment::for_pre_commit(log_root);
        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url, PRE_COMMIT_VERSION)?;

        // Create Transaction<CreateTable> with pre-commit snapshot
        Transaction::try_new_create_table(
            Arc::new(Snapshot::new(
                log_segment,
                table_configuration,
                Arc::new(LazyCrc::new(None)),
                None,
            )),
            self.engine_info,
            committer,
            system_domain_metadata,
            clustering_columns,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    use crate::utils::test_utils::assert_result_error_with_message;
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
    fn test_validate_supported_properties() {
        // Empty properties are allowed
        let properties = HashMap::new();
        let result = validate_extract_table_features_and_properties(properties);
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert!(validated.properties.is_empty());
        assert!(validated.reader_features.is_empty());
        assert!(validated.writer_features.is_empty());

        // User/application properties are allowed and preserved
        let mut properties = HashMap::new();
        properties.insert("myapp.version".to_string(), "1.0".to_string());
        properties.insert("custom.setting".to_string(), "value".to_string());
        let result = validate_extract_table_features_and_properties(properties);
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.properties.len(), 2);
        assert_eq!(
            validated.properties.get("myapp.version"),
            Some(&"1.0".to_string())
        );
        assert_eq!(
            validated.properties.get("custom.setting"),
            Some(&"value".to_string())
        );

        // Feature signal for domainMetadata IS allowed (it's in ALLOWED_DELTA_FEATURES)
        let properties = HashMap::from([(
            "delta.feature.domainMetadata".to_string(),
            "supported".to_string(),
        )]);
        let result = validate_extract_table_features_and_properties(properties);
        assert!(result.is_ok());
        let validated = result.unwrap();
        // Feature signals are removed from properties (not stored in metadata)
        assert!(validated.properties.is_empty());
        // DomainMetadata is a writer-only feature
        assert!(validated.reader_features.is_empty());
        assert!(validated
            .writer_features
            .contains(&TableFeature::DomainMetadata));
    }

    #[test]
    fn test_validate_unsupported_properties() {
        use crate::table_properties::{APPEND_ONLY, ENABLE_CHANGE_DATA_FEED};

        // Delta properties not on allow list are rejected
        let mut properties = HashMap::new();
        properties.insert(ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string());
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Setting delta property 'delta.enableChangeDataFeed' is not supported",
        );

        // Feature signals for features not in ALLOWED_DELTA_FEATURES are rejected
        let properties = HashMap::from([(
            "delta.feature.deletionVectors".to_string(),
            "supported".to_string(),
        )]);
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Enabling feature 'deletionVectors' via 'delta.feature.deletionVectors' is not supported",
        );

        // Clustering feature signal is rejected - users must use with_clustering_columns() instead
        let properties = HashMap::from([(
            "delta.feature.clustering".to_string(),
            "supported".to_string(),
        )]);
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Enabling feature 'clustering' via 'delta.feature.clustering' is not supported",
        );

        // Mixed properties with unsupported delta property are rejected
        let mut properties = HashMap::new();
        properties.insert("myapp.version".to_string(), "1.0".to_string());
        properties.insert(APPEND_ONLY.to_string(), "true".to_string());
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Setting delta property 'delta.appendOnly' is not supported",
        );
    }

    #[test]
    fn test_clustering_support_valid() {
        use crate::clustering::CLUSTERING_DOMAIN_NAME;
        use crate::expressions::ColumnName;

        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]));

        let mut reader_features = vec![];
        let mut writer_features = vec![];

        let dm = apply_clustering_for_table_create(
            &schema,
            &[ColumnName::new(["id"])],
            &mut reader_features,
            &mut writer_features,
        )
        .unwrap();

        assert_eq!(dm.domain(), CLUSTERING_DOMAIN_NAME);
        assert!(writer_features.contains(&TableFeature::DomainMetadata));
        assert!(writer_features.contains(&TableFeature::ClusteredTable));
        // DomainMetadata is a writer-only feature, ClusteredTable is also writer-only
        // So reader_features should be empty
        assert!(reader_features.is_empty());
    }

    #[test]
    fn test_clustering_support_multiple_columns() {
        use crate::expressions::ColumnName;

        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("date", DataType::STRING, true),
            StructField::new("region", DataType::STRING, true),
        ]));

        let mut reader_features = vec![];
        let mut writer_features = vec![];

        let dm = apply_clustering_for_table_create(
            &schema,
            &[ColumnName::new(["id"]), ColumnName::new(["date"])],
            &mut reader_features,
            &mut writer_features,
        )
        .unwrap();

        // Verify domain metadata contains both columns with correct names
        let config: serde_json::Value = serde_json::from_str(dm.configuration()).unwrap();
        let clustering_cols = config["clusteringColumns"].as_array().unwrap();
        assert_eq!(clustering_cols.len(), 2);
        assert_eq!(clustering_cols[0], serde_json::json!(["id"]));
        assert_eq!(clustering_cols[1], serde_json::json!(["date"]));
    }

    #[test]
    fn test_clustering_column_not_in_schema() {
        use crate::expressions::ColumnName;

        let schema = Arc::new(StructType::new_unchecked(vec![StructField::new(
            "id",
            DataType::INTEGER,
            false,
        )]));

        let mut reader_features = vec![];
        let mut writer_features = vec![];

        let result = apply_clustering_for_table_create(
            &schema,
            &[ColumnName::new(["nonexistent"])],
            &mut reader_features,
            &mut writer_features,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in schema"));
    }

    #[test]
    fn test_clustering_nested_column_accepted() {
        use crate::clustering::CLUSTERING_DOMAIN_NAME;
        use crate::expressions::ColumnName;

        let address_struct = StructType::new_unchecked(vec![
            StructField::new("city", DataType::STRING, true),
            StructField::new("zip", DataType::STRING, true),
        ]);
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("address", DataType::Struct(Box::new(address_struct)), true),
        ]));

        let mut reader_features = vec![];
        let mut writer_features = vec![];

        let nested_col = ColumnName::new(["address", "city"]);
        let dm = apply_clustering_for_table_create(
            &schema,
            &[nested_col],
            &mut reader_features,
            &mut writer_features,
        )
        .unwrap();

        assert_eq!(dm.domain(), CLUSTERING_DOMAIN_NAME);
        assert!(writer_features.contains(&TableFeature::ClusteredTable));
    }

    #[test]
    fn test_with_data_layout() {
        let schema = test_schema();

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_data_layout(DataLayout::clustered(["id"]));

        assert!(builder.data_layout.is_clustered());
    }

    #[test]
    fn test_variant_auto_enabled_from_schema() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("v", DataType::unshredded_variant(), true),
        ]));
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        maybe_enable_variant_type(&schema, &mut validated);

        assert!(validated
            .reader_features
            .contains(&TableFeature::VariantType));
        assert!(validated
            .writer_features
            .contains(&TableFeature::VariantType));
    }

    #[test]
    fn test_variant_not_enabled_without_variant_columns() {
        let schema = test_schema();
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        maybe_enable_variant_type(&schema, &mut validated);

        assert!(validated.reader_features.is_empty());
        assert!(validated.writer_features.is_empty());
    }

    #[test]
    fn test_variant_auto_enabled_nested() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "inner_v",
                    DataType::unshredded_variant(),
                    true,
                )]))),
                true,
            ),
        ]));
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        maybe_enable_variant_type(&schema, &mut validated);

        assert!(validated
            .reader_features
            .contains(&TableFeature::VariantType));
        assert!(validated
            .writer_features
            .contains(&TableFeature::VariantType));
    }
}
