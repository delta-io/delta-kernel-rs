//! Builder for creating new Delta tables.
//!
//! This module contains [`CreateTableTransactionBuilder`], which validates and constructs a
//! [`CreateTableTransaction`] from user-provided schema, properties, and data layout options.
//!
//! Use [`create_table()`](super::super::create_table::create_table) as the entry point rather
//! than constructing the builder directly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use url::Url;

use crate::actions::{DomainMetadata, Metadata, Protocol};
use crate::clustering::{create_clustering_domain_metadata, validate_clustering_columns};
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::schema::variant_utils::schema_contains_variant_type;
use crate::schema::{DataType, SchemaRef, StructType};
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    assign_column_mapping_metadata, get_any_level_column_physical_name,
    get_column_mapping_mode_from_properties, schema_contains_timestamp_ntz, ColumnMappingMode,
    EnablementCheck, FeatureType, TableFeature, SET_TABLE_FEATURE_SUPPORTED_PREFIX,
    SET_TABLE_FEATURE_SUPPORTED_VALUE,
};
use crate::table_properties::{
    TableProperties, CHECKPOINT_WRITE_STATS_AS_JSON, CHECKPOINT_WRITE_STATS_AS_STRUCT,
    COLUMN_MAPPING_MAX_COLUMN_ID, COLUMN_MAPPING_MODE, DELTA_PROPERTY_PREFIX,
    ENABLE_DELETION_VECTORS, ENABLE_IN_COMMIT_TIMESTAMPS, SET_TRANSACTION_RETENTION_DURATION,
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
    // InCommitTimestamp enables in-commit timestamps (writer-only)
    TableFeature::InCommitTimestamp,
    // VacuumProtocolCheck ensures consistent protocol checks during VACUUM
    TableFeature::VacuumProtocolCheck,
    // CatalogManaged enables catalog-managed table support
    #[cfg(feature = "catalog-managed")]
    TableFeature::CatalogManaged,
    // Note: Clustering is NOT included here. Users should not enable clustering via
    // `delta.feature.clustering = supported`. Instead, clustering is enabled by
    // specifying clustering columns via `with_data_layout()`.
    TableFeature::DeletionVectors,
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
    // InCommitTimestamp enablement property: triggers ICT auto-enablement
    ENABLE_IN_COMMIT_TIMESTAMPS,
    // Checkpoint stats format properties
    CHECKPOINT_WRITE_STATS_AS_JSON,
    CHECKPOINT_WRITE_STATS_AS_STRUCT,
    // DeletionVectors enablement property: auto-enables the DeletionVectors feature
    ENABLE_DELETION_VECTORS,
    // Set transaction retention duration: controls expiration of txn identifiers
    SET_TRANSACTION_RETENTION_DURATION,
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
                    "Table already exists at path: {table_path}"
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

/// Result of applying data layout configuration during table creation.
///
/// Contains all outputs needed by `build()` from the data layout processing step.
#[derive(Debug, Default)]
struct DataLayoutResult {
    /// Domain metadata actions (clustering stores `delta.clustering` domain metadata).
    system_domain_metadata: Vec<DomainMetadata>,
    /// Clustering columns for stats schema (physical names, `None` if not clustered).
    clustering_columns: Option<Vec<ColumnName>>,
    /// Partition columns (logical names, `None` if not partitioned).
    partition_columns: Option<Vec<ColumnName>>,
}

/// Validates partition columns against the table schema.
///
/// Similar to [`validate_clustering_columns`] (duplicate check, schema lookup), but with
/// stricter constraints: partition columns must be top-level and primitive-typed, while
/// clustering columns may be nested and accept all stats-eligible types.
///
/// Partition columns must be:
/// 1. Top-level columns (nested paths are not supported)
/// 2. Present in the schema
/// 3. Not duplicated
/// 4. Of a primitive type (Struct, Array, Map are rejected because partition values
///    must be representable as directory-path strings)
/// 5. A strict subset of the schema columns (at least one non-partition column required)
fn validate_partition_columns(
    schema: &StructType,
    partition_columns: &[ColumnName],
) -> DeltaResult<()> {
    if partition_columns.is_empty() {
        return Err(Error::generic("Partitioning requires at least one column"));
    }
    if partition_columns.len() >= schema.fields().len() {
        return Err(Error::generic(
            "Table must have at least one non-partition column",
        ));
    }

    let mut seen = HashSet::new();
    for col in partition_columns {
        let path = col.path();
        if path.len() != 1 {
            return Err(Error::generic(format!(
                "Partition column '{}' must be a top-level column (nested paths are not supported)",
                col
            )));
        }

        if !seen.insert(col) {
            return Err(Error::generic(format!(
                "Duplicate partition column: '{col}'"
            )));
        }

        // Safety: path.len() == 1 is enforced by the top-level check above
        let col_name = &path[0];
        let field = schema.field(col_name).ok_or_else(|| {
            Error::generic(format!("Partition column '{col}' not found in schema"))
        })?;

        if !matches!(field.data_type(), DataType::Primitive(_)) {
            return Err(Error::generic(format!(
                "Partition column '{col}' has non-primitive type '{}'. \
                 Partition columns must have primitive types.",
                field.data_type()
            )));
        }
    }
    Ok(())
}

/// Applies data layout configuration for table creation.
///
/// Handles all [`DataLayout`] variants:
///
/// - **None**: Returns defaults (no domain metadata, no clustering/partition columns).
/// - **Clustered**: Validates clustering columns, resolves to physical names, adds
///   `DomainMetadata` + `ClusteredTable` features, creates clustering domain metadata.
/// - **Partitioned**: Validates partition columns and stores logical names. No domain
///   metadata or special features are needed (partitioning is a core Delta feature).
fn apply_data_layout(
    data_layout: &DataLayout,
    effective_schema: &SchemaRef,
    column_mapping_mode: ColumnMappingMode,
    validated: &mut ValidatedTableProperties,
) -> DeltaResult<DataLayoutResult> {
    match data_layout {
        DataLayout::None => Ok(DataLayoutResult::default()),

        DataLayout::Clustered { columns } => {
            validate_clustering_columns(effective_schema, columns)?;

            let physical_columns: Vec<ColumnName> = columns
                .iter()
                .map(|c| {
                    get_any_level_column_physical_name(effective_schema, c, column_mapping_mode)
                })
                .try_collect()?;

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

            let dm = create_clustering_domain_metadata(&physical_columns);

            Ok(DataLayoutResult {
                system_domain_metadata: vec![dm],
                clustering_columns: Some(physical_columns),
                partition_columns: None,
            })
        }

        DataLayout::Partitioned { columns } => {
            validate_partition_columns(effective_schema, columns)?;

            Ok(DataLayoutResult {
                system_domain_metadata: vec![],
                clustering_columns: None,
                partition_columns: Some(columns.clone()),
            })
        }
    }
}

/// Conditionally adds the `variantType` feature to the protocol when the schema contains Variant
/// columns anywhere in the schema tree (top-level, nested structs, arrays, maps).
fn maybe_enable_variant_type(schema: &SchemaRef, validated: &mut ValidatedTableProperties) {
    if schema_contains_variant_type(schema) {
        add_feature_to_lists(
            TableFeature::VariantType,
            &mut validated.reader_features,
            &mut validated.writer_features,
        );
    }
}

/// Conditionally adds the `timestampNtz` feature to the protocol when the schema contains
/// TimestampNTZ columns anywhere in the schema tree (top-level, nested structs, arrays, maps).
fn maybe_enable_timestamp_ntz(schema: &SchemaRef, validated: &mut ValidatedTableProperties) {
    if schema_contains_timestamp_ntz(schema) {
        add_feature_to_lists(
            TableFeature::TimestampWithoutTimezone,
            &mut validated.reader_features,
            &mut validated.writer_features,
        );
    }
}

/// Auto-enables allowed features whose [`EnablementCheck::EnabledIf`] check is satisfied by the
/// table properties. Features with [`EnablementCheck::AlwaysIfSupported`] are skipped since they
/// don't require property-driven enablement.
fn maybe_auto_enable_property_driven_features(validated: &mut ValidatedTableProperties) {
    let table_properties = TableProperties::from(validated.properties.iter());
    for feature in ALLOWED_DELTA_FEATURES {
        if let EnablementCheck::EnabledIf(check) = feature.info().enablement_check {
            if check(&table_properties) {
                add_feature_to_lists(
                    feature.clone(),
                    &mut validated.reader_features,
                    &mut validated.writer_features,
                );
            }
        }
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
///
/// Note: This function does not auto-set enablement properties. A feature signal like
/// `delta.feature.deletionVectors=supported` adds the feature to the protocol but does
/// not insert `delta.enableDeletionVectors=true` into the properties. Property-driven
/// auto-enablement is handled separately by [`maybe_auto_enable_property_driven_features`]
/// called after validation.
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
                "Invalid value '{value}' for '{key}'. Only '{SET_TABLE_FEATURE_SUPPORTED_VALUE}' is allowed."
            )));
        }

        // Parse feature name to TableFeature (unknown features become TableFeature::Unknown)
        let feature: TableFeature = feature_name
            .parse()
            .unwrap_or_else(|_| TableFeature::Unknown(feature_name.to_string()));

        if !ALLOWED_DELTA_FEATURES.contains(&feature) {
            return Err(Error::generic(format!(
                "Enabling feature '{feature_name}' via '{key}' is not supported during CREATE TABLE"
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
                "Setting delta property '{key}' is not supported during CREATE TABLE"
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
    /// - [`DataLayout::Partitioned`]: Data files are organized into directories by partition
    ///   column values
    ///
    /// Partitioning and clustering are mutually exclusive.
    ///
    /// Calling this method multiple times replaces the previous layout. Only the last
    /// `with_data_layout()` call takes effect.
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
    /// // Clustered layout:
    /// let builder = create_table("/path/to/table", schema.clone(), "MyApp/1.0")
    ///     .with_data_layout(DataLayout::clustered(["id"]));
    ///
    /// // Partitioned layout:
    /// let builder = create_table("/path/to/table", schema, "MyApp/1.0")
    ///     .with_data_layout(DataLayout::partitioned(["date"]));
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
    /// - Validates the data layout is valid
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
    /// - The data layout is invalid
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

        // Apply column mapping if mode is name or id (must happen BEFORE data layout)
        let (effective_schema, column_mapping_mode) =
            maybe_apply_column_mapping_for_table_create(&self.schema, &mut validated)?;

        // Validate data layout and resolve column names (physical for clustering, logical
        // for partitioning). Adds required table features for clustering.
        let data_layout_result = apply_data_layout(
            &self.data_layout,
            &effective_schema,
            column_mapping_mode,
            &mut validated,
        )?;

        // Schema-driven auto-enablement: detect types that require a feature
        maybe_enable_variant_type(&effective_schema, &mut validated);
        maybe_enable_timestamp_ntz(&effective_schema, &mut validated);

        // Property-driven auto-enablement: check enablement properties
        maybe_auto_enable_property_driven_features(&mut validated);

        // Create Protocol action with table features support
        let protocol =
            Protocol::try_new_modern(validated.reader_features, validated.writer_features)?;

        // Create Metadata action with filtered properties (feature signals removed)
        // Use effective_schema which includes column mapping annotations if enabled
        // Partition columns are validated to be top-level, so each ColumnName has
        // exactly one path component. Extract it with remove(0).
        let partition_columns: Vec<String> = data_layout_result
            .partition_columns
            .map(|cols| cols.into_iter().map(|c| c.into_inner().remove(0)).collect())
            .unwrap_or_default();
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            effective_schema.clone(),
            partition_columns,
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
            Arc::new(Snapshot::new(log_segment, table_configuration)),
            self.engine_info,
            committer,
            data_layout_result.system_domain_metadata,
            data_layout_result.clustering_columns,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expressions::ColumnName;
    use crate::schema::{DataType, StructField, StructType};
    use crate::utils::test_utils::assert_result_error_with_message;

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
            "delta.feature.identityColumns".to_string(),
            "supported".to_string(),
        )]);
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Enabling feature 'identityColumns' via 'delta.feature.identityColumns' is not supported",
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

    #[rstest::rstest]
    #[case::clustered(DataLayout::clustered(["id"]), true, false)]
    #[case::partitioned(DataLayout::partitioned(["id"]), false, true)]
    #[case::none(DataLayout::default(), false, false)]
    fn test_with_data_layout(
        #[case] layout: DataLayout,
        #[case] expect_clustered: bool,
        #[case] expect_partitioned: bool,
    ) {
        let schema = test_schema();

        let builder = CreateTableTransactionBuilder::new("/path/to/table", schema, "TestApp/1.0")
            .with_data_layout(layout);

        assert_eq!(builder.data_layout.is_clustered(), expect_clustered);
        assert_eq!(builder.data_layout.is_partitioned(), expect_partitioned);
    }

    #[rstest::rstest]
    #[case::variant_top_level(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("v", DataType::unshredded_variant(), true),
        ])),
        &[TableFeature::VariantType],
    )]
    #[case::variant_nested(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("inner_v", DataType::unshredded_variant(), true),
                ]))),
                true,
            ),
        ])),
        &[TableFeature::VariantType],
    )]
    #[case::ntz_top_level(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("ts", DataType::TIMESTAMP_NTZ, true),
        ])),
        &[TableFeature::TimestampWithoutTimezone],
    )]
    #[case::ntz_nested(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("inner_ts", DataType::TIMESTAMP_NTZ, true),
                ]))),
                true,
            ),
        ])),
        &[TableFeature::TimestampWithoutTimezone],
    )]
    #[case::both_variant_and_ntz(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("v", DataType::unshredded_variant(), true),
            StructField::new("ts", DataType::TIMESTAMP_NTZ, true),
        ])),
        &[TableFeature::VariantType, TableFeature::TimestampWithoutTimezone],
    )]
    #[case::no_special_types(
        test_schema(),
        &[],
    )]
    fn test_schema_driven_feature_auto_enablement(
        #[case] schema: SchemaRef,
        #[case] expected_features: &[TableFeature],
    ) {
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        maybe_enable_variant_type(&schema, &mut validated);
        maybe_enable_timestamp_ntz(&schema, &mut validated);

        for feature in expected_features {
            assert!(
                validated.reader_features.contains(feature),
                "Expected {feature:?} in reader_features"
            );
            assert!(
                validated.writer_features.contains(feature),
                "Expected {feature:?} in writer_features"
            );
        }
        assert_eq!(
            validated.reader_features.len(),
            expected_features.len(),
            "Unexpected extra reader features: {:?}",
            validated.reader_features
        );
        assert_eq!(
            validated.writer_features.len(),
            expected_features.len(),
            "Unexpected extra writer features: {:?}",
            validated.writer_features
        );
    }

    #[rstest::rstest]
    #[case::property_true(&[("delta.enableInCommitTimestamps", "true")], true, true)]
    #[case::property_false(&[("delta.enableInCommitTimestamps", "false")], false, true)]
    #[case::property_absent(&[], false, false)]
    #[case::feature_signal(&[("delta.feature.inCommitTimestamp", "supported")], true, false)]
    fn test_ict_support_and_enablement(
        #[case] properties: &[(&str, &str)],
        #[case] expect_in_writer_features: bool,
        #[case] expect_property_preserved: bool,
    ) {
        let properties: HashMap<String, String> = properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut validated = validate_extract_table_features_and_properties(properties).unwrap();

        maybe_auto_enable_property_driven_features(&mut validated);

        assert_eq!(
            validated
                .writer_features
                .contains(&TableFeature::InCommitTimestamp),
            expect_in_writer_features,
        );
        assert_eq!(
            validated
                .properties
                .contains_key(ENABLE_IN_COMMIT_TIMESTAMPS),
            expect_property_preserved,
        );
        assert!(
            validated.reader_features.is_empty(),
            "InCommitTimestamp is writer-only, reader_features should always be empty"
        );
    }

    #[test]
    fn test_vacuum_protocol_check_feature_signal() {
        let properties = HashMap::from([(
            "delta.feature.vacuumProtocolCheck".to_string(),
            "supported".to_string(),
        )]);
        let validated = validate_extract_table_features_and_properties(properties).unwrap();
        assert!(
            validated.properties.is_empty(),
            "Feature signal should be removed from properties"
        );
        assert!(
            validated
                .writer_features
                .contains(&TableFeature::VacuumProtocolCheck),
            "VacuumProtocolCheck should be in writer_features"
        );
        assert!(
            validated
                .reader_features
                .contains(&TableFeature::VacuumProtocolCheck),
            "VacuumProtocolCheck should be in reader_features (ReaderWriter feature)"
        );
    }

    fn multi_column_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("date", DataType::DATE, true),
        ]))
    }

    struct DataLayoutExpectation {
        layout: DataLayout,
        has_domain_metadata: bool,
        has_clustering_columns: bool,
        expected_partition_columns: Option<Vec<ColumnName>>,
        expected_writer_features: Vec<TableFeature>,
    }

    #[rstest::rstest]
    #[case::none(DataLayoutExpectation {
        layout: DataLayout::default(),
        has_domain_metadata: false,
        has_clustering_columns: false,
        expected_partition_columns: None,
        expected_writer_features: vec![],
    })]
    #[case::clustered(DataLayoutExpectation {
        layout: DataLayout::clustered(["id"]),
        has_domain_metadata: true,
        has_clustering_columns: true,
        expected_partition_columns: None,
        expected_writer_features: vec![TableFeature::DomainMetadata, TableFeature::ClusteredTable],
    })]
    #[case::partitioned_single(DataLayoutExpectation {
        layout: DataLayout::partitioned(["date"]),
        has_domain_metadata: false,
        has_clustering_columns: false,
        expected_partition_columns: Some(vec![ColumnName::new(["date"])]),
        expected_writer_features: vec![],
    })]
    #[case::partitioned_multiple(DataLayoutExpectation {
        layout: DataLayout::partitioned(["id", "date"]),
        has_domain_metadata: false,
        has_clustering_columns: false,
        expected_partition_columns: Some(vec![ColumnName::new(["id"]), ColumnName::new(["date"])]),
        expected_writer_features: vec![],
    })]
    fn test_apply_data_layout(#[case] expectation: DataLayoutExpectation) {
        let schema = multi_column_schema();
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        let result = apply_data_layout(
            &expectation.layout,
            &schema,
            ColumnMappingMode::None,
            &mut validated,
        )
        .unwrap();

        assert_eq!(
            !result.system_domain_metadata.is_empty(),
            expectation.has_domain_metadata
        );
        assert_eq!(
            result.clustering_columns.is_some(),
            expectation.has_clustering_columns
        );
        assert_eq!(
            result.partition_columns,
            expectation.expected_partition_columns
        );

        for feature in &expectation.expected_writer_features {
            assert!(
                validated.writer_features.contains(feature),
                "Expected {feature:?} in writer_features"
            );
        }
    }

    #[rstest::rstest]
    #[case::clustered_invalid_col(DataLayout::clustered(["nonexistent"]), "not found in schema")]
    #[case::partitioned_invalid_col(DataLayout::partitioned(["nonexistent"]), "not found in schema")]
    #[case::partitioned_duplicate(DataLayout::partitioned(["id", "id"]), "Duplicate partition column")]
    #[case::partitioned_empty(DataLayout::Partitioned { columns: vec![] }, "at least one column")]
    #[case::partitioned_all_columns(DataLayout::partitioned(["id", "name", "date"]), "at least one non-partition column")]
    fn test_apply_data_layout_validation_errors(
        #[case] layout: DataLayout,
        #[case] expected_error: &str,
    ) {
        let schema = multi_column_schema();
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        let result = apply_data_layout(&layout, &schema, ColumnMappingMode::None, &mut validated);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(expected_error),
            "Expected error containing '{expected_error}'"
        );
    }

    #[test]
    fn test_validate_partition_columns_nested_rejected() {
        let address_struct =
            StructType::new_unchecked(vec![StructField::new("city", DataType::STRING, true)]);
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("address", DataType::Struct(Box::new(address_struct)), true),
        ]);

        let columns = vec![ColumnName::new(["address", "city"])];
        let result = validate_partition_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be a top-level column"));
    }

    #[rstest::rstest]
    #[case::struct_type(
        "struct_col",
        DataType::Struct(Box::new(StructType::new_unchecked(vec![
            StructField::new("inner", DataType::STRING, false),
        ]))),
    )]
    #[case::array_type(
        "array_col",
        DataType::Array(Box::new(crate::schema::ArrayType::new(DataType::INTEGER, false)))
    )]
    #[case::map_type(
        "map_col",
        DataType::Map(Box::new(crate::schema::MapType::new(
            DataType::STRING,
            DataType::INTEGER,
            false
        )))
    )]
    fn test_validate_partition_columns_complex_types_rejected(
        #[case] col_name: &str,
        #[case] data_type: DataType,
    ) {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(col_name, data_type, false),
        ]);
        let columns = vec![ColumnName::new([col_name])];
        let result = validate_partition_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("non-primitive type"));
    }

    #[rstest::rstest]
    #[case::integer(DataType::INTEGER)]
    #[case::string(DataType::STRING)]
    #[case::date(DataType::DATE)]
    #[case::timestamp(DataType::TIMESTAMP)]
    #[case::boolean(DataType::BOOLEAN)]
    #[case::long(DataType::LONG)]
    fn test_validate_partition_columns_primitive_types_accepted(#[case] data_type: DataType) {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("col", data_type, false),
        ]);
        let columns = vec![ColumnName::new(["col"])];
        assert!(validate_partition_columns(&schema, &columns).is_ok());
    }

    #[cfg(feature = "catalog-managed")]
    #[test]
    fn test_catalog_managed_feature_signal_accepted() {
        let properties = HashMap::from([(
            "delta.feature.catalogManaged".to_string(),
            "supported".to_string(),
        )]);
        let validated = validate_extract_table_features_and_properties(properties).unwrap();
        assert!(
            validated.properties.is_empty(),
            "Feature signal should be removed from properties"
        );
        assert!(
            validated
                .writer_features
                .contains(&TableFeature::CatalogManaged),
            "CatalogManaged should be in writer_features"
        );
        assert!(
            validated
                .reader_features
                .contains(&TableFeature::CatalogManaged),
            "CatalogManaged should be in reader_features (ReaderWriter feature)"
        );
    }
}
