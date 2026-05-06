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
use uuid::Uuid;

use crate::actions::{DomainMetadata, Metadata, Protocol};
use crate::clustering::{create_clustering_domain_metadata, validate_clustering_columns};
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::variant_utils::schema_contains_variant_type;
use crate::schema::{
    normalize_column_names_to_schema_casing, schema_contains_non_null_fields, DataType, SchemaRef,
    StructType,
};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    assign_column_mapping_metadata, get_any_level_column_physical_name,
    get_column_mapping_mode_from_properties, schema_contains_timestamp_ntz, ColumnMappingMode,
    EnablementCheck, FeatureType, TableFeature, SET_TABLE_FEATURE_SUPPORTED_PREFIX,
    SET_TABLE_FEATURE_SUPPORTED_VALUE,
};
use crate::table_properties::{
    TableProperties, APPEND_ONLY, CHECKPOINT_WRITE_STATS_AS_JSON, CHECKPOINT_WRITE_STATS_AS_STRUCT,
    COLUMN_MAPPING_MAX_COLUMN_ID, COLUMN_MAPPING_MODE, DELTA_PROPERTY_PREFIX,
    ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS, ENABLE_ICEBERG_COMPAT_V1,
    ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3, ENABLE_IN_COMMIT_TIMESTAMPS,
    ENABLE_ROW_TRACKING, ENABLE_TYPE_WIDENING, MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
    MATERIALIZED_ROW_ID_COLUMN_NAME, PARQUET_FORMAT_VERSION, ROW_TRACKING_SUSPENDED,
    SET_TRANSACTION_RETENTION_DURATION,
};
use crate::transaction::create_table::CreateTableTransaction;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::Transaction;
use crate::utils::{current_time_ms, try_parse_uri};
use crate::{DeltaResult, Engine, Error, StorageHandler};

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
    TableFeature::CatalogManaged,
    // Note: Clustering is NOT included here. Users should not enable clustering via
    // `delta.feature.clustering = supported`. Instead, clustering is enabled by
    // specifying clustering columns via `with_data_layout()`.
    TableFeature::DeletionVectors,
    TableFeature::V2Checkpoint,
    // Simple protocol-only features: enabling these only updates the protocol action.
    // They can also be auto-enabled via their enablement properties (e.g. delta.appendOnly=true)
    // through `maybe_auto_enable_property_driven_features`.
    TableFeature::AppendOnly,
    TableFeature::ChangeDataFeed,
    TableFeature::TypeWidening,
    TableFeature::RowTracking,
    // Invariants is auto-enabled by `maybe_enable_invariants` when the schema has non-null
    // fields. Allowing explicit `delta.feature.invariants=supported` lets users pre-enable
    // the feature on an all-nullable table so a later ALTER TABLE ADD COLUMN NOT NULL does
    // not need a protocol upgrade.
    TableFeature::Invariants,
    // MaterializePartitionColumns keeps partition columns in the data files instead of
    // dropping them on write. There is no `delta.*` enablement property; the only opt-in at
    // create time is the explicit feature signal
    // `delta.feature.materializePartitionColumns=supported`.
    TableFeature::MaterializePartitionColumns,
    // IcebergCompatV3 is a writer-only feature that gates Iceberg V3 conversion compatibility.
    // Dependent features (ColumnMapping, RowTracking, DomainMetadata) are auto-added during
    // create table.
    TableFeature::IcebergCompatV3,
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
    // Property-driven feature enablement properties
    ENABLE_DELETION_VECTORS,
    ENABLE_CHANGE_DATA_FEED,
    ENABLE_TYPE_WIDENING,
    APPEND_ONLY,
    ENABLE_ROW_TRACKING,
    // Set transaction retention duration: controls expiration of txn identifiers
    SET_TRANSACTION_RETENTION_DURATION,
    // Parquet format version: controls the Parquet writer version for data files
    PARQUET_FORMAT_VERSION,
    // IcebergCompatV3 enablement: triggers auto-enablement of ColumnMapping,
    // RowTracking, DomainMetadata.
    ENABLE_ICEBERG_COMPAT_V3,
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

impl ValidatedTableProperties {
    /// Returns `true` iff `properties[key] == "true"`.
    fn is_property_true(&self, key: &str) -> bool {
        self.properties.get(key).map(String::as_str) == Some("true")
    }
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

/// Test-only helper for clustering support during table creation.
///
/// Validates clustering columns, adds the `DomainMetadata` and `ClusteredTable` features
/// directly, and creates the domain metadata action.
#[cfg(test)]
fn validate_clustering_and_make_domain_metadata(
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
/// 4. Of a primitive type (Struct, Array, Map are rejected because partition values must be
///    representable as directory-path strings)
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
/// - **Clustered**: Validates clustering columns, resolves to physical names, adds the
///   `DomainMetadata` and `ClusteredTable` features, creates clustering domain metadata.
/// - **Partitioned**: Validates partition columns and stores logical names. No domain metadata or
///   special features are needed (partitioning is a core Delta feature).
fn apply_data_layout(
    data_layout: &DataLayout,
    effective_schema: &SchemaRef,
    column_mapping_mode: ColumnMappingMode,
    validated: &mut ValidatedTableProperties,
) -> DeltaResult<DataLayoutResult> {
    match data_layout {
        DataLayout::None => Ok(DataLayoutResult::default()),

        DataLayout::Clustered { columns } => {
            // Normalize clustering column names to match schema casing. This allows users
            // to specify clustering columns case-insensitively (e.g. schema has columns
            // "A", "B", "C" and user clusters by "c", "a").
            let normalized = normalize_column_names_to_schema_casing(effective_schema, columns);
            validate_clustering_columns(effective_schema, &normalized)?;

            let physical_columns: Vec<ColumnName> = normalized
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
            let normalized = normalize_column_names_to_schema_casing(effective_schema, columns);
            validate_partition_columns(effective_schema, &normalized)?;

            Ok(DataLayoutResult {
                system_domain_metadata: vec![],
                clustering_columns: None,
                partition_columns: Some(normalized),
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

/// Conditionally adds the `invariants` writer feature to the protocol when the schema contains
/// any non-null column (`nullable: false`) anywhere in the tree.
///
/// Delta-Spark treats `nullable: false` as an implicit column invariant and requires the
/// `invariants` writer feature to be listed in the protocol's `writerFeatures` to read/write
/// such tables. Auto-enabling ensures kernel-created tables with non-null columns are
/// compatible with Spark readers/writers.
///
/// Explicit `delta.invariants` metadata annotations are rejected by
/// `validate_schema`, so this only flips on the feature for nullability-driven
/// invariants. Kernel does not itself enforce the null mask at write time -- it relies on
/// the engine's `ParquetHandler` to do so. Kernel's default `ParquetHandler` uses
/// `arrow-rs`, whose `RecordBatch::try_new` rejects null values in fields marked
/// `nullable: false`. Other engine implementations must provide an equivalent guarantee
/// in their write path.
fn maybe_enable_invariants(schema: &SchemaRef, validated: &mut ValidatedTableProperties) {
    if schema_contains_non_null_fields(schema) {
        add_feature_to_lists(
            TableFeature::Invariants,
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
                // RowTracking requires DomainMetadata as a dependency
                if *feature == TableFeature::RowTracking {
                    add_feature_to_lists(
                        TableFeature::DomainMetadata,
                        &mut validated.reader_features,
                        &mut validated.writer_features,
                    );
                }
            }
        }
    }
}

/// Sets materialized column name properties when row tracking is enabled.
///
/// Writes `delta.rowTracking.materializedRowIdColumnName` and
/// `delta.rowTracking.materializedRowCommitVersionColumnName` into the table
/// properties using UUID-based column names (`_row-id-col-{uuid}` and
/// `_row-commit-version-col-{uuid}`). These names record which physical columns
/// store materialized row IDs and commit versions.
///
/// Only fires when `delta.enableRowTracking=true` is set. Feature-signal-only tables
/// (`delta.feature.rowTracking=supported` without the enablement property) do not get
/// these properties because the materialized columns are part of the "enabled" state, not
/// the "supported" state.
fn maybe_set_materialized_row_tracking_column_name_properties(
    validated: &mut ValidatedTableProperties,
) {
    if validated
        .properties
        .get(ENABLE_ROW_TRACKING)
        .is_none_or(|v| v != "true")
    {
        return;
    }
    validated.properties.insert(
        MATERIALIZED_ROW_ID_COLUMN_NAME.to_string(),
        format!("_row-id-col-{}", Uuid::new_v4()),
    );
    validated.properties.insert(
        MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME.to_string(),
        format!("_row-commit-version-col-{}", Uuid::new_v4()),
    );
}

/// Ensures that `inCommitTimestamp` is enabled when `catalogManaged` is present. Adds the ICT
/// feature to the protocol and sets the enablement property if not already present.
fn maybe_enable_ict_for_catalog_managed(
    validated: &mut ValidatedTableProperties,
) -> DeltaResult<()> {
    let has_catalog_managed = validated
        .writer_features
        .contains(&TableFeature::CatalogManaged);
    if has_catalog_managed {
        if validated
            .properties
            .get(ENABLE_IN_COMMIT_TIMESTAMPS)
            .is_some_and(|v| v != "true")
        {
            return Err(Error::generic(format!(
                "Catalog-managed tables require '{ENABLE_IN_COMMIT_TIMESTAMPS}=true', \
                 but it was explicitly set to '{}'",
                validated.properties[ENABLE_IN_COMMIT_TIMESTAMPS]
            )));
        }
        add_feature_to_lists(
            TableFeature::InCommitTimestamp,
            &mut validated.reader_features,
            &mut validated.writer_features,
        );
        validated
            .properties
            .entry(ENABLE_IN_COMMIT_TIMESTAMPS.to_string())
            .or_insert_with(|| "true".to_string());
    }
    Ok(())
}

/// Witness that all property-flipping passes which must run before column mapping is applied
/// have completed.
#[must_use]
#[derive(Debug)]
struct PreColumnMappingResolved;

/// When `delta.enableIcebergCompatV3=true` is set, auto-enables V3's required dependencies in
/// `validated.properties` (defaulting them when absent, rejecting conflicting values).
///
/// Specifically:
///   * Set `delta.columnMapping.mode` to `name` when absent, reject if it's `none`.
///   * Set `delta.enableRowTracking` to `true` when absent, reject if it's `false`.
///   * Reject if `delta.rowTrackingSuspended` is `true`.
///   * Reject if `delta.enableIcebergCompatV1` or `delta.enableIcebergCompatV2` is `true`.
///
/// Returns a [`PreColumnMappingResolved`] witness that
/// [`maybe_apply_column_mapping_for_table_create`] requires, ensuring this pass runs first.
fn maybe_enable_iceberg_compat_v3_dependencies(
    validated: &mut ValidatedTableProperties,
) -> DeltaResult<PreColumnMappingResolved> {
    if !validated.is_property_true(ENABLE_ICEBERG_COMPAT_V3) {
        return Ok(PreColumnMappingResolved);
    }

    // Column mapping: require `name` or `id`; default to `name`.
    match validated
        .properties
        .get(COLUMN_MAPPING_MODE)
        .map(String::as_str)
    {
        None => {
            validated
                .properties
                .insert(COLUMN_MAPPING_MODE.to_string(), "name".to_string());
        }
        Some("name") | Some("id") => {}
        Some(other) => {
            return Err(Error::generic(format!(
                "IcebergCompatV3 requires '{COLUMN_MAPPING_MODE}' to be 'name' or 'id', got \
                 '{other}'"
            )));
        }
    }

    // Row tracking must not be suspended (suspension cannot coexist with row tracking actively
    // enabled, which V3 requires).
    if validated.is_property_true(ROW_TRACKING_SUSPENDED) {
        return Err(Error::generic(format!(
            "IcebergCompatV3 cannot be enabled while '{ROW_TRACKING_SUSPENDED}' is 'true'"
        )));
    }

    // Row tracking enablement: require `true`; default to `true`.
    match validated
        .properties
        .get(ENABLE_ROW_TRACKING)
        .map(String::as_str)
    {
        None => {
            validated
                .properties
                .insert(ENABLE_ROW_TRACKING.to_string(), "true".to_string());
        }
        Some("true") => {}
        Some(other) => {
            return Err(Error::generic(format!(
                "IcebergCompatV3 requires '{ENABLE_ROW_TRACKING}' to be 'true', got '{other}'"
            )));
        }
    }

    // V1/V2 must not be active.
    for key in [ENABLE_ICEBERG_COMPAT_V1, ENABLE_ICEBERG_COMPAT_V2] {
        if validated.is_property_true(key) {
            return Err(Error::generic(format!(
                "IcebergCompatV3 cannot be enabled together with '{key}'"
            )));
        }
    }

    Ok(PreColumnMappingResolved)
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
    _pre_cm: PreColumnMappingResolved,
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

            // Transform schema: assign IDs and physical names to all fields. When
            // IcebergCompatV3 is enabled, allocated nested ids for element/key/value in Array/Map
            // as well.
            let assign_nested_field_ids = validated.is_property_true(ENABLE_ICEBERG_COMPAT_V3);
            let mut max_id = 0i64;
            let transformed_schema =
                assign_column_mapping_metadata(schema, &mut max_id, assign_nested_field_ids)?;

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
        let needs_domain_metadata = feature == TableFeature::RowTracking;
        add_feature_to_lists(feature, &mut reader_features, &mut writer_features);
        // RowTracking requires DomainMetadata as a dependency
        if needs_domain_metadata {
            add_feature_to_lists(
                TableFeature::DomainMetadata,
                &mut reader_features,
                &mut writer_features,
            );
        }
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
    /// Feature flags (`delta.feature.*=supported`) are supported for the subset of features
    /// listed in `ALLOWED_DELTA_FEATURES`.
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
    /// # let schema = Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, true)])?);
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
    /// - [`DataLayout::Partitioned`]: Data files are organized into directories by partition column
    ///   values
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
    /// #     StructField::new("id", DataType::INTEGER, true),
    /// #     StructField::new("date", DataType::STRING, true),
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
    /// - Rejects schemas with `delta.invariants` metadata annotations (unsupported by kernel)
    /// - Validates the data layout is valid
    /// - Validates table properties against the allow list
    ///
    /// Non-null columns (`nullable: false`) are allowed. The `invariants` writer feature is
    /// auto-added to the protocol when the schema has any non-null column.
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
    /// - The schema has `delta.invariants` metadata on any column
    /// - The data layout is invalid
    /// - Unsupported delta properties or feature flags are specified
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<CreateTableTransaction> {
        // Validate path
        let table_url = try_parse_uri(&self.path)?;

        // Check if table already exists by looking for _delta_log directory
        let delta_log_url = table_url.join("_delta_log/")?;
        let storage = engine.storage_handler();
        ensure_table_does_not_exist(storage.as_ref(), &delta_log_url, &self.path)?;

        // Validate and transform table properties
        // - Extracts and validates feature signals
        // - Removes feature signals from properties (they shouldn't be stored in metadata)
        // - Returns reader/writer features to add to protocol
        let mut validated = validate_extract_table_features_and_properties(self.table_properties)?;

        // When IcebergCompatV3 is enabled, fill in / validate required dependencies before
        // column mapping is applied so the CM mode is in place. The returned witness is
        // required by `maybe_apply_column_mapping_for_table_create` below.
        let pre_cm = maybe_enable_iceberg_compat_v3_dependencies(&mut validated)?;

        // Apply column mapping if mode is name or id (must happen BEFORE data layout)
        let (effective_schema, column_mapping_mode) =
            maybe_apply_column_mapping_for_table_create(&self.schema, &mut validated, pre_cm)?;

        // Validate schema (non-empty, column names, duplicates, no `delta.invariants` metadata)
        validate_schema(&effective_schema, column_mapping_mode)?;

        // Validate data layout and resolve column names (physical for clustering, logical
        // for partitioning). Adds required table features for clustering.
        let data_layout_result = apply_data_layout(
            &self.data_layout,
            &effective_schema,
            column_mapping_mode,
            &mut validated,
        )?;

        // Schema-driven auto-enablement: detect types or annotations that require a feature
        maybe_enable_variant_type(&effective_schema, &mut validated);
        maybe_enable_timestamp_ntz(&effective_schema, &mut validated);
        maybe_enable_invariants(&effective_schema, &mut validated);

        // Property-driven auto-enablement: check enablement properties
        maybe_auto_enable_property_driven_features(&mut validated);

        // Auto-enable inCommitTimestamp for catalogManaged tables
        maybe_enable_ict_for_catalog_managed(&mut validated)?;

        // Set materialized row tracking column names when row tracking is enabled.
        maybe_set_materialized_row_tracking_column_name_properties(&mut validated);

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

        // Build TableConfiguration directly for the new table
        let table_configuration = TableConfiguration::try_new(metadata, protocol, table_url, 0)?;

        // Create Transaction<CreateTable> with the effective table configuration
        Transaction::try_new_create_table(
            table_configuration,
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

    use rstest::rstest;

    use super::*;
    use crate::expressions::ColumnName;
    use crate::scan::data_skipping::stats_schema::StripFieldMetadataTransform;
    use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
    use crate::table_features::FeatureType;
    use crate::table_properties::{
        COLUMN_MAPPING_MAX_COLUMN_ID, ENABLE_ICEBERG_COMPAT_V1, ENABLE_ICEBERG_COMPAT_V3,
        PARQUET_FORMAT_VERSION,
    };
    use crate::transforms::SchemaTransform;
    use crate::utils::test_utils::{
        assert_result_error_with_message, build_complex_nested_kernel_schema,
    };

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
    }

    #[test]
    fn test_parquet_format_version_accepted() {
        let properties =
            HashMap::from([(PARQUET_FORMAT_VERSION.to_string(), "2.12.0".to_string())]);
        let validated = validate_extract_table_features_and_properties(properties).unwrap();
        assert_eq!(
            validated.properties.get(PARQUET_FORMAT_VERSION),
            Some(&"2.12.0".to_string()),
        );
        assert!(validated.reader_features.is_empty());
        assert!(validated.writer_features.is_empty());
    }

    #[test]
    fn test_validate_unsupported_properties() {
        // Delta properties not on allow list are rejected
        let mut properties = HashMap::new();
        properties.insert(ENABLE_ICEBERG_COMPAT_V1.to_string(), "true".to_string());
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Setting delta property 'delta.enableIcebergCompatV1' is not supported",
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
        properties.insert(ENABLE_ICEBERG_COMPAT_V1.to_string(), "true".to_string());
        assert_result_error_with_message(
            validate_extract_table_features_and_properties(properties),
            "Setting delta property 'delta.enableIcebergCompatV1' is not supported",
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

        let dm = validate_clustering_and_make_domain_metadata(
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

        let dm = validate_clustering_and_make_domain_metadata(
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

        let result = validate_clustering_and_make_domain_metadata(
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
        let dm = validate_clustering_and_make_domain_metadata(
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
    #[case::all_nullable(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, true),
            StructField::new("name", DataType::STRING, true),
        ])),
        false,
    )]
    #[case::top_level_non_null(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])),
        true,
    )]
    #[case::nested_non_null(
        Arc::new(StructType::new_unchecked(vec![StructField::new(
            "parent",
            DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                "child",
                DataType::INTEGER,
                false,
            )]))),
            true,
        )])),
        true,
    )]
    #[case::variant_only(
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, true),
            StructField::new("v", DataType::unshredded_variant(), true),
        ])),
        false,
    )]
    fn test_maybe_enable_invariants(#[case] schema: SchemaRef, #[case] expect_invariants: bool) {
        let mut validated = ValidatedTableProperties {
            properties: HashMap::new(),
            reader_features: vec![],
            writer_features: vec![],
        };

        maybe_enable_invariants(&schema, &mut validated);

        assert_eq!(
            validated
                .writer_features
                .contains(&TableFeature::Invariants),
            expect_invariants,
            "Expected Invariants feature presence = {expect_invariants}"
        );
        assert!(
            validated.reader_features.is_empty(),
            "Invariants is writer-only; reader_features should be empty"
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

    #[rstest::rstest]
    #[case::vacuum_protocol_check(TableFeature::VacuumProtocolCheck, "vacuumProtocolCheck")]
    #[case::domain_metadata(TableFeature::DomainMetadata, "domainMetadata")]
    #[case::column_mapping(TableFeature::ColumnMapping, "columnMapping")]
    #[case::in_commit_timestamp(TableFeature::InCommitTimestamp, "inCommitTimestamp")]
    #[case::deletion_vectors(TableFeature::DeletionVectors, "deletionVectors")]
    #[case::v2_checkpoint(TableFeature::V2Checkpoint, "v2Checkpoint")]
    #[case::append_only(TableFeature::AppendOnly, "appendOnly")]
    #[case::change_data_feed(TableFeature::ChangeDataFeed, "changeDataFeed")]
    #[case::type_widening(TableFeature::TypeWidening, "typeWidening")]
    #[case::catalog_managed(TableFeature::CatalogManaged, "catalogManaged")]
    #[case::invariants(TableFeature::Invariants, "invariants")]
    fn test_feature_signal_accepted(#[case] feature: TableFeature, #[case] feature_name: &str) {
        let key = format!("delta.feature.{feature_name}");
        let properties = HashMap::from([(key, "supported".to_string())]);
        let validated = validate_extract_table_features_and_properties(properties).unwrap();

        assert!(
            validated.properties.is_empty(),
            "Feature signal should be removed from properties"
        );
        assert!(
            validated.writer_features.contains(&feature),
            "{feature:?} should be in writer_features"
        );
        match feature.feature_type() {
            FeatureType::ReaderWriter => assert!(
                validated.reader_features.contains(&feature),
                "{feature:?} is ReaderWriter but missing from reader_features"
            ),
            _ => assert!(
                validated.reader_features.is_empty(),
                "{feature:?} is WriterOnly but reader_features is not empty"
            ),
        }
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

    #[test]
    fn test_catalog_managed_auto_enables_ict() {
        let properties = HashMap::from([(
            "delta.feature.catalogManaged".to_string(),
            "supported".to_string(),
        )]);
        let mut validated = validate_extract_table_features_and_properties(properties).unwrap();
        maybe_auto_enable_property_driven_features(&mut validated);
        maybe_enable_ict_for_catalog_managed(&mut validated).unwrap();

        assert!(
            validated
                .writer_features
                .contains(&TableFeature::InCommitTimestamp),
            "ICT should be auto-added to writer_features"
        );
        assert_eq!(
            validated.properties.get(ENABLE_IN_COMMIT_TIMESTAMPS),
            Some(&"true".to_string()),
            "delta.enableInCommitTimestamps should be set to true"
        );
    }

    #[test]
    fn test_catalog_managed_with_ict_true_succeeds() {
        let properties = HashMap::from([
            (
                "delta.feature.catalogManaged".to_string(),
                "supported".to_string(),
            ),
            (
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            ),
        ]);
        let mut validated = validate_extract_table_features_and_properties(properties).unwrap();
        maybe_auto_enable_property_driven_features(&mut validated);
        maybe_enable_ict_for_catalog_managed(&mut validated).unwrap();

        assert!(validated
            .writer_features
            .contains(&TableFeature::InCommitTimestamp));
        assert_eq!(
            validated.properties.get(ENABLE_IN_COMMIT_TIMESTAMPS),
            Some(&"true".to_string()),
        );
    }

    /// Verifies that both activation paths add `RowTracking` and `DomainMetadata` to
    /// `writer_features`. For the feature-signal path, `delta.enableRowTracking` must NOT
    /// be present in the properties (signal grants support, not enablement).
    #[rstest::rstest]
    #[case::enablement_property(
        HashMap::from([(ENABLE_ROW_TRACKING.to_string(), "true".to_string())]),
        true, // enablement property is set
    )]
    #[case::feature_signal(
        HashMap::from([("delta.feature.rowTracking".to_string(), "supported".to_string())]),
        false, // enablement property is NOT set
    )]
    fn test_row_tracking_activation_adds_required_features(
        #[case] properties: HashMap<String, String>,
        #[case] expect_enablement_property: bool,
    ) {
        let mut validated = validate_extract_table_features_and_properties(properties).unwrap();
        maybe_auto_enable_property_driven_features(&mut validated);

        assert!(
            validated
                .writer_features
                .contains(&TableFeature::RowTracking),
            "Expected RowTracking in writer_features"
        );
        assert!(
            validated
                .writer_features
                .contains(&TableFeature::DomainMetadata),
            "Expected DomainMetadata in writer_features"
        );
        assert_eq!(
            validated.properties.contains_key(ENABLE_ROW_TRACKING),
            expect_enablement_property,
            "delta.enableRowTracking presence mismatch"
        );
    }

    #[test]
    fn test_catalog_managed_with_ict_false_fails() {
        let properties = HashMap::from([
            (
                "delta.feature.catalogManaged".to_string(),
                "supported".to_string(),
            ),
            (
                "delta.enableInCommitTimestamps".to_string(),
                "false".to_string(),
            ),
        ]);
        let mut validated = validate_extract_table_features_and_properties(properties).unwrap();
        maybe_auto_enable_property_driven_features(&mut validated);
        let err = maybe_enable_ict_for_catalog_managed(&mut validated).unwrap_err();
        assert!(
            err.to_string().contains("enableInCommitTimestamps"),
            "expected ICT conflict error, got: {err}"
        );
    }

    /// Builds the icebergCompatV3 create-table test schema:
    ///
    /// ```json
    /// {
    ///   "type": "struct",
    ///   "fields": [
    ///     {
    ///       "name": "top",
    ///       "type": {
    ///         "type": "map",
    ///         "keyType":   {"type": "array", "elementType": "integer"},
    ///         "valueType": {"type": "struct", "fields": [{
    ///           "name": "inner",
    ///           "type": {"type": "map", "keyType": "integer",
    ///                    "valueType": {"type": "array", "elementType": "integer"}}
    ///         }]}
    ///       }
    ///     },
    ///     {"name": "region", "type": "string"}
    ///   ]
    /// }
    /// ```
    fn build_iceberg_compat_v3_test_schema() -> SchemaRef {
        let with_metadata =
            build_complex_nested_kernel_schema(ColumnMetadataKey::ColumnMappingNestedIds.as_ref());
        let complex = StripFieldMetadataTransform
            .transform_struct(&with_metadata)
            .into_owned();
        let mut fields: Vec<StructField> = complex.fields().cloned().collect();
        fields.push(StructField::nullable("region", DataType::STRING));
        Arc::new(StructType::try_new(fields).unwrap())
    }

    /// V3 create-table flow with the same schema for minimum and maximum feature sets.
    /// Validates final features, CM id assignments, and nested-id JSON contents.
    #[rstest]
    #[case::v3_only(
        /* extra_props */ &[],
        /* expected_features */ &[
            TableFeature::IcebergCompatV3,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
    )]
    #[case::v3_with_cross_features(
        /* extra_props */ &[
            (ENABLE_DELETION_VECTORS, "true"),
            (ENABLE_IN_COMMIT_TIMESTAMPS, "true"),
            (ENABLE_TYPE_WIDENING, "true"),
            (APPEND_ONLY, "true"),
            (ENABLE_CHANGE_DATA_FEED, "true"),
        ],
        /* expected_features */ &[
            TableFeature::IcebergCompatV3,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
            TableFeature::DeletionVectors,
            TableFeature::InCommitTimestamp,
            TableFeature::TypeWidening,
            TableFeature::AppendOnly,
            TableFeature::ChangeDataFeed,
        ],
    )]
    fn test_create_table_iceberg_compat_v3(
        #[case] extra_props: &[(&str, &str)],
        #[case] expected_features: &[TableFeature],
    ) {
        let schema = build_iceberg_compat_v3_test_schema();
        let mut props: HashMap<String, String> =
            HashMap::from([(ENABLE_ICEBERG_COMPAT_V3.to_string(), "true".to_string())]);
        for (k, v) in extra_props {
            props.insert(k.to_string(), v.to_string());
        }
        let mut validated = validate_extract_table_features_and_properties(props).unwrap();

        // === V3 dependency defaults ===
        let pre_cm = maybe_enable_iceberg_compat_v3_dependencies(&mut validated).unwrap();
        assert_eq!(
            validated
                .properties
                .get(COLUMN_MAPPING_MODE)
                .map(String::as_str),
            Some("name"),
        );
        assert_eq!(
            validated
                .properties
                .get(ENABLE_ROW_TRACKING)
                .map(String::as_str),
            Some("true"),
        );

        // === Column mapping + nested-id assignment ===
        let (effective_schema, mode) =
            maybe_apply_column_mapping_for_table_create(&schema, &mut validated, pre_cm).unwrap();
        assert_eq!(mode, ColumnMappingMode::Name);

        // Spark-aligned two-pass numbering:
        //   Pass 1 (CM ids, DFS over StructFields): top=1, inner=2, region=3.
        //   Pass 2 (nested ids, per StructField, starting from max=3):
        //     top:   {key:4, key.element:5, value:6}
        //     inner: {key:7, value:8, value.element:9}
        let top = effective_schema.field("top").expect("missing top");
        let top_physical = expect_field_id_and_physical_name(top, 1);
        assert_eq!(
            extract_nested_ids(top),
            serde_json::json!({
                format!("{top_physical}.key"): 4,
                format!("{top_physical}.key.element"): 5,
                format!("{top_physical}.value"): 6,
            }),
            "top's nested-ids JSON",
        );

        let DataType::Map(top_map) = top.data_type() else {
            panic!("top must be a map");
        };
        let DataType::Struct(value_struct) = top_map.value_type() else {
            panic!("top's value must be a struct");
        };
        let inner = value_struct.fields().next().expect("missing inner");
        let inner_physical = expect_field_id_and_physical_name(inner, 2);
        assert_eq!(
            extract_nested_ids(inner),
            serde_json::json!({
                format!("{inner_physical}.key"): 7,
                format!("{inner_physical}.value"): 8,
                format!("{inner_physical}.value.element"): 9,
            }),
            "inner's nested-ids JSON",
        );

        let region = effective_schema.field("region").expect("missing region");
        let _ = expect_field_id_and_physical_name(region, 3);
        assert!(!region
            .metadata
            .contains_key(ColumnMetadataKey::ColumnMappingNestedIds.as_ref()));

        // maxColumnId tracks the largest assigned CM id, including nested ids.
        assert_eq!(
            validated
                .properties
                .get(COLUMN_MAPPING_MAX_COLUMN_ID)
                .map(String::as_str),
            Some("9"),
        );

        // === Property-driven feature enablement + Invariants ===
        maybe_enable_invariants(&effective_schema, &mut validated);
        maybe_auto_enable_property_driven_features(&mut validated);

        for f in expected_features {
            assert!(
                validated.writer_features.contains(f) || validated.reader_features.contains(f),
                "expected {f:?} in protocol features (writer={:?}, reader={:?})",
                validated.writer_features,
                validated.reader_features,
            );
        }
        // V3 implies partition materialization without adding the MaterializePartitionColumns flag.
        assert!(
            !validated
                .writer_features
                .contains(&TableFeature::MaterializePartitionColumns),
            "MaterializePartitionColumns flag should not be auto-added by V3",
        );
    }

    /// Property combinations that violate V3's dependency requirements must be rejected by
    /// `maybe_enable_iceberg_compat_v3_dependencies`. Some of these properties (e.g.
    /// `delta.rowTrackingSuspended`, `delta.enableIcebergCompatV1`) aren't in the CREATE-TABLE
    /// allow-list, so we construct `ValidatedTableProperties` directly to get around the
    /// allow-list.
    #[rstest]
    #[case::cm_mode_none(
        &[(COLUMN_MAPPING_MODE, "none")],
        "delta.columnMapping.mode",
    )]
    #[case::row_tracking_false(
        &[(ENABLE_ROW_TRACKING, "false")],
        "delta.enableRowTracking",
    )]
    #[case::row_tracking_suspended(
        &[("delta.rowTrackingSuspended", "true")],
        "delta.rowTrackingSuspended",
    )]
    #[case::v1_concurrent(
        &[(ENABLE_ICEBERG_COMPAT_V1, "true")],
        "delta.enableIcebergCompatV1",
    )]
    #[case::v2_concurrent(
        &[("delta.enableIcebergCompatV2", "true")],
        "delta.enableIcebergCompatV2",
    )]
    fn test_v3_dependencies_rejects_invalid_combinations(
        #[case] extra_props: &[(&str, &str)],
        #[case] expected_substring: &str,
    ) {
        let mut properties: HashMap<String, String> =
            HashMap::from([(ENABLE_ICEBERG_COMPAT_V3.to_string(), "true".to_string())]);
        for (k, v) in extra_props {
            properties.insert(k.to_string(), v.to_string());
        }
        let mut validated = ValidatedTableProperties {
            properties,
            reader_features: Vec::new(),
            writer_features: Vec::new(),
        };
        let err = maybe_enable_iceberg_compat_v3_dependencies(&mut validated).unwrap_err();
        assert!(
            err.to_string().contains(expected_substring),
            "expected error mentioning '{expected_substring}', got: {err}",
        );
    }

    /// `assign_column_mapping_metadata` rejects schemas where any field has pre-populated
    /// nested-ids metadata under either the canonical or the legacy key.
    #[rstest]
    #[case::canonical_key(ColumnMetadataKey::ColumnMappingNestedIds.as_ref())]
    #[case::legacy_key(ColumnMetadataKey::ParquetFieldNestedIds.as_ref())]
    fn test_create_table_v3_rejects_pre_existing_nested_ids(#[case] nested_ids_meta_key: &str) {
        // The fixture's kernel schema already carries the chosen nested-ids key on the
        // `top` and `inner` StructFields, so feeding it through the V3 create path must
        // surface the pre-population error.
        let schema = Arc::new(build_complex_nested_kernel_schema(nested_ids_meta_key));
        let mut validated = validate_extract_table_features_and_properties(HashMap::from([(
            ENABLE_ICEBERG_COMPAT_V3.to_string(),
            "true".to_string(),
        )]))
        .unwrap();
        let pre_cm = maybe_enable_iceberg_compat_v3_dependencies(&mut validated).unwrap();
        let err = maybe_apply_column_mapping_for_table_create(&schema, &mut validated, pre_cm)
            .unwrap_err();
        assert!(
            err.to_string().contains("has pre-populated"),
            "expected pre-populated metadata error, got: {err}",
        );
    }

    /// Listing IcebergCompatV3 in writerFeatures (i.e. "supported") without setting
    /// `delta.enableIcebergCompatV3=true` does NOT activate V3 behavior: column mapping is
    /// applied per the explicit property, but no nested ids are set on Array/Map fields.
    #[test]
    fn test_create_table_v3_supported_but_not_enabled_skips_nested_ids() {
        let schema = build_iceberg_compat_v3_test_schema();
        let mut validated = validate_extract_table_features_and_properties(HashMap::from([
            (
                "delta.feature.icebergCompatV3".to_string(),
                "supported".to_string(),
            ),
            (COLUMN_MAPPING_MODE.to_string(), "name".to_string()),
        ]))
        .unwrap();
        assert!(
            validated
                .writer_features
                .contains(&TableFeature::IcebergCompatV3),
            "V3 must be in writerFeatures (supported) for this test to be meaningful",
        );
        let pre_cm = maybe_enable_iceberg_compat_v3_dependencies(&mut validated).unwrap();
        let (effective_schema, mode) =
            maybe_apply_column_mapping_for_table_create(&schema, &mut validated, pre_cm).unwrap();
        assert_eq!(mode, ColumnMappingMode::Name);

        // Top-level Map field gets CM id + physicalName but no nested-ids metadata under
        // either the canonical or the legacy key.
        let top = effective_schema.field("top").expect("missing field top");
        assert!(
            !top.metadata()
                .contains_key(ColumnMetadataKey::ColumnMappingNestedIds.as_ref()),
            "top should not carry delta.columnMapping.nested.ids when V3 is not enabled",
        );
        assert!(
            !top.metadata()
                .contains_key(ColumnMetadataKey::ParquetFieldNestedIds.as_ref()),
            "top should not carry parquet.field.nested.ids when V3 is not enabled",
        );
    }

    /// Asserts the field has CM id == `expected_id` and a `col-` prefixed physical name; returns
    /// the physical name for further use.
    fn expect_field_id_and_physical_name(field: &StructField, expected_id: i64) -> String {
        assert_eq!(
            field
                .metadata
                .get(ColumnMetadataKey::ColumnMappingId.as_ref()),
            Some(&MetadataValue::Number(expected_id)),
            "field '{}' CM id mismatch",
            field.name(),
        );
        let MetadataValue::String(physical_name) = field
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap_or_else(|| panic!("field '{}' missing physicalName", field.name()))
        else {
            panic!("field '{}' physicalName must be a String", field.name());
        };
        assert!(
            physical_name.starts_with("col-"),
            "field '{}' physicalName '{physical_name}' must start with 'col-'",
            field.name(),
        );
        physical_name.clone()
    }

    /// Extracts the `delta.columnMapping.nested.ids` JSON object from a field, panicking if
    /// it's missing or wrongly typed.
    fn extract_nested_ids(field: &StructField) -> serde_json::Value {
        let MetadataValue::Other(value) = field
            .metadata
            .get(ColumnMetadataKey::ColumnMappingNestedIds.as_ref())
            .unwrap_or_else(|| panic!("field '{}' missing nested-ids", field.name()))
        else {
            panic!("field '{}' nested-ids must be a JSON object", field.name());
        };
        value.clone()
    }
}
