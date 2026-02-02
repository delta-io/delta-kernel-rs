//! Built-in transforms for table creation.
//!
//! This module contains the standard transforms that are applied during table creation:
//!
//! - [`ProtocolVersionTransform`]: Sets protocol version from properties or defaults
//! - [`DeltaPropertyValidationTransform`]: Validates `delta.*` properties are allowed

use crate::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
use crate::{DeltaResult, Error};

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

use super::{
    ProtocolMetadataTransform, TransformContext, TransformDependency, TransformId, TransformOutput,
};

// ============================================================================
// ProtocolVersionTransform
// ============================================================================

/// Determines and sets the protocol version from user properties.
///
/// This transform:
/// 1. Reads version properties from TransformContext (raw user properties)
/// 2. Parses version properties (defaults to 3/7 if not specified)
/// 3. Validates they are supported (only 3/7 currently)
/// 4. Sets the protocol version
///
/// Note: Version properties are NOT stored in metadata - they are filtered out
/// in `TableProtocolMetadataConfig::new_base_for_create()`. This transform reads
/// them from `TransformContext.properties` which contains all raw user properties.
///
/// The transform always runs, whether or not version properties are specified,
/// because protocol needs a version set. If no properties, defaults to 3/7.
#[derive(Debug)]
pub(crate) struct ProtocolVersionTransform;

impl ProtocolMetadataTransform for ProtocolVersionTransform {
    fn id(&self) -> TransformId {
        TransformId::ProtocolVersion
    }

    fn name(&self) -> &'static str {
        "ProtocolVersion: sets version from properties or defaults"
    }

    fn apply(
        &self,
        mut config: TableProtocolMetadataConfig,
        context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        // Read version properties from context (raw user properties)
        // not from config.metadata which only has user properties (delta.* filtered)

        // Parse reader version (default to 3 for table features support)
        let reader_version = context
            .properties
            .get(MIN_READER_VERSION_PROP)
            .map(|v| {
                v.parse::<u8>().map_err(|_| {
                    Error::generic(format!(
                        "Invalid value '{}' for {}. Must be an integer.",
                        v, MIN_READER_VERSION_PROP
                    ))
                })
            })
            .transpose()?
            .unwrap_or(TABLE_FEATURES_MIN_READER_VERSION as u8);

        // Parse writer version (default to 7 for table features support)
        let writer_version = context
            .properties
            .get(MIN_WRITER_VERSION_PROP)
            .map(|v| {
                v.parse::<u8>().map_err(|_| {
                    Error::generic(format!(
                        "Invalid value '{}' for {}. Must be an integer.",
                        v, MIN_WRITER_VERSION_PROP
                    ))
                })
            })
            .transpose()?
            .unwrap_or(TABLE_FEATURES_MIN_WRITER_VERSION as u8);

        // Validate versions: currently only support 3/7 (table features protocol)
        // Supporting older protocol versions would require different handling for features
        if reader_version != TABLE_FEATURES_MIN_READER_VERSION as u8 {
            return Err(Error::generic(format!(
                "Invalid value '{}' for {}. Only '{}' is supported for CREATE TABLE.",
                reader_version, MIN_READER_VERSION_PROP, TABLE_FEATURES_MIN_READER_VERSION
            )));
        }
        if writer_version != TABLE_FEATURES_MIN_WRITER_VERSION as u8 {
            return Err(Error::generic(format!(
                "Invalid value '{}' for {}. Only '{}' is supported for CREATE TABLE.",
                writer_version, MIN_WRITER_VERSION_PROP, TABLE_FEATURES_MIN_WRITER_VERSION
            )));
        }

        // Update the protocol with the validated versions
        config.protocol = config
            .protocol
            .with_versions(reader_version.into(), writer_version.into())?;

        // No need to strip version properties from metadata - they were already
        // filtered out in new_base_for_create()

        Ok(TransformOutput::new(config))
    }
}

// ============================================================================
// DeltaPropertyValidationTransform
// ============================================================================

/// Validates that all delta.* properties are supported.
///
/// This transform runs first to reject unsupported properties early, before
/// any other transforms process the configuration. It ensures users don't
/// accidentally think a property is being used when it's actually ignored.
///
/// Allowed property patterns:
/// - `delta.feature.*` - Feature signal flags
/// - `delta.minReaderVersion` - Protocol version (processed by ProtocolVersionTransform)
/// - `delta.minWriterVersion` - Protocol version (processed by ProtocolVersionTransform)
/// - Non-delta properties (e.g., `myapp.version`) - passed through unmodified
#[derive(Debug)]
pub(crate) struct DeltaPropertyValidationTransform;

impl ProtocolMetadataTransform for DeltaPropertyValidationTransform {
    fn id(&self) -> TransformId {
        TransformId::DeltaPropertyValidation
    }

    fn name(&self) -> &'static str {
        "DeltaPropertyValidation: validates delta.* properties"
    }

    fn validate_preconditions(
        &self,
        _config: &TableProtocolMetadataConfig,
        context: &TransformContext<'_>,
    ) -> DeltaResult<()> {
        use crate::table_features::{
            TableFeature, SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
        };

        // Validate against raw user properties from context
        // (config.metadata only has user properties with delta.* filtered out)
        for (key, value) in context.properties.iter() {
            // Check feature signals specially for better error messages
            if let Some(feature_name) = key.strip_prefix(SET_TABLE_FEATURE_SUPPORTED_PREFIX) {
                // First validate the value is "supported"
                if value != SET_TABLE_FEATURE_SUPPORTED_VALUE {
                    return Err(Error::generic(format!(
                        "Invalid value '{}' for {}. Only '{}' is allowed.",
                        value, key, SET_TABLE_FEATURE_SUPPORTED_VALUE
                    )));
                }

                // Then check if the feature is allowed
                if let Ok(feature) = feature_name.parse::<TableFeature>() {
                    if !TableProtocolMetadataConfig::is_delta_feature_allowed(&feature) {
                        return Err(Error::generic(format!(
                            "Enabling feature '{}' is not supported during CREATE TABLE",
                            feature_name
                        )));
                    }
                } else {
                    // Unknown feature name
                    return Err(Error::generic(format!(
                        "Unknown feature '{}' in property '{}'",
                        feature_name, key
                    )));
                }
                continue;
            }

            // For other properties, use the general validation
            if !TableProtocolMetadataConfig::is_delta_property_allowed(key) {
                return Err(Error::generic(format!(
                    "Property '{}' is not supported during CREATE TABLE. \
                     Only delta.feature.* signals and delta.minReaderVersion/delta.minWriterVersion are allowed.",
                    key
                )));
            }
        }
        Ok(())
    }

    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        // Validation-only transform - no modifications
        Ok(TransformOutput::new(config))
    }
}

// ============================================================================
// DomainMetadataTransform
// ============================================================================

/// Enables the DomainMetadata writer feature.
///
/// This transform is added as a dependency when clustering is enabled,
/// since clustering writes domain metadata.
#[derive(Debug)]
pub(crate) struct DomainMetadataTransform;

impl ProtocolMetadataTransform for DomainMetadataTransform {
    fn id(&self) -> TransformId {
        TransformId::DomainMetadata
    }

    fn name(&self) -> &'static str {
        "DomainMetadata: enables domain metadata feature"
    }

    fn apply(
        &self,
        mut config: TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        // Add DomainMetadata writer feature
        if !config
            .protocol
            .has_table_feature(&TableFeature::DomainMetadata)
        {
            config.protocol = config.protocol.with_feature(TableFeature::DomainMetadata)?;
        }
        Ok(TransformOutput::new(config))
    }
}

// ============================================================================
// ColumnMappingTransform
// ============================================================================

/// Enables column mapping with optional schema transformation.
///
/// This transform handles two activation paths:
///
/// 1. **Feature signal only** (`delta.feature.columnMapping=supported`):
///    - Adds `columnMapping` to protocol reader/writer features
///    - Does NOT transform the schema (no IDs or physical names assigned)
///    - Table supports column mapping but it's not active
///
/// 2. **Mode property** (`delta.columnMapping.mode=name|id`):
///    - Adds `columnMapping` to protocol reader/writer features
///    - Transforms the schema (assigns IDs and physical names)
///    - Sets `delta.columnMapping.maxColumnId` property
///    - Column mapping is fully active
///
/// The transform determines which path by checking if the mode property is set
/// and is not `none`.
#[derive(Debug)]
pub(crate) struct ColumnMappingTransform;

impl ProtocolMetadataTransform for ColumnMappingTransform {
    fn id(&self) -> TransformId {
        TransformId::ColumnMapping
    }

    fn name(&self) -> &'static str {
        "ColumnMapping: enables column mapping feature and optionally transforms schema"
    }

    fn apply(
        &self,
        mut config: TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        use crate::table_features::{
            assign_column_mapping_metadata, get_column_mapping_mode_from_properties,
            ColumnMappingMode, COLUMN_MAPPING_MAX_COLUMN_ID_KEY,
        };

        // Always add the columnMapping feature to protocol (idempotent)
        if !config
            .protocol
            .has_table_feature(&TableFeature::ColumnMapping)
        {
            config.protocol = config.protocol.with_feature(TableFeature::ColumnMapping)?;
        }

        // Check if mode is actually set (not just the feature signal)
        let mode = get_column_mapping_mode_from_properties(config.metadata.configuration())?;

        match mode {
            ColumnMappingMode::None => {
                // Path 1: Feature signal only - just update protocol, no schema changes
                Ok(TransformOutput::new(config))
            }
            ColumnMappingMode::Name | ColumnMappingMode::Id => {
                // Path 2: Full activation - update protocol AND transform schema
                let schema = config.metadata.parse_schema()?;
                let mut max_id = 0i64;
                let new_schema = assign_column_mapping_metadata(&schema, &mut max_id)?;

                // Update metadata with new schema and maxColumnId
                config.metadata = config
                    .metadata
                    .with_schema(new_schema)?
                    .with_configuration_value(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, max_id.to_string());

                Ok(TransformOutput::new(config))
            }
        }
    }
}

// ============================================================================
// PartitioningTransform
// ============================================================================

/// Sets partition columns on the table metadata.
///
/// Validates that partition columns exist in the schema and are top-level columns.
#[derive(Debug)]
pub(crate) struct PartitioningTransform {
    columns: Vec<crate::schema::ColumnName>,
}

impl PartitioningTransform {
    pub(crate) fn new(columns: Vec<crate::schema::ColumnName>) -> Self {
        Self { columns }
    }
}

impl ProtocolMetadataTransform for PartitioningTransform {
    fn id(&self) -> TransformId {
        TransformId::Partitioning
    }

    fn name(&self) -> &'static str {
        "Partitioning: sets partition columns"
    }

    fn validate_preconditions(
        &self,
        config: &TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<()> {
        let schema = config.metadata.parse_schema()?;
        for col in &self.columns {
            // Partition columns must be top-level (single path element)
            if col.path().len() != 1 {
                return Err(Error::generic(format!(
                    "Partition column '{}' must be a top-level column, not a nested path",
                    col
                )));
            }

            let col_name = &col.path()[0];
            if schema.field(col_name).is_none() {
                return Err(Error::generic(format!(
                    "Partition column '{}' not found in schema",
                    col_name
                )));
            }
        }
        Ok(())
    }

    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        let partition_columns: Vec<String> =
            self.columns.iter().map(|c| c.path()[0].clone()).collect();

        Ok(TransformOutput::new(
            config.with_partition_columns(partition_columns),
        ))
    }
}

// ============================================================================
// ClusteringTransform
// ============================================================================

/// Enables clustering with domain metadata.
///
/// This transform:
/// 1. Validates clustering columns exist in schema
/// 2. Validates column count is within limits
/// 3. Adds ClusteredTable writer feature
/// 4. Creates delta.clustering domain metadata
///
/// Requires DomainMetadata transform to run first (declared as dependency).
#[derive(Debug)]
pub(crate) struct ClusteringTransform {
    columns: Vec<crate::schema::ColumnName>,
}

impl ClusteringTransform {
    pub(crate) fn new(columns: Vec<crate::schema::ColumnName>) -> Self {
        Self { columns }
    }
}

impl ProtocolMetadataTransform for ClusteringTransform {
    fn id(&self) -> TransformId {
        TransformId::Clustering
    }

    fn name(&self) -> &'static str {
        "Clustering: enables clustered table"
    }

    fn dependencies(&self) -> Vec<TransformDependency> {
        vec![
            // Clustering requires DomainMetadata to be enabled first
            TransformDependency::TransformRequired(TransformId::DomainMetadata),
            // If column mapping is in the pipeline, it should run before clustering
            // so we can use physical column names in domain metadata
            TransformDependency::TransformCompletedIfPresent(TransformId::ColumnMapping),
        ]
    }

    fn validate_preconditions(
        &self,
        config: &TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<()> {
        use crate::transaction::data_layout::MAX_CLUSTERING_COLUMNS;

        // Validate column count
        if self.columns.len() > MAX_CLUSTERING_COLUMNS {
            return Err(Error::generic(format!(
                "Clustering supports at most {} columns, but {} were specified",
                MAX_CLUSTERING_COLUMNS,
                self.columns.len()
            )));
        }

        // Validate columns exist in schema
        let schema = config.metadata.parse_schema()?;
        for col in &self.columns {
            // Clustering columns must be top-level (single path element)
            if col.path().len() != 1 {
                return Err(Error::generic(format!(
                    "Clustering column '{}' must be a top-level column, not a nested path",
                    col
                )));
            }

            let col_name = &col.path()[0];
            if schema.field(col_name).is_none() {
                return Err(Error::generic(format!(
                    "Clustering column '{}' not found in schema",
                    col_name
                )));
            }
        }

        Ok(())
    }

    fn apply(
        &self,
        mut config: TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        use crate::actions::DomainMetadata;
        use crate::clustering::{ClusteringDomainMetadata, CLUSTERING_DOMAIN_NAME};
        use crate::table_features::{
            get_column_mapping_mode_from_properties, resolve_logical_to_physical_path,
        };

        // Add ClusteredTable writer feature
        if !config
            .protocol
            .has_table_feature(&TableFeature::ClusteredTable)
        {
            config.protocol = config.protocol.with_feature(TableFeature::ClusteredTable)?;
        }

        // Get column mapping mode to determine if we need to resolve physical names
        // Note: If ColumnMappingTransform has run, the schema already has physical names
        // assigned, and we need to use them in the domain metadata
        let column_mapping_mode =
            get_column_mapping_mode_from_properties(config.metadata.configuration())?;
        let schema = config.metadata.parse_schema()?;

        // Create clustering domain metadata
        // When column mapping is enabled, we store physical column names in domain metadata
        // so the clustering metadata remains valid after column renames
        let column_paths: Vec<Vec<String>> = self
            .columns
            .iter()
            .map(|c| resolve_logical_to_physical_path(c.path(), &schema, column_mapping_mode))
            .collect::<DeltaResult<Vec<_>>>()?;

        let clustering_metadata = ClusteringDomainMetadata {
            clustering_columns: column_paths,
        };

        let domain_metadata = DomainMetadata::new(
            CLUSTERING_DOMAIN_NAME.to_string(),
            serde_json::to_string(&clustering_metadata).map_err(|e| {
                Error::generic(format!("Failed to serialize clustering metadata: {}", e))
            })?,
        );

        Ok(TransformOutput::with_domain_metadata(
            config,
            vec![domain_metadata],
        ))
    }
}
