//! Built-in transforms for table creation.
//!
//! This module contains the standard transforms that are applied during table creation:
//!
//! - [`FeatureSignalTransform`]: Processes `delta.feature.X=supported` signals
//! - [`ProtocolVersionTransform`]: Sets protocol version from properties or defaults
//! - [`DeltaPropertyValidationTransform`]: Validates `delta.*` properties are allowed

use std::collections::HashMap;

use crate::table_features::{
    TableFeature, SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
    TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
use crate::{DeltaResult, Error};

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

use super::{ProtocolMetadataTransform, TransformId, TransformOutput};

// ============================================================================
// FeatureSignalTransform
// ============================================================================

/// Processes delta.feature.X=supported signals.
///
/// This transform:
/// 1. Scans properties for delta.feature.X patterns
/// 2. Validates values are "supported"
/// 3. Adds features to the protocol
/// 4. Strips the signals from metadata (they shouldn't be stored)
#[derive(Debug)]
pub(crate) struct FeatureSignalTransform {
    /// Parsed feature signals (feature name, original property key)
    pub(super) signals: Vec<(TableFeature, String)>,
}

impl FeatureSignalTransform {
    /// Create a transform from raw properties if any feature signals are present.
    ///
    /// Returns `None` if no `delta.feature.*` signals are found in the properties.
    /// Returns `Err` if signals are found but have invalid values or unknown features.
    pub(crate) fn new(properties: &HashMap<String, String>) -> DeltaResult<Option<Self>> {
        let signals = Self::parse_feature_signals(properties)?;
        Ok((!signals.is_empty()).then_some(Self { signals }))
    }

    /// Parse and validate feature signals from properties.
    fn parse_feature_signals(
        properties: &HashMap<String, String>,
    ) -> DeltaResult<Vec<(TableFeature, String)>> {
        let mut signals = Vec::new();

        for (key, value) in properties {
            if let Some(feature_name) = key.strip_prefix(SET_TABLE_FEATURE_SUPPORTED_PREFIX) {
                // Validate value is "supported"
                if value != SET_TABLE_FEATURE_SUPPORTED_VALUE {
                    return Err(Error::generic(format!(
                        "Invalid value '{}' for '{}'. Only '{}' is allowed.",
                        value, key, SET_TABLE_FEATURE_SUPPORTED_VALUE
                    )));
                }

                // Parse feature name
                let feature = TableFeature::from_name(feature_name);
                if matches!(feature, TableFeature::Unknown(_)) {
                    return Err(Error::generic(format!(
                        "Unknown table feature '{}'. Cannot create table with unsupported features.",
                        feature_name
                    )));
                }

                signals.push((feature, key.clone()));
            }
        }

        Ok(signals)
    }
}

impl ProtocolMetadataTransform for FeatureSignalTransform {
    fn id(&self) -> TransformId {
        TransformId::FeatureSignals
    }

    fn name(&self) -> &'static str {
        "FeatureSignals: processes delta.feature.* signals"
    }

    fn validate_preconditions(&self, _config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
        // Validate all requested features are in the allowed list
        for (feature, _key) in &self.signals {
            if !TableProtocolMetadataConfig::is_delta_feature_allowed(feature) {
                return Err(Error::generic(format!(
                    "Enabling feature '{}' is not supported during CREATE TABLE",
                    feature
                )));
            }
        }
        Ok(())
    }

    fn apply(&self, mut config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
        // Add each feature to the protocol
        // Note: with_feature() is idempotent - if feature already exists, it's a no-op.
        // This handles the case where a feature was:
        // 1. Explicitly requested by the user AND auto-added as a dependency
        // 2. Already enabled on the table (for ALTER TABLE scenarios)
        for (feature, _key) in &self.signals {
            // Skip if feature is already enabled (explicit idempotency check for clarity)
            if config.protocol.has_table_feature(feature) {
                continue;
            }
            config.protocol = config.protocol.with_feature(feature.clone())?;
        }

        // Strip signal flags from metadata - they are transient signals, not table properties
        // Per Delta protocol, delta.feature.X=supported is only used during CREATE TABLE
        // to enable features; it should NOT be persisted in the table's metadata.
        let mut filtered_config = config.metadata.configuration().clone();
        for (_feature, key) in &self.signals {
            filtered_config.remove(key);
        }
        config.metadata = config.metadata.with_configuration(filtered_config);

        Ok(TransformOutput::new(config))
    }
}

// ============================================================================
// ProtocolVersionTransform
// ============================================================================

/// Determines and sets the protocol version from user properties.
///
/// This transform:
/// 1. Parses version properties (defaults to 3/7 if not specified)
/// 2. Validates they are supported (only 3/7 currently)
/// 3. Sets the protocol version
/// 4. Strips the version properties from metadata (transient signals)
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

    fn apply(&self, mut config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
        let metadata_config = config.metadata.configuration();

        // Parse reader version (default to 3 for table features support)
        let reader_version = metadata_config
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
        let writer_version = metadata_config
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
        config.protocol =
            config
                .protocol
                .with_versions(reader_version.into(), writer_version.into())?;

        // Strip version properties from metadata - they are transient signals
        // The version is encoded in the Protocol action, not in table properties
        let mut filtered_config = config.metadata.configuration().clone();
        filtered_config.remove(MIN_READER_VERSION_PROP);
        filtered_config.remove(MIN_WRITER_VERSION_PROP);
        config.metadata = config.metadata.with_configuration(filtered_config);

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
/// - `delta.feature.*` - Feature signal flags (processed by FeatureSignalTransform)
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

    fn validate_preconditions(&self, config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
        for key in config.metadata.configuration().keys() {
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

    fn apply(&self, config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
        // Validation-only transform - no modifications
        Ok(TransformOutput::new(config))
    }
}

// ============================================================================
// PartitioningTransform
// ============================================================================

/// Sets partition columns on metadata.
///
/// This transform:
/// 1. Validates partition columns exist in schema (top-level only)
/// 2. Sets the partition_columns on metadata
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

    fn validate_preconditions(&self, config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
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

    fn apply(&self, config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
        let partition_columns: Vec<String> = self
            .columns
            .iter()
            .map(|c| c.path()[0].clone())
            .collect();

        Ok(TransformOutput::new(
            config.with_partition_columns(partition_columns),
        ))
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

    fn apply(&self, mut config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
        // Add DomainMetadata writer feature
        if !config.protocol.has_table_feature(&TableFeature::DomainMetadata) {
            config.protocol = config.protocol.with_feature(TableFeature::DomainMetadata)?;
        }
        Ok(TransformOutput::new(config))
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

    fn dependencies(&self) -> Vec<super::TransformDependency> {
        // Clustering requires DomainMetadata to be enabled first
        vec![super::TransformDependency::TransformRequired(
            TransformId::DomainMetadata,
        )]
    }

    fn validate_preconditions(&self, config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
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

    fn apply(&self, mut config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
        use crate::actions::DomainMetadata;
        use crate::clustering::{ClusteringMetadataDomain, CLUSTERING_DOMAIN_NAME};

        // Add ClusteredTable writer feature
        if !config.protocol.has_table_feature(&TableFeature::ClusteredTable) {
            config.protocol = config.protocol.with_feature(TableFeature::ClusteredTable)?;
        }

        // Create clustering domain metadata
        let column_names: Vec<String> = self
            .columns
            .iter()
            .map(|c| c.path()[0].clone())
            .collect();

        let clustering_metadata = ClusteringMetadataDomain {
            clustering_columns: column_names,
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
