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

use super::{ProtocolMetadataTransform, TransformId};

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

    fn apply(
        &self,
        mut config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
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

        Ok(config)
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

    fn apply(
        &self,
        mut config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
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

        Ok(config)
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

    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
        // Validation-only transform - no modifications
        Ok(config)
    }
}
