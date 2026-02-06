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

use super::{ProtocolMetadataTransform, TransformContext, TransformId, TransformOutput};

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
