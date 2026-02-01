//! Built-in transforms for table creation.
//!
//! This module contains the standard transforms that are applied during table creation:
//!
//! - [`FeatureSignalTransform`]: Processes `delta.feature.X=supported` signals
//! - [`ProtocolVersionTransform`]: Sets protocol version from properties or defaults
//! - [`DeltaPropertyValidationTransform`]: Validates `delta.*` properties are allowed
//!
//! NOTE: This is the stub implementation. Full implementations are in a follow-up commit.

use std::collections::HashMap;

use crate::DeltaResult;

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

use super::{ProtocolMetadataTransform, TransformId};

// ============================================================================
// FeatureSignalTransform (Stub)
// ============================================================================

/// Processes delta.feature.X=supported signals.
///
/// This transform:
/// 1. Scans properties for delta.feature.X patterns
/// 2. Validates values are "supported"
/// 3. Adds features to the protocol
/// 4. Strips the signals from metadata (they shouldn't be stored)
#[derive(Debug)]
#[allow(dead_code)] // Constructed by registry
pub(crate) struct FeatureSignalTransform {
    /// Parsed feature signals (feature name, original property key)
    pub(super) signals: Vec<(crate::table_features::TableFeature, String)>,
}

impl FeatureSignalTransform {
    /// Create a transform from raw properties if any feature signals are present.
    ///
    /// Returns `None` if no `delta.feature.*` signals are found in the properties.
    /// Returns `Err` if signals are found but have invalid values or unknown features.
    #[allow(unused_variables)]
    #[allow(dead_code)]
    pub(crate) fn new(properties: &HashMap<String, String>) -> DeltaResult<Option<Self>> {
        // Stub: no feature signals processed yet
        Ok(None)
    }
}

impl ProtocolMetadataTransform for FeatureSignalTransform {
    fn id(&self) -> TransformId {
        TransformId::FeatureSignals
    }

    fn name(&self) -> &'static str {
        "FeatureSignals: processes delta.feature.* signals"
    }

    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
        // Stub: pass through unchanged
        Ok(config)
    }
}

// ============================================================================
// ProtocolVersionTransform (Stub)
// ============================================================================

/// Determines and sets the protocol version from user properties.
///
/// This transform:
/// 1. Parses version properties (defaults to 3/7 if not specified)
/// 2. Validates they are supported (only 3/7 currently)
/// 3. Sets the protocol version
/// 4. Strips the version properties from metadata (transient signals)
#[derive(Debug)]
#[allow(dead_code)] // Constructed by registry
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
        config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
        // Stub: pass through unchanged (version already set in Protocol::new())
        Ok(config)
    }
}

// ============================================================================
// DeltaPropertyValidationTransform (Stub)
// ============================================================================

/// Validates that all delta.* properties are supported.
///
/// This transform runs first to reject unsupported properties early, before
/// any other transforms process the configuration.
#[derive(Debug)]
#[allow(dead_code)] // Constructed by registry
pub(crate) struct DeltaPropertyValidationTransform;

impl ProtocolMetadataTransform for DeltaPropertyValidationTransform {
    fn id(&self) -> TransformId {
        TransformId::DeltaPropertyValidation
    }

    fn name(&self) -> &'static str {
        "DeltaPropertyValidation: validates delta.* properties"
    }

    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
        // Stub: pass through unchanged
        Ok(config)
    }
}
