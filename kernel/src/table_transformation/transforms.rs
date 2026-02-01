//! Built-in transforms for table creation.
//!
//! This module contains the standard transforms that are applied during table creation:
//!
//! - [`FeatureSignalTransform`]: Processes `delta.feature.X=supported` signals
//! - [`ProtocolVersionTransform`]: Sets protocol version from properties or defaults
//! - [`DeltaPropertyValidationTransform`]: Validates `delta.*` properties are allowed
//!
//! NOTE: This is the stub implementation. Full implementations are in a follow-up commit.

use crate::DeltaResult;

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

use super::{ProtocolMetadataTransform, TransformId};

// ============================================================================
// FeatureSignalTransform (Stub)
// ============================================================================

/// Processes delta.feature.X=supported signals.
#[derive(Debug)]
#[allow(dead_code)] // Constructed by registry
pub(crate) struct FeatureSignalTransform;

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
        // Stub: pass through unchanged
        Ok(config)
    }
}

// ============================================================================
// DeltaPropertyValidationTransform (Stub)
// ============================================================================

/// Validates that delta.* properties are allowed for table creation.
#[derive(Debug)]
#[allow(dead_code)] // Constructed by registry
pub(crate) struct DeltaPropertyValidationTransform;

impl ProtocolMetadataTransform for DeltaPropertyValidationTransform {
    fn id(&self) -> TransformId {
        TransformId::DeltaPropertyValidation
    }

    fn name(&self) -> &'static str {
        "DeltaPropertyValidation: validates delta.* properties are allowed"
    }

    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
        // Stub: pass through unchanged (no validation yet)
        Ok(config)
    }
}
