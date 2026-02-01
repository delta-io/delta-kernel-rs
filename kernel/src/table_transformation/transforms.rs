//! Built-in transforms for table creation.
//!
//! This module contains the standard transforms that are applied during table creation:
//!
//! - [`ProtocolVersionTransform`]: Sets protocol version from properties or defaults
//! - [`DeltaPropertyValidationTransform`]: Validates `delta.*` properties are allowed
//!
//! NOTE: This is the stub implementation. Full implementations are in a follow-up commit.

use crate::DeltaResult;

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

use super::{ProtocolMetadataTransform, TransformContext, TransformId};

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
        _context: &TransformContext<'_>,
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
        _context: &TransformContext<'_>,
    ) -> DeltaResult<TableProtocolMetadataConfig> {
        // Stub: pass through unchanged
        Ok(config)
    }
}
