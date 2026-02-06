//! Transform pipeline for table creation and modification.
//!
//! This module provides the infrastructure for applying transformations to
//! [`TableProtocolMetadataConfig`] during table creation. Transforms can:
//!
//! - Add features to the protocol based on properties or schema
//! - Validate and process delta.* properties
//! - Strip signal flags from metadata
//!
//! See [`TransformationPipeline::apply_transforms`] for the main entry point.

mod registry;
mod transforms;

use crate::DeltaResult;

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

// Re-export for use by the pipeline
#[allow(unused_imports)]
pub(crate) use registry::TransformContext;
#[allow(unused_imports)]
pub(crate) use registry::TRANSFORM_REGISTRY;

// ============================================================================
// Transform Types
// ============================================================================

/// Canonical identifier for each transform type.
///
/// Used for:
/// - Dependency declarations (compile-time safe)
/// - Tracking completed transforms
/// - Topological sort ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)] // Used by TransformationPipeline
pub(crate) enum TransformId {
    /// Validates delta.* properties are allowed
    DeltaPropertyValidation,
    /// Sets protocol version from properties or defaults
    ProtocolVersion,
    /// Processes delta.feature.X=supported signals
    FeatureSignals,
}

/// Dependencies that must be satisfied before a transform can run.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Used by TransformationPipeline
pub(crate) enum TransformDependency {
    /// Hard ordering: This transform MUST be in the pipeline and run before me.
    #[allow(dead_code)]
    TransformRequired(TransformId),

    /// Soft ordering: If this transform is in the pipeline, run it before me.
    #[allow(dead_code)]
    TransformCompletedIfPresent(TransformId),
}

// ============================================================================
// Transform Trait
// ============================================================================

/// A transformation step that modifies protocol and/or metadata.
///
/// Transforms are registered in a central registry and applied by the
/// [`TransformationPipeline`]. Each transform declares its dependencies
/// and validation logic.
#[allow(dead_code)] // Trait methods used by TransformationPipeline
pub(crate) trait ProtocolMetadataTransform: std::fmt::Debug {
    /// Canonical identifier for this transform type.
    fn id(&self) -> TransformId;

    /// Human-readable description of this transform.
    fn name(&self) -> &'static str;

    /// Dependencies that must be satisfied before this transform can run.
    fn dependencies(&self) -> &'static [TransformDependency] {
        &[]
    }

    /// Pre-validate the configuration before applying this transform.
    fn validate_preconditions(
        &self,
        _config: &TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<()> {
        Ok(())
    }

    /// Apply the transformation to protocol and metadata.
    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
        context: &TransformContext<'_>,
    ) -> DeltaResult<TableProtocolMetadataConfig>;

    /// Validate the config AFTER this transform has been applied.
    fn validate_postconditions(&self, _config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
        Ok(())
    }
}

// ============================================================================
// Transformation Pipeline (stub - full implementation in follow-up commit)
// ============================================================================

/// Pipeline that applies transforms in dependency order.
#[allow(dead_code)]
pub(crate) struct TransformationPipeline;

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
}
