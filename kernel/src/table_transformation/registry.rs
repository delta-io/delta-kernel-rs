//! Transform registry for selecting applicable transforms.
//!
//! The [`TransformRegistry`] manages transform registrations. Each registration
//! associates a trigger condition with a factory function to create the transform.
//!
//! See [`TRANSFORM_REGISTRY`] for the singleton instance.

use std::sync::LazyLock;

use super::ProtocolMetadataTransform;

/// Factory function to create a transform instance.
#[allow(dead_code)]
type TransformFactory = fn() -> Box<dyn ProtocolMetadataTransform>;

// ============================================================================
// Registry (stub - full implementation in follow-up commit)
// ============================================================================

/// The global transform registry singleton.
///
/// Contains all registered transforms that can be triggered during table creation.
#[allow(dead_code)] // Used by TransformationPipeline
pub(crate) static TRANSFORM_REGISTRY: LazyLock<TransformRegistry> =
    LazyLock::new(TransformRegistry::new);

/// Registry for transform registrations.
#[derive(Debug)]
#[allow(dead_code)] // Constructed by LazyLock
pub(crate) struct TransformRegistry;

#[allow(dead_code)] // Methods used by pipeline
impl TransformRegistry {
    fn new() -> Self {
        Self
    }
}
