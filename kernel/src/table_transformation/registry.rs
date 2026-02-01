//! Transform registry for selecting applicable transforms.
//!
//! The [`TransformRegistry`] manages transform registrations. Each registration
//! associates a trigger condition with a factory function to create the transform.
//!
//! See [`TRANSFORM_REGISTRY`] for the singleton instance.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::ProtocolMetadataTransform;

// ============================================================================
// Transform Context
// ============================================================================

/// Runtime context passed to transforms.
///
/// Contains all user-provided properties including delta.* signals.
/// Transforms should read properties from this context rather than from
/// the metadata configuration.
#[derive(Debug)]
#[allow(dead_code)] // Fields accessed by transforms
pub(crate) struct TransformContext<'a> {
    /// Raw properties from the user (includes delta.* signals)
    pub properties: &'a HashMap<String, String>,
}

impl<'a> TransformContext<'a> {
    #[allow(dead_code)] // Used by TransformationPipeline in later commits
    pub(crate) fn new(properties: &'a HashMap<String, String>) -> Self {
        Self { properties }
    }
}

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
