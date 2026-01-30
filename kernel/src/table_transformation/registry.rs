//! Transform registry for selecting applicable transforms.
//!
//! The [`TransformRegistry`] is a singleton that manages the mapping from
//! properties/schema characteristics to transforms.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::transaction::data_layout::DataLayout;
use crate::DeltaResult;

use super::transforms::{
    ClusteringTransform, DeltaPropertyValidationTransform, DomainMetadataTransform,
    FeatureSignalTransform, PartitioningTransform, ProtocolVersionTransform,
};
use super::ProtocolMetadataTransform;

/// Factory function to create a transform instance.
#[allow(dead_code)]
type TransformFactory = fn() -> Box<dyn ProtocolMetadataTransform>;

/// Central registry of all available transforms.
///
/// Transforms are selected based on user-provided properties. The registry maps:
/// - Property names (e.g., `delta.enableDeletionVectors`) → property-triggered transforms
/// - Schema analysis → schema-triggered transforms (e.g., for TIMESTAMP_NTZ types)
///
/// The registry is a singleton - all builders share the same registry.
pub(crate) struct TransformRegistry {
    /// Property name -> transform factory
    #[allow(dead_code)]
    property_transforms: HashMap<&'static str, TransformFactory>,
    /// Schema-driven transforms (added when schema analysis is needed)
    #[allow(dead_code)]
    schema_transforms: Vec<TransformFactory>,
}

impl TransformRegistry {
    fn new() -> Self {
        Self {
            property_transforms: HashMap::new(),
            schema_transforms: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn register_property(&mut self, property: &'static str, factory: TransformFactory) {
        self.property_transforms.insert(property, factory);
    }

    #[allow(dead_code)]
    fn register_schema(&mut self, factory: TransformFactory) {
        self.schema_transforms.push(factory);
    }

    /// Get all transforms that may apply based on raw properties and data layout.
    ///
    /// Uses raw properties (not config) so transforms can see signal flags
    /// like delta.feature.* before they're processed.
    pub(crate) fn select_transforms(
        &self,
        properties: &HashMap<String, String>,
        data_layout: &DataLayout,
    ) -> DeltaResult<Vec<Box<dyn ProtocolMetadataTransform>>> {
        let mut transforms: Vec<Box<dyn ProtocolMetadataTransform>> = Vec::new();

        // First: validate all delta.* properties are allowed
        transforms.push(Box::new(DeltaPropertyValidationTransform));

        // Set protocol version from properties or defaults
        transforms.push(Box::new(ProtocolVersionTransform));

        // Feature signal transforms: process delta.feature.X=supported signals
        // These add explicit features to the protocol (e.g., deletionVectors, columnMapping)
        if let Some(feature_transform) = FeatureSignalTransform::new(properties)? {
            transforms.push(Box::new(feature_transform));
        }

        // Data layout transforms: partitioning or clustering
        match data_layout {
            DataLayout::None => {}
            DataLayout::Partitioned { columns } => {
                transforms.push(Box::new(PartitioningTransform::new(columns.clone())));
            }
            DataLayout::Clustered { columns } => {
                // Clustering requires DomainMetadata feature, so add that transform first
                transforms.push(Box::new(DomainMetadataTransform));
                transforms.push(Box::new(ClusteringTransform::new(columns.clone())));
            }
        }

        // Property-triggered transforms: enable features based on delta.* properties
        // Future examples (not yet implemented):
        //   - delta.enableDeletionVectors=true → adds DeletionVectors feature
        //   - delta.columnMapping.mode=name → adds ColumnMapping feature, transforms schema
        //   - delta.enableChangeDataFeed=true → adds ChangeDataFeed feature
        for (prop, factory) in &self.property_transforms {
            if properties.contains_key(*prop) {
                transforms.push(factory());
            }
        }

        // Schema-triggered transforms: auto-detect features required by schema
        // Future examples (not yet implemented):
        //   - Schema contains TIMESTAMP_NTZ type → adds TimestampWithoutTimezone feature
        //   - Schema contains VARIANT type → adds VariantType feature
        for factory in &self.schema_transforms {
            transforms.push(factory());
        }

        Ok(transforms)
    }
}

/// Global singleton registry.
pub(crate) static TRANSFORM_REGISTRY: LazyLock<TransformRegistry> =
    LazyLock::new(TransformRegistry::new);
