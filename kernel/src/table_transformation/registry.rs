//! Transform registry for selecting applicable transforms.
//!
//! The [`TransformRegistry`] is a singleton that manages transform registrations.
//! Each registration associates a [`TableFeature`] (optional) with a trigger condition
//! and a factory function to create the transform.
//!
//! # Design
//!
//! Transforms are registered declaratively with trigger conditions and context-aware factories:
//!
//! ```ignore
//! registry.register_feature_transform(
//!     TransformId::DeletionVectors,
//!     TableFeature::DeletionVectors,
//!     TransformTrigger::Property("delta.enableDeletionVectors"),
//!     |_ctx| Some(Box::new(DeletionVectorsTransform)),
//! );
//! ```
//!
//! The `select_transforms_to_trigger()` method automatically selects transforms whose triggers match,
//! passing runtime context to the factory functions.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::schema::{DataType, StructType};
use crate::table_features::TableFeature;
use crate::transaction::data_layout::DataLayout;
use crate::DeltaResult;

use super::transforms::{
    ClusteringTransform, DeltaPropertyValidationTransform, DomainMetadataTransform,
    PartitioningTransform, ProtocolVersionTransform,
};
use super::{ProtocolMetadataTransform, TransformDependency, TransformId};

// ============================================================================
// Transform Context
// ============================================================================

/// Runtime context passed to transform factories.
///
/// Contains all information needed to decide whether to create a transform
/// and how to configure it.
#[derive(Debug)]
#[allow(dead_code)] // Fields accessed by factory functions
pub(crate) struct TransformContext<'a> {
    /// Raw properties from the user (includes signal flags like delta.feature.*)
    pub properties: &'a HashMap<String, String>,
    /// Data layout (partitioning/clustering) configuration
    pub data_layout: &'a DataLayout,
}

impl<'a> TransformContext<'a> {
    /// Create a new transform context.
    pub(crate) fn new(
        properties: &'a HashMap<String, String>,
        data_layout: &'a DataLayout,
    ) -> Self {
        Self {
            properties,
            data_layout,
        }
    }
}

/// Factory function to create a transform instance.
///
/// Receives runtime context and returns:
/// - `Some(transform)` if the transform should be created
/// - `None` if the transform should not be created (e.g., conditions not met)
type TransformFactory = fn(&TransformContext<'_>) -> Option<Box<dyn ProtocolMetadataTransform>>;

// ============================================================================
// Trigger Types
// ============================================================================

/// How to check schema for type presence.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum SchemaTypeCheck {
    /// Schema contains this data type at any level (including nested).
    ContainsType(DataType),
}

/// Which data layout triggers the transform.
///
/// Used when DataLayout support is added to trigger transforms based on
/// whether the table is partitioned or clustered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataLayoutKind {
    Partitioned,
    Clustered,
}

/// Trigger condition for when a transform should be selected.
///
/// The registry checks each registration's trigger against the current context
/// (properties, schema, data layout) to determine which transforms to include.
#[derive(Debug, Clone)]
pub(crate) enum TransformTrigger {
    /// Always runs (for core transforms like validation, protocol version).
    Always,

    /// Triggered by `delta.feature.X=supported` signal.
    ///
    /// Example: `TransformTrigger::FeatureSignal(TableFeature::DeletionVectors)`
    /// triggers when `delta.feature.deletionVectors=supported` is present.
    FeatureSignal(TableFeature),

    /// Triggered by presence of a property.
    ///
    /// Example: `TransformTrigger::Property("delta.columnMapping.mode")`
    /// triggers when the property key exists in the properties map.
    #[allow(dead_code)]
    Property(&'static str),

    /// Triggered by schema containing a specific type.
    ///
    /// Example: `TransformTrigger::SchemaType(SchemaTypeCheck::ContainsType(DataType::TIMESTAMP_NTZ))`
    /// triggers when the schema contains a TIMESTAMP_NTZ field.
    #[allow(dead_code)]
    SchemaType(SchemaTypeCheck),

    /// Triggered by data layout (partitioned or clustered).
    ///
    /// Example: `TransformTrigger::DataLayout(DataLayoutKind::Clustered)`
    /// triggers when the table uses clustered data layout.
    DataLayout(DataLayoutKind),
}

// ============================================================================
// Registration
// ============================================================================

/// Registration entry for a transform.
///
/// Associates an optional feature with a trigger condition and factory.
pub(crate) struct TransformRegistration {
    /// The transform ID this registration produces.
    /// Used for dependency resolution - when a transform declares a required dependency,
    /// the registry can find and create that dependency by looking up this ID.
    pub transform_id: TransformId,
    /// The table feature this transform handles (None for core transforms).
    #[allow(dead_code)] // Will be used for feature-specific validation
    pub feature: Option<TableFeature>,
    /// When to trigger this transform.
    pub trigger: TransformTrigger,
    /// Factory to create the transform instance.
    pub factory: TransformFactory,
}

// ============================================================================
// Registry
// ============================================================================

/// Central registry of all available transforms.
///
/// Transforms are selected based on trigger conditions matching the current context.
/// The registry is a singleton - all builders share the same registry.
pub(crate) struct TransformRegistry {
    /// All registered transforms.
    registrations: Vec<TransformRegistration>,
}

impl TransformRegistry {
    fn new() -> Self {
        Self {
            registrations: Vec::new(),
        }
    }

    /// Register a feature-associated transform.
    ///
    /// Use this for transforms that enable a specific Delta table feature
    /// (e.g., DeletionVectors, ColumnMapping, DomainMetadata).
    fn register_feature_transform(
        &mut self,
        transform_id: TransformId,
        feature: TableFeature,
        trigger: TransformTrigger,
        factory: TransformFactory,
    ) {
        self.registrations.push(TransformRegistration {
            transform_id,
            feature: Some(feature),
            trigger,
            factory,
        });
    }

    /// Register a builder transform (not tied to a specific feature).
    ///
    /// Use this for transforms that are part of the table creation/modification process
    /// but don't enable a specific table feature (e.g., validation, protocol version,
    /// partitioning, clustering).
    fn register_builder_transform(
        &mut self,
        transform_id: TransformId,
        trigger: TransformTrigger,
        factory: TransformFactory,
    ) {
        self.registrations.push(TransformRegistration {
            transform_id,
            feature: None,
            trigger,
            factory,
        });
    }

    /// Select all transforms to trigger based on the current context.
    ///
    /// This method:
    /// 1. Selects transforms whose triggers match the context
    /// 2. Resolves required dependencies - if a triggered transform declares
    ///    `TransformDependency::TransformRequired(id)`, the registry automatically
    ///    includes that dependency (even if it wasn't triggered directly)
    ///
    /// # Arguments
    ///
    /// * `properties` - Raw properties map (for property and signal triggers)
    /// * `data_layout` - Data layout configuration (for partitioning/clustering triggers)
    ///
    /// # Returns
    ///
    /// A vector of transform instances. The caller (pipeline) is responsible for
    /// ordering them by dependencies via topological sort.
    pub(crate) fn select_transforms_to_trigger(
        &self,
        properties: &HashMap<String, String>,
        data_layout: &DataLayout,
    ) -> DeltaResult<Vec<Box<dyn ProtocolMetadataTransform>>> {
        let context = TransformContext::new(properties, data_layout);
        let mut transforms: Vec<Box<dyn ProtocolMetadataTransform>> = Vec::new();
        let mut included_ids: HashSet<TransformId> = HashSet::new();

        // Phase 1: Select transforms whose triggers match
        for registration in &self.registrations {
            let should_include = match &registration.trigger {
                // Core transforms that always run
                TransformTrigger::Always => true,

                // Feature signal: delta.feature.X=supported
                TransformTrigger::FeatureSignal(feature) => {
                    let key = format!("delta.feature.{}", feature.as_ref());
                    properties
                        .get(&key)
                        .map(|v| v == "supported")
                        .unwrap_or(false)
                }

                // Property presence
                TransformTrigger::Property(prop_name) => properties.contains_key(*prop_name),

                // Schema type detection - requires schema parameter (future enhancement)
                TransformTrigger::SchemaType(_check) => false,

                // Data layout triggers
                TransformTrigger::DataLayout(kind) => matches!(
                    (kind, data_layout),
                    (DataLayoutKind::Partitioned, DataLayout::Partitioned { .. })
                        | (DataLayoutKind::Clustered, DataLayout::Clustered { .. })
                ),
            };

            if should_include {
                // Call factory with context - it may return None if conditions aren't met
                if let Some(transform) = (registration.factory)(&context) {
                    let id = transform.id();

                    // Deduplicate: same transform may be triggered multiple ways
                    if !included_ids.contains(&id) {
                        included_ids.insert(id);
                        transforms.push(transform);
                    }
                }
            }
        }

        // Phase 2: Resolve required dependencies
        self.resolve_transform_dependencies(&context, &mut transforms, &mut included_ids);

        Ok(transforms)
    }

    /// Resolves required dependencies for the selected transforms.
    ///
    /// When a transform declares `TransformDependency::TransformRequired(id)`, this method
    /// finds and creates that dependency (even if it wasn't directly triggered).
    ///
    /// This enables patterns like:
    /// - `ClusteringTransform` requires `DomainMetadataTransform`
    /// - When clustering is used, `DomainMetadataTransform` is auto-included
    ///
    /// The method iterates until no new dependencies are found, handling transitive dependencies.
    fn resolve_transform_dependencies(
        &self,
        context: &TransformContext<'_>,
        transforms: &mut Vec<Box<dyn ProtocolMetadataTransform>>,
        included_ids: &mut HashSet<TransformId>,
    ) {
        loop {
            let mut new_deps: Vec<TransformId> = Vec::new();

            // Collect all required dependencies that aren't yet included
            for transform in transforms.iter() {
                for dep in transform.dependencies() {
                    if let TransformDependency::TransformRequired(dep_id) = dep {
                        if !included_ids.contains(&dep_id) {
                            new_deps.push(dep_id);
                        }
                    }
                }
            }

            if new_deps.is_empty() {
                break;
            }

            // Find and create each missing dependency
            for dep_id in new_deps {
                if included_ids.contains(&dep_id) {
                    continue; // Already added in this iteration
                }

                // Find the registration that produces this transform ID
                if let Some(registration) =
                    self.registrations.iter().find(|r| r.transform_id == dep_id)
                {
                    // Call factory - for dependencies, we always want to create them
                    if let Some(transform) = (registration.factory)(context) {
                        included_ids.insert(dep_id);
                        transforms.push(transform);
                    }
                }
            }
        }
    }
}

/// Recursively check if schema contains a data type.
#[allow(dead_code)] // Used by select_transforms_to_trigger
fn schema_contains_type(schema: &StructType, target: &DataType) -> bool {
    for field in schema.fields() {
        if field.data_type() == target {
            return true;
        }
        // Recurse into nested types
        match field.data_type() {
            DataType::Struct(inner) => {
                if schema_contains_type(inner, target) {
                    return true;
                }
            }
            DataType::Array(arr) => {
                if arr.element_type() == target {
                    return true;
                }
                // Check nested struct in array
                if let DataType::Struct(inner) = arr.element_type() {
                    if schema_contains_type(inner, target) {
                        return true;
                    }
                }
            }
            DataType::Map(map) => {
                if map.key_type() == target || map.value_type() == target {
                    return true;
                }
                // Check nested structs in map
                if let DataType::Struct(inner) = map.key_type() {
                    if schema_contains_type(inner, target) {
                        return true;
                    }
                }
                if let DataType::Struct(inner) = map.value_type() {
                    if schema_contains_type(inner, target) {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

// ============================================================================
// Global Singleton
// ============================================================================

/// Global singleton registry initialized with all supported transforms.
///
/// # Future: Composable Registries for Different Operations
///
/// For operations like ALTER TABLE that share some transforms with CREATE TABLE,
/// consider a composable approach:
///
/// ```ignore
/// fn base_registry() -> TransformRegistry {
///     // Shared transforms (validation, protocol version, etc.)
/// }
///
/// fn create_table_registry() -> TransformRegistry {
///     let mut registry = base_registry();
///     registry.register_feature(...);  // CREATE-specific transforms
///     registry
/// }
///
/// fn alter_table_registry() -> TransformRegistry {
///     let mut registry = base_registry();
///     registry.register_feature(...);  // ALTER-specific transforms
///     registry
/// }
/// ```
pub(crate) static TRANSFORM_REGISTRY: LazyLock<TransformRegistry> = LazyLock::new(|| {
    let mut registry = TransformRegistry::new();

    // =========================================================================
    // Builder transforms (triggered by the operation, not specific features)
    // =========================================================================

    // First: validate all delta.* properties are allowed
    registry.register_builder_transform(
        TransformId::DeltaPropertyValidation,
        TransformTrigger::Always,
        |_ctx| Some(Box::new(DeltaPropertyValidationTransform)),
    );

    // Set protocol version from properties or defaults
    registry.register_builder_transform(
        TransformId::ProtocolVersion,
        TransformTrigger::Always,
        |_ctx| Some(Box::new(ProtocolVersionTransform)),
    );

    // =========================================================================
    // Feature transforms
    // =========================================================================

    // DomainMetadata - enables domainMetadata writer feature
    // Can be triggered by:
    // 1. Explicit signal: delta.feature.domainMetadata=supported
    // 2. Auto-included as a required dependency of ClusteringTransform
    registry.register_feature_transform(
        TransformId::DomainMetadata,
        TableFeature::DomainMetadata,
        TransformTrigger::FeatureSignal(TableFeature::DomainMetadata),
        |_ctx| Some(Box::new(DomainMetadataTransform)),
    );

    // =========================================================================
    // Data layout transforms
    // =========================================================================

    // Partitioning - sets partition columns on metadata
    // Triggered when DataLayout::Partitioned is specified
    registry.register_builder_transform(
        TransformId::Partitioning,
        TransformTrigger::DataLayout(DataLayoutKind::Partitioned),
        |ctx| {
            if let DataLayout::Partitioned { columns } = ctx.data_layout {
                Some(Box::new(PartitioningTransform::new(columns.clone())))
            } else {
                None
            }
        },
    );

    // Clustering - enables clustered table with domain metadata
    // Triggered when DataLayout::Clustered is specified
    // DomainMetadataTransform is auto-included via dependency resolution
    registry.register_builder_transform(
        TransformId::Clustering,
        TransformTrigger::DataLayout(DataLayoutKind::Clustered),
        |ctx| {
            if let DataLayout::Clustered { columns } = ctx.data_layout {
                Some(Box::new(ClusteringTransform::new(columns.clone())))
            } else {
                None
            }
        },
    );

    // =========================================================================
    // Future: Additional feature transforms
    // =========================================================================
    //
    // When implementing new features, register them here. Examples:
    //
    // registry.register_feature_transform(
    //     TransformId::DeletionVectors,
    //     TableFeature::DeletionVectors,
    //     TransformTrigger::Property("delta.enableDeletionVectors"),
    //     |_ctx| Some(Box::new(DeletionVectorsTransform)),
    // );
    //
    // registry.register_feature_transform(
    //     TransformId::ColumnMapping,
    //     TableFeature::ColumnMapping,
    //     TransformTrigger::Property("delta.columnMapping.mode"),
    //     |_ctx| Some(Box::new(ColumnMappingTransform)),
    // );
    //
    // registry.register_feature_transform(
    //     TransformId::TimestampNtz,
    //     TableFeature::TimestampWithoutTimezone,
    //     TransformTrigger::SchemaType(SchemaTypeCheck::ContainsType(DataType::TIMESTAMP_NTZ)),
    //     |_ctx| Some(Box::new(TimestampNtzTransform)),
    // );

    registry
});

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnName, DataType, StructField, StructType};

    #[test]
    fn test_always_triggers_select_core_transforms() {
        let props = HashMap::new();

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &DataLayout::None)
            .unwrap();

        // Should have validation and protocol version transforms
        assert_eq!(transforms.len(), 2);
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::DeltaPropertyValidation));
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::ProtocolVersion));
    }

    #[test]
    fn test_partitioned_layout_triggers_partitioning_transform() {
        let props = HashMap::new();
        let data_layout = DataLayout::Partitioned {
            columns: vec![ColumnName::new(["region"])],
        };

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &data_layout)
            .unwrap();

        // Should include Partitioning transform
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::Partitioning));
    }

    #[test]
    fn test_no_partitioning_when_layout_none() {
        let props = HashMap::new();

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &DataLayout::None)
            .unwrap();

        // Should NOT include Partitioning transform
        assert!(!transforms
            .iter()
            .any(|t| t.id() == TransformId::Partitioning));
    }

    #[test]
    fn test_clustered_layout_triggers_clustering_transform() {
        let props = HashMap::new();
        let data_layout = DataLayout::Clustered {
            columns: vec![ColumnName::new(["id"]), ColumnName::new(["name"])],
        };

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &data_layout)
            .unwrap();

        // Should include Clustering transform
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::Clustering));

        // Should also include DomainMetadata (auto-included as dependency)
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::DomainMetadata));
    }

    #[test]
    fn test_no_clustering_when_layout_none() {
        let props = HashMap::new();

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &DataLayout::None)
            .unwrap();

        // Should NOT include Clustering transform
        assert!(!transforms
            .iter()
            .any(|t| t.id() == TransformId::Clustering));
    }

    #[test]
    fn test_schema_contains_type_simple() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        assert!(schema_contains_type(&schema, &DataType::INTEGER));
        assert!(schema_contains_type(&schema, &DataType::STRING));
        assert!(!schema_contains_type(&schema, &DataType::BOOLEAN));
    }

    #[test]
    fn test_schema_contains_type_nested() {
        let inner =
            StructType::new_unchecked(vec![StructField::new("value", DataType::DOUBLE, false)]);
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("nested", DataType::Struct(Box::new(inner)), true),
        ]);

        assert!(schema_contains_type(&schema, &DataType::INTEGER));
        assert!(schema_contains_type(&schema, &DataType::DOUBLE));
        assert!(!schema_contains_type(&schema, &DataType::STRING));
    }
}
