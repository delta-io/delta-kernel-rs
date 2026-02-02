//! Transform registry for selecting applicable transforms.
//!
//! The [`TransformRegistry`] is a singleton that manages transform registrations.
//! Each registration associates a [`TableFeature`] (optional) with a trigger condition
//! and a factory function to create the transform.
//!
//! # Design
//!
//! Transforms are registered declaratively with trigger conditions:
//!
//! ```ignore
//! registry.register_feature(
//!     TableFeature::DeletionVectors,
//!     TransformTrigger::Property("delta.enableDeletionVectors"),
//!     || Box::new(DeletionVectorsTransform),
//! );
//! ```
//!
//! The `select_transforms()` method automatically selects transforms whose triggers match.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::schema::{DataType, StructType};
use crate::table_features::TableFeature;
use crate::transaction::data_layout::DataLayout;
use crate::DeltaResult;

use super::transforms::{
    DeltaPropertyValidationTransform, DomainMetadataTransform, FeatureSignalTransform,
    PartitioningTransform, ProtocolVersionTransform,
};
use super::{ProtocolMetadataTransform, TransformId};

/// Factory function to create a transform instance.
type TransformFactory = fn() -> Box<dyn ProtocolMetadataTransform>;

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
#[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    DataLayout(DataLayoutKind),

    /// Special trigger for FeatureSignalTransform which needs access to properties
    /// to parse all delta.feature.* signals at once.
    FeatureSignalsPresent,
}

// ============================================================================
// Registration
// ============================================================================

/// Registration entry for a transform.
///
/// Associates an optional feature with a trigger condition and factory.
#[allow(dead_code)]
pub(crate) struct FeatureTransformRegistration {
    /// The table feature this transform handles (None for core transforms).
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
#[allow(dead_code)] // Used by TransformationPipeline
pub(crate) struct TransformRegistry {
    /// All registered transforms.
    registrations: Vec<FeatureTransformRegistration>,
}

#[allow(dead_code)] // Methods used by TransformationPipeline
impl TransformRegistry {
    fn new() -> Self {
        Self {
            registrations: Vec::new(),
        }
    }

    /// Register a feature-associated transform.
    fn register_feature(
        &mut self,
        feature: TableFeature,
        trigger: TransformTrigger,
        factory: TransformFactory,
    ) {
        self.registrations.push(FeatureTransformRegistration {
            feature: Some(feature),
            trigger,
            factory,
        });
    }

    /// Register an operation transform (not tied to a specific feature).
    ///
    /// These are transforms triggered by the operation (e.g., validation, protocol version)
    /// rather than by enabling a specific table feature.
    fn register_operation_transform(
        &mut self,
        trigger: TransformTrigger,
        factory: TransformFactory,
    ) {
        self.registrations.push(FeatureTransformRegistration {
            feature: None,
            trigger,
            factory,
        });
    }

    /// Select all transforms to trigger based on the current context.
    ///
    /// # Arguments
    ///
    /// * `properties` - Raw properties map (for property and signal triggers)
    /// * `schema` - Table schema (for schema type triggers)
    /// * `data_layout` - Data layout configuration (for partitioning/clustering triggers)
    ///
    /// # Returns
    ///
    /// A vector of transform instances. The caller (pipeline) is responsible for
    /// ordering them by dependencies via topological sort.
    pub(crate) fn select_transforms_to_trigger(
        &self,
        properties: &HashMap<String, String>,
        schema: &StructType,
        data_layout: &DataLayout,
    ) -> DeltaResult<Vec<Box<dyn ProtocolMetadataTransform>>> {
        let mut transforms: Vec<Box<dyn ProtocolMetadataTransform>> = Vec::new();
        let mut included_ids: HashSet<TransformId> = HashSet::new();

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

                // Schema type detection
                TransformTrigger::SchemaType(check) => match check {
                    SchemaTypeCheck::ContainsType(dtype) => schema_contains_type(schema, dtype),
                },

                // Data layout trigger
                TransformTrigger::DataLayout(kind) => matches!(
                    (kind, data_layout),
                    (DataLayoutKind::Partitioned, DataLayout::Partitioned { .. })
                        | (DataLayoutKind::Clustered, DataLayout::Clustered { .. })
                ),

                // Special: check if any delta.feature.* signals exist
                TransformTrigger::FeatureSignalsPresent => {
                    properties.keys().any(|k| k.starts_with("delta.feature."))
                }
            };

            if should_include {
                // Create the transform and check for duplicates
                let transform = (registration.factory)();
                let id = transform.id();

                // Deduplicate: same transform may be triggered multiple ways
                if !included_ids.contains(&id) {
                    included_ids.insert(id);
                    transforms.push(transform);
                }
            }
        }

        // Handle data-layout triggered transforms that need column information
        match data_layout {
            DataLayout::Partitioned { columns } => {
                if !included_ids.contains(&TransformId::Partitioning) {
                    included_ids.insert(TransformId::Partitioning);
                    transforms.push(Box::new(PartitioningTransform::new(columns.clone())));
                }
            }
            DataLayout::Clustered { .. } | DataLayout::None => {
                // Clustering support will be added in a subsequent commit
            }
        }

        Ok(transforms)
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
#[allow(dead_code)] // Used by TransformationPipeline
pub(crate) static TRANSFORM_REGISTRY: LazyLock<TransformRegistry> = LazyLock::new(|| {
    let mut registry = TransformRegistry::new();

    // =========================================================================
    // Operation transforms (triggered by the operation, not specific features)
    // =========================================================================

    // First: validate all delta.* properties are allowed
    registry.register_operation_transform(TransformTrigger::Always, || {
        Box::new(DeltaPropertyValidationTransform)
    });

    // Set protocol version from properties or defaults
    registry.register_operation_transform(TransformTrigger::Always, || {
        Box::new(ProtocolVersionTransform)
    });

    // Process delta.feature.X=supported signals (if any present)
    // Note: This uses a special factory that parses properties at creation time
    registry.register_operation_transform(TransformTrigger::FeatureSignalsPresent, || {
        // The transform parses signals from config.metadata in validate_preconditions/apply
        Box::new(FeatureSignalTransform)
    });

    // =========================================================================
    // Feature-specific transforms
    // =========================================================================

    // DomainMetadata: enables the domain metadata writer feature
    // Triggered via delta.feature.domainMetadata=supported signal
    registry.register_feature(
        TableFeature::DomainMetadata,
        TransformTrigger::FeatureSignal(TableFeature::DomainMetadata),
        || Box::new(DomainMetadataTransform),
    );

    // =========================================================================
    // Future: Additional feature-specific transforms
    // =========================================================================
    //
    // When implementing new features, register them here. Examples:
    //
    // registry.register_feature(
    //     TableFeature::DeletionVectors,
    //     TransformTrigger::Property("delta.enableDeletionVectors"),
    //     || Box::new(DeletionVectorsTransform),
    // );
    //
    // registry.register_feature(
    //     TableFeature::ColumnMapping,
    //     TransformTrigger::Property("delta.columnMapping.mode"),
    //     || Box::new(ColumnMappingTransform),
    // );
    //
    // registry.register_feature(
    //     TableFeature::TimestampWithoutTimezone,
    //     TransformTrigger::SchemaType(SchemaTypeCheck::ContainsType(DataType::TIMESTAMP_NTZ)),
    //     || Box::new(TimestampNtzTransform),
    // );

    registry
});

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    fn test_schema() -> StructType {
        StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)])
    }

    #[test]
    fn test_always_triggers_select_core_transforms() {
        let props = HashMap::new();
        let schema = test_schema();

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &schema, &DataLayout::None)
            .unwrap();

        // Should have at least validation and protocol version transforms
        assert!(transforms.len() >= 2);
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::DeltaPropertyValidation));
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::ProtocolVersion));
    }

    #[test]
    fn test_feature_signal_trigger() {
        let mut props = HashMap::new();
        props.insert(
            "delta.feature.deletionVectors".to_string(),
            "supported".to_string(),
        );
        let schema = test_schema();

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &schema, &DataLayout::None)
            .unwrap();

        // Should include FeatureSignals transform when signals present
        assert!(transforms
            .iter()
            .any(|t| t.id() == TransformId::FeatureSignals));
    }

    #[test]
    fn test_no_feature_signal_when_absent() {
        let props = HashMap::new();
        let schema = test_schema();

        let transforms = TRANSFORM_REGISTRY
            .select_transforms_to_trigger(&props, &schema, &DataLayout::None)
            .unwrap();

        // Should NOT include FeatureSignals when no signals present
        assert!(!transforms
            .iter()
            .any(|t| t.id() == TransformId::FeatureSignals));
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
