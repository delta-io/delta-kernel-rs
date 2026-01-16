//! Transform pipeline for table creation and modification.
//!
//! This module provides the infrastructure for applying transformations to
//! [`TableProtocolMetadataConfig`] during table creation. Transforms can:
//!
//! - Add features to the protocol based on properties or schema
//! - Validate and process delta.* properties
//! - Strip signal flags from metadata
//!
//! # Architecture
//!
//! ```text
//!  ┌──────────────────────────────────────────────────────────────┐
//!  │                      Table Operations                        │
//!  │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐   │
//!  │  │ CREATE TABLE │  │ALTER TABLE(*)│  │ REPLACE TABLE (*) │   │
//!  │  └──────┬───────┘  └──────┬───────┘  └─────────┬─────────┘   │
//!  │         │                 │                    │             │
//!  │         └─────────────────┼────────────────────┘             │
//!  │                           ▼                                  │
//!  │             User Properties + Schema                         │
//!  └──────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//!                TransformRegistry.select_transforms()
//!                              │
//!                              ▼
//!                ┌─────────────────────────────┐
//!                │   TransformationPipeline    │
//!                │  1. Topological sort        │
//!                │  2. Apply each transform    │
//!                │  3. Final validation        │
//!                └─────────────────────────────┘
//!                              │
//!                              ▼
//!                 Final Config (Protocol + Metadata)
//!
//! (*) Future operations - not yet implemented
//! ```
//!
//! # Example
//!
//! ```ignore
//! use delta_kernel::table_transformation::TransformationPipeline;
//!
//! let config = TableProtocolMetadataConfig::new(schema, vec![], props.clone())?;
//! let final_config = TransformationPipeline::apply_transforms(config, &props)?;
//! ```

mod registry;
mod transforms;

use std::collections::{HashMap, HashSet};

use crate::actions::DomainMetadata;
use crate::schema::variant_utils::validate_variant_type_feature_support;
use crate::table_features::{
    column_mapping_mode, validate_schema_column_mapping, validate_timestamp_ntz_feature_support,
};
use crate::{DeltaResult, Error};

use crate::table_protocol_metadata_config::TableProtocolMetadataConfig;

// Re-export for use by the pipeline
#[allow(unused_imports)]
pub(crate) use registry::TransformContext;
pub(crate) use registry::TRANSFORM_REGISTRY;

// Re-export transforms for tests
#[cfg(test)]
pub(crate) use transforms::{DeltaPropertyValidationTransform, ProtocolVersionTransform};

// ============================================================================
// Transform Types
// ============================================================================

/// Canonical identifier for each transform type.
///
/// Used for:
/// - Dependency declarations (compile-time safe)
/// - Tracking completed transforms
/// - Topological sort ordering
///
/// Each transform returns its ID via `ProtocolMetadataTransform::id()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TransformId {
    /// Validates delta.* properties are allowed
    DeltaPropertyValidation,
    /// Sets protocol version from properties or defaults
    ProtocolVersion,
    /// Enables DomainMetadata writer feature
    DomainMetadata,
    // Future transforms:
    // ColumnMapping,
    // Clustering,
    // DeletionVectors,
    // etc.
}

#[derive(Debug, Clone)]
pub(crate) enum TransformDependency {
    /// Hard ordering: This transform MUST be in the pipeline and run before me.
    /// If the transform is not in the pipeline, execution will fail.
    ///
    /// Example: ClusteringTransform requires DomainMetadataTransform to have run,
    /// because Clustering needs to write domain metadata.
    #[allow(dead_code)] // Used when ClusteringTransform is added
    TransformRequired(TransformId),

    /// Soft ordering: If this transform is in the pipeline, run it before me.
    /// If the transform is not in the pipeline, that's fine - no error.
    ///
    /// Example: If ColumnMappingTransform is in the pipeline, it should run before
    /// ClusteringTransform (to transform schema first), but Clustering doesn't
    /// require ColumnMapping to be enabled.
    #[allow(dead_code)] // Used when soft dependencies are needed
    TransformCompletedIfPresent(TransformId),
}

// ============================================================================
// Transform Output
// ============================================================================

/// Output from a transform's `apply()` method.
///
/// Contains the updated config and any domain metadata actions produced by the transform.
/// Domain metadata is collected by the pipeline and returned alongside the final config.
#[derive(Debug)]
pub(crate) struct TransformOutput {
    /// The updated protocol/metadata configuration
    pub config: TableProtocolMetadataConfig,
    /// Domain metadata actions to be written to the commit (e.g., delta.clustering)
    pub domain_metadata: Vec<DomainMetadata>,
}

impl TransformOutput {
    /// Create output with just the config (no domain metadata)
    pub(crate) fn new(config: TableProtocolMetadataConfig) -> Self {
        Self {
            config,
            domain_metadata: vec![],
        }
    }

    /// Create output with config and domain metadata
    pub(crate) fn with_domain_metadata(
        config: TableProtocolMetadataConfig,
        domain_metadata: Vec<DomainMetadata>,
    ) -> Self {
        Self {
            config,
            domain_metadata,
        }
    }
}

// ============================================================================
// Transform Trait
// ============================================================================

/// A transformation step that modifies protocol and/or metadata.
///
/// Transforms are registered in a central registry and applied by the
/// [`TransformationPipeline`]. Each transform declares its dependencies
/// and validation logic.
///
/// # Lifecycle
///
/// 1. Registry selects transforms based on properties
/// 2. Pipeline sorts transforms by dependencies
/// 3. `validate_preconditions()` checks for incompatible configurations
/// 4. `apply()` performs the transformation
/// 5. `validate_postconditions()` verifies the result
///
/// # Example
///
/// ```ignore
/// struct MyFeatureTransform;
///
/// impl ProtocolMetadataTransform for MyFeatureTransform {
///     fn id(&self) -> TransformId {
///         TransformId::MyFeature
///     }
///
///     fn name(&self) -> &'static str {
///         "MyFeature: enables feature when delta.enableMyFeature is set"
///     }
///     
///     fn apply(&self, mut config: TableProtocolMetadataConfig) -> DeltaResult<TransformOutput> {
///         config.protocol = config.protocol.with_feature(TableFeature::MyFeature)?;
///         Ok(TransformOutput::new(config))
///     }
/// }
/// ```
pub(crate) trait ProtocolMetadataTransform: std::fmt::Debug {
    /// Canonical identifier for this transform type.
    ///
    /// Used for:
    /// - Dependency declarations (compile-time safe)
    /// - Tracking completed transforms
    /// - Topological sort ordering
    fn id(&self) -> TransformId;

    /// Human-readable description of this transform.
    ///
    /// Format: "TransformName: reason" (e.g., "ProtocolVersion: sets version from properties")
    ///
    /// Used for logging, debugging, and error messages.
    fn name(&self) -> &'static str;

    /// Dependencies that must be satisfied before this transform can run.
    /// Returns empty vec if no dependencies.
    fn dependencies(&self) -> Vec<TransformDependency> {
        vec![]
    }

    /// Pre-validate the configuration before applying this transform.
    ///
    /// Called BEFORE `apply()`. Return Err if the transform cannot be applied.
    ///
    /// Use this for checking:
    /// - Incompatible property combinations (e.g., conflicting settings)
    /// - Missing required companion properties
    /// - Invalid configurations that span multiple properties
    ///
    /// # Examples
    ///
    /// - Clustering columns not included in `dataSkippingStatsColumns`
    /// - Clustering enabled but `dataSkippingNumIndexedCols` set to 0
    /// - Both partitioning and clustering set on the same columns
    fn validate_preconditions(
        &self,
        _config: &TableProtocolMetadataConfig,
        _context: &TransformContext<'_>,
    ) -> DeltaResult<()> {
        Ok(())
    }

    /// Apply the transformation to protocol and metadata.
    ///
    /// Returns a [`TransformOutput`] containing the updated config and any domain
    /// metadata actions produced by this transform.
    fn apply(
        &self,
        config: TableProtocolMetadataConfig,
        context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput>;

    /// Validate the config AFTER this transform has been applied.
    /// Called immediately after apply() succeeds.
    fn validate_postconditions(&self, _config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
        Ok(())
    }
}

// ============================================================================
// Transformation Pipeline
// ============================================================================

/// Pipeline that applies transforms in dependency order.
///
/// Responsibilities:
/// - Topological sort of transforms by dependencies
/// - Trigger detection (which transforms should run)
/// - Dependency validation
/// - Sequential execution with validation
/// - Final protocol/metadata compatibility validation
pub(crate) struct TransformationPipeline {
    transforms: Vec<Box<dyn ProtocolMetadataTransform>>,
    completed: HashSet<TransformId>,
}

impl TransformationPipeline {
    /// Create a new pipeline with the given transforms.
    pub(crate) fn new(transforms: Vec<Box<dyn ProtocolMetadataTransform>>) -> Self {
        Self {
            transforms,
            completed: HashSet::new(),
        }
    }

    /// Main entry point: apply transforms to config based on properties and schema.
    ///
    /// This is called from the builder after creating initial config via `try_from`.
    ///
    /// # Arguments
    ///
    /// * `config` - Initial config from `TableProtocolMetadataConfig::new()`
    /// * `properties` - Raw properties map (for transform lookup)
    ///
    /// # Steps
    ///
    /// 1. Get applicable transforms from registry based on properties
    /// 2. Topological sort by dependencies
    /// 3. Apply each transform (with validation)
    /// 4. Run final validation
    ///
    /// Returns a [`TransformOutput`] containing the final config and any domain metadata
    /// collected from all transforms.
    pub(crate) fn apply_transforms(
        config: TableProtocolMetadataConfig,
        properties: &HashMap<String, String>,
    ) -> DeltaResult<TransformOutput> {
        // Get transforms from registry using raw properties
        // The registry auto-resolves dependencies (e.g., ClusteringTransform -> DomainMetadataTransform)
        let transforms = TRANSFORM_REGISTRY.select_transforms_to_trigger(properties)?;

        // Create context for transforms
        let context = TransformContext::new(properties);

        // Apply via pipeline
        let mut pipeline = Self::new(transforms);
        pipeline.apply_all(config, &context)
    }

    /// Apply all transforms to the config.
    ///
    /// Transforms are selected by the registry based on properties, so the pipeline
    /// simply executes them in dependency order without re-checking triggers.
    ///
    /// Returns a [`TransformOutput`] containing the final config and collected
    /// domain metadata from all transforms.
    pub(crate) fn apply_all(
        &mut self,
        mut config: TableProtocolMetadataConfig,
        context: &TransformContext<'_>,
    ) -> DeltaResult<TransformOutput> {
        // 1. Topological sort
        let ordered_indices = self.order_transform_dependencies()?;

        // Collect domain metadata from all transforms
        let mut all_domain_metadata = Vec::new();

        // 2. Apply each transform
        for idx in ordered_indices {
            let transform = &self.transforms[idx];

            // Check transform ordering dependencies are satisfied
            self.check_transform_dependencies(transform.as_ref())?;

            // Validate no conflicting configurations and feature requirements
            transform.validate_preconditions(&config, context)?;

            // Apply
            let output = transform.apply(config, context)?;
            config = output.config;
            all_domain_metadata.extend(output.domain_metadata);

            // Post-apply validation
            transform.validate_postconditions(&config)?;

            // Mark completed
            self.completed.insert(transform.id());
        }

        // 3. Final validation
        self.validate_final(&config)?;

        Ok(TransformOutput::with_domain_metadata(
            config,
            all_domain_metadata,
        ))
    }

    /// Performs a topological sort of transforms based on their dependencies.
    ///
    /// Uses depth-first search (DFS) to produce an ordering where each transform
    /// appears after all transforms it depends on. This ensures that when we execute
    /// transforms in the returned order, all dependencies are satisfied.
    ///
    /// # Algorithm
    /// 1. Build a lookup map from transform name -> index for O(1) dependency resolution
    /// 2. For each transform, recursively visit its dependencies first (DFS)
    /// 3. After visiting all dependencies, add the transform to the ordered list
    /// 4. Track "in progress" nodes to detect circular dependencies
    ///
    /// # Returns
    /// A vector of indices into `self.transforms` in the order they should execute.
    ///
    /// # Errors
    /// - Circular dependency detected (transform A depends on B, B depends on A)
    fn order_transform_dependencies(&self) -> DeltaResult<Vec<usize>> {
        // Build name -> index lookup for O(1) dependency resolution
        let id_to_idx: HashMap<TransformId, usize> = self
            .transforms
            .iter()
            .enumerate()
            .map(|(idx, t)| (t.id(), idx))
            .collect();

        let mut ordered = Vec::with_capacity(self.transforms.len());
        let mut visited = HashSet::new();
        let mut in_progress = HashSet::new();

        for idx in 0..self.transforms.len() {
            self.visit(
                idx,
                &id_to_idx,
                &mut visited,
                &mut in_progress,
                &mut ordered,
            )?;
        }

        Ok(ordered)
    }

    /// DFS helper for topological sort.
    fn visit(
        &self,
        idx: usize,
        id_to_idx: &HashMap<TransformId, usize>,
        visited: &mut HashSet<usize>,
        in_progress: &mut HashSet<usize>,
        ordered: &mut Vec<usize>,
    ) -> DeltaResult<()> {
        if visited.contains(&idx) {
            return Ok(());
        }
        if in_progress.contains(&idx) {
            return Err(Error::generic(format!(
                "Circular dependency detected involving transform '{}'",
                self.transforms[idx].name()
            )));
        }

        in_progress.insert(idx);

        // Visit dependencies first
        for dep in self.transforms[idx].dependencies() {
            let dep_id = match &dep {
                TransformDependency::TransformRequired(id) => id,
                TransformDependency::TransformCompletedIfPresent(id) => id,
            };

            if let Some(&dep_idx) = id_to_idx.get(dep_id) {
                self.visit(dep_idx, id_to_idx, visited, in_progress, ordered)?;
            }
        }

        in_progress.remove(&idx);
        visited.insert(idx);
        ordered.push(idx);

        Ok(())
    }

    /// Validates that all dependencies declared by a transform have been satisfied.
    ///
    /// Called before applying each transform to ensure execution order is correct.
    ///
    /// Two types of dependencies:
    /// - `TransformRequired(name)`: Hard dependency - the named transform MUST have run.
    ///   Fails if the transform is not in the pipeline or hasn't completed.
    /// - `TransformCompletedIfPresent(name)`: Soft dependency - if the named transform
    ///   is in the pipeline, it must have run. No error if it's not in the pipeline.
    ///
    /// # Note on Feature Requirements
    /// Feature-level requirements (e.g., "Clustering requires DomainMetadata feature")
    /// should be validated in `validate_preconditions()` using `FeatureInfo.feature_requirements`.
    /// TODO: Implement this when per-feature transforms (ColumnMapping, DomainMetadata, etc.)
    /// are added. By that point, the topological sort has ensured dependent transforms have
    /// already run.
    fn check_transform_dependencies(
        &self,
        transform: &dyn ProtocolMetadataTransform,
    ) -> DeltaResult<()> {
        for dep in transform.dependencies() {
            match &dep {
                TransformDependency::TransformRequired(id) => {
                    // Hard dependency: transform MUST be in pipeline and completed
                    if !self.completed.contains(id) {
                        return Err(Error::generic(format!(
                            "Transform '{}' requires {:?} to complete first, but it is not in the pipeline",
                            transform.name(),
                            id
                        )));
                    }
                }
                TransformDependency::TransformCompletedIfPresent(id) => {
                    // Soft dependency: if the transform is in the pipeline, it must have completed.
                    // If it's not in the pipeline, that's fine (soft = optional).
                    let is_in_pipeline = self.transforms.iter().any(|t| t.id() == *id);
                    if is_in_pipeline && !self.completed.contains(id) {
                        // Transform is in pipeline but hasn't completed - this is a bug
                        // in the topological sort or execution order
                        return Err(Error::generic(format!(
                            "Transform '{}' should run after {:?}, but {:?} has not completed yet",
                            transform.name(),
                            id,
                            id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Final validation of protocol + metadata compatibility.
    fn validate_final(&self, config: &TableProtocolMetadataConfig) -> DeltaResult<()> {
        // Validate protocol + metadata compatibility
        let schema = config.metadata.parse_schema()?;
        let table_properties = config.metadata.parse_table_properties();
        let col_mapping_mode = column_mapping_mode(&config.protocol, &table_properties);

        validate_schema_column_mapping(&schema, col_mapping_mode)?;
        validate_timestamp_ntz_feature_support(&schema, &config.protocol)?;
        validate_variant_type_feature_support(&schema, &config.protocol)?;

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    /// Helper to construct a HashMap<String, String> from string slice pairs.
    fn props<const N: usize>(pairs: [(&str, &str); N]) -> HashMap<String, String> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect()
    }

    /// Helper to create a simple test schema.
    fn test_schema() -> StructType {
        StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)])
    }

    /// Helper to create a TransformContext from properties.
    fn test_context(properties: &HashMap<String, String>) -> TransformContext<'_> {
        TransformContext::new(properties)
    }

    // =========================================================================
    // ProtocolVersionTransform Tests
    // =========================================================================

    #[test]
    fn test_protocol_version_transform_uses_defaults() {
        let properties = HashMap::new();
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = ProtocolVersionTransform;
        let result = transform.apply(config, &context).unwrap();

        // Default to v3/v7 for table features support
        assert_eq!(result.config.protocol.min_reader_version(), 3);
        assert_eq!(result.config.protocol.min_writer_version(), 7);
    }

    #[test]
    fn test_protocol_version_transform_sets_version() {
        let properties = props([
            ("delta.minReaderVersion", "3"),
            ("delta.minWriterVersion", "7"),
        ]);
        // Note: delta.* properties are filtered out in new_base_for_create,
        // but the transform reads from context.properties (raw user properties)
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = ProtocolVersionTransform;
        let result = transform.apply(config, &context).unwrap();

        assert_eq!(result.config.protocol.min_reader_version(), 3);
        assert_eq!(result.config.protocol.min_writer_version(), 7);
    }

    #[test]
    fn test_protocol_version_transform_rejects_invalid_reader_version() {
        let properties = props([("delta.minReaderVersion", "2")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = ProtocolVersionTransform;
        let result = transform.apply(config, &context);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("minReaderVersion"));
    }

    #[test]
    fn test_protocol_version_transform_rejects_invalid_writer_version() {
        let properties = props([("delta.minWriterVersion", "6")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = ProtocolVersionTransform;
        let result = transform.apply(config, &context);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("minWriterVersion"));
    }

    #[test]
    fn test_protocol_version_transform_rejects_non_integer() {
        let properties = props([("delta.minReaderVersion", "abc")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = ProtocolVersionTransform;
        let result = transform.apply(config, &context);

        assert!(result.is_err());
    }

    #[test]
    fn test_protocol_version_delta_props_not_in_metadata() {
        // This test verifies that delta.* properties are NOT stored in metadata
        // They are filtered out in new_base_for_create() and read from TransformContext
        let properties = props([
            ("delta.minReaderVersion", "3"),
            ("delta.minWriterVersion", "7"),
            ("myapp.version", "1.0"),
        ]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = ProtocolVersionTransform;
        let result = transform.apply(config, &context).unwrap();

        // Version signals should NOT be in metadata (filtered in new_base_for_create)
        assert!(!result
            .config
            .metadata
            .configuration()
            .contains_key("delta.minReaderVersion"));
        assert!(!result
            .config
            .metadata
            .configuration()
            .contains_key("delta.minWriterVersion"));
        // User property should remain
        assert!(result
            .config
            .metadata
            .configuration()
            .contains_key("myapp.version"));
    }

    // =========================================================================
    // DeltaPropertyValidationTransform Tests
    // =========================================================================

    #[test]
    fn test_delta_property_validation_rejects_feature_not_on_allow_list() {
        // Feature signals are only allowed for features on the allow list
        // deletionVectors is not on the allow list, so it should be rejected
        let properties = props([("delta.feature.deletionVectors", "supported")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = DeltaPropertyValidationTransform;
        let result = transform.validate_preconditions(&config, &context);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Enabling feature 'deletionVectors' is not supported"));
    }

    #[test]
    fn test_delta_property_validation_allows_version_properties() {
        let properties = props([
            ("delta.minReaderVersion", "3"),
            ("delta.minWriterVersion", "7"),
        ]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = DeltaPropertyValidationTransform;
        assert!(transform.validate_preconditions(&config, &context).is_ok());
    }

    #[test]
    fn test_delta_property_validation_allows_user_properties() {
        let properties = props([("myapp.version", "1.0"), ("spark.sql.shuffle", "200")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = DeltaPropertyValidationTransform;
        assert!(transform.validate_preconditions(&config, &context).is_ok());
    }

    #[test]
    fn test_delta_property_validation_rejects_unsupported_property() {
        let properties = props([("delta.enableDeletionVectors", "true")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();
        let context = test_context(&properties);

        let transform = DeltaPropertyValidationTransform;
        // Validation reads from context.properties, not config.metadata
        let result = transform.validate_preconditions(&config, &context);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not supported during CREATE TABLE"));
    }

    // =========================================================================
    // Pipeline Integration Tests
    // =========================================================================

    #[test]
    fn test_pipeline_with_invalid_version() {
        let properties = props([("delta.minReaderVersion", "1")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();

        let result = TransformationPipeline::apply_transforms(config, &properties);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("minReaderVersion"));
    }

    #[test]
    fn test_pipeline_with_no_signals() {
        let properties = props([("myapp.version", "1.0")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();

        let result = TransformationPipeline::apply_transforms(config, &properties);

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.config.protocol.writer_features().unwrap().is_empty());
    }
}
