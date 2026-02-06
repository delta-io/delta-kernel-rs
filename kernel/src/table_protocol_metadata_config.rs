//! Configuration for table property-based protocol and metadata modifications.
//!
//! This module provides [`TableProtocolMetadataConfig`], which creates Protocol and Metadata
//! from user-provided table properties during table creation or modification operations.
//!
//! # Architecture
//!
//! The configuration flows through a transform pipeline:
//!
//! 1. `TableProtocolMetadataConfig::new_base_for_create()` - Creates initial config with bare
//!    protocol and user properties only (delta.* properties filtered out)
//! 2. `TransformationPipeline::apply_transforms()` - Applies registered transforms based on
//!    the raw properties from `TransformContext` (enables features, validates properties, etc.)
//!
//! See [`crate::table_transformation`] for the transform pipeline implementation.
//!
//! # Difference from [`TableConfiguration`](crate::table_configuration::TableConfiguration)
//!
//! - **[`TableConfiguration`](crate::table_configuration::TableConfiguration)**: Reads the
//!   configuration of an *existing* table from a snapshot.
//!
//! - **[`TableProtocolMetadataConfig`]**: Creates Protocol and Metadata from *user-provided*
//!   properties during table creation or modification operations.

use std::collections::HashMap;

use crate::actions::{Metadata, Protocol};
use crate::schema::StructType;
use crate::table_features::{
    TableFeature, SET_TABLE_FEATURE_SUPPORTED_PREFIX, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
use crate::utils::current_time_ms;
use crate::DeltaResult;

/// Protocol and Metadata state that flows through transforms.
///
/// This struct is the initial state created from user-provided table properties
/// and schema during table creation. It flows through the transform pipeline
/// which may:
/// - Add features to the protocol based on properties or schema
/// - Validate and process delta.* properties
/// - Add column mapping metadata to the schema (future)
///
/// # Example
/// ```ignore
/// use delta_kernel::schema::StructType;
/// use delta_kernel::table_transformation::TransformationPipeline;
/// use std::collections::HashMap;
///
/// let schema = StructType::new_unchecked(vec![/* fields */]);
/// let props = HashMap::from([
///     ("myapp.version".to_string(), "1.0".to_string()),
/// ]);
///
/// // Create initial config (delta.* properties are filtered out)
/// let config = TableProtocolMetadataConfig::new_base_for_create(schema, vec![], props.clone())?;
///
/// // Apply transforms (they read delta.* from TransformContext)
/// let final_config = TransformationPipeline::apply_transforms(config, &props)?;
/// ```
#[derive(Debug)]
pub(crate) struct TableProtocolMetadataConfig {
    /// The protocol (starts as bare v3/v7, transforms add features).
    pub(crate) protocol: Protocol,
    /// The metadata containing schema, partition columns, and all table properties.
    pub(crate) metadata: Metadata,
}

impl TableProtocolMetadataConfig {
    /// Create base config for CREATE TABLE with only user properties.
    ///
    /// This creates a "bare" configuration:
    /// - Protocol: v3/v7 with empty feature lists
    /// - Metadata: schema + partition columns + user properties (delta.* filtered out)
    ///
    /// Delta.* properties are transient signals processed by transforms via
    /// `TransformContext`, not stored in table metadata.
    /// See [`crate::table_transformation::TransformationPipeline`].
    ///
    /// # Arguments
    ///
    /// * `schema` - The table schema
    /// * `partition_columns` - Column names for partitioning
    /// * `properties` - All user-provided table properties (delta.* will be filtered)
    pub(crate) fn new_base_for_create(
        schema: StructType,
        partition_columns: Vec<String>,
        properties: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        // Create bare protocol - v3/v7 with empty features
        // Transforms will add features based on properties
        let empty_features: [TableFeature; 0] = [];
        let protocol = Protocol::try_new(
            TABLE_FEATURES_MIN_READER_VERSION,
            TABLE_FEATURES_MIN_WRITER_VERSION,
            Some(empty_features.iter()),
            Some(empty_features.iter()),
        )?;

        // Filter out delta.* properties - they are transient signals
        // processed by transforms via TransformContext, not stored in metadata
        let user_properties: HashMap<String, String> = properties
            .into_iter()
            .filter(|(k, _)| !k.starts_with("delta."))
            .collect();

        let metadata = Metadata::try_new(
            None, // name
            None, // description
            schema,
            partition_columns,
            current_time_ms()?,
            user_properties,
        )?;

        Ok(Self { protocol, metadata })
    }

    /// Check if a table feature is allowed during CREATE TABLE.
    pub(crate) fn is_delta_feature_allowed(feature: &TableFeature) -> bool {
        Self::ALLOWED_DELTA_FEATURES.contains(feature)
    }

    /// Check if a property is allowed during CREATE TABLE.
    ///
    /// Returns `true` if the property is:
    /// - Not a delta.* property (user properties like `myapp.version` are always allowed)
    /// - A feature signal (`delta.feature.*`) for an allowed feature
    /// - `delta.minReaderVersion` or `delta.minWriterVersion`
    /// - An explicitly allowed delta property
    pub(crate) fn is_delta_property_allowed(key: &str) -> bool {
        // Non-delta properties are always allowed (user properties)
        if !key.starts_with("delta.") {
            return true;
        }

        // Feature signals: delta.feature.X - check if X is an allowed feature
        if let Some(feature_name) = key.strip_prefix(SET_TABLE_FEATURE_SUPPORTED_PREFIX) {
            // Parse the feature name and check if it's allowed
            if let Ok(feature) = feature_name.parse::<TableFeature>() {
                return Self::is_delta_feature_allowed(&feature);
            }
            // Unknown feature name - not allowed
            return false;
        }

        // Version properties are always allowed
        if key == MIN_READER_VERSION_PROP || key == MIN_WRITER_VERSION_PROP {
            return true;
        }

        // Check against explicitly allowed delta properties
        Self::ALLOWED_DELTA_PROPERTIES.contains(&key)
    }

    /// Table features allowed during CREATE TABLE.
    const ALLOWED_DELTA_FEATURES: [TableFeature; 3] = [
        // DomainMetadata: required for clustering, can also be enabled explicitly
        TableFeature::DomainMetadata,
        // ClusteredTable: enabled when using clustered data layout
        TableFeature::ClusteredTable,
        // ColumnMapping: enables column mapping (name/id mode)
        TableFeature::ColumnMapping,
        // As transforms are added, their features go here:
        // TableFeature::DeletionVectors,
        // TableFeature::TimestampWithoutTimezone,
    ];

    /// Delta properties allowed during CREATE TABLE.
    /// Signal flags (delta.feature.*, delta.minReaderVersion, delta.minWriterVersion)
    /// are always allowed and handled separately.
    const ALLOWED_DELTA_PROPERTIES: [&'static str; 1] = [
        // ColumnMapping mode property: triggers column mapping transform
        "delta.columnMapping.mode",
        // As property transforms are added, their properties go here:
        // "delta.enableDeletionVectors",
        // "delta.enableChangeDataFeed",
    ];

    /// Returns a new config with updated partition columns.
    pub(crate) fn with_partition_columns(self, partition_columns: Vec<String>) -> Self {
        Self {
            protocol: self.protocol,
            metadata: self.metadata.with_partition_columns(partition_columns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField};

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

    // =========================================================================
    // try_from Tests - Initial Config Creation
    // =========================================================================

    #[test]
    fn test_new_base_for_create_creates_bare_protocol() {
        let properties = props([("myapp.version", "1.0"), ("custom.property", "value")]);
        let config =
            TableProtocolMetadataConfig::new_base_for_create(test_schema(), vec![], properties)
                .unwrap();

        // Protocol should be bare v3/v7 with empty features
        assert_eq!(config.protocol.min_reader_version(), 3);
        assert_eq!(config.protocol.min_writer_version(), 7);
        assert!(config.protocol.reader_features().unwrap().is_empty());
        assert!(config.protocol.writer_features().unwrap().is_empty());
    }

    #[test]
    fn test_new_base_for_create_filters_delta_properties() {
        let properties = props([
            ("myapp.version", "1.0"),
            ("delta.feature.deletionVectors", "supported"),
            ("delta.minReaderVersion", "3"),
            ("custom.property", "value"),
        ]);
        let config =
            TableProtocolMetadataConfig::new_base_for_create(test_schema(), vec![], properties)
                .unwrap();

        // Only user properties should be in metadata - delta.* properties are filtered out
        assert_eq!(config.metadata.configuration().len(), 2);
        assert_eq!(
            config.metadata.configuration().get("myapp.version"),
            Some(&"1.0".to_string())
        );
        assert_eq!(
            config.metadata.configuration().get("custom.property"),
            Some(&"value".to_string())
        );
        // delta.* properties should NOT be in metadata
        assert!(!config
            .metadata
            .configuration()
            .contains_key("delta.feature.deletionVectors"));
        assert!(!config
            .metadata
            .configuration()
            .contains_key("delta.minReaderVersion"));
    }

    #[test]
    fn test_new_base_for_create_with_partition_columns() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("region", DataType::STRING, false),
        ]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            schema,
            vec!["region".to_string()],
            HashMap::new(),
        )
        .unwrap();

        assert_eq!(config.metadata.partition_columns(), &["region".to_string()]);
    }

    // =========================================================================
    // Pipeline Integration Tests
    // =========================================================================

    #[test]
    fn test_apply_transforms_with_no_signals() {
        use crate::table_transformation::TransformationPipeline;
        use crate::transaction::data_layout::DataLayout;

        // With no signal flags, config passes through with empty features
        let properties = props([("myapp.version", "1.0")]);
        let config = TableProtocolMetadataConfig::new_base_for_create(
            test_schema(),
            vec![],
            properties.clone(),
        )
        .unwrap();

        let output =
            TransformationPipeline::apply_transforms(config, &properties, &DataLayout::None)
                .unwrap();

        // Protocol still bare, properties still there
        assert!(output.config.protocol.writer_features().unwrap().is_empty());
        assert_eq!(
            output.config.metadata.configuration().get("myapp.version"),
            Some(&"1.0".to_string())
        );
    }
}
