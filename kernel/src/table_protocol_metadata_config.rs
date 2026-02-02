//! Configuration for table property-based protocol and metadata modifications.
//!
//! This module provides [`TableProtocolMetadataConfig`], which creates Protocol and Metadata
//! from user-provided table properties during table creation or modification operations.
//!
//! # Architecture
//!
//! The configuration flows through a transform pipeline:
//!
//! 1. `TableProtocolMetadataConfig::new()` - Creates initial config with bare protocol
//!    and all properties passed through to metadata
//! 2. `TransformationPipeline::apply_transforms()` - Applies registered transforms based on
//!    the raw properties (enables features, validates properties, etc.)
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
/// // Create initial config
/// let config = TableProtocolMetadataConfig::new(schema, vec![], props.clone())?;
///
/// // Apply transforms
/// let final_config = TransformationPipeline::apply_transforms(config, &props)?;
/// ```
#[derive(Debug)]
#[allow(dead_code)] // Used by table_transformation module
pub(crate) struct TableProtocolMetadataConfig {
    /// The protocol (starts as bare v3/v7, transforms add features).
    pub(crate) protocol: Protocol,
    /// The metadata containing schema, partition columns, and all table properties.
    pub(crate) metadata: Metadata,
}

#[allow(dead_code)] // Used by table_transformation module
impl TableProtocolMetadataConfig {
    /// Create initial config from schema, partition columns, and properties.
    ///
    /// This creates a "bare" configuration:
    /// - Protocol: v3/v7 with empty feature lists
    /// - Metadata: schema + partition columns + ALL properties (no filtering)
    ///
    /// Signal processing (delta.feature.*, version props) happens in transforms.
    /// See [`crate::table_transformation::TransformationPipeline`].
    ///
    /// # Arguments
    ///
    /// * `schema` - The table schema
    /// * `partition_columns` - Column names for partitioning
    /// * `properties` - All user-provided table properties (pass-through)
    pub(crate) fn new(
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

        // Create Metadata with ALL properties - no filtering here
        // Transforms will validate and process delta.* properties
        let metadata = Metadata::try_new(
            None, // name
            None, // description
            schema,
            partition_columns,
            current_time_ms()?,
            properties,
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
    /// - A signal flag (`delta.feature.*`, `delta.minReaderVersion`, `delta.minWriterVersion`)
    /// - An explicitly allowed delta property
    pub(crate) fn is_delta_property_allowed(key: &str) -> bool {
        // Non-delta properties are always allowed (user properties)
        if !key.starts_with("delta.") {
            return true;
        }

        // Signal flags are always allowed (they're processed and stripped)
        if key.starts_with(SET_TABLE_FEATURE_SUPPORTED_PREFIX) {
            return true;
        }
        if key == MIN_READER_VERSION_PROP || key == MIN_WRITER_VERSION_PROP {
            return true;
        }

        // Check against explicitly allowed delta properties
        Self::ALLOWED_DELTA_PROPERTIES.contains(&key)
    }

    /// Table features allowed during CREATE TABLE.
    const ALLOWED_DELTA_FEATURES: [TableFeature; 1] = [
        // DomainMetadata: required for clustering, can also be enabled explicitly
        TableFeature::DomainMetadata,
        // As transforms are added, their features go here:
        // TableFeature::DeletionVectors,
        // TableFeature::ColumnMapping,
        // TableFeature::TimestampWithoutTimezone,
    ];

    /// Delta properties allowed during CREATE TABLE.
    /// Signal flags (delta.feature.*, delta.minReaderVersion, delta.minWriterVersion)
    /// are always allowed and handled separately.
    const ALLOWED_DELTA_PROPERTIES: [&'static str; 0] = [
        // Currently empty - no delta.* properties allowed yet
        // As property transforms are added, their properties go here:
        // "delta.enableDeletionVectors",
        // "delta.columnMapping.mode",
        // "delta.enableChangeDataFeed",
    ];
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
    fn test_try_from_creates_bare_protocol() {
        let properties = props([("myapp.version", "1.0"), ("custom.property", "value")]);
        let config = TableProtocolMetadataConfig::new(test_schema(), vec![], properties).unwrap();

        // Protocol should be bare v3/v7 with empty features
        assert_eq!(config.protocol.min_reader_version(), 3);
        assert_eq!(config.protocol.min_writer_version(), 7);
        assert!(config.protocol.reader_features().unwrap().is_empty());
        assert!(config.protocol.writer_features().unwrap().is_empty());
    }

    #[test]
    fn test_try_from_passes_all_properties_through() {
        let properties = props([
            ("myapp.version", "1.0"),
            ("delta.feature.deletionVectors", "supported"),
            ("delta.minReaderVersion", "3"),
            ("custom.property", "value"),
        ]);
        let config = TableProtocolMetadataConfig::new(test_schema(), vec![], properties).unwrap();

        // ALL properties should be in metadata - no filtering in try_from
        assert_eq!(config.metadata.configuration().len(), 4);
        assert_eq!(
            config.metadata.configuration().get("myapp.version"),
            Some(&"1.0".to_string())
        );
        assert_eq!(
            config
                .metadata
                .configuration()
                .get("delta.feature.deletionVectors"),
            Some(&"supported".to_string())
        );
        assert_eq!(
            config
                .metadata
                .configuration()
                .get("delta.minReaderVersion"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn test_try_from_with_partition_columns() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("region", DataType::STRING, false),
        ]);
        let config =
            TableProtocolMetadataConfig::new(schema, vec!["region".to_string()], HashMap::new())
                .unwrap();

        assert_eq!(config.metadata.partition_columns(), &["region".to_string()]);
    }

    // =========================================================================
    // Pipeline Integration Tests
    // =========================================================================

    #[test]
    fn test_apply_transforms_with_no_signals() {
        use crate::table_transformation::TransformationPipeline;

        // With no signal flags, config passes through with empty features
        let properties = props([("myapp.version", "1.0")]);
        let config =
            TableProtocolMetadataConfig::new(test_schema(), vec![], properties.clone()).unwrap();

        let output = TransformationPipeline::apply_transforms(config, &properties).unwrap();

        // Protocol still bare, properties still there
        assert!(output.config.protocol.writer_features().unwrap().is_empty());
        assert_eq!(
            output.config.metadata.configuration().get("myapp.version"),
            Some(&"1.0".to_string())
        );
    }
}
