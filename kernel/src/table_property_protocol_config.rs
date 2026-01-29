//! Configuration for table property-based protocol modifications.
//!
//! This module provides [`TablePropertyProtocolConfig`], which parses user-provided table
//! properties to extract signal flags and create protocol configurations for table operations.
//!
//! # Difference from [`TableConfiguration`](crate::table_configuration::TableConfiguration)
//!
//! - **[`TableConfiguration`](crate::table_configuration::TableConfiguration)**: Reads the
//!   configuration of an *existing* table from a snapshot. It combines the Protocol and Metadata
//!   actions to provide a unified view of a table's current state.
//!
//! - **[`TablePropertyProtocolConfig`]**: Parses *user-provided* properties during table creation
//!   or modification operations (CREATE TABLE, ALTER TABLE SET TBLPROPERTIES). It extracts signal
//!   flags (like `delta.feature.<name>` and protocol versions) that affect the Protocol but are
//!   not stored in metadata.
//!
//! In short: `TableConfiguration` is for **reading** existing tables, while
//! `TablePropertyProtocolConfig` is for **creating/modifying** tables.

use std::collections::HashMap;

use crate::actions::Protocol;
use crate::table_features::{
    TableFeature, SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
    TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
use crate::{DeltaResult, Error};

/// Configuration parsed from user-provided table properties during table creation or modification.
///
/// This struct extracts signal flags (protocol versions, feature overrides) from
/// the input properties and separates them from the table properties that will
/// be stored in metadata.
///
/// Signal flags include:
/// - `delta.feature.<name> = supported` - Explicit feature enablement
/// - `delta.minReaderVersion` - Protocol reader version hint
/// - `delta.minWriterVersion` - Protocol writer version hint
///
/// These signal flags affect protocol creation but are not stored in the table's
/// metadata configuration.
///
/// # Example
/// ```ignore
/// let props = HashMap::from([
///     ("delta.feature.deletionVectors".to_string(), "supported".to_string()),
///     ("delta.minReaderVersion".to_string(), "3".to_string()),
///     ("myapp.version".to_string(), "1.0".to_string()),
/// ]);
/// let config = TablePropertyProtocolConfig::try_from(props)?;
/// config.validate_for_create()?;
/// // config.protocol contains Protocol with reader/writer features
/// // config.table_properties = {"myapp.version": "1.0"}
/// ```
#[derive(Debug)]
pub(crate) struct TablePropertyProtocolConfig {
    /// The resolved protocol (always uses table features protocol: v3/v7).
    /// Created during parsing with user-specified features.
    pub(crate) protocol: Protocol,
    /// Remaining properties to store in Metadata.configuration.
    /// Signal flags (delta.feature.*) and version props are stripped out.
    pub(crate) table_properties: HashMap<String, String>,
}

impl TryFrom<HashMap<String, String>> for TablePropertyProtocolConfig {
    type Error = Error;

    fn try_from(properties: HashMap<String, String>) -> DeltaResult<Self> {
        let mut all_features = Vec::new();
        let mut user_reader_version: Option<i32> = None;
        let mut user_writer_version: Option<i32> = None;
        let mut table_properties = HashMap::new();

        for (key, value) in properties {
            if let Some(feature_name) = key.strip_prefix(SET_TABLE_FEATURE_SUPPORTED_PREFIX) {
                // Parse delta.feature.* signal flags
                if value != SET_TABLE_FEATURE_SUPPORTED_VALUE {
                    return Err(Error::generic(format!(
                        "Invalid value '{}' for '{}'. Only '{}' is allowed.",
                        value, key, SET_TABLE_FEATURE_SUPPORTED_VALUE
                    )));
                }
                let feature = TableFeature::from_name(feature_name);
                if matches!(feature, TableFeature::Unknown(_)) {
                    return Err(Error::generic(format!(
                        "Unknown table feature '{}'. Cannot create table with unsupported features.",
                        feature_name
                    )));
                }
                all_features.push(feature);
            } else if key == MIN_READER_VERSION_PROP {
                // Parse delta.minReaderVersion
                let version: i32 = value.parse().map_err(|_| {
                    Error::generic(format!(
                        "Invalid value '{}' for '{}'. Must be an integer.",
                        value, key
                    ))
                })?;
                // Validate immediately: if specified, must be the required version
                if version != TABLE_FEATURES_MIN_READER_VERSION {
                    return Err(Error::generic(format!(
                        "Invalid value '{}' for '{}'. Only '{}' is supported.",
                        version, MIN_READER_VERSION_PROP, TABLE_FEATURES_MIN_READER_VERSION
                    )));
                }
                user_reader_version = Some(version);
            } else if key == MIN_WRITER_VERSION_PROP {
                // Parse delta.minWriterVersion
                let version: i32 = value.parse().map_err(|_| {
                    Error::generic(format!(
                        "Invalid value '{}' for '{}'. Must be an integer.",
                        value, key
                    ))
                })?;
                // Validate immediately: if specified, must be the required version
                if version != TABLE_FEATURES_MIN_WRITER_VERSION {
                    return Err(Error::generic(format!(
                        "Invalid value '{}' for '{}'. Only '{}' is supported.",
                        version, MIN_WRITER_VERSION_PROP, TABLE_FEATURES_MIN_WRITER_VERSION
                    )));
                }
                user_writer_version = Some(version);
            } else {
                // Pass through to table properties
                table_properties.insert(key, value);
            }
        }

        // Partition features: ReaderWriter -> reader list, all -> writer list
        let reader_features: Vec<_> = all_features
            .iter()
            .filter(|f| f.is_reader_writer())
            .cloned()
            .collect();

        // Create Protocol with resolved versions.
        // User-specified versions (if any) were validated above; default to required versions.
        let protocol = Protocol::try_new(
            user_reader_version.unwrap_or(TABLE_FEATURES_MIN_READER_VERSION),
            user_writer_version.unwrap_or(TABLE_FEATURES_MIN_WRITER_VERSION),
            Some(reader_features.iter()),
            Some(all_features.iter()),
        )?;

        Ok(Self {
            protocol,
            table_properties,
        })
    }
}

impl TablePropertyProtocolConfig {
    /// Delta properties allowed during CREATE TABLE.
    /// Expand this list as more features are supported (e.g., column mapping, clustering).
    const ALLOWED_DELTA_PROPERTIES: &[&str] = &[
        // Currently empty - no delta.* properties allowed yet
        // Future: "delta.enableDeletionVectors", "delta.columnMapping.mode", etc.
    ];

    /// Table features allowed during CREATE TABLE.
    /// Expand this list as more features are supported.
    const ALLOWED_DELTA_FEATURES: &[TableFeature] = &[
        // Currently empty - no explicit feature overrides allowed yet
        // Future: TableFeature::DeletionVectors, TableFeature::ColumnMapping, etc.
    ];

    /// Validates the configuration for CREATE TABLE.
    ///
    /// Checks:
    /// - Only allowed delta properties can be set
    /// - Only allowed features can be explicitly enabled
    ///
    /// Note: Protocol version validation happens during parsing in `try_from`.
    pub(crate) fn validate_for_create(&self) -> DeltaResult<()> {
        use crate::table_properties::DELTA_PROPERTY_PREFIX;

        // Validate delta properties against allow-list
        for key in self.table_properties.keys() {
            if key.starts_with(DELTA_PROPERTY_PREFIX)
                && !Self::ALLOWED_DELTA_PROPERTIES.contains(&key.as_str())
            {
                return Err(Error::generic(format!(
                    "Setting delta property '{}' is not supported during CREATE TABLE",
                    key
                )));
            }
        }

        // Validate features against allow-list
        if let Some(writer_features) = self.protocol.writer_features() {
            for feature in writer_features {
                if !Self::ALLOWED_DELTA_FEATURES.contains(feature) {
                    return Err(Error::generic(format!(
                        "Enabling feature '{}' is not supported during CREATE TABLE",
                        feature
                    )));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_properties::{MIN_READER_VERSION_PROP, MIN_WRITER_VERSION_PROP};
    use crate::utils::test_utils::assert_result_error_with_message;

    /// Helper to construct a HashMap<String, String> from string slice pairs.
    fn props<const N: usize>(pairs: [(&str, &str); N]) -> HashMap<String, String> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect()
    }

    #[test]
    fn test_table_property_protocol_config_basic() {
        let config = TablePropertyProtocolConfig::try_from(props([
            ("delta.feature.deletionVectors", "supported"),
            ("delta.enableDeletionVectors", "true"),
        ]))
        .unwrap();

        // DeletionVectors is a ReaderWriter feature - check via protocol
        let reader_features = config.protocol.reader_features().unwrap();
        let writer_features = config.protocol.writer_features().unwrap();
        assert_eq!(reader_features.len(), 1);
        assert_eq!(reader_features[0], TableFeature::DeletionVectors);
        assert_eq!(writer_features.len(), 1);
        assert_eq!(writer_features[0], TableFeature::DeletionVectors);

        // Feature override should be removed from table_properties
        assert!(!config
            .table_properties
            .contains_key("delta.feature.deletionVectors"));
        // Regular property should be retained
        assert_eq!(
            config.table_properties.get("delta.enableDeletionVectors"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_table_property_protocol_config_multiple_features() {
        let config = TablePropertyProtocolConfig::try_from(props([
            ("delta.feature.deletionVectors", "supported"),
            ("delta.feature.changeDataFeed", "supported"),
            ("delta.feature.appendOnly", "supported"),
            ("delta.enableDeletionVectors", "true"),
        ]))
        .unwrap();

        // All 3 features go into writer_features - check via protocol
        let writer_features = config.protocol.writer_features().unwrap();
        assert_eq!(writer_features.len(), 3);
        assert!(writer_features.contains(&TableFeature::DeletionVectors));
        assert!(writer_features.contains(&TableFeature::ChangeDataFeed));
        assert!(writer_features.contains(&TableFeature::AppendOnly));

        // Only ReaderWriter features go into reader_features (DeletionVectors)
        // ChangeDataFeed and AppendOnly are Writer-only features
        let reader_features = config.protocol.reader_features().unwrap();
        assert_eq!(reader_features.len(), 1);
        assert!(reader_features.contains(&TableFeature::DeletionVectors));

        // Only the regular property should remain
        assert_eq!(config.table_properties.len(), 1);
        assert_eq!(
            config.table_properties.get("delta.enableDeletionVectors"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_table_property_protocol_config_no_features() {
        let config = TablePropertyProtocolConfig::try_from(props([
            ("delta.enableDeletionVectors", "true"),
            ("delta.appendOnly", "true"),
            ("custom.property", "value"),
        ]))
        .unwrap();

        // No features specified - protocol should have empty feature lists
        let reader_features = config.protocol.reader_features().unwrap();
        let writer_features = config.protocol.writer_features().unwrap();
        assert!(reader_features.is_empty());
        assert!(writer_features.is_empty());
        assert_eq!(config.table_properties.len(), 3);
    }

    #[test]
    fn test_table_property_protocol_config_parsing_failures() {
        // Invalid feature value ("enabled" instead of "supported")
        assert_result_error_with_message(
            TablePropertyProtocolConfig::try_from(props([(
                "delta.feature.deletionVectors",
                "enabled",
            )])),
            "Invalid value 'enabled'",
        );

        // Unknown feature name
        assert_result_error_with_message(
            TablePropertyProtocolConfig::try_from(props([(
                "delta.feature.futureFeature",
                "supported",
            )])),
            "Unknown table feature 'futureFeature'",
        );
    }

    #[test]
    fn test_table_property_protocol_config_valid_versions() {
        let config = TablePropertyProtocolConfig::try_from(props([
            (MIN_READER_VERSION_PROP, "3"),
            (MIN_WRITER_VERSION_PROP, "7"),
            ("custom.property", "value"),
        ]))
        .unwrap();

        // Protocol created with correct versions
        assert_eq!(config.protocol.min_reader_version(), 3);
        assert_eq!(config.protocol.min_writer_version(), 7);
        // Protocol version properties stripped from table_properties
        assert!(!config
            .table_properties
            .contains_key(MIN_READER_VERSION_PROP));
        assert!(!config
            .table_properties
            .contains_key(MIN_WRITER_VERSION_PROP));
        // Other properties remain
        assert_eq!(
            config.table_properties.get("custom.property"),
            Some(&"value".to_string())
        );
        // Validation passes
        assert!(config.validate_for_create().is_ok());
    }

    #[test]
    fn test_table_property_protocol_config_validation_errors() {
        // Each case: (description, input properties, expected error substring, fails_at_parse)
        let error_cases: &[(&str, &[(&str, &str)], &str, bool)] = &[
            (
                "Invalid reader version (only 3 supported)",
                &[(MIN_READER_VERSION_PROP, "2")],
                "Only '3' is supported",
                true, // fails at parsing
            ),
            (
                "Invalid writer version (only 7 supported)",
                &[(MIN_WRITER_VERSION_PROP, "5")],
                "Only '7' is supported",
                true, // fails at parsing
            ),
            (
                "Feature not on allow list",
                &[("delta.feature.deletionVectors", "supported")],
                "Enabling feature 'deletionVectors' is not supported",
                false, // fails at validation
            ),
            (
                "Delta property not on allow list",
                &[("delta.appendOnly", "true")],
                "Setting delta property 'delta.appendOnly' is not supported",
                false, // fails at validation
            ),
        ];

        for (description, input, expected_error, fails_at_parse) in error_cases {
            let input_map: HashMap<String, String> = input
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            if *fails_at_parse {
                assert_result_error_with_message(
                    TablePropertyProtocolConfig::try_from(input_map),
                    expected_error,
                );
            } else {
                let config = TablePropertyProtocolConfig::try_from(input_map)
                    .unwrap_or_else(|e| panic!("{}: unexpected parse error: {}", description, e));
                assert_result_error_with_message(config.validate_for_create(), expected_error);
            }
        }
    }
}
