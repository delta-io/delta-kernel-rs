//! This module defines [`TableConfiguration`], a high level api to check feature support and
//! feature enablement for a table at a given version. This encapsulates [`Protocol`], [`Metadata`],
//! [`Schema`], [`TableProperties`], and [`ColumnMappingMode`]. These structs in isolation should
//! be considered raw and unvalidated if they are not a part of [`TableConfiguration`]. We unify
//! these fields because they are deeply intertwined when dealing with table features. For example:
//! To check that deletion vector writes are enabled, you must check both both the protocol's
//! reader/writer features, and ensure that the deletion vector table property is enabled in the
//! [`TableProperties`].
//!
//! [`Schema`]: crate::schema::Schema
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::schema::{Schema, SchemaRef};
use crate::table_features::{
    column_mapping_mode, validate_schema_column_mapping, ColumnMappingMode, ReaderFeatures,
    WriterFeatures, SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES,
};
use crate::table_properties::property_names::{
    COLUMN_MAPPING_MODE, ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::Version;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum InvalidTableConfigurationError {
    #[error("Invalid schema string: {0}")]
    InvalidSchemaString(String),
    #[error("Table with reader version 3 should have reader features, but none were found.")]
    ReaderFeaturesNotFound,
    #[error("Table with writer version 7 should have writer features, but none were found.")]
    WriterFeaturesNotFound,
    #[error("Reader version {1} should not have reader features, but found {}", ._0.iter().map(ToString::to_string).join(", "))]
    ReaderFeaturesInLegacyVersion(HashSet<ReaderFeatures>, i32),
    #[error("Writer version {1} should not have writer features, but found {}", ._0.iter().map(ToString::to_string).join(", "))]
    WriterFeaturesInLegacyVersion(HashSet<WriterFeatures>, i32),
    #[error("Expected all reader features to be in writer features, but the following features were not found in writer features: {}",
        .0.iter().map(ToString::to_string).join(", "))]
    ReaderFeaturesNotFoundInWriterFeatures(HashSet<WriterFeatures>),
    #[error("Found invalid column mapping configuration: {0}")]
    InvalidColumnMapping(String),
    #[error("Failed to parse feature in protocol: {0}")]
    FeatureParseError(String),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum SupportError {
    #[error("The following reader features are not supported: {}", ._0.iter().map(ToString::to_string).join(", "))]
    UnsupportedReaderFeatures(HashSet<ReaderFeatures>),
    #[error("The following writer features are not supported: {}", ._0.iter().map(ToString::to_string).join(", "))]
    UnsupportedWriterFeatures(HashSet<WriterFeatures>),
    #[error("The following table property is not supported: {0}")]
    UnsupportedTableProperty(String),
    #[error("Reader version {0} is not supported")]
    UnsupportedReaderVersion(i32),
    #[error("Writer version {0} is not supported")]
    UnsupportedWriterVersion(i32),
    #[error("Kernel does not support writing with reader version {0} and writer version {1}")]
    WriteUnsupportedOnReaderWriterVersion(i32, i32),
    #[error("Missing table property: {0}")]
    MissingTableProperty(String),
    #[error("Missing the reader feature {0}")]
    MissingReaderFeature(ReaderFeatures),
    #[error("Missing the writer feature {0}")]
    MissingWriterFeature(WriterFeatures),
}

type TableConfigurationResult<T> = Result<T, InvalidTableConfigurationError>;
type SupportResult<T> = Result<T, SupportError>;

pub(crate) struct ReadSupportedTableConfiguration(TableConfiguration);
impl ReadSupportedTableConfiguration {
    fn try_new(table_configuration: TableConfiguration) -> SupportResult<Self> {
        table_configuration.is_read_supported()?;
        Ok(ReadSupportedTableConfiguration(table_configuration))
    }
}
impl TryFrom<TableConfiguration> for ReadSupportedTableConfiguration {
    type Error = SupportError;

    fn try_from(value: TableConfiguration) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}
impl Deref for ReadSupportedTableConfiguration {
    type Target = TableConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct WriteSupportedTableConfiguration(TableConfiguration);
impl WriteSupportedTableConfiguration {
    fn try_new(table_configuration: TableConfiguration) -> SupportResult<Self> {
        table_configuration.is_write_supported()?;
        Ok(WriteSupportedTableConfiguration(table_configuration))
    }
}
impl TryFrom<TableConfiguration> for WriteSupportedTableConfiguration {
    type Error = SupportError;

    fn try_from(value: TableConfiguration) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}
impl Deref for WriteSupportedTableConfiguration {
    type Target = TableConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Holds all the configuration for a table at a specific version. This includes the supported
/// reader and writer features, table properties, schema, version, and table root. This can be used
/// to check whether a table supports a feature or has it enabled. For example, deletion vector
/// support can be checked with [`TableConfiguration::is_deletion_vector_supported`] and deletion
/// vector write enablement can be checked with [`TableConfiguration::is_deletion_vector_enabled`].
///
/// [`TableConfiguration`] performs checks upon construction with `TableConfiguration::try_new`
/// to validate that Metadata and Protocol are correctly formatted and mutually compatible. If
/// `try_new` successfully returns `TableConfiguration`, it is also guaranteed that reading the
/// table is supported.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[derive(Debug, Clone)]
pub(crate) struct TableConfiguration {
    metadata: Metadata,
    protocol: Protocol,
    schema: SchemaRef,
    table_properties: TableProperties,
    column_mapping_mode: ColumnMappingMode,
    table_root: Url,
    version: Version,
    reader_features: Option<HashSet<ReaderFeatures>>,
    writer_features: Option<HashSet<WriterFeatures>>,
}

impl TableConfiguration {
    /// Constructs a [`TableConfiguration`] for a table located in `table_root` at `version`.
    /// This validates that the [`Metadata`] and [`Protocol`] are compatible with one another
    /// and that the kernel supports reading from this table.
    ///
    /// Note: This only returns successfully kernel supports reading the table. It's important
    /// to do this validation is done in `try_new` because all table accesses must first construct
    /// the [`TableConfiguration`]. This ensures that developers never forget to check that kernel
    /// supports reading the table, and that all table accesses are legal.
    ///
    /// Note: In the future, we will perform stricter checks on the set of reader and writer
    /// features. In particular, we will check that:
    ///     - Non-legacy features must appear in both reader features and writer features lists.
    ///       If such a feature is present, the reader version and writer version must be 3, and 5
    ///       respectively.
    ///     - Legacy reader features occur when the reader version is 3, but the writer version is
    ///       either 5 or 6. In this case, the writer feature list must be empty.
    ///     - Column mapping is the only legacy feature present in kernel. No future delta versions
    ///       will introduce new legacy features.
    /// See: <https://github.com/delta-io/delta-kernel-rs/issues/650>
    pub(crate) fn try_new(
        metadata: Metadata,
        protocol: Protocol,
        table_root: Url,
        version: Version,
    ) -> TableConfigurationResult<Self> {
        let schema = Arc::new(metadata.parse_schema().map_err(|_| {
            InvalidTableConfigurationError::InvalidSchemaString(metadata.schema_string.clone())
        })?);
        let table_properties = metadata.parse_table_properties();
        let column_mapping_mode = column_mapping_mode(&protocol, &table_properties);

        // validate column mapping mode -- all schema fields should be correctly (un)annotated
        validate_schema_column_mapping(&schema, column_mapping_mode)
            .map_err(|err| InvalidTableConfigurationError::InvalidColumnMapping(err.to_string()))?;

        let reader_features = Self::parse_features(protocol.reader_features())?;
        let writer_features = Self::parse_features(protocol.writer_features())?;
        Self::validate_protocol(
            protocol.min_reader_version(),
            protocol.min_writer_version(),
            &reader_features,
            &writer_features,
        )?;

        Ok(Self {
            schema,
            metadata,
            protocol,
            table_properties,
            column_mapping_mode,
            table_root,
            version,
            reader_features,
            writer_features,
        })
    }

    fn validate_protocol(
        reader_version: i32,
        writer_version: i32,
        reader_features: &Option<HashSet<ReaderFeatures>>,
        writer_features: &Option<HashSet<WriterFeatures>>,
    ) -> Result<(), InvalidTableConfigurationError> {
        match (reader_features, reader_version) {
            (None, 3) => return Err(InvalidTableConfigurationError::ReaderFeaturesNotFound),
            (Some(features), version) if (1..=2).contains(&version) => {
                return Err(
                    InvalidTableConfigurationError::ReaderFeaturesInLegacyVersion(
                        features.clone(),
                        version,
                    ),
                )
            }
            _ => { /* No error */ }
        }
        match (writer_features, writer_version) {
            (None, 7) => return Err(InvalidTableConfigurationError::WriterFeaturesNotFound),
            (Some(features), version) if (1..=2).contains(&version) => {
                return Err(
                    InvalidTableConfigurationError::WriterFeaturesInLegacyVersion(
                        features.clone(),
                        version,
                    ),
                )
            }
            _ => { /* No error */ }
        }
        if let (Some(r), Some(w)) = (reader_features, writer_features) {
            let x: HashSet<_> = r.iter().map(WriterFeatures::from).collect();
            let difference = &x - w;
            let is_legacy_feature = difference == [WriterFeatures::ColumnMapping].into()
                && (5..=6).contains(&writer_version);
            require!(
                difference.is_empty() || is_legacy_feature,
                InvalidTableConfigurationError::ReaderFeaturesNotFoundInWriterFeatures(difference)
            )
        }

        Ok(())
    }
    fn parse_features<T: FromStr + Hash + Eq>(
        features_opt: Option<&[String]>,
    ) -> TableConfigurationResult<Option<HashSet<T>>> {
        features_opt
            .map(|features| {
                features
                    .iter()
                    .map(|feature| {
                        T::from_str(feature).map_err(|_| {
                            InvalidTableConfigurationError::FeatureParseError(feature.to_string())
                        })
                    })
                    .try_collect()
            })
            .transpose()
    }

    /// Check if reading a table with this protocol is supported. That is: does the kernel support
    /// the specified protocol reader version and all enabled reader features? If yes, returns unit
    /// type, otherwise will return an error.
    fn is_read_supported(&self) -> Result<(), SupportError> {
        match (&self.reader_features, self.protocol.min_reader_version()) {
            // if min_reader_version = 3 and all reader features are subset of supported => OK
            (Some(reader_features), 3) => {
                let difference = reader_features - &(*SUPPORTED_READER_FEATURES);
                if difference.is_empty() {
                    Ok(())
                } else {
                    Err(SupportError::UnsupportedReaderFeatures(difference))
                }
            }
            // if min_reader_version = 1,2 and there are reader features => ERROR
            (Some(_), version) => {
                // NOTE this should be caught in [`TableConfiguration::try_new`]
                debug_assert!(!(1..=2).contains(&version), "try_new should not allow a TableConfiguration at version 1 or 2 with non-empty reader features");
                Err(SupportError::UnsupportedReaderVersion(version))
            }
            // if min_reader_version = 3 and no reader features => ERROR
            // NOTE this is caught by the protocol parsing.
            (None, 3) => {
                debug_assert!(false, "try_new should not allow a ");
                Err(SupportError::UnsupportedReaderFeatures(HashSet::new()))
            }
            // if min_reader_version = 1,2 and there are no reader features => OK
            (None, 1) | (None, 2) => Ok(()),
            // any other min_reader_version is not supported
            (_, version) => Err(SupportError::UnsupportedReaderVersion(version)),
        }
    }

    /// Returns `Ok` if the kernel supports writing to this table. This checks that the
    /// protocol's writer features are all supported.
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub fn is_write_supported(&self) -> SupportResult<()> {
        self.is_read_supported()?;
        match (
            &self.writer_features,
            self.protocol().min_reader_version(),
            self.protocol().min_writer_version(),
        ) {
            // if min_reader_version = 3 and min_writer_version = 7 and all writer features are
            // supported => OK
            (Some(writer_features), 3, 7) => {
                let difference = writer_features - &(*SUPPORTED_WRITER_FEATURES);
                if difference.is_empty() {
                    Ok(())
                } else {
                    Err(SupportError::UnsupportedWriterFeatures(difference))
                }
            }
            // otherwise not supported
            (_, reader_version, writer_version) => Err(
                SupportError::WriteUnsupportedOnReaderWriterVersion(reader_version, writer_version),
            ),
        }
    }
    /// The [`Metadata`] for this table at this version.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    /// The [`Protocol`] of this table at this version.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn protocol(&self) -> &Protocol {
        &self.protocol
    }
    /// The [`Schema`] of for this table at this version.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }
    /// The [`TableProperties`] of this table at this version.
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }
    /// The [`ColumnMappingMode`] for this table at this version.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }
    /// The [`Url`] of the table this [`TableConfiguration`] belongs to
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn table_root(&self) -> &Url {
        &self.table_root
    }
    /// The [`Version`] which this [`TableConfiguration`] belongs to.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn version(&self) -> Version {
        self.version
    }
    /// Returns `true` if kernel supports reading Change Data Feed on this table.
    /// See the documentation of [`TableChanges`] for more details.
    ///
    /// [`TableChanges`]: crate::table_changes::TableChanges
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_cdf_read_supported(&self) -> SupportResult<()> {
        static CDF_SUPPORTED_READER_FEATURES: LazyLock<HashSet<ReaderFeatures>> =
            LazyLock::new(|| HashSet::from([ReaderFeatures::DeletionVectors]));
        match (&self.reader_features, self.protocol.min_reader_version()) {
            // if min_reader_version = 3 and all reader features are subset of supported => OK
            (Some(reader_features), 3) => {
                let difference = reader_features - &CDF_SUPPORTED_READER_FEATURES;
                require!(
                    difference.is_empty(),
                    SupportError::UnsupportedReaderFeatures(difference)
                );
            }
            // if min_reader_version = 1 and there are no reader features => OK
            (features, 2) => {
                assert!(features.is_none());
                return Err(SupportError::UnsupportedReaderVersion(2));
            }
            (features, 1) => {
                // Reader version 1 is supported
                assert!(features.is_none());
            }
            // any other protocol is not supported
            (_, version) => {
                return Err(SupportError::UnsupportedReaderVersion(version));
            }
        };
        self.is_read_supported()?;
        require!(
            self.table_properties
                .enable_change_data_feed
                .unwrap_or(false),
            SupportError::MissingTableProperty(ENABLE_CHANGE_DATA_FEED.to_string())
        );
        require!(
            matches!(
                self.table_properties.column_mapping_mode,
                None | Some(ColumnMappingMode::None)
            ),
            SupportError::UnsupportedTableProperty(COLUMN_MAPPING_MODE.to_string())
        );

        Ok(())
    }
    /// Returns `true` if deletion vectors is supported on this table. To support deletion vectors,
    /// a table must support reader version 3, writer version 7, and the deletionVectors feature in
    /// both the protocol's readerFeatures and writerFeatures.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_deletion_vector_supported(&self) -> SupportResult<()> {
        require!(
            self.reader_features
                .as_ref()
                .is_some_and(|features| features.contains(&ReaderFeatures::DeletionVectors)),
            SupportError::MissingReaderFeature(ReaderFeatures::DeletionVectors)
        );
        require!(
            self.protocol.min_reader_version() == 3,
            SupportError::UnsupportedReaderVersion(self.protocol.min_reader_version())
        );

        let dv_write_support = self
            .writer_features
            .as_ref()
            .is_some_and(|features| features.contains(&WriterFeatures::DeletionVectors));
        debug_assert!(
            dv_write_support,
            "try_now should verify that all non-legacy reader features are present in writer features"
        );
        require!(
            dv_write_support,
            SupportError::UnsupportedReaderVersion(self.protocol.min_reader_version())
        );

        require!(
            self.protocol.min_writer_version() == 7,
            SupportError::UnsupportedWriterVersion(self.protocol.min_writer_version())
        );
        Ok(())
    }

    /// Returns `true` if writing deletion vectors is enabled for this table. This is the case
    /// when the deletion vectors is supported on this table and the `delta.enableDeletionVectors`
    /// table property is set to `true`.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_deletion_vector_enabled(&self) -> SupportResult<()> {
        self.is_deletion_vector_supported()?;
        require!(
            self.table_properties
                .enable_deletion_vectors
                .unwrap_or(false),
            SupportError::MissingTableProperty(ENABLE_DELETION_VECTORS.to_string())
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use url::Url;

    use crate::actions::{Metadata, Protocol};
    use crate::table_configuration::{InvalidTableConfigurationError, SupportError};
    use crate::table_features::{ReaderFeatures, WriterFeatures};
    use crate::table_properties::property_names::ENABLE_DELETION_VECTORS;

    use super::TableConfiguration;

    #[test]
    fn dv_supported_not_enabled() {
        let metadata = Metadata {
            configuration: HashMap::from_iter([(
                "delta.enableChangeDataFeed".to_string(),
                "true".to_string(),
            )]),
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
        let protocol = Protocol::new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            Some([WriterFeatures::DeletionVectors]),
        );
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        table_config
            .is_deletion_vector_supported()
            .expect("dvs are supported in features");
        let Err(SupportError::MissingTableProperty(property)) =
            table_config.is_deletion_vector_enabled()
        else {
            panic!("deletion vector enabled should fail when property is not set");
        };
        assert_eq!(property, ENABLE_DELETION_VECTORS);
    }
    #[test]
    fn dv_enabled() {
        let metadata = Metadata {
            configuration: HashMap::from_iter([(
                "delta.enableChangeDataFeed".to_string(),
                "true".to_string(),
            ),
            (
                "delta.enableDeletionVectors".to_string(),
                "true".to_string(),
            )]),
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
        let protocol = Protocol::new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            Some([WriterFeatures::DeletionVectors]),
        );
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert_eq!(table_config.is_deletion_vector_supported(), Ok(()));
        assert_eq!(table_config.is_deletion_vector_enabled(), Ok(()));
    }
    #[ignore] // TODO (Oussama): Remove if this is no longer necessary
    #[test]
    fn fails_on_unsupported_feature() {
        let metadata = Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
        let protocol = Protocol::new(
            3,
            7,
            Some([ReaderFeatures::V2Checkpoint]),
            Some([WriterFeatures::V2Checkpoint]),
        );
        let table_root = Url::try_from("file:///").unwrap();
        TableConfiguration::try_new(metadata, protocol, table_root, 0)
            .expect_err("V2 checkpoint is not supported in kernel");
    }
    #[test]
    fn dv_not_supported() {
        let metadata = Metadata {
            configuration: HashMap::from_iter([(
                "delta.enableChangeDataFeed".to_string(),
                "true".to_string(),
            )]),
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
        let protocol = Protocol::new(
            3,
            7,
            Some([ReaderFeatures::TimestampWithoutTimezone]),
            Some([
                WriterFeatures::TimestampWithoutTimezone,
                WriterFeatures::DeletionVectors,
            ]),
        );
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();

        assert_eq!(
            Err(SupportError::MissingReaderFeature(
                ReaderFeatures::DeletionVectors
            )),
            table_config.is_deletion_vector_supported()
        );

        assert_eq!(
            Err(SupportError::MissingReaderFeature(
                ReaderFeatures::DeletionVectors
            )),
            table_config.is_deletion_vector_enabled()
        )
    }

    #[test]
    fn test_validate_protocol() {
        assert_eq!(
            Err(InvalidTableConfigurationError::ReaderFeaturesNotFound),
            TableConfiguration::validate_protocol(3, 7, &None, &Some(Default::default())),
        );
        assert_eq!(
            Err(InvalidTableConfigurationError::WriterFeaturesNotFound),
            TableConfiguration::validate_protocol(3, 7, &Some(Default::default()), &None),
        );
        assert!(matches!(
            TableConfiguration::validate_protocol(3, 7, &None, &None),
            Err(InvalidTableConfigurationError::WriterFeaturesNotFound)
                | Err(InvalidTableConfigurationError::ReaderFeaturesNotFound)
        ));
    }
    //#[test]
    //fn test_v2_checkpoint_unsupported() {
    //    let protocol = Protocol {
    //        min_reader_version: 3,
    //        min_writer_version: 7,
    //        reader_features: Some([ReaderFeatures::V2Checkpoint.into()]),
    //        writer_features: Some([ReaderFeatures::V2Checkpoint.into()]),
    //    };
    //
    //    let protocol = Protocol::try_new(
    //        4,
    //        7,
    //        Some([ReaderFeatures::V2Checkpoint]),
    //        Some([ReaderFeatures::V2Checkpoint]),
    //    )
    //    .unwrap();
    //    assert!(protocol.ensure_read_supported().is_err());
    //}
    //
    //#[test]
    //fn test_ensure_read_supported() {
    //    let protocol = Protocol {
    //        min_reader_version: 3,
    //        min_writer_version: 7,
    //        reader_features: Some(vec![]),
    //        writer_features: Some(vec![]),
    //    };
    //    assert!(protocol.ensure_read_supported().is_ok());
    //
    //    let empty_features: [String; 0] = [];
    //    let protocol = Protocol::try_new(
    //        3,
    //        7,
    //        Some([ReaderFeatures::V2Checkpoint]),
    //        Some(&empty_features),
    //    )
    //    .unwrap();
    //    assert!(protocol.ensure_read_supported().is_err());
    //
    //    let protocol = Protocol::try_new(
    //        3,
    //        7,
    //        Some(&empty_features),
    //        Some([WriterFeatures::V2Checkpoint]),
    //    )
    //    .unwrap();
    //    assert!(protocol.ensure_read_supported().is_ok());
    //
    //    let protocol = Protocol::try_new(
    //        3,
    //        7,
    //        Some([ReaderFeatures::V2Checkpoint]),
    //        Some([WriterFeatures::V2Checkpoint]),
    //    )
    //    .unwrap();
    //    assert!(protocol.ensure_read_supported().is_err());
    //
    //    let protocol = Protocol {
    //        min_reader_version: 1,
    //        min_writer_version: 7,
    //        reader_features: None,
    //        writer_features: None,
    //    };
    //    assert!(protocol.ensure_read_supported().is_ok());
    //
    //    let protocol = Protocol {
    //        min_reader_version: 2,
    //        min_writer_version: 7,
    //        reader_features: None,
    //        writer_features: None,
    //    };
    //    assert!(protocol.ensure_read_supported().is_ok());
    //}
    //
    //#[test]
    //fn test_ensure_write_supported() {
    //    let protocol = Protocol {
    //        min_reader_version: 3,
    //        min_writer_version: 7,
    //        reader_features: Some(vec![]),
    //        writer_features: Some(vec![]),
    //    };
    //    assert!(protocol.ensure_write_supported().is_ok());
    //
    //    let protocol = Protocol::try_new(
    //        3,
    //        7,
    //        Some([ReaderFeatures::DeletionVectors]),
    //        Some([WriterFeatures::DeletionVectors]),
    //    )
    //    .unwrap();
    //    assert!(protocol.ensure_write_supported().is_err());
    //}
    //
    //#[test]
    //fn test_ensure_supported_features() {
    //    let supported_features = [
    //        ReaderFeatures::ColumnMapping,
    //        ReaderFeatures::DeletionVectors,
    //    ]
    //    .into_iter()
    //    .collect();
    //    let table_features = vec![ReaderFeatures::ColumnMapping.to_string()];
    //    ensure_supported_features(&table_features, &supported_features).unwrap();
    //
    //    // test unknown features
    //    let table_features = vec![ReaderFeatures::ColumnMapping.to_string(), "idk".to_string()];
    //    let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
    //    match error {
    //        Error::Unsupported(e) if e ==
    //            "Unknown ReaderFeatures [\"idk\"]. Supported ReaderFeatures are [ColumnMapping, DeletionVectors]"
    //        => {},
    //        Error::Unsupported(e) if e ==
    //            "Unknown ReaderFeatures [\"idk\"]. Supported ReaderFeatures are [DeletionVectors, ColumnMapping]"
    //        => {},
    //        _ => panic!("Expected unsupported error"),
    //    }
    //}
}
