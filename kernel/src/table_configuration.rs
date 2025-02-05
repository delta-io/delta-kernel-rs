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
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::{ensure_supported_features, Metadata, Protocol};
use crate::schema::{Schema, SchemaRef};
use crate::table_features::{
    column_mapping_mode, validate_schema_column_mapping, ColumnMappingMode, ReaderFeatures,
    WriterFeatures,
};
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Version};

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
#[derive(Debug)]
pub(crate) struct TableConfiguration {
    metadata: Metadata,
    protocol: Protocol,
    schema: SchemaRef,
    table_properties: TableProperties,
    column_mapping_mode: ColumnMappingMode,
    table_root: Url,
    version: Version,
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
    ) -> DeltaResult<Self> {
        protocol.ensure_read_supported()?;

        let schema = Arc::new(metadata.parse_schema()?);
        let table_properties = metadata.parse_table_properties();
        let column_mapping_mode = column_mapping_mode(&protocol, &table_properties);

        // validate column mapping mode -- all schema fields should be correctly (un)annotated
        validate_schema_column_mapping(&schema, column_mapping_mode)?;
        Ok(Self {
            schema,
            metadata,
            protocol,
            table_properties,
            column_mapping_mode,
            table_root,
            version,
        })
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
    /// Returns `true` if the kernel supports writing to this table. This checks that the
    /// protocol's writer features are all supported.
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_write_supported(&self) -> bool {
        self.protocol.ensure_write_supported().is_ok()
    }
    /// Returns `true` if kernel supports reading Change Data Feed on this table.
    /// See the documentation of [`TableChanges`] for more details.
    ///
    /// [`TableChanges`]: crate::table_changes::TableChanges
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_cdf_read_supported(&self) -> bool {
        static CDF_SUPPORTED_READER_FEATURES: LazyLock<HashSet<ReaderFeatures>> =
            LazyLock::new(|| HashSet::from([ReaderFeatures::DeletionVectors]));
        let protocol_supported = match self.protocol.reader_features() {
            // if min_reader_version = 3 and all reader features are subset of supported => OK
            Some(reader_features) if self.protocol.min_reader_version() == 3 => {
                ensure_supported_features(reader_features, &CDF_SUPPORTED_READER_FEATURES).is_ok()
            }
            // if min_reader_version = 1 and there are no reader features => OK
            None => self.protocol.min_reader_version() == 1,
            // any other protocol is not supported
            _ => false,
        };
        let cdf_enabled = self
            .table_properties
            .enable_change_data_feed
            .unwrap_or(false);
        let column_mapping_disabled = matches!(
            self.table_properties.column_mapping_mode,
            None | Some(ColumnMappingMode::None)
        );
        protocol_supported && cdf_enabled && column_mapping_disabled
    }
    /// Returns `true` if deletion vectors is supported on this table. To support deletion vectors,
    /// a table must support reader version 3, writer version 7, and the deletionVectors feature in
    /// both the protocol's readerFeatures and writerFeatures.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_deletion_vector_supported(&self) -> bool {
        let read_supported = self
            .protocol()
            .has_reader_feature(&ReaderFeatures::DeletionVectors)
            && self.protocol.min_reader_version() == 3;
        let write_supported = self
            .protocol()
            .has_writer_feature(&WriterFeatures::DeletionVectors)
            && self.protocol.min_writer_version() == 7;
        read_supported && write_supported
    }

    /// Returns `true` if writing deletion vectors is enabled for this table. This is the case
    /// when the deletion vectors is supported on this table and the `delta.enableDeletionVectors`
    /// table property is set to `true`.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    #[allow(unused)]
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn is_deletion_vector_enabled(&self) -> bool {
        self.is_deletion_vector_supported()
            && self
                .table_properties
                .enable_deletion_vectors
                .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use url::Url;

    use crate::actions::{Metadata, Protocol};
    use crate::table_features::{ReaderFeatures, WriterFeatures};

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
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            Some([WriterFeatures::DeletionVectors]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_deletion_vector_supported());
        assert!(!table_config.is_deletion_vector_enabled());
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
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            Some([WriterFeatures::DeletionVectors]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_deletion_vector_supported());
        assert!(table_config.is_deletion_vector_enabled());
    }
    #[test]
    fn fails_on_unsupported_feature() {
        let metadata = Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::V2Checkpoint]),
            Some([WriterFeatures::V2Checkpoint]),
        )
        .unwrap();
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
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::TimestampWithoutTimezone]),
            Some([WriterFeatures::TimestampWithoutTimezone]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(!table_config.is_deletion_vector_supported());
        assert!(!table_config.is_deletion_vector_enabled());
    }
}
