//! Provides a level api to check feature support and enablement for a table.
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
/// reader and writer features, table properties, and schema. [`TableConfiguration`] performs
/// checks when constructed using [`TableConfiguration::try_new`]  to validate that Metadata
/// and Protocol are correctly formatted and mutually compatible.
///
/// For example, deletion vector support can be checked with
/// [`TableConfiguration::is_deletion_vector_supported`] and deletion vector write enablement can
/// be checked  with [`TableConfiguration::is_deletion_vector_enabled`]. [`TableConfiguration`]
/// wraps both a [`Metadata`]  and a [`Protocol`], and validates that they are well-formed.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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
    /// Constructs a [`TableConfiguration`] for a table located in `table_root` at version
    /// `version`. This validates  that the [`Metadata`] and [`Protocol`] are mutually compatible,
    /// and that the kernel supports reading from this table.
    pub(crate) fn try_new(
        metadata: Metadata,
        protocol: Protocol,
        table_root: Url,
        version: Version,
    ) -> DeltaResult<Self> {
        // important! before a read/write to the table we must check it is supported
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
    /// The [`Protocol`] of this table at  this version.
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
    pub(crate) fn column_mapping_mode(&self) -> &ColumnMappingMode {
        &self.column_mapping_mode
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
    /// Ensures that kernel supports reading Change Data Feed on this table and that it is enabled.
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
            None if self.protocol.min_reader_version() == 1 => true,
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

    /// Returns `true` if writing deletion vectors is supported on this table. This is the case
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
