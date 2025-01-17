//! High level api to check table feature status.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::actions::{ensure_supported_features, Metadata, Protocol};
use crate::schema::{Schema, SchemaRef};
use crate::table_features::{
    column_mapping_mode, validate_schema_column_mapping, ColumnMappingMode, ReaderFeatures,
    WriterFeatures,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Error};

pub struct TableConfiguration {
    metadata: Metadata,
    protocol: Protocol,
    schema: SchemaRef,
    table_properties: TableProperties,
    column_mapping_mode: ColumnMappingMode,
}

impl TableConfiguration {
    pub fn try_new(metadata: Metadata, protocol: Protocol) -> DeltaResult<Self> {
        // important! before a read/write to the table we must check it is supported
        protocol.ensure_read_supported()?;

        // validate column mapping mode -- all schema fields should be correctly (un)annotated
        let schema = Arc::new(metadata.parse_schema()?);
        let table_properties = metadata.parse_table_properties();
        let column_mapping_mode = column_mapping_mode(&protocol, &table_properties);
        validate_schema_column_mapping(&schema, column_mapping_mode)?;
        Ok(Self {
            schema,
            metadata,
            protocol,
            table_properties,
            column_mapping_mode,
        })
    }
    pub(crate) fn column_mapping_mode(&self) -> &ColumnMappingMode {
        &self.column_mapping_mode
    }
    pub(crate) fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }
    pub(crate) fn protocol(&self) -> &Protocol {
        &self.protocol
    }
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub(crate) fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }

    /// Ensures that Change Data Feed is supported for a table with this [`Protocol`] .
    /// See the documentation of [`TableChanges`] for more details.
    ///
    /// [`TableChanges`]: crate::table_changes::TableChanges
    pub fn is_cdf_read_supported(&self) -> bool {
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
    /// Returns `Ok(())` if reading deletion vectors is supported on this table.
    ///
    /// Note:  readers are not disallowed from reading deletion vectors if the table property is
    /// false or not present. The protocol only states that:
    /// > Readers must read the table considering the existence of DVs, even when the
    /// > delta.enableDeletionVectors table property is not set.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    pub fn is_deletion_vector_supported(&self) -> bool {
        static DELETION_VECTOR_READER_FEATURE: LazyLock<HashSet<ReaderFeatures>> =
            LazyLock::new(|| HashSet::from([ReaderFeatures::DeletionVectors]));
        static DELETION_VECTOR_WRITER_FEATURE: LazyLock<HashSet<WriterFeatures>> =
            LazyLock::new(|| HashSet::from([WriterFeatures::DeletionVectors]));
        let reader_supported = match self.protocol().reader_features() {
            Some(features) if self.protocol().min_reader_version() == 3 => {
                ensure_supported_features(features, &DELETION_VECTOR_READER_FEATURE).is_ok()
            }
            _ => false,
        };
        let writer_supported = match self.protocol().writer_features() {
            Some(features) if self.protocol().min_writer_version() == 7 => {
                ensure_supported_features(features, &DELETION_VECTOR_WRITER_FEATURE).is_ok()
            }
            _ => false,
        };
        reader_supported && writer_supported
    }

    /// Returns `Ok(())` if writing deletion vectors is supported on this table.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    pub fn is_deletion_vector_enabled(&self) -> bool {
        self.is_deletion_vector_supported()
            && self
                .table_properties
                .enable_deletion_vectors
                .unwrap_or(false)
    }
}
