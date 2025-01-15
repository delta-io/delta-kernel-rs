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
    pub fn new(metadata: Metadata, protocol: Protocol) -> DeltaResult<Self> {
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
    pub fn with_protocol(self, protocol: impl Into<Option<Protocol>>) -> DeltaResult<Self> {
        let Some(protocol) = protocol.into() else {
            return Ok(self);
        };
        // important! before a read/write to the table we must check it is supported
        protocol.ensure_read_supported()?;

        let column_mapping_mode = column_mapping_mode(&protocol, &self.table_properties);
        Ok(Self {
            protocol,
            column_mapping_mode,
            ..self
        })
    }
    pub fn with_metadata(self, metadata: impl Into<Option<Metadata>>) -> DeltaResult<Self> {
        let Some(metadata) = metadata.into() else {
            return Ok(self);
        };
        let schema = Arc::new(metadata.parse_schema()?);
        let table_properties = metadata.parse_table_properties();
        let column_mapping_mode = column_mapping_mode(&self.protocol, &table_properties);
        validate_schema_column_mapping(&schema, column_mapping_mode)?;
        Ok(Self {
            schema,
            table_properties,
            column_mapping_mode,
            ..self
        })
    }
    pub fn column_mapping_mode(&self) -> &ColumnMappingMode {
        &self.column_mapping_mode
    }
    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }
    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }

    /// Ensures that Change Data Feed is supported for a table with this [`Protocol`] .
    /// See the documentation of [`TableChanges`] for more details.
    ///
    /// [`TableChanges`]: crate::table_changes::TableChanges
    pub fn is_cdf_read_supported(&self) -> DeltaResult<()> {
        static CDF_SUPPORTED_READER_FEATURES: LazyLock<HashSet<ReaderFeatures>> =
            LazyLock::new(|| HashSet::from([ReaderFeatures::DeletionVectors]));
        let protocol = &self.protocol;
        match protocol.reader_features() {
            // if min_reader_version = 3 and all reader features are subset of supported => OK
            Some(reader_features) if protocol.min_reader_version() == 3 => {
                ensure_supported_features(reader_features, &CDF_SUPPORTED_READER_FEATURES)?
            }
            // if min_reader_version = 1 and there are no reader features => OK
            None if protocol.min_reader_version() == 1 => (),
            // any other protocol is not supported
            _ => {
                return Err(Error::unsupported(
                    "Change data feed not supported on this protocol",
                ));
            }
        };
        require!(
            self.table_properties
                .enable_change_data_feed
                .unwrap_or(false),
            Error::unsupported("Change data feed is not enabled")
        );
        require!(
            matches!(
                self.table_properties.column_mapping_mode,
                None | Some(ColumnMappingMode::None)
            ),
            Error::unsupported("Change data feed not supported when column mapping is enabled")
        );
        Ok(())
    }
    /// Returns `Ok(())` if reading deletion vectors is supported on this table.
    ///
    /// Note:  readers are not disallowed from reading deletion vectors if the table property is
    /// false or not present. The protocol only states that:
    /// > Readers must read the table considering the existence of DVs, even when the
    /// > delta.enableDeletionVectors table property is not set.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    pub fn is_deletion_vector_read_supported(&self) -> DeltaResult<()> {
        static DELETION_VECTOR_READER_FEATURE: LazyLock<HashSet<ReaderFeatures>> =
            LazyLock::new(|| HashSet::from([ReaderFeatures::DeletionVectors]));
        require!(
            self.protocol.min_reader_version() == 3,
            Error::unsupported("Table must be version 3 for deletion vectors to be supported")
        );
        let Some(features) = self.protocol.reader_features() else {
            return Err(Error::unsupported(
                "Deletion vector reader feature not found for protocol",
            ));
        };
        ensure_supported_features(features, &DELETION_VECTOR_READER_FEATURE)?;
        Ok(())
    }

    /// Returns `Ok(())` if writing deletion vectors is supported on this table.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    pub fn is_deletion_vector_write_supported(&self) -> DeltaResult<()> {
        static DELETION_VECTOR_WRITER_FEATURE: LazyLock<HashSet<WriterFeatures>> =
            LazyLock::new(|| HashSet::from([WriterFeatures::DeletionVectors]));
        require!(
            self.protocol.min_reader_version() == 3,
            Error::unsupported("Table must be version 3 for deletion vectors to be supported")
        );
        let Some(features) = self.protocol.reader_features() else {
            return Err(Error::unsupported(
                "Deletion vector writer feature not found for protocol",
            ));
        };
        ensure_supported_features(features, &DELETION_VECTOR_WRITER_FEATURE)?;

        let dv_enabled = self.table_properties.enable_deletion_vectors;
        require!(
            dv_enabled.unwrap_or(false),
            Error::unsupported(
                "Cannot write deletion vector: `delta.enableDeletionVectors` is not set to true"
            )
        );

        Ok(())
    }
}
