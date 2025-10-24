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
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::{ensure_supported_features, Metadata, Protocol};
use crate::schema::variant_utils::validate_variant_type_feature_support;
use crate::schema::{InvariantChecker, SchemaRef};
use crate::table_features::{
    column_mapping_mode, validate_schema_column_mapping, validate_timestamp_ntz_feature_support,
    ColumnMappingMode, TableFeature,
};
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error, Version};
use delta_kernel_derive::internal_api;

/// Information about in-commit timestamp enablement state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InCommitTimestampEnablement {
    /// In-commit timestamps is not enabled
    NotEnabled,
    /// In-commit timestamps is enabled
    Enabled {
        /// Enablement information, if available. `None` indicates the table was created
        /// with ICT enabled from the beginning (no enablement properties needed).
        enablement: Option<(Version, i64)>,
    },
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
#[internal_api]
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Note: This only returns successfully if kernel supports reading the table. It's important
    /// to do this validation in `try_new` because all table accesses must first construct
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
    #[internal_api]
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

        validate_timestamp_ntz_feature_support(&schema, &protocol)?;

        validate_variant_type_feature_support(&schema, &protocol)?;

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

    pub(crate) fn try_new_from(
        table_configuration: &Self,
        new_metadata: Option<Metadata>,
        new_protocol: Option<Protocol>,
        new_version: Version,
    ) -> DeltaResult<Self> {
        // simplest case: no new P/M, just return the existing table configuration with new version
        if new_metadata.is_none() && new_protocol.is_none() {
            return Ok(Self {
                version: new_version,
                ..table_configuration.clone()
            });
        }

        // note that while we could pick apart the protocol/metadata updates and validate them
        // individually, instead we just re-parse so that we can recycle the try_new validation
        // (instead of duplicating it here).
        Self::try_new(
            new_metadata.unwrap_or_else(|| table_configuration.metadata.clone()),
            new_protocol.unwrap_or_else(|| table_configuration.protocol.clone()),
            table_configuration.table_root.clone(),
            new_version,
        )
    }

    /// The [`Metadata`] for this table at this version.
    #[internal_api]
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// The [`Protocol`] of this table at this version.
    #[internal_api]
    pub(crate) fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    /// The logical schema ([`SchemaRef`]) of this table at this version.
    #[internal_api]
    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The [`TableProperties`] of this table at this version.
    #[internal_api]
    pub(crate) fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }

    /// The [`ColumnMappingMode`] for this table at this version.
    #[internal_api]
    pub(crate) fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }

    /// The [`Url`] of the table this [`TableConfiguration`] belongs to
    #[internal_api]
    pub(crate) fn table_root(&self) -> &Url {
        &self.table_root
    }

    /// The [`Version`] which this [`TableConfiguration`] belongs to.
    #[internal_api]
    pub(crate) fn version(&self) -> Version {
        self.version
    }

    /// Returns `true` if the kernel supports writing to this table. This checks that the
    /// protocol's writer features are all supported.
    #[internal_api]
    pub(crate) fn ensure_write_supported(&self) -> DeltaResult<()> {
        self.protocol.ensure_write_supported()?;

        // for now we don't allow invariants so although we support writer version 2 and the
        // ColumnInvariant TableFeature we _must_ check here that they are not actually in use
        if self.is_invariants_supported()
            && InvariantChecker::has_invariants(self.schema().as_ref())
        {
            return Err(Error::unsupported(
                "Column invariants are not yet supported",
            ));
        }

        // Fail if row tracking is both enabled and suspended
        if self.is_row_tracking_enabled() && self.is_row_tracking_suspended() {
            return Err(Error::unsupported(
                "Row tracking cannot be both enabled and suspended",
            ));
        }

        Ok(())
    }

    /// Returns `true` if kernel supports reading Change Data Feed on this table.
    /// See the documentation of [`TableChanges`] for more details.
    ///
    /// [`TableChanges`]: crate::table_changes::TableChanges
    #[internal_api]
    pub(crate) fn is_cdf_read_supported(&self) -> bool {
        static CDF_SUPPORTED_READER_FEATURES: LazyLock<Vec<TableFeature>> =
            LazyLock::new(|| vec![TableFeature::DeletionVectors]);
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
    #[internal_api]
    #[allow(unused)] // needed to compile w/o default features
    pub(crate) fn is_deletion_vector_supported(&self) -> bool {
        self.protocol()
            .has_table_feature(&TableFeature::DeletionVectors)
            && self.protocol.min_reader_version() == 3
            && self.protocol.min_writer_version() == 7
    }

    /// Returns `true` if writing deletion vectors is enabled for this table. This is the case
    /// when the deletion vectors is supported on this table and the `delta.enableDeletionVectors`
    /// table property is set to `true`.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors>
    #[internal_api]
    #[allow(unused)] // needed to compile w/o default features
    pub(crate) fn is_deletion_vector_enabled(&self) -> bool {
        self.is_deletion_vector_supported()
            && self
                .table_properties
                .enable_deletion_vectors
                .unwrap_or(false)
    }

    /// Returns `true` if the table supports the appendOnly table feature. To support this feature:
    /// - The table must have a writer version between 2 and 7 (inclusive)
    /// - If the table is on writer version 7, it must have the [`TableFeature::AppendOnly`]
    ///   writer feature.
    pub(crate) fn is_append_only_supported(&self) -> bool {
        let protocol = &self.protocol;
        match protocol.min_writer_version() {
            7 if protocol.has_table_feature(&TableFeature::AppendOnly) => true,
            version => (2..=6).contains(&version),
        }
    }

    #[allow(unused)]
    pub(crate) fn is_append_only_enabled(&self) -> bool {
        self.is_append_only_supported() && self.table_properties.append_only.unwrap_or(false)
    }

    /// Returns `true` if the table supports the column invariant table feature.
    pub(crate) fn is_invariants_supported(&self) -> bool {
        let protocol = &self.protocol;
        match protocol.min_writer_version() {
            7 if protocol.has_table_feature(&TableFeature::Invariants) => true,
            version => (2..=6).contains(&version),
        }
    }

    /// Returns `true` if V2 checkpoint is supported on this table. To support V2 checkpoint,
    /// a table must support reader version 3, writer version 7, and the v2Checkpoint feature in
    /// both the protocol's readerFeatures and writerFeatures.
    ///
    /// See: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-checkpoint-table-feature>
    pub(crate) fn is_v2_checkpoint_write_supported(&self) -> bool {
        self.protocol()
            .has_table_feature(&TableFeature::V2Checkpoint)
    }

    /// Returns `true` if the table supports writing in-commit timestamps.
    ///
    /// To support this feature the table must:
    /// - Have a min_writer_version of 7
    /// - Have the [`TableFeature::InCommitTimestamp`] writer feature.
    #[allow(unused)]
    pub(crate) fn is_in_commit_timestamps_supported(&self) -> bool {
        self.protocol().min_writer_version() == 7
            && self
                .protocol()
                .has_table_feature(&TableFeature::InCommitTimestamp)
    }

    /// Returns `true` if in-commit timestamps is supported and it is enabled. In-commit timestamps
    /// is enabled when the `delta.enableInCommitTimestamps` configuration is set to `true`.
    #[allow(unused)]
    pub(crate) fn is_in_commit_timestamps_enabled(&self) -> bool {
        self.is_in_commit_timestamps_supported()
            && self
                .table_properties()
                .enable_in_commit_timestamps
                .unwrap_or(false)
    }

    /// Returns information about in-commit timestamp enablement state.
    ///
    /// Returns an error if only one of the enablement properties is present, as this indicates
    /// an inconsistent state.
    #[allow(unused)]
    pub(crate) fn in_commit_timestamp_enablement(
        &self,
    ) -> DeltaResult<InCommitTimestampEnablement> {
        if !self.is_in_commit_timestamps_enabled() {
            return Ok(InCommitTimestampEnablement::NotEnabled);
        }

        let enablement_version = self
            .table_properties()
            .in_commit_timestamp_enablement_version;
        let enablement_timestamp = self
            .table_properties()
            .in_commit_timestamp_enablement_timestamp;

        match (enablement_version, enablement_timestamp) {
            (Some(version), Some(timestamp)) => Ok(InCommitTimestampEnablement::Enabled {
                enablement: Some((version, timestamp)),
            }),
            (Some(_), None) => Err(Error::generic(
                "In-commit timestamp enabled, but enablement timestamp is missing",
            )),
            (None, Some(_)) => Err(Error::generic(
                "In-commit timestamp enabled, but enablement version is missing",
            )),
            // If InCommitTimestamps was enabled at the beginning of the table's history,
            // it may have an empty enablement version and timestamp
            (None, None) => Ok(InCommitTimestampEnablement::Enabled { enablement: None }),
        }
    }

    /// Returns `true` if the table supports writing domain metadata.
    ///
    /// To support this feature the table must:
    /// - Have a min_writer_version of 7.
    /// - Have the [`TableFeature::DomainMetadata`] writer feature.
    #[allow(unused)]
    pub(crate) fn is_domain_metadata_supported(&self) -> bool {
        self.protocol().min_writer_version() == 7
            && self
                .protocol()
                .has_table_feature(&TableFeature::DomainMetadata)
    }

    /// Returns `true` if the table supports writing row tracking metadata.
    ///
    /// To support this feature the table must:
    /// - Have a min_writer_version of 7.
    /// - Have the [`TableFeature::RowTracking`] writer feature.
    pub(crate) fn is_row_tracking_supported(&self) -> bool {
        self.protocol().min_writer_version() == 7
            && self
                .protocol()
                .has_table_feature(&TableFeature::RowTracking)
    }

    /// Returns `true` if row tracking is enabled for this table.
    ///
    /// In order to enable row tracking the table must:
    /// - Support row tracking (see [`Self::is_row_tracking_supported`]).
    /// - Have the `delta.enableRowTracking` table property set to `true`.
    pub(crate) fn is_row_tracking_enabled(&self) -> bool {
        self.is_row_tracking_supported()
            && self.table_properties().enable_row_tracking.unwrap_or(false)
    }

    /// Returns `true` if row tracking is suspended for this table.
    ///
    /// Row tracking is suspended when the `delta.rowTrackingSuspended` table property is set to `true`.
    /// Note that:
    /// - Row tracking can be _supported_ and _suspended_ at the same time.
    /// - Row tracking cannot be _enabled_ while _suspended_.
    pub(crate) fn is_row_tracking_suspended(&self) -> bool {
        self.table_properties()
            .row_tracking_suspended
            .unwrap_or(false)
    }

    /// Returns `true` if row tracking information should be written for this table.
    ///
    /// Row tracking information should be written when:
    /// - Row tracking is supported
    /// - Row tracking is not suspended
    ///
    /// Note: We ignore [`is_row_tracking_enabled`] at this point because Kernel does not
    /// preserve row IDs and row commit versions yet.
    pub(crate) fn should_write_row_tracking(&self) -> bool {
        self.is_row_tracking_supported() && !self.is_row_tracking_suspended()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use url::Url;

    use crate::actions::{Metadata, Protocol};
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::table_properties::TableProperties;
    use crate::utils::test_utils::assert_result_error_with_message;
    use crate::Error;

    use super::{InCommitTimestampEnablement, TableConfiguration};

    #[test]
    fn dv_supported_not_enabled() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors]),
            Some([TableFeature::DeletionVectors]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_deletion_vector_supported());
        assert!(!table_config.is_deletion_vector_enabled());
    }
    #[test]
    fn dv_enabled() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([
                ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                (
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                ),
            ]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors]),
            Some([TableFeature::DeletionVectors]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_deletion_vector_supported());
        assert!(table_config.is_deletion_vector_enabled());
    }
    #[test]
    fn ict_enabled_from_table_creation() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0, // Table creation version
            HashMap::from_iter([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            )]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some([TableFeature::InCommitTimestamp]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_in_commit_timestamps_supported());
        assert!(table_config.is_in_commit_timestamps_enabled());
        // When ICT is enabled from table creation (version 0), it's perfectly normal
        // for enablement properties to be missing
        let info = table_config.in_commit_timestamp_enablement().unwrap();
        assert_eq!(
            info,
            InCommitTimestampEnablement::Enabled { enablement: None }
        );
    }
    #[test]
    fn ict_supported_and_enabled() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([
                (
                    "delta.enableInCommitTimestamps".to_string(),
                    "true".to_string(),
                ),
                (
                    "delta.inCommitTimestampEnablementVersion".to_string(),
                    "5".to_string(),
                ),
                (
                    "delta.inCommitTimestampEnablementTimestamp".to_string(),
                    "100".to_string(),
                ),
            ]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some([TableFeature::InCommitTimestamp]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_in_commit_timestamps_supported());
        assert!(table_config.is_in_commit_timestamps_enabled());
        let info = table_config.in_commit_timestamp_enablement().unwrap();
        assert_eq!(
            info,
            InCommitTimestampEnablement::Enabled {
                enablement: Some((5, 100))
            }
        )
    }
    #[test]
    fn ict_enabled_with_partial_enablement_info() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([
                (
                    "delta.enableInCommitTimestamps".to_string(),
                    "true".to_string(),
                ),
                (
                    "delta.inCommitTimestampEnablementVersion".to_string(),
                    "5".to_string(),
                ),
                // Missing enablement timestamp
            ]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some([TableFeature::InCommitTimestamp]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_in_commit_timestamps_supported());
        assert!(table_config.is_in_commit_timestamps_enabled());
        assert!(matches!(
            table_config.in_commit_timestamp_enablement(),
            Err(Error::Generic(msg)) if msg.contains("In-commit timestamp enabled, but enablement timestamp is missing")
        ));
    }
    #[test]
    fn ict_supported_and_not_enabled() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some([TableFeature::InCommitTimestamp]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(table_config.is_in_commit_timestamps_supported());
        assert!(!table_config.is_in_commit_timestamps_enabled());
        let info = table_config.in_commit_timestamp_enablement().unwrap();
        assert_eq!(info, InCommitTimestampEnablement::NotEnabled);
    }
    #[test]
    fn fails_on_unsupported_feature() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let protocol = Protocol::try_new(3, 7, Some(["unknown"]), Some(["unknown"])).unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        TableConfiguration::try_new(metadata, protocol, table_root, 0)
            .expect_err("Unknown feature is not supported in kernel");
    }
    #[test]
    fn dv_not_supported() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::TimestampWithoutTimezone]),
            Some([TableFeature::TimestampWithoutTimezone]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(!table_config.is_deletion_vector_supported());
        assert!(!table_config.is_deletion_vector_enabled());
    }

    #[test]
    fn test_try_new_from() {
        let schema = StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
        )
        .unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors]),
            Some([TableFeature::DeletionVectors]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();

        let new_schema =
            StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)]);
        let new_metadata = Metadata::try_new(
            None,
            None,
            new_schema,
            vec![],
            0,
            HashMap::from_iter([
                (
                    "delta.enableChangeDataFeed".to_string(),
                    "false".to_string(),
                ),
                (
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                ),
            ]),
        )
        .unwrap();
        let new_protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors, TableFeature::V2Checkpoint]),
            Some([
                TableFeature::DeletionVectors,
                TableFeature::V2Checkpoint,
                TableFeature::AppendOnly,
            ]),
        )
        .unwrap();
        let new_version = 1;
        let new_table_config = TableConfiguration::try_new_from(
            &table_config,
            Some(new_metadata.clone()),
            Some(new_protocol.clone()),
            new_version,
        )
        .unwrap();

        assert_eq!(new_table_config.version(), new_version);
        assert_eq!(new_table_config.metadata(), &new_metadata);
        assert_eq!(new_table_config.protocol(), &new_protocol);
        assert_eq!(new_table_config.schema(), table_config.schema());
        assert_eq!(
            new_table_config.table_properties(),
            &TableProperties {
                enable_change_data_feed: Some(false),
                enable_deletion_vectors: Some(true),
                ..Default::default()
            }
        );
        assert_eq!(
            new_table_config.column_mapping_mode(),
            table_config.column_mapping_mode()
        );
        assert_eq!(new_table_config.table_root(), table_config.table_root());
    }

    #[test]
    fn test_timestamp_ntz_validation_integration() {
        // Schema with TIMESTAMP_NTZ column
        let schema =
            StructType::new_unchecked([StructField::nullable("ts", DataType::TIMESTAMP_NTZ)]);
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();

        let protocol_without_timestamp_ntz_features = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some::<Vec<String>>(vec![]),
        )
        .unwrap();

        let protocol_with_timestamp_ntz_features = Protocol::try_new(
            3,
            7,
            Some([TableFeature::TimestampWithoutTimezone]),
            Some([TableFeature::TimestampWithoutTimezone]),
        )
        .unwrap();

        let table_root = Url::try_from("file:///").unwrap();

        let result = TableConfiguration::try_new(
            metadata.clone(),
            protocol_without_timestamp_ntz_features,
            table_root.clone(),
            0,
        );
        assert_result_error_with_message(result, "Unsupported: Table contains TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature in reader and writer features");

        let result = TableConfiguration::try_new(
            metadata,
            protocol_with_timestamp_ntz_features,
            table_root,
            0,
        );
        assert!(
            result.is_ok(),
            "Should succeed when TIMESTAMP_NTZ is used with required features"
        );
    }

    #[test]
    fn test_variant_validation_integration() {
        // Schema with VARIANT column
        let schema =
            StructType::new_unchecked([StructField::nullable("v", DataType::unshredded_variant())]);
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();

        let protocol_without_variant_features = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some::<Vec<String>>(vec![]),
        )
        .unwrap();

        let protocol_with_variant_features = Protocol::try_new(
            3,
            7,
            Some([TableFeature::VariantType]),
            Some([TableFeature::VariantType]),
        )
        .unwrap();

        let table_root = Url::try_from("file:///").unwrap();

        let result: Result<TableConfiguration, Error> = TableConfiguration::try_new(
            metadata.clone(),
            protocol_without_variant_features,
            table_root.clone(),
            0,
        );
        assert_result_error_with_message(result, "Unsupported: Table contains VARIANT columns but does not have the required 'variantType' feature in reader and writer features");

        let result =
            TableConfiguration::try_new(metadata, protocol_with_variant_features, table_root, 0);
        assert!(
            result.is_ok(),
            "Should succeed when VARIANT is used with required features"
        );
    }
}
