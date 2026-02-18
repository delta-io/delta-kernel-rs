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
use std::sync::Arc;

use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::expressions::ColumnName;
use crate::scan::data_skipping::stats_schema::{
    expected_stats_schema, stats_column_names, PhysicalStatsSchemaTransform,
};
use crate::schema::variant_utils::validate_variant_type_feature_support;
use crate::schema::{InvariantChecker, SchemaRef, SchemaTransform, StructType};
use crate::table_features::{
    column_mapping_mode, get_any_level_column_physical_name, validate_schema_column_mapping,
    validate_timestamp_ntz_feature_support, ColumnMappingMode, EnablementCheck, FeatureInfo,
    FeatureRequirement, FeatureType, KernelSupport, Operation, TableFeature,
    LEGACY_READER_FEATURES, LEGACY_WRITER_FEATURES,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Error, Version};
use delta_kernel_derive::internal_api;
use tracing::warn;

/// Expected schemas for file statistics.
///
/// Contains both logical and physical versions of the stats schema:
/// - **Logical schema**: Uses original column names (matching table schema)
/// - **Physical schema**: Uses physical column names (for column mapping)
///
/// When column mapping is disabled (`ColumnMappingMode::None`), both schemas are identical.
#[allow(unused)]
#[derive(Debug, Clone)]
pub(crate) struct ExpectedStatsSchemas {
    /// Stats schema using logical (user-facing) column names.
    pub logical: SchemaRef,
    /// Stats schema using physical column names (for storage).
    pub physical: SchemaRef,
}

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
/// support can be checked with [`TableConfiguration::is_feature_supported`] and deletion
/// vector write enablement can be checked with [`TableConfiguration::is_feature_enabled`].
///
/// [`TableConfiguration`] performs checks upon construction with `TableConfiguration::try_new`
/// to validate that Metadata and Protocol are correctly formatted and mutually compatible.
/// After construction, call `ensure_operation_supported` to verify that the kernel supports the
/// required operations for the table's protocol features.
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
        let schema = Arc::new(metadata.parse_schema()?);
        let table_properties = metadata.parse_table_properties();
        let column_mapping_mode = column_mapping_mode(&protocol, &table_properties);

        // validate column mapping mode -- all schema fields should be correctly (un)annotated
        validate_schema_column_mapping(&schema, column_mapping_mode)?;

        validate_timestamp_ntz_feature_support(&schema, &protocol)?;

        validate_variant_type_feature_support(&schema, &protocol)?;

        let table_config = Self {
            schema,
            metadata,
            protocol,
            table_properties,
            column_mapping_mode,
            table_root,
            version,
        };

        Ok(table_config)
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

    /// Creates a new [`TableConfiguration`] representing the table configuration immediately
    /// after a commit.
    ///
    /// This method takes a pre-commit table configuration and produces a post-commit
    /// configuration at the committed version. This allows immediate use of the new table
    /// configuration without re-reading metadata from storage.
    ///
    /// TODO: Take in Protocol (when Kernel-RS supports protocol changes)
    /// TODO: Take in Metadata (when Kernel-RS supports metadata changes)
    pub(crate) fn new_post_commit(table_configuration: &Self, new_version: Version) -> Self {
        Self {
            version: new_version,
            ..table_configuration.clone()
        }
    }

    /// Generates the expected schemas for file statistics (both logical and physical).
    ///
    /// Engines can provide statistics for files written to the delta table, enabling
    /// data skipping and other optimizations. This method generates the expected schemas
    /// for structured statistics based on the table configuration.
    ///
    /// Returns a tuple of `(logical_stats_schema, physical_stats_schema)`:
    /// - **Logical schema**: Uses original column names (matching table schema)
    /// - **Physical schema**: Uses physical column names (respecting column mapping mode)
    ///
    /// Both schemas are structured as:
    /// ```text
    /// {
    ///   numRecords: long,
    ///   nullCount: { <columns with LONG type> },
    ///   minValues: { <columns with original types> },
    ///   maxValues: { <columns with original types> },
    /// }
    /// ```
    ///
    /// The schemas are affected by:
    /// - **Column mapping mode**: Physical schema field names use physical names from column
    ///   mapping metadata.
    /// - **`delta.dataSkippingStatsColumns`**: If set, only specified columns are included.
    /// - **`delta.dataSkippingNumIndexedCols`**: Otherwise, includes the first N leaf columns
    ///   (default 32).
    /// - **Clustering columns**: Per the Delta protocol, clustering columns are always included
    ///   in statistics, regardless of the above settings.
    ///
    /// See the Delta protocol for more details on per-file statistics:
    /// <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics>
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn build_expected_stats_schemas(
        &self,
        clustering_columns: Option<&[ColumnName]>,
    ) -> DeltaResult<ExpectedStatsSchemas> {
        let logical_data_schema = self.logical_data_schema();
        let logical_stats_schema = Arc::new(expected_stats_schema(
            &logical_data_schema,
            self.table_properties(),
            clustering_columns,
        )?);
        let physical_stats_schema = match self.column_mapping_mode() {
            ColumnMappingMode::None => logical_stats_schema.clone(),
            _ => PhysicalStatsSchemaTransform {
                column_mapping_mode: self.column_mapping_mode(),
            }
            .transform_struct(&logical_stats_schema)
            .map(|s| Arc::new(s.into_owned()))
            .unwrap_or_else(|| logical_stats_schema.clone()),
        };
        Ok(ExpectedStatsSchemas {
            logical: logical_stats_schema,
            physical: physical_stats_schema,
        })
    }

    /// Returns the list of logical column names that should have statistics collected.
    ///
    /// Returns leaf column paths as [`ColumnName`] objects, which store path components
    /// separately and handle escaping of special characters (dots, spaces) via backticks.
    ///
    /// Per the Delta protocol, clustering columns are always included in statistics,
    /// regardless of the `delta.dataSkippingStatsColumns` or `delta.dataSkippingNumIndexedCols`
    /// settings.
    pub(crate) fn stats_column_names_logical(
        &self,
        clustering_columns: Option<&[ColumnName]>,
    ) -> Vec<ColumnName> {
        stats_column_names(
            &self.logical_data_schema(),
            self.table_properties(),
            clustering_columns,
        )
    }

    /// Returns the list of physical column names that should have statistics collected.
    ///
    /// Similar to [`stats_column_names_logical`], but returns physical column names.
    pub(crate) fn stats_column_names_physical(
        &self,
        clustering_columns: Option<&[ColumnName]>,
    ) -> Vec<ColumnName> {
        let logical_names = self.stats_column_names_logical(clustering_columns);
        let column_mapping_mode = self.column_mapping_mode();
        logical_names
            .into_iter()
            .filter_map(|col_name| {
                get_any_level_column_physical_name(
                    self.schema.as_ref(),
                    &col_name,
                    column_mapping_mode,
                )
                .inspect_err(|e| {
                    warn!("Couldn't find physical name for {col_name}: {e}");
                })
                .ok()
            })
            .collect()
    }

    /// Returns the logical schema for data columns (excludes partition columns).
    ///
    /// Partition columns are excluded because statistics are only collected for data columns
    /// that are physically stored in the parquet files. Partition values are stored in the
    /// file path, not in the file content, so they don't have file-level statistics.
    fn logical_data_schema(&self) -> SchemaRef {
        let partition_columns = self.metadata().partition_columns();
        Arc::new(StructType::new_unchecked(
            self.schema
                .fields()
                .filter(|field| !partition_columns.contains(field.name()))
                .cloned(),
        ))
    }

    /// The [`Metadata`] for this table at this version.
    #[internal_api]
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// The [`Protocol`] of this table at this version.
    #[allow(unused)]
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

    /// Validates that all feature requirements for a given feature are satisfied.
    fn validate_feature_requirements(
        &self,
        feature_name: &str,
        requirements: &[FeatureRequirement],
    ) -> DeltaResult<()> {
        for req in requirements {
            match req {
                FeatureRequirement::Supported(dep) => {
                    require!(
                        self.is_feature_supported(dep),
                        Error::invalid_protocol(format!(
                            "{} requires {} to be supported",
                            feature_name, dep
                        ))
                    );
                }
                FeatureRequirement::Enabled(dep) => {
                    require!(
                        self.is_feature_enabled(dep),
                        Error::invalid_protocol(format!(
                            "{} requires {} to be enabled",
                            feature_name, dep
                        ))
                    );
                }
                FeatureRequirement::NotSupported(dep) => {
                    require!(
                        !self.is_feature_supported(dep),
                        Error::invalid_protocol(format!(
                            "{} requires {} to not be supported",
                            feature_name, dep
                        ))
                    );
                }
                FeatureRequirement::NotEnabled(dep) => {
                    require!(
                        !self.is_feature_enabled(dep),
                        Error::invalid_protocol(format!(
                            "{} requires {} to not be enabled",
                            feature_name, dep
                        ))
                    );
                }
                FeatureRequirement::Custom(check) => {
                    check(&self.protocol, &self.table_properties)?;
                }
            }
        }
        Ok(())
    }

    /// Checks that kernel supports a feature for the given operation.
    /// Returns an error if the feature is unknown, not supported, or fails validation.
    fn check_feature_support(
        &self,
        feature: &TableFeature,
        operation: Operation,
    ) -> DeltaResult<()> {
        let Some(info) = feature.info() else {
            return Err(Error::unsupported(format!("Unknown feature '{}'", feature)));
        };

        match &info.kernel_support {
            KernelSupport::Supported => {}
            KernelSupport::NotSupported => {
                return Err(Error::unsupported(format!(
                    "Feature '{}' is not supported",
                    info.name
                )))
            }
            KernelSupport::Custom(check) => {
                check(&self.protocol, &self.table_properties, operation)?;
            }
        };

        self.validate_feature_requirements(info.name, info.feature_requirements)
    }

    /// Returns all reader features enabled for this table based on protocol version.
    /// For table features protocol (v3), returns the explicit reader_features list.
    /// For legacy protocol (v1-2), infers features from the version number.
    fn get_enabled_reader_features(&self) -> Vec<TableFeature> {
        match self.protocol.min_reader_version() {
            3 => {
                // Table features reader: use explicit reader_features list
                self.protocol
                    .reader_features()
                    .map(|f| f.to_vec())
                    .unwrap_or_default()
            }
            v if (1..=2).contains(&v) => {
                // Legacy reader: infer features from version
                LEGACY_READER_FEATURES
                    .iter()
                    .filter(|f| {
                        f.info()
                            .map(|info| v >= info.min_reader_version)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    /// Returns all writer features enabled for this table based on protocol version.
    /// For table features protocol (v7), returns the explicit writer_features list.
    /// For legacy protocol (v1-6), infers features from the version number.
    fn get_enabled_writer_features(&self) -> Vec<TableFeature> {
        match self.protocol.min_writer_version() {
            7 => {
                // Table features writer: use explicit writer_features list
                self.protocol
                    .writer_features()
                    .map(|f| f.to_vec())
                    .unwrap_or_default()
            }
            v if (1..=6).contains(&v) => {
                // Legacy writer: infer features from version
                LEGACY_WRITER_FEATURES
                    .iter()
                    .filter(|f| {
                        f.info()
                            .map(|info| v >= info.min_writer_version)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    /// Returns `Ok` if the kernel supports the given operation on this table. This checks that
    /// the protocol's features are all supported for the requested operation type.
    ///
    /// - For `Scan` and `Cdf` operations: checks reader version and reader features
    /// - For `Write` operations: checks writer version and writer features
    #[internal_api]
    pub(crate) fn ensure_operation_supported(&self, operation: Operation) -> DeltaResult<()> {
        match operation {
            Operation::Scan | Operation::Cdf => self.ensure_read_supported(operation),
            Operation::Write => self.ensure_write_supported(),
        }
    }

    /// Internal helper for read operations (Scan, Cdf)
    fn ensure_read_supported(&self, operation: Operation) -> DeltaResult<()> {
        // Version check: kernel supports reader versions 1-3
        if self.protocol.min_reader_version() > 3 {
            return Err(Error::unsupported(format!(
                "Unsupported minimum reader version {}",
                self.protocol.min_reader_version()
            )));
        }

        // Check all enabled reader features have kernel support
        for feature in self.get_enabled_reader_features() {
            self.check_feature_support(&feature, operation)?;
        }

        Ok(())
    }

    /// Internal helper for write operations
    fn ensure_write_supported(&self) -> DeltaResult<()> {
        // Version check: kernel supports writer versions 1-7
        if self.protocol.min_writer_version() > 7 {
            return Err(Error::unsupported(format!(
                "Unsupported minimum writer version {}",
                self.protocol.min_writer_version()
            )));
        }

        // Check all enabled writer features have kernel support
        for feature in self.get_enabled_writer_features() {
            self.check_feature_support(&feature, Operation::Write)?;
        }

        // Schema-dependent validation for Invariants (can't be in FeatureInfo)
        // TODO: Better story for schema validation for Invariants and other features
        if self.is_feature_supported(&TableFeature::Invariants)
            && InvariantChecker::has_invariants(self.schema.as_ref())
        {
            return Err(Error::unsupported(
                "Column invariants are not yet supported",
            ));
        }

        Ok(())
    }

    /// Returns information about in-commit timestamp enablement state.
    ///
    /// Returns an error if only one of the enablement properties is present, as this indicates
    /// an inconsistent state.
    #[allow(unused)]
    pub(crate) fn in_commit_timestamp_enablement(
        &self,
    ) -> DeltaResult<InCommitTimestampEnablement> {
        if !self.is_feature_enabled(&TableFeature::InCommitTimestamp) {
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
        self.is_feature_supported(&TableFeature::RowTracking) && !self.is_row_tracking_suspended()
    }

    /// Returns true if the protocol uses legacy reader version (< 3)
    #[allow(dead_code)]
    fn is_legacy_reader_version(&self) -> bool {
        self.protocol.min_reader_version() < 3
    }

    /// Returns true if the protocol uses legacy writer version (< 7)
    #[allow(dead_code)]
    fn is_legacy_writer_version(&self) -> bool {
        self.protocol.min_writer_version() < 7
    }

    /// Helper to check if a feature is present in a feature list.
    fn has_feature(features: Option<&[TableFeature]>, feature: &TableFeature) -> bool {
        features
            .map(|features| features.contains(feature))
            .unwrap_or(false)
    }

    /// Helper method to check if a feature is supported based on its FeatureInfo.
    /// This checks protocol versions and feature lists but does NOT check enablement properties.
    #[allow(dead_code)]
    fn is_feature_info_supported(&self, feature: &TableFeature, info: &FeatureInfo) -> bool {
        match info.feature_type {
            FeatureType::Writer => {
                if self.is_legacy_writer_version() {
                    // Legacy writer: protocol writer version meets minimum requirement
                    self.protocol.min_writer_version() >= info.min_writer_version
                } else {
                    // Table features writer: feature is in writer_features list
                    Self::has_feature(self.protocol.writer_features(), feature)
                }
            }
            FeatureType::ReaderWriter => {
                let reader_supported = if self.is_legacy_reader_version() {
                    // Legacy reader: protocol reader version meets minimum requirement
                    self.protocol.min_reader_version() >= info.min_reader_version
                } else {
                    // Table features reader: feature is in reader_features list
                    Self::has_feature(self.protocol.reader_features(), feature)
                };

                let writer_supported = if self.is_legacy_writer_version() {
                    // Legacy writer: protocol writer version meets minimum requirement
                    self.protocol.min_writer_version() >= info.min_writer_version
                } else {
                    // Table features writer: feature is in writer_features list
                    Self::has_feature(self.protocol.writer_features(), feature)
                };

                reader_supported && writer_supported
            }
            FeatureType::Unknown => false,
        }
    }

    /// Helper method to check if a feature is enabled based on its FeatureInfo.
    /// This checks both protocol support and enablement via table properties.
    #[allow(dead_code)]
    fn is_feature_info_enabled(&self, feature: &TableFeature, info: &FeatureInfo) -> bool {
        if !self.is_feature_info_supported(feature, info) {
            return false;
        }

        match info.enablement_check {
            EnablementCheck::AlwaysIfSupported => true,
            EnablementCheck::EnabledIf(check_fn) => check_fn(&self.table_properties),
        }
    }

    /// Generic method to check if a feature is supported in the protocol.
    /// This does NOT check if the feature is enabled via table properties.
    #[internal_api]
    pub(crate) fn is_feature_supported(&self, feature: &TableFeature) -> bool {
        let Some(info) = feature.info() else {
            return false;
        };
        self.is_feature_info_supported(feature, info)
    }

    /// Generic method to check if a feature is enabled.
    ///
    /// A feature is enabled if:
    /// 1. It is supported in the protocol
    /// 2. The enablement check passes
    #[internal_api]
    pub(crate) fn is_feature_enabled(&self, feature: &TableFeature) -> bool {
        let Some(info) = feature.info() else {
            return false;
        };
        self.is_feature_info_enabled(feature, info)
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;
    use std::sync::Arc;

    use url::Url;

    use crate::actions::{Metadata, Protocol};
    use crate::schema::ColumnName;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::table_features::ColumnMappingMode;
    use crate::table_features::{
        EnablementCheck, FeatureInfo, FeatureType, KernelSupport, Operation, TableFeature,
    };
    use crate::table_properties::TableProperties;
    use crate::utils::test_utils::{
        assert_result_error_with_message, test_schema_flat, test_schema_flat_with_column_mapping,
        test_schema_nested, test_schema_nested_with_column_mapping, test_schema_with_array,
        test_schema_with_array_and_column_mapping, test_schema_with_map,
        test_schema_with_map_and_column_mapping,
    };
    use crate::Error;
    use rstest::rstest;

    use super::{InCommitTimestampEnablement, TableConfiguration};

    fn create_mock_table_config(
        props_to_enable: &[(&str, &str)],
        features: &[TableFeature],
    ) -> TableConfiguration {
        create_mock_table_config_with_version(props_to_enable, Some(features), 3, 7)
    }

    fn create_mock_table_config_with_version(
        props_to_enable: &[(&str, &str)],
        features_opt: Option<&[TableFeature]>,
        min_reader_version: i32,
        min_writer_version: i32,
    ) -> TableConfiguration {
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter(
                props_to_enable
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string())),
            ),
        )
        .unwrap();

        let (reader_features_opt, writer_features_opt) = if let Some(features) = features_opt {
            let reader_features = features
                .iter()
                .filter(|feature| matches!(feature.feature_type(), FeatureType::ReaderWriter))
                .cloned()
                .collect::<Vec<_>>();
            let writer_features = features
                .iter()
                .filter(|feature| {
                    matches!(
                        feature.feature_type(),
                        FeatureType::Writer | FeatureType::ReaderWriter
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            (
                // Only add reader_features if reader >= 3 (non-legacy reader mode)
                // Protocol requires Some (even if empty) when reader = 3
                if min_reader_version >= 3 {
                    Some(reader_features)
                } else {
                    None
                },
                // Only add writer_features if writer >= 7 (non-legacy writer mode)
                // Protocol requires Some (even if empty) when writer = 7
                if min_writer_version >= 7 {
                    Some(writer_features)
                } else {
                    None
                },
            )
        } else {
            (None, None)
        };

        let reader_features_iter = reader_features_opt.as_ref().map(|f| f.iter());
        let writer_features_iter = writer_features_opt.as_ref().map(|f| f.iter());

        let protocol = Protocol::try_new(
            min_reader_version,
            min_writer_version,
            reader_features_iter,
            writer_features_iter,
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap()
    }

    #[test]
    fn dv_supported_not_enabled() {
        use crate::table_properties::ENABLE_CHANGE_DATA_FEED;

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([(ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string())]),
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
        assert!(table_config.is_feature_supported(&TableFeature::DeletionVectors));
        assert!(!table_config.is_feature_enabled(&TableFeature::DeletionVectors));
    }

    #[test]
    fn dv_enabled() {
        use crate::table_properties::{ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS};

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([
                (ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string()),
                (ENABLE_DELETION_VECTORS.to_string(), "true".to_string()),
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
        assert!(table_config.is_feature_supported(&TableFeature::DeletionVectors));
        assert!(table_config.is_feature_enabled(&TableFeature::DeletionVectors));
    }

    #[test]
    fn write_with_cdf() {
        use crate::table_properties::{APPEND_ONLY, ENABLE_CHANGE_DATA_FEED};
        use TableFeature::*;
        let cases = [
            (
                // Writing to CDF-enabled table is supported for writes
                create_mock_table_config(&[(ENABLE_CHANGE_DATA_FEED, "true")], &[ChangeDataFeed]),
                Ok(()),
            ),
            (
                // Should succeed even if AppendOnly is supported but not enabled
                create_mock_table_config(
                    &[(ENABLE_CHANGE_DATA_FEED, "true")],
                    &[ChangeDataFeed, AppendOnly],
                ),
                Ok(()),
            ),
            (
                // Should succeed since AppendOnly is enabled
                create_mock_table_config(
                    &[(ENABLE_CHANGE_DATA_FEED, "true"), (APPEND_ONLY, "true")],
                    &[ChangeDataFeed, AppendOnly],
                ),
                Ok(()),
            ),
            (
                // Writer version > 7 is not supported
                create_mock_table_config_with_version(
                    &[(ENABLE_CHANGE_DATA_FEED, "true")],
                    None,
                    1,
                    8,
                ),
                Err(Error::unsupported("Unsupported minimum writer version 8")),
            ),
            // Column mapping is now supported for writes.
            (
                // CDF + column mapping: both supported, should succeed
                create_mock_table_config(
                    &[(ENABLE_CHANGE_DATA_FEED, "true"), (APPEND_ONLY, "true")],
                    &[ChangeDataFeed, ColumnMapping, AppendOnly],
                ),
                Ok(()),
            ),
            (
                // Column mapping + AppendOnly, no CDF enabled: should succeed
                create_mock_table_config(
                    &[(APPEND_ONLY, "true")],
                    &[ChangeDataFeed, ColumnMapping, AppendOnly],
                ),
                Ok(()),
            ),
            (
                // Should succeed since change data feed is not enabled
                create_mock_table_config(&[(APPEND_ONLY, "true")], &[AppendOnly]),
                Ok(()),
            ),
        ];

        for (table_configuration, result) in cases {
            match (
                table_configuration.ensure_operation_supported(Operation::Write),
                result,
            ) {
                (Ok(()), Ok(())) => { /* Correct result */ }
                (actual_result, Err(expected)) => {
                    assert_result_error_with_message(actual_result, &expected.to_string());
                }
                (Err(actual_result), Ok(())) => {
                    panic!("Expected Ok but got error: {actual_result}");
                }
            }
        }
    }
    #[test]
    fn ict_enabled_from_table_creation() {
        use crate::table_properties::ENABLE_IN_COMMIT_TIMESTAMPS;

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0, // Table creation version
            HashMap::from_iter([(ENABLE_IN_COMMIT_TIMESTAMPS.to_string(), "true".to_string())]),
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
        assert!(table_config.is_feature_supported(&TableFeature::InCommitTimestamp));
        assert!(table_config.is_feature_enabled(&TableFeature::InCommitTimestamp));
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
        use crate::table_properties::{
            ENABLE_IN_COMMIT_TIMESTAMPS, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
            IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        };

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([
                (ENABLE_IN_COMMIT_TIMESTAMPS.to_string(), "true".to_string()),
                (
                    IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.to_string(),
                    "5".to_string(),
                ),
                (
                    IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.to_string(),
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
        assert!(table_config.is_feature_supported(&TableFeature::InCommitTimestamp));
        assert!(table_config.is_feature_enabled(&TableFeature::InCommitTimestamp));
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
        use crate::table_properties::{
            ENABLE_IN_COMMIT_TIMESTAMPS, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        };

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([
                (ENABLE_IN_COMMIT_TIMESTAMPS.to_string(), "true".to_string()),
                (
                    IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.to_string(),
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
        assert!(table_config.is_feature_supported(&TableFeature::InCommitTimestamp));
        assert!(table_config.is_feature_enabled(&TableFeature::InCommitTimestamp));
        assert!(matches!(
            table_config.in_commit_timestamp_enablement(),
            Err(Error::Generic(msg)) if msg.contains("In-commit timestamp enabled, but enablement timestamp is missing")
        ));
    }
    #[test]
    fn ict_supported_and_not_enabled() {
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
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
        assert!(table_config.is_feature_supported(&TableFeature::InCommitTimestamp));
        assert!(!table_config.is_feature_enabled(&TableFeature::InCommitTimestamp));
        let info = table_config.in_commit_timestamp_enablement().unwrap();
        assert_eq!(info, InCommitTimestampEnablement::NotEnabled);
    }
    #[test]
    fn fails_on_unsupported_feature() {
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let protocol = Protocol::try_new(3, 7, Some(["unknown"]), Some(["unknown"])).unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        table_config
            .ensure_operation_supported(Operation::Scan)
            .expect_err("Unknown feature is not supported in kernel");
    }
    #[test]
    fn dv_not_supported() {
        use crate::table_properties::ENABLE_CHANGE_DATA_FEED;

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([(ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string())]),
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
        assert!(!table_config.is_feature_supported(&TableFeature::DeletionVectors));
        assert!(!table_config.is_feature_enabled(&TableFeature::DeletionVectors));
    }

    #[test]
    fn test_try_new_from() {
        use crate::table_properties::{ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS};

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from_iter([(ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string())]),
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

        let new_schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let new_metadata = Metadata::try_new(
            None,
            None,
            new_schema,
            vec![],
            0,
            HashMap::from_iter([
                (ENABLE_CHANGE_DATA_FEED.to_string(), "false".to_string()),
                (ENABLE_DELETION_VECTORS.to_string(), "true".to_string()),
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
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "ts",
            DataType::TIMESTAMP_NTZ,
        )]));
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
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "v",
            DataType::unshredded_variant(),
        )]));
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

    #[test]
    fn test_is_feature_supported_returns_false_without_info() {
        // is_feature_supported should return false for features without FeatureInfo
        let config = create_mock_table_config(&[], &[TableFeature::DeletionVectors]);
        assert!(!config.is_feature_supported(&TableFeature::unknown("futureFeature")));
    }

    #[test]
    fn test_is_feature_enabled_returns_false_without_info() {
        // is_feature_enabled should return false for features without FeatureInfo
        let config = create_mock_table_config(&[], &[TableFeature::DeletionVectors]);
        assert!(!config.is_feature_enabled(&TableFeature::unknown("futureFeature")));
    }

    #[test]
    fn test_is_feature_info_supported_writer() {
        // Use ColumnMapping (a ReaderWriter feature) with custom FeatureInfo as Writer type
        let feature = TableFeature::ColumnMapping;

        // Custom FeatureInfo that treats ColumnMapping as Writer-only with min_writer_version = 2
        let custom_feature_info = FeatureInfo {
            name: "columnMapping",
            min_reader_version: 1,
            min_writer_version: 2,
            feature_type: FeatureType::Writer,
            feature_requirements: &[],
            kernel_support: KernelSupport::Supported,
            enablement_check: EnablementCheck::AlwaysIfSupported,
        };

        // Test with legacy protocol writer v2 - should be supported
        let config = create_mock_table_config_with_version(&[], None, 1, 2);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with legacy protocol writer v1 - should NOT be supported
        let config = create_mock_table_config_with_version(&[], None, 1, 1);
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with asymmetric: reader=2 (legacy), writer=7 (non-legacy)
        // For this to work with a Writer-only FeatureInfo, we need a real Writer-only feature
        // Use AppendOnly instead of ColumnMapping for the 2,7 test cases
        let writer_only_feature = TableFeature::AppendOnly;
        let writer_only_info = FeatureInfo {
            name: "appendOnly",
            min_reader_version: 1,
            min_writer_version: 2,
            feature_type: FeatureType::Writer,
            feature_requirements: &[],
            kernel_support: KernelSupport::Supported,
            enablement_check: EnablementCheck::AlwaysIfSupported,
        };

        // reader=2 (legacy), writer=7 (non-legacy) - feature in list, should be supported
        let config =
            create_mock_table_config_with_version(&[], Some(&[TableFeature::AppendOnly]), 2, 7);
        assert!(config.is_feature_info_supported(&writer_only_feature, &writer_only_info));

        // reader=2 (legacy), writer=7 (non-legacy) - feature NOT in list, should NOT be supported
        // Use ChangeDataFeed which is also a Writer-only feature
        let config =
            create_mock_table_config_with_version(&[], Some(&[TableFeature::ChangeDataFeed]), 2, 7);
        assert!(!config.is_feature_info_supported(&writer_only_feature, &writer_only_info));

        // Test with protocol reader=3, writer=7 (both non-legacy) - feature in list, should be supported
        let config = create_mock_table_config(&[], &[TableFeature::ColumnMapping]);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with protocol reader=3, writer=7 (both non-legacy) - feature NOT in list, should NOT be supported
        let config = create_mock_table_config(&[], &[TableFeature::DeletionVectors]);
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));
    }

    #[test]
    fn test_is_feature_info_supported_reader_writer() {
        // Use ColumnMapping (a real ReaderWriter feature) with custom FeatureInfo
        // ColumnMapping is a legacy feature (reader=2, writer=5) which makes it ideal for
        // testing both legacy mode (version checks) and non-legacy mode (feature list checks)
        let feature = TableFeature::ColumnMapping;

        // Custom FeatureInfo that requires reader=2, writer=5
        let custom_feature_info = FeatureInfo {
            name: "columnMapping",
            min_reader_version: 2,
            min_writer_version: 5,
            feature_type: FeatureType::ReaderWriter,
            feature_requirements: &[],
            kernel_support: KernelSupport::Supported,
            enablement_check: EnablementCheck::AlwaysIfSupported,
        };

        // Test with sufficient versions (legacy mode) - should be supported
        let config = create_mock_table_config_with_version(&[], None, 2, 5);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with insufficient reader version - should NOT be supported
        let config = create_mock_table_config_with_version(&[], None, 1, 5);
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with insufficient writer version - should NOT be supported
        let config = create_mock_table_config_with_version(&[], None, 2, 4);
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with asymmetric: reader=2 (legacy), writer=7 (non-legacy)
        // ReaderWriter features CANNOT be enabled in this protocol state (protocol validation)
        // But we still need to test that the code correctly identifies them as NOT supported
        // Create a table with only Writer-only features (e.g., AppendOnly)
        let config =
            create_mock_table_config_with_version(&[], Some(&[TableFeature::AppendOnly]), 2, 7);
        // ColumnMapping (ReaderWriter) should NOT be supported because:
        // - reader=2 (legacy) checks version: 2 >= 2 ✓ (reader_supported = true)
        // - writer=7 (non-legacy) checks list: ColumnMapping not in writer_features ✗ (writer_supported = false)
        // - Result: false (requires BOTH to be true)
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with non-legacy mode (3,7) - feature in list, should be supported
        let config = create_mock_table_config(&[], &[TableFeature::ColumnMapping]);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));

        // Test with non-legacy mode (3,7) - feature NOT in list, should NOT be supported
        let config = create_mock_table_config(&[], &[TableFeature::DeletionVectors]);
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));
    }

    #[test]
    fn test_is_feature_info_enabled_with_custom_property_check() {
        use crate::table_properties::APPEND_ONLY;

        // Create a custom feature with a property check function
        let custom_feature_info = FeatureInfo {
            name: "customPropertyFeature",
            min_reader_version: 1,
            min_writer_version: 2,
            feature_type: FeatureType::Writer,
            feature_requirements: &[],
            kernel_support: KernelSupport::Supported,
            enablement_check: EnablementCheck::EnabledIf(|props| props.append_only == Some(true)),
        };

        let feature = TableFeature::unknown("customPropertyFeature");

        // Test when property check fails - should be supported but not enabled
        let config = create_mock_table_config_with_version(&[], None, 1, 2);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));
        assert!(!config.is_feature_info_enabled(&feature, &custom_feature_info));

        // Test when property check passes - should be both supported and enabled
        let config = create_mock_table_config_with_version(&[(APPEND_ONLY, "true")], None, 1, 2);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));
        assert!(config.is_feature_info_enabled(&feature, &custom_feature_info));
    }

    #[test]
    fn test_is_feature_info_enabled_always_if_supported() {
        // Create a custom feature that's always enabled if supported
        let custom_feature_info = FeatureInfo {
            name: "alwaysEnabledFeature",
            min_reader_version: 1,
            min_writer_version: 3,
            feature_type: FeatureType::Writer,
            feature_requirements: &[],
            kernel_support: KernelSupport::Supported,
            enablement_check: EnablementCheck::AlwaysIfSupported,
        };

        let feature = TableFeature::unknown("alwaysEnabledFeature");

        // Test when supported - should be both supported and enabled
        let config = create_mock_table_config_with_version(&[], None, 1, 3);
        assert!(config.is_feature_info_supported(&feature, &custom_feature_info));
        assert!(config.is_feature_info_enabled(&feature, &custom_feature_info));

        // Test when not supported - should be neither supported nor enabled
        let config = create_mock_table_config_with_version(&[], None, 1, 2);
        assert!(!config.is_feature_info_supported(&feature, &custom_feature_info));
        assert!(!config.is_feature_info_enabled(&feature, &custom_feature_info));
    }

    #[test]
    fn test_ensure_operation_supported_reads() {
        let config = create_mock_table_config(&[], &[]);
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        let config = create_mock_table_config(&[], &[TableFeature::V2Checkpoint]);
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        let config = create_mock_table_config_with_version(&[], None, 1, 2);
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        let config = create_mock_table_config_with_version(
            &[],
            Some(&[TableFeature::InCommitTimestamp]),
            2,
            7,
        );
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());
    }

    #[test]
    fn test_ensure_operation_supported_writes() {
        let config = create_mock_table_config(
            &[],
            &[
                TableFeature::AppendOnly,
                TableFeature::DeletionVectors,
                TableFeature::DomainMetadata,
                TableFeature::Invariants,
                TableFeature::RowTracking,
            ],
        );
        assert!(config.ensure_operation_supported(Operation::Write).is_ok());

        // Type Widening is not supported for writes
        let config = create_mock_table_config(&[], &[TableFeature::TypeWidening]);
        assert_result_error_with_message(
            config.ensure_operation_supported(Operation::Write),
            r#"Feature 'typeWidening' is not supported for writes"#,
        );
    }

    #[test]
    fn test_illegal_writer_feature_combination() {
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some(vec![TableFeature::RowTracking]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert_result_error_with_message(
            config.ensure_operation_supported(Operation::Write),
            "rowTracking requires domainMetadata to be supported",
        );
    }

    #[test]
    fn test_row_tracking_with_domain_metadata_requirement() {
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some(vec![
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ]),
        )
        .unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
        assert!(
            config.ensure_operation_supported(Operation::Write).is_ok(),
            "RowTracking with DomainMetadata should be supported for writes"
        );
    }

    #[cfg(feature = "catalog-managed")]
    #[test]
    fn test_catalog_managed_writes() {
        let config = create_mock_table_config(&[], &[TableFeature::CatalogManaged]);
        assert!(config.ensure_operation_supported(Operation::Write).is_ok());

        let config = create_mock_table_config(&[], &[TableFeature::CatalogOwnedPreview]);
        assert!(config.ensure_operation_supported(Operation::Write).is_ok());
    }

    /// Helper to create a schema with column mapping metadata using JSON deserialization
    fn schema_with_column_mapping() -> SchemaRef {
        let field_a: StructField = serde_json::from_str(
            r#"{
                "name": "col_a",
                "type": "long",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": "phys_col_a"
                }
            }"#,
        )
        .unwrap();

        let field_b: StructField = serde_json::from_str(
            r#"{
                "name": "col_b",
                "type": "string",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 2,
                    "delta.columnMapping.physicalName": "phys_col_b"
                }
            }"#,
        )
        .unwrap();

        Arc::new(StructType::new_unchecked([field_a, field_b]))
    }

    fn create_table_config_with_column_mapping(
        schema: SchemaRef,
        column_mapping_mode: &str,
    ) -> TableConfiguration {
        use crate::table_properties::COLUMN_MAPPING_MODE;

        let mut props = HashMap::new();
        props.insert(
            COLUMN_MAPPING_MODE.to_string(),
            column_mapping_mode.to_string(),
        );

        let metadata = Metadata::try_new(None, None, schema, vec![], 0, props).unwrap();

        // Use reader version 2 which supports column mapping
        let protocol = Protocol::try_new(2, 5, None::<Vec<String>>, None::<Vec<String>>).unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap()
    }

    #[test]
    fn test_build_expected_stats_schemas_no_column_mapping() {
        // Without column mapping, logical and physical schemas should be identical
        let schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("col_a", DataType::LONG),
            StructField::nullable("col_b", DataType::STRING),
        ]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let protocol = Protocol::try_new(1, 2, None::<Vec<String>>, None::<Vec<String>>).unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        let config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();

        assert_eq!(config.column_mapping_mode(), ColumnMappingMode::None);

        let stats_schemas = config.build_expected_stats_schemas(None).unwrap();

        // Both schemas should be identical (same Arc)
        assert!(Arc::ptr_eq(&stats_schemas.logical, &stats_schemas.physical));

        // Verify field names are logical names
        let min_values = stats_schemas
            .logical
            .field("minValues")
            .unwrap()
            .data_type();
        if let DataType::Struct(inner) = min_values {
            assert!(inner.field("col_a").is_some());
            assert!(inner.field("col_b").is_some());
        } else {
            panic!("Expected minValues to be a struct");
        }
    }

    #[test]
    fn test_build_expected_stats_schemas_with_column_mapping() {
        // With column mapping, logical schema should have logical names,
        // physical schema should have physical names
        let schema = schema_with_column_mapping();
        let config = create_table_config_with_column_mapping(schema, "name");

        assert_eq!(config.column_mapping_mode(), ColumnMappingMode::Name);

        let stats_schemas = config.build_expected_stats_schemas(None).unwrap();

        // Schemas should be different (not the same Arc)
        assert!(!Arc::ptr_eq(
            &stats_schemas.logical,
            &stats_schemas.physical
        ));

        // Verify logical schema has logical names
        let logical_min_values = stats_schemas
            .logical
            .field("minValues")
            .unwrap()
            .data_type();
        if let DataType::Struct(inner) = logical_min_values {
            assert!(
                inner.field("col_a").is_some(),
                "Logical schema should have col_a"
            );
            assert!(
                inner.field("col_b").is_some(),
                "Logical schema should have col_b"
            );
            assert!(inner.field("phys_col_a").is_none());
        } else {
            panic!("Expected minValues to be a struct");
        }

        // Verify physical schema has physical names
        let physical_min_values = stats_schemas
            .physical
            .field("minValues")
            .unwrap()
            .data_type();
        if let DataType::Struct(inner) = physical_min_values {
            assert!(
                inner.field("phys_col_a").is_some(),
                "Physical schema should have phys_col_a"
            );
            assert!(
                inner.field("phys_col_b").is_some(),
                "Physical schema should have phys_col_b"
            );
            assert!(inner.field("col_a").is_none());
        } else {
            panic!("Expected minValues to be a struct");
        }
    }

    #[test]
    fn test_build_expected_stats_schemas_id_mode_has_no_parquet_field_ids() {
        // With column mapping mode `id`, make_physical() injects ParquetFieldId metadata for
        // data file reading. But the physical stats schema must NOT contain these field IDs
        // because stats are read from JSON commit files or checkpoint Parquet files, neither of
        // which use parquet field IDs.
        use crate::schema::{ColumnMetadataKey, MetadataValue};

        let schema = schema_with_column_mapping();
        let config = create_table_config_with_column_mapping(schema, "id");

        assert_eq!(config.column_mapping_mode(), ColumnMappingMode::Id);

        let stats_schemas = config.build_expected_stats_schemas(None).unwrap();

        // Verify physical schema has physical names
        let physical_min_values = stats_schemas
            .physical
            .field("minValues")
            .unwrap()
            .data_type();
        let DataType::Struct(inner) = physical_min_values else {
            panic!("Expected minValues to be a struct");
        };
        assert!(
            inner.field("phys_col_a").is_some(),
            "Physical schema should have phys_col_a"
        );
        assert!(
            inner.field("phys_col_b").is_some(),
            "Physical schema should have phys_col_b"
        );
        assert!(inner.field("col_a").is_none());

        // Verify no field has ParquetFieldId metadata
        for field in inner.fields() {
            assert!(
                field
                    .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                    .is_none(),
                "Physical stats schema field '{}' should not have ParquetFieldId metadata",
                field.name()
            );
        }

        // Verify that make_physical on the same schema DOES produce ParquetFieldId (sanity check)
        let data_schema = schema_with_column_mapping();
        let physical_data = data_schema.make_physical(ColumnMappingMode::Id);
        let data_field = physical_data.field("phys_col_a").unwrap();
        assert!(
            matches!(
                data_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
                Some(MetadataValue::Number(_))
            ),
            "make_physical should inject ParquetFieldId for data schemas in Id mode"
        );
    }

    #[test]
    fn test_stats_column_names_logical_returns_logical_names() {
        // stats_column_names_logical should return logical column names
        let schema = schema_with_column_mapping();
        let config = create_table_config_with_column_mapping(schema, "name");

        let column_names = config.stats_column_names_logical(None /* clustering_columns */);

        // Should return logical names, not physical names
        assert!(column_names.contains(&ColumnName::new(["col_a"])));
        assert!(column_names.contains(&ColumnName::new(["col_b"])));
        assert!(!column_names.contains(&ColumnName::new(["phys_col_a"])));
        assert!(!column_names.contains(&ColumnName::new(["phys_col_b"])));
    }

    #[test]
    fn test_stats_column_names_physical_returns_physical_names() {
        // stats_column_names_physical should return physical column names
        let schema = schema_with_column_mapping();
        let config = create_table_config_with_column_mapping(schema, "name");

        let column_names = config.stats_column_names_physical(None /* clustering_columns */);

        // Should return physical names, not logical names
        assert_eq!(
            column_names,
            vec![
                ColumnName::new(["phys_col_a"]),
                ColumnName::new(["phys_col_b"]),
            ],
            "Expected physical column names, not logical names"
        );
    }

    #[rstest]
    // --- flat schema ---
    #[case::flat_none(
        test_schema_flat(),
        "none",
        vec![ColumnName::new(["id"]), ColumnName::new(["name"])],
    )]
    #[case::flat_name(
        test_schema_flat_with_column_mapping(),
        "name",
        vec![ColumnName::new(["phys_id"]), ColumnName::new(["phys_name"])],
    )]
    #[case::flat_id(
        test_schema_flat_with_column_mapping(),
        "id",
        vec![ColumnName::new(["phys_id"]), ColumnName::new(["phys_name"])],
    )]
    // --- nested schema ---
    #[case::nested_none(
        test_schema_nested(),
        "none",
        vec![
            ColumnName::new(["id"]),
            ColumnName::new(["info", "name"]),
            ColumnName::new(["info", "age"]),
        ],
    )]
    #[case::nested_name(
        test_schema_nested_with_column_mapping(),
        "name",
        vec![
            ColumnName::new(["phys_id"]),
            ColumnName::new(["phys_info", "phys_name"]),
            ColumnName::new(["phys_info", "phys_age"]),
        ],
    )]
    #[case::nested_id(
        test_schema_nested_with_column_mapping(),
        "id",
        vec![
            ColumnName::new(["phys_id"]),
            ColumnName::new(["phys_info", "phys_name"]),
            ColumnName::new(["phys_info", "phys_age"]),
        ],
    )]
    // --- schema with map (map fields excluded from stats) ---
    #[case::map_none(
        test_schema_with_map(),
        "none",
        vec![ColumnName::new(["id"]), ColumnName::new(["name"])],
    )]
    #[case::map_name(
        test_schema_with_map_and_column_mapping(),
        "name",
        vec![ColumnName::new(["phys_id"]), ColumnName::new(["phys_name"])],
    )]
    #[case::map_id(
        test_schema_with_map_and_column_mapping(),
        "id",
        vec![ColumnName::new(["phys_id"]), ColumnName::new(["phys_name"])],
    )]
    // --- schema with array (array fields excluded from stats) ---
    #[case::array_none(
        test_schema_with_array(),
        "none",
        vec![ColumnName::new(["id"]), ColumnName::new(["name"])],
    )]
    #[case::array_name(
        test_schema_with_array_and_column_mapping(),
        "name",
        vec![ColumnName::new(["phys_id"]), ColumnName::new(["phys_name"])],
    )]
    #[case::array_id(
        test_schema_with_array_and_column_mapping(),
        "id",
        vec![ColumnName::new(["phys_id"]), ColumnName::new(["phys_name"])],
    )]
    fn test_stats_column_names_physical_all_schemas(
        #[case] schema: SchemaRef,
        #[case] mode: &str,
        #[case] expected_physical: Vec<ColumnName>,
    ) {
        let config = create_table_config_with_column_mapping(schema, mode);
        let physical_names = config.stats_column_names_physical(None);
        assert_eq!(
            physical_names, expected_physical,
            "Incorrect physical column names for mode '{mode}'"
        );
    }

    #[cfg(feature = "clustered-table")]
    #[test]
    fn test_clustered_table_writes() {
        // ClusteredTable requires DomainMetadata to be supported
        let config = create_mock_table_config(
            &[],
            &[TableFeature::ClusteredTable, TableFeature::DomainMetadata],
        );
        assert!(
            config.ensure_operation_supported(Operation::Write).is_ok(),
            "ClusteredTable with DomainMetadata should be supported for writes"
        );
    }

    #[cfg(not(feature = "clustered-table"))]
    #[test]
    fn test_clustered_table_writes_not_supported() {
        // Without the clustered-table feature, writes to clustered tables should fail
        let config = create_mock_table_config(
            &[],
            &[TableFeature::ClusteredTable, TableFeature::DomainMetadata],
        );
        assert!(
            config.ensure_operation_supported(Operation::Write).is_err(),
            "ClusteredTable should not be supported for writes without feature flag"
        );
    }
}
