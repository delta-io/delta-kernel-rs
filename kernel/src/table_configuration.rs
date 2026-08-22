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
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use tracing::warn;
use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::expressions::ColumnName;
use crate::scan::data_skipping::stats_schema::{
    expected_stats_schema, stats_column_names, StatsConfig, StripFieldMetadataTransform,
};
pub(crate) use crate::schema::variant_utils::validate_variant_type_feature_support;
use crate::schema::void_utils::strip_void_from_schema;
use crate::schema::{
    schema_has_invariants, validate_column_defaults_metadata, SchemaRef, StructField, StructType,
};
#[cfg(feature = "geo-type-in-dev")]
use crate::table_features::validate_geospatial_feature_support;
use crate::table_features::{
    check_reader_version_range, column_mapping_mode, extract_enabled_reader_features,
    extract_enabled_writer_features, get_any_level_column_physical_name,
    validate_iceberg_compat_if_needed, validate_timestamp_ntz_feature_support, ColumnMappingMode,
    EnablementCheck, FeatureRequirement, FeatureType, KernelSupport, Operation, TableFeature,
    MAX_VALID_WRITER_VERSION, MIN_VALID_RW_VERSION, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION, V3_VALIDATOR,
};
use crate::table_properties::TableProperties;
use crate::transforms::SchemaTransform as _;
use crate::utils::require;
use crate::{DeltaResult, Error, Version};

/// Expected schema for file statistics, using physical column names.
///
/// Wrapped in a struct so it can be extended with a logical-name variant if needed.
#[allow(unused)]
#[derive(Debug, Clone)]
#[internal_api]
pub(crate) struct ExpectedStatsSchemas {
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

/// Utility function to strip field metadata from stats schemas. This metadata describes logical
/// table columns, not the stats. Keeping it can cause schema mismatches when combining the parsed
/// stats from a checkpoint written before logical metadata was added.
fn strip_metadata(schema: SchemaRef) -> SchemaRef {
    match StripFieldMetadataTransform.transform_struct(&schema) {
        Cow::Owned(s) => Arc::new(s),
        _ => schema,
    }
}

fn validate_partition_columns(metadata: &Metadata, logical_schema: &StructType) -> DeltaResult<()> {
    let mut seen = HashSet::new();
    for col in metadata.partition_columns() {
        if !seen.insert(col) {
            return Err(Error::generic(format!(
                "Duplicate partition column: '{col}'"
            )));
        }
        require!(
            logical_schema.field(col).is_some(),
            Error::generic(format!("Partition column '{col}' not found in schema"))
        );
    }
    Ok(())
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
    /// Logical schema: field names are the user-facing (logical) column names.
    logical_schema: SchemaRef,
    /// Whether any field in the logical schema declares a column default.
    has_column_with_default: bool,
    /// The subset of the logical schema that remains after excluding partition columns.
    logical_schema_without_partition_columns: SchemaRef,
    /// Physical schema for all columns (field names respect column mapping mode).
    physical_schema: SchemaRef,
    /// The subset of the physical schema that remains after excluding partition columns.
    physical_data_schema_without_partition_columns: SchemaRef,
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
    ///     - Non-legacy features must appear in both reader features and writer features lists. If
    ///       such a feature is present, the reader version and writer version must be 3, and 5
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
        let logical_schema = Arc::new(metadata.parse_schema()?);
        Self::try_new_inner(metadata, protocol, table_root, version, logical_schema)
    }

    /// Like [`try_new`](Self::try_new), but reuses `base`'s protocol, table root, and version
    /// and takes a pre-parsed `logical_schema`.
    pub(crate) fn try_new_with_schema(
        base: &Self,
        metadata: Metadata,
        logical_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        Self::try_new_inner(
            metadata,
            base.protocol.clone(),
            base.table_root.clone(),
            base.version,
            logical_schema,
        )
    }

    fn try_new_inner(
        metadata: Metadata,
        protocol: Protocol,
        table_root: Url,
        version: Version,
        logical_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let table_properties = metadata.parse_table_properties();
        let column_mapping_mode = column_mapping_mode(&protocol, &table_properties);

        let physical_schema = Arc::new(logical_schema.make_physical(column_mapping_mode)?);
        let partition_columns: HashSet<&str> = metadata
            .partition_columns()
            .iter()
            .map(|s| s.as_str())
            .collect();
        let physical_data_schema_without_partition_columns = {
            let fields = logical_schema
                .fields()
                .zip(physical_schema.fields())
                .filter(|(logical_field, _)| {
                    !partition_columns.contains(logical_field.name().as_str())
                })
                .map(|(_, physical_field)| physical_field.clone());
            // Safety: subset of an already-valid schema.
            Arc::new(StructType::new_unchecked(fields))
        };
        let logical_schema_without_partition_columns = {
            let fields = logical_schema
                .fields()
                .filter(|field| !partition_columns.contains(field.name().as_str()))
                .cloned();
            // Safety: subset of an already-valid schema.
            Arc::new(StructType::new_unchecked(fields))
        };

        let mut table_config = Self {
            logical_schema,
            has_column_with_default: false,
            logical_schema_without_partition_columns,
            physical_schema,
            physical_data_schema_without_partition_columns,
            metadata,
            protocol,
            table_properties,
            column_mapping_mode,
            table_root,
            version,
        };

        validate_partition_columns(&table_config.metadata, &table_config.logical_schema)?;

        // Validate schema against protocol features now that we have a TC instance.
        validate_timestamp_ntz_feature_support(&table_config)?;
        validate_variant_type_feature_support(&table_config)?;
        // Reject corrupt column-default metadata (a non-string `CURRENT_DEFAULT`, or a non-`NULL`
        // default on a Variant column) and retain whether the validated schema declares any column
        // defaults.
        table_config.has_column_with_default =
            validate_column_defaults_metadata(&table_config.logical_schema)?;
        // Reject tables with geo-typed columns that don't declare the `geospatial` feature.
        #[cfg(feature = "geo-type-in-dev")]
        validate_geospatial_feature_support(&table_config)?;
        validate_iceberg_compat_if_needed(&table_config, &V3_VALIDATOR)?;

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

        // Note that while we could pick apart the protocol/metadata updates and validate them
        // individually, instead we re-parse so that we can recycle the try_new validation.
        Self::try_new(
            new_metadata.unwrap_or_else(|| table_configuration.metadata.clone()),
            new_protocol.unwrap_or_else(|| table_configuration.protocol.clone()),
            table_configuration.table_root.clone(),
            new_version,
        )
    }

    /// Validate that the kernel supports adding `feature` to this table configuration.
    pub(crate) fn validate_feature_for_addition(&self, feature: &TableFeature) -> DeltaResult<()> {
        require!(
            !matches!(
                feature,
                TableFeature::CatalogManaged | TableFeature::CatalogOwnedPreview
            ),
            Error::unsupported("Upgrading an existing table to catalog-managed is not supported")
        );

        // Adding protocol support would immediately activate ICT, but this generic path cannot
        // emit the required enablement metadata or first in-commit timestamp.
        require!(
            feature != &TableFeature::InCommitTimestamp
                || self.table_properties().enable_in_commit_timestamps != Some(true),
            Error::unsupported(
                "Adding 'inCommitTimestamp' to an existing table with \
                 'delta.enableInCommitTimestamps=true' is not supported"
            )
        );

        match feature.feature_type() {
            FeatureType::WriterOnly => self.check_feature_support(feature, Operation::Write),
            FeatureType::ReaderWriter => {
                self.check_feature_support(feature, Operation::Scan)?;
                self.check_feature_support(feature, Operation::Write)
            }
            FeatureType::Unknown => Err(Error::unsupported(format!(
                "Unknown feature '{feature}' cannot be added"
            ))),
        }
    }

    /// Creates a new [`TableConfiguration`] representing the table configuration immediately
    /// after a commit.
    ///
    /// This method takes the current table configuration and produces a post-commit
    /// configuration at the committed version. If the commit included new Protocol or Metadata
    /// actions (e.g. ALTER TABLE), those are passed in and the configuration is rebuilt with
    /// full validation. Otherwise the existing configuration is cloned with only the version
    /// updated.
    ///
    /// Returns the new [`TableConfiguration`] at `new_version`.
    ///
    /// # Errors
    ///
    /// Returns an error if the new metadata/protocol combination fails
    /// [`TableConfiguration::try_new`] validation (e.g., unsupported features, invalid schema).
    pub(crate) fn new_post_commit(
        table_configuration: &Self,
        new_version: Version,
        new_metadata: Option<Metadata>,
        new_protocol: Option<Protocol>,
    ) -> DeltaResult<Self> {
        Self::try_new_from(table_configuration, new_metadata, new_protocol, new_version)
    }

    /// Generates the expected schema for file statistics.
    ///
    /// Engines can provide statistics for files written to the delta table, enabling
    /// data skipping and other optimizations. Returns the physical stats schema wrapped in
    /// an `ExpectedStatsSchemas`.
    ///
    /// The schema is structured as:
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
    /// - **Required columns** (e.g. clustering columns): Per the Delta protocol, always included in
    ///   statistics, regardless of the above settings.
    /// - **Requested columns**: Optional output filter that limits which columns appear in the
    ///   schema without affecting column counting.
    ///
    /// See the Delta protocol for more details on per-file statistics:
    /// <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics>
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn build_expected_stats_schemas(
        &self,
        required_physical_columns: Option<&[ColumnName]>,
        requested_physical_columns: Option<&[ColumnName]>,
    ) -> DeltaResult<ExpectedStatsSchemas> {
        let physical_data_schema = self.physical_data_schema_without_partition_columns();
        let required_physical_stats_columns = self.required_physical_stats_columns();
        let config = StatsConfig {
            data_skipping_stats_columns: required_physical_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: self.table_properties().data_skipping_num_indexed_cols,
        };
        let physical_stats_schema = Arc::new(expected_stats_schema(
            &physical_data_schema,
            &config,
            required_physical_columns,
            requested_physical_columns,
        )?);
        let physical_stats_schema = strip_metadata(physical_stats_schema);

        Ok(ExpectedStatsSchemas {
            physical: physical_stats_schema,
        })
    }

    /// Returns the list of physical column names that should have statistics collected.
    ///
    /// Partition columns are excluded first (their values are already in the Add action's
    /// `partitionValues` field). Among the remaining columns, if `required_columns` is `Some`,
    /// those columns are always included regardless of `dataSkippingNumIndexedCols` or
    /// `dataSkippingStatsColumns` settings (e.g. clustering columns).
    pub(crate) fn physical_stats_column_names(
        &self,
        required_columns: Option<&[ColumnName]>,
    ) -> Vec<ColumnName> {
        let physical_stats_columns = self.required_physical_stats_columns();
        let config = StatsConfig {
            data_skipping_stats_columns: physical_stats_columns.as_deref(),
            data_skipping_num_indexed_cols: self.table_properties().data_skipping_num_indexed_cols,
        };
        stats_column_names(
            &self.physical_data_schema_without_partition_columns(),
            &config,
            required_columns,
        )
    }

    /// Stats-column set for `DataSkippingFilter`'s predicate-rewrite gate. The gate tests
    /// every column reference in the rewritten predicate against this set; every data-skipping
    /// call site shares this entry point so their gate input stays in lockstep.
    pub(crate) fn physical_stats_columns_set(
        &self,
        required_columns: Option<&[ColumnName]>,
    ) -> HashSet<ColumnName> {
        self.physical_stats_column_names(required_columns)
            .into_iter()
            .collect()
    }

    /// Returns the physical partition schema for `partitionValues_parsed`.
    ///
    /// Field names are physical column names (respecting column mapping mode),
    /// and field types are the actual partition column data types with their original nullability.
    /// Returns `None` if the table has no partition columns.
    pub(crate) fn build_partition_values_parsed_schema(&self) -> Option<SchemaRef> {
        if self.logical_partition_columns().is_empty() {
            return None;
        }
        let partition_fields: Vec<StructField> = self
            .physical_partition_fields()
            .map(|(field, physical_name)| {
                StructField::new(
                    physical_name.to_owned(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
        Some(Arc::new(StructType::new_unchecked(partition_fields)))
    }

    /// Typed physical partition schema for `DataSkippingFilter`, narrowed to the partition
    /// columns referenced by `predicate_refs` (physical leaf names) with every field forced
    /// nullable.
    ///
    /// Returns `None` when the table has no partition columns or the predicate references none;
    /// the filter then treats partitions as unavailable and folds partition predicates to
    /// keep-all. Every retained field is nullable because a `MapToStruct` over `partitionValues`
    /// yields null for a missing key or the protocol's empty-string-is-null rule, which a
    /// non-nullable field would reject.
    pub(crate) fn predicate_partition_schema(
        &self,
        predicate_refs: &[ColumnName],
    ) -> Option<SchemaRef> {
        let referenced: HashSet<&str> = predicate_refs
            .iter()
            .filter_map(|c| match c.path() {
                [name] => Some(name.as_str()),
                _ => None,
            })
            .collect();
        let nullable_fields: Vec<StructField> = self
            .build_partition_values_parsed_schema()?
            .fields()
            .filter(|f| referenced.contains(f.name().as_str()))
            .map(|f| StructField::nullable(f.name(), f.data_type().clone()))
            .collect();
        (!nullable_fields.is_empty()).then(|| Arc::new(StructType::new_unchecked(nullable_fields)))
    }

    /// Returns the logical schema excluding partition columns.
    pub(crate) fn logical_schema_without_partition_columns(&self) -> SchemaRef {
        self.logical_schema_without_partition_columns.clone()
    }

    /// Returns the physical data schema excluding partition columns.
    pub(crate) fn physical_data_schema_without_partition_columns(&self) -> SchemaRef {
        self.physical_data_schema_without_partition_columns.clone()
    }

    /// Translates `delta.dataSkippingStatsColumns` entries to physical column names.
    ///
    /// Returns `None` if the table property is not set. Entries that cannot be resolved
    /// (e.g. non-existent columns) are silently skipped with a warning.
    fn required_physical_stats_columns(&self) -> Option<Vec<ColumnName>> {
        self.table_properties()
            .data_skipping_stats_columns
            .as_ref()
            .map(|cols| {
                let logical_schema = self.logical_schema_without_partition_columns();
                let mode = self.column_mapping_mode();
                cols.iter()
                    .filter_map(|col| {
                        get_any_level_column_physical_name(&logical_schema, col, mode)
                            // Theoretically this should always resolve — if it doesn't,
                            // the user specified a non-existent column in
                            // delta.dataSkippingStatsColumns, which is safe to ignore.
                            .inspect_err(|e| {
                                warn!(
                                    "Couldn't translate dataSkippingStatsColumns entry '{col}' \
                                     to physical name: {e}; skipping"
                                );
                            })
                            .ok()
                    })
                    .collect()
            })
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
    pub(crate) fn logical_schema(&self) -> SchemaRef {
        self.logical_schema.clone()
    }

    /// Borrows this table's logical schema, tied to `&self` (no `Arc` clone).
    ///
    /// Use this over [`logical_schema`](Self::logical_schema) when callers need to derive
    /// `&self`-bound borrows from the schema (e.g. `&DataType` of a field).
    pub(crate) fn logical_schema_ref(&self) -> &SchemaRef {
        &self.logical_schema
    }

    /// Whether any field in the logical schema declares a column default.
    ///
    /// This includes nested fields and is independent of the `allowColumnDefaults` feature.
    pub(crate) fn has_column_with_default(&self) -> bool {
        self.has_column_with_default
    }

    /// The physical schema ([`SchemaRef`]) of this table at this version.
    ///
    /// When column mapping is disabled, this is identical to
    /// [`logical_schema`](Self::logical_schema). Otherwise, field names are replaced with
    /// physical column names derived from column mapping metadata.
    #[internal_api]
    pub(crate) fn physical_schema(&self) -> SchemaRef {
        self.physical_schema.clone()
    }

    /// Whether partition column values must be materialized into data files.
    /// Returns true when either:
    ///   * The [`MaterializePartitionColumns`] writer feature is enabled, or
    ///   * [`IcebergCompatV3`] is enabled
    ///
    /// [`MaterializePartitionColumns`]: crate::table_features::TableFeature::MaterializePartitionColumns
    /// [`IcebergCompatV3`]: crate::table_features::TableFeature::IcebergCompatV3
    pub(crate) fn should_materialize_partition_columns(&self) -> bool {
        // TODO(#1125): add IcebergcompatV1/V2 here when they are supported.
        self.is_feature_enabled(&TableFeature::MaterializePartitionColumns)
            || self.is_feature_enabled(&TableFeature::IcebergCompatV3)
    }

    /// The physical schema for writing data files.
    ///
    /// When [`should_materialize_partition_columns`] is true, returns the full physical schema
    /// (partition columns are materialized in data files). Otherwise, returns the physical
    /// schema with partition columns excluded. Void columns are always stripped from the
    /// returned schema, since they are never written to Parquet.
    ///
    /// [`should_materialize_partition_columns`]: Self::should_materialize_partition_columns
    pub(crate) fn physical_write_schema(&self) -> SchemaRef {
        let with_partition_cols = if self.should_materialize_partition_columns() {
            self.physical_schema()
        } else {
            self.physical_data_schema_without_partition_columns()
        };
        strip_void_from_schema(with_partition_cols)
    }

    /// The [`TableProperties`] of this table at this version.
    #[internal_api]
    pub(crate) fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }

    /// Whether this table is catalog-managed (has the CatalogManaged or CatalogOwnedPreview
    /// table feature).
    #[internal_api]
    pub(crate) fn is_catalog_managed(&self) -> bool {
        self.is_feature_supported(&TableFeature::CatalogManaged)
            || self.is_feature_supported(&TableFeature::CatalogOwnedPreview)
    }

    /// The [`ColumnMappingMode`] for this table at this version.
    #[internal_api]
    pub(crate) fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }

    /// The logical partition columns of this table (empty if unpartitioned).
    #[internal_api]
    pub(crate) fn logical_partition_columns(&self) -> &[String] {
        self.metadata().partition_columns()
    }

    /// The physical partition columns of this table (empty if unpartitioned).
    pub(crate) fn physical_partition_columns(&self) -> impl Iterator<Item = String> + '_ {
        self.physical_partition_fields()
            .map(|(_, physical_name)| physical_name.to_owned())
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

    // TODO(#3020): Unify scan-state schema construction and write-context serialization to call
    // this.
    fn physical_partition_fields(&self) -> impl Iterator<Item = (&StructField, &str)> + '_ {
        let column_mapping_mode = self.column_mapping_mode();
        self.logical_partition_columns()
            .iter()
            .filter_map(move |name| {
                // SAFETY: Construction already validates that every partition column exists in
                // the schema. Keep this iterator infallible for a simpler return type, with a
                // defensive warning if the invariant is violated.
                let field = self.logical_schema.field(name);
                if field.is_none() {
                    warn!("Partition column '{name}' not found in table schema");
                }
                field.map(|field| (field, field.physical_name(column_mapping_mode)))
            })
    }

    /// Validates that all feature requirements for a given feature are satisfied.
    fn validate_feature_requirements(&self, feature: &TableFeature) -> DeltaResult<()> {
        for req in feature.info().feature_requirements {
            match req {
                FeatureRequirement::Supported(dep) => {
                    require!(
                        self.is_feature_supported(dep),
                        Error::invalid_protocol(format!(
                            "Feature '{feature}' requires '{dep}' to be supported"
                        ))
                    );
                }
                FeatureRequirement::Enabled(dep) => {
                    require!(
                        self.is_feature_enabled(dep),
                        Error::invalid_protocol(format!(
                            "Feature '{feature}' requires '{dep}' to be enabled"
                        ))
                    );
                }
                FeatureRequirement::NotSupported(dep) => {
                    require!(
                        !self.is_feature_supported(dep),
                        Error::invalid_protocol(format!(
                            "Feature '{feature}' requires '{dep}' to not be supported"
                        ))
                    );
                }
                FeatureRequirement::NotEnabled(dep) => {
                    require!(
                        !self.is_feature_enabled(dep),
                        Error::invalid_protocol(format!(
                            "Feature '{feature}' requires '{dep}' to not be enabled"
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
        let info = feature.info();
        match &info.kernel_support {
            KernelSupport::Supported => {}
            KernelSupport::NotSupported => {
                return Err(Error::unsupported(format!(
                    "Feature '{feature}' is not supported"
                )))
            }
            KernelSupport::Custom(check) => {
                check(&self.protocol, &self.table_properties, operation)?;
            }
        };
        self.validate_feature_requirements(feature)
    }

    /// Returns all reader features enabled for this table based on protocol version.
    /// For table features protocol (v3), returns the explicit reader_features list.
    /// For legacy protocol (v1-2), infers features from the version number.
    fn get_enabled_reader_features(&self) -> Vec<TableFeature> {
        extract_enabled_reader_features(&self.protocol)
    }

    /// Returns all writer features enabled for this table based on protocol version.
    /// For table features protocol (v7), returns the explicit writer_features list.
    /// For legacy protocol (v1-6), infers features from the version number.
    fn get_enabled_writer_features(&self) -> Vec<TableFeature> {
        extract_enabled_writer_features(&self.protocol)
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

    /// Ensures Kernel supports both scanning and writing this table.
    pub(crate) fn ensure_read_write_supported(&self) -> DeltaResult<()> {
        self.ensure_operation_supported(Operation::Scan)?;
        self.ensure_operation_supported(Operation::Write)
    }

    /// Internal helper for read operations (Scan, Cdf)
    fn ensure_read_supported(&self, operation: Operation) -> DeltaResult<()> {
        check_reader_version_range(&self.protocol)?;

        // Check all enabled reader features have kernel support
        for feature in self.get_enabled_reader_features() {
            self.check_feature_support(&feature, operation)?;
        }

        Ok(())
    }

    /// Internal helper for write operations
    fn ensure_write_supported(&self) -> DeltaResult<()> {
        // Version check: kernel supports writer versions
        // MIN_VALID_RW_VERSION..=MAX_VALID_WRITER_VERSION
        require!(
            self.protocol.min_writer_version() >= MIN_VALID_RW_VERSION,
            Error::InvalidProtocol(format!(
                "min_writer_version must be >= {MIN_VALID_RW_VERSION}, got {}",
                self.protocol.min_writer_version()
            ))
        );
        // Version check: kernel supports writer versions 1..=MAX_VALID_WRITER_VERSION
        if self.protocol.min_writer_version() > MAX_VALID_WRITER_VERSION {
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
            && schema_has_invariants(self.logical_schema.as_ref())
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
    /// Row tracking is suspended when the `delta.rowTrackingSuspended` table property is set to
    /// `true`. Note that:
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
        self.protocol.min_reader_version() < TABLE_FEATURES_MIN_READER_VERSION
    }

    /// Returns true if the protocol uses legacy writer version (< 7)
    #[allow(dead_code)]
    fn is_legacy_writer_version(&self) -> bool {
        self.protocol.min_writer_version() < TABLE_FEATURES_MIN_WRITER_VERSION
    }

    /// Helper to check if a feature is present in a feature list.
    fn has_feature(features: Option<&[TableFeature]>, feature: &TableFeature) -> bool {
        features
            .map(|features| features.contains(feature))
            .unwrap_or(false)
    }

    /// Helper method to check if a feature is supported.
    /// This checks protocol versions and feature lists but does NOT check enablement properties.
    #[internal_api]
    pub(crate) fn is_feature_supported(&self, feature: &TableFeature) -> bool {
        let info = feature.info();
        let min_legacy_version = info.min_legacy_version.as_ref();
        let min_reader_version =
            min_legacy_version.map_or(TABLE_FEATURES_MIN_READER_VERSION, |v| v.reader);
        let min_writer_version =
            min_legacy_version.map_or(TABLE_FEATURES_MIN_WRITER_VERSION, |v| v.writer);
        match info.feature_type {
            FeatureType::WriterOnly => {
                if self.is_legacy_writer_version() {
                    // Legacy writer: protocol writer version meets minimum requirement
                    self.protocol.min_writer_version() >= min_writer_version
                } else {
                    // Table features writer: feature is in writer_features list
                    Self::has_feature(self.protocol.writer_features(), feature)
                }
            }
            FeatureType::ReaderWriter => {
                let reader_supported = if self.is_legacy_reader_version() {
                    // Legacy reader: protocol reader version meets minimum requirement
                    self.protocol.min_reader_version() >= min_reader_version
                } else {
                    // Reader-supported if the feature is in reader_features, or it is a legacy
                    // ReaderWriter feature (only ColumnMapping) whose minimum reader version is
                    // met. The second case stays compatible with tables a past delta-spark bug
                    // created with ReaderWriter features in writerFeatures only, absent from
                    // readerFeatures.
                    Self::has_feature(self.protocol.reader_features(), feature)
                        || feature.is_valid_for_legacy_reader(self.protocol.min_reader_version())
                };

                let writer_supported = if self.is_legacy_writer_version() {
                    // Legacy writer: protocol writer version meets minimum requirement
                    self.protocol.min_writer_version() >= min_writer_version
                } else {
                    // Table features writer: feature is in writer_features list
                    Self::has_feature(self.protocol.writer_features(), feature)
                };

                reader_supported && writer_supported
            }
            FeatureType::Unknown => Self::has_feature(self.protocol.writer_features(), feature),
        }
    }

    /// Generic method to check if a feature is enabled.
    ///
    /// A feature is enabled if:
    /// 1. It is supported in the protocol
    /// 2. The enablement check passes
    #[internal_api]
    pub(crate) fn is_feature_enabled(&self, feature: &TableFeature) -> bool {
        if !self.is_feature_supported(feature) {
            return false;
        }

        match feature.info().enablement_check {
            EnablementCheck::AlwaysIfSupported => true,
            EnablementCheck::EnabledIf(check_fn) => check_fn(&self.table_properties),
        }
    }

    /// Returns true when the table requires every AddFile to carry a non-null
    /// `stats.numRecords`.
    pub(crate) fn requires_stats_num_records(&self) -> bool {
        // TODO(#1125): Add icebergCompatV2 to the list when it is supported.
        self.is_feature_enabled(&TableFeature::IcebergCompatV3)
    }

    /// TODO(#2538): Row-tracking is not fully supported for removeFile currently.
    /// See `crate::table_features::ROW_TRACKING_INFO` for more details.
    pub(crate) fn validate_feature_support_for_remove(&self) -> DeltaResult<()> {
        // RowTracking is a prerequisite for IcebergCompatV3, so the IcebergCompatV3 arm is
        // technically redundant. Just be conservative here to check both.
        if self.should_write_row_tracking() {
            return Err(Error::unsupported(
                "Remove actions are not yet supported on tables with rowTracking supported \
                 and not suspended",
            ));
        }
        if self.is_feature_enabled(&TableFeature::IcebergCompatV3) {
            return Err(Error::unsupported(
                "Remove actions are not yet supported on tables with icebergCompatV3 enabled",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use rstest::rstest;

    use super::{InCommitTimestampEnablement, TableConfiguration};
    use crate::actions::{Metadata, Protocol, MIN_VALUES};
    use crate::schema::{
        column_name, schema, schema_ref, ColumnName, DataType, SchemaRef, StructField,
    };
    use crate::table_features::{
        ColumnMappingMode, FeatureType, Operation, TableFeature, TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION,
    };
    use crate::table_properties::{
        TableProperties, ENABLE_DELETION_VECTORS, ENABLE_ICEBERG_COMPAT_V1,
        ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3, ENABLE_IN_COMMIT_TIMESTAMPS,
        ENABLE_ROW_TRACKING, ROW_TRACKING_SUSPENDED,
    };
    use crate::unit_test_utils::{
        assert_result_error_with_message, test_schema_flat, test_schema_flat_with_column_mapping,
        test_schema_nested, test_schema_nested_with_column_mapping, test_schema_with_array,
        test_schema_with_array_and_column_mapping, test_schema_with_map,
        test_schema_with_map_and_column_mapping, MockProtocolBuilder,
        MockTableConfigurationBuilder,
    };
    use crate::Error;

    #[test]
    fn validate_feature_for_addition_checks_required_kernel_operations() {
        let supported_writer = create_mock_table_config(&[], &[TableFeature::DomainMetadata]);
        assert!(supported_writer
            .validate_feature_for_addition(&TableFeature::DomainMetadata)
            .is_ok());

        let supported_reader_writer =
            create_mock_table_config(&[], &[TableFeature::DeletionVectors]);
        assert!(supported_reader_writer
            .validate_feature_for_addition(&TableFeature::DeletionVectors)
            .is_ok());

        let unsupported = create_mock_table_config(&[], &[TableFeature::GeospatialType]);
        assert!(unsupported
            .validate_feature_for_addition(&TableFeature::GeospatialType)
            .is_err());

        let write_unsupported = create_mock_table_config(&[], &[TableFeature::TypeWidening]);
        assert_result_error_with_message(
            write_unsupported.validate_feature_for_addition(&TableFeature::TypeWidening),
            "Feature 'typeWidening' is not supported for writes",
        );
    }

    #[test]
    fn table_configuration_rejects_partition_column_missing_from_schema() {
        let result = MockTableConfigurationBuilder::new()
            .with_partition_columns(["missing"])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .try_build();

        assert_result_error_with_message(result, "Partition column 'missing' not found in schema");
    }

    #[test]
    fn table_configuration_rejects_duplicate_partition_columns() {
        let result = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                nullable "value": INTEGER,
                nullable "part": STRING,
            })
            .with_partition_columns(["part", "part"])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .try_build();

        assert_result_error_with_message(result, "Duplicate partition column: 'part'");
    }

    #[test]
    fn dv_supported_not_enabled() {
        use crate::table_properties::ENABLE_CHANGE_DATA_FEED;

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::DeletionVectors, TableFeature::ChangeDataFeed])
                    .build(),
            )
            .build();
        assert!(table_config.is_feature_supported(&TableFeature::DeletionVectors));
        assert!(!table_config.is_feature_enabled(&TableFeature::DeletionVectors));
    }

    #[test]
    fn dv_enabled() {
        use crate::table_properties::{ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS};

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([
                (ENABLE_CHANGE_DATA_FEED, "true"),
                (ENABLE_DELETION_VECTORS, "true"),
            ])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::DeletionVectors, TableFeature::ChangeDataFeed])
                    .build(),
            )
            .build();
        assert!(table_config.is_feature_supported(&TableFeature::DeletionVectors));
        assert!(table_config.is_feature_enabled(&TableFeature::DeletionVectors));
    }

    #[rstest]
    #[case(-1, 2, Operation::Scan)]
    #[case(1, -1, Operation::Write)]
    fn reject_protocol_version_below_minimum(
        #[case] rv: i32,
        #[case] wv: i32,
        #[case] op: Operation,
    ) {
        let table_config = MockTableConfigurationBuilder::new()
            .with_protocol(Protocol::new_unchecked(
                rv,
                wv,
                TableFeature::NO_LIST,
                TableFeature::NO_LIST,
            ))
            .build();
        let expected = if rv < 1 {
            format!("Invalid protocol action in the delta log: min_reader_version must be >= 1, got {rv}")
        } else {
            format!("Invalid protocol action in the delta log: min_writer_version must be >= 1, got {wv}")
        };
        assert_result_error_with_message(table_config.ensure_operation_supported(op), &expected);
    }

    #[test]
    fn write_with_cdf() {
        use TableFeature::*;

        use crate::table_properties::{APPEND_ONLY, ENABLE_CHANGE_DATA_FEED};
        let cases = [
            (
                // Writing to CDF-enabled table is supported for writes
                MockTableConfigurationBuilder::new()
                    .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
                    .with_protocol(
                        MockProtocolBuilder::new()
                            .with_features([ChangeDataFeed])
                            .build(),
                    )
                    .build(),
                Ok(()),
            ),
            (
                // Should succeed even if AppendOnly is supported but not enabled
                MockTableConfigurationBuilder::new()
                    .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
                    .with_protocol(
                        MockProtocolBuilder::new()
                            .with_features([ChangeDataFeed, AppendOnly])
                            .build(),
                    )
                    .build(),
                Ok(()),
            ),
            (
                // Should succeed since AppendOnly is enabled
                MockTableConfigurationBuilder::new()
                    .with_properties([(ENABLE_CHANGE_DATA_FEED, "true"), (APPEND_ONLY, "true")])
                    .with_protocol(
                        MockProtocolBuilder::new()
                            .with_features([ChangeDataFeed, AppendOnly])
                            .build(),
                    )
                    .build(),
                Ok(()),
            ),
            (
                // Writer version > 7 is not supported
                MockTableConfigurationBuilder::new()
                    .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
                    .with_protocol(MockProtocolBuilder::new().with_versions(1, 8).build())
                    .build(),
                Err(Error::unsupported("Unsupported minimum writer version 8")),
            ),
            // Column mapping is now supported for writes.
            (
                // CDF + column mapping: both supported, should succeed
                MockTableConfigurationBuilder::new()
                    .with_properties([(ENABLE_CHANGE_DATA_FEED, "true"), (APPEND_ONLY, "true")])
                    .with_protocol(
                        MockProtocolBuilder::new()
                            .with_features([ChangeDataFeed, ColumnMapping, AppendOnly])
                            .build(),
                    )
                    .build(),
                Ok(()),
            ),
            (
                // Column mapping + AppendOnly, no CDF enabled: should succeed
                MockTableConfigurationBuilder::new()
                    .with_properties([(APPEND_ONLY, "true")])
                    .with_protocol(
                        MockProtocolBuilder::new()
                            .with_features([ChangeDataFeed, ColumnMapping, AppendOnly])
                            .build(),
                    )
                    .build(),
                Ok(()),
            ),
            (
                // Should succeed since change data feed is not enabled
                MockTableConfigurationBuilder::new()
                    .with_properties([(APPEND_ONLY, "true")])
                    .with_protocol(
                        MockProtocolBuilder::new()
                            .with_features([AppendOnly])
                            .build(),
                    )
                    .build(),
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

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_IN_COMMIT_TIMESTAMPS, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::InCommitTimestamp])
                    .build(),
            )
            .build();
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

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([
                (ENABLE_IN_COMMIT_TIMESTAMPS, "true"),
                (IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "5"),
                (IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, "100"),
            ])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::InCommitTimestamp])
                    .build(),
            )
            .build();
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

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([
                (ENABLE_IN_COMMIT_TIMESTAMPS, "true"),
                (IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "5"),
                // Missing enablement timestamp
            ])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::InCommitTimestamp])
                    .build(),
            )
            .build();
        assert!(table_config.is_feature_supported(&TableFeature::InCommitTimestamp));
        assert!(table_config.is_feature_enabled(&TableFeature::InCommitTimestamp));
        assert!(matches!(
            table_config.in_commit_timestamp_enablement(),
            Err(Error::Generic(msg)) if msg.contains("In-commit timestamp enabled, but enablement timestamp is missing")
        ));
    }
    #[test]
    fn ict_supported_and_not_enabled() {
        let table_config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::InCommitTimestamp])
                    .build(),
            )
            .build();
        assert!(table_config.is_feature_supported(&TableFeature::InCommitTimestamp));
        assert!(!table_config.is_feature_enabled(&TableFeature::InCommitTimestamp));
        let info = table_config.in_commit_timestamp_enablement().unwrap();
        assert_eq!(info, InCommitTimestampEnablement::NotEnabled);
    }
    #[test]
    fn fails_on_unsupported_feature() {
        let unknown = TableFeature::unknown("unknown");
        let table_config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features([&unknown])
                    .with_writer_features([&unknown])
                    .build(),
            )
            .build();
        table_config
            .ensure_operation_supported(Operation::Scan)
            .expect_err("Unknown feature is not supported in kernel");
    }
    #[test]
    fn dv_not_supported() {
        use crate::table_properties::ENABLE_CHANGE_DATA_FEED;

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::TimestampWithoutTimezone,
                        TableFeature::ChangeDataFeed,
                    ])
                    .build(),
            )
            .build();
        assert!(!table_config.is_feature_supported(&TableFeature::DeletionVectors));
        assert!(!table_config.is_feature_enabled(&TableFeature::DeletionVectors));
    }

    #[test]
    fn test_try_new_from() {
        use crate::table_properties::{ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS};

        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::DeletionVectors, TableFeature::ChangeDataFeed])
                    .build(),
            )
            .build();

        let new_schema = schema_ref! { nullable "value": INTEGER };
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
        let new_protocol = Protocol::try_new_modern(
            [TableFeature::DeletionVectors, TableFeature::V2Checkpoint],
            [
                TableFeature::DeletionVectors,
                TableFeature::V2Checkpoint,
                TableFeature::AppendOnly,
                TableFeature::ChangeDataFeed,
            ],
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
        assert_eq!(
            new_table_config.logical_schema(),
            table_config.logical_schema()
        );
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
        let ntz_config = |features: &[TableFeature]| {
            MockTableConfigurationBuilder::new()
                .with_schema(schema! { nullable "ts": TIMESTAMP_NTZ })
                .with_protocol(MockProtocolBuilder::new().with_features(features).build())
                .try_build()
        };

        let result = ntz_config(&[]);
        assert_result_error_with_message(result, "Unsupported: Table contains TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature in reader and writer features");

        let result = ntz_config(&[TableFeature::TimestampWithoutTimezone]);
        assert!(
            result.is_ok(),
            "Should succeed when TIMESTAMP_NTZ is used with required features"
        );
    }

    #[test]
    fn test_timestamp_ntz_legacy_alias_unblocks_read_and_write() {
        // Build the protocol from the legacy string alias to exercise the real read path.
        let protocol =
            Protocol::try_new_modern(["timestampWithoutTimezone"], ["timestampWithoutTimezone"])
                .unwrap();

        assert_eq!(
            protocol.reader_features(),
            Some([TableFeature::TimestampWithoutTimezone].as_slice())
        );
        assert_eq!(
            protocol.writer_features(),
            Some([TableFeature::TimestampWithoutTimezone].as_slice())
        );

        let table_config = MockTableConfigurationBuilder::new()
            .with_schema(schema! { nullable "ts": TIMESTAMP_NTZ })
            .with_protocol(protocol)
            .build();

        table_config
            .ensure_operation_supported(Operation::Scan)
            .unwrap();
        table_config
            .ensure_operation_supported(Operation::Write)
            .unwrap();
    }

    #[test]
    fn test_variant_validation_integration() {
        // Schema with VARIANT column
        let variant_config = |features: &[TableFeature]| {
            MockTableConfigurationBuilder::new()
                .with_schema(schema! { nullable "v": (DataType::unshredded_variant()) })
                .with_protocol(MockProtocolBuilder::new().with_features(features).build())
                .try_build()
        };

        let result = variant_config(&[]);
        assert_result_error_with_message(result, "Unsupported: Table contains VARIANT columns but does not have the required 'variantType' feature in reader and writer features");

        let result = variant_config(&[TableFeature::VariantType]);
        assert!(
            result.is_ok(),
            "Should succeed when VARIANT is used with required features"
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum UnknownFeatureShape {
        NotListed,
        WriterOnly,
        ReaderWriter,
    }

    fn create_unknown_feature_config(
        shape: UnknownFeatureShape,
    ) -> (TableFeature, TableConfiguration) {
        let unknown = TableFeature::unknown("futureFeature");
        let reader_features = match shape {
            UnknownFeatureShape::ReaderWriter => vec![unknown.clone()],
            _ => vec![],
        };
        let writer_features = match shape {
            UnknownFeatureShape::NotListed => vec![],
            _ => vec![unknown.clone()],
        };
        let tc = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features(reader_features)
                    .with_writer_features(writer_features)
                    .build(),
            )
            .build();
        (unknown, tc)
    }

    #[rstest]
    #[case::not_listed(UnknownFeatureShape::NotListed, false)]
    #[case::writer_only(UnknownFeatureShape::WriterOnly, true)]
    #[case::reader_writer(UnknownFeatureShape::ReaderWriter, true)]
    fn test_unknown_feature_protocol_support(
        #[case] shape: UnknownFeatureShape,
        #[case] expected_supported: bool,
    ) {
        let (unknown, config) = create_unknown_feature_config(shape);
        assert_eq!(config.is_feature_supported(&unknown), expected_supported);
    }

    #[rstest]
    #[case::not_listed(UnknownFeatureShape::NotListed, false)]
    #[case::writer_only(UnknownFeatureShape::WriterOnly, true)]
    #[case::reader_writer(UnknownFeatureShape::ReaderWriter, true)]
    fn test_unknown_feature_protocol_enablement(
        #[case] shape: UnknownFeatureShape,
        #[case] expected_enabled: bool,
    ) {
        let (unknown, config) = create_unknown_feature_config(shape);
        assert_eq!(config.is_feature_enabled(&unknown), expected_enabled);
    }

    #[rstest]
    fn test_unknown_feature_capabilities(
        #[values(
            UnknownFeatureShape::NotListed,
            UnknownFeatureShape::WriterOnly,
            UnknownFeatureShape::ReaderWriter
        )]
        shape: UnknownFeatureShape,
        #[values(Operation::Scan, Operation::Cdf, Operation::Write)] operation: Operation,
    ) {
        let (_, config) = create_unknown_feature_config(shape);
        let expected_ok = match shape {
            UnknownFeatureShape::NotListed => true,
            UnknownFeatureShape::WriterOnly => operation != Operation::Write,
            UnknownFeatureShape::ReaderWriter => false,
        };
        assert_eq!(
            config.ensure_operation_supported(operation).is_ok(),
            expected_ok
        );
    }

    #[test]
    fn test_is_feature_supported_writer_only() {
        let feature = TableFeature::AppendOnly;

        // Test with legacy protocol writer v2 - should be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();
        assert!(config.is_feature_supported(&feature));

        // Test with legacy protocol writer v1 - should NOT be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 1).build())
            .build();
        assert!(!config.is_feature_supported(&feature));

        // reader=2 (legacy), writer=7 (non-legacy) - feature in list, should be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::AppendOnly])
                    .with_versions(2, 7)
                    .build(),
            )
            .build();
        assert!(config.is_feature_supported(&feature));

        // reader=2 (legacy), writer=7 (non-legacy) - feature NOT in list, should NOT be supported
        // Use ChangeDataFeed which is also a WriterOnly feature
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::ChangeDataFeed])
                    .with_versions(2, 7)
                    .build(),
            )
            .build();
        assert!(!config.is_feature_supported(&feature));

        // Test with protocol reader=3, writer=7 (both non-legacy) - feature in list, should be
        // supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::AppendOnly])
                    .build(),
            )
            .build();
        assert!(config.is_feature_supported(&feature));

        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::DeletionVectors])
                    .build(),
            )
            .build();
        assert!(!config.is_feature_supported(&feature));
    }

    #[test]
    fn test_is_feature_supported_reader_writer() {
        let feature = TableFeature::ColumnMapping;

        // Test with sufficient versions (legacy mode) - should be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();
        assert!(config.is_feature_supported(&feature));

        // Test with insufficient reader version - should NOT be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 5).build())
            .build();
        assert!(!config.is_feature_supported(&feature));

        // Test with insufficient writer version - should NOT be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 4).build())
            .build();
        assert!(!config.is_feature_supported(&feature));

        // Test with asymmetric: reader=2 (legacy), writer=7 (non-legacy)
        // ReaderWriter features CANNOT be enabled in this protocol state (protocol validation)
        // But we still need to test that the code correctly identifies them as NOT supported
        // Create a table with only WriterOnly features (e.g., AppendOnly)
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::AppendOnly])
                    .with_versions(2, 7)
                    .build(),
            )
            .build();
        // ColumnMapping (ReaderWriter) should NOT be supported because:
        // - reader=2 (legacy) checks version: 2 >= 2 (reader_supported = true)
        // - writer=7 (non-legacy) checks list: ColumnMapping not in writer_features
        //   (writer_supported = false)
        // - Result: false (requires BOTH to be true)
        assert!(!config.is_feature_supported(&feature));

        // Test with non-legacy mode (3,7) - feature in list, should be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::ColumnMapping])
                    .build(),
            )
            .build();
        assert!(config.is_feature_supported(&feature));

        // Test with non-legacy mode (3,7) - feature NOT in list, should NOT be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::DeletionVectors])
                    .build(),
            )
            .build();
        assert!(!config.is_feature_supported(&feature));
    }

    #[test]
    fn test_is_feature_supported_orphaned_column_mapping() {
        // A (3, 7) table with ColumnMapping in writerFeatures but missing from readerFeatures.
        // ColumnMapping is a legacy ReaderWriter feature whose minimum reader version (2) is met by
        // reader version 3, so it counts as reader-supported even though it is absent from
        // readerFeatures. It is in writerFeatures, so it is writer-supported too.
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_flat_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features(TableFeature::EMPTY_LIST)
                    .with_writer_features([TableFeature::ColumnMapping])
                    .build(),
            )
            .build();
        assert!(config.is_feature_supported(&TableFeature::ColumnMapping));

        // A non-legacy ReaderWriter feature in the same writer-only position has no legacy reader
        // version to fall back on, so the protocol is rejected outright rather than tolerated.
        assert!(Protocol::try_new(
            TABLE_FEATURES_MIN_READER_VERSION,
            TABLE_FEATURES_MIN_WRITER_VERSION,
            Some(TableFeature::EMPTY_LIST),
            Some(vec![TableFeature::DeletionVectors]),
        )
        .is_err());

        // The conformant shape (ColumnMapping in both lists) is still reported supported: the
        // legacy-version fallback does not perturb the normal reader_features membership path.
        let conformant = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::ColumnMapping])
                    .build(),
            )
            .build();
        assert!(conformant.is_feature_supported(&TableFeature::ColumnMapping));
    }

    #[test]
    fn test_column_mapping_absent_from_both_lists_is_unsupported() {
        // ColumnMapping in neither list must not be treated as supported: the writer half fails.
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::AppendOnly])
                    .build(),
            )
            .build();
        assert!(!config.is_feature_supported(&TableFeature::ColumnMapping));
    }

    #[test]
    fn test_is_feature_enabled_with_property_check() {
        use crate::table_properties::APPEND_ONLY;

        let feature = TableFeature::AppendOnly;

        // Test when property check fails - should be supported but not enabled
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();
        assert!(config.is_feature_supported(&feature));
        assert!(!config.is_feature_enabled(&feature));

        // Test when property check passes - should be both supported and enabled
        let config = MockTableConfigurationBuilder::new()
            .with_properties([(APPEND_ONLY, "true")])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();
        assert!(config.is_feature_supported(&feature));
        assert!(config.is_feature_enabled(&feature));

        // Test when property is set but feature is not supported by protocol versions.
        // TODO: Reject this orphaned metadata
        let config = MockTableConfigurationBuilder::new()
            .with_properties([(APPEND_ONLY, "true")])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 1).build())
            .build();
        assert!(!config.is_feature_supported(&feature));
        assert!(!config.is_feature_enabled(&feature));
    }

    #[test]
    fn test_is_feature_enabled_always_if_supported() {
        let feature = TableFeature::V2Checkpoint;

        // Test when supported - should be both supported and enabled
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::V2Checkpoint])
                    .build(),
            )
            .build();
        assert!(config.is_feature_supported(&feature));
        assert!(config.is_feature_enabled(&feature));

        // Test when not supported - should be neither supported nor enabled
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::DeletionVectors])
                    .build(),
            )
            .build();
        assert!(!config.is_feature_supported(&feature));
        assert!(!config.is_feature_enabled(&feature));
    }

    #[test]
    fn test_ensure_operation_supported_reads() {
        let config = MockTableConfigurationBuilder::new().build();
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::V2Checkpoint])
                    .build(),
            )
            .build();
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::InCommitTimestamp])
                    .with_versions(2, 7)
                    .build(),
            )
            .build();
        assert!(config.ensure_operation_supported(Operation::Scan).is_ok());

        #[cfg(feature = "geo-type-in-dev")]
        {
            let config = MockTableConfigurationBuilder::new()
                .with_protocol(
                    MockProtocolBuilder::new()
                        .with_features([TableFeature::GeospatialType])
                        .build(),
                )
                .build();
            assert!(config.ensure_operation_supported(Operation::Scan).is_ok());
            assert!(config.ensure_operation_supported(Operation::Cdf).is_ok());
        }
    }

    #[test]
    fn test_ensure_operation_supported_writes() {
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::AppendOnly,
                        TableFeature::DeletionVectors,
                        TableFeature::DomainMetadata,
                        TableFeature::Invariants,
                        TableFeature::RowTracking,
                    ])
                    .build(),
            )
            .build();
        assert!(config.ensure_operation_supported(Operation::Write).is_ok());

        // Type Widening is not supported for writes
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::TypeWidening])
                    .build(),
            )
            .build();
        assert_result_error_with_message(
            config.ensure_operation_supported(Operation::Write),
            r#"Feature 'typeWidening' is not supported for writes"#,
        );

        #[cfg(feature = "geo-type-in-dev")]
        {
            let config = MockTableConfigurationBuilder::new()
                .with_protocol(
                    MockProtocolBuilder::new()
                        .with_features([TableFeature::GeospatialType])
                        .build(),
                )
                .build();
            assert_result_error_with_message(
                config.ensure_operation_supported(Operation::Write),
                r#"Feature 'geospatial' is not supported for writes"#,
            );
        }
    }

    #[cfg(not(feature = "geo-type-in-dev"))]
    #[rstest]
    #[case::scan(Operation::Scan)]
    #[case::cdf(Operation::Cdf)]
    #[case::write(Operation::Write)]
    fn test_geospatial_not_supported_without_cargo_feature(#[case] operation: Operation) {
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::GeospatialType])
                    .build(),
            )
            .build();
        assert_result_error_with_message(
            config.ensure_operation_supported(operation),
            "Feature 'geospatial' is not supported",
        );
    }

    #[test]
    fn test_illegal_writer_feature_combination() {
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::RowTracking])
                    .build(),
            )
            .build();
        assert_result_error_with_message(
            config.ensure_operation_supported(Operation::Write),
            "Feature 'rowTracking' requires 'domainMetadata' to be supported",
        );
    }

    #[test]
    fn test_row_tracking_with_domain_metadata_requirement() {
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::RowTracking, TableFeature::DomainMetadata])
                    .build(),
            )
            .build();
        assert!(
            config.ensure_operation_supported(Operation::Write).is_ok(),
            "RowTracking with DomainMetadata should be supported for writes"
        );
    }

    #[test]
    fn test_catalog_managed_writes() {
        // CatalogManaged requires ICT to be supported and enabled
        let config = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_IN_COMMIT_TIMESTAMPS, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::CatalogManaged,
                        TableFeature::InCommitTimestamp,
                    ])
                    .build(),
            )
            .build();
        assert!(config.ensure_operation_supported(Operation::Write).is_ok());

        let config = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_IN_COMMIT_TIMESTAMPS, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::CatalogOwnedPreview,
                        TableFeature::InCommitTimestamp,
                    ])
                    .build(),
            )
            .build();
        assert!(config.ensure_operation_supported(Operation::Write).is_ok());
    }

    // A catalog-managed table requires inCommitTimestamp to be enabled.
    #[rstest]
    #[case::catalog_managed(
        TableFeature::CatalogManaged,
        "Feature 'catalogManaged' requires 'inCommitTimestamp' to be enabled"
    )]
    #[case::catalog_owned_preview(
        TableFeature::CatalogOwnedPreview,
        "Feature 'catalogOwned-preview' requires 'inCommitTimestamp' to be enabled"
    )]
    fn test_catalog_managed_requires_in_commit_timestamp(
        #[case] feature: TableFeature,
        #[case] expected_error: &str,
    ) {
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_features([feature]).build())
            .build();
        let result = config.ensure_operation_supported(Operation::Write);
        assert_result_error_with_message(result, expected_error);
    }

    fn test_schema_for_column_mapping(mode: Option<ColumnMappingMode>) -> SchemaRef {
        match mode {
            Some(ColumnMappingMode::Name) | Some(ColumnMappingMode::Id) => {
                test_schema_flat_with_column_mapping()
            }
            Some(ColumnMappingMode::None) | None => test_schema_flat(),
        }
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

        schema_ref! {
            (field_a),
            (field_b),
        }
    }

    #[test]
    fn test_build_expected_stats_schemas_no_column_mapping() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                nullable "col_a": LONG,
                nullable "col_b": STRING,
            })
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();

        assert_eq!(config.column_mapping_mode(), ColumnMappingMode::None);

        let stats_schemas = config.build_expected_stats_schemas(None, None).unwrap();

        // Verify field names are logical names
        let min_values = stats_schemas
            .physical
            .field(MIN_VALUES)
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
        // With column mapping, physical schema should have physical names
        let schema = schema_with_column_mapping();
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        assert_eq!(config.column_mapping_mode(), ColumnMappingMode::Name);

        let stats_schemas = config.build_expected_stats_schemas(None, None).unwrap();

        // Verify physical schema has physical names
        let physical_min_values = stats_schemas
            .physical
            .field(MIN_VALUES)
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
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .with_column_mapping(ColumnMappingMode::Id)
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        assert_eq!(config.column_mapping_mode(), ColumnMappingMode::Id);

        let stats_schemas = config.build_expected_stats_schemas(None, None).unwrap();

        // Verify physical schema has physical names
        let physical_min_values = stats_schemas
            .physical
            .field(MIN_VALUES)
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
        let physical_data = data_schema.make_physical(ColumnMappingMode::Id).unwrap();
        let data_field = physical_data.field("phys_col_a").unwrap();
        assert!(
            matches!(
                data_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
                Some(MetadataValue::Number(_))
            ),
            "make_physical should inject ParquetFieldId for data schemas in Id mode"
        );
    }

    /// Schema with a data column and two partition columns, all with column mapping metadata.
    /// data_col (long) -> phys_data, part_a (string) -> phys_part_a, part_b (integer) ->
    /// phys_part_b
    fn partitioned_schema_with_column_mapping() -> SchemaRef {
        let data_col: StructField = serde_json::from_str(
            r#"{
                "name": "data_col",
                "type": "long",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": "phys_data"
                }
            }"#,
        )
        .unwrap();
        let part_a: StructField = serde_json::from_str(
            r#"{
                "name": "part_a",
                "type": "string",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 2,
                    "delta.columnMapping.physicalName": "phys_part_a"
                }
            }"#,
        )
        .unwrap();
        let part_b: StructField = serde_json::from_str(
            r#"{
                "name": "part_b",
                "type": "integer",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 3,
                    "delta.columnMapping.physicalName": "phys_part_b"
                }
            }"#,
        )
        .unwrap();
        schema_ref! {
            (data_col),
            (part_a),
            (part_b),
        }
    }

    #[test]
    fn test_build_expected_stats_schemas_excludes_partition_columns() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_partition_columns(["part_a", "part_b"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        let stats_schemas = config.build_expected_stats_schemas(None, None).unwrap();

        let DataType::Struct(inner) = stats_schemas
            .physical
            .field(MIN_VALUES)
            .unwrap()
            .data_type()
        else {
            panic!("Expected minValues to be a struct");
        };
        assert!(
            inner.field("phys_data").is_some(),
            "Data column should be present with physical name"
        );
        assert!(
            inner.field("phys_part_a").is_none(),
            "Partition column a should be excluded"
        );
        assert!(
            inner.field("phys_part_b").is_none(),
            "Partition column b should be excluded"
        );
    }

    #[test]
    fn test_partition_columns_are_logical_under_column_mapping() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_partition_columns(["part_a", "part_b"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        assert_eq!(config.logical_partition_columns(), ["part_a", "part_b"]);
        let partition_schema = config
            .build_partition_values_parsed_schema()
            .expect("partition schema should be present");
        assert!(partition_schema.field("phys_part_a").is_some());
        assert!(partition_schema.field("phys_part_b").is_some());
        assert!(partition_schema.field("part_a").is_none());
        assert!(partition_schema.field("part_b").is_none());
    }

    #[rstest]
    #[case::none(ColumnMappingMode::None, ["part_a", "part_b"])]
    #[case::name(ColumnMappingMode::Name, ["phys_part_a", "phys_part_b"])]
    #[case::id(ColumnMappingMode::Id, ["phys_part_a", "phys_part_b"])]
    fn test_physical_partition_columns(
        #[case] column_mapping_mode: ColumnMappingMode,
        #[case] expected: [&str; 2],
    ) {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema_with_column_mapping())
            .with_column_mapping(column_mapping_mode)
            .with_partition_columns(["part_a", "part_b"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        assert_eq!(
            config.physical_partition_columns().collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn test_physical_stats_column_names_excludes_partition_columns() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_partition_columns(["part_a", "part_b"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        let column_names = config.physical_stats_column_names(None);
        assert_eq!(column_names, vec![column_name!("phys_data")]);

        // Also verify partition columns are excluded when passed as required columns
        let required = [column_name!("phys_part_a"), column_name!("phys_part_b")];
        let column_names = config.physical_stats_column_names(Some(&required));
        assert_eq!(column_names, vec![column_name!("phys_data")]);
    }

    #[test]
    fn test_physical_stats_column_names_excludes_partition_columns_no_column_mapping() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                nullable "data_col": LONG,
                nullable "part_a": STRING,
                nullable "part_b": INTEGER,
            })
            .with_partition_columns(["part_a", "part_b"])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();

        let column_names = config.physical_stats_column_names(None);
        assert_eq!(column_names, vec![column_name!("data_col")]);
    }

    #[test]
    fn test_physical_stats_column_names_all_partition_columns_returns_empty() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                nullable "part_a": STRING,
                nullable "part_b": INTEGER,
            })
            .with_partition_columns(["part_a", "part_b"])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();

        let column_names = config.physical_stats_column_names(None);
        assert!(column_names.is_empty());
    }

    #[test]
    fn test_physical_stats_column_names_returns_physical_names() {
        // physical_stats_column_names should return physical column names
        let schema = schema_with_column_mapping();
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();

        let column_names = config.physical_stats_column_names(None /* required_columns */);

        // Should return physical names, not logical names
        assert_eq!(
            column_names,
            vec![column_name!("phys_col_a"), column_name!("phys_col_b"),],
            "Expected physical column names, not logical names"
        );
    }

    #[test]
    fn test_physical_stats_column_names_with_data_skipping_stats_columns() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_nested_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_properties([("delta.dataSkippingStatsColumns", "id,info.name")])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();
        let column_names = config.physical_stats_column_names(None);
        assert_eq!(
            column_names,
            vec![column_name!("phys_id"), column_name!("phys_info.phys_name"),],
        );
    }

    #[test]
    fn test_physical_stats_column_names_skips_nonexistent_data_skipping_stats_column() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_nested_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_properties([("delta.dataSkippingStatsColumns", "id,nonexistent")])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();
        let column_names = config.physical_stats_column_names(None);
        assert_eq!(column_names, vec![column_name!("phys_id")],);
    }

    #[rstest]
    // --- flat schema ---
    #[case::flat_none(
        test_schema_flat(),
        ColumnMappingMode::None,
        vec![column_name!("id"), column_name!("name")],
    )]
    #[case::flat_name(
        test_schema_flat_with_column_mapping(),
        ColumnMappingMode::Name,
        vec![column_name!("phys_id"), column_name!("phys_name")],
    )]
    #[case::flat_id(
        test_schema_flat_with_column_mapping(),
        ColumnMappingMode::Id,
        vec![column_name!("phys_id"), column_name!("phys_name")],
    )]
    // --- nested schema (includes map/array inside struct as leaf columns) ---
    #[case::nested_none(
        test_schema_nested(),
        ColumnMappingMode::None,
        vec![
            column_name!("id"),
            column_name!("info.name"),
            column_name!("info.age"),
            column_name!("info.tags"),
            column_name!("info.scores"),
        ],
    )]
    #[case::nested_name(
        test_schema_nested_with_column_mapping(),
        ColumnMappingMode::Name,
        vec![
            column_name!("phys_id"),
            column_name!("phys_info.phys_name"),
            column_name!("phys_info.phys_age"),
            column_name!("phys_info.phys_tags"),
            column_name!("phys_info.phys_scores"),
        ],
    )]
    #[case::nested_id(
        test_schema_nested_with_column_mapping(),
        ColumnMappingMode::Id,
        vec![
            column_name!("phys_id"),
            column_name!("phys_info.phys_name"),
            column_name!("phys_info.phys_age"),
            column_name!("phys_info.phys_tags"),
            column_name!("phys_info.phys_scores"),
        ],
    )]
    // --- schema with map (included as leaf for nullCount stats) ---
    #[case::map_none(
        test_schema_with_map(),
        ColumnMappingMode::None,
        vec![column_name!("id"), column_name!("entries"), column_name!("name")],
    )]
    #[case::map_name(
        test_schema_with_map_and_column_mapping(),
        ColumnMappingMode::Name,
        vec![column_name!("phys_id"), column_name!("phys_entries"), column_name!("phys_name")],
    )]
    #[case::map_id(
        test_schema_with_map_and_column_mapping(),
        ColumnMappingMode::Id,
        vec![column_name!("phys_id"), column_name!("phys_entries"), column_name!("phys_name")],
    )]
    // --- schema with array (included as leaf for nullCount stats) ---
    #[case::array_none(
        test_schema_with_array(),
        ColumnMappingMode::None,
        vec![column_name!("id"), column_name!("items"), column_name!("name")],
    )]
    #[case::array_name(
        test_schema_with_array_and_column_mapping(),
        ColumnMappingMode::Name,
        vec![column_name!("phys_id"), column_name!("phys_items"), column_name!("phys_name")],
    )]
    #[case::array_id(
        test_schema_with_array_and_column_mapping(),
        ColumnMappingMode::Id,
        vec![column_name!("phys_id"), column_name!("phys_items"), column_name!("phys_name")],
    )]
    fn test_physical_stats_column_names_all_schemas(
        #[case] schema: SchemaRef,
        #[case] mode: ColumnMappingMode,
        #[case] expected_physical: Vec<ColumnName>,
    ) {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .with_column_mapping(mode)
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .build();
        let physical_names = config.physical_stats_column_names(None);
        assert_eq!(
            physical_names, expected_physical,
            "Incorrect physical column names for mode '{mode:?}'"
        );
    }

    #[test]
    fn test_clustered_table_writes() {
        // ClusteredTable requires DomainMetadata to be supported
        let config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::ClusteredTable, TableFeature::DomainMetadata])
                    .build(),
            )
            .build();
        assert!(
            config.ensure_operation_supported(Operation::Write).is_ok(),
            "ClusteredTable with DomainMetadata should be supported for writes"
        );
    }

    // V3 supported + property set -> partition column materialized into the write schema;
    // V3 supported but property unset -> partition column stripped from the write schema.
    #[rstest]
    #[case::v3_enabled(
        &[(ENABLE_ICEBERG_COMPAT_V3, "true"), (ENABLE_ROW_TRACKING, "true")],
        // pcol is included, meaning we expect the partition col to be materialized to disk.
        vec!["value", "pcol"],
    )]
    #[case::v3_supported_but_property_unset(&[], vec!["value"])]
    fn test_physical_write_schema_materializes_pv_when_iceberg_compat_v3_enabled(
        #[case] extra_props: &[(&str, &str)],
        #[case] expected_field_names: Vec<&str>,
    ) {
        // Partitioned schema: one data col + one partition col. No column mapping, so physical
        // names equal logical names.
        // IcebergCompatV3 requires column mapping. This test bypasses that requirement for
        // convenience.
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                nullable "value": INTEGER,
                nullable "pcol": STRING,
            })
            .with_partition_columns(["pcol"])
            .with_properties(extra_props)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::IcebergCompatV3,
                        TableFeature::RowTracking,
                        TableFeature::DomainMetadata,
                    ])
                    .build(),
            )
            .build();

        let write_schema = config.physical_write_schema();
        // This is the final check: whether the partition column `pcol` is present in the
        // physical schema as expected.
        let field_names: Vec<&str> = write_schema.fields().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, expected_field_names);
    }

    #[test]
    fn test_physical_write_schema_strips_void_columns() {
        // Pins the invariant that `physical_write_schema()` strips void columns from the
        // returned schema (top level and nested), so callers receive a Parquet-writable
        // schema without needing to apply `strip_void_from_schema` themselves.
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                nullable "id": INTEGER,
                nullable "v": VOID,
                nullable "s": {
                    nullable "a": INTEGER,
                    nullable "nested_void": VOID,
                },
            })
            .build();

        let write_schema = config.physical_write_schema();

        // Top-level void is dropped.
        let top_names: Vec<&str> = write_schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(top_names, vec!["id", "s"]);

        // Nested void is also dropped, leaving only `a` inside `s`.
        let s_field = write_schema.field("s").expect("s present after strip");
        let DataType::Struct(s_inner) = s_field.data_type() else {
            panic!("s should still be a struct");
        };
        let s_names: Vec<&str> = s_inner.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(s_names, vec!["a"]);
    }

    #[test]
    fn test_iceberg_compat_v3_write_supported() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_flat_with_column_mapping())
            .with_properties([
                (ENABLE_ICEBERG_COMPAT_V3, "true"),
                (ENABLE_ROW_TRACKING, "true"),
            ])
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features([TableFeature::ColumnMapping])
                    .with_writer_features([
                        TableFeature::IcebergCompatV3,
                        TableFeature::ColumnMapping,
                        TableFeature::RowTracking,
                        TableFeature::DomainMetadata,
                    ])
                    .build(),
            )
            .build();
        config
            .ensure_operation_supported(Operation::Write)
            .expect("V3 write should be supported once kernel_support flips to Supported");
    }

    #[rstest]
    #[case::unset(None, false)]
    #[case::true_(Some("true"), true)]
    #[case::false_(Some("false"), false)]
    fn test_iceberg_compat_v3_enablement_follows_table_property(
        #[case] property_value: Option<&str>,
        #[case] expected_enabled: bool,
    ) {
        let extra = property_value
            .map(|v| vec![(ENABLE_ICEBERG_COMPAT_V3, v)])
            .unwrap_or_default();
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_flat_with_column_mapping())
            .with_properties(&extra)
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features([TableFeature::ColumnMapping])
                    .with_writer_features([
                        TableFeature::IcebergCompatV3,
                        TableFeature::ColumnMapping,
                        TableFeature::RowTracking,
                        TableFeature::DomainMetadata,
                    ])
                    .build(),
            )
            .build();
        assert_eq!(
            config.is_feature_enabled(&TableFeature::IcebergCompatV3),
            expected_enabled,
        );
    }

    #[rstest]
    #[case::column_mapping_not_supported(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        None,
        vec![],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        Some("requires 'columnMapping' to be enabled"),
    )]
    #[case::column_mapping_mode_none(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        Some(ColumnMappingMode::None),
        vec![TableFeature::ColumnMapping],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        // column mapping mode = none is considered as not enabled.
        Some("requires 'columnMapping' to be enabled"),
    )]
    // RowTracking feature supported but `delta.enableRowTracking` is unset, so it's not enabled.
    #[case::row_tracking_not_enabled(
        &[(ENABLE_ICEBERG_COMPAT_V3, "true")],
        Some(ColumnMappingMode::Name),
        vec![TableFeature::ColumnMapping],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        Some("requires 'rowTracking' to be enabled"),
    )]
    #[case::with_iceberg_compat_v1_enabled(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (ENABLE_ICEBERG_COMPAT_V1, "true"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        Some(ColumnMappingMode::Name),
        vec![TableFeature::ColumnMapping],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::IcebergCompatV1,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        Some("requires 'icebergCompatV1' to not be enabled"),
    )]
    #[case::with_iceberg_compat_v2_enabled(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (ENABLE_ICEBERG_COMPAT_V2, "true"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        Some(ColumnMappingMode::Name),
        vec![TableFeature::ColumnMapping],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::IcebergCompatV2,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        Some("requires 'icebergCompatV2' to not be enabled"),
    )]
    // Positive paths for both supported column-mapping modes (`name` and `id`).
    #[case::all_satisfied_cm_name_mode(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        Some(ColumnMappingMode::Name),
        vec![TableFeature::ColumnMapping],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        None,
    )]
    #[case::all_satisfied_cm_id_mode(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        Some(ColumnMappingMode::Id),
        vec![TableFeature::ColumnMapping],
        vec![
            TableFeature::IcebergCompatV3,
            TableFeature::ColumnMapping,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
        ],
        None,
    )]
    fn test_iceberg_compat_v3_feature_requirements(
        #[case] props: &[(&str, &str)],
        #[case] cm_mode: Option<ColumnMappingMode>,
        #[case] reader_features: Vec<TableFeature>,
        #[case] writer_features: Vec<TableFeature>,
        #[case] expected_error_substring: Option<&str>,
    ) {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_for_column_mapping(cm_mode))
            .with_properties(props)
            .with_column_mapping(cm_mode)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features(&reader_features)
                    .with_writer_features(&writer_features)
                    .build(),
            )
            .build();
        let result = config.validate_feature_requirements(&TableFeature::IcebergCompatV3);
        match expected_error_substring {
            Some(msg) => assert_result_error_with_message(result, msg),
            None => assert!(result.is_ok(), "expected Ok, got {result:?}"),
        }
    }

    // `validate_feature_requirements` reads only `feature_requirements`, which is compiled
    // regardless of the `adaptive-metadata-in-dev` gate, so these cases run in the default build.
    // See the adaptiveMetadata RFC (delta-io/delta#6978) for the enablement rules being checked.
    #[rstest]
    #[case::all_satisfied(
        all_adaptive_metadata_props(),
        Some(ColumnMappingMode::Id),
        all_adaptive_metadata_deps(),
        None
    )]
    // Column mapping enabled but in `name` mode -> the `id`-mode Custom check fires.
    #[case::cm_name_mode_rejected(
        all_adaptive_metadata_props(),
        Some(ColumnMappingMode::Name),
        all_adaptive_metadata_deps(),
        Some("column mapping in 'id' mode")
    )]
    // Column mapping feature absent entirely -> the `Enabled(ColumnMapping)` arm fires first.
    #[case::column_mapping_not_supported(
        all_adaptive_metadata_props(),
        None,
        adaptive_metadata_deps_without(TableFeature::ColumnMapping),
        Some("requires 'columnMapping' to be enabled")
    )]
    #[case::row_tracking_not_enabled(
        adaptive_metadata_props_without(ENABLE_ROW_TRACKING),
        Some(ColumnMappingMode::Id),
        all_adaptive_metadata_deps(),
        Some("requires 'rowTracking' to be enabled")
    )]
    #[case::domain_metadata_not_supported(
        all_adaptive_metadata_props(),
        Some(ColumnMappingMode::Id),
        adaptive_metadata_deps_without(TableFeature::DomainMetadata),
        Some("requires 'domainMetadata' to be enabled")
    )]
    #[case::deletion_vectors_not_enabled(
        adaptive_metadata_props_without(ENABLE_DELETION_VECTORS),
        Some(ColumnMappingMode::Id),
        all_adaptive_metadata_deps(),
        Some("requires 'deletionVectors' to be enabled")
    )]
    #[case::in_commit_timestamp_not_enabled(
        adaptive_metadata_props_without(ENABLE_IN_COMMIT_TIMESTAMPS),
        Some(ColumnMappingMode::Id),
        all_adaptive_metadata_deps(),
        Some("requires 'inCommitTimestamp' to be enabled")
    )]
    fn test_adaptive_metadata_feature_requirements(
        #[case] props: Vec<(&str, &str)>,
        #[case] cm_mode: Option<ColumnMappingMode>,
        #[case] deps: Vec<TableFeature>,
        #[case] expected_error_substring: Option<&str>,
    ) {
        // Reader+writer features (adaptiveMetadata-preview itself, columnMapping, deletionVectors)
        // must appear in both protocol lists to count as supported.
        let reader_features: Vec<TableFeature> =
            std::iter::once(TableFeature::AdaptiveMetadataPreview)
                .chain(
                    deps.iter()
                        .filter(|f| f.feature_type() == FeatureType::ReaderWriter)
                        .cloned(),
                )
                .collect();
        let writer_features: Vec<TableFeature> =
            std::iter::once(TableFeature::AdaptiveMetadataPreview)
                .chain(deps.iter().cloned())
                .collect();
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_for_column_mapping(cm_mode))
            .with_properties(&props)
            .with_column_mapping(cm_mode)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features(&reader_features)
                    .with_writer_features(&writer_features)
                    .build(),
            )
            .build();
        let result = config.validate_feature_requirements(&TableFeature::AdaptiveMetadataPreview);
        match expected_error_substring {
            Some(msg) => assert_result_error_with_message(result, msg),
            None => assert!(result.is_ok(), "expected Ok, got {result:?}"),
        }
    }

    /// The table properties that enable adaptiveMetadata-preview's property-gated dependencies.
    fn all_adaptive_metadata_props() -> Vec<(&'static str, &'static str)> {
        vec![
            (ENABLE_ROW_TRACKING, "true"),
            (ENABLE_DELETION_VECTORS, "true"),
            (ENABLE_IN_COMMIT_TIMESTAMPS, "true"),
        ]
    }

    /// The adaptiveMetadata-preview enabling properties with `excluded` removed, to drive the
    /// "dependency not enabled" requirement checks.
    fn adaptive_metadata_props_without(excluded: &str) -> Vec<(&'static str, &'static str)> {
        all_adaptive_metadata_props()
            .into_iter()
            .filter(|(k, _)| *k != excluded)
            .collect()
    }

    /// The full set of adaptiveMetadata-preview dependency features.
    fn all_adaptive_metadata_deps() -> Vec<TableFeature> {
        vec![
            TableFeature::ColumnMapping,
            TableFeature::DeletionVectors,
            TableFeature::RowTracking,
            TableFeature::DomainMetadata,
            TableFeature::InCommitTimestamp,
        ]
    }

    /// The adaptiveMetadata-preview dependencies with `excluded` removed, to drive the
    /// "dependency not supported" requirement checks.
    fn adaptive_metadata_deps_without(excluded: TableFeature) -> Vec<TableFeature> {
        all_adaptive_metadata_deps()
            .into_iter()
            .filter(|f| *f != excluded)
            .collect()
    }

    // IcebergCompatV1/V2/V3 are pairwise mutually exclusive.
    #[rstest]
    #[case::v1_rejects_v2(
        TableFeature::IcebergCompatV1,
        TableFeature::IcebergCompatV2,
        "requires 'icebergCompatV2' to not be enabled"
    )]
    #[case::v1_rejects_v3(
        TableFeature::IcebergCompatV1,
        TableFeature::IcebergCompatV3,
        "requires 'icebergCompatV3' to not be enabled"
    )]
    #[case::v2_rejects_v1(
        TableFeature::IcebergCompatV2,
        TableFeature::IcebergCompatV1,
        "requires 'icebergCompatV1' to not be enabled"
    )]
    #[case::v2_rejects_v3(
        TableFeature::IcebergCompatV2,
        TableFeature::IcebergCompatV3,
        "requires 'icebergCompatV3' to not be enabled"
    )]
    #[case::v3_rejects_v1(
        TableFeature::IcebergCompatV3,
        TableFeature::IcebergCompatV1,
        "requires 'icebergCompatV1' to not be enabled"
    )]
    #[case::v3_rejects_v2(
        TableFeature::IcebergCompatV3,
        TableFeature::IcebergCompatV2,
        "requires 'icebergCompatV2' to not be enabled"
    )]
    fn test_iceberg_compat_mutual_exclusion(
        #[case] feature_to_enable: TableFeature,
        #[case] conflicting_feature: TableFeature,
        #[case] expected_error_substring: &str,
    ) {
        // Map each IcebergCompat feature to the table property that enables it.
        let conflicting_enable_property = match conflicting_feature {
            TableFeature::IcebergCompatV1 => ENABLE_ICEBERG_COMPAT_V1,
            TableFeature::IcebergCompatV2 => ENABLE_ICEBERG_COMPAT_V2,
            TableFeature::IcebergCompatV3 => ENABLE_ICEBERG_COMPAT_V3,
            ref other => panic!("unexpected feature in iceberg-compat exclusion test: {other:?}"),
        };
        // V3 also requires Column mapping and RowTracking enabled; enable them unconditionally so
        // V3 cases reach the mutual-exclusion check.
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_flat_with_column_mapping())
            .with_properties([
                (conflicting_enable_property, "true"),
                (ENABLE_ROW_TRACKING, "true"),
            ])
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features([TableFeature::ColumnMapping])
                    .with_writer_features([
                        feature_to_enable.clone(),
                        conflicting_feature,
                        TableFeature::ColumnMapping,
                        TableFeature::RowTracking,
                        TableFeature::DomainMetadata,
                    ])
                    .build(),
            )
            .build();
        assert_result_error_with_message(
            config.validate_feature_requirements(&feature_to_enable),
            expected_error_substring,
        );
    }

    /// `validate_feature_support_for_remove` must fire whenever row tracking is _supported_
    /// and not _suspended_, which is broader than _enabled_.
    #[rstest]
    #[case::supported_only(&[], Some("rowTracking"))]
    #[case::supported_and_enabled(&[(ENABLE_ROW_TRACKING, "true")], Some("rowTracking"))]
    #[case::supported_and_suspended(&[(ROW_TRACKING_SUSPENDED, "true")], None /*expected_error_substring */)]
    fn test_validate_feature_support_for_remove_row_tracking(
        #[case] props: &[(&str, &str)],
        #[case] expected_error_substring: Option<&str>,
    ) {
        let config = MockTableConfigurationBuilder::new()
            .with_properties(props)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::RowTracking])
                    .build(),
            )
            .build();
        let result = config.validate_feature_support_for_remove();
        match expected_error_substring {
            Some(msg) => assert_result_error_with_message(result, msg),
            None => assert!(result.is_ok(), "expected Ok, got {result:?}"),
        }
    }
}
