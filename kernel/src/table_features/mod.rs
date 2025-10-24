use std::collections::HashSet;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumCount, EnumString};

use crate::actions::Protocol;
use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::DataType;
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};
use delta_kernel_derive::internal_api;

pub(crate) use column_mapping::column_mapping_mode;
pub use column_mapping::{validate_schema_column_mapping, ColumnMappingMode};
pub(crate) use timestamp_ntz::validate_timestamp_ntz_feature_support;
mod column_mapping;
mod timestamp_ntz;

/// Table features communicate capabilities that must be implemented in order to correctly
/// read or write a given table. Features can be:
/// - Writer-only: Only affect write operations
/// - ReaderWriter: Affect both read and write operations (must appear in both feature lists)
///
/// The kernel currently supports most table features with some limitations.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    EnumString,
    StrumDisplay,
    AsRefStr,
    EnumCount,
    Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) enum TableFeature {
    // Writer-only features
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// ID Columns
    IdentityColumns,
    /// Monotonically increasing timestamps in the CommitInfo
    InCommitTimestamp,
    /// Row tracking on tables
    RowTracking,
    /// domain specific metadata
    DomainMetadata,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// Iceberg compatibility support
    IcebergCompatV2,
    /// The Clustered Table feature facilitates the physical clustering of rows
    /// that share similar values on a predefined set of clustering columns.
    #[strum(serialize = "clustering")]
    #[serde(rename = "clustering")]
    ClusteredTable,

    // ReaderWriter features
    /// CatalogManaged tables:
    /// <https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md>
    CatalogManaged,
    #[strum(serialize = "catalogOwned-preview")]
    #[serde(rename = "catalogOwned-preview")]
    CatalogOwnedPreview,
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[strum(serialize = "timestampNtz")]
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // Allow columns to change type
    TypeWidening,
    #[strum(serialize = "typeWidening-preview")]
    #[serde(rename = "typeWidening-preview")]
    TypeWideningPreview,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// vacuumProtocolCheck ReaderWriter feature ensures consistent application of reader and writer
    /// protocol checks during VACUUM operations
    VacuumProtocolCheck,
    /// This feature enables support for the variant data type, which stores semi-structured data.
    VariantType,
    #[strum(serialize = "variantType-preview")]
    #[serde(rename = "variantType-preview")]
    VariantTypePreview,
    #[strum(serialize = "variantShredding-preview")]
    #[serde(rename = "variantShredding-preview")]
    VariantShreddingPreview,

    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

/// Classifies table features by their type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeatureType {
    /// Feature only affects write operations
    Writer,
    /// Feature affects both read and write operations (must appear in both feature lists)
    ReaderWriter,
    /// Unknown feature type (for forward compatibility)
    Unknown,
}

/// Defines how a feature's enablement is determined
#[derive(Debug, Clone, Copy)]
pub(crate) enum EnablementCheck {
    /// Feature is enabled if it's supported (appears in protocol feature lists)
    AlwaysIfSupported,
    /// Feature is enabled if supported AND the provided function returns true when checking table properties
    EnabledIf(fn(&TableProperties) -> bool),
}

/// Rich metadata about a table feature including version requirements, dependencies, and support status
#[derive(Debug, Clone)]
pub(crate) struct FeatureInfo {
    /// The feature's canonical name as it appears in the protocol
    pub name: &'static str,
    /// Minimum reader protocol version required for this feature
    pub min_reader_version: i32,
    /// Minimum writer protocol version required for this feature
    pub min_writer_version: i32,
    /// The type of feature (Writer, ReaderWriter, or Unknown)
    pub feature_type: FeatureType,
    /// Other features that must be present for this feature to work
    pub required_features: &'static [TableFeature],
    /// Whether the Rust kernel has read support for this feature
    pub has_read_support: bool,
    /// Whether the Rust kernel has write support for this feature
    pub has_write_support: bool,
    /// How to check if this feature is enabled in a table
    pub enablement_check: EnablementCheck,
}

// Static FeatureInfo instances for each table feature
static APPEND_ONLY_INFO: FeatureInfo = FeatureInfo {
    name: "appendOnly",
    min_reader_version: 1,
    min_writer_version: 2,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: true,
    enablement_check: EnablementCheck::EnabledIf(|props| props.append_only == Some(true)),
};

static INVARIANTS_INFO: FeatureInfo = FeatureInfo {
    name: "invariants",
    min_reader_version: 1,
    min_writer_version: 2,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: true,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CHECK_CONSTRAINTS_INFO: FeatureInfo = FeatureInfo {
    name: "checkConstraints",
    min_reader_version: 1,
    min_writer_version: 3,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CHANGE_DATA_FEED_INFO: FeatureInfo = FeatureInfo {
    name: "changeDataFeed",
    min_reader_version: 1,
    min_writer_version: 4,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_change_data_feed == Some(true)
    }),
};

static GENERATED_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    name: "generatedColumns",
    min_reader_version: 1,
    min_writer_version: 4,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static IDENTITY_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    name: "identityColumns",
    min_reader_version: 1,
    min_writer_version: 6,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static IN_COMMIT_TIMESTAMP_INFO: FeatureInfo = FeatureInfo {
    name: "inCommitTimestamp",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: true,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_in_commit_timestamps == Some(true)
    }),
};

static ROW_TRACKING_INFO: FeatureInfo = FeatureInfo {
    name: "rowTracking",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    required_features: &[TableFeature::DomainMetadata],
    has_read_support: false,
    has_write_support: true,
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_row_tracking == Some(true)),
};

static DOMAIN_METADATA_INFO: FeatureInfo = FeatureInfo {
    name: "domainMetadata",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: true,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static ICEBERG_COMPAT_V1_INFO: FeatureInfo = FeatureInfo {
    name: "icebergCompatV1",
    min_reader_version: 1,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static ICEBERG_COMPAT_V2_INFO: FeatureInfo = FeatureInfo {
    name: "icebergCompatV2",
    min_reader_version: 2,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    required_features: &[TableFeature::ColumnMapping],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CLUSTERED_TABLE_INFO: FeatureInfo = FeatureInfo {
    name: "clustering",
    min_reader_version: 1,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    required_features: &[],
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CATALOG_MANAGED_INFO: FeatureInfo = FeatureInfo {
    name: "catalogManaged",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    #[cfg(feature = "catalog-managed")]
    has_read_support: true,
    #[cfg(not(feature = "catalog-managed"))]
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CATALOG_OWNED_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "catalogOwned-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    #[cfg(feature = "catalog-managed")]
    has_read_support: true,
    #[cfg(not(feature = "catalog-managed"))]
    has_read_support: false,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static COLUMN_MAPPING_INFO: FeatureInfo = FeatureInfo {
    name: "columnMapping",
    min_reader_version: 2,
    min_writer_version: 5,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: false,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.column_mapping_mode.is_some()
            && props.column_mapping_mode != Some(ColumnMappingMode::None)
    }),
};

static DELETION_VECTORS_INFO: FeatureInfo = FeatureInfo {
    name: "deletionVectors",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: true,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_deletion_vectors == Some(true)
    }),
};

static TIMESTAMP_WITHOUT_TIMEZONE_INFO: FeatureInfo = FeatureInfo {
    name: "timestampNtz",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: true,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static TYPE_WIDENING_INFO: FeatureInfo = FeatureInfo {
    name: "typeWidening",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static TYPE_WIDENING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "typeWidening-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static V2_CHECKPOINT_INFO: FeatureInfo = FeatureInfo {
    name: "v2Checkpoint",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VACUUM_PROTOCOL_CHECK_INFO: FeatureInfo = FeatureInfo {
    name: "vacuumProtocolCheck",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: false,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_TYPE_INFO: FeatureInfo = FeatureInfo {
    name: "variantType",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: true,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_TYPE_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "variantType-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: true,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_SHREDDING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "variantShredding-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    required_features: &[],
    has_read_support: true,
    has_write_support: true,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

impl TableFeature {
    /// Returns the feature type (Writer, ReaderWriter, or Unknown)
    pub(crate) fn feature_type(&self) -> FeatureType {
        match self {
            // Writer-only features
            TableFeature::AppendOnly => FeatureType::Writer,
            TableFeature::Invariants => FeatureType::Writer,
            TableFeature::CheckConstraints => FeatureType::Writer,
            TableFeature::ChangeDataFeed => FeatureType::Writer,
            TableFeature::GeneratedColumns => FeatureType::Writer,
            TableFeature::IdentityColumns => FeatureType::Writer,
            TableFeature::InCommitTimestamp => FeatureType::Writer,
            TableFeature::RowTracking => FeatureType::Writer,
            TableFeature::DomainMetadata => FeatureType::Writer,
            TableFeature::IcebergCompatV1 => FeatureType::Writer,
            TableFeature::IcebergCompatV2 => FeatureType::Writer,
            TableFeature::ClusteredTable => FeatureType::Writer,

            // ReaderWriter features
            TableFeature::CatalogManaged => FeatureType::ReaderWriter,
            TableFeature::CatalogOwnedPreview => FeatureType::ReaderWriter,
            TableFeature::ColumnMapping => FeatureType::ReaderWriter,
            TableFeature::DeletionVectors => FeatureType::ReaderWriter,
            TableFeature::TimestampWithoutTimezone => FeatureType::ReaderWriter,
            TableFeature::TypeWidening => FeatureType::ReaderWriter,
            TableFeature::TypeWideningPreview => FeatureType::ReaderWriter,
            TableFeature::V2Checkpoint => FeatureType::ReaderWriter,
            TableFeature::VacuumProtocolCheck => FeatureType::ReaderWriter,
            TableFeature::VariantType => FeatureType::ReaderWriter,
            TableFeature::VariantTypePreview => FeatureType::ReaderWriter,
            TableFeature::VariantShreddingPreview => FeatureType::ReaderWriter,

            // Unknown features
            TableFeature::Unknown(_) => FeatureType::Unknown,
        }
    }

    /// Returns rich metadata about this table feature including version requirements,
    /// dependencies, and support status. For Unknown features, returns None.
    pub(crate) fn info(&self) -> Option<&'static FeatureInfo> {
        match self {
            // Writer-only features
            TableFeature::AppendOnly => Some(&APPEND_ONLY_INFO),
            TableFeature::Invariants => Some(&INVARIANTS_INFO),
            TableFeature::CheckConstraints => Some(&CHECK_CONSTRAINTS_INFO),
            TableFeature::ChangeDataFeed => Some(&CHANGE_DATA_FEED_INFO),
            TableFeature::GeneratedColumns => Some(&GENERATED_COLUMNS_INFO),
            TableFeature::IdentityColumns => Some(&IDENTITY_COLUMNS_INFO),
            TableFeature::InCommitTimestamp => Some(&IN_COMMIT_TIMESTAMP_INFO),
            TableFeature::RowTracking => Some(&ROW_TRACKING_INFO),
            TableFeature::DomainMetadata => Some(&DOMAIN_METADATA_INFO),
            TableFeature::IcebergCompatV1 => Some(&ICEBERG_COMPAT_V1_INFO),
            TableFeature::IcebergCompatV2 => Some(&ICEBERG_COMPAT_V2_INFO),
            TableFeature::ClusteredTable => Some(&CLUSTERED_TABLE_INFO),

            // ReaderWriter features
            TableFeature::CatalogManaged => Some(&CATALOG_MANAGED_INFO),
            TableFeature::CatalogOwnedPreview => Some(&CATALOG_OWNED_PREVIEW_INFO),
            TableFeature::ColumnMapping => Some(&COLUMN_MAPPING_INFO),
            TableFeature::DeletionVectors => Some(&DELETION_VECTORS_INFO),
            TableFeature::TimestampWithoutTimezone => Some(&TIMESTAMP_WITHOUT_TIMEZONE_INFO),
            TableFeature::TypeWidening => Some(&TYPE_WIDENING_INFO),
            TableFeature::TypeWideningPreview => Some(&TYPE_WIDENING_PREVIEW_INFO),
            TableFeature::V2Checkpoint => Some(&V2_CHECKPOINT_INFO),
            TableFeature::VacuumProtocolCheck => Some(&VACUUM_PROTOCOL_CHECK_INFO),
            TableFeature::VariantType => Some(&VARIANT_TYPE_INFO),
            TableFeature::VariantTypePreview => Some(&VARIANT_TYPE_PREVIEW_INFO),
            TableFeature::VariantShreddingPreview => Some(&VARIANT_SHREDDING_PREVIEW_INFO),

            // Unknown features have no metadata
            TableFeature::Unknown(_) => None,
        }
    }
}

impl ToDataType for TableFeature {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<TableFeature> for Scalar {
    fn from(feature: TableFeature) -> Self {
        Scalar::String(feature.to_string())
    }
}

#[cfg(test)] // currently only used in tests
impl TableFeature {
    pub(crate) fn unknown(s: impl ToString) -> Self {
        TableFeature::Unknown(s.to_string())
    }
}

pub(crate) static SUPPORTED_READER_FEATURES: LazyLock<Vec<TableFeature>> = LazyLock::new(|| {
    vec![
        #[cfg(feature = "catalog-managed")]
        TableFeature::CatalogManaged,
        #[cfg(feature = "catalog-managed")]
        TableFeature::CatalogOwnedPreview,
        TableFeature::ColumnMapping,
        TableFeature::DeletionVectors,
        TableFeature::TimestampWithoutTimezone,
        TableFeature::TypeWidening,
        TableFeature::TypeWideningPreview,
        TableFeature::VacuumProtocolCheck,
        TableFeature::V2Checkpoint,
        TableFeature::VariantType,
        TableFeature::VariantTypePreview,
        // The default engine currently DOES NOT support shredded Variant reads and the parquet
        // reader will reject the read if it sees a shredded schema in the parquet file. That being
        // said, kernel does permit reconstructing shredded variants into the
        // `STRUCT<metadata: BINARY, value: BINARY>` representation if parquet readers of
        // third-party engines support it.
        TableFeature::VariantShreddingPreview,
    ]
});

/// The writer features have the following limitations:
/// - We 'support' Invariants only insofar as we check that they are not present.
/// - We support writing to tables that have Invariants enabled but not used.
/// - We only support DeletionVectors in that we never write them (no DML).
/// - We support writing to existing tables with row tracking, but we don't support creating
///   tables with row tracking yet.
pub(crate) static SUPPORTED_WRITER_FEATURES: LazyLock<Vec<TableFeature>> = LazyLock::new(|| {
    vec![
        TableFeature::AppendOnly,
        TableFeature::ColumnMapping,
        TableFeature::DeletionVectors,
        TableFeature::DomainMetadata,
        TableFeature::InCommitTimestamp,
        TableFeature::Invariants,
        TableFeature::RowTracking,
        TableFeature::TimestampWithoutTimezone,
        TableFeature::VariantType,
        TableFeature::VariantTypePreview,
        TableFeature::VariantShreddingPreview,
    ]
});

/// A unified interface for checking table feature support and enablement.
///
/// This struct combines Protocol and TableProperties to provide a single source of truth for:
/// - Whether a feature is supported (present in protocol and meets version requirements)
/// - Whether a feature is enabled (supported AND table properties indicate it's active)
/// - Validation of read/write support
/// - Dependency validation between features
pub(crate) struct TableFeatures<'a> {
    protocol: &'a Protocol,
    properties: &'a TableProperties,
}

impl<'a> TableFeatures<'a> {
    /// Creates a new TableFeatures instance for checking feature support and enablement
    pub(crate) fn new(protocol: &'a Protocol, properties: &'a TableProperties) -> Self {
        Self {
            protocol,
            properties,
        }
    }

    /// Checks if a feature is supported by the table.
    ///
    /// A feature is supported if:
    /// 1. For legacy protocol (reader < 3 or writer < 7): Protocol versions meet minimum requirements
    /// 2. For table features protocol (reader >= 3 and writer >= 7): Feature is explicitly listed
    ///    in the appropriate feature list (reader_features or writer_features)
    ///
    /// This does NOT check if the feature is enabled via table properties.
    pub(crate) fn is_supported(&self, feature: &TableFeature) -> bool {
        let Some(info) = feature.info() else {
            // Unknown features are not supported
            return false;
        };

        // Check if using legacy protocol or table features protocol
        let is_legacy_protocol =
            self.protocol.min_reader_version() < 3 || self.protocol.min_writer_version() < 7;

        if is_legacy_protocol {
            // For legacy protocol, check version requirements
            self.protocol.min_reader_version() >= info.min_reader_version
                && self.protocol.min_writer_version() >= info.min_writer_version
        } else {
            // For table features protocol, feature must be explicitly listed
            self.is_in_feature_lists(feature)
        }
    }

    /// Checks if a feature is enabled in the table.
    ///
    /// A feature is enabled if:
    /// 1. It is supported (see is_supported)
    /// 2. The enablement check passes (either AlwaysIfSupported or the property check returns true)
    pub(crate) fn is_enabled(&self, feature: &TableFeature) -> bool {
        if !self.is_supported(feature) {
            return false;
        }

        let Some(info) = feature.info() else {
            return false;
        };

        match info.enablement_check {
            EnablementCheck::AlwaysIfSupported => true,
            EnablementCheck::EnabledIf(check_fn) => check_fn(self.properties),
        }
    }

    /// Returns the set of all supported features for this table
    pub(crate) fn get_supported_features(&self) -> HashSet<TableFeature> {
        let mut features = HashSet::new();

        // Add all reader features if present
        if let Some(reader_features) = self.protocol.reader_features() {
            for feature in reader_features {
                if self.is_supported(feature) {
                    features.insert(feature.clone());
                }
            }
        }

        // Add all writer features if present
        if let Some(writer_features) = self.protocol.writer_features() {
            for feature in writer_features {
                if self.is_supported(feature) {
                    features.insert(feature.clone());
                }
            }
        }

        features
    }

    /// Helper method to check if a feature appears in the protocol's feature lists.
    /// This is used for table features protocol (min_reader >= 3 && min_writer >= 7).
    pub(crate) fn is_in_feature_lists(&self, feature: &TableFeature) -> bool {
        let feature_type = feature.feature_type();

        match feature_type {
            FeatureType::Writer => {
                // Writer-only features must appear in writer_features
                self.protocol
                    .writer_features()
                    .map(|features| features.contains(feature))
                    .unwrap_or(false)
            }
            FeatureType::ReaderWriter => {
                // ReaderWriter features must appear in both lists
                let in_reader = self
                    .protocol
                    .reader_features()
                    .map(|features| features.contains(feature))
                    .unwrap_or(false);
                let in_writer = self
                    .protocol
                    .writer_features()
                    .map(|features| features.contains(feature))
                    .unwrap_or(false);

                in_reader && in_writer
            }
            FeatureType::Unknown => false,
        }
    }

    /// Validates that the Rust kernel has read support for all features required by the table.
    ///
    /// Returns an error if any ReaderWriter feature lacks read support in the kernel.
    /// Writer-only features are not checked since they don't affect reading.
    pub(crate) fn validate_read_support(&self) -> DeltaResult<()> {
        let supported_features = self.get_supported_features();

        for feature in supported_features {
            if let Some(info) = feature.info() {
                // Only check ReaderWriter features for read support
                if matches!(info.feature_type, FeatureType::ReaderWriter) && !info.has_read_support
                {
                    return Err(Error::unsupported(format!(
                        "Table feature '{}' is not supported for reading",
                        feature
                    )));
                }
            }
        }

        Ok(())
    }

    /// Validates that the Rust kernel has write support for all features required by the table.
    ///
    /// Returns an error if any supported feature (Writer or ReaderWriter) lacks write support in the kernel.
    pub(crate) fn validate_write_support(&self) -> DeltaResult<()> {
        let supported_features = self.get_supported_features();

        for feature in supported_features {
            if let Some(info) = feature.info() {
                // Check both Writer and ReaderWriter features for write support
                if matches!(
                    info.feature_type,
                    FeatureType::Writer | FeatureType::ReaderWriter
                ) && !info.has_write_support
                {
                    return Err(Error::unsupported(format!(
                        "Table feature '{}' is not supported for writing",
                        feature
                    )));
                }
            }
        }

        Ok(())
    }

    /// Validates that all feature dependencies are satisfied.
    ///
    /// For each supported feature, checks that all its required features are also supported.
    /// Returns an error if any dependency is missing.
    pub(crate) fn validate_dependencies(&self) -> DeltaResult<()> {
        let supported_features = self.get_supported_features();

        for feature in &supported_features {
            if let Some(info) = feature.info() {
                for required_feature in info.required_features {
                    if !self.is_supported(required_feature) {
                        return Err(Error::generic(format!(
                            "Feature '{}' requires '{}' but it is not supported",
                            feature, required_feature
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_features() {
        let mixed_features = &[
            TableFeature::DeletionVectors,
            TableFeature::unknown("cool_feature"),
            TableFeature::ColumnMapping,
            TableFeature::AppendOnly,
        ];

        let serialized = serde_json::to_string(mixed_features).unwrap();

        assert_eq!(
            &serialized,
            "[\"deletionVectors\",\"cool_feature\",\"columnMapping\",\"appendOnly\"]"
        );

        let deserialized: Vec<TableFeature> = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.len(), 4);
        assert_eq!(&deserialized, mixed_features);
    }

    #[test]
    fn test_roundtrip_table_features() {
        let cases = [
            // Writer-only features
            (TableFeature::AppendOnly, "appendOnly"),
            (TableFeature::Invariants, "invariants"),
            (TableFeature::CheckConstraints, "checkConstraints"),
            (TableFeature::ChangeDataFeed, "changeDataFeed"),
            (TableFeature::GeneratedColumns, "generatedColumns"),
            (TableFeature::IdentityColumns, "identityColumns"),
            (TableFeature::InCommitTimestamp, "inCommitTimestamp"),
            (TableFeature::RowTracking, "rowTracking"),
            (TableFeature::DomainMetadata, "domainMetadata"),
            (TableFeature::IcebergCompatV1, "icebergCompatV1"),
            (TableFeature::IcebergCompatV2, "icebergCompatV2"),
            (TableFeature::ClusteredTable, "clustering"),
            // ReaderWriter features
            (TableFeature::CatalogManaged, "catalogManaged"),
            (TableFeature::CatalogOwnedPreview, "catalogOwned-preview"),
            (TableFeature::ColumnMapping, "columnMapping"),
            (TableFeature::DeletionVectors, "deletionVectors"),
            (TableFeature::TimestampWithoutTimezone, "timestampNtz"),
            (TableFeature::TypeWidening, "typeWidening"),
            (TableFeature::TypeWideningPreview, "typeWidening-preview"),
            (TableFeature::V2Checkpoint, "v2Checkpoint"),
            (TableFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (TableFeature::VariantType, "variantType"),
            (TableFeature::VariantTypePreview, "variantType-preview"),
            (
                TableFeature::VariantShreddingPreview,
                "variantShredding-preview",
            ),
            (TableFeature::unknown("something"), "something"),
        ];

        assert_eq!(TableFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: TableFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: TableFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }

    #[test]
    fn test_feature_info() {
        // Test that all known features return Some(FeatureInfo)
        assert!(TableFeature::AppendOnly.info().is_some());
        assert!(TableFeature::ColumnMapping.info().is_some());
        assert!(TableFeature::DeletionVectors.info().is_some());

        // Test that unknown features return None
        assert!(TableFeature::unknown("unknown").info().is_none());

        // Test specific feature info
        let dv_info = TableFeature::DeletionVectors.info().unwrap();
        assert_eq!(dv_info.name, "deletionVectors");
        assert_eq!(dv_info.min_reader_version, 3);
        assert_eq!(dv_info.min_writer_version, 7);
        assert_eq!(dv_info.feature_type, FeatureType::ReaderWriter);
        assert!(dv_info.has_read_support);
        assert!(dv_info.has_write_support);

        // Test feature with dependencies
        let row_tracking_info = TableFeature::RowTracking.info().unwrap();
        assert_eq!(row_tracking_info.required_features.len(), 1);
        assert_eq!(
            row_tracking_info.required_features[0],
            TableFeature::DomainMetadata
        );
    }

    mod table_features_tests {
        use super::*;
        use crate::actions::Protocol;

        fn make_legacy_protocol(min_reader: i32, min_writer: i32) -> Protocol {
            Protocol::try_new(
                min_reader,
                min_writer,
                None::<Vec<TableFeature>>,
                None::<Vec<TableFeature>>,
            )
            .unwrap()
        }

        fn make_table_features_protocol(
            reader_features: Vec<TableFeature>,
            writer_features: Vec<TableFeature>,
        ) -> Protocol {
            Protocol::try_new(3, 7, Some(reader_features), Some(writer_features)).unwrap()
        }

        #[test]
        fn test_legacy_protocol_support() {
            let protocol = make_legacy_protocol(1, 2);
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // AppendOnly requires (1, 2)
            assert!(features.is_supported(&TableFeature::AppendOnly));

            // DeletionVectors requires (3, 7)
            assert!(!features.is_supported(&TableFeature::DeletionVectors));
        }

        #[test]
        fn test_table_features_protocol_support() {
            let protocol = make_table_features_protocol(
                vec![TableFeature::DeletionVectors, TableFeature::ColumnMapping],
                vec![
                    TableFeature::DeletionVectors,
                    TableFeature::ColumnMapping,
                    TableFeature::AppendOnly,
                ],
            );
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // DeletionVectors is in both lists
            assert!(features.is_supported(&TableFeature::DeletionVectors));

            // AppendOnly is writer-only and in writer_features
            assert!(features.is_supported(&TableFeature::AppendOnly));

            // RowTracking is not in any list
            assert!(!features.is_supported(&TableFeature::RowTracking));

            // ColumnMapping is ReaderWriter and in both lists
            assert!(features.is_supported(&TableFeature::ColumnMapping));
        }

        #[test]
        fn test_is_enabled_always_if_supported() {
            let protocol = make_table_features_protocol(
                vec![TableFeature::DeletionVectors],
                vec![TableFeature::DeletionVectors],
            );
            let mut properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // DeletionVectors uses EnabledIf check, so needs property set
            assert!(!features.is_enabled(&TableFeature::DeletionVectors));

            // Set the property
            properties.enable_deletion_vectors = Some(true);
            let features = TableFeatures::new(&protocol, &properties);
            assert!(features.is_enabled(&TableFeature::DeletionVectors));
        }

        #[test]
        fn test_is_enabled_with_property_check() {
            let protocol = make_table_features_protocol(vec![], vec![TableFeature::AppendOnly]);
            let mut properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // AppendOnly is supported but not enabled
            assert!(features.is_supported(&TableFeature::AppendOnly));
            assert!(!features.is_enabled(&TableFeature::AppendOnly));

            // Enable it via property
            properties.append_only = Some(true);
            let features = TableFeatures::new(&protocol, &properties);
            assert!(features.is_enabled(&TableFeature::AppendOnly));
        }

        #[test]
        fn test_get_supported_features() {
            let protocol = make_table_features_protocol(
                vec![TableFeature::ColumnMapping],
                vec![
                    TableFeature::ColumnMapping,
                    TableFeature::AppendOnly,
                    TableFeature::DomainMetadata,
                ],
            );
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            let supported = features.get_supported_features();
            assert_eq!(supported.len(), 3);
            assert!(supported.contains(&TableFeature::ColumnMapping));
            assert!(supported.contains(&TableFeature::AppendOnly));
            assert!(supported.contains(&TableFeature::DomainMetadata));
        }

        #[test]
        fn test_validate_read_support() {
            let protocol = make_table_features_protocol(
                vec![TableFeature::DeletionVectors],
                vec![TableFeature::DeletionVectors],
            );
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // DeletionVectors has read support
            assert!(features.validate_read_support().is_ok());

            // CheckConstraints doesn't have read support
            let protocol =
                make_table_features_protocol(vec![], vec![TableFeature::CheckConstraints]);
            let features = TableFeatures::new(&protocol, &properties);
            assert!(features.validate_read_support().is_ok());
        }

        #[test]
        fn test_validate_write_support() {
            let protocol = make_table_features_protocol(vec![], vec![TableFeature::AppendOnly]);
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // AppendOnly has write support
            assert!(features.validate_write_support().is_ok());

            // CheckConstraints doesn't have write support
            let protocol =
                make_table_features_protocol(vec![], vec![TableFeature::CheckConstraints]);
            let features = TableFeatures::new(&protocol, &properties);
            assert!(features.validate_write_support().is_err());
        }

        #[test]
        fn test_validate_dependencies() {
            // RowTracking requires DomainMetadata
            let protocol = make_table_features_protocol(
                vec![],
                vec![TableFeature::RowTracking, TableFeature::DomainMetadata],
            );
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // Should pass because DomainMetadata is present
            assert!(features.validate_dependencies().is_ok());

            // Missing DomainMetadata should fail
            let protocol = make_table_features_protocol(vec![], vec![TableFeature::RowTracking]);
            let features = TableFeatures::new(&protocol, &properties);
            assert!(features.validate_dependencies().is_err());
        }

        #[test]
        fn test_is_in_feature_lists_writer_only() {
            let protocol = make_table_features_protocol(vec![], vec![TableFeature::AppendOnly]);
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            assert!(features.is_in_feature_lists(&TableFeature::AppendOnly));
            assert!(!features.is_in_feature_lists(&TableFeature::RowTracking));
        }

        #[test]
        fn test_is_in_feature_lists_reader_writer() {
            // ReaderWriter feature must be in both lists
            let protocol = make_table_features_protocol(
                vec![TableFeature::DeletionVectors],
                vec![TableFeature::DeletionVectors],
            );
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            assert!(features.is_in_feature_lists(&TableFeature::DeletionVectors));

            // Test that a feature not in any list returns false
            assert!(!features.is_in_feature_lists(&TableFeature::ColumnMapping));

            // Test writer-only feature in writer list
            let protocol = make_table_features_protocol(vec![], vec![TableFeature::AppendOnly]);
            let features = TableFeatures::new(&protocol, &properties);
            assert!(features.is_in_feature_lists(&TableFeature::AppendOnly));
        }

        #[test]
        fn test_unknown_feature_not_supported() {
            let protocol = make_table_features_protocol(
                vec![TableFeature::unknown("cool_feature")],
                vec![TableFeature::unknown("cool_feature")],
            );
            let properties = TableProperties::default();
            let features = TableFeatures::new(&protocol, &properties);

            // Unknown features are never supported
            assert!(!features.is_supported(&TableFeature::unknown("cool_feature")));
            assert!(!features.is_enabled(&TableFeature::unknown("cool_feature")));
        }
    }
}
