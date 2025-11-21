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

/// Table features represent protocol capabilities required to correctly read or write a given table.
/// - Readers must implement all features required for correct table reads.
/// - Writers must implement all features required for correct table writes.
///
/// Each variant corresponds to one such feature. A feature is either:
/// - **ReaderWriter** (must be supported by both readers and writers), or
/// - **Writer only** (applies only to writers).
/// There are no Reader only features. See `TableFeature::feature_type` for the category of each.
///
/// The kernel currently supports all reader features except `V2Checkpoint`.
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
    //////////////////////////
    // Writer-only features //
    //////////////////////////
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

    ///////////////////////////
    // ReaderWriter features //
    ///////////////////////////
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

/// ReaderWriter features that can be supported by legacy readers (min_reader_version < 3).
/// Only ColumnMapping qualifies with min_reader_version = 2.
pub(crate) static LEGACY_READER_FEATURES: [TableFeature; 1] = [TableFeature::ColumnMapping];

/// Writer and ReaderWriter features that can be supported by legacy writers (min_writer_version < 7).
/// These are features with min_writer_version in range [1, 6].
pub(crate) static LEGACY_WRITER_FEATURES: [TableFeature; 7] = [
    // Writer-only features (min_writer < 7)
    TableFeature::AppendOnly,       // min_writer = 2
    TableFeature::Invariants,       // min_writer = 2
    TableFeature::CheckConstraints, // min_writer = 3
    TableFeature::ChangeDataFeed,   // min_writer = 4
    TableFeature::GeneratedColumns, // min_writer = 4
    TableFeature::IdentityColumns,  // min_writer = 6
    // ReaderWriter features (min_writer < 7)
    TableFeature::ColumnMapping, // min_writer = 5
];

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
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum EnablementCheck {
    /// Feature is enabled if it's supported (appears in protocol feature lists)
    AlwaysIfSupported,
    /// Feature is enabled if supported AND the provided function returns true when checking table properties
    EnabledIf(fn(&TableProperties) -> bool),
}

/// Represents the type of data being accessed in an operation (used with both read and write)
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    /// Operations on regular table data
    Scan,
    /// Operations on change data feed data
    Cdf,
}

/// Defines whether the Rust kernel has implementation support for a feature's operation
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) enum KernelSupport {
    /// Kernel has full support for any operation on this feature
    Supported,
    /// Kernel does not support this operation on this feature
    NotSupported,
    /// Custom logic to determine support based on operation type and table properties.
    /// For example: Column Mapping may support Scan but not CDF, or CDF writes may only
    /// be supported when AppendOnly is true.
    Custom(fn(&Protocol, &TableProperties, Operation) -> DeltaResult<()>),
}

/// Types of requirements for feature dependencies
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum FeatureRequirement {
    /// Feature must be supported (in protocol)
    Supported(TableFeature),
    /// Feature must be enabled (supported + property set)
    Enabled(TableFeature),
    /// Feature must NOT be supported
    NotSupported(TableFeature),
    /// Feature must NOT be enabled (may be supported but property must not activate it)
    NotEnabled(TableFeature),
    /// Custom validation logic
    Custom(fn(&Protocol, &TableProperties) -> DeltaResult<()>),
}

/// Rich metadata about a table feature including version requirements, dependencies, and support status
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct FeatureInfo {
    /// The feature's canonical name as it appears in the protocol
    pub name: &'static str,
    /// Minimum reader protocol version required for this feature
    pub min_reader_version: i32,
    /// Minimum writer protocol version required for this feature
    pub min_writer_version: i32,
    /// The type of feature (Writer, ReaderWriter, or Unknown)
    pub feature_type: FeatureType,
    /// Requirements this feature has (features + custom validations)
    pub feature_requirements: &'static [FeatureRequirement],
    /// Rust kernel's read support for this feature (may vary by Operation type)
    pub read_support: KernelSupport,
    /// Rust kernel's write support for this feature (may vary by Operation type)
    pub write_support: KernelSupport,
    /// How to check if this feature is enabled in a table
    pub enablement_check: EnablementCheck,
}

// Static FeatureInfo instances for each table feature
#[allow(dead_code)]
static APPEND_ONLY_INFO: FeatureInfo = FeatureInfo {
    name: "appendOnly",
    min_reader_version: 1,
    min_writer_version: 2,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| props.append_only == Some(true)),
};

#[allow(dead_code)]
// Although kernel marks invariants as "Supported", invariants must NOT actually be present in the table schema.
// Kernel will fail to read/write any table that actually uses invariants (see check in TableConfiguration::ensure_write_supported).
// This is to allow legacy tables with the Invariants feature enabled but not in use.
static INVARIANTS_INFO: FeatureInfo = FeatureInfo {
    name: "invariants",
    min_reader_version: 1,
    min_writer_version: 2,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static CHECK_CONSTRAINTS_INFO: FeatureInfo = FeatureInfo {
    name: "checkConstraints",
    min_reader_version: 1,
    min_writer_version: 3,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::NotSupported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static CHANGE_DATA_FEED_INFO: FeatureInfo = FeatureInfo {
    name: "changeDataFeed",
    min_reader_version: 1,
    min_writer_version: 4,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_change_data_feed == Some(true)
    }),
};

#[allow(dead_code)]
static GENERATED_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    name: "generatedColumns",
    min_reader_version: 1,
    min_writer_version: 4,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::NotSupported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static IDENTITY_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    name: "identityColumns",
    min_reader_version: 1,
    min_writer_version: 6,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::NotSupported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static IN_COMMIT_TIMESTAMP_INFO: FeatureInfo = FeatureInfo {
    name: "inCommitTimestamp",
    min_reader_version: 1,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::Custom(|_protocol, _properties, operation| match operation {
        Operation::Scan => Ok(()),
        Operation::Cdf => Err(Error::unsupported(
            "CDF reads are not supported for tables with In-Commit Timestamps enabled",
        )),
    }),
    write_support: KernelSupport::Custom(|_protocol, _properties, operation| match operation {
        Operation::Scan => Ok(()),
        Operation::Cdf => Err(Error::unsupported(
            "CDF writes are not supported for tables with In-Commit Timestamps enabled",
        )),
    }),
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_in_commit_timestamps == Some(true)
    }),
};

#[allow(dead_code)]
static ROW_TRACKING_INFO: FeatureInfo = FeatureInfo {
    name: "rowTracking",
    min_reader_version: 1,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    feature_requirements: &[FeatureRequirement::Supported(TableFeature::DomainMetadata)],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_row_tracking == Some(true) && props.row_tracking_suspended != Some(true)
    }),
};

#[allow(dead_code)]
static DOMAIN_METADATA_INFO: FeatureInfo = FeatureInfo {
    name: "domainMetadata",
    min_reader_version: 1,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

// TODO(#1125): IcebergCompatV1 requires schema type validation to block Map, Array, and Void types.
// This validation is not yet implemented. The feature is marked as NotSupported for writes until proper validation is added.
// See Delta Spark: IcebergCompat.scala CheckNoListMapNullType (lines 422-433)
// See Java Kernel: IcebergWriterCompatMetadataValidatorAndUpdater.java UNSUPPORTED_TYPES_CHECK
// See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv1 for more requirements to support
#[allow(dead_code)]
static ICEBERG_COMPAT_V1_INFO: FeatureInfo = FeatureInfo {
    name: "icebergCompatV1",
    min_reader_version: 2,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    feature_requirements: &[
        FeatureRequirement::Enabled(TableFeature::ColumnMapping),
        FeatureRequirement::Custom(|_protocol, properties| {
            let mode = properties.column_mapping_mode;
            if !matches!(
                mode,
                Some(ColumnMappingMode::Name) | Some(ColumnMappingMode::Id)
            ) {
                return Err(Error::generic(
                    "IcebergCompatV1 requires Column Mapping in 'name' or 'id' mode",
                ));
            }
            Ok(())
        }),
        FeatureRequirement::NotSupported(TableFeature::DeletionVectors),
    ],
    read_support: KernelSupport::NotSupported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v1 == Some(true)
    }),
};

// TODO(#1125): IcebergCompatV2 requires schema type validation. Unlike V1, V2 allows Map and Array
// types but needs validation against an allowlist of supported types.
// This validation is not yet implemented. The feature is marked as NotSupported for writes until proper validation is added.
// See Delta Spark: IcebergCompat.scala CheckTypeInV2AllowList (lines 450-459)
// See Java Kernel: IcebergCompatMetadataValidatorAndUpdater.java V2_SUPPORTED_TYPES
// See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv2 for more requirements to support.
#[allow(dead_code)]
static ICEBERG_COMPAT_V2_INFO: FeatureInfo = FeatureInfo {
    name: "icebergCompatV2",
    min_reader_version: 2,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    feature_requirements: &[
        FeatureRequirement::Enabled(TableFeature::ColumnMapping),
        FeatureRequirement::Custom(|_protocol, properties| {
            let mode = properties.column_mapping_mode;
            if !matches!(
                mode,
                Some(ColumnMappingMode::Name) | Some(ColumnMappingMode::Id)
            ) {
                return Err(Error::generic(
                    "IcebergCompatV2 requires Column Mapping in 'name' or 'id' mode",
                ));
            }
            Ok(())
        }),
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV1),
        FeatureRequirement::NotEnabled(TableFeature::DeletionVectors),
    ],
    read_support: KernelSupport::NotSupported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v2 == Some(true)
    }),
};

#[allow(dead_code)]
static CLUSTERED_TABLE_INFO: FeatureInfo = FeatureInfo {
    name: "clustering",
    min_reader_version: 1,
    min_writer_version: 7,
    feature_type: FeatureType::Writer,
    feature_requirements: &[FeatureRequirement::Supported(TableFeature::DomainMetadata)],
    read_support: KernelSupport::NotSupported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static CATALOG_MANAGED_INFO: FeatureInfo = FeatureInfo {
    name: "catalogManaged",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    #[cfg(feature = "catalog-managed")]
    read_support: KernelSupport::Supported,
    #[cfg(not(feature = "catalog-managed"))]
    read_support: KernelSupport::NotSupported,
    #[cfg(feature = "catalog-managed")]
    write_support: KernelSupport::Supported,
    #[cfg(not(feature = "catalog-managed"))]
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static CATALOG_OWNED_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "catalogOwned-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    #[cfg(feature = "catalog-managed")]
    read_support: KernelSupport::Supported,
    #[cfg(not(feature = "catalog-managed"))]
    read_support: KernelSupport::NotSupported,
    #[cfg(feature = "catalog-managed")]
    write_support: KernelSupport::Supported,
    #[cfg(not(feature = "catalog-managed"))]
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static COLUMN_MAPPING_INFO: FeatureInfo = FeatureInfo {
    name: "columnMapping",
    min_reader_version: 2,
    min_writer_version: 5,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.column_mapping_mode.is_some()
            && props.column_mapping_mode != Some(ColumnMappingMode::None)
    }),
};

#[allow(dead_code)]
static DELETION_VECTORS_INFO: FeatureInfo = FeatureInfo {
    name: "deletionVectors",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    // We support writing to tables with DeletionVectors enabled, but we never write DV files
    // ourselves (no DML). The kernel only performs append operations.
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_deletion_vectors == Some(true)
    }),
};

#[allow(dead_code)]
static TIMESTAMP_WITHOUT_TIMEZONE_INFO: FeatureInfo = FeatureInfo {
    name: "timestampNtz",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static TYPE_WIDENING_INFO: FeatureInfo = FeatureInfo {
    name: "typeWidening",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_type_widening == Some(true)),
};

#[allow(dead_code)]
static TYPE_WIDENING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "typeWidening-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_type_widening == Some(true)),
};

#[allow(dead_code)]
static V2_CHECKPOINT_INFO: FeatureInfo = FeatureInfo {
    name: "v2Checkpoint",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static VACUUM_PROTOCOL_CHECK_INFO: FeatureInfo = FeatureInfo {
    name: "vacuumProtocolCheck",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static VARIANT_TYPE_INFO: FeatureInfo = FeatureInfo {
    name: "variantType",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static VARIANT_TYPE_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "variantType-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

#[allow(dead_code)]
static VARIANT_SHREDDING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    name: "variantShredding-preview",
    min_reader_version: 3,
    min_writer_version: 7,
    feature_type: FeatureType::ReaderWriter,
    feature_requirements: &[],
    read_support: KernelSupport::Supported,
    write_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

impl TableFeature {
    pub(crate) fn feature_type(&self) -> FeatureType {
        match self {
            TableFeature::CatalogManaged
            | TableFeature::CatalogOwnedPreview
            | TableFeature::ColumnMapping
            | TableFeature::DeletionVectors
            | TableFeature::TimestampWithoutTimezone
            | TableFeature::TypeWidening
            | TableFeature::TypeWideningPreview
            | TableFeature::V2Checkpoint
            | TableFeature::VacuumProtocolCheck
            | TableFeature::VariantType
            | TableFeature::VariantTypePreview
            | TableFeature::VariantShreddingPreview => FeatureType::ReaderWriter,
            TableFeature::AppendOnly
            | TableFeature::DomainMetadata
            | TableFeature::Invariants
            | TableFeature::RowTracking
            | TableFeature::CheckConstraints
            | TableFeature::ChangeDataFeed
            | TableFeature::GeneratedColumns
            | TableFeature::IdentityColumns
            | TableFeature::InCommitTimestamp
            | TableFeature::IcebergCompatV1
            | TableFeature::IcebergCompatV2
            | TableFeature::ClusteredTable => FeatureType::Writer,
            TableFeature::Unknown(_) => FeatureType::Unknown,
        }
    }

    /// Returns rich metadata about this table feature including version requirements,
    /// dependencies, and support status. For Unknown features, returns None.
    #[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_features() {
        let mixed_reader = &[
            TableFeature::DeletionVectors,
            TableFeature::unknown("cool_feature"),
            TableFeature::ColumnMapping,
        ];
        let mixed_writer = &[
            TableFeature::DeletionVectors,
            TableFeature::unknown("cool_feature"),
            TableFeature::AppendOnly,
        ];

        let reader_string = serde_json::to_string(mixed_reader).unwrap();
        let writer_string = serde_json::to_string(mixed_writer).unwrap();

        assert_eq!(
            &reader_string,
            "[\"deletionVectors\",\"cool_feature\",\"columnMapping\"]"
        );
        assert_eq!(
            &writer_string,
            "[\"deletionVectors\",\"cool_feature\",\"appendOnly\"]"
        );

        let typed_reader: Vec<TableFeature> = serde_json::from_str(&reader_string).unwrap();
        let typed_writer: Vec<TableFeature> = serde_json::from_str(&writer_string).unwrap();

        assert_eq!(typed_reader.len(), 3);
        assert_eq!(&typed_reader, mixed_reader);
        assert_eq!(typed_writer.len(), 3);
        assert_eq!(&typed_writer, mixed_writer);
    }

    #[test]
    fn test_roundtrip_table_features() {
        let cases = [
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
    fn test_roundtrip_writer_features() {
        let cases = [
            (TableFeature::AppendOnly, "appendOnly"),
            (TableFeature::CatalogManaged, "catalogManaged"),
            (TableFeature::CatalogOwnedPreview, "catalogOwned-preview"),
            (TableFeature::Invariants, "invariants"),
            (TableFeature::CheckConstraints, "checkConstraints"),
            (TableFeature::ChangeDataFeed, "changeDataFeed"),
            (TableFeature::GeneratedColumns, "generatedColumns"),
            (TableFeature::ColumnMapping, "columnMapping"),
            (TableFeature::IdentityColumns, "identityColumns"),
            (TableFeature::InCommitTimestamp, "inCommitTimestamp"),
            (TableFeature::DeletionVectors, "deletionVectors"),
            (TableFeature::RowTracking, "rowTracking"),
            (TableFeature::TimestampWithoutTimezone, "timestampNtz"),
            (TableFeature::TypeWidening, "typeWidening"),
            (TableFeature::TypeWideningPreview, "typeWidening-preview"),
            (TableFeature::DomainMetadata, "domainMetadata"),
            (TableFeature::V2Checkpoint, "v2Checkpoint"),
            (TableFeature::IcebergCompatV1, "icebergCompatV1"),
            (TableFeature::IcebergCompatV2, "icebergCompatV2"),
            (TableFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (TableFeature::ClusteredTable, "clustering"),
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
}
