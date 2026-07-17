#[internal_api]
pub(crate) use column_mapping::get_any_level_column_physical_name;
#[deprecated = "Enable internal-api and use TableConfiguration instead"]
pub use column_mapping::validate_schema_column_mapping;
pub use column_mapping::ColumnMappingMode;
#[internal_api]
pub(crate) use column_mapping::{assign_column_mapping_metadata, find_max_column_id_in_schema};
pub(crate) use column_mapping::{
    column_mapping_mode, get_column_mapping_mode_from_properties, physical_to_logical_column_name,
    schema_has_column_mapping_metadata, strip_stray_column_mapping_metadata,
    try_assign_flat_column_mapping_info, validate_and_extract_column_mapping_annotations,
    validate_column_mapping_id, StaleAnnotationPolicy,
};
use delta_kernel_derive::internal_api;
#[cfg(feature = "column-defaults-in-dev")]
pub(crate) use iceberg_compat::v3::iceberg_compat_v3_column_defaults_validation;
pub(crate) use iceberg_compat::v3::V3_VALIDATOR;
pub(crate) use iceberg_compat::validate_iceberg_compat_if_needed;
pub(crate) use interval_type::{
    schema_contains_interval_type, validate_interval_type_feature_support_on_write,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumCount, EnumIter, EnumString};
pub(crate) use timestamp_ntz::{
    schema_contains_timestamp_ntz, validate_timestamp_ntz_feature_support,
};

use crate::actions::Protocol;
use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::DataType;
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Error};

mod column_mapping;
mod iceberg_compat;
mod interval_type;
mod timestamp_ntz;

/// Minimum reader/writer protocol version that the kernel can handle.
pub const MIN_VALID_RW_VERSION: i32 = 1;

/// Maximum reader protocol version that the kernel can handle.
pub const MAX_VALID_READER_VERSION: i32 = 3;

/// Maximum writer protocol version that the kernel can handle.
pub const MAX_VALID_WRITER_VERSION: i32 = 7;

/// Minimum reader version for tables that use table features.
/// When set to 3, the protocol requires an explicit `readerFeatures` array.
pub const TABLE_FEATURES_MIN_READER_VERSION: i32 = 3;

/// Minimum writer version for tables that use table features.
/// When set to 7, the protocol requires an explicit `writerFeatures` array.
pub const TABLE_FEATURES_MIN_WRITER_VERSION: i32 = 7;

/// Prefix for table feature override properties.
/// Properties with this prefix (e.g., `delta.feature.deletionVectors`) are used to
/// explicitly turn on support for the feature in the protocol.
pub const SET_TABLE_FEATURE_SUPPORTED_PREFIX: &str = "delta.feature.";

/// Value to add support for a table feature when used with [`SET_TABLE_FEATURE_SUPPORTED_PREFIX`].
/// Example: `"delta.feature.deletionVectors" -> "supported"`
pub const SET_TABLE_FEATURE_SUPPORTED_VALUE: &str = "supported";

/// Table features represent protocol capabilities required to correctly read or write a given
/// table.
/// - Readers must implement all features required for correct table reads.
/// - Writers must implement all features required for correct table writes.
///
/// Each variant corresponds to one such feature. A feature is either:
/// - **ReaderWriter** (must be supported by both readers and writers), or
/// - **WriterOnly** (applies only to writers).
/// There are no ReaderOnly features. See `TableFeature::feature_type` for the category of each.
///
/// The kernel currently supports all reader features.
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
#[derive(EnumIter)]
// ^^ We must derive EnumIter only after internal_api adjusts visibility. Otherwise, internal-api
// builds will fail because the now-public `TableFeature::iter()` returns a pub(crate) type.
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
    /// Iceberg V1 compatibility support
    IcebergCompatV1,
    /// Iceberg V2 compatibility support
    IcebergCompatV2,
    /// Iceberg V3 compatibility support
    IcebergCompatV3,
    /// The Clustered Table feature facilitates the physical clustering of rows
    /// that share similar values on a predefined set of clustering columns.
    #[strum(serialize = "clustering")]
    #[serde(rename = "clustering")]
    ClusteredTable,
    /// Materialize partition columns in parquet data files.
    MaterializePartitionColumns,
    /// Column Default Values.
    ///
    /// TODO(#2630): column-defaults is not fully supported yet. Kernel support is gated by
    /// the `column-defaults-in-dev` cargo feature.
    AllowColumnDefaults,

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
    /// ANSI interval types, in preview pending RFC ratification (`intervalType-preview`).
    ///
    /// TODO(#2840): intervalType support is gated by the `interval-type-in-dev` cargo feature.
    /// Connectors may enable this protocol feature explicitly. It is not auto-enabled from schema
    /// contents so tables created for legacy interoperability remain readable by connectors that
    /// predate the feature.
    #[strum(serialize = "intervalType-preview")]
    #[serde(rename = "intervalType-preview")]
    IntervalTypePreview,
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
    /// vacuumProtocolCheck ReaderWriter feature ensures consistent application of reader and
    /// writer protocol checks during VACUUM operations
    VacuumProtocolCheck,
    /// This feature enables support for the variant data type, which stores semi-structured data.
    VariantType,
    #[strum(serialize = "variantType-preview")]
    #[serde(rename = "variantType-preview")]
    VariantTypePreview,
    VariantShredding,
    #[strum(serialize = "variantShredding-preview")]
    #[serde(rename = "variantShredding-preview")]
    VariantShreddingPreview,
    /// Iceberg V4 adaptive metadata tree as the table's native content metadata format.
    ///
    /// TODO(#2866): gated by the `adaptive-metadata-in-dev` cargo feature until fully supported.
    #[strum(serialize = "adaptiveMetadata-preview")]
    #[serde(rename = "adaptiveMetadata-preview")]
    AdaptiveMetadataPreview,

    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

/// ReaderWriter features that can be supported by legacy readers (min_reader_version < 3).
/// Only ColumnMapping qualifies with min_reader_version = 2.
pub(crate) static LEGACY_READER_FEATURES: [TableFeature; 1] = [TableFeature::ColumnMapping];

/// Writer and ReaderWriter features that can be supported by legacy writers (min_writer_version <
/// 7). These are features with min_writer_version in range [1, 6].
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
    WriterOnly,
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
    /// Feature is enabled if supported AND the provided function returns true when checking table
    /// properties
    EnabledIf(fn(&TableProperties) -> bool),
}

/// Represents the type of operation being performed on a table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) enum Operation {
    /// Read operations on regular table data
    Scan,
    /// Read operations on change data feed data
    Cdf,
    /// Write operations on regular table data
    Write,
}

/// Defines whether the Rust kernel has implementation support for a feature's operation
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
#[derive(Debug)]
pub(crate) enum FeatureRequirement {
    /// Feature must be supported (in protocol)
    Supported(TableFeature),
    /// Feature must be enabled (supported + property set)
    Enabled(TableFeature),
    /// Feature must NOT be supported
    NotSupported(TableFeature),
    /// Feature must NOT be enabled (may be supported but property must not activate it)
    NotEnabled(TableFeature),
    /// Custom validation logic run against the protocol and table properties.
    Custom(fn(&Protocol, &TableProperties) -> DeltaResult<()>),
}

/// Minimum protocol versions for legacy (pre-feature-list) inference.
pub(crate) struct MinReaderWriterVersion {
    pub reader: i32,
    pub writer: i32,
}

impl MinReaderWriterVersion {
    pub(crate) const fn new(reader: i32, writer: i32) -> Self {
        Self { reader, writer }
    }
}

/// Rich metadata about a table feature including version requirements, dependencies, and support
/// status
pub(crate) struct FeatureInfo {
    /// The type of feature (WriterOnly, ReaderWriter, or Unknown)
    pub feature_type: FeatureType,
    /// Minimum legacy protocol versions for version-based feature inference.
    /// `Some` for features that predate feature lists and can be inferred from protocol version.
    /// `None` for features that require explicit feature lists (reader v3+ / writer v7+).
    pub min_legacy_version: Option<MinReaderWriterVersion>,
    /// Requirements this feature has (features + custom validations)
    pub feature_requirements: &'static [FeatureRequirement],
    /// Rust kernel's support for this feature (may vary by Operation type)
    ///
    /// Note: `kernel_support` validation depends on `feature_type`:
    /// WriterOnly features: Only checked during `Operation::Write`
    /// ReaderWriter features: Checked during all operations (Scan/Write/CDF)
    /// Read operations (Scan/CDF) only validate reader features, so `kernel_support` for
    /// WriterOnly features are never invoked for Scan/CDF regardless of the custom check logic.
    pub kernel_support: KernelSupport,
    /// How to check if this feature is enabled in a table
    pub enablement_check: EnablementCheck,
}

// Static FeatureInfo instances for each table feature
static APPEND_ONLY_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 2)),
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| props.append_only == Some(true)),
};

// Although kernel marks invariants as "Supported", invariants must NOT actually be present in the
// table schema. Kernel will fail to write to any table that actually uses invariants (see check in
// TableConfiguration::ensure_write_supported). This is to allow legacy tables with the Invariants
// feature enabled but not in use.
static INVARIANTS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 2)),
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CHECK_CONSTRAINTS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 3)),
    feature_requirements: &[],
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CHANGE_DATA_FEED_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 4)),
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_change_data_feed == Some(true)
    }),
};

static GENERATED_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 4)),
    feature_requirements: &[],
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static IDENTITY_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 6)),
    feature_requirements: &[],
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static IN_COMMIT_TIMESTAMP_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Custom(|_protocol, _properties, operation| match operation {
        Operation::Scan | Operation::Write | Operation::Cdf => Ok(()),
    }),
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_in_commit_timestamps == Some(true)
    }),
};

// TODO(#2538): Currently we reject `Transaction::commit` when it contains staged remove-file
// actions on RowTracking-supported (and not-suspended) tables because
//   1. kernel does not yet materialize stable row IDs / commit versions on write, which blocks COW
//      rewrites,
//   2. kernel does not yet validate if remove actions correctly reserved row IDs / commit versions.
// Unblock after both 1 and 2 are supported.
//
// TODO: When kernel writes the materialized `row_id` / `row_commit_version` columns, they must
// use the reserved parquet field IDs defined by the protocol on IcebergCompatV3 tables, not
// auto-assigned IDs.
static ROW_TRACKING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[FeatureRequirement::Supported(TableFeature::DomainMetadata)],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_row_tracking == Some(true) && props.row_tracking_suspended != Some(true)
    }),
};

static DOMAIN_METADATA_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

// TODO(#1125): IcebergCompatV1 requires schema type validation to block Map, Array, and Void types.
// This validation is not yet implemented. The feature is marked as NotSupported for writes until
// proper validation is added.
//
// See Delta Spark: IcebergCompat.scala CheckNoListMapNullType (lines 422-433)
// See Java Kernel: IcebergWriterCompatMetadataValidatorAndUpdater.java
// UNSUPPORTED_TYPES_CHECK See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv1 for more requirements to support
static ICEBERG_COMPAT_V1_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[
        FeatureRequirement::Enabled(TableFeature::ColumnMapping),
        FeatureRequirement::NotSupported(TableFeature::DeletionVectors),
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV2),
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV3),
    ],
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v1 == Some(true)
    }),
};

// TODO(#1125): IcebergCompatV2 requires schema type validation. Unlike V1, V2 allows Map and Array
// types but needs validation against an allowlist of supported types.
// This validation is not yet implemented. The feature is marked as NotSupported for writes until
// proper validation is added.

// See Delta Spark: IcebergCompat.scala CheckTypeInV2AllowList (lines 450-459)
// See Java Kernel: IcebergCompatMetadataValidatorAndUpdater.java V2_SUPPORTED_TYPES
// See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv2 for more requirements to support.
static ICEBERG_COMPAT_V2_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[
        FeatureRequirement::Enabled(TableFeature::ColumnMapping),
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV1),
        FeatureRequirement::NotEnabled(TableFeature::DeletionVectors),
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV3),
    ],
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v2 == Some(true)
    }),
};

/// IcebergCompatV3 ensures tables can be converted to Apache Iceberg V3.
///
/// Spec: <https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-compat-v3.md>
///
/// TODO: Implement the write-side requirements for IcebergCompatV3.
/// TODO: Support ALTER TABLE on tables with IcebergCompatV3 enabled.
///
/// Attention in the future:
/// - Geo types: when supported, they must not be usable as partition columns on IcebergCompatV3
///   tables.
/// - REPLACE TABLE: when supported, partition columns must not change across the replace.
/// - Timestamp parquet encoding: when kernel can write INT96 or INT64, IcebergCompatV3 tables must
///   always use INT64; INT96 is forbidden.
/// - ALTER TABLE SET/UNSET TBLPROPERTIES: when supported, reject any property change that would
///   disable IcebergCompatV3 on an existing table.
/// - Void type: when delta-spark supports VOID type on icebergCompatV3 tables, add it to V3's type
///   allowlist (`is_v3_supported_type` in `iceberg_compat::v3`).
///
/// Tracking issue: <https://github.com/delta-io/delta-kernel-rs/issues/2492>
static ICEBERG_COMPAT_V3_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[
        FeatureRequirement::Enabled(TableFeature::ColumnMapping),
        FeatureRequirement::Enabled(TableFeature::RowTracking),
        // Unlike V1/V2, V3 intentionally permits DeletionVectors per the RFC. No
        // `NotEnabled(DeletionVectors)` requirement is needed.
        //
        // V1/V2 may remain in `writerFeatures` (supported) as long as they are not active,
        // hence `NotEnabled` rather than `NotSupported`.
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV1),
        FeatureRequirement::NotEnabled(TableFeature::IcebergCompatV2),
    ],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v3 == Some(true)
    }),
};

static CLUSTERED_TABLE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[FeatureRequirement::Supported(TableFeature::DomainMetadata)],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static MATERIALIZE_PARTITION_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

// TODO(#2630): drop the gate once column-defaults is fully supported.
static ALLOW_COLUMN_DEFAULTS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    #[cfg(feature = "column-defaults-in-dev")]
    kernel_support: KernelSupport::Supported,
    #[cfg(not(feature = "column-defaults-in-dev"))]
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CATALOG_MANAGED_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[FeatureRequirement::Enabled(TableFeature::InCommitTimestamp)],
    kernel_support: KernelSupport::Custom(|_, _, op| match op {
        Operation::Scan | Operation::Write => Ok(()),
        Operation::Cdf => Err(Error::unsupported(
            "Feature 'catalogManaged' is not supported for CDF",
        )),
    }),
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CATALOG_OWNED_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[FeatureRequirement::Enabled(TableFeature::InCommitTimestamp)],
    kernel_support: KernelSupport::Custom(|_, _, op| match op {
        Operation::Scan | Operation::Write => Ok(()),
        Operation::Cdf => Err(Error::unsupported(
            "Feature 'catalogOwned-preview' is not supported for CDF",
        )),
    }),
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static COLUMN_MAPPING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: Some(MinReaderWriterVersion::new(2, 5)),
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.column_mapping_mode.is_some()
            && props.column_mapping_mode != Some(ColumnMappingMode::None)
    }),
};

static DELETION_VECTORS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    // The kernel can read DV-bearing tables and install connector-authored DV descriptors via
    // `Transaction::update_deletion_vectors`, including through the FFI
    // `transaction_update_deletion_vectors` path.
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_deletion_vectors == Some(true)
    }),
};

static TIMESTAMP_WITHOUT_TIMEZONE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static INTERVAL_TYPE_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    #[cfg(feature = "interval-type-in-dev")]
    kernel_support: KernelSupport::Supported,
    #[cfg(not(feature = "interval-type-in-dev"))]
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

/// TODO: When type widening is supported on writes, restrict the allowed
/// widenings on IcebergCompatV3 tables to the subset permitted by the Iceberg v3
/// schema-evolution rules. Ref: <https://iceberg.apache.org/spec/#schema-evolution>
static TYPE_WIDENING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Custom(|_, _, op| match op {
        Operation::Scan | Operation::Cdf => Ok(()),
        Operation::Write => Err(Error::unsupported(
            "Feature 'typeWidening' is not supported for writes",
        )),
    }),
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_type_widening == Some(true)),
};

/// TODO: When type widening is supported on writes, restrict the allowed
/// widenings on IcebergCompatV3 tables to the subset permitted by the Iceberg
/// schema-evolution rules. Ref: <https://iceberg.apache.org/spec/#schema-evolution>
static TYPE_WIDENING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Custom(|_, _, op| match op {
        Operation::Scan | Operation::Cdf => Ok(()),
        Operation::Write => Err(Error::unsupported(
            "Feature 'typeWidening-preview' is not supported for writes",
        )),
    }),
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_type_widening == Some(true)),
};

static V2_CHECKPOINT_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VACUUM_PROTOCOL_CHECK_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_TYPE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_TYPE_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_SHREDDING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_SHREDDING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

// Dependencies per the adaptiveMetadata RFC (delta-io/delta#6978) "Table Feature Enablement"
// section. Enforcement is covered by `test_adaptive_metadata_feature_requirements`.
// TODO(#2866): drop the `adaptive-metadata-in-dev` gate once adaptiveMetadata is fully supported.
static ADAPTIVE_METADATA_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[
        FeatureRequirement::Enabled(TableFeature::ColumnMapping),
        FeatureRequirement::Custom(|_protocol, properties| {
            require!(
                properties.column_mapping_mode == Some(ColumnMappingMode::Id),
                Error::invalid_protocol(
                    "Feature 'adaptiveMetadata-preview' requires column mapping in 'id' mode"
                )
            );
            Ok(())
        }),
        FeatureRequirement::Enabled(TableFeature::RowTracking),
        FeatureRequirement::Enabled(TableFeature::DomainMetadata),
        FeatureRequirement::Enabled(TableFeature::DeletionVectors),
        FeatureRequirement::Enabled(TableFeature::InCommitTimestamp),
    ],
    #[cfg(feature = "adaptive-metadata-in-dev")]
    kernel_support: KernelSupport::Supported,
    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

/// By definition, kernel cannot know how to handle unknown features and must assume they're always
/// enabled if supported in protocol. However, the read path ignores all writer-only features,
/// including unknown ones. Unknown features are never inferred from legacy protocol versions.
static UNKNOWN_FEATURE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::Unknown,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::NotSupported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

impl TableFeature {
    #[cfg(test)]
    pub(crate) const NO_LIST: Option<Vec<TableFeature>> = None;
    #[cfg(test)]
    pub(crate) const EMPTY_LIST: Vec<TableFeature> = vec![];

    pub(crate) fn feature_type(&self) -> FeatureType {
        match self {
            TableFeature::CatalogManaged
            | TableFeature::CatalogOwnedPreview
            | TableFeature::ColumnMapping
            | TableFeature::DeletionVectors
            | TableFeature::IntervalTypePreview
            | TableFeature::TimestampWithoutTimezone
            | TableFeature::TypeWidening
            | TableFeature::TypeWideningPreview
            | TableFeature::V2Checkpoint
            | TableFeature::VacuumProtocolCheck
            | TableFeature::VariantType
            | TableFeature::VariantTypePreview
            | TableFeature::VariantShredding
            | TableFeature::VariantShreddingPreview
            | TableFeature::AdaptiveMetadataPreview => FeatureType::ReaderWriter,
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
            | TableFeature::IcebergCompatV3
            | TableFeature::ClusteredTable
            | TableFeature::MaterializePartitionColumns => FeatureType::WriterOnly,
            TableFeature::AllowColumnDefaults => FeatureType::WriterOnly,
            TableFeature::Unknown(_) => FeatureType::Unknown,
        }
    }

    /// Returns true if this feature can be inferred from a legacy reader protocol version.
    /// Always returns false for modern features (use feature lists instead).
    pub(crate) fn is_valid_for_legacy_reader(&self, reader_version: i32) -> bool {
        matches!(&self.info().min_legacy_version, Some(v) if reader_version >= v.reader)
    }

    /// Returns true if this feature can be inferred from a legacy writer protocol version.
    /// Always returns false for modern features (use feature lists instead).
    pub(crate) fn is_valid_for_legacy_writer(&self, writer_version: i32) -> bool {
        matches!(&self.info().min_legacy_version, Some(v) if writer_version >= v.writer)
    }

    /// Returns rich metadata about this table feature including protocol version requirements,
    /// dependencies, and kernel support status.
    pub(crate) fn info(&self) -> &FeatureInfo {
        match self {
            // Writer-only features
            TableFeature::AppendOnly => &APPEND_ONLY_INFO,
            TableFeature::Invariants => &INVARIANTS_INFO,
            TableFeature::CheckConstraints => &CHECK_CONSTRAINTS_INFO,
            TableFeature::ChangeDataFeed => &CHANGE_DATA_FEED_INFO,
            TableFeature::GeneratedColumns => &GENERATED_COLUMNS_INFO,
            TableFeature::IdentityColumns => &IDENTITY_COLUMNS_INFO,
            TableFeature::InCommitTimestamp => &IN_COMMIT_TIMESTAMP_INFO,
            TableFeature::RowTracking => &ROW_TRACKING_INFO,
            TableFeature::DomainMetadata => &DOMAIN_METADATA_INFO,
            TableFeature::IcebergCompatV1 => &ICEBERG_COMPAT_V1_INFO,
            TableFeature::IcebergCompatV2 => &ICEBERG_COMPAT_V2_INFO,
            TableFeature::IcebergCompatV3 => &ICEBERG_COMPAT_V3_INFO,
            TableFeature::ClusteredTable => &CLUSTERED_TABLE_INFO,
            TableFeature::MaterializePartitionColumns => &MATERIALIZE_PARTITION_COLUMNS_INFO,
            TableFeature::AllowColumnDefaults => &ALLOW_COLUMN_DEFAULTS_INFO,

            // ReaderWriter features
            TableFeature::CatalogManaged => &CATALOG_MANAGED_INFO,
            TableFeature::CatalogOwnedPreview => &CATALOG_OWNED_PREVIEW_INFO,
            TableFeature::ColumnMapping => &COLUMN_MAPPING_INFO,
            TableFeature::DeletionVectors => &DELETION_VECTORS_INFO,
            TableFeature::IntervalTypePreview => &INTERVAL_TYPE_PREVIEW_INFO,
            TableFeature::TimestampWithoutTimezone => &TIMESTAMP_WITHOUT_TIMEZONE_INFO,
            TableFeature::TypeWidening => &TYPE_WIDENING_INFO,
            TableFeature::TypeWideningPreview => &TYPE_WIDENING_PREVIEW_INFO,
            TableFeature::V2Checkpoint => &V2_CHECKPOINT_INFO,
            TableFeature::VacuumProtocolCheck => &VACUUM_PROTOCOL_CHECK_INFO,
            TableFeature::VariantType => &VARIANT_TYPE_INFO,
            TableFeature::VariantTypePreview => &VARIANT_TYPE_PREVIEW_INFO,
            TableFeature::VariantShredding => &VARIANT_SHREDDING_INFO,
            TableFeature::VariantShreddingPreview => &VARIANT_SHREDDING_PREVIEW_INFO,
            TableFeature::AdaptiveMetadataPreview => &ADAPTIVE_METADATA_PREVIEW_INFO,

            // Unknown features: not supported by kernel, no legacy version inference.
            TableFeature::Unknown(_) => &UNKNOWN_FEATURE_INFO,
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

impl From<String> for TableFeature {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}

impl From<&TableFeature> for TableFeature {
    fn from(value: &TableFeature) -> Self {
        value.clone()
    }
}

/// Formats a slice of table features using Delta's standard serialization (camelCase).
pub(crate) fn format_features(features: &[TableFeature]) -> String {
    let feature_strings: Vec<&str> = features.iter().map(|f| f.as_ref()).collect_vec();
    format!("[{}]", feature_strings.join(", "))
}

/// Extract the reader features enabled for `protocol`. For `min_reader_version == 3` returns the
/// explicit `reader_features` list; for `1..=2` returns the legacy-inferred features.
pub(crate) fn extract_enabled_reader_features(protocol: &Protocol) -> Vec<TableFeature> {
    match protocol.min_reader_version() {
        TABLE_FEATURES_MIN_READER_VERSION => protocol
            .reader_features()
            .map(|f| f.to_vec())
            .unwrap_or_default(),
        v if (1..=2).contains(&v) => LEGACY_READER_FEATURES
            .iter()
            .filter(|f| f.is_valid_for_legacy_reader(v))
            .cloned()
            .collect(),
        _ => Vec::new(),
    }
}

/// Add `feature` to the appropriate feature list(s) for its type, skipping duplicates.
pub(crate) fn add_feature_to_lists(
    feature: TableFeature,
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
) {
    match feature.feature_type() {
        FeatureType::ReaderWriter => {
            if !reader_features.contains(&feature) {
                reader_features.push(feature.clone());
            }
            if !writer_features.contains(&feature) {
                writer_features.push(feature);
            }
        }
        FeatureType::WriterOnly | FeatureType::Unknown => {
            if !writer_features.contains(&feature) {
                writer_features.push(feature);
            }
        }
    }
}

/// Enable each `allowed_table_features` entry whose [`EnablementCheck::EnabledIf`] check is
/// satisfied by `table_properties`, appending it to `reader_features`/`writer_features`
/// (deduplicated). Features with [`EnablementCheck::AlwaysIfSupported`] are skipped since they need
/// no property-driven enablement. `RowTracking` additionally pulls in its `DomainMetadata`
/// dependency.
pub(crate) fn auto_enable_property_driven_features(
    allowed_table_features: &[TableFeature],
    table_properties: &TableProperties,
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
) {
    for table_feature in allowed_table_features {
        if let EnablementCheck::EnabledIf(check) = table_feature.info().enablement_check {
            if check(table_properties) {
                add_feature_to_lists(table_feature.clone(), reader_features, writer_features);
                if *table_feature == TableFeature::RowTracking {
                    add_feature_to_lists(
                        TableFeature::DomainMetadata,
                        reader_features,
                        writer_features,
                    );
                }
            }
        }
    }
}

/// Enforce that `protocol.min_reader_version()` lies within
/// [`MIN_VALID_RW_VERSION`]..=[`MAX_VALID_READER_VERSION`]. Below the minimum yields
/// [`Error::InvalidProtocol`]; above the maximum yields [`Error::Unsupported`].
pub(crate) fn check_reader_version_range(protocol: &Protocol) -> DeltaResult<()> {
    require!(
        protocol.min_reader_version() >= MIN_VALID_RW_VERSION,
        Error::InvalidProtocol(format!(
            "min_reader_version must be >= {MIN_VALID_RW_VERSION}, got {}",
            protocol.min_reader_version()
        ))
    );
    if protocol.min_reader_version() > MAX_VALID_READER_VERSION {
        return Err(Error::unsupported(format!(
            "Unsupported minimum reader version {}",
            protocol.min_reader_version()
        )));
    }
    Ok(())
}

/// Protocol-level check that the kernel can read tables governed by `protocol`.
///
/// Unlike `TableConfiguration::ensure_operation_supported`, this does not require a
/// `Metadata` action or any table properties.
pub(crate) fn ensure_table_can_be_read(protocol: &Protocol) -> DeltaResult<()> {
    check_reader_version_range(protocol)?;

    for feature in extract_enabled_reader_features(protocol) {
        match feature.info().kernel_support {
            KernelSupport::Supported => {}
            KernelSupport::NotSupported => {
                return Err(Error::unsupported(format!(
                    "Feature '{feature}' is not supported by kernel",
                )));
            }
            KernelSupport::Custom(_) => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

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

    /// Expected outcome of `ensure_table_can_be_read` for a given protocol: either readable,
    /// or an error of a specific variant.
    enum ExpectRead {
        Ok,
        InvalidProtocol,
        Unsupported,
    }

    #[rstest]
    #[case::reader_version_below_minimum(
        Protocol::new_unchecked(0, 1, None, None),
        ExpectRead::InvalidProtocol
    )]
    #[case::reader_version_above_maximum(
        Protocol::new_unchecked(99, 1, None, None),
        ExpectRead::Unsupported
    )]
    #[case::legacy_reader_v1(Protocol::try_new_legacy(1, 1).unwrap(), ExpectRead::Ok)]
    #[case::legacy_reader_v2(Protocol::try_new_legacy(2, 5).unwrap(), ExpectRead::Ok)]
    #[case::v3_empty_reader_features(
        Protocol::new_unchecked(3, 7, Some(vec![]), Some(vec![])),
        ExpectRead::Ok
    )]
    #[case::supported_explicit_feature(
        Protocol::try_new_modern(
            [TableFeature::DeletionVectors],
            [TableFeature::DeletionVectors],
        )
        .unwrap(),
        ExpectRead::Ok
    )]
    #[case::unknown_reader_feature(
        Protocol::try_new_modern(
            [TableFeature::unknown("notARealFeature")],
            [TableFeature::unknown("notARealFeature")],
        )
        .unwrap(),
        ExpectRead::Unsupported
    )]
    #[case::custom_support_feature(
        Protocol::try_new_modern(
            [TableFeature::CatalogManaged],
            [TableFeature::CatalogManaged],
        )
        .unwrap(),
        ExpectRead::Ok
    )]
    // adaptiveMetadata-preview is gated by the `adaptive-metadata-in-dev` cargo feature: readable
    // only when the flag is on, otherwise rejected as unsupported.
    #[cfg_attr(
        feature = "adaptive-metadata-in-dev",
        case::adaptive_metadata_supported(
            Protocol::try_new_modern(
                [TableFeature::AdaptiveMetadataPreview],
                [TableFeature::AdaptiveMetadataPreview],
            )
            .unwrap(),
            ExpectRead::Ok
        )
    )]
    #[cfg_attr(
        not(feature = "adaptive-metadata-in-dev"),
        case::adaptive_metadata_gated_off(
            Protocol::try_new_modern(
                [TableFeature::AdaptiveMetadataPreview],
                [TableFeature::AdaptiveMetadataPreview],
            )
            .unwrap(),
            ExpectRead::Unsupported
        )
    )]
    fn validate_protocol_for_read(#[case] protocol: Protocol, #[case] expected: ExpectRead) {
        let result = ensure_table_can_be_read(&protocol);
        match expected {
            ExpectRead::Ok => result.expect("protocol must be readable"),
            ExpectRead::InvalidProtocol => assert!(
                matches!(result, Err(Error::InvalidProtocol(_))),
                "expected InvalidProtocol, got: {result:?}"
            ),
            ExpectRead::Unsupported => assert!(
                matches!(result, Err(Error::Unsupported(_))),
                "expected Unsupported, got: {result:?}"
            ),
        }
    }

    /// A table that declares `intervalType-preview` in its reader features is readable exactly when
    /// kernel support is compiled in (the `interval-type-in-dev` gate); otherwise the read is
    /// refused.
    #[test]
    fn test_read_protocol_with_interval_type_feature() {
        let protocol = Protocol::try_new_modern(
            [TableFeature::IntervalTypePreview],
            [TableFeature::IntervalTypePreview],
        )
        .unwrap();
        let result = ensure_table_can_be_read(&protocol);
        if cfg!(feature = "interval-type-in-dev") {
            result
                .expect("intervalType-preview table must be readable when the feature is enabled");
        } else {
            assert!(
                matches!(result, Err(Error::Unsupported(_))),
                "intervalType-preview read must be Unsupported when the feature is off, got: {result:?}"
            );
        }
    }

    #[test]
    fn test_roundtrip_table_features() {
        use strum::IntoEnumIterator as _;

        for feature in TableFeature::iter() {
            let expected = match feature {
                TableFeature::AppendOnly => "appendOnly",
                TableFeature::Invariants => "invariants",
                TableFeature::CheckConstraints => "checkConstraints",
                TableFeature::ChangeDataFeed => "changeDataFeed",
                TableFeature::GeneratedColumns => "generatedColumns",
                TableFeature::IdentityColumns => "identityColumns",
                TableFeature::InCommitTimestamp => "inCommitTimestamp",
                TableFeature::RowTracking => "rowTracking",
                TableFeature::DomainMetadata => "domainMetadata",
                TableFeature::IcebergCompatV1 => "icebergCompatV1",
                TableFeature::IcebergCompatV2 => "icebergCompatV2",
                TableFeature::IcebergCompatV3 => "icebergCompatV3",
                TableFeature::ClusteredTable => "clustering",
                TableFeature::MaterializePartitionColumns => "materializePartitionColumns",
                TableFeature::CatalogManaged => "catalogManaged",
                TableFeature::CatalogOwnedPreview => "catalogOwned-preview",
                TableFeature::ColumnMapping => "columnMapping",
                TableFeature::DeletionVectors => "deletionVectors",
                TableFeature::IntervalTypePreview => "intervalType-preview",
                TableFeature::TimestampWithoutTimezone => "timestampNtz",
                TableFeature::TypeWidening => "typeWidening",
                TableFeature::TypeWideningPreview => "typeWidening-preview",
                TableFeature::V2Checkpoint => "v2Checkpoint",
                TableFeature::VacuumProtocolCheck => "vacuumProtocolCheck",
                TableFeature::VariantType => "variantType",
                TableFeature::VariantTypePreview => "variantType-preview",
                TableFeature::VariantShredding => "variantShredding",
                TableFeature::VariantShreddingPreview => "variantShredding-preview",
                TableFeature::AdaptiveMetadataPreview => "adaptiveMetadata-preview",
                TableFeature::AllowColumnDefaults => "allowColumnDefaults",
                TableFeature::Unknown(_) => continue, // tested in test_unknown_features
            };

            // strum
            assert_eq!(feature.to_string(), expected);
            assert_eq!(feature, TableFeature::from(expected));

            // json
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: TableFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);
        }
    }
}
