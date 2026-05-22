#[internal_api]
pub(crate) use column_mapping::get_any_level_column_physical_name;
#[deprecated = "Enable internal-api and use TableConfiguration instead"]
pub use column_mapping::validate_schema_column_mapping;
pub use column_mapping::ColumnMappingMode;
pub(crate) use column_mapping::{
    assign_column_mapping_metadata, column_mapping_mode, find_max_column_id_in_schema,
    get_column_mapping_mode_from_properties, physical_to_logical_column_name,
    try_assign_flat_column_mapping_info, validate_and_extract_column_mapping_annotations,
    validate_column_mapping_id,
};
use delta_kernel_derive::internal_api;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumCount, EnumIter, EnumString};
pub(crate) use timestamp_ntz::{
    schema_contains_timestamp_ntz, validate_timestamp_ntz_feature_support,
};

use crate::actions::Protocol;
use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::{schema_has_invariants, DataType};
use crate::table_properties::TableProperties;
use crate::DeltaResult;

mod column_mapping;
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
#[strum(
    serialize_all = "camelCase",
    parse_err_fn = xxx__not_needed__default_variant_means_parsing_is_infallible__xxx,
    parse_err_ty = Infallible // ignored, sadly: https://github.com/Peternator7/strum/issues/430
)]
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

/// Read sub-operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) enum ReadOp {
    /// Regular table data read (scan).
    Scan,
    /// Change data feed read.
    Cdf,
}

/// Data-writing sub-operations. A "data write" commits add and/or remove file actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) enum DataWriteOp {
    /// Append-only data write: only add file actions are staged (no removes), with
    /// `data_change=true`. Also covers metadata-only commits routed through the data-write
    /// path (e.g. `SetTransaction` / `DomainMetadata`-only commits with no file actions);
    /// those have no file-level effect and are treated as Append for gating purposes.
    /// Schema-modifying DDL (`AlterTable`) goes through `Operation::Ddl`, not this op.
    Append,
    /// DML data write (UPDATE / DELETE / MERGE-shaped commit, with remove actions or
    /// mixed add+remove with `data_change=true`).
    Dml,
    /// Maintenance (any data-preserving commit with `data_change=false`). Covers OPTIMIZE
    /// and ZORDER file compaction, statistics updates, row-id / row-commit-version
    /// backfill, and any other file-action commit that does not change the logical
    /// row content of the table.
    Maintenance,
}

/// DDL (schema-modifying) sub-operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[internal_api]
pub(crate) enum DdlOp {
    /// Add a new top-level column.
    AddColumn,
    /// Relax a column from NOT NULL to nullable.
    SetNullable,
    /// Drop an existing column.
    DropColumn,
    /// Rename an existing column.
    RenameColumn,
}

/// Represents the type of operation being performed on a table.
///
/// CREATE is intentionally not represented here. CREATE needs an "origin" distinction
/// (user opt-in vs kernel inference) that the unified operation dispatcher does not have;
/// see [`ensure_create_supported`](crate::table_configuration::TableConfiguration::ensure_create_supported).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) enum Operation {
    /// Read path operations.
    Read(ReadOp),
    /// Data-writing operations (commits that produce add/remove file actions).
    DataWrite(DataWriteOp),
    /// Schema-modifying (DDL) operations.
    Ddl(DdlOp),
}

/// Distinguishes how a feature came to be in the protocol at CREATE time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[internal_api]
pub(crate) enum CreatePath {
    /// The user explicitly opted in (e.g. via `delta.feature.X=supported`, or a property
    /// that drives auto-enablement like `delta.enableRowTracking=true`).
    UserOptIn,
    /// The kernel inferred the feature was needed (e.g. from a `timestampNtz` schema
    /// column, a clustering layout, or a dependency cascade).
    KernelInference,
}

/// Per-cell policy for Read / DataWrite / DDL operations.
///
/// On rejection every variant produces an error of the form `"Feature 'X': <msg>"` where
/// `<msg>` is the cell's static suffix and the dispatcher (`apply_cell`) prepends the
/// feature name.
pub(crate) enum OpSupport {
    /// Operation is allowed.
    Allowed,
    /// Cell is not reachable from the dispatcher under the current design. Used at the
    /// definition site to make intent explicit (e.g. the `read.scan` / `read.cdf` cells on
    /// a writer-only feature, which are never iterated because the read path only walks
    /// reader features). The dispatcher treats `NotApplicable` as `Allowed` at runtime;
    /// debug builds `debug_assert!(false)` to fail loud if a refactor ever reaches this
    /// cell.
    NotApplicable,
    /// Blocks whenever the feature is in the protocol (writerFeatures / readerFeatures),
    /// regardless of active state. Use when kernel cannot safely handle the op even when
    /// the feature is "idle". The `&'static str` is the error message suffix.
    ForbiddenIfSupported(&'static str),
    /// Blocks only when the feature is active per its [`EnablementCheck`] (i.e. the
    /// relevant property is set). The `&'static str` is the error message suffix.
    ForbiddenIfEnabled(&'static str),
    /// Blocks when the predicate returns `true`. Use for schema- or property-state-dependent
    /// checks that can't be expressed as `ForbiddenIfSupported`/`ForbiddenIfEnabled`. The
    /// `&'static str` is the error message suffix. Used sparingly.
    ForbiddenIf {
        msg: &'static str,
        predicate: fn(&crate::table_configuration::TableConfiguration) -> bool,
    },
}

/// Per-cell policy for the CREATE path. CREATE has its own origin (user opt-in vs kernel
/// inference) and therefore a different cell enum from [`OpSupport`].
pub(crate) enum CreateSupport {
    /// Feature may land in protocol via either path (user opt-in or kernel inference).
    Allowed,
    /// Feature may only land via kernel inference (e.g. schema-implied, dep cascade,
    /// or `with_data_layout`). User opt-in is rejected.
    AllowedOnlyViaKernelInference(&'static str),
    /// Feature cannot land in protocol via any path.
    Forbidden(&'static str),
}

/// Read-side policy for a feature.
pub(crate) struct ReadSupport {
    pub(crate) scan: OpSupport,
    pub(crate) cdf: OpSupport,
}

impl ReadSupport {
    pub(crate) const ALL_ALLOWED: Self = Self {
        scan: OpSupport::Allowed,
        cdf: OpSupport::Allowed,
    };

    /// For writer-only features whose `read` cells are unreachable. The dispatcher only
    /// iterates reader features for read ops, so a writer-only feature's read cells should
    /// never run. Definition-site documentation only; runtime behavior is `Allowed` with a
    /// `debug_assert!(false)` in `apply_cell` if the invariant ever breaks.
    pub(crate) const NOT_APPLICABLE: Self = Self {
        scan: OpSupport::NotApplicable,
        cdf: OpSupport::NotApplicable,
    };
}

/// Data-write policy for a feature.
pub(crate) struct DataWriteSupport {
    pub(crate) append: OpSupport,
    pub(crate) dml: OpSupport,
    pub(crate) maintenance: OpSupport,
}

impl DataWriteSupport {
    pub(crate) const ALL_ALLOWED: Self = Self {
        append: OpSupport::Allowed,
        dml: OpSupport::Allowed,
        maintenance: OpSupport::Allowed,
    };

    /// All data-write cells forbidden whenever the feature is supported by the protocol.
    pub(crate) const fn all_forbidden_if_supported(msg: &'static str) -> Self {
        Self {
            append: OpSupport::ForbiddenIfSupported(msg),
            dml: OpSupport::ForbiddenIfSupported(msg),
            maintenance: OpSupport::ForbiddenIfSupported(msg),
        }
    }

    /// Restrict data writes to Append-only when the feature is active. DML (UPDATE / DELETE /
    /// MERGE) and Maintenance (OPTIMIZE, stats updates, row-id backfill, etc.) are forbidden;
    /// blind append remains allowed.
    pub(crate) const fn restrict_to_appends_when_enabled(msg: &'static str) -> Self {
        Self {
            append: OpSupport::Allowed,
            dml: OpSupport::ForbiddenIfEnabled(msg),
            maintenance: OpSupport::ForbiddenIfEnabled(msg),
        }
    }
}

/// DDL (schema-modifying) policy for a feature.
pub(crate) struct DdlSupport {
    pub(crate) add_column: OpSupport,
    pub(crate) set_nullable: OpSupport,
    pub(crate) drop_column: OpSupport,
    pub(crate) rename_column: OpSupport,
}

impl DdlSupport {
    pub(crate) const ALL_ALLOWED: Self = Self {
        add_column: OpSupport::Allowed,
        set_nullable: OpSupport::Allowed,
        drop_column: OpSupport::Allowed,
        rename_column: OpSupport::Allowed,
    };

    /// All DDL cells forbidden whenever the feature is supported by the protocol.
    pub(crate) const fn all_forbidden_if_supported(msg: &'static str) -> Self {
        Self {
            add_column: OpSupport::ForbiddenIfSupported(msg),
            set_nullable: OpSupport::ForbiddenIfSupported(msg),
            drop_column: OpSupport::ForbiddenIfSupported(msg),
            rename_column: OpSupport::ForbiddenIfSupported(msg),
        }
    }

    /// All DDL cells forbidden whenever the feature is active.
    pub(crate) const fn all_forbidden_if_enabled(msg: &'static str) -> Self {
        Self {
            add_column: OpSupport::ForbiddenIfEnabled(msg),
            set_nullable: OpSupport::ForbiddenIfEnabled(msg),
            drop_column: OpSupport::ForbiddenIfEnabled(msg),
            rename_column: OpSupport::ForbiddenIfEnabled(msg),
        }
    }
}

/// Full per-feature operation support matrix. Every (feature, operation) pair has a
/// dedicated cell, so the question "does feature X support operation Y?" answers as a
/// single field lookup.
pub(crate) struct OperationSupport {
    pub(crate) read: ReadSupport,
    pub(crate) data_write: DataWriteSupport,
    pub(crate) ddl: DdlSupport,
    pub(crate) create: CreateSupport,
}

impl OperationSupport {
    pub(crate) const ALL_ALLOWED: Self = Self {
        read: ReadSupport::ALL_ALLOWED,
        data_write: DataWriteSupport::ALL_ALLOWED,
        ddl: DdlSupport::ALL_ALLOWED,
        create: CreateSupport::Allowed,
    };
}

/// `ForbiddenIf` predicate for the Invariants cells: returns `true` when the schema actually
/// carries `delta.invariants` metadata on any field. The static feature presence in the
/// protocol alone does not block writes; only an in-use invariant does.
fn schema_has_invariants_predicate(
    table_config: &crate::table_configuration::TableConfiguration,
) -> bool {
    schema_has_invariants(table_config.logical_schema().as_ref())
}

/// `ForbiddenIf` predicate for [`ROW_TRACKING_INFO`]'s remove-producing cells.
///
/// Fires whenever the table may carry row IDs that kernel cannot preserve through the
/// commit. Broader than `is_feature_enabled`: it fires whenever row tracking is supported
/// by the protocol AND not suspended, even if `delta.enableRowTracking` is `false`. See
/// [`crate::table_configuration::TableConfiguration::should_write_row_tracking`] for the
/// authoritative definition. Kernel cannot yet preserve stable row IDs across removes
/// (#2538), so the predicate conservatively reports `true` whenever the table may have
/// row IDs.
fn row_tracking_supported_and_not_suspended_predicate(
    table_config: &crate::table_configuration::TableConfiguration,
) -> bool {
    table_config.should_write_row_tracking()
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
    /// Custom validation logic. Currently unused, but already integrated into the
    /// validation pipeline(`TableConfiguration::validate_feature_requirements`), so kept for
    /// future use.
    #[allow(dead_code)]
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
    /// Per-operation support matrix for this feature.
    ///
    /// Read cells (`read.scan`, `read.cdf`) only apply to ReaderWriter features; for
    /// WriterOnly features they are unreachable because the read path iterates only
    /// reader features. DataWrite and DDL cells apply to both WriterOnly and ReaderWriter
    /// features.
    pub operation_support: OperationSupport,
    /// How to check if this feature is enabled in a table
    pub enablement_check: EnablementCheck,
}

// === Shared error message suffixes ===
//
// The dispatcher prepends "Feature 'X': " to every error message, so per-cell strings
// here are *suffixes* describing the failure, not full sentences.
mod msg {
    pub(super) const NOT_SUPPORTED: &str = "is not supported";
    pub(super) const NOT_SUPPORTED_FOR_WRITES: &str = "is not supported for writes";
    pub(super) const NOT_SUPPORTED_FOR_CDF: &str = "is not supported for CDF";
    pub(super) const CREATE_FORBIDDEN: &str = "cannot be enabled at table create time";
    pub(super) const CREATE_FORBIDDEN_KERNEL_CANNOT_WRITE: &str =
        "cannot be enabled at table create time: kernel cannot write to tables with this feature";
    pub(super) const CREATE_KERNEL_INFERENCE_ONLY_SCHEMA: &str =
        "is enabled by schema column type; do not set the corresponding delta.feature.* signal at create time";
    pub(super) const CREATE_KERNEL_INFERENCE_ONLY_CLUSTERING: &str =
        "is enabled by with_data_layout(clustered(...)); do not set delta.feature.clustering at create time";
    pub(super) const CATALOG_OWNED_PREVIEW_DEPRECATED_FOR_CREATE: &str =
        "is deprecated for CREATE TABLE; use 'catalogManaged' instead";

    pub(super) const CDF_DML_NOT_YET_SUPPORTED: &str =
        "CDF writes not yet supported for UPDATE/DELETE/MERGE-shaped commits";
    pub(super) const INVARIANTS_IN_SCHEMA: &str = "Column invariants are not yet supported";
    pub(super) const ROW_TRACKING_BLOCKS_REMOVES: &str =
        "Remove actions are not yet supported when rowTracking is supported and not suspended (#2538)";
    pub(super) const V3_BLOCKS_REMOVES: &str =
        "Remove actions are not yet supported on icebergCompatV3-enabled tables";
    pub(super) const V3_BLOCKS_ALTER: &str =
        "ALTER TABLE not yet supported on icebergCompatV3-enabled tables";
}

// Static FeatureInfo instances for each table feature
static APPEND_ONLY_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 2)),
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
    enablement_check: EnablementCheck::EnabledIf(|props| props.append_only == Some(true)),
};

// Invariants must NOT actually be present in the table schema. Kernel will fail any data write or
// DDL on a table whose schema contains invariants. The `Conditional` cells run a schema scan to
// enforce that, while still allowing legacy tables that list the Invariants feature but do not
// use it.
static INVARIANTS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 2)),
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport {
            append: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
            dml: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
            maintenance: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
        },
        ddl: DdlSupport {
            add_column: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
            set_nullable: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
            drop_column: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
            rename_column: OpSupport::ForbiddenIf {
                msg: msg::INVARIANTS_IN_SCHEMA,
                predicate: schema_has_invariants_predicate,
            },
        },
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Allowed,
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CHECK_CONSTRAINTS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 3)),
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN),
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CHANGE_DATA_FEED_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 4)),
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport {
            dml: OpSupport::ForbiddenIfEnabled(msg::CDF_DML_NOT_YET_SUPPORTED),
            ..DataWriteSupport::ALL_ALLOWED
        },
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_change_data_feed == Some(true)
    }),
};

static GENERATED_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 4)),
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN),
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static IDENTITY_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: Some(MinReaderWriterVersion::new(1, 6)),
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN),
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static IN_COMMIT_TIMESTAMP_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
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
    operation_support: OperationSupport {
        // Gate fires whenever row tracking is *supported and not suspended* (broader than
        // just *enabled*). See [`row_tracking_supported_and_not_suspended_predicate`].
        data_write: DataWriteSupport {
            append: OpSupport::Allowed,
            dml: OpSupport::ForbiddenIf {
                msg: msg::ROW_TRACKING_BLOCKS_REMOVES,
                predicate: row_tracking_supported_and_not_suspended_predicate,
            },
            maintenance: OpSupport::ForbiddenIf {
                msg: msg::ROW_TRACKING_BLOCKS_REMOVES,
                predicate: row_tracking_supported_and_not_suspended_predicate,
            },
        },
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_row_tracking == Some(true) && props.row_tracking_suspended != Some(true)
    }),
};

static DOMAIN_METADATA_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

// TODO(#1125): IcebergCompatV1 requires schema type validation to block Map, Array, and Void types.
// This validation is not yet implemented. The feature is marked as not supported for writes until
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
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN),
    },
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v1 == Some(true)
    }),
};

// TODO(#1125): IcebergCompatV2 requires schema type validation. Unlike V1, V2 allows Map and Array
// types but needs validation against an allowlist of supported types.
// This validation is not yet implemented. The feature is marked as not supported for writes until
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
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN),
    },
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
/// - Column defaults: when supported, only literal expressions are allowed.
/// - REPLACE TABLE: when supported, partition columns must not change across the replace.
/// - Timestamp parquet encoding: when kernel can write INT96 or INT64, IcebergCompatV3 tables must
///   always use INT64; INT96 is forbidden.
/// - ALTER TABLE SET/UNSET TBLPROPERTIES: when supported, reject any property change that would
///   disable IcebergCompatV3 on an existing table.
/// - Void type: when supported, it must not appear inside map or array types.
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
    operation_support: OperationSupport {
        data_write: DataWriteSupport::restrict_to_appends_when_enabled(msg::V3_BLOCKS_REMOVES),
        ddl: DdlSupport::all_forbidden_if_enabled(msg::V3_BLOCKS_ALTER),
        read: ReadSupport::NOT_APPLICABLE,
        create: CreateSupport::Allowed,
    },
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_iceberg_compat_v3 == Some(true)
    }),
};

static CLUSTERED_TABLE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[FeatureRequirement::Supported(TableFeature::DomainMetadata)],
    operation_support: OperationSupport {
        create: CreateSupport::AllowedOnlyViaKernelInference(
            msg::CREATE_KERNEL_INFERENCE_ONLY_CLUSTERING,
        ),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static MATERIALIZE_PARTITION_COLUMNS_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::WriterOnly,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CATALOG_MANAGED_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        read: ReadSupport {
            cdf: OpSupport::ForbiddenIfSupported(msg::NOT_SUPPORTED_FOR_CDF),
            ..ReadSupport::ALL_ALLOWED
        },
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static CATALOG_OWNED_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        read: ReadSupport {
            cdf: OpSupport::ForbiddenIfSupported(msg::NOT_SUPPORTED_FOR_CDF),
            ..ReadSupport::ALL_ALLOWED
        },
        create: CreateSupport::Forbidden(msg::CATALOG_OWNED_PREVIEW_DEPRECATED_FOR_CREATE),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static COLUMN_MAPPING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: Some(MinReaderWriterVersion::new(2, 5)),
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
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
    operation_support: OperationSupport::ALL_ALLOWED,
    enablement_check: EnablementCheck::EnabledIf(|props| {
        props.enable_deletion_vectors == Some(true)
    }),
};

static TIMESTAMP_WITHOUT_TIMEZONE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        create: CreateSupport::AllowedOnlyViaKernelInference(
            msg::CREATE_KERNEL_INFERENCE_ONLY_SCHEMA,
        ),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

/// TODO: When type widening is supported on writes, restrict the allowed
/// widenings on IcebergCompatV3 tables to the subset permitted by the Iceberg v3
/// schema-evolution rules. Ref: <https://iceberg.apache.org/spec/#schema-evolution>
static TYPE_WIDENING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED_FOR_WRITES),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED_FOR_WRITES),
        read: ReadSupport::ALL_ALLOWED,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN_KERNEL_CANNOT_WRITE),
    },
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_type_widening == Some(true)),
};

/// TODO: When type widening is supported on writes, restrict the allowed
/// widenings on IcebergCompatV3 tables to the subset permitted by the Iceberg
/// schema-evolution rules. Ref: <https://iceberg.apache.org/spec/#schema-evolution>
static TYPE_WIDENING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED_FOR_WRITES),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED_FOR_WRITES),
        read: ReadSupport::ALL_ALLOWED,
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN_KERNEL_CANNOT_WRITE),
    },
    enablement_check: EnablementCheck::EnabledIf(|props| props.enable_type_widening == Some(true)),
};

static V2_CHECKPOINT_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VACUUM_PROTOCOL_CHECK_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport::ALL_ALLOWED,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_TYPE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        create: CreateSupport::AllowedOnlyViaKernelInference(
            msg::CREATE_KERNEL_INFERENCE_ONLY_SCHEMA,
        ),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_TYPE_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        create: CreateSupport::AllowedOnlyViaKernelInference(
            msg::CREATE_KERNEL_INFERENCE_ONLY_SCHEMA,
        ),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_SHREDDING_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        create: CreateSupport::AllowedOnlyViaKernelInference(
            msg::CREATE_KERNEL_INFERENCE_ONLY_SCHEMA,
        ),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

static VARIANT_SHREDDING_PREVIEW_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        create: CreateSupport::AllowedOnlyViaKernelInference(
            msg::CREATE_KERNEL_INFERENCE_ONLY_SCHEMA,
        ),
        ..OperationSupport::ALL_ALLOWED
    },
    enablement_check: EnablementCheck::AlwaysIfSupported,
};

/// By definition, kernel cannot know how to handle unknown features and must assume they're always
/// enabled if supported in protocol. However, the read path ignores all writer-only features,
/// including unknown ones. Unknown features are never inferred from legacy protocol versions.
static UNKNOWN_FEATURE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::Unknown,
    min_legacy_version: None,
    feature_requirements: &[],
    operation_support: OperationSupport {
        read: ReadSupport {
            scan: OpSupport::ForbiddenIfSupported(msg::NOT_SUPPORTED),
            cdf: OpSupport::ForbiddenIfSupported(msg::NOT_SUPPORTED),
        },
        data_write: DataWriteSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        ddl: DdlSupport::all_forbidden_if_supported(msg::NOT_SUPPORTED),
        create: CreateSupport::Forbidden(msg::CREATE_FORBIDDEN),
    },
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
            | TableFeature::TimestampWithoutTimezone
            | TableFeature::TypeWidening
            | TableFeature::TypeWideningPreview
            | TableFeature::V2Checkpoint
            | TableFeature::VacuumProtocolCheck
            | TableFeature::VariantType
            | TableFeature::VariantTypePreview
            | TableFeature::VariantShredding
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
            | TableFeature::IcebergCompatV3
            | TableFeature::ClusteredTable
            | TableFeature::MaterializePartitionColumns => FeatureType::WriterOnly,
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
    /// dependencies, and per-operation support matrix.
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

            // ReaderWriter features
            TableFeature::CatalogManaged => &CATALOG_MANAGED_INFO,
            TableFeature::CatalogOwnedPreview => &CATALOG_OWNED_PREVIEW_INFO,
            TableFeature::ColumnMapping => &COLUMN_MAPPING_INFO,
            TableFeature::DeletionVectors => &DELETION_VECTORS_INFO,
            TableFeature::TimestampWithoutTimezone => &TIMESTAMP_WITHOUT_TIMEZONE_INFO,
            TableFeature::TypeWidening => &TYPE_WIDENING_INFO,
            TableFeature::TypeWideningPreview => &TYPE_WIDENING_PREVIEW_INFO,
            TableFeature::V2Checkpoint => &V2_CHECKPOINT_INFO,
            TableFeature::VacuumProtocolCheck => &VACUUM_PROTOCOL_CHECK_INFO,
            TableFeature::VariantType => &VARIANT_TYPE_INFO,
            TableFeature::VariantTypePreview => &VARIANT_TYPE_PREVIEW_INFO,
            TableFeature::VariantShredding => &VARIANT_SHREDDING_INFO,
            TableFeature::VariantShreddingPreview => &VARIANT_SHREDDING_PREVIEW_INFO,

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

/// Like `Into<TableFeature>`, but avoids collisions between strum's derived `EnumString` and the
/// blanket impl `TryFrom<&str>` that `From<&str> for TableFeature` would trigger.
///
/// Parsing is infallible: the `Unknown` default variant catches any unrecognized feature name. If
/// https://github.com/Peternator7/strum/pull/432 merges, use impl From for TableFeature instead.
pub(crate) trait IntoTableFeature {
    fn into_table_feature(self) -> TableFeature;
}

impl IntoTableFeature for TableFeature {
    fn into_table_feature(self) -> TableFeature {
        self
    }
}

impl IntoTableFeature for &TableFeature {
    fn into_table_feature(self) -> TableFeature {
        self.clone()
    }
}

/// Parsing is infallible thanks to `TableFeature::Unknown` default variant
impl IntoTableFeature for &str {
    fn into_table_feature(self) -> TableFeature {
        #[allow(clippy::unwrap_used)] // infallible, see strum parse_err_fn
        self.parse().unwrap()
    }
}

impl IntoTableFeature for String {
    fn into_table_feature(self) -> TableFeature {
        self.as_str().into_table_feature()
    }
}

/// Formats a slice of table features using Delta's standard serialization (camelCase).
pub(crate) fn format_features(features: &[TableFeature]) -> String {
    let feature_strings: Vec<&str> = features.iter().map(|f| f.as_ref()).collect_vec();
    format!("[{}]", feature_strings.join(", "))
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
                TableFeature::TimestampWithoutTimezone => "timestampNtz",
                TableFeature::TypeWidening => "typeWidening",
                TableFeature::TypeWideningPreview => "typeWidening-preview",
                TableFeature::V2Checkpoint => "v2Checkpoint",
                TableFeature::VacuumProtocolCheck => "vacuumProtocolCheck",
                TableFeature::VariantType => "variantType",
                TableFeature::VariantTypePreview => "variantType-preview",
                TableFeature::VariantShredding => "variantShredding",
                TableFeature::VariantShreddingPreview => "variantShredding-preview",
                TableFeature::Unknown(_) => continue, // tested in test_unknown_features
            };

            // strum
            assert_eq!(feature.to_string(), expected);
            assert_eq!(feature, expected.into_table_feature());

            // json
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: TableFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);
        }
    }
}
