use std::collections::HashMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString};

use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::DataType;
use delta_kernel_derive::internal_api;

pub(crate) use column_mapping::column_mapping_mode;
pub use column_mapping::{validate_schema_column_mapping, ColumnMappingMode};
pub(crate) use timestamp_ntz::validate_timestamp_ntz_feature_support;

// Export table features validation and query functions
pub(crate) use table_features::{
    ensure_read_supported, ensure_write_supported, is_append_only_enabled,
    is_append_only_supported, is_cdf_read_supported, is_deletion_vector_enabled,
    is_deletion_vector_supported, is_domain_metadata_supported, is_in_commit_timestamps_enabled,
    is_in_commit_timestamps_supported, is_invariants_supported, is_row_tracking_enabled,
    is_row_tracking_supported, is_v2_checkpoint_write_supported, should_write_row_tracking,
    validate_read_table, validate_write_table,
};

mod column_mapping;
mod table_features;
mod timestamp_ntz;

impl ToDataType for TableFeature {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<TableFeature> for Scalar {
    fn from(feature: TableFeature) -> Self {
        // Use serde to get the correct string representation
        Scalar::String(
            serde_json::to_string(&feature)
                .unwrap_or_else(|_| "unknown".to_string())
                .trim_matches('"')
                .to_string(),
        )
    }
}

#[cfg(test)] // currently only used in tests
impl TableFeature {
    pub(crate) fn unknown(s: impl ToString) -> Self {
        TableFeature::Unknown(s.to_string())
    }
}

/// Supported TableFeatures for reading (ReaderWriter features only)
pub(crate) static SUPPORTED_TABLE_FEATURES_READ: LazyLock<Vec<TableFeature>> =
    LazyLock::new(|| {
        vec![
            #[cfg(feature = "catalog-managed")]
            TableFeature::CatalogManaged,
            #[cfg(feature = "catalog-managed")]
            TableFeature::CatalogOwnedPreview,
            TableFeature::ColumnMapping,
            TableFeature::DeletionVectors,
            TableFeature::TimestampNtz,
            TableFeature::TypeWidening,
            TableFeature::TypeWideningPreview,
            TableFeature::VacuumProtocolCheck,
            TableFeature::V2Checkpoint,
            TableFeature::VariantType,
            TableFeature::VariantTypePreview,
            TableFeature::VariantShreddingPreview,
        ]
    });

/// Supported TableFeatures for writing (both Writer and ReaderWriter)
pub(crate) static SUPPORTED_TABLE_FEATURES_WRITE: LazyLock<Vec<TableFeature>> =
    LazyLock::new(|| {
        vec![
            TableFeature::AppendOnly,
            TableFeature::DeletionVectors,
            TableFeature::DomainMetadata,
            TableFeature::Invariants,
            TableFeature::RowTracking,
            TableFeature::TimestampNtz,
            TableFeature::VariantType,
            TableFeature::VariantTypePreview,
            TableFeature::VariantShreddingPreview,
        ]
    });

//==================================================================================================
// NEW: Unified TableFeature representation with rich metadata
//==================================================================================================

/// Unified representation of a Delta table feature with all its metadata.
/// This replaces the separate ReaderFeature and WriterFeature enums with a single
/// enum that contains all feature metadata (min versions, dependencies, feature type).
#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, EnumString, StrumDisplay, AsRefStr,
)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
#[internal_api]
pub(crate) enum TableFeature {
    // Writer-only features (legacy, minWriterVersion < 7)
    AppendOnly,
    Invariants,
    CheckConstraints,
    ChangeDataFeed,
    GeneratedColumns,
    IdentityColumns,

    // Writer-only features (table features, minWriterVersion = 7)
    InCommitTimestamp,
    RowTracking,
    DomainMetadata,
    #[serde(rename = "clustering")]
    #[strum(serialize = "clustering")]
    ClusteredTable,
    IcebergCompatV2,
    IcebergCompatV3,
    IcebergWriterCompatV1,
    IcebergWriterCompatV3,

    // ReaderWriter features (legacy, minReaderVersion < 3)
    ColumnMapping,

    // ReaderWriter features (table features, minReaderVersion = 3, minWriterVersion = 7)
    DeletionVectors,
    #[serde(rename = "timestampNtz")]
    #[strum(serialize = "timestampNtz")]
    TimestampNtz,
    TypeWidening,
    #[serde(rename = "typeWidening-preview")]
    #[strum(serialize = "typeWidening-preview")]
    TypeWideningPreview,
    #[serde(rename = "v2Checkpoint")]
    #[strum(serialize = "v2Checkpoint")]
    V2Checkpoint,
    VacuumProtocolCheck,
    VariantType,
    #[serde(rename = "variantType-preview")]
    #[strum(serialize = "variantType-preview")]
    VariantTypePreview,
    #[serde(rename = "variantShredding-preview")]
    #[strum(serialize = "variantShredding-preview")]
    VariantShreddingPreview,

    // Catalog-managed features
    #[cfg(feature = "catalog-managed")]
    CatalogManaged,
    #[cfg(feature = "catalog-managed")]
    #[serde(rename = "catalogOwned-preview")]
    #[strum(serialize = "catalogOwned-preview")]
    CatalogOwnedPreview,

    // For forward compatibility - unknown features
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

/// Metadata for a table feature - single source of truth for all feature properties
#[derive(Debug, Clone, PartialEq, Eq)]
struct FeatureMetadata {
    name: &'static str,
    min_reader_version: i32,
    min_writer_version: i32,
    feature_type: FeatureType,
    required_features: Vec<TableFeature>,
    has_read_support: bool,
    has_write_support: bool,
}

impl FeatureMetadata {
    fn new(
        name: &'static str,
        min_reader_version: i32,
        min_writer_version: i32,
        feature_type: FeatureType,
        required_features: Vec<TableFeature>,
        has_read_support: bool,
        has_write_support: bool,
    ) -> Self {
        Self {
            name,
            min_reader_version,
            min_writer_version,
            feature_type,
            required_features,
            has_read_support,
            has_write_support,
        }
    }
}

/// Type of table feature: Writer-only or ReaderWriter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeatureType {
    /// Writer-only feature: only writers need to understand this feature
    Writer,
    /// ReaderWriter feature: both readers and writers must understand this feature
    ReaderWriter,
}

/// Static map of all table feature metadata - initialized once on first access
static FEATURE_METADATA: LazyLock<HashMap<TableFeature, FeatureMetadata>> = LazyLock::new(|| {
    use FeatureType::*;
    use TableFeature::*;
    let mut map = HashMap::new();

    // Legacy Writer Features (minWriterVersion < 7)
    // Writer-only: has_read_support always true (readers don't need to understand them)
    map.insert(
        AppendOnly,
        FeatureMetadata::new("appendOnly", 1, 2, Writer, vec![], true, true),
    );
    map.insert(
        Invariants,
        FeatureMetadata::new("invariants", 1, 2, Writer, vec![], true, true),
    );
    map.insert(
        CheckConstraints,
        FeatureMetadata::new("checkConstraints", 1, 3, Writer, vec![], true, false),
    );
    map.insert(
        ChangeDataFeed,
        FeatureMetadata::new("changeDataFeed", 1, 4, Writer, vec![], true, false),
    );
    map.insert(
        GeneratedColumns,
        FeatureMetadata::new("generatedColumns", 1, 4, Writer, vec![], true, false),
    );
    map.insert(
        IdentityColumns,
        FeatureMetadata::new("identityColumns", 1, 6, Writer, vec![], true, false),
    );

    // Legacy ReaderWriter Features (minReaderVersion < 3)
    map.insert(
        ColumnMapping,
        FeatureMetadata::new("columnMapping", 2, 5, ReaderWriter, vec![], true, false),
    );

    // Modern Writer Features (minWriterVersion = 7)
    map.insert(
        InCommitTimestamp,
        FeatureMetadata::new("inCommitTimestamp", 1, 7, Writer, vec![], true, false),
    );
    map.insert(
        RowTracking,
        FeatureMetadata::new(
            "rowTracking",
            1,
            7,
            Writer,
            vec![DomainMetadata],
            true,
            true,
        ),
    );
    map.insert(
        DomainMetadata,
        FeatureMetadata::new("domainMetadata", 1, 7, Writer, vec![], true, true),
    );
    map.insert(
        ClusteredTable,
        FeatureMetadata::new(
            "clustering",
            1,
            7,
            Writer,
            vec![DomainMetadata],
            true,
            false,
        ),
    );
    map.insert(
        IcebergCompatV2,
        FeatureMetadata::new(
            "icebergCompatV2",
            1,
            7,
            Writer,
            vec![ColumnMapping],
            true,
            false,
        ),
    );
    map.insert(
        IcebergCompatV3,
        FeatureMetadata::new(
            "icebergCompatV3",
            1,
            7,
            Writer,
            vec![ColumnMapping, RowTracking],
            true,
            false,
        ),
    );
    map.insert(
        IcebergWriterCompatV1,
        FeatureMetadata::new(
            "icebergWriterCompatV1",
            1,
            7,
            Writer,
            vec![IcebergCompatV2],
            true,
            false,
        ),
    );
    map.insert(
        IcebergWriterCompatV3,
        FeatureMetadata::new(
            "icebergWriterCompatV3",
            1,
            7,
            Writer,
            vec![IcebergCompatV3],
            true,
            false,
        ),
    );

    // Modern ReaderWriter Features (minReaderVersion = 3, minWriterVersion = 7)
    map.insert(
        DeletionVectors,
        FeatureMetadata::new("deletionVectors", 3, 7, ReaderWriter, vec![], true, true),
    );
    map.insert(
        TimestampNtz,
        FeatureMetadata::new("timestampNtz", 3, 7, ReaderWriter, vec![], true, true),
    );
    map.insert(
        TypeWidening,
        FeatureMetadata::new("typeWidening", 3, 7, ReaderWriter, vec![], true, false),
    );
    map.insert(
        TypeWideningPreview,
        FeatureMetadata::new(
            "typeWidening-preview",
            3,
            7,
            ReaderWriter,
            vec![],
            true,
            false,
        ),
    );
    map.insert(
        V2Checkpoint,
        FeatureMetadata::new("v2Checkpoint", 3, 7, ReaderWriter, vec![], true, false),
    );
    map.insert(
        VacuumProtocolCheck,
        FeatureMetadata::new(
            "vacuumProtocolCheck",
            3,
            7,
            ReaderWriter,
            vec![],
            true,
            false,
        ),
    );
    map.insert(
        VariantType,
        FeatureMetadata::new("variantType", 3, 7, ReaderWriter, vec![], true, true),
    );
    map.insert(
        VariantTypePreview,
        FeatureMetadata::new(
            "variantType-preview",
            3,
            7,
            ReaderWriter,
            vec![],
            true,
            true,
        ),
    );
    map.insert(
        VariantShreddingPreview,
        FeatureMetadata::new(
            "variantShredding-preview",
            3,
            7,
            ReaderWriter,
            vec![],
            true,
            true,
        ),
    );

    // Catalog-managed features
    #[cfg(feature = "catalog-managed")]
    {
        map.insert(
            CatalogManaged,
            FeatureMetadata::new(
                "catalogManaged",
                3,
                7,
                ReaderWriter,
                vec![InCommitTimestamp],
                true,
                false,
            ),
        );
        map.insert(
            CatalogOwnedPreview,
            FeatureMetadata::new(
                "catalogOwned-preview",
                3,
                7,
                ReaderWriter,
                vec![InCommitTimestamp],
                true,
                false,
            ),
        );
    }

    map
});

impl TableFeature {
    /// Check if this is an unknown feature
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }

    /// Get the string name of this feature as it appears in the protocol
    pub fn name(&self) -> &str {
        match self {
            Self::Unknown(name) => name.as_str(),
            _ => FEATURE_METADATA[self].name,
        }
    }

    /// Get the feature type (Writer or ReaderWriter)
    pub fn feature_type(&self) -> FeatureType {
        FEATURE_METADATA[self].feature_type
    }

    /// Get the minimum reader version required for this feature
    pub fn min_reader_version(&self) -> i32 {
        FEATURE_METADATA[self].min_reader_version
    }

    /// Get the minimum writer version required for this feature
    pub fn min_writer_version(&self) -> i32 {
        FEATURE_METADATA[self].min_writer_version
    }

    /// Check if this is a writer-only feature
    pub fn is_writer_only(&self) -> bool {
        matches!(self.feature_type(), FeatureType::Writer)
    }

    /// Check if this is a reader-writer feature
    pub fn is_reader_writer(&self) -> bool {
        matches!(self.feature_type(), FeatureType::ReaderWriter)
    }

    /// Check if this is a legacy feature (implicitly supported by protocol version)
    pub fn is_legacy(&self) -> bool {
        self.min_writer_version() < 7
    }

    /// Check if this is a table feature (explicitly listed in protocol)
    pub fn is_table_feature(&self) -> bool {
        self.min_writer_version() >= 7
    }

    /// Get the features that this feature depends on (must also be enabled)
    pub fn required_features(&self) -> &[TableFeature] {
        &FEATURE_METADATA[self].required_features
    }

    /// Check if Kernel has support to read tables with this feature.
    /// Should only be called for reader-writer features.
    pub fn has_kernel_read_support(&self) -> bool {
        assert!(
            self.is_reader_writer(),
            "has_kernel_read_support should only be called for reader-writer features"
        );
        FEATURE_METADATA[self].has_read_support
    }

    /// Check if Kernel has support to write tables with this feature.
    /// Some features may require checking metadata configuration.
    pub fn has_kernel_write_support(&self) -> bool {
        FEATURE_METADATA[self].has_write_support
    }
}
