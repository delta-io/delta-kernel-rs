use std::collections::HashMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumCount, EnumString};

use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::DataType;
use delta_kernel_derive::internal_api;

pub(crate) use column_mapping::column_mapping_mode;
pub use column_mapping::{validate_schema_column_mapping, ColumnMappingMode};
pub(crate) use timestamp_ntz::validate_timestamp_ntz_feature_support;
mod column_mapping;
mod timestamp_ntz;

/// Reader features communicate capabilities that must be implemented in order to correctly read a
/// given table. That is, readers must implement and respect all features listed in a table's
/// `ReaderFeatures`. Note that any feature listed as a `ReaderFeature` must also have a
/// corresponding `WriterFeature`.
///
/// The kernel currently supports all reader features except for V2Checkpoints.
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
pub(crate) enum ReaderFeature {
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

/// Similar to reader features, writer features communicate capabilities that must be implemented
/// in order to correctly write to a given table. That is, writers must implement and respect all
/// features listed in a table's `WriterFeatures`.
///
/// Kernel write support is currently in progress and as such these are not supported.
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
pub(crate) enum WriterFeature {
    /// CatalogManaged tables:
    /// <https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md>
    CatalogManaged,
    #[strum(serialize = "catalogOwned-preview")]
    #[serde(rename = "catalogOwned-preview")]
    CatalogOwnedPreview,
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
    /// Mapping of one column to another
    ColumnMapping,
    /// ID Columns
    IdentityColumns,
    /// Monotonically increasing timestamps in the CommitInfo
    InCommitTimestamp,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// Row tracking on tables
    RowTracking,
    /// timestamps without timezone support
    #[strum(serialize = "timestampNtz")]
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // Allow columns to change type
    TypeWidening,
    #[strum(serialize = "typeWidening-preview")]
    #[serde(rename = "typeWidening-preview")]
    TypeWideningPreview,
    /// domain specific metadata
    DomainMetadata,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// Iceberg compatibility support
    IcebergCompatV2,
    /// vacuumProtocolCheck ReaderWriter feature ensures consistent application of reader and writer
    /// protocol checks during VACUUM operations
    VacuumProtocolCheck,
    /// The Clustered Table feature facilitates the physical clustering of rows
    /// that share similar values on a predefined set of clustering columns.
    #[strum(serialize = "clustering")]
    #[serde(rename = "clustering")]
    ClusteredTable,
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

impl ToDataType for ReaderFeature {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl ToDataType for WriterFeature {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<ReaderFeature> for Scalar {
    fn from(feature: ReaderFeature) -> Self {
        Scalar::String(feature.to_string())
    }
}

impl From<WriterFeature> for Scalar {
    fn from(feature: WriterFeature) -> Self {
        Scalar::String(feature.to_string())
    }
}

#[cfg(test)] // currently only used in tests
impl ReaderFeature {
    pub(crate) fn unknown(s: impl ToString) -> Self {
        ReaderFeature::Unknown(s.to_string())
    }
}

#[cfg(test)] // currently only used in tests
impl WriterFeature {
    pub(crate) fn unknown(s: impl ToString) -> Self {
        WriterFeature::Unknown(s.to_string())
    }
}

pub(crate) static SUPPORTED_READER_FEATURES: LazyLock<Vec<ReaderFeature>> = LazyLock::new(|| {
    vec![
        #[cfg(feature = "catalog-managed")]
        ReaderFeature::CatalogManaged,
        #[cfg(feature = "catalog-managed")]
        ReaderFeature::CatalogOwnedPreview,
        ReaderFeature::ColumnMapping,
        ReaderFeature::DeletionVectors,
        ReaderFeature::TimestampWithoutTimezone,
        ReaderFeature::TypeWidening,
        ReaderFeature::TypeWideningPreview,
        ReaderFeature::VacuumProtocolCheck,
        ReaderFeature::V2Checkpoint,
        ReaderFeature::VariantType,
        ReaderFeature::VariantTypePreview,
        // The default engine currently DOES NOT support shredded Variant reads and the parquet
        // reader will reject the read if it sees a shredded schema in the parquet file. That being
        // said, kernel does permit reconstructing shredded variants into the
        // `STRUCT<metadata: BINARY, value: BINARY>` representation if parquet readers of
        // third-party engines support it.
        ReaderFeature::VariantShreddingPreview,
    ]
});

/// The writer features have the following limitations:
/// - We 'support' Invariants only insofar as we check that they are not present.
/// - We support writing to tables that have Invariants enabled but not used.
/// - We only support DeletionVectors in that we never write them (no DML).
/// - We support writing to existing tables with row tracking, but we don't support creating
///   tables with row tracking yet.
pub(crate) static SUPPORTED_WRITER_FEATURES: LazyLock<Vec<WriterFeature>> = LazyLock::new(|| {
    vec![
        WriterFeature::AppendOnly,
        WriterFeature::DeletionVectors,
        WriterFeature::DomainMetadata,
        WriterFeature::Invariants,
        WriterFeature::RowTracking,
        WriterFeature::TimestampWithoutTimezone,
        WriterFeature::VariantType,
        WriterFeature::VariantTypePreview,
        WriterFeature::VariantShreddingPreview,
    ]
});

//==================================================================================================
// NEW: Unified TableFeature representation with rich metadata
//==================================================================================================

/// Unified representation of a Delta table feature with all its metadata.
/// This replaces the separate ReaderFeature and WriterFeature enums with a single
/// enum that contains all feature metadata (min versions, dependencies, feature type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableFeature {
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
    ClusteredTable,
    IcebergCompatV2,
    IcebergCompatV3,
    IcebergWriterCompatV1,
    IcebergWriterCompatV3,

    // ReaderWriter features (legacy, minReaderVersion < 3)
    ColumnMapping,

    // ReaderWriter features (table features, minReaderVersion = 3, minWriterVersion = 7)
    DeletionVectors,
    TimestampNtz,
    TypeWidening,
    TypeWideningPreview,
    V2Checkpoint,
    VacuumProtocolCheck,
    VariantType,
    VariantTypePreview,
    VariantShreddingPreview,

    // Catalog-managed features
    #[cfg(feature = "catalog-managed")]
    CatalogManaged,
    #[cfg(feature = "catalog-managed")]
    CatalogOwnedPreview,
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
    /// Get the string name of this feature as it appears in the protocol
    pub fn name(&self) -> &'static str {
        FEATURE_METADATA[self].name
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_features() {
        let mixed_reader = &[
            ReaderFeature::DeletionVectors,
            ReaderFeature::unknown("cool_feature"),
            ReaderFeature::ColumnMapping,
        ];
        let mixed_writer = &[
            WriterFeature::DeletionVectors,
            WriterFeature::unknown("cool_feature"),
            WriterFeature::AppendOnly,
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

        let typed_reader: Vec<ReaderFeature> = serde_json::from_str(&reader_string).unwrap();
        let typed_writer: Vec<WriterFeature> = serde_json::from_str(&writer_string).unwrap();

        assert_eq!(typed_reader.len(), 3);
        assert_eq!(&typed_reader, mixed_reader);
        assert_eq!(typed_writer.len(), 3);
        assert_eq!(&typed_writer, mixed_writer);
    }

    #[test]
    fn test_roundtrip_reader_features() {
        let cases = [
            (ReaderFeature::CatalogManaged, "catalogManaged"),
            (ReaderFeature::CatalogOwnedPreview, "catalogOwned-preview"),
            (ReaderFeature::ColumnMapping, "columnMapping"),
            (ReaderFeature::DeletionVectors, "deletionVectors"),
            (ReaderFeature::TimestampWithoutTimezone, "timestampNtz"),
            (ReaderFeature::TypeWidening, "typeWidening"),
            (ReaderFeature::TypeWideningPreview, "typeWidening-preview"),
            (ReaderFeature::V2Checkpoint, "v2Checkpoint"),
            (ReaderFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (ReaderFeature::VariantType, "variantType"),
            (ReaderFeature::VariantTypePreview, "variantType-preview"),
            (
                ReaderFeature::VariantShreddingPreview,
                "variantShredding-preview",
            ),
            (ReaderFeature::unknown("something"), "something"),
        ];

        assert_eq!(ReaderFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: ReaderFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: ReaderFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }

    #[test]
    fn test_roundtrip_writer_features() {
        let cases = [
            (WriterFeature::AppendOnly, "appendOnly"),
            (WriterFeature::CatalogManaged, "catalogManaged"),
            (WriterFeature::CatalogOwnedPreview, "catalogOwned-preview"),
            (WriterFeature::Invariants, "invariants"),
            (WriterFeature::CheckConstraints, "checkConstraints"),
            (WriterFeature::ChangeDataFeed, "changeDataFeed"),
            (WriterFeature::GeneratedColumns, "generatedColumns"),
            (WriterFeature::ColumnMapping, "columnMapping"),
            (WriterFeature::IdentityColumns, "identityColumns"),
            (WriterFeature::InCommitTimestamp, "inCommitTimestamp"),
            (WriterFeature::DeletionVectors, "deletionVectors"),
            (WriterFeature::RowTracking, "rowTracking"),
            (WriterFeature::TimestampWithoutTimezone, "timestampNtz"),
            (WriterFeature::TypeWidening, "typeWidening"),
            (WriterFeature::TypeWideningPreview, "typeWidening-preview"),
            (WriterFeature::DomainMetadata, "domainMetadata"),
            (WriterFeature::V2Checkpoint, "v2Checkpoint"),
            (WriterFeature::IcebergCompatV1, "icebergCompatV1"),
            (WriterFeature::IcebergCompatV2, "icebergCompatV2"),
            (WriterFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (WriterFeature::ClusteredTable, "clustering"),
            (WriterFeature::VariantType, "variantType"),
            (WriterFeature::VariantTypePreview, "variantType-preview"),
            (
                WriterFeature::VariantShreddingPreview,
                "variantShredding-preview",
            ),
            (WriterFeature::unknown("something"), "something"),
        ];

        assert_eq!(WriterFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: WriterFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }
}
