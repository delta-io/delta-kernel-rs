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
