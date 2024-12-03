use std::collections::HashSet;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString, VariantNames};

pub(crate) use column_mapping::column_mapping_mode;
pub use column_mapping::{validate_schema_column_mapping, ColumnMappingMode};
mod column_mapping;

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
    VariantNames,
    Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReaderFeatures {
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
    VariantNames,
    Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum WriterFeatures {
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
}

impl From<ReaderFeatures> for String {
    fn from(feature: ReaderFeatures) -> Self {
        feature.to_string()
    }
}

impl From<WriterFeatures> for String {
    fn from(feature: WriterFeatures) -> Self {
        feature.to_string()
    }
}

// we support everything except V2 checkpoints
pub(crate) static SUPPORTED_READER_FEATURES: LazyLock<HashSet<ReaderFeatures>> =
    LazyLock::new(|| {
        HashSet::from([
            ReaderFeatures::ColumnMapping,
            ReaderFeatures::DeletionVectors,
            ReaderFeatures::TimestampWithoutTimezone,
            ReaderFeatures::TypeWidening,
            ReaderFeatures::TypeWideningPreview,
            ReaderFeatures::VacuumProtocolCheck,
        ])
    });

// write support wip: no table features are supported yet
pub(crate) static SUPPORTED_WRITER_FEATURES: LazyLock<HashSet<WriterFeatures>> =
    LazyLock::new(|| HashSet::from([]));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_reader_features() {
        let cases = [
            (ReaderFeatures::ColumnMapping, "columnMapping"),
            (ReaderFeatures::DeletionVectors, "deletionVectors"),
            (ReaderFeatures::TimestampWithoutTimezone, "timestampNtz"),
            (ReaderFeatures::TypeWidening, "typeWidening"),
            (ReaderFeatures::TypeWideningPreview, "typeWidening-preview"),
            (ReaderFeatures::V2Checkpoint, "v2Checkpoint"),
            (ReaderFeatures::VacuumProtocolCheck, "vacuumProtocolCheck"),
        ];

        assert_eq!(ReaderFeatures::VARIANTS.len(), cases.len());

        for ((feature, expected), name) in cases.into_iter().zip(ReaderFeatures::VARIANTS) {
            assert_eq!(*name, expected);

            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: ReaderFeatures = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: ReaderFeatures = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }

    #[test]
    fn test_roundtrip_writer_features() {
        let cases = [
            (WriterFeatures::AppendOnly, "appendOnly"),
            (WriterFeatures::Invariants, "invariants"),
            (WriterFeatures::CheckConstraints, "checkConstraints"),
            (WriterFeatures::ChangeDataFeed, "changeDataFeed"),
            (WriterFeatures::GeneratedColumns, "generatedColumns"),
            (WriterFeatures::ColumnMapping, "columnMapping"),
            (WriterFeatures::IdentityColumns, "identityColumns"),
            (WriterFeatures::DeletionVectors, "deletionVectors"),
            (WriterFeatures::RowTracking, "rowTracking"),
            (WriterFeatures::TimestampWithoutTimezone, "timestampNtz"),
            (WriterFeatures::TypeWidening, "typeWidening"),
            (WriterFeatures::TypeWideningPreview, "typeWidening-preview"),
            (WriterFeatures::DomainMetadata, "domainMetadata"),
            (WriterFeatures::V2Checkpoint, "v2Checkpoint"),
            (WriterFeatures::IcebergCompatV1, "icebergCompatV1"),
            (WriterFeatures::IcebergCompatV2, "icebergCompatV2"),
            (WriterFeatures::VacuumProtocolCheck, "vacuumProtocolCheck"),
        ];

        assert_eq!(WriterFeatures::VARIANTS.len(), cases.len());

        for ((feature, expected), name) in cases.into_iter().zip(WriterFeatures::VARIANTS) {
            assert_eq!(*name, expected);

            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: WriterFeatures = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeatures = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }
}
