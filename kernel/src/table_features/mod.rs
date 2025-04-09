use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumCount, EnumString};

use crate::actions::schemas::ToDataType;
use crate::schema::DataType;

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
    EnumCount,
    Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReaderFeature {
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
pub enum WriterFeature {
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
        ReaderFeature::ColumnMapping,
        ReaderFeature::DeletionVectors,
        ReaderFeature::TimestampWithoutTimezone,
        ReaderFeature::TypeWidening,
        ReaderFeature::TypeWideningPreview,
        ReaderFeature::VacuumProtocolCheck,
        ReaderFeature::V2Checkpoint,
    ]
});

// note: we 'support' Invariants, but only insofar as we check that they are not present.
// we support writing to tables that have Invariants enabled but not used. similarly, we only
// support DeletionVectors in that we never write them (no DML).
pub(crate) static SUPPORTED_WRITER_FEATURES: LazyLock<Vec<WriterFeature>> = LazyLock::new(|| {
    vec![
        WriterFeature::AppendOnly,
        WriterFeature::DeletionVectors,
        WriterFeature::Invariants,
    ]
});

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
            (ReaderFeature::ColumnMapping, "columnMapping"),
            (ReaderFeature::DeletionVectors, "deletionVectors"),
            (ReaderFeature::TimestampWithoutTimezone, "timestampNtz"),
            (ReaderFeature::TypeWidening, "typeWidening"),
            (ReaderFeature::TypeWideningPreview, "typeWidening-preview"),
            (ReaderFeature::V2Checkpoint, "v2Checkpoint"),
            (ReaderFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (ReaderFeature::unknown("something"), "something"),
        ];

        assert_eq!(ReaderFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

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
            (WriterFeature::Invariants, "invariants"),
            (WriterFeature::CheckConstraints, "checkConstraints"),
            (WriterFeature::ChangeDataFeed, "changeDataFeed"),
            (WriterFeature::GeneratedColumns, "generatedColumns"),
            (WriterFeature::ColumnMapping, "columnMapping"),
            (WriterFeature::IdentityColumns, "identityColumns"),
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
            (WriterFeature::unknown("something"), "something"),
        ];

        assert_eq!(WriterFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: WriterFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }
}
