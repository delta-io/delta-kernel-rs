use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::string::ToString;
use std::sync::LazyLock;
use strum::{AsRefStr, Display as StrumDisplay, EnumString, VariantNames};

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
    #[serde(untagged)]
    #[strum(to_string = "{0}")]
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
    #[serde(untagged)]
    Unknown(String),
}

impl From<ReaderFeatures> for String {
    fn from(feature: ReaderFeatures) -> Self {
        match feature {
            ReaderFeatures::Unknown(prop) => prop,
            _ => feature.to_string(),
        }
    }
}

impl From<WriterFeatures> for String {
    fn from(feature: WriterFeatures) -> Self {
        match feature {
            WriterFeatures::Unknown(prop) => prop,
            _ => feature.to_string(),
        }
    }
}

pub(crate) static SUPPORTED_READER_FEATURES: LazyLock<HashSet<ReaderFeatures>> =
    LazyLock::new(|| {
        HashSet::from([
            ReaderFeatures::ColumnMapping,
            ReaderFeatures::DeletionVectors,
            ReaderFeatures::TimestampWithoutTimezone,
            ReaderFeatures::TypeWidening,
            ReaderFeatures::TypeWideningPreview,
            ReaderFeatures::VacuumProtocolCheck,
            ReaderFeatures::V2Checkpoint,
        ])
    });

pub(crate) static SUPPORTED_WRITER_FEATURES: LazyLock<HashSet<WriterFeatures>> =
    // note: we 'support' Invariants, but only insofar as we check that they are not present.
    // we support writing to tables that have Invariants enabled but not used. similarly, we only
    // support DeletionVectors in that we never write them (no DML).
    LazyLock::new(|| {
            HashSet::from([
                WriterFeatures::AppendOnly,
                WriterFeatures::DeletionVectors,
                WriterFeatures::Invariants,
            ])
        });

impl ToDataType for ReaderFeatures {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl ToDataType for WriterFeatures {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_features() {
        let mixed_reader = &[
            ReaderFeatures::DeletionVectors,
            ReaderFeatures::Unknown("cool_feature".to_string()),
            ReaderFeatures::ColumnMapping,
        ];
        let mixed_writer = &[
            WriterFeatures::DeletionVectors,
            WriterFeatures::Unknown("cool_feature".to_string()),
            WriterFeatures::AppendOnly,
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

        let typed_reader: Vec<ReaderFeatures> = serde_json::from_str(&reader_string).unwrap();
        let typed_writer: Vec<WriterFeatures> = serde_json::from_str(&writer_string).unwrap();

        assert_eq!(typed_reader.len(), 3);
        assert_eq!(&typed_reader, mixed_reader);
        assert_eq!(typed_writer.len(), 3);
        assert_eq!(&typed_writer, mixed_writer);
    }

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

        assert_eq!(ReaderFeatures::VARIANTS.len() - 1, cases.len());
        let num_cases = cases.len();
        for ((feature, expected), name) in cases
            .into_iter()
            .zip(&ReaderFeatures::VARIANTS[..num_cases])
        {
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

        assert_eq!(WriterFeatures::VARIANTS.len() - 1, cases.len());
        let num_cases = cases.len();
        for ((feature, expected), name) in cases
            .into_iter()
            .zip(&WriterFeatures::VARIANTS[..num_cases])
        {
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
