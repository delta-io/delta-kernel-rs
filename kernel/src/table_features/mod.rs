use crate::{DeltaResult, Error};
use enumset::{enum_set, EnumSet, EnumSetType};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::any::type_name;
use std::fmt::Display;
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
    EnumString,
    StrumDisplay,
    AsRefStr,
    VariantNames,
    Hash,
    EnumSetType,
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
    EnumString,
    StrumDisplay,
    AsRefStr,
    VariantNames,
    Hash,
    EnumSetType,
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
}

impl From<ReaderFeature> for String {
    fn from(feature: ReaderFeature) -> Self {
        feature.to_string()
    }
}

impl From<WriterFeature> for String {
    fn from(feature: WriterFeature) -> Self {
        feature.to_string()
    }
}

pub(crate) static SUPPORTED_READER_FEATURES: EnumSet<ReaderFeature> = enum_set!(
    ReaderFeature::ColumnMapping
        | ReaderFeature::DeletionVectors
        | ReaderFeature::TimestampWithoutTimezone
        | ReaderFeature::TypeWidening
        | ReaderFeature::TypeWideningPreview
        | ReaderFeature::VacuumProtocolCheck
        | ReaderFeature::V2Checkpoint
);

pub(crate) static SUPPORTED_WRITER_FEATURES: EnumSet<WriterFeature> =
    // note: we 'support' Invariants, but only insofar as we check that they are not present.
    // we support writing to tables that have Invariants enabled but not used. similarly, we only
    // support DeletionVectors in that we never write them (no DML).
    enum_set!(
        WriterFeature::AppendOnly | WriterFeature::DeletionVectors | WriterFeature::Invariants
    );

pub(crate) static CDF_SUPPORTED_READER_FEATURES: EnumSet<ReaderFeature> =
    enum_set!(ReaderFeature::DeletionVectors);

pub(crate) fn ensure_supported_features<F>(
    features: &[String],
    supported: &EnumSet<F>,
) -> DeltaResult<()>
where
    F: DeserializeOwned + EnumSetType + Display,
{
    let features_type = type_name::<F>().rsplit("::").next().unwrap();
    for feature_str in features {
        match serde_json::from_str::<F>(&format!("\"{}\"", feature_str)) {
            Ok(feature_enum) => {
                if !supported.contains(feature_enum) {
                    return Err(Error::Unsupported(format!(
                        "Unsupported {} variant `{}`. Supported features: {}",
                        features_type, feature_enum, supported
                    )));
                }
            }
            Err(_) => {
                return Err(Error::Unsupported(format!(
                    "Unknown {} variant `{}`. Supported features: {}",
                    features_type, feature_str, supported
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        ];

        assert_eq!(ReaderFeature::VARIANTS.len(), cases.len());

        for ((feature, expected), name) in cases.into_iter().zip(ReaderFeature::VARIANTS) {
            assert_eq!(*name, expected);

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
        ];

        assert_eq!(WriterFeature::VARIANTS.len(), cases.len());

        for ((feature, expected), name) in cases.into_iter().zip(WriterFeature::VARIANTS) {
            assert_eq!(*name, expected);

            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{}\"", expected));

            let deserialized: WriterFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }

    #[test]
    fn test_ensure_supported_features() {
        let supported_features =
            enum_set!(ReaderFeature::ColumnMapping | ReaderFeature::DeletionVectors);
        let table_features = vec![ReaderFeature::ColumnMapping.to_string()];
        ensure_supported_features(&table_features, &supported_features).unwrap();
    }

    #[test]
    fn test_ensure_supported_features_unsupported() {
        let supported_features =
            enum_set!(ReaderFeature::ColumnMapping | ReaderFeature::DeletionVectors);
        let table_features = vec![ReaderFeature::TimestampWithoutTimezone.to_string()];
        let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
        assert_eq!(error.to_string(), "Unsupported: Unsupported ReaderFeature variant `timestampNtz`. Supported features: columnMapping | deletionVectors".to_string());
    }

    #[test]
    fn test_ensure_supported_features_unknown() {
        let supported_features =
            enum_set!(ReaderFeature::ColumnMapping | ReaderFeature::DeletionVectors);
        let table_features = vec![ReaderFeature::ColumnMapping.to_string(), "idk".to_string()];
        let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
        assert_eq!(error.to_string(), "Unsupported: Unknown ReaderFeature variant `idk`. Supported features: columnMapping | deletionVectors".to_string());
    }
}
