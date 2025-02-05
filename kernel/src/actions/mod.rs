//! Provides parsing and manipulation of the various actions defined in the [Delta
//! specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::LazyLock;

use self::deletion_vector::DeletionVectorDescriptor;
use crate::actions::schemas::GetStructField;
use crate::schema::{SchemaRef, StructType};
use crate::table_features::{
    ReaderFeatures, WriterFeatures, SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error, RowVisitor as _};
use visitors::{MetadataVisitor, ProtocolVisitor};

use delta_kernel_derive::Schema;
use serde::{Deserialize, Serialize};

pub mod deletion_vector;
pub mod set_transaction;

pub(crate) mod schemas;
#[cfg(feature = "developer-visibility")]
pub mod visitors;
#[cfg(not(feature = "developer-visibility"))]
pub(crate) mod visitors;

#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const ADD_NAME: &str = "add";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const REMOVE_NAME: &str = "remove";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const METADATA_NAME: &str = "metaData";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const PROTOCOL_NAME: &str = "protocol";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const SET_TRANSACTION_NAME: &str = "txn";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const COMMIT_INFO_NAME: &str = "commitInfo";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const CDC_NAME: &str = "cdc";
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) const SIDECAR_NAME: &str = "sidecar";

static LOG_ADD_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| StructType::new([Option::<Add>::get_struct_field(ADD_NAME)]).into());

static LOG_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<CommitInfo>::get_struct_field(COMMIT_INFO_NAME),
        Option::<Cdc>::get_struct_field(CDC_NAME),
        Option::<Sidecar>::get_struct_field(SIDECAR_NAME),
        // We don't support the following actions yet
        //Option::<DomainMetadata>::get_struct_field(DOMAIN_METADATA_NAME),
    ])
    .into()
});

static LOG_COMMIT_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([Option::<CommitInfo>::get_struct_field(COMMIT_INFO_NAME)]).into()
});

#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_log_schema() -> &'static SchemaRef {
    &LOG_SCHEMA
}

#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_log_add_schema() -> &'static SchemaRef {
    &LOG_ADD_SCHEMA
}

pub(crate) fn get_log_commit_info_schema() -> &'static SchemaRef {
    &LOG_COMMIT_INFO_SCHEMA
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(test, derive(Serialize), serde(rename_all = "camelCase"))]
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containing configuration options for the format
    pub options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(test, derive(Serialize), serde(rename_all = "camelCase"))]
pub struct Metadata {
    /// Unique identifier for this table
    pub id: String,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: Format,
    /// Schema of the table
    pub schema_string: String,
    /// Column names by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<i64>,
    /// Configuration options for the metadata action. These are parsed into [`TableProperties`].
    pub configuration: HashMap<String, String>,
}

impl Metadata {
    pub fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Metadata>> {
        let mut visitor = MetadataVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.metadata)
    }

    pub fn parse_schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }

    /// Parse the metadata configuration HashMap<String, String> into a TableProperties struct.
    /// Note that parsing is infallible -- any items that fail to parse are simply propagated
    /// through to the `TableProperties.unknown_properties` field.
    pub fn parse_table_properties(&self) -> TableProperties {
        TableProperties::from(self.configuration.iter())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// TODO move to another module so that we disallow constructing this struct without using the
// try_new function.
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<Vec<String>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<String>>,
}

impl Protocol {
    /// Try to create a new Protocol instance from reader/writer versions and table features. This
    /// can fail if the protocol is invalid.
    pub fn try_new(
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Option<impl IntoIterator<Item = impl Into<String>>>,
        writer_features: Option<impl IntoIterator<Item = impl Into<String>>>,
    ) -> DeltaResult<Self> {
        if min_reader_version == 3 {
            require!(
                reader_features.is_some(),
                Error::invalid_protocol(
                    "Reader features must be present when minimum reader version = 3"
                )
            );
        }
        if min_writer_version == 7 {
            require!(
                writer_features.is_some(),
                Error::invalid_protocol(
                    "Writer features must be present when minimum writer version = 7"
                )
            );
        }
        let reader_features = reader_features.map(|f| f.into_iter().map(Into::into).collect());
        let writer_features = writer_features.map(|f| f.into_iter().map(Into::into).collect());
        Ok(Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        })
    }

    /// Create a new Protocol by visiting the EngineData and extracting the first protocol row into
    /// a Protocol instance. If no protocol row is found, returns Ok(None).
    pub fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Protocol>> {
        let mut visitor = ProtocolVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.protocol)
    }

    /// This protocol's minimum reader version
    pub fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    /// This protocol's minimum writer version
    pub fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    /// Get the reader features for the protocol
    pub fn reader_features(&self) -> Option<&[String]> {
        self.reader_features.as_deref()
    }

    /// Get the writer features for the protocol
    pub fn writer_features(&self) -> Option<&[String]> {
        self.writer_features.as_deref()
    }

    /// True if this protocol has the requested reader feature
    pub fn has_reader_feature(&self, feature: &ReaderFeatures) -> bool {
        self.reader_features()
            .is_some_and(|features| features.iter().any(|f| f == feature.as_ref()))
    }

    /// True if this protocol has the requested writer feature
    pub fn has_writer_feature(&self, feature: &WriterFeatures) -> bool {
        self.writer_features()
            .is_some_and(|features| features.iter().any(|f| f == feature.as_ref()))
    }

    /// Check if reading a table with this protocol is supported. That is: does the kernel support
    /// the specified protocol reader version and all enabled reader features? If yes, returns unit
    /// type, otherwise will return an error.
    pub fn ensure_read_supported(&self) -> DeltaResult<()> {
        match &self.reader_features {
            // if min_reader_version = 3 and all reader features are subset of supported => OK
            Some(reader_features) if self.min_reader_version == 3 => {
                ensure_supported_features(reader_features, &SUPPORTED_READER_FEATURES)
            }
            // if min_reader_version = 3 and no reader features => ERROR
            // NOTE this is caught by the protocol parsing.
            None if self.min_reader_version == 3 => Err(Error::internal_error(
                "Reader features must be present when minimum reader version = 3",
            )),
            // if min_reader_version = 1,2 and there are no reader features => OK
            None if self.min_reader_version == 1 || self.min_reader_version == 2 => Ok(()),
            // if min_reader_version = 1,2 and there are reader features => ERROR
            // NOTE this is caught by the protocol parsing.
            Some(_) if self.min_reader_version == 1 || self.min_reader_version == 2 => {
                Err(Error::internal_error(
                    "Reader features must not be present when minimum reader version = 1 or 2",
                ))
            }
            // any other min_reader_version is not supported
            _ => Err(Error::Unsupported(format!(
                "Unsupported minimum reader version {}",
                self.min_reader_version
            ))),
        }
    }

    /// Check if writing to a table with this protocol is supported. That is: does the kernel
    /// support the specified protocol writer version and all enabled writer features?
    pub fn ensure_write_supported(&self) -> DeltaResult<()> {
        match &self.writer_features {
            // if min_reader_version = 3 and min_writer_version = 7 and all writer features are
            // supported => OK
            Some(writer_features)
                if self.min_reader_version == 3 && self.min_writer_version == 7 =>
            {
                ensure_supported_features(writer_features, &SUPPORTED_WRITER_FEATURES)
            }
            // otherwise not supported
            _ => Err(Error::unsupported(
                "Only tables with min reader version 3 and min writer version 7 with no table features are supported."
            )),
        }
    }
}

// given unparsed `table_features`, parse and check if they are subset of `supported_features`
pub(crate) fn ensure_supported_features<T>(
    table_features: &[String],
    supported_features: &HashSet<T>,
) -> DeltaResult<()>
where
    <T as FromStr>::Err: Display,
    T: Debug + FromStr + Hash + Eq,
{
    let error = |unsupported, unsupported_or_unknown| {
        let supported = supported_features.iter().collect::<Vec<_>>();
        let features_type = type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("table features");
        Error::Unsupported(format!(
            "{} {} {:?}. Supported {} are {:?}",
            unsupported_or_unknown, features_type, unsupported, features_type, supported
        ))
    };
    let parsed_features: HashSet<T> = table_features
        .iter()
        .map(|s| T::from_str(s).map_err(|_| error(vec![s.to_string()], "Unknown")))
        .collect::<Result<_, Error>>()?;
    parsed_features
        .is_subset(supported_features)
        .then_some(())
        .ok_or_else(|| {
            let unsupported = parsed_features
                .difference(supported_features)
                .map(|f| format!("{:?}", f))
                .collect::<Vec<_>>();
            error(unsupported, "Unsupported")
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub(crate) struct CommitInfo {
    /// The time this logical file was created, as milliseconds since the epoch.
    /// Read: optional, write: required (that is, kernel always writes).
    pub(crate) timestamp: Option<i64>,
    /// The time this logical file was created, as milliseconds since the epoch. Unlike
    /// `timestamp`, this field is guaranteed to be monotonically increase with each commit.
    /// Note: If in-commit timestamps are enabled, both the following must be true:
    /// - The `inCommitTimestamp` field must always be present in CommitInfo.
    /// - The CommitInfo action must always be the first one in a commit.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// An arbitrary string that identifies the operation associated with this commit. This is
    /// specified by the engine. Read: optional, write: required (that is, kernel alwarys writes).
    pub(crate) operation: Option<String>,
    /// Map of arbitrary string key-value pairs that provide additional information about the
    /// operation. This is specified by the engine. For now this is always empty on write.
    pub(crate) operation_parameters: Option<HashMap<String, String>>,
    /// The version of the delta_kernel crate used to write this commit. The kernel will always
    /// write this field, but it is optional since many tables will not have this field (i.e. any
    /// tables not written by kernel).
    pub(crate) kernel_version: Option<String>,
    /// A place for the engine to store additional metadata associated with this commit encoded as
    /// a map of strings.
    pub(crate) engine_commit_info: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// A map from partition column to value for this logical file. This map can contain null in the
    /// values meaning a partition is null. We drop those values from this map, due to the
    /// `drop_null_container_values` annotation. This means an engine can assume that if a partition
    /// is found in [`Metadata`] `partition_columns`, but not in this map, its value is null.
    #[drop_null_container_values]
    pub partition_values: HashMap<String, String>,

    /// The size of this data file in bytes
    pub size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file encoded as a JSON string.
    ///
    /// [statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub stats: Option<String>,

    /// Map containing metadata about this logical file.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub default_row_commit_version: Option<i64>,

    /// The name of the clustering implementation
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub clustering_provider: Option<String>,
}

impl Add {
    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub(crate) struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// The time this logical file was created, as milliseconds since the epoch.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) deletion_timestamp: Option<i64>,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) partition_values: Option<HashMap<String, String>>,

    /// The size of this data file in bytes
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) size: Option<i64>,

    /// Map containing metadata about this logical file.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) default_row_commit_version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub(crate) struct Cdc {
    /// A relative path to a change data file from the root of the table or an absolute path to a
    /// change data file that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// A map from partition column to value for this logical file. This map can contain null in the
    /// values meaning a partition is null. We drop those values from this map, due to the
    /// `drop_null_container_values` annotation. This means an engine can assume that if a partition
    /// is found in [`Metadata`] `partition_columns`, but not in this map, its value is null.
    #[drop_null_container_values]
    pub partition_values: HashMap<String, String>,

    /// The size of this cdc file in bytes
    pub size: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    ///
    /// Should always be set to false for `cdc` actions because they *do not* change the underlying
    /// data of the table
    pub data_change: bool,

    /// Map containing metadata about this logical file.
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct SetTransaction {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub last_updated: Option<i64>,
}

/// The sidecar action references a sidecar file which provides some of the checkpoint's
/// file actions. This action is only allowed in checkpoints following the V2 spec.
///
/// [More info]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
#[allow(unused)] //TODO: Remove once we implement V2 checkpoint file processing
#[derive(Schema, Debug, PartialEq)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct Sidecar {
    /// A path to a sidecar file that can be either:
    /// - A relative path (just the file name) within the `_delta_log/_sidecars` directory.  
    /// - An absolute path
    /// The path is a URI as specified by [RFC 2396 URI Generic Syntax], which needs to be decoded
    /// to get the file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// The size of the sidecar file in bytes.
    pub size_in_bytes: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// A map containing any additional metadata about the logicial file.
    pub tags: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField};

    #[test]
    fn test_metadata_schema() {
        let schema = get_log_schema()
            .project(&[METADATA_NAME])
            .expect("Couldn't get metaData field");

        let expected = Arc::new(StructType::new([StructField::nullable(
            "metaData",
            StructType::new([
                StructField::not_null("id", DataType::STRING),
                StructField::nullable("name", DataType::STRING),
                StructField::nullable("description", DataType::STRING),
                StructField::not_null(
                    "format",
                    StructType::new([
                        StructField::not_null("provider", DataType::STRING),
                        StructField::not_null(
                            "options",
                            MapType::new(DataType::STRING, DataType::STRING, false),
                        ),
                    ]),
                ),
                StructField::not_null("schemaString", DataType::STRING),
                StructField::not_null("partitionColumns", ArrayType::new(DataType::STRING, false)),
                StructField::nullable("createdTime", DataType::LONG),
                StructField::not_null(
                    "configuration",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_add_schema() {
        let schema = get_log_schema()
            .project(&[ADD_NAME])
            .expect("Couldn't get add field");

        let expected = Arc::new(StructType::new([StructField::nullable(
            "add",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::not_null(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                ),
                StructField::not_null("size", DataType::LONG),
                StructField::not_null("modificationTime", DataType::LONG),
                StructField::not_null("dataChange", DataType::BOOLEAN),
                StructField::nullable("stats", DataType::STRING),
                StructField::nullable(
                    "tags",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
                deletion_vector_field(),
                StructField::nullable("baseRowId", DataType::LONG),
                StructField::nullable("defaultRowCommitVersion", DataType::LONG),
                StructField::nullable("clusteringProvider", DataType::STRING),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    fn tags_field() -> StructField {
        StructField::nullable(
            "tags",
            MapType::new(DataType::STRING, DataType::STRING, false),
        )
    }

    fn partition_values_field() -> StructField {
        StructField::nullable(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, false),
        )
    }

    fn deletion_vector_field() -> StructField {
        StructField::nullable(
            "deletionVector",
            DataType::struct_type([
                StructField::not_null("storageType", DataType::STRING),
                StructField::not_null("pathOrInlineDv", DataType::STRING),
                StructField::nullable("offset", DataType::INTEGER),
                StructField::not_null("sizeInBytes", DataType::INTEGER),
                StructField::not_null("cardinality", DataType::LONG),
            ]),
        )
    }

    #[test]
    fn test_remove_schema() {
        let schema = get_log_schema()
            .project(&[REMOVE_NAME])
            .expect("Couldn't get remove field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "remove",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::nullable("deletionTimestamp", DataType::LONG),
                StructField::not_null("dataChange", DataType::BOOLEAN),
                StructField::nullable("extendedFileMetadata", DataType::BOOLEAN),
                partition_values_field(),
                StructField::nullable("size", DataType::LONG),
                tags_field(),
                deletion_vector_field(),
                StructField::nullable("baseRowId", DataType::LONG),
                StructField::nullable("defaultRowCommitVersion", DataType::LONG),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_cdc_schema() {
        let schema = get_log_schema()
            .project(&[CDC_NAME])
            .expect("Couldn't get cdc field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "cdc",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::not_null(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                ),
                StructField::not_null("size", DataType::LONG),
                StructField::not_null("dataChange", DataType::BOOLEAN),
                tags_field(),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_sidecar_schema() {
        let schema = get_log_schema()
            .project(&[SIDECAR_NAME])
            .expect("Couldn't get sidecar field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "sidecar",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::not_null("sizeInBytes", DataType::LONG),
                StructField::not_null("modificationTime", DataType::LONG),
                tags_field(),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_transaction_schema() {
        let schema = get_log_schema()
            .project(&["txn"])
            .expect("Couldn't get transaction field");

        let expected = Arc::new(StructType::new([StructField::nullable(
            "txn",
            StructType::new([
                StructField::not_null("appId", DataType::STRING),
                StructField::not_null("version", DataType::LONG),
                StructField::nullable("lastUpdated", DataType::LONG),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_commit_info_schema() {
        let schema = get_log_schema()
            .project(&["commitInfo"])
            .expect("Couldn't get commitInfo field");

        let expected = Arc::new(StructType::new(vec![StructField::nullable(
            "commitInfo",
            StructType::new(vec![
                StructField::nullable("timestamp", DataType::LONG),
                StructField::nullable("inCommitTimestamp", DataType::LONG),
                StructField::nullable("operation", DataType::STRING),
                StructField::nullable(
                    "operationParameters",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
                StructField::nullable("kernelVersion", DataType::STRING),
                StructField::nullable(
                    "engineCommitInfo",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_validate_protocol() {
        let invalid_protocols = [
            Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: None,
                writer_features: Some(vec![]),
            },
            Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: Some(vec![]),
                writer_features: None,
            },
            Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: None,
                writer_features: None,
            },
        ];
        for Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        } in invalid_protocols
        {
            assert!(matches!(
                Protocol::try_new(
                    min_reader_version,
                    min_writer_version,
                    reader_features,
                    writer_features
                ),
                Err(Error::InvalidProtocol(_)),
            ));
        }
    }

    #[test]
    fn test_v2_checkpoint_unsupported() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::V2Checkpoint]),
            Some([ReaderFeatures::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_err());

        let protocol = Protocol::try_new(
            4,
            7,
            Some([ReaderFeatures::V2Checkpoint]),
            Some([ReaderFeatures::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_err());
    }

    #[test]
    fn test_ensure_read_supported() {
        let protocol = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![]),
            writer_features: Some(vec![]),
        };
        assert!(protocol.ensure_read_supported().is_ok());

        let empty_features: [String; 0] = [];
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::V2Checkpoint]),
            Some(&empty_features),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_err());

        let protocol = Protocol::try_new(
            3,
            7,
            Some(&empty_features),
            Some([WriterFeatures::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::V2Checkpoint]),
            Some([WriterFeatures::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_err());

        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 7,
            reader_features: None,
            writer_features: None,
        };
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol {
            min_reader_version: 2,
            min_writer_version: 7,
            reader_features: None,
            writer_features: None,
        };
        assert!(protocol.ensure_read_supported().is_ok());
    }

    #[test]
    fn test_ensure_write_supported() {
        let protocol = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![]),
            writer_features: Some(vec![]),
        };
        assert!(protocol.ensure_write_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            Some([WriterFeatures::DeletionVectors]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_err());
    }

    #[test]
    fn test_ensure_supported_features() {
        let supported_features = [
            ReaderFeatures::ColumnMapping,
            ReaderFeatures::DeletionVectors,
        ]
        .into_iter()
        .collect();
        let table_features = vec![ReaderFeatures::ColumnMapping.to_string()];
        ensure_supported_features(&table_features, &supported_features).unwrap();

        // test unknown features
        let table_features = vec![ReaderFeatures::ColumnMapping.to_string(), "idk".to_string()];
        let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
        match error {
            Error::Unsupported(e) if e ==
                "Unknown ReaderFeatures [\"idk\"]. Supported ReaderFeatures are [ColumnMapping, DeletionVectors]"
            => {},
            Error::Unsupported(e) if e ==
                "Unknown ReaderFeatures [\"idk\"]. Supported ReaderFeatures are [DeletionVectors, ColumnMapping]"
            => {},
            _ => panic!("Expected unsupported error"),
        }
    }
}
