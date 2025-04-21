//! Provides parsing and manipulation of the various actions defined in the [Delta
//! specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::LazyLock;

use self::deletion_vector::DeletionVectorDescriptor;
use crate::actions::schemas::GetStructField;
use crate::internal_mod;
use crate::schema::{SchemaRef, StructType};
use crate::table_features::{
    ReaderFeature, WriterFeature, SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::EvaluationHandlerExtension;
use crate::{DeltaResult, Engine, EngineData, Error, FileMeta, RowVisitor as _};

use url::Url;
use visitors::{MetadataVisitor, ProtocolVisitor};

use delta_kernel_derive::{internal_api, Schema};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

pub mod deletion_vector;
pub mod set_transaction;

pub(crate) mod schemas;
internal_mod!(pub(crate) mod visitors);

#[internal_api]
pub(crate) const ADD_NAME: &str = "add";
#[internal_api]
pub(crate) const REMOVE_NAME: &str = "remove";
#[internal_api]
pub(crate) const METADATA_NAME: &str = "metaData";
#[internal_api]
pub(crate) const PROTOCOL_NAME: &str = "protocol";
#[internal_api]
pub(crate) const SET_TRANSACTION_NAME: &str = "txn";
#[internal_api]
pub(crate) const COMMIT_INFO_NAME: &str = "commitInfo";
#[internal_api]
pub(crate) const CDC_NAME: &str = "cdc";
#[internal_api]
pub(crate) const SIDECAR_NAME: &str = "sidecar";
#[internal_api]
pub(crate) const CHECKPOINT_METADATA_NAME: &str = "checkpointMetadata";

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
        Option::<CheckpointMetadata>::get_struct_field(CHECKPOINT_METADATA_NAME),
        // We don't support the following actions yet
        //Option::<DomainMetadata>::get_struct_field(DOMAIN_METADATA_NAME),
    ])
    .into()
});

static LOG_COMMIT_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([Option::<CommitInfo>::get_struct_field(COMMIT_INFO_NAME)]).into()
});

static LOG_TXN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([Option::<SetTransaction>::get_struct_field(
        SET_TRANSACTION_NAME,
    )])
    .into()
});

#[internal_api]
pub(crate) fn get_log_schema() -> &'static SchemaRef {
    &LOG_SCHEMA
}

#[internal_api]
pub(crate) fn get_log_add_schema() -> &'static SchemaRef {
    &LOG_ADD_SCHEMA
}

pub(crate) fn get_log_commit_info_schema() -> &'static SchemaRef {
    &LOG_COMMIT_INFO_SCHEMA
}

pub(crate) fn get_log_txn_schema() -> &'static SchemaRef {
    &LOG_TXN_SCHEMA
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[internal_api]
#[cfg_attr(test, derive(Serialize), serde(rename_all = "camelCase"))]
pub(crate) struct Format {
    /// Name of the encoding for files in this table
    pub(crate) provider: String,
    /// A map containing configuration options for the format
    pub(crate) options: HashMap<String, String>,
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
#[internal_api]
pub(crate) struct Metadata {
    /// Unique identifier for this table
    pub(crate) id: String,
    /// User-provided identifier for this table
    pub(crate) name: Option<String>,
    /// User-provided description for this table
    pub(crate) description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub(crate) format: Format,
    /// Schema of the table
    pub(crate) schema_string: String,
    /// Column names by which the data should be partitioned
    pub(crate) partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub(crate) created_time: Option<i64>,
    /// Configuration options for the metadata action. These are parsed into [`TableProperties`].
    pub(crate) configuration: HashMap<String, String>,
}

impl Metadata {
    pub(crate) fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Metadata>> {
        let mut visitor = MetadataVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.metadata)
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    pub(crate) fn parse_schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn partition_columns(&self) -> &Vec<String> {
        &self.partition_columns
    }

    /// Parse the metadata configuration HashMap<String, String> into a TableProperties struct.
    /// Note that parsing is infallible -- any items that fail to parse are simply propagated
    /// through to the `TableProperties.unknown_properties` field.
    pub(crate) fn parse_table_properties(&self) -> TableProperties {
        TableProperties::from(self.configuration.iter())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Schema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[internal_api]
// TODO move to another module so that we disallow constructing this struct without using the
// try_new function.
pub(crate) struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<Vec<ReaderFeature>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<WriterFeature>>,
}

fn parse_features<T>(features: Option<impl IntoIterator<Item = impl ToString>>) -> Option<Vec<T>>
where
    T: FromStr,
    T::Err: Debug,
{
    features
        .map(|fs| {
            fs.into_iter()
                .map(|f| T::from_str(&f.to_string()))
                .collect()
        })
        .transpose()
        .expect("Parsing FromStr should never fail with strum 'default'")
}

impl Protocol {
    /// Try to create a new Protocol instance from reader/writer versions and table features. This
    /// can fail if the protocol is invalid.
    pub(crate) fn try_new(
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Option<impl IntoIterator<Item = impl ToString>>,
        writer_features: Option<impl IntoIterator<Item = impl ToString>>,
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

        let reader_features = parse_features(reader_features);
        let writer_features = parse_features(writer_features);

        Ok(Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        })
    }

    /// Create a new Protocol by visiting the EngineData and extracting the first protocol row into
    /// a Protocol instance. If no protocol row is found, returns Ok(None).
    pub(crate) fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Protocol>> {
        let mut visitor = ProtocolVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.protocol)
    }

    /// This protocol's minimum reader version
    #[internal_api]
    pub(crate) fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    /// This protocol's minimum writer version
    #[internal_api]
    pub(crate) fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    /// Get the reader features for the protocol
    pub(crate) fn reader_features(&self) -> Option<&[ReaderFeature]> {
        self.reader_features.as_deref()
    }

    /// Get the writer features for the protocol
    pub(crate) fn writer_features(&self) -> Option<&[WriterFeature]> {
        self.writer_features.as_deref()
    }

    /// True if this protocol has the requested reader feature
    pub(crate) fn has_reader_feature(&self, feature: &ReaderFeature) -> bool {
        self.reader_features()
            .is_some_and(|features| features.contains(feature))
    }

    /// True if this protocol has the requested writer feature
    pub(crate) fn has_writer_feature(&self, feature: &WriterFeature) -> bool {
        self.writer_features()
            .is_some_and(|features| features.contains(feature))
    }

    /// Check if reading a table with this protocol is supported. That is: does the kernel support
    /// the specified protocol reader version and all enabled reader features? If yes, returns unit
    /// type, otherwise will return an error.
    pub(crate) fn ensure_read_supported(&self) -> DeltaResult<()> {
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
    pub(crate) fn ensure_write_supported(&self) -> DeltaResult<()> {
        match &self.writer_features {
            Some(writer_features) if self.min_writer_version == 7 => {
                // if we're on version 7, make sure we support all the specified features
                ensure_supported_features(writer_features, &SUPPORTED_WRITER_FEATURES)
            }
            Some(_) => {
                // there are features, but we're not on 7, so the protocol is actually broken
                Err(Error::unsupported(
                    "Tables with min writer version != 7 should not have table features.",
                ))
            }
            None => {
                // no features, we currently only support version 1 or 2 in this case
                require!(
                    self.min_writer_version == 1 || self.min_writer_version == 2,
                    Error::unsupported(
                        "Currently delta-kernel-rs can only write to tables with protocol.minWriterVersion = 1, 2, or 7"
                    )
                );
                Ok(())
            }
        }
    }
}

// given `table_features`, check if they are subset of `supported_features`
pub(crate) fn ensure_supported_features<T>(
    table_features: &[T],
    supported_features: &[T],
) -> DeltaResult<()>
where
    T: Display + FromStr + Hash + Eq,
    <T as FromStr>::Err: Display,
{
    // first check if all features are supported, else we proceed to craft an error message
    if table_features
        .iter()
        .all(|feature| supported_features.contains(feature))
    {
        return Ok(());
    }

    // we get the type name (ReaderFeature/WriterFeature) for better error messages
    let features_type = std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("table feature");

    // NB: we didn't do this above to avoid allocation in the common case
    let mut unsupported = table_features
        .iter()
        .filter(|feature| !supported_features.contains(*feature));

    Err(Error::Unsupported(format!(
        "Unknown {}s: \"{}\". Supported {}s: \"{}\"",
        features_type,
        unsupported.join("\", \""),
        features_type,
        supported_features.iter().join("\", \""),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[internal_api]
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
#[internal_api]
pub(crate) struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// A map from partition column to value for this logical file. This map can contain null in the
    /// values meaning a partition is null. We drop those values from this map, due to the
    /// `drop_null_container_values` annotation. This means an engine can assume that if a partition
    /// is found in [`Metadata`] `partition_columns`, but not in this map, its value is null.
    #[drop_null_container_values]
    pub(crate) partition_values: HashMap<String, String>,

    /// The size of this data file in bytes
    pub(crate) size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub(crate) modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

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
    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[internal_api]
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
#[internal_api]
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
#[internal_api]
pub(crate) struct SetTransaction {
    /// A unique identifier for the application performing the transaction.
    pub(crate) app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub(crate) version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub(crate) last_updated: Option<i64>,
}

impl SetTransaction {
    pub(crate) fn new(app_id: String, version: i64, last_updated: Option<i64>) -> Self {
        Self {
            app_id,
            version,
            last_updated,
        }
    }

    pub(crate) fn into_engine_data(self, engine: &dyn Engine) -> DeltaResult<Box<dyn EngineData>> {
        let values = [
            self.app_id.into(),
            self.version.into(),
            self.last_updated.into(),
        ];
        let evaluator = engine.evaluation_handler();
        evaluator.create_one(get_log_txn_schema().clone(), &values)
    }
}

/// The sidecar action references a sidecar file which provides some of the checkpoint's
/// file actions. This action is only allowed in checkpoints following the V2 spec.
///
/// [More info]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
#[derive(Schema, Debug, PartialEq)]
#[internal_api]
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

impl Sidecar {
    /// Convert a Sidecar record to a FileMeta.
    ///
    /// This helper first builds the URL by joining the provided log_root with
    /// the "_sidecars/" folder and the given sidecar path.
    pub(crate) fn to_filemeta(&self, log_root: &Url) -> DeltaResult<FileMeta> {
        Ok(FileMeta {
            location: log_root.join("_sidecars/")?.join(&self.path)?,
            last_modified: self.modification_time,
            size: self.size_in_bytes.try_into().map_err(|_| {
                Error::generic(format!(
                    "Failed to convert sidecar size {} to usize",
                    self.size_in_bytes
                ))
            })?,
        })
    }
}

/// The CheckpointMetadata action describes details about a checkpoint following the V2 specification.
///
/// [More info]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-metadata
#[derive(Debug, Clone, PartialEq, Eq, Schema)]
#[internal_api]
pub(crate) struct CheckpointMetadata {
    /// The version of the V2 spec checkpoint.
    ///
    /// Currently using `i64` for compatibility with other actions' representations.
    /// Future work will address converting numeric fields to unsigned types (e.g., `u64`) where
    /// semantically appropriate (e.g., for version, size, timestamps, etc.).
    /// See issue #786 for tracking progress.
    pub(crate) version: i64,

    /// Map containing any additional metadata about the V2 spec checkpoint.
    pub(crate) tags: Option<HashMap<String, String>>,
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
    fn test_checkpoint_metadata_schema() {
        let schema = get_log_schema()
            .project(&[CHECKPOINT_METADATA_NAME])
            .expect("Couldn't get checkpointMetadata field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "checkpointMetadata",
            StructType::new([
                StructField::not_null("version", DataType::LONG),
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
    fn test_v2_checkpoint_supported() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::V2Checkpoint]),
            Some([ReaderFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol::try_new(
            4,
            7,
            Some([ReaderFeature::V2Checkpoint]),
            Some([ReaderFeature::V2Checkpoint]),
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
            Some([ReaderFeature::V2Checkpoint]),
            Some(&empty_features),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some(&empty_features),
            Some([WriterFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::V2Checkpoint]),
            Some([WriterFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

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
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some(vec![
                WriterFeature::AppendOnly,
                WriterFeature::DeletionVectors,
                WriterFeature::Invariants,
            ]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::DeletionVectors]),
            Some([WriterFeature::RowTracking]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_err());
    }

    #[test]
    fn test_ensure_supported_features() {
        let supported_features = [ReaderFeature::ColumnMapping, ReaderFeature::DeletionVectors];
        let table_features = vec![ReaderFeature::ColumnMapping];
        ensure_supported_features(&table_features, &supported_features).unwrap();

        // test unknown features
        let table_features = vec![ReaderFeature::ColumnMapping, ReaderFeature::unknown("idk")];
        let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
        match error {
            Error::Unsupported(e) if e ==
                "Unknown ReaderFeatures: \"idk\". Supported ReaderFeatures: \"columnMapping\", \"deletionVectors\""
            => {},
            _ => panic!("Expected unsupported error, got: {error}"),
        }
    }

    #[test]
    fn test_parse_table_feature_never_fails() {
        // parse a non-str
        let features = Some([5]);
        let expected = Some(vec![ReaderFeature::unknown("5")]);
        assert_eq!(parse_features::<ReaderFeature>(features), expected);

        // weird strs
        let features = Some(["", "absurD_)(+13%^⚙️"]);
        let expected = Some(vec![
            ReaderFeature::unknown(""),
            ReaderFeature::unknown("absurD_)(+13%^⚙️"),
        ]);
        assert_eq!(parse_features::<ReaderFeature>(features), expected);
    }
}
