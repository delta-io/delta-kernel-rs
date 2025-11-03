//! Provides parsing and manipulation of the various actions defined in the [Delta
//! specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use self::deletion_vector::DeletionVectorDescriptor;
use crate::expressions::{ArrayData, MapData, Scalar, StructData};
use crate::schema::{
    ArrayType, DataType, MapType, SchemaRef, StructField, StructType, ToSchema as _,
};
use crate::table_features::{
    FeatureType, TableFeature, SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension as _, FileMeta,
    IntoEngineData, RowVisitor as _,
};

use url::Url;
use visitors::{MetadataVisitor, ProtocolVisitor};

use delta_kernel_derive::{internal_api, IntoEngineData, ToSchema};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");
const UNKNOWN_OPERATION: &str = "UNKNOWN";

pub mod deletion_vector;
pub mod set_transaction;

pub(crate) mod crc;
pub(crate) mod domain_metadata;

// see comment in ../lib.rs for the path module for why we include this way
#[cfg(feature = "internal-api")]
pub mod visitors;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod visitors;

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
#[internal_api]
pub(crate) const DOMAIN_METADATA_NAME: &str = "domainMetadata";

pub(crate) const INTERNAL_DOMAIN_PREFIX: &str = "delta.";

static COMMIT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
        StructField::nullable(COMMIT_INFO_NAME, CommitInfo::to_schema()),
        StructField::nullable(CDC_NAME, Cdc::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
    ]))
});

static ALL_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(
        get_commit_schema().fields().cloned().chain([
            StructField::nullable(CHECKPOINT_METADATA_NAME, CheckpointMetadata::to_schema()),
            StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()),
        ]),
    ))
});

static LOG_ADD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        ADD_NAME,
        Add::to_schema(),
    )]))
});

static LOG_COMMIT_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        COMMIT_INFO_NAME,
        CommitInfo::to_schema(),
    )]))
});

static LOG_TXN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        SET_TRANSACTION_NAME,
        SetTransaction::to_schema(),
    )]))
});

static LOG_DOMAIN_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        DOMAIN_METADATA_NAME,
        DomainMetadata::to_schema(),
    )]))
});

#[internal_api]
/// Gets the schema for all actions that can appear in commits
/// logs.  This excludes actions that can only appear in checkpoints.
pub(crate) fn get_commit_schema() -> &'static SchemaRef {
    &COMMIT_SCHEMA
}

#[internal_api]
#[allow(dead_code)]
/// Gets a schema for all actions defined by the delta spec.
pub(crate) fn get_all_actions_schema() -> &'static SchemaRef {
    &ALL_ACTIONS_SCHEMA
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

pub(crate) fn get_log_domain_metadata_schema() -> &'static SchemaRef {
    &LOG_DOMAIN_METADATA_SCHEMA
}

/// Returns true if the schema contains file actions (add or remove)
/// columns.
#[internal_api]
pub(crate) fn schema_contains_file_actions(schema: &SchemaRef) -> bool {
    schema.contains(ADD_NAME) || schema.contains(REMOVE_NAME)
}

/// Nest an existing add action schema in an additional [`ADD_NAME`] struct.
///
/// This is useful for JSON conversion, as it allows us to wrap a dynamically maintained add action
/// schema in a top-level "add" struct.
pub(crate) fn as_log_add_schema(schema: SchemaRef) -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        ADD_NAME, schema,
    )]))
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(
    any(test, feature = "internal-api"),
    derive(Serialize, Deserialize),
    serde(rename_all = "camelCase")
)]
#[internal_api]
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

impl TryFrom<Format> for Scalar {
    type Error = Error;

    fn try_from(format: Format) -> DeltaResult<Self> {
        let provider = Scalar::from(format.provider);
        let options = MapData::try_new(
            MapType::new(DataType::STRING, DataType::STRING, false),
            format.options,
        )
        .map(Scalar::Map)?;
        Ok(Scalar::Struct(StructData::try_new(
            Format::to_schema().into_fields().collect(),
            vec![provider, options],
        )?))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(
    any(test, feature = "internal-api"),
    derive(Serialize, Deserialize),
    serde(rename_all = "camelCase")
)]
#[internal_api]
pub(crate) struct Metadata {
    /// Unique identifier for this table
    id: String,
    /// User-provided identifier for this table
    name: Option<String>,
    /// User-provided description for this table
    description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    format: Format,
    /// Schema of the table
    schema_string: String,
    /// Column names by which the data should be partitioned
    partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    created_time: Option<i64>,
    /// Configuration options for the metadata action. These are parsed into [`TableProperties`].
    configuration: HashMap<String, String>,
}

impl Metadata {
    /// Create a new [`Metadata`] instances.
    ///
    /// # Errors
    ///
    /// Returns an error if there are any metadata columns in the schema.
    // TODO: remove allow(dead_code) after we use this API in CREATE TABLE, etc.
    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn try_new(
        name: Option<String>,
        description: Option<String>,
        schema: StructType,
        partition_columns: Vec<String>,
        created_time: i64,
        configuration: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        // Validate that the schema does not contain metadata columns
        // Note: We don't have to look for nested metadata columns because that is already validated
        // when creating a StructType.
        if let Some(metadata_field) = schema.fields().find(|field| field.is_metadata_column()) {
            return Err(Error::Schema(format!(
                "Table schema must not contain metadata columns. Found metadata column: '{}'",
                metadata_field.name
            )));
        }

        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            description,
            // As of Delta Lake 0.3.0, user-facing APIs only allow the creation of tables where
            // format = 'parquet' and options = {}. Support for reading other formats is present
            // both for legacy reasons and to enable possible support for other formats in the
            // future (See delta-io/delta#87).
            format: Format::default(),
            schema_string: serde_json::to_string(&schema)?,
            partition_columns,
            created_time: Some(created_time),
            configuration,
        })
    }

    #[internal_api]
    pub(crate) fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Metadata>> {
        let mut visitor = MetadataVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.metadata)
    }

    // TODO(#1068/1069): make these just pub directly or make better internal_api macro for fields
    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn created_time(&self) -> Option<i64> {
        self.created_time
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    #[internal_api]
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
    #[internal_api]
    pub(crate) fn parse_table_properties(&self) -> TableProperties {
        TableProperties::from(self.configuration.iter())
    }
}

// TODO: derive IntoEngineData instead (see issue #1083)
impl IntoEngineData for Metadata {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // For format, we need to provide individual scalars for provider and options
        let values = [
            self.id.into(),
            self.name.into(),
            self.description.into(),
            self.format.provider.into(),
            self.format.options.try_into()?,
            self.schema_string.into(),
            self.partition_columns.try_into()?,
            self.created_time.into(),
            self.configuration.try_into()?,
        ];

        engine.evaluation_handler().create_one(schema, &values)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, ToSchema, Serialize, Deserialize)]
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
    reader_features: Option<Vec<TableFeature>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<TableFeature>>,
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
        .ok()?
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
        let reader_features = parse_features(reader_features);
        let writer_features = parse_features(writer_features);

        let protocol = Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        };
        protocol.validate_table_features()?;

        Ok(protocol)
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
    #[internal_api]
    pub(crate) fn reader_features(&self) -> Option<&[TableFeature]> {
        self.reader_features.as_deref()
    }

    /// Get the writer features for the protocol
    #[internal_api]
    pub(crate) fn writer_features(&self) -> Option<&[TableFeature]> {
        self.writer_features.as_deref()
    }

    /// True if this protocol has the requested feature
    pub(crate) fn has_table_feature(&self, feature: &TableFeature) -> bool {
        // Since each reader features is a subset of writer features, we only check writer feature
        self.writer_features()
            .is_some_and(|features| features.contains(feature))
    }

    /// Validates the relationship between reader features and writer features in the protocol.
    pub(crate) fn validate_table_features(&self) -> DeltaResult<()> {
        // The protocol states that Reader features may be present if and only if the min_reader_version is 3
        if self.min_reader_version == 3 {
            require!(
                self.reader_features.is_some(),
                Error::invalid_protocol(
                    "Reader features must be present when minimum reader version = 3"
                )
            );
        } else {
            require!(
                self.reader_features.is_none(),
                Error::invalid_protocol(
                    "Reader features must not be present when minimum reader version != 3"
                )
            );
        }

        // The protocol states that Writer features may be present if and only if the min_writer_version is 7
        if self.min_writer_version == 7 {
            require!(
                self.writer_features.is_some(),
                Error::invalid_protocol(
                    "Writer features must be present when minimum writer version = 7"
                )
            );
        } else {
            require!(
                self.writer_features.is_none(),
                Error::invalid_protocol(
                    "Writer features must not be present when minimum writer version != 7"
                )
            );
        }

        match (&self.reader_features, &self.writer_features) {
            (Some(reader_features), Some(writer_features)) => {
                // Check all reader features are ReaderWriter and present in writer features.
                // Unknown features are treated as potentially ReaderWriter for forward compatibility.
                let check_r = reader_features.iter().all(|feature| {
                    matches!(
                        feature.feature_type(),
                        FeatureType::ReaderWriter | FeatureType::Unknown
                    ) && writer_features.contains(feature)
                });
                require!(
                    check_r,
                    Error::invalid_protocol(
                        "Reader features must contain only ReaderWriter features that are also listed in writer features"
                    )
                );

                // Check all writer features that are ReaderWriter must also be in reader features
                // Unknown features are treated as potentially Writer-only for forward compatibility.
                let check_w = writer_features
                    .iter()
                    .all(|feature| match feature.feature_type() {
                        FeatureType::Writer | FeatureType::Unknown => true,
                        FeatureType::ReaderWriter => reader_features.contains(feature),
                    });
                require!(
                    check_w,
                    Error::invalid_protocol(
                        "Writer features must be Writer-only or also listed in reader features"
                    )
                );
                Ok(())
            }
            (None, None) => Ok(()),
            (None, Some(writer_features)) => {
                // Unknown features are treated as potentially Writer-only for forward compatibility.
                require!(
                    writer_features.iter().all(|feature| matches!(
                        feature.feature_type(),
                        FeatureType::Writer | FeatureType::Unknown
                    )),
                    Error::invalid_protocol(
                        "Writer features must be Writer-only or also listed in reader features"
                    )
                );
                Ok(())
            }
            (Some(_), None) => Err(Error::invalid_protocol(
                "Reader features should be present in writer features",
            )),
        }
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
        #[cfg(feature = "catalog-managed")]
        require!(
            !self.is_catalog_managed(),
            Error::unsupported("Writes are not yet supported for catalog-managed tables")
        );
        match &self.writer_features {
            Some(writer_features) if self.min_writer_version == 7 => {
                // if we're on version 7, make sure we support all the specified features
                ensure_supported_features(writer_features, &SUPPORTED_WRITER_FEATURES)?;

                // ensure that there is no illegal combination of features
                if writer_features.contains(&TableFeature::RowTracking)
                    && !writer_features.contains(&TableFeature::DomainMetadata)
                {
                    Err(Error::invalid_protocol(
                        "rowTracking feature requires domainMetadata to also be enabled",
                    ))
                } else {
                    Ok(())
                }
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

    #[cfg(feature = "catalog-managed")]
    pub(crate) fn is_catalog_managed(&self) -> bool {
        self.has_table_feature(&TableFeature::CatalogManaged)
            || self.has_table_feature(&TableFeature::CatalogOwnedPreview)
    }
}

// TODO: implement Scalar::From<HashMap<K, V>> so we can derive IntoEngineData using a macro (issue#1083)
impl IntoEngineData for Protocol {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>> {
        fn features_to_scalar<T>(
            features: Option<impl IntoIterator<Item = T>>,
        ) -> DeltaResult<Scalar>
        where
            T: Into<Scalar>,
        {
            match features {
                Some(features) => {
                    let features: Vec<Scalar> = features.into_iter().map(Into::into).collect();
                    Ok(Scalar::Array(ArrayData::try_new(
                        ArrayType::new(DataType::STRING, false),
                        features,
                    )?))
                }
                None => Ok(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
                    DataType::STRING,
                    false,
                ))))),
            }
        }

        let values = [
            self.min_reader_version.into(),
            self.min_writer_version.into(),
            features_to_scalar(self.reader_features)?,
            features_to_scalar(self.writer_features)?,
        ];

        engine.evaluation_handler().create_one(schema, &values)
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

    // we get the type name (TableFeature) for better error messages
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

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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
    /// A place for the engine to store additional metadata associated with this commit
    pub(crate) engine_info: Option<String>,
    /// A unique transaction identifier for this commit.
    pub(crate) txn_id: Option<String>,
}

impl CommitInfo {
    pub(crate) fn new(
        timestamp: i64,
        in_commit_timestamp: Option<i64>,
        operation: Option<String>,
        engine_info: Option<String>,
    ) -> Self {
        Self {
            timestamp: Some(timestamp),
            in_commit_timestamp,
            operation: Some(operation.unwrap_or_else(|| UNKNOWN_OPERATION.to_string())),
            operation_parameters: None,
            kernel_version: Some(format!("v{KERNEL_VERSION}")),
            engine_info,
            txn_id: Some(uuid::Uuid::new_v4().to_string()),
        }
    }
}

// TODO: implement Scalar::From<HashMap<K, V>> so we can derive IntoEngineData using a macro (issue#1083)
impl IntoEngineData for CommitInfo {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let values = [
            self.timestamp.into(),
            self.in_commit_timestamp.into(),
            self.operation.into(),
            self.operation_parameters.unwrap_or_default().try_into()?,
            self.kernel_version.into(),
            self.engine_info.into(),
            self.txn_id.into(),
        ];

        engine.evaluation_handler().create_one(schema, &values)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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
    /// `allow_null_container_values` annotation allowing them and because [`materialize`] drops
    /// null values. This means an engine can assume that if a partition is found in
    /// [`Metadata::partition_columns`] but not in this map, its value is null.
    ///
    /// [`materialize`]: crate::engine_data::EngineMap::materialize
    #[allow_null_container_values]
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
    /// row within the file to the base Row ID.
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

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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
    /// `allow_null_container_values` annotation allowing them and because [`materialize`] drops
    /// null values. This means an engine can assume that if a partition is found in
    /// [`Metadata::partition_columns`] but not in this map, its value is null.
    ///
    /// [`materialize`]: crate::engine_data::EngineMap::materialize
    #[allow_null_container_values]
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

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, IntoEngineData)]
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
}

/// The sidecar action references a sidecar file which provides some of the checkpoint's
/// file actions. This action is only allowed in checkpoints following the V2 spec.
///
/// [More info]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
#[derive(ToSchema, Debug, PartialEq)]
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
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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

/// The [DomainMetadata] action contains a configuration (string) for a named metadata domain. Two
/// overlapping transactions conflict if they both contain a domain metadata action for the same
/// metadata domain.
///
/// Note that the `delta.*` domain is reserved for internal use.
///
/// [DomainMetadata]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
#[derive(Debug, Clone, PartialEq, Eq, ToSchema, IntoEngineData)]
#[internal_api]
pub(crate) struct DomainMetadata {
    domain: String,
    configuration: String,
    removed: bool,
}

impl DomainMetadata {
    /// Create a new DomainMetadata action.
    pub(crate) fn new(domain: String, configuration: String) -> Self {
        Self {
            domain,
            configuration,
            removed: false,
        }
    }

    /// Create a new DomainMetadata action to remove a domain.
    pub(crate) fn remove(domain: String, configuration: String) -> Self {
        Self {
            domain,
            configuration,
            removed: true,
        }
    }

    // returns true if the domain metadata is an system-controlled domain (all domains that start
    // with "delta.")
    #[allow(unused)]
    pub(crate) fn is_internal(&self) -> bool {
        self.domain.starts_with(INTERNAL_DOMAIN_PREFIX)
    }

    pub(crate) fn domain(&self) -> &str {
        &self.domain
    }

    pub(crate) fn configuration(&self) -> &str {
        &self.configuration
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arrow::array::{
            Array, BooleanArray, Int32Array, Int64Array, ListArray, ListBuilder, MapBuilder,
            MapFieldNames, RecordBatch, StringArray, StringBuilder, StructArray,
        },
        arrow::datatypes::{DataType as ArrowDataType, Field, Schema},
        arrow::json::ReaderBuilder,
        engine::{arrow_data::ArrowEngineData, arrow_expression::ArrowEvaluationHandler},
        schema::{ArrayType, DataType, MapType, StructField},
        utils::test_utils::assert_result_error_with_message,
        Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
    };
    use serde_json::json;

    // duplicated
    struct ExprEngine(Arc<dyn EvaluationHandler>);

    impl ExprEngine {
        fn new() -> Self {
            ExprEngine(Arc::new(ArrowEvaluationHandler))
        }
    }

    impl Engine for ExprEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.0.clone()
        }

        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }

        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }

        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            unimplemented!()
        }
    }

    fn create_string_map_builder(
        nullable_values: bool,
    ) -> MapBuilder<StringBuilder, StringBuilder> {
        MapBuilder::new(
            Some(MapFieldNames {
                entry: "key_value".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        )
        .with_values_field(Field::new(
            "value".to_string(),
            ArrowDataType::Utf8,
            nullable_values,
        ))
    }

    #[test]
    fn test_metadata_schema() {
        let schema = get_commit_schema()
            .project(&[METADATA_NAME])
            .expect("Couldn't get metaData field");

        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "metaData",
            StructType::new_unchecked([
                StructField::not_null("id", DataType::STRING),
                StructField::nullable("name", DataType::STRING),
                StructField::nullable("description", DataType::STRING),
                StructField::not_null(
                    "format",
                    StructType::new_unchecked([
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
        let schema = get_commit_schema()
            .project(&[ADD_NAME])
            .expect("Couldn't get add field");

        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "add",
            StructType::new_unchecked([
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
            DataType::struct_type_unchecked([
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
        let schema = get_commit_schema()
            .project(&[REMOVE_NAME])
            .expect("Couldn't get remove field");
        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "remove",
            StructType::new_unchecked([
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
        let schema = get_commit_schema()
            .project(&[CDC_NAME])
            .expect("Couldn't get cdc field");
        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "cdc",
            StructType::new_unchecked([
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
        let schema = Sidecar::to_schema();
        let expected = StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("sizeInBytes", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            tags_field(),
        ]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_checkpoint_metadata_schema() {
        let schema = get_all_actions_schema()
            .project(&[CHECKPOINT_METADATA_NAME])
            .expect("Couldn't get checkpointMetadata field");
        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "checkpointMetadata",
            StructType::new_unchecked([
                StructField::not_null("version", DataType::LONG),
                tags_field(),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_transaction_schema() {
        let schema = get_commit_schema()
            .project(&["txn"])
            .expect("Couldn't get transaction field");

        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "txn",
            StructType::new_unchecked([
                StructField::not_null("appId", DataType::STRING),
                StructField::not_null("version", DataType::LONG),
                StructField::nullable("lastUpdated", DataType::LONG),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_commit_info_schema() {
        let schema = get_commit_schema()
            .project(&["commitInfo"])
            .expect("Couldn't get commitInfo field");

        let expected = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "commitInfo",
            StructType::new_unchecked(vec![
                StructField::nullable("timestamp", DataType::LONG),
                StructField::nullable("inCommitTimestamp", DataType::LONG),
                StructField::nullable("operation", DataType::STRING),
                StructField::nullable(
                    "operationParameters",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
                StructField::nullable("kernelVersion", DataType::STRING),
                StructField::nullable("engineInfo", DataType::STRING),
                StructField::nullable("txnId", DataType::STRING),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_domain_metadata_schema() {
        let schema = get_commit_schema()
            .project(&[DOMAIN_METADATA_NAME])
            .expect("Couldn't get domainMetadata field");
        let expected = Arc::new(StructType::new_unchecked([StructField::nullable(
            "domainMetadata",
            StructType::new_unchecked([
                StructField::not_null("domain", DataType::STRING),
                StructField::not_null("configuration", DataType::STRING),
                StructField::not_null("removed", DataType::BOOLEAN),
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
    fn test_validate_table_features_invalid() {
        // (reader_feature, writer_feature)
        let invalid_features = [
            // ReaderWriter feature not present in writer features
            (
                Some(vec![TableFeature::DeletionVectors]),
                Some(vec![TableFeature::AppendOnly]),
                "Reader features must contain only ReaderWriter features that are also listed in writer features",
            ),
            (Some(vec![TableFeature::DeletionVectors]), Some(vec![]), "Reader features must contain only ReaderWriter features that are also listed in writer features"),
            // ReaderWriter feature not present in reader features
            (Some(vec![]), Some(vec![TableFeature::DeletionVectors]), "Writer features must be Writer-only or also listed in reader features"),
            (
                Some(vec![TableFeature::VariantType]),
                Some(vec![
                    TableFeature::VariantType,
                    TableFeature::DeletionVectors,
                ]),
                "Writer features must be Writer-only or also listed in reader features",
            ),
            // Writer only feature present in reader features
            (
                Some(vec![TableFeature::AppendOnly]),
                Some(vec![TableFeature::AppendOnly]),
                "Reader features must contain only ReaderWriter features that are also listed in writer features",
            ),
        ];

        for (reader_features, writer_features, error_msg) in invalid_features {
            let protocol = Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features,
                writer_features,
            };

            let res = protocol.validate_table_features();
            assert!(
                matches!(
                    protocol.validate_table_features(),
                    Err(Error::InvalidProtocol(error)) if error.to_string().eq(error_msg)
                ),
                "Expected:\t{error_msg}\nBut got:{res:?}\n"
            );
        }
    }

    #[test]
    fn test_validate_table_features_unknown() {
        // Unknown features are allowed during validation for forward compatibility,
        // but will be rejected when trying to use the protocol (ensure_read_supported/ensure_write_supported)

        // Test unknown features in reader - validation passes
        let protocol = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![TableFeature::Unknown("unknown_reader".to_string())]),
            writer_features: Some(vec![TableFeature::Unknown("unknown_reader".to_string())]),
        };
        assert!(protocol.validate_table_features().is_ok());
        // But ensure_read_supported should fail
        assert!(matches!(
            protocol.ensure_read_supported(),
            Err(Error::Unsupported(_))
        ));

        // Test unknown features in writer - validation passes
        let protocol = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![]),
            writer_features: Some(vec![TableFeature::Unknown("unknown_writer".to_string())]),
        };
        assert!(protocol.validate_table_features().is_ok());
        // But ensure_write_supported should fail
        assert!(matches!(
            protocol.ensure_write_supported(),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn test_validate_table_features_valid() {
        // (reader_feature, writer_feature)
        let valid_features = [
            // ReaderWriter feature present in both reader/writer features,
            // Writer only feature present in writer feature
            (
                Some(vec![TableFeature::DeletionVectors]),
                Some(vec![TableFeature::DeletionVectors]),
            ),
            (Some(vec![]), Some(vec![TableFeature::AppendOnly])),
            (
                Some(vec![TableFeature::VariantType]),
                Some(vec![TableFeature::VariantType, TableFeature::AppendOnly]),
            ),
            // Unknown feature may be ReaderWriter or Writer only (for forward compatibility)
            (
                Some(vec![TableFeature::Unknown("rw".to_string())]),
                Some(vec![
                    TableFeature::Unknown("rw".to_string()),
                    TableFeature::Unknown("w".to_string()),
                ]),
            ),
            // Empty feature set is valid
            (Some(vec![]), Some(vec![])),
        ];

        for (reader_features, writer_features) in valid_features {
            let protocol = Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features,
                writer_features,
            };

            assert!(matches!(protocol.validate_table_features(), Ok(()),));
        }
    }

    #[test]
    fn test_v2_checkpoint_supported() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::V2Checkpoint]),
            Some([TableFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());
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

        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::V2Checkpoint]),
            Some([TableFeature::V2Checkpoint]),
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
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![
                TableFeature::AppendOnly,
                TableFeature::DeletionVectors,
                TableFeature::DomainMetadata,
                TableFeature::Invariants,
                TableFeature::RowTracking,
            ]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_ok());

        // Verify that unsupported writer features are rejected
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some([TableFeature::IdentityColumns]),
        )
        .unwrap();
        assert_result_error_with_message(
            protocol.ensure_write_supported(),
            r#"Unsupported: Unknown TableFeatures: "identityColumns". Supported TableFeatures: "appendOnly", "deletionVectors", "domainMetadata", "inCommitTimestamp", "invariants", "rowTracking", "timestampNtz", "variantType", "variantType-preview", "variantShredding-preview""#,
        );

        // Unknown writer features are allowed during creation for forward compatibility,
        // but will fail when trying to write
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some([TableFeature::Unknown("unsupported writer".to_string())]),
        )
        .unwrap();
        assert_result_error_with_message(
            protocol.ensure_write_supported(),
            r#"Unsupported: Unknown TableFeatures: "unsupported writer". Supported TableFeatures: "appendOnly", "deletionVectors", "domainMetadata", "inCommitTimestamp", "invariants", "rowTracking", "timestampNtz", "variantType", "variantType-preview", "variantShredding-preview""#,
        );
    }

    #[test]
    fn test_illegal_writer_feature_combination() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some(vec![
                // No domain metadata even though that is required
                TableFeature::RowTracking,
            ]),
        )
        .unwrap();

        assert_result_error_with_message(
            protocol.ensure_write_supported(),
            "rowTracking feature requires domainMetadata to also be enabled",
        );
    }

    #[test]
    fn test_ensure_supported_features() {
        let supported_features = [TableFeature::ColumnMapping, TableFeature::DeletionVectors];
        let table_features = vec![TableFeature::ColumnMapping];
        ensure_supported_features(&table_features, &supported_features).unwrap();

        // test unknown features
        let table_features = vec![TableFeature::ColumnMapping, TableFeature::unknown("idk")];
        let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
        match error {
            Error::Unsupported(e) if e ==
                "Unknown TableFeatures: \"idk\". Supported TableFeatures: \"columnMapping\", \"deletionVectors\""
            => {},
            _ => panic!("Expected unsupported error, got: {error}"),
        }
    }

    #[test]
    fn test_parse_table_feature_never_fails() {
        // parse a non-str
        let features = Some([5]);
        let expected = Some(vec![TableFeature::unknown("5")]);
        assert_eq!(parse_features::<TableFeature>(features), expected);

        // weird strs
        let features = Some(["", "absurD_)(+13%^⚙️"]);
        let expected = Some(vec![
            TableFeature::unknown(""),
            TableFeature::unknown("absurD_)(+13%^⚙️"),
        ]);
        assert_eq!(parse_features::<TableFeature>(features), expected);
    }

    #[test]
    fn test_no_catalog_managed_writes() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::CatalogManaged]),
            Some([TableFeature::CatalogManaged]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_err());
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::CatalogOwnedPreview]),
            Some([TableFeature::CatalogOwnedPreview]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_err());
    }

    #[test]
    fn test_into_engine_data() {
        let engine = ExprEngine::new();

        let set_transaction = SetTransaction {
            app_id: "app_id".to_string(),
            version: 0,
            last_updated: None,
        };

        let engine_data =
            set_transaction.into_engine_data(SetTransaction::to_schema().into(), &engine);

        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("appId", ArrowDataType::Utf8, false),
            Field::new("version", ArrowDataType::Int64, false),
            Field::new("lastUpdated", ArrowDataType::Int64, true),
        ]));

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["app_id"])),
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_commit_info_into_engine_data() {
        let engine = ExprEngine::new();

        let commit_info = CommitInfo::new(0, None, None, None);
        let commit_info_txn_id = commit_info.txn_id.clone();

        let engine_data = commit_info.into_engine_data(CommitInfo::to_schema().into(), &engine);

        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let mut map_builder = create_string_map_builder(false);
        map_builder.append(true).unwrap();
        let operation_parameters = Arc::new(map_builder.finish());

        let expected = RecordBatch::try_new(
            record_batch.schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(0)])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(StringArray::from(vec![Some("UNKNOWN")])),
                operation_parameters,
                Arc::new(StringArray::from(vec![Some(format!("v{KERNEL_VERSION}"))])),
                Arc::new(StringArray::from(vec![None::<String>])),
                Arc::new(StringArray::from(vec![commit_info_txn_id])),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_domain_metadata_into_engine_data() {
        let engine = ExprEngine::new();

        let domain_metadata = DomainMetadata {
            domain: "my.domain".to_string(),
            configuration: "config_value".to_string(),
            removed: false,
        };

        let engine_data =
            domain_metadata.into_engine_data(DomainMetadata::to_schema().into(), &engine);

        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let expected = RecordBatch::try_new(
            record_batch.schema(),
            vec![
                Arc::new(StringArray::from(vec!["my.domain"])),
                Arc::new(StringArray::from(vec!["config_value"])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_metadata_try_new() {
        let schema = StructType::new_unchecked([StructField::not_null("id", DataType::INTEGER)]);
        let config = HashMap::from([("key1".to_string(), "value1".to_string())]);

        let metadata = Metadata::try_new(
            Some("test_table".to_string()),
            Some("description".to_string()),
            schema.clone(),
            vec!["year".to_string()],
            1234567890,
            config.clone(),
        )
        .unwrap();

        assert!(!metadata.id.is_empty());
        assert_eq!(metadata.name, Some("test_table".to_string()));
        assert_eq!(
            metadata.schema_string,
            serde_json::to_string(&schema).unwrap()
        );
        assert_eq!(metadata.created_time, Some(1234567890));
        assert_eq!(metadata.configuration, config);
    }

    #[test]
    fn test_metadata_try_new_default() {
        let schema = StructType::new_unchecked([StructField::not_null("id", DataType::INTEGER)]);
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();

        assert!(!metadata.id.is_empty());
        assert_eq!(metadata.name, None);
        assert_eq!(metadata.description, None);
    }

    #[test]
    fn test_metadata_unique_ids() {
        let schema = StructType::new_unchecked([StructField::not_null("id", DataType::INTEGER)]);
        let m1 = Metadata::try_new(None, None, schema.clone(), vec![], 0, HashMap::new()).unwrap();
        let m2 = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        assert_ne!(m1.id, m2.id);
    }

    #[test]
    fn test_format_try_from_scalar() {
        let options = HashMap::from([
            ("path".to_string(), "/delta/table".to_string()),
            ("compressionType".to_string(), "snappy".to_string()),
        ]);
        let format = Format {
            provider: "parquet".to_string(),
            options,
        };
        let scalar = Scalar::try_from(format).unwrap();

        let Scalar::Struct(struct_data) = scalar else {
            panic!("Expected struct scalar");
        };
        assert_eq!(struct_data.fields()[0].name(), "provider");
        assert_eq!(struct_data.fields()[1].name(), "options");

        let Scalar::String(provider) = &struct_data.values()[0] else {
            panic!("Expected string provider");
        };
        assert_eq!(provider, "parquet");

        let Scalar::Map(map_data) = &struct_data.values()[1] else {
            panic!("Expected map options");
        };
        assert_eq!(map_data.pairs().len(), 2);
    }

    #[test]
    fn test_format_default() {
        let format = Format::default();
        let expected = Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        };
        assert_eq!(format, expected);
    }

    #[test]
    fn test_format_empty_options() {
        let format = Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        };
        let scalar = Scalar::try_from(format).unwrap();

        let Scalar::Struct(struct_data) = scalar else {
            panic!("Expected struct");
        };
        let Scalar::Map(map_data) = &struct_data.values()[1] else {
            panic!("Expected map");
        };
        assert!(map_data.pairs().is_empty());
    }

    #[test]
    fn test_format_special_characters() {
        let options = HashMap::from([
            ("path".to_string(), "/path/with spaces".to_string()),
            ("unicode".to_string(), "测试🎉".to_string()),
            ("empty".to_string(), "".to_string()),
        ]);
        let format = Format {
            provider: "custom".to_string(),
            options,
        };
        let scalar = Scalar::try_from(format).unwrap();

        let Scalar::Struct(struct_data) = scalar else {
            panic!("Expected struct");
        };
        let Scalar::Map(map_data) = &struct_data.values()[1] else {
            panic!("Expected map");
        };
        assert_eq!(map_data.pairs().len(), 3);
    }

    #[test]
    fn test_metadata_into_engine_data() {
        let engine = ExprEngine::new();
        let schema = StructType::new_unchecked([StructField::not_null("id", DataType::INTEGER)]);

        let test_metadata = Metadata::try_new(
            Some("test".to_string()),
            Some("my table".to_string()),
            schema.clone(),
            vec!["part".to_string()],
            123,
            HashMap::from([("k".to_string(), "v".to_string())]),
        )
        .unwrap();

        // have to get the id since it's random
        let test_id = test_metadata.id.clone();

        let actual: RecordBatch = test_metadata
            .into_engine_data(Metadata::to_schema().into(), &engine)
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let expected_json = json!({
            "id": test_id,
            "name": "test",
            "description": "my table",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}",
            "partitionColumns": ["part"],
            "createdTime": 123,
            "configuration": {
                "k": "v"
            }
        }).to_string();
        let expected = ReaderBuilder::new(actual.schema())
            .build(expected_json.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_metadata_with_log_schema() {
        let engine = ExprEngine::new();
        let schema = StructType::new_unchecked([StructField::not_null("id", DataType::INTEGER)]);

        let metadata = Metadata::try_new(
            Some("table".to_string()),
            None, // test that omitting description will omit entire field
            schema,
            vec![],
            456,
            HashMap::new(),
        )
        .unwrap();

        let metadata_id = metadata.id.clone();

        // test with the full log schema that wraps metadata in a "metaData" field
        let commit_schema = get_commit_schema().project(&[METADATA_NAME]).unwrap();
        let actual: RecordBatch = metadata
            .into_engine_data(commit_schema, &engine)
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let expected_json = json!({
            "metaData": {
                "id": metadata_id,
                "name": "table",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}",
                "partitionColumns": [],
                "createdTime": 456,
                "configuration": {}
            }
        }).to_string();
        let expected = ReaderBuilder::new(actual.schema())
            .build(expected_json.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_protocol_into_engine_data() {
        let engine = ExprEngine::new();
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors, TableFeature::ColumnMapping]),
            Some([TableFeature::DeletionVectors, TableFeature::ColumnMapping]),
        )
        .unwrap();

        let engine_data = protocol
            .clone()
            .into_engine_data(Protocol::to_schema().into(), &engine);
        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let list_field = Arc::new(Field::new("element", ArrowDataType::Utf8, false));
        let protocol_fields = vec![
            Field::new("minReaderVersion", ArrowDataType::Int32, false),
            Field::new("minWriterVersion", ArrowDataType::Int32, false),
            Field::new(
                "readerFeatures",
                ArrowDataType::List(list_field.clone()),
                true, // nullable
            ),
            Field::new(
                "writerFeatures",
                ArrowDataType::List(list_field.clone()),
                true, // nullable
            ),
        ];
        let schema = Arc::new(Schema::new(protocol_fields.clone()));

        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(list_field.clone());
        list_builder.values().append_value("deletionVectors");
        list_builder.values().append_value("columnMapping");
        list_builder.append(true);
        let reader_features_array = list_builder.finish();

        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(list_field.clone());
        list_builder.values().append_value("deletionVectors");
        list_builder.values().append_value("columnMapping");
        list_builder.append(true);
        let writer_features_array = list_builder.finish();

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(reader_features_array.clone()),
                Arc::new(writer_features_array.clone()),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);

        // test with the full log schema that wraps protocol in a "protocol" field
        let commit_schema = get_commit_schema().project(&[PROTOCOL_NAME]).unwrap();
        let engine_data = protocol.into_engine_data(commit_schema, &engine);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "protocol",
            ArrowDataType::Struct(protocol_fields.into()),
            true,
        )]));

        let expected = RecordBatch::try_new(
            schema,
            vec![Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("minReaderVersion", ArrowDataType::Int32, false)),
                    Arc::new(Int32Array::from(vec![3])) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new("minWriterVersion", ArrowDataType::Int32, false)),
                    Arc::new(Int32Array::from(vec![7])) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new(
                        "readerFeatures",
                        ArrowDataType::List(list_field.clone()),
                        true,
                    )),
                    Arc::new(reader_features_array) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new(
                        "writerFeatures",
                        ArrowDataType::List(list_field),
                        true,
                    )),
                    Arc::new(writer_features_array) as Arc<dyn Array>,
                ),
            ]))],
        )
        .unwrap();

        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_protocol_into_engine_data_empty_features() {
        let engine = ExprEngine::new();
        let empty_features: Vec<String> = vec![];
        let protocol =
            Protocol::try_new(3, 7, Some(empty_features.clone()), Some(empty_features)).unwrap();

        let engine_data = protocol
            .into_engine_data(Protocol::to_schema().into(), &engine)
            .unwrap();
        let record_batch: RecordBatch = engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 4);

        // reader/writer features are Some([]) lists
        let reader_features_col = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(reader_features_col.len(), 1);
        assert_eq!(reader_features_col.value(0).len(), 0); // empty list
        let writer_features_col = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(writer_features_col.len(), 1);
        assert_eq!(writer_features_col.value(0).len(), 0); // empty list
    }

    #[test]
    fn test_protocol_into_engine_data_no_features() {
        let engine = ExprEngine::new();
        let protocol = Protocol::try_new(1, 2, None::<Vec<String>>, None::<Vec<String>>).unwrap();

        let engine_data = protocol
            .into_engine_data(Protocol::to_schema().into(), &engine)
            .unwrap();
        let record_batch: RecordBatch = engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 4);

        // reader/writer features are null
        assert!(record_batch.column(2).is_null(0));
        assert!(record_batch.column(3).is_null(0));
    }

    #[test]
    fn test_schema_contains_file_actions_with_add() {
        let schema = get_commit_schema()
            .project(&[ADD_NAME, PROTOCOL_NAME])
            .unwrap();
        assert!(schema_contains_file_actions(&schema));
        assert!(schema_contains_file_actions(
            &schema.project(&[ADD_NAME]).unwrap()
        ));
    }

    #[test]
    fn test_schema_contains_file_actions_with_remove() {
        let schema = get_commit_schema()
            .project(&[REMOVE_NAME, METADATA_NAME])
            .unwrap();
        assert!(schema_contains_file_actions(&schema));
        assert!(schema_contains_file_actions(
            &schema.project(&[REMOVE_NAME]).unwrap()
        ));
    }

    #[test]
    fn test_schema_contains_file_actions_with_both() {
        let schema = get_commit_schema()
            .project(&[ADD_NAME, REMOVE_NAME])
            .unwrap();
        assert!(schema_contains_file_actions(&schema));
    }

    #[test]
    fn test_schema_contains_file_actions_with_neither() {
        let schema = get_commit_schema()
            .project(&[PROTOCOL_NAME, METADATA_NAME])
            .unwrap();
        assert!(!schema_contains_file_actions(&schema));
    }

    #[test]
    fn test_schema_contains_file_actions_empty_schema() {
        let schema = Arc::new(StructType::new_unchecked([]));
        assert!(!schema_contains_file_actions(&schema));
    }
}
