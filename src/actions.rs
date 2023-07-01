use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema};

#[derive(Debug)]
pub enum ActionType {
    /// modify the data in a table by adding individual logical files
    Add,
    /// add a file containing only the data that was changed as part of the transaction
    Cdc,
    /// additional provenance information about what higher-level operation was being performed
    CommitInfo,
    /// contains a configuration (string-string map) for a named metadata domain
    DomainMetadata,
    /// changes the current metadata of the table
    Metadata,
    /// increase the version of the Delta protocol that is required to read or write a given table
    Protocol,
    /// modify the data in a table by removing individual logical files
    Remove,
    /// The Row ID high-water mark tracks the largest ID that has been assigned to a row in the table.
    RowIdHighWaterMark,
    Txn,
}

impl ActionType {
    pub fn field(&self) -> Field {
        match self {
            Self::Add => get_root("add", self.fields()),
            Self::Cdc => get_root("cdc", self.fields()),
            Self::CommitInfo => get_root("commitInfo", self.fields()),
            Self::DomainMetadata => get_root("domainMetadata", self.fields()),
            Self::Metadata => get_root("metaData", self.fields()),
            Self::Protocol => get_root("protocol", self.fields()),
            Self::Remove => get_root("remove", self.fields()),
            Self::RowIdHighWaterMark => get_root("rowIdHighWaterMark", self.fields()),
            Self::Txn => get_root("txn", self.fields()),
        }
    }

    pub fn fields(&self) -> Vec<Field> {
        match self {
            Self::Add => add_fields(),
            Self::Cdc => cdc_fields(),
            Self::CommitInfo => commit_info_fields(),
            Self::DomainMetadata => domain_metadata_fields(),
            Self::Metadata => metadata_fields(),
            Self::Protocol => protocol_fields(),
            Self::Remove => remove_fields(),
            Self::RowIdHighWaterMark => watermark_fields(),
            Self::Txn => txn_fields(),
        }
    }
}

pub fn get_log_schema() -> Schema {
    Schema {
        fields: Fields::from_iter([
            ActionType::Add.field(),
            ActionType::Cdc.field(),
            ActionType::CommitInfo.field(),
            ActionType::DomainMetadata.field(),
            ActionType::Metadata.field(),
            ActionType::Protocol.field(),
            ActionType::Remove.field(),
            ActionType::RowIdHighWaterMark.field(),
            ActionType::Txn.field(),
        ]),
        metadata: Default::default(),
    }
}

fn get_root(name: &str, fields: Vec<Field>) -> Field {
    Field::new(name, DataType::Struct(Fields::from_iter(fields)), true)
}

fn add_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("modificationTime", DataType::Int64, false),
        Field::new("dataChange", DataType::Boolean, false),
        Field::new("stats", DataType::Utf8, true),
        Field::new(
            "partitionValues",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
        Field::new(
            "tags",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
        Field::new(
            "deletionVector",
            DataType::Struct(Fields::from(vec![
                Field::new("storageType", DataType::Utf8, false),
                Field::new("pathOrInlineDv", DataType::Utf8, false),
                Field::new("offset", DataType::UInt64, true),
                Field::new("sizeInBytes", DataType::UInt64, false),
                Field::new("cardinality", DataType::UInt64, false),
            ])),
            true,
        ),
        Field::new("baseRowId", DataType::Int64, true),
        Field::new("defaultRowCommitVersion", DataType::Int64, true),
    ])
}

fn cdc_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("path", DataType::Utf8, true),
        Field::new(
            "partitionValues",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
        Field::new("size", DataType::Int64, true),
        Field::new("dataChange", DataType::Boolean, true),
        Field::new(
            "tags",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
    ])
}

fn remove_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("path", DataType::Utf8, true),
        Field::new("deletionTimestamp", DataType::Int64, true),
        Field::new("dataChange", DataType::Boolean, true),
        Field::new("extendedFileMetadata", DataType::Boolean, true),
        Field::new("size", DataType::Int64, true),
        Field::new(
            "partitionValues",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
        Field::new(
            "tags",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
    ])
}

fn metadata_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new(
            "format",
            DataType::Struct(Fields::from_iter([
                Field::new("provider", DataType::Utf8, true),
                Field::new(
                    "options",
                    DataType::Map(
                        Arc::new(Field::new(
                            "key_value",
                            DataType::Struct(Fields::from_iter([
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    false,
                ),
            ])),
            true,
        ),
        Field::new("schemaString", DataType::Utf8, true),
        Field::new("createdTime", DataType::Int64, true),
        Field::new(
            "partitionColumns",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "configuration",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from_iter([
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ])
}

fn protocol_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("minReaderVersion", DataType::Int32, true),
        Field::new("minWriterVersion", DataType::Int32, true),
        Field::new(
            "readerFeatures",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "writerFeatures",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
    ])
}

fn txn_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("appId", DataType::Utf8, true),
        Field::new("version", DataType::Int64, true),
        Field::new("lastUpdated", DataType::Int64, true),
    ])
}

fn watermark_fields() -> Vec<Field> {
    Vec::from_iter([Field::new("highWaterMark", DataType::Int64, true)])
}

fn commit_info_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("timestamp", DataType::Int64, true),
        Field::new("operation", DataType::Utf8, true),
        Field::new("isolationLevel", DataType::Utf8, true),
        Field::new("isBlindAppend", DataType::Boolean, true),
        Field::new("txnId", DataType::Utf8, true),
        Field::new("readVersion", DataType::Int32, true),
        Field::new(
            "operationParameters",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
        Field::new(
            "operationMetrics",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
    ])
}

fn domain_metadata_fields() -> Vec<Field> {
    Vec::from_iter([
        Field::new("domain", DataType::Utf8, true),
        Field::new(
            "configuration",
            DataType::Map(Arc::new(get_map_field()), false),
            true,
        ),
        Field::new("removed", DataType::Boolean, true),
    ])
}

fn get_map_field() -> Field {
    Field::new(
        "key_value",
        DataType::Struct(Fields::from_iter([
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    )
}
