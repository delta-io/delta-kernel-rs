//! Schema definitions for action types

use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

/// A trait that says you can ask for the [`Schema`] of the implementor
pub(crate) trait GetSchema {
    fn get_schema() -> StructField;
}

/// A trait that allows getting a `StructField` based on the provided name and nullability
pub(crate) trait GetField {
    fn get_field(name: impl Into<String>) -> StructField;
}

macro_rules! impl_get_field {
    ( $(($rust_type: ty, $kernel_type: expr)), * ) => {
        $(
            impl GetField for $rust_type {
                fn get_field(name: impl Into<String>) -> StructField {
                    StructField::new(name, $kernel_type, false)
                }
            }
        )*
    };
}

impl_get_field!(
    (String, DataType::STRING),
    (i64, DataType::LONG),
    (i32, DataType::INTEGER),
    (i16, DataType::SHORT),
    (char, DataType::BYTE),
    (f32, DataType::FLOAT),
    (f64, DataType::DOUBLE),
    (bool, DataType::BOOLEAN),
    (HashMap<String, String>, MapType::new(DataType::STRING, DataType::STRING, false)),
    (HashMap<String, Option<String>>, MapType::new(DataType::STRING, DataType::STRING, true)),
    (Vec<String>, ArrayType::new(DataType::STRING, false))
);

impl<T: GetField> GetField for Option<T> {
    fn get_field(name: impl Into<String>) -> StructField {
        let mut inner = T::get_field(name);
        inner.nullable = true;
        inner
    }
}

lazy_static! {
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
    pub(crate) static ref METADATA_FIELD: StructField = StructField::new(
        "metaData",
        StructType::new(vec![
            StructField::new("id", DataType::STRING, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("description", DataType::STRING, true),
            StructField::new(
                "format",
                StructType::new(vec![
                    StructField::new("provider", DataType::STRING, false),
                    StructField::new(
                        "options",
                        MapType::new(
                            DataType::STRING,
                            DataType::STRING,
                            true,
                        ),
                        true,
                    ),
                ]),
                false,
            ),
            StructField::new("schemaString", DataType::STRING, false),
            StructField::new(
                "partitionColumns",
                ArrayType::new(DataType::STRING, false),
                false,
            ),
            StructField::new("createdTime", DataType::LONG, true),
            StructField::new(
                "configuration",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                false,
            ),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution
    pub(crate) static ref PROTOCOL_FIELD: StructField = StructField::new(
        "protocol",
        StructType::new(vec![
            StructField::new("minReaderVersion", DataType::INTEGER, false),
            StructField::new("minWriterVersion", DataType::INTEGER, false),
            StructField::new(
                "readerFeatures",
                ArrayType::new(DataType::STRING, false),
                true,
            ),
            StructField::new(
                "writerFeatures",
                ArrayType::new(DataType::STRING, false),
                true,
            ),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information
    static ref COMMIT_INFO_FIELD: StructField = StructField::new(
        "commitInfo",
        StructType::new(vec![
            StructField::new("timestamp", DataType::LONG, false),
            StructField::new("operation", DataType::STRING, false),
            StructField::new("isolationLevel", DataType::STRING, true),
            StructField::new("isBlindAppend", DataType::BOOLEAN, true),
            StructField::new("txnId", DataType::STRING, true),
            StructField::new("readVersion", DataType::LONG, true),
            StructField::new(
                "operationParameters",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                true,
            ),
            StructField::new(
                "operationMetrics",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                true,
            ),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    pub(crate) static ref ADD_FIELD: StructField = StructField::new(
        "add",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            partition_values_field(),
            StructField::new("size", DataType::LONG, false),
            StructField::new("modificationTime", DataType::LONG, false),
            StructField::new("dataChange", DataType::BOOLEAN, false),
            StructField::new("stats", DataType::STRING, true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::LONG, true),
            StructField::new("defaultRowCommitVersion", DataType::LONG, true),
            StructField::new("clusteringProvider", DataType::STRING, true),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    pub(crate) static ref REMOVE_FIELD: StructField = StructField::new(
        "remove",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("deletionTimestamp", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, false),
            StructField::new("extendedFileMetadata", DataType::BOOLEAN, true),
            partition_values_field(),
            StructField::new("size", DataType::LONG, true),
            StructField::new("stats", DataType::STRING, true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::LONG, true),
            StructField::new("defaultRowCommitVersion", DataType::LONG, true),
        ]),
        true,
    );
    static ref REMOVE_FIELD_CHECKPOINT: StructField = StructField::new(
        "remove",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("deletionTimestamp", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, false),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file
    static ref CDC_FIELD: StructField = StructField::new(
        "cdc",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            partition_values_field(),
            StructField::new("size", DataType::LONG, false),
            StructField::new("dataChange", DataType::BOOLEAN, false),
            tags_field(),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers
    static ref TXN_FIELD: StructField = StructField::new(
        "txn",
        StructType::new(vec![
            StructField::new("appId", DataType::STRING, false),
            StructField::new("version", DataType::LONG, false),
            StructField::new("lastUpdated", DataType::LONG, true),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
    static ref DOMAIN_METADATA_FIELD: StructField = StructField::new(
        "domainMetadata",
        StructType::new(vec![
            StructField::new("domain", DataType::STRING, false),
            StructField::new(
                "configuration",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                false,
            ),
            StructField::new("removed", DataType::BOOLEAN, false),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-metadata
    static ref CHECKPOINT_METADATA_FIELD: StructField = StructField::new(
        "checkpointMetadata",
        StructType::new(vec![
            StructField::new("flavor", DataType::STRING, false),
            tags_field(),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
    static ref SIDECAR_FIELD: StructField = StructField::new(
        "sidecar",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("sizeInBytes", DataType::LONG, false),
            StructField::new("modificationTime", DataType::LONG, false),
            StructField::new("type", DataType::STRING, false),
            tags_field(),
        ]),
        true,
    );

    static ref LOG_SCHEMA: StructType = StructType::new(
        vec![
            ADD_FIELD.clone(),
            CDC_FIELD.clone(),
            COMMIT_INFO_FIELD.clone(),
            DOMAIN_METADATA_FIELD.clone(),
            METADATA_FIELD.clone(),
            PROTOCOL_FIELD.clone(),
            REMOVE_FIELD.clone(),
            TXN_FIELD.clone(),
        ]
    );
}

fn tags_field() -> StructField {
    StructField::new(
        "tags",
        MapType::new(DataType::STRING, DataType::STRING, true),
        true,
    )
}

fn partition_values_field() -> StructField {
    StructField::new(
        "partitionValues",
        MapType::new(DataType::STRING, DataType::STRING, true),
        false,
    )
}

fn deletion_vector_field() -> StructField {
    StructField::new(
        "deletionVector",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("storageType", DataType::STRING, false),
            StructField::new("pathOrInlineDv", DataType::STRING, false),
            StructField::new("offset", DataType::INTEGER, true),
            StructField::new("sizeInBytes", DataType::INTEGER, false),
            StructField::new("cardinality", DataType::LONG, false),
        ]))),
        true,
    )
}

#[cfg(test)]
pub(crate) fn log_schema() -> &'static StructType {
    &LOG_SCHEMA
}
