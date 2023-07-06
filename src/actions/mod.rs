use std::collections::HashMap;

use arrow_array::{
    Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};

use crate::{DeltaResult, Error};

pub(crate) mod schemas;
pub(crate) mod types;

pub use schemas::get_log_schema;
pub use types::*;

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

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Metadata(Metadata),
    Protocol(Protocol),
}

pub(crate) fn parse_actions(
    batch: &RecordBatch,
    action_type: &ActionType,
) -> DeltaResult<Vec<Action>> {
    let column_name = match action_type {
        ActionType::Metadata => "metaData",
        ActionType::Protocol => "protocol",
        _ => unimplemented!(),
    };

    let arr = batch
        .column_by_name(column_name)
        .ok_or(Error::MissingColumn(column_name.into()))?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(Error::UnexpectedColumnType(
            "Cannot downcast to StructArray".into(),
        ))?;

    match action_type {
        ActionType::Metadata => {
            let metadata =
                parse_action_metadata(&arr)?.ok_or(Error::MissingData("No metadta data".into()))?;
            Ok(vec![Action::Metadata(metadata)])
        }
        ActionType::Protocol => {
            let protocol = parse_action_protocol(&arr)?
                .ok_or(Error::MissingData("No protocol data".into()))?;
            Ok(vec![Action::Protocol(protocol)])
        }
        _ => todo!(),
    }
}

fn parse_action_metadata(arr: &StructArray) -> DeltaResult<Option<Metadata>> {
    let ids = cast_struct_column::<StringArray>(arr, "id")?;
    let schema_strings = cast_struct_column::<StringArray>(arr, "schemaString")?;
    let metadata = ids
        .into_iter()
        .zip(schema_strings.into_iter())
        .filter_map(|(maybe_id, maybe_schema_string)| {
            if let (Some(id), Some(schema_string)) = (maybe_id, maybe_schema_string) {
                Some(Metadata::new(
                    id,
                    Format {
                        provider: "parquet".into(),
                        options: Default::default(),
                    },
                    schema_string,
                    Vec::<String>::new(),
                    None,
                ))
            } else {
                None
            }
        })
        .next();

    if metadata.is_none() {
        return Ok(metadata);
    }
    let mut metadata = metadata.unwrap();

    metadata.partition_columns = cast_struct_column::<ListArray>(arr, "partitionColumns")
        .ok()
        .map(|arr| {
            arr.iter()
                .filter_map(|it| {
                    if let Some(features) = it {
                        let vals = features
                            .as_any()
                            .downcast_ref::<StringArray>()?
                            .iter()
                            .filter_map(|v| v.map(|inner| inner.to_owned()))
                            .collect::<Vec<_>>();
                        Some(vals)
                    } else {
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    metadata.name = cast_struct_column::<StringArray>(arr, "name")
        .ok()
        .and_then(|arr| {
            arr.iter()
                .flat_map(|maybe| maybe.map(|v| v.to_string()))
                .next()
        });
    metadata.description = cast_struct_column::<StringArray>(arr, "description")
        .ok()
        .and_then(|arr| {
            arr.iter()
                .flat_map(|maybe| maybe.map(|v| v.to_string()))
                .next()
        });
    metadata.created_time = cast_struct_column::<Int64Array>(arr, "createdTime")
        .ok()
        .and_then(|arr| arr.iter().flat_map(|v| v).next());

    if let Some(config) = cast_struct_column::<MapArray>(arr, "configuration").ok() {
        let keys = config
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::MissingData("expected key column in map".into()))?;
        let values = config
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::MissingData("expected value column in map".into()))?;
        metadata.configuration = keys
            .into_iter()
            .zip(values.into_iter())
            .filter_map(|(k, v)| {
                if let Some(key) = k {
                    Some((key.to_string(), v.map(|vv| vv.to_string())))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
    };

    Ok(Some(metadata))
}

fn parse_action_protocol(arr: &StructArray) -> DeltaResult<Option<Protocol>> {
    let min_reader = cast_struct_column::<Int32Array>(arr, "minReaderVersion")?;
    let min_writer = cast_struct_column::<Int32Array>(arr, "minWriterVersion")?;
    let protocol = min_reader
        .into_iter()
        .zip(min_writer.into_iter())
        .filter_map(|(r, w)| {
            if let (Some(min_reader_version), Some(min_wrriter_version)) = (r, w) {
                Some(Protocol::new(min_reader_version, min_wrriter_version))
            } else {
                None
            }
        })
        .next();

    if protocol.is_none() {
        return Ok(protocol);
    }
    let mut protocol = protocol.unwrap();

    protocol.reader_features = cast_struct_column::<ListArray>(arr, "readerFeatures")
        .ok()
        .map(|arr| {
            arr.iter()
                .filter_map(|it| {
                    if let Some(features) = it {
                        let vals = features
                            .as_any()
                            .downcast_ref::<StringArray>()?
                            .iter()
                            .filter_map(|v| v.map(|inner| inner.to_owned()))
                            .collect::<Vec<_>>();
                        Some(vals)
                    } else {
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>()
        });

    protocol.writer_features = cast_struct_column::<ListArray>(arr, "writerFeatures")
        .ok()
        .map(|arr| {
            arr.iter()
                .filter_map(|it| {
                    if let Some(features) = it {
                        let vals = features
                            .as_any()
                            .downcast_ref::<StringArray>()?
                            .iter()
                            .filter_map(|v| v.map(|inner| inner.to_string()))
                            .collect::<Vec<_>>();
                        Some(vals)
                    } else {
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>()
        });

    Ok(Some(protocol))
}

fn cast_struct_column<T: 'static>(arr: &StructArray, name: impl AsRef<str>) -> DeltaResult<&T> {
    arr.column_by_name(name.as_ref())
        .ok_or(Error::MissingColumn(name.as_ref().into()))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or(Error::UnexpectedColumnType(
            "Cannot downcast to expected type".into(),
        ))
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::actions::Protocol;
    use crate::client::json::DefaultJsonHandler;
    use crate::JsonHandler;

    fn action_batch() -> RecordBatch {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store);

        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(get_log_schema());
        handler.parse_json(json_strings, output_schema).unwrap()
    }

    #[test]
    fn test_parse_protocol() {
        let batch = action_batch();
        let action = parse_actions(&batch, &ActionType::Protocol).unwrap();
        let expected = Action::Protocol(Protocol {
            min_reader_version: 3,
            min_wrriter_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        });
        assert_eq!(action[0], expected)
    }

    #[test]
    fn test_parse_metadata() {
        let batch = action_batch();
        let action = parse_actions(&batch, &ActionType::Metadata).unwrap();
        let configuration = HashMap::from_iter([
            (
                "delta.enableDeletionVectors".to_string(),
                Some("true".to_string()),
            ),
            (
                "delta.columnMapping.mode".to_string(),
                Some("none".to_string()),
            ),
        ]);
        let expected = Action::Metadata(Metadata {
            id: "testId".into(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".into(),
                options: Default::default(),
            },
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            partition_columns: Vec::new(),
            created_time: Some(1677811175819),
            configuration,
        });
        assert_eq!(action[0], expected)
    }
}
