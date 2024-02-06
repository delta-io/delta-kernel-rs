use std::collections::HashMap;

use arrow_array::{
    BooleanArray, Int32Array, Int64Array, MapArray, RecordBatch, StringArray, StructArray,
};
use either::Either;
use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use itertools::izip;

use crate::{DeltaResult, Error};

pub(crate) mod action_definitions;
pub(crate) mod schemas;
pub(crate) mod types;

pub use action_definitions::{Format, Metadata, Protocol};
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
    Txn,
    CheckpointMetadata,
    Sidecar,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Action {
    Metadata(Metadata),
    Protocol(Protocol),
    Add(Add),
    Remove(Remove),
}

#[fix_hidden_lifetime_bug]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn parse_actions<'a>(
    batch: &RecordBatch,
    types: impl IntoIterator<Item = &'a ActionType>,
) -> DeltaResult<impl Iterator<Item = Action>> {
    Ok(types
        .into_iter()
        .filter_map(|action| parse_action(batch, action).ok())
        .flatten())
}

#[fix_hidden_lifetime_bug]
pub(crate) fn parse_action(
    batch: &RecordBatch,
    action_type: &ActionType,
) -> DeltaResult<impl Iterator<Item = Action>> {
    let column_name = match action_type {
        ActionType::Metadata => "metaData",
        ActionType::Protocol => "protocol",
        ActionType::Add => "add",
        ActionType::Remove => "remove",
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
        ActionType::Metadata => panic!(),
        ActionType::Protocol => panic!(),
        ActionType::Add => parse_actions_add(arr),
        ActionType::Remove => parse_actions_remove(arr),
        _ => todo!(),
    }
}

fn parse_actions_add(arr: &StructArray) -> DeltaResult<Box<dyn Iterator<Item = Action> + '_>> {
    let paths = cast_struct_column::<StringArray>(arr, "path")?;
    let sizes = cast_struct_column::<Int64Array>(arr, "size")?;
    let modification_times = cast_struct_column::<Int64Array>(arr, "modificationTime")?;
    let data_changes = cast_struct_column::<BooleanArray>(arr, "dataChange")?;
    let partition_values = cast_struct_column::<MapArray>(arr, "partitionValues")?
        .iter()
        .map(|data| data.map(|d| struct_array_to_map(&d).unwrap()));

    let tags = if let Ok(stats) = cast_struct_column::<MapArray>(arr, "tags") {
        Either::Left(
            stats
                .iter()
                .map(|data| data.map(|d| struct_array_to_map(&d).unwrap())),
        )
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let stats = if let Ok(stats) = cast_struct_column::<StringArray>(arr, "stats") {
        Either::Left(stats.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let base_row_ids = if let Ok(row_ids) = cast_struct_column::<Int64Array>(arr, "baseRowId") {
        Either::Left(row_ids.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let commit_versions =
        if let Ok(versions) = cast_struct_column::<Int64Array>(arr, "defaultRowCommitVersion") {
            Either::Left(versions.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(sizes.len()))
        };

    let deletion_vectors = if let Ok(dvs) = cast_struct_column::<StructArray>(arr, "deletionVector")
    {
        Either::Left(parse_dv(dvs)?)
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let zipped = izip!(
        paths,
        sizes,
        modification_times,
        data_changes,
        partition_values,
        stats,
        tags,
        base_row_ids,
        commit_versions,
        deletion_vectors,
    );
    let zipped = zipped.map(
        |(
            maybe_paths,
            maybe_size,
            maybe_modification_time,
            maybe_data_change,
            partition_values,
            stat,
            tags,
            base_row_id,
            default_row_commit_version,
            deletion_vector,
        )| {
            if let (Some(path), Some(size), Some(modification_time), Some(data_change)) = (
                maybe_paths,
                maybe_size,
                maybe_modification_time,
                maybe_data_change,
            ) {
                Some(Add {
                    path: path.into(),
                    size,
                    modification_time,
                    data_change,
                    partition_values: partition_values.unwrap_or_default(),
                    stats: stat.map(|v| v.to_string()),
                    tags: tags.unwrap_or_default(),
                    base_row_id,
                    default_row_commit_version,
                    deletion_vector,
                })
            } else {
                None
            }
        },
    );

    Ok(Box::new(zipped.flatten().map(Action::Add)))
}

fn parse_actions_remove(arr: &StructArray) -> DeltaResult<Box<dyn Iterator<Item = Action> + '_>> {
    let paths = cast_struct_column::<StringArray>(arr, "path")?;
    let data_changes = cast_struct_column::<BooleanArray>(arr, "dataChange")?;

    let deletion_timestamps =
        if let Ok(ts) = cast_struct_column::<Int64Array>(arr, "deletionTimestamp") {
            Either::Left(ts.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let extended_file_metadata =
        if let Ok(metas) = cast_struct_column::<BooleanArray>(arr, "extendedFileMetadata") {
            Either::Left(metas.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let partition_values =
        if let Ok(values) = cast_struct_column::<MapArray>(arr, "partitionValues") {
            Either::Left(
                values
                    .iter()
                    .map(|data| data.map(|d| struct_array_to_map(&d).unwrap())),
            )
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let sizes = if let Ok(size) = cast_struct_column::<Int64Array>(arr, "size") {
        Either::Left(size.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let tags = if let Ok(tags) = cast_struct_column::<MapArray>(arr, "tags") {
        Either::Left(
            tags.iter()
                .map(|data| data.map(|d| struct_array_to_map(&d).unwrap())),
        )
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let deletion_vectors = if let Ok(dvs) = cast_struct_column::<StructArray>(arr, "deletionVector")
    {
        Either::Left(parse_dv(dvs)?)
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let base_row_ids = if let Ok(row_ids) = cast_struct_column::<Int64Array>(arr, "baseRowId") {
        Either::Left(row_ids.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let commit_versions =
        if let Ok(row_ids) = cast_struct_column::<Int64Array>(arr, "defaultRowCommitVersion") {
            Either::Left(row_ids.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let zipped = izip!(
        paths,
        data_changes,
        deletion_timestamps,
        extended_file_metadata,
        partition_values,
        sizes,
        tags,
        deletion_vectors,
        base_row_ids,
        commit_versions,
    );

    let zipped = zipped.map(
        |(
            maybe_paths,
            maybe_data_change,
            deletion_timestamp,
            extended_file_metadata,
            partition_values,
            size,
            tags,
            deletion_vector,
            base_row_id,
            default_row_commit_version,
        )| {
            if let (Some(path), Some(data_change)) = (maybe_paths, maybe_data_change) {
                Some(Remove {
                    path: path.into(),
                    data_change,
                    deletion_timestamp,
                    extended_file_metadata,
                    partition_values,
                    size,
                    tags,
                    deletion_vector,
                    base_row_id,
                    default_row_commit_version,
                })
            } else {
                None
            }
        },
    );

    Ok(Box::new(zipped.flatten().map(Action::Remove)))
}

fn parse_dv(
    arr: &StructArray,
) -> DeltaResult<impl Iterator<Item = Option<DeletionVectorDescriptor>> + '_> {
    let storage_types = cast_struct_column::<StringArray>(arr, "storageType")?;
    let paths_or_inlines = cast_struct_column::<StringArray>(arr, "pathOrInlineDv")?;
    let sizes_in_bytes = cast_struct_column::<Int32Array>(arr, "sizeInBytes")?;
    let cardinalities = cast_struct_column::<Int64Array>(arr, "cardinality")?;

    let offsets = if let Ok(offsets) = cast_struct_column::<Int32Array>(arr, "offset") {
        Either::Left(offsets.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(cardinalities.len()))
    };

    let zipped = izip!(
        storage_types,
        paths_or_inlines,
        sizes_in_bytes,
        cardinalities,
        offsets,
    );

    Ok(zipped.map(
        |(maybe_type, maybe_path_or_inline_dv, maybe_size_in_bytes, maybe_cardinality, offset)| {
            if let (
                Some(storage_type),
                Some(path_or_inline_dv),
                Some(size_in_bytes),
                Some(cardinality),
            ) = (
                maybe_type,
                maybe_path_or_inline_dv,
                maybe_size_in_bytes,
                maybe_cardinality,
            ) {
                Some(DeletionVectorDescriptor {
                    storage_type: storage_type.into(),
                    path_or_inline_dv: path_or_inline_dv.into(),
                    size_in_bytes,
                    cardinality,
                    offset,
                })
            } else {
                None
            }
        },
    ))
}

fn cast_struct_column<T: 'static>(arr: &StructArray, name: impl AsRef<str>) -> DeltaResult<&T> {
    arr.column_by_name(name.as_ref())
        .ok_or(Error::MissingColumn(name.as_ref().into()))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or(Error::UnexpectedColumnType(format!(
            "Cannot downcast '{}' to expected type",
            name.as_ref()
        )))
}

fn struct_array_to_map(arr: &StructArray) -> DeltaResult<HashMap<String, Option<String>>> {
    let keys = cast_struct_column::<StringArray>(arr, "keys")?;
    let values = cast_struct_column::<StringArray>(arr, "values")?;
    Ok(keys
        .into_iter()
        .zip(values)
        .filter_map(|(k, v)| k.map(|key| (key.to_string(), v.map(|vv| vv.to_string()))))
        .collect())
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::actions::schemas::log_schema;
    use crate::actions::Protocol;
    use crate::simple_client::{data::SimpleData, json::SimpleJsonHandler, SimpleClient};
    use crate::{EngineData, JsonHandler};

    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(SimpleData::new(batch))
    }

    fn action_batch() -> Box<SimpleData> {
        let handler = SimpleJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(log_schema().clone());
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        SimpleData::from_engine_data(parsed).unwrap()
    }

    #[test]
    fn test_parse_protocol() {
        let client = SimpleClient::new();
        let data = action_batch();
        let parsed = Protocol::try_new_from_data(&client, data.as_ref()).unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(parsed, expected)
    }

    #[test]
    fn test_parse_metadata() {
        let client = SimpleClient::new();
        let data = action_batch();
        let parsed = Metadata::try_new_from_data(&client, data.as_ref()).unwrap();

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
        let expected = Metadata {
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
        };
        assert_eq!(parsed, expected)
    }

    #[test]
    fn test_parse_add_partitioned() {
        let handler = SimpleJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"commitInfo":{"timestamp":1670892998177,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[\"c1\",\"c2\"]"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"1356"},"engineInfo":"Apache-Spark/3.3.1 Delta-Lake/2.2.0","txnId":"046a258f-45e3-4657-b0bf-abfb0f76681c"}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet","partitionValues":{"c1":"5","c2":"b"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}"}}"#,
        ]
        .into();
        let output_schema = Arc::new(log_schema().clone());
        let batch = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let batch = SimpleData::from_engine_data(batch).unwrap().into_record_batch();
        let actions = parse_action(&batch, &ActionType::Add)
            .unwrap()
            .collect::<Vec<_>>();
        println!("{:?}", actions)
    }
}
