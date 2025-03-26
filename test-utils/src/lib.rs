//! A number of utilities useful for testing that we want to use in multiple crates

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::EngineData;
use itertools::Itertools;
use object_store::{path::Path, ObjectStore};

/// A common useful initial metadata and protocol. Also includes a single commitInfo
pub const METADATA: &str = r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

/// A common useful initial metadata and protocol. Also includes a single commitInfo
pub const METADATA_WITH_PARTITION_COLS: &str = r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["val"],"configuration":{},"createdTime":1587968585495}}"#;

pub enum TestAction {
    Add(String),
    Remove(String),
    Metadata,
}

// TODO: We need a better way to mock tables :)

/// Convert a vector of actions into a newline delimited json string, with standard metadata
pub fn actions_to_string(actions: Vec<TestAction>) -> String {
    actions_to_string_with_metadata(actions, METADATA)
}

/// Convert a vector of actions into a newline delimited json string, with metadata including a partition column
pub fn actions_to_string_partitioned(actions: Vec<TestAction>) -> String {
    actions_to_string_with_metadata(actions, METADATA_WITH_PARTITION_COLS)
}

fn actions_to_string_with_metadata(actions: Vec<TestAction>, metadata: &str) -> String {
    actions
        .into_iter()
        .map(|test_action| match test_action {
            TestAction::Add(path) => format!(r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true, "stats":"{{\"numRecords\":2,\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\": 1}},\"maxValues\":{{\"id\":3}}}}"}}}}"#),
            TestAction::Remove(path) => format!(r#"{{"remove":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true}}}}"#),
            TestAction::Metadata => metadata.into(),
        })
        .join("\n")
}

/// convert a RecordBatch into a vector of bytes. We can't use `From` since these are both foreign
/// types
pub fn record_batch_to_bytes(batch: &RecordBatch) -> Vec<u8> {
    let props = WriterProperties::builder().build();
    record_batch_to_bytes_with_props(batch, props)
}

pub fn record_batch_to_bytes_with_props(
    batch: &RecordBatch,
    writer_properties: WriterProperties,
) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut data, batch.schema(), Some(writer_properties)).unwrap();
    writer.write(batch).expect("Writing batch");
    // writer must be closed to write footer
    writer.close().unwrap();
    data
}

/// Anything that implements `IntoArray` can turn itself into a reference to an arrow array
pub trait IntoArray {
    fn into_array(self) -> ArrayRef;
}

impl IntoArray for Vec<i32> {
    fn into_array(self) -> ArrayRef {
        Arc::new(Int32Array::from(self))
    }
}

impl IntoArray for Vec<&'static str> {
    fn into_array(self) -> ArrayRef {
        Arc::new(StringArray::from(self))
    }
}

/// Generate a record batch from an iterator over (name, array) pairs. Each pair specifies a column
/// name and the array to associate with it
pub fn generate_batch<I, F>(items: I) -> Result<RecordBatch, ArrowError>
where
    I: IntoIterator<Item = (F, ArrayRef)>,
    F: AsRef<str>,
{
    RecordBatch::try_from_iter(items)
}

/// Generate a RecordBatch with two columns (id: int, val: str), with values "1,2,3" and "a,b,c"
/// respectively
pub fn generate_simple_batch() -> Result<RecordBatch, ArrowError> {
    generate_batch(vec![
        ("id", vec![1, 2, 3].into_array()),
        ("val", vec!["a", "b", "c"].into_array()),
    ])
}

/// get an ObjectStore path for a delta file, based on the version
pub fn delta_path_for_version(version: u64, suffix: &str) -> Path {
    let path = format!("_delta_log/{version:020}.{suffix}");
    Path::from(path.as_str())
}

/// put a commit file into the specified object store.
pub async fn add_commit(
    store: &dyn ObjectStore,
    version: u64,
    data: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = delta_path_for_version(version, "json");
    store.put(&path, data.into()).await?;
    Ok(())
}

/// Try to convert an `EngineData` into a `RecordBatch`. Panics if not using `ArrowEngineData` from
/// the default module
pub fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
    ArrowEngineData::try_from_engine_data(engine_data)
        .unwrap()
        .into()
}

/// We implement abs_diff here so we don't have to bump our msrv.
/// TODO: Remove and use std version when msrv >= 1.81.0
pub fn abs_diff(self_dur: std::time::Duration, other: std::time::Duration) -> std::time::Duration {
    if let Some(res) = self_dur.checked_sub(other) {
        res
    } else {
        other.checked_sub(self_dur).unwrap()
    }
}
