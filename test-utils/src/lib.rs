//! A number of utilities useful for testing that we want to use in multiple crates

use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};

use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::scan::Scan;
use delta_kernel::schema::SchemaRef;
use delta_kernel::{DeltaResult, Engine, EngineData, Snapshot};

use itertools::Itertools;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{path::Path, ObjectStore};
use serde_json::{json, to_vec};
use url::Url;

/// unpack the test data from {test_parent_dir}/{test_name}.tar.zst into a temp dir, and return the
/// dir it was unpacked into
pub fn load_test_data(
    test_parent_dir: &str,
    test_name: &str,
) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let path = format!("{test_parent_dir}/{test_name}.tar.zst");
    let tar = zstd::Decoder::new(std::fs::File::open(path)?)?;
    let mut archive = tar::Archive::new(tar);
    let temp_dir = tempfile::tempdir()?;
    archive.unpack(temp_dir.path())?;
    Ok(temp_dir)
}

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

pub fn actions_to_string_with_metadata(actions: Vec<TestAction>, metadata: &str) -> String {
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

impl IntoArray for Vec<i64> {
    fn into_array(self) -> ArrayRef {
        Arc::new(Int64Array::from(self))
    }
}

impl IntoArray for Vec<bool> {
    fn into_array(self) -> ArrayRef {
        Arc::new(BooleanArray::from(self))
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

pub fn staged_commit_path_for_version(version: u64) -> Path {
    let uuid = uuid::Uuid::new_v4();
    let path = format!("_delta_log/_staged_commits/{version:020}.{uuid}.json");
    Path::from(path.as_str())
}

/// get an ObjectStore path for a compressed log file, based on the start/end versions
pub fn compacted_log_path_for_versions(start_version: u64, end_version: u64, suffix: &str) -> Path {
    let path = format!("_delta_log/{start_version:020}.{end_version:020}.compacted.{suffix}");
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

pub async fn add_staged_commit(
    store: &dyn ObjectStore,
    version: u64,
    data: String,
) -> Result<Path, Box<dyn std::error::Error>> {
    let path = staged_commit_path_for_version(version);
    store.put(&path, data.into()).await?;
    Ok(path)
}

/// Try to convert an `EngineData` into a `RecordBatch`. Panics if not using `ArrowEngineData` from
/// the default module
pub fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
    ArrowEngineData::try_from_engine_data(engine_data)
        .unwrap()
        .into()
}

/// Simple extension trait with helpful methods (just constuctor for now) for creating/using
/// DefaultEngine in our tests.
///
/// Note: we implment this extension trait here so that we can import this trait (from test-utils
/// crate) and get to use all these test-only helper methods from places where we don't have access
pub trait DefaultEngineExtension {
    type Executor: TaskExecutor;

    fn new_local() -> Arc<DefaultEngine<Self::Executor>>;
}

impl DefaultEngineExtension for DefaultEngine<TokioBackgroundExecutor> {
    type Executor = TokioBackgroundExecutor;

    fn new_local() -> Arc<DefaultEngine<TokioBackgroundExecutor>> {
        let object_store = Arc::new(LocalFileSystem::new());
        Arc::new(DefaultEngine::new(
            object_store,
            TokioBackgroundExecutor::new().into(),
        ))
    }
}

// setup default engine with in-memory (local_directory=None) or local fs (local_directory=Some(Url))
pub fn engine_store_setup(
    table_name: &str,
    local_directory: Option<&Url>,
) -> (
    Arc<dyn ObjectStore>,
    DefaultEngine<TokioBackgroundExecutor>,
    Url,
) {
    let (storage, url): (Arc<dyn ObjectStore>, Url) = match local_directory {
        None => (
            Arc::new(InMemory::new()),
            Url::parse(format!("memory:///{table_name}/").as_str()).expect("valid url"),
        ),
        Some(dir) => (
            Arc::new(LocalFileSystem::new()),
            Url::parse(format!("{dir}{table_name}/").as_str()).expect("valid url"),
        ),
    };
    let executor = Arc::new(TokioBackgroundExecutor::new());
    let engine = DefaultEngine::new(Arc::clone(&storage), executor);

    (storage, engine, url)
}

// we provide this table creation function since we only do appends to existing tables for now.
// this will just create an empty table with the given schema. (just protocol + metadata actions)
#[allow(clippy::too_many_arguments)]
pub async fn create_table(
    store: Arc<dyn ObjectStore>,
    table_path: Url,
    schema: SchemaRef,
    partition_columns: &[&str],
    use_37_protocol: bool,
    reader_features: Vec<&str>,
    writer_features: Vec<&str>,
) -> Result<Url, Box<dyn std::error::Error>> {
    let table_id = "test_id";
    let schema = serde_json::to_string(&schema)?;

    let protocol = if use_37_protocol {
        json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": reader_features,
                "writerFeatures": writer_features,
            }
        })
    } else {
        json!({
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 1,
            }
        })
    };

    let configuration = {
        let mut config = serde_json::Map::new();

        if reader_features.contains(&"columnMapping") {
            config.insert("delta.columnMapping.mode".to_string(), json!("name"));
        }
        if writer_features.contains(&"rowTracking") {
            config.insert(
                "delta.materializedRowIdColumnName".to_string(),
                json!("some_dummy_column_name"),
            );
            config.insert(
                "delta.materializedRowCommitVersionColumnName".to_string(),
                json!("another_dummy_column_name"),
            );
        }
        if writer_features.contains(&"inCommitTimestamp") {
            config.insert("delta.enableInCommitTimestamps".to_string(), json!("true"));
            config.insert(
                "delta.inCommitTimestampEnablementVersion".to_string(),
                json!("0"),
            );
            config.insert(
                "delta.inCommitTimestampEnablementTimestamp".to_string(),
                json!("1612345678"),
            );
        }

        config
    };

    let metadata = json!({
        "metaData": {
            "id": table_id,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": schema,
            "partitionColumns": partition_columns,
            "configuration": configuration,
            "createdTime": 1677811175819u64
        }
    });

    // Add commitInfo with ICT if ICT is enabled
    let commit_info = if writer_features.contains(&"inCommitTimestamp") {
        // When ICT is enabled from version 0, we need to include it in the initial commit
        let timestamp = 1612345678i64; // Use a fixed timestamp for testing
        Some(json!({
            "commitInfo": {
                "timestamp": timestamp,
                "inCommitTimestamp": timestamp,
                "operation": "CREATE TABLE",
                "operationParameters": {},
                "isBlindAppend": true
            }
        }))
    } else {
        None
    };

    let data = if let Some(commit_info) = commit_info {
        [
            to_vec(&commit_info).unwrap(),
            b"\n".to_vec(),
            to_vec(&protocol).unwrap(),
            b"\n".to_vec(),
            to_vec(&metadata).unwrap(),
        ]
        .concat()
    } else {
        [
            to_vec(&protocol).unwrap(),
            b"\n".to_vec(),
            to_vec(&metadata).unwrap(),
        ]
        .concat()
    };

    // put 0.json with protocol + metadata
    let path = table_path.join("_delta_log/00000000000000000000.json")?;

    store
        .put(&Path::from_url_path(path.path())?, data.into())
        .await?;
    Ok(table_path)
}

/// Creates two empty test tables, one with 37 protocol and one with 11 protocol.
/// the tables will be named {table_base_name}_11 and table_base_name}_37. The local_directory param
/// can be set to write out the tables to the local filesystem, passing in None will create in-memory tables
pub async fn setup_test_tables(
    schema: SchemaRef,
    partition_columns: &[&str],
    local_directory: Option<&Url>,
    table_base_name: &str,
) -> Result<
    Vec<(
        Url,
        DefaultEngine<TokioBackgroundExecutor>,
        Arc<dyn ObjectStore>,
        &'static str,
    )>,
    Box<dyn std::error::Error>,
> {
    let table_name_11 = format!("{table_base_name}_11");
    let table_name_37 = format!("{table_base_name}_37");
    let (store_11, engine_11, table_location_11) =
        engine_store_setup(table_name_11.as_str(), local_directory);
    let (store_37, engine_37, table_location_37) =
        engine_store_setup(table_name_37.as_str(), local_directory);
    Ok(vec![
        (
            create_table(
                store_37.clone(),
                table_location_37,
                schema.clone(),
                partition_columns,
                true,
                vec![],
                vec![],
            )
            .await?,
            engine_37,
            store_37,
            "test_table_37",
        ),
        (
            create_table(
                store_11.clone(),
                table_location_11,
                schema,
                partition_columns,
                false,
                vec![],
                vec![],
            )
            .await?,
            engine_11,
            store_11,
            "test_table_11",
        ),
    ])
}

pub fn to_arrow(data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    Ok(data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into())
}

pub fn read_scan(scan: &Scan, engine: Arc<dyn Engine>) -> DeltaResult<Vec<RecordBatch>> {
    let scan_results = scan.execute(engine)?;
    scan_results
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch = to_arrow(data)?;
            if let Some(mask) = mask {
                Ok(filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
        })
        .try_collect()
}

pub fn test_read(
    expected: &ArrowEngineData,
    url: &Url,
    engine: Arc<dyn Engine>,
) -> DeltaResult<()> {
    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();

    let expected = pretty_format_batches(&[expected.record_batch().clone()])
        .unwrap()
        .to_string();

    println!("actual:\n{formatted}");
    println!("expected:\n{expected}");
    assert_eq!(formatted, expected);

    Ok(())
}

// Helper function to set json values in a serde_json Values
pub fn set_json_value(
    value: &mut serde_json::Value,
    path: &str,
    new_value: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut path_string = path.replace(".", "/");
    path_string.insert(0, '/');
    let v = value
        .pointer_mut(&path_string)
        .ok_or_else(|| format!("key '{path}' not found"))?;
    *v = new_value;
    Ok(())
}

pub fn assert_result_error_with_message<T, E: ToString>(res: Result<T, E>, message: &str) {
    match res {
        Ok(_) => panic!("Expected error, but got Ok result"),
        Err(error) => {
            let error_str = error.to_string();
            assert!(
                error_str.contains(message),
                "Error message does not contain the expected message.\nExpected message:\t{message}\nActual message:\t\t{error_str}"
            );
        }
    }
}
