//! A number of utilities useful for testing that we want to use in multiple crates

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::get_log_add_schema;
use delta_kernel::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, MapArray, RecordBatch,
    StringArray, StructArray,
};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::scan::Scan;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Engine, EngineData, Snapshot};

use itertools::Itertools;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{path::Path, ObjectStore};
use serde_json::{json, to_vec};
use std::sync::Mutex;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::SubscriberExt;
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

/// Recursively copies a directory and all its contents from source to destination.
///
/// This function is used to create isolated copies of test tables, enabling parallel
/// test execution without interference. Each test gets its own copy of the table data,
/// preventing race conditions and cross-test pollution.
///
/// # Arguments
///
/// * `source` - Path to the source directory to copy from
/// * `dest` - Path to the destination directory (will be created if it doesn't exist)
///
/// # Note
///
/// This function copies ALL files and subdirectories, including any test artifacts
/// that may have been created in the source directory. Ensure the source directory
/// contains only the intended baseline data.
pub fn copy_directory(
    source: &std::path::Path,
    dest: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(dest)?;

    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        let dest_path = dest.join(&file_name);

        if path.is_dir() {
            copy_directory(&path, &dest_path)?;
        } else {
            std::fs::copy(&path, &dest_path)?;
        }
    }

    Ok(())
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

/// Helper to create a DefaultEngine with the default executor for tests.
///
/// Uses `TokioBackgroundExecutor` as the default executor.
pub fn create_default_engine(
    table_root: &url::Url,
) -> DeltaResult<Arc<DefaultEngine<TokioBackgroundExecutor>>> {
    let store = store_from_url(table_root)?;
    Ok(Arc::new(DefaultEngineBuilder::new(store).build()))
}

/// Helper to create a DefaultEngine with the default executor for tests.
///
/// Uses `TokioBackgroundExecutor` as the default executor.
pub fn create_default_engine_mt_executor(
    table_root: &url::Url,
) -> DeltaResult<Arc<DefaultEngine<TokioMultiThreadExecutor>>> {
    let store = store_from_url(table_root)?;
    let task_executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    Ok(Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(task_executor)
            .build(),
    ))
}

/// Test setup helper that creates a temporary directory and a `DefaultEngine` backed by
/// [`TokioBackgroundExecutor`].
///
/// Returns `(temp_dir, table_path, engine)` for use in integration tests.
/// The `temp_dir` must be kept alive for the duration of the test to prevent cleanup.
///
/// # Example
///
/// ```ignore
/// let (_temp_dir, table_path, engine) = test_table_setup()?;
/// ```
pub fn test_table_setup() -> DeltaResult<(
    tempfile::TempDir,
    String,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
)> {
    let temp_dir = tempfile::tempdir().map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let table_path = temp_dir
        .path()
        .to_str()
        .ok_or_else(|| delta_kernel::Error::generic("Invalid path"))?
        .to_string();
    let table_url = url::Url::from_directory_path(&table_path)
        .map_err(|_| delta_kernel::Error::generic("Invalid URL"))?;
    let engine = create_default_engine(&table_url)?;
    Ok((temp_dir, table_path, engine))
}

/// Test setup helper that creates a temporary directory and a `DefaultEngine` backed by
/// [`TokioMultiThreadExecutor`].
///
/// Returns `(temp_dir, table_path, engine)` for use in integration tests.
/// The `temp_dir` must be kept alive for the duration of the test to prevent cleanup.
pub fn test_table_setup_mt() -> DeltaResult<(
    tempfile::TempDir,
    String,
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
)> {
    let temp_dir = tempfile::tempdir().map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let table_path = temp_dir
        .path()
        .to_str()
        .ok_or_else(|| delta_kernel::Error::generic("Invalid path"))?
        .to_string();
    let table_url = url::Url::from_directory_path(&table_path)
        .map_err(|_| delta_kernel::Error::generic("Invalid URL"))?;
    let engine = create_default_engine_mt_executor(&table_url)?;
    Ok((temp_dir, table_path, engine))
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
    let engine = DefaultEngineBuilder::new(Arc::clone(&storage)).build();

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
        if writer_features.contains(&"changeDataFeed") {
            config.insert("delta.enableChangeDataFeed".to_string(), json!("true"));
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

/// Creates two empty test tables, one with 37 protocol and one with 11 protocol.  the tables will
/// be named {table_base_name}_11 and {table_base_name}_37. The local_directory param can be set to
/// write out the tables to the local filesystem, passing in None will create in-memory tables
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

pub fn read_scan(scan: &Scan, engine: Arc<dyn Engine>) -> DeltaResult<Vec<RecordBatch>> {
    let scan_results = scan.execute(engine)?;
    scan_results
        .map(EngineDataArrowExt::try_into_record_batch)
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

/// Insert column arrays into an existing table in a single commit.
///
/// Takes a snapshot and column arrays, constructs a [`RecordBatch`] from the snapshot schema,
/// opens a transaction, writes the batch as a parquet file, and commits.
/// Useful for quickly seeding test tables without writing the transaction boilerplate each time.
///
/// # Example
///
/// ```ignore
/// let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
/// insert_data(snapshot, &engine, vec![Arc::new(Int32Array::from(vec![1]))]).await?;
/// ```
pub async fn insert_data<E: TaskExecutor>(
    snapshot: Arc<Snapshot>,
    engine: &Arc<DefaultEngine<E>>,
    columns: Vec<ArrayRef>,
) -> DeltaResult<CommitResult> {
    let arrow_schema = TryFromKernel::try_from_kernel(snapshot.schema().as_ref())?;
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), columns)
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);

    let write_context = txn.get_write_context();
    let add_files_metadata = engine
        .write_parquet(&ArrowEngineData::new(batch), &write_context, HashMap::new())
        .await?;
    txn.add_files(add_files_metadata);

    txn.commit(engine.as_ref())
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

/// Returns a nested schema with 6 top-level fields including a nested struct:
/// `[row_number: long, name: string, score: double, address: {street: string, city: string}, tag: string, value: int]`
pub fn nested_schema() -> Result<SchemaRef, Box<dyn std::error::Error>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::not_null("row_number", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("score", DataType::DOUBLE),
        StructField::nullable(
            "address",
            StructType::try_new(vec![
                StructField::not_null("street", DataType::STRING),
                StructField::nullable("city", DataType::STRING),
            ])?,
        ),
        StructField::nullable("tag", DataType::STRING),
        StructField::nullable("value", DataType::INTEGER),
    ])?))
}

/// Returns two [`RecordBatch`]es with hardcoded test data matching [`nested_schema`].
///
/// Batch 1: rows 1..3, names alice/bob/charlie, streets st1..st3
/// Batch 2: rows 4..6, names dave/eve/frank, streets st4..st6
pub fn nested_batches() -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let schema = nested_schema()?;
    let arrow_schema: ArrowSchema = TryFromKernel::try_from_kernel(schema.as_ref())?;
    let address_fields = match arrow_schema.field_with_name("address").unwrap().data_type() {
        ArrowDataType::Struct(fields) => fields.clone(),
        _ => panic!("expected struct"),
    };

    let build = |ids: Vec<i64>,
                 names: Vec<&str>,
                 scores: Vec<f64>,
                 streets: Vec<&str>,
                 cities: Vec<Option<&str>>,
                 tags: Vec<Option<&str>>,
                 values: Vec<Option<i32>>|
     -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let address_array = StructArray::new(
            address_fields.clone(),
            vec![
                Arc::new(StringArray::from(streets)) as ArrayRef,
                Arc::new(StringArray::from(cities)) as ArrayRef,
            ],
            None,
        );
        Ok(RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(Float64Array::from(scores)) as ArrayRef,
                Arc::new(address_array) as ArrayRef,
                Arc::new(StringArray::from(tags)) as ArrayRef,
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ],
        )?)
    };

    Ok(vec![
        build(
            vec![1, 2, 3],
            vec!["alice", "bob", "charlie"],
            vec![1.0, 2.0, 3.0],
            vec!["st1", "st2", "st3"],
            vec![Some("c1"), None, Some("c3")],
            vec![Some("t1"), Some("t2"), None],
            vec![Some(10), Some(20), None],
        )?,
        build(
            vec![4, 5, 6],
            vec!["dave", "eve", "frank"],
            vec![4.0, 5.0, 6.0],
            vec!["st4", "st5", "st6"],
            vec![Some("c4"), Some("c5"), Some("c6")],
            vec![None, Some("t5"), Some("t6")],
            vec![Some(40), None, Some(60)],
        )?,
    ])
}

/// Asserts that a field exists at the given dot-separated path in a [`StructType`] schema,
/// traversing into nested structs as needed. Panics if any segment of the path is missing
/// or if a non-terminal segment is not a struct type.
///
/// # Example
///
/// ```ignore
/// // Given schema: { address: { street: string, city: string } }
/// assert_schema_has_field(&schema, &["address".into(), "street".into()]);
/// ```
pub fn assert_schema_has_field(schema: &delta_kernel::schema::StructType, path: &[String]) {
    let path_str = path.join(".");
    let mut current = schema;
    for (i, name) in path.iter().enumerate() {
        let field = current
            .field(name)
            .unwrap_or_else(|| panic!("schema missing field '{path_str}'"));
        if i + 1 < path.len() {
            current = match field.data_type() {
                delta_kernel::schema::DataType::Struct(s) => s,
                _ => panic!("expected struct at '{path_str}'"),
            };
        }
    }
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

/// Creates add file metadata for one or more files without partition values.
/// Each tuple contains: (file_path, file_size, mod_time, num_records)
pub fn create_add_files_metadata(
    add_files_schema: &SchemaRef,
    files: Vec<(&str, i64, i64, i64)>,
) -> Result<Box<dyn delta_kernel::EngineData>, Box<dyn std::error::Error>> {
    let num_files = files.len();

    // Build arrays for each file
    let path_array = StringArray::from(files.iter().map(|(p, _, _, _)| *p).collect::<Vec<_>>());
    let size_array = Int64Array::from(files.iter().map(|(_, s, _, _)| *s).collect::<Vec<_>>());
    let mod_time_array = Int64Array::from(files.iter().map(|(_, _, m, _)| *m).collect::<Vec<_>>());
    let num_records_array =
        Int64Array::from(files.iter().map(|(_, _, _, n)| *n).collect::<Vec<_>>());

    // Create empty map for partitionValues (repeated for each file)
    let entries_field = Arc::new(Field::new(
        "key_value",
        ArrowDataType::Struct(
            vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    ));
    let empty_keys = StringArray::from(Vec::<&str>::new());
    let empty_values = StringArray::from(Vec::<Option<&str>>::new());
    let empty_entries = StructArray::from(vec![
        (
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(empty_keys) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            Arc::new(empty_values) as ArrayRef,
        ),
    ]);
    let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
    let partition_values_array = Arc::new(MapArray::new(
        entries_field,
        offsets,
        empty_entries,
        None,
        false,
    ));

    // Build stats struct with all fields: numRecords, nullCount, minValues, maxValues, tightBounds
    // nullCount, minValues, maxValues are empty structs (structure depends on data schema)
    let empty_struct_fields: delta_kernel::arrow::datatypes::Fields =
        Vec::<Arc<Field>>::new().into();
    let empty_struct = StructArray::new_empty_fields(num_files, None);
    let tight_bounds_array = BooleanArray::from(vec![true; num_files]);

    let stats_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(num_records_array) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "nullCount",
                ArrowDataType::Struct(empty_struct_fields.clone()),
                true,
            )),
            Arc::new(empty_struct.clone()) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "minValues",
                ArrowDataType::Struct(empty_struct_fields.clone()),
                true,
            )),
            Arc::new(empty_struct.clone()) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "maxValues",
                ArrowDataType::Struct(empty_struct_fields),
                true,
            )),
            Arc::new(empty_struct) as ArrayRef,
        ),
        (
            Arc::new(Field::new("tightBounds", ArrowDataType::Boolean, true)),
            Arc::new(tight_bounds_array) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(TryFromKernel::try_from_kernel(add_files_schema.as_ref())?),
        vec![
            Arc::new(path_array) as ArrayRef,
            partition_values_array as ArrayRef,
            Arc::new(size_array) as ArrayRef,
            Arc::new(mod_time_array) as ArrayRef,
            Arc::new(stats_struct) as ArrayRef,
        ],
    )?;

    Ok(Box::new(ArrowEngineData::new(batch)))
}

/// Writes a [`RecordBatch`] to a table, commits the transaction, and returns the post-commit
/// snapshot.
pub async fn write_batch_to_table(
    snapshot: &Arc<Snapshot>,
    engine: &DefaultEngine<impl delta_kernel::engine::default::executor::TaskExecutor>,
    data: RecordBatch,
    partition_values: std::collections::HashMap<String, String>,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_engine_info("DefaultEngine")
        .with_data_change(true);
    let write_context = txn.get_write_context();
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(data),
            &write_context,
            partition_values,
        )
        .await?;
    txn.add_files(add_meta);
    match txn.commit(engine)? {
        delta_kernel::transaction::CommitResult::CommittedTransaction(c) => Ok(c
            .post_commit_snapshot()
            .expect("Failed to get post_commit_snapshot")
            .clone()),
        _ => panic!("Write commit should succeed"),
    }
}

/// An add info extracted from the log segment.
pub struct AddInfo {
    pub path: String,
    pub stats: Option<serde_json::Value>,
}

/// Reads all add infos from a snapshot's log segment using internal API.
pub fn read_add_infos(
    snapshot: &Snapshot,
    engine: &impl Engine,
) -> Result<Vec<AddInfo>, Box<dyn std::error::Error>> {
    let schema = get_log_add_schema().clone();
    let batches = snapshot.log_segment().read_actions(engine, schema, None)?;
    let mut actions = Vec::new();
    for batch_result in batches {
        let actions_batch = batch_result?;
        let engine_data = ArrowEngineData::try_from_engine_data(actions_batch.actions)?;
        let record_batch = engine_data.record_batch();
        let add_struct = match record_batch.schema().index_of("add").ok().and_then(|idx| {
            record_batch
                .column(idx)
                .as_any()
                .downcast_ref::<StructArray>()
        }) {
            Some(s) => s,
            None => continue,
        };
        let path_arr = add_struct
            .column_by_name("path")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let stats_arr = add_struct
            .column_by_name("stats")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let len = add_struct.len();
        for i in 0..len {
            if let Some(path) = path_arr.and_then(|a| (!a.is_null(i)).then(|| a.value(i))) {
                let stats = stats_arr
                    .and_then(|a| (!a.is_null(i)).then(|| a.value(i)))
                    .map(serde_json::from_str)
                    .transpose()?;
                actions.push(AddInfo {
                    path: path.to_string(),
                    stats,
                });
            }
        }
    }
    Ok(actions)
}

/// Helper to create a table with the given properties, then load and return its snapshot.
pub fn create_table_and_load_snapshot(
    table_path: &str,
    schema: SchemaRef,
    engine: &dyn Engine,
    properties: &[(&str, &str)],
) -> DeltaResult<Arc<Snapshot>> {
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::transaction::create_table::create_table;

    let _ = create_table(table_path, schema, "Test/1.0")
        .with_table_properties(properties.to_vec())
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?;

    let table_url = delta_kernel::try_parse_uri(table_path)?;
    Snapshot::builder_for(table_url).build(engine)
}

// Writer that captures log output into a shared buffer for test assertions
pub struct LogWriter(pub Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

// Test helper that sets up tracing to capture log output
// The guard keeps the tracing subscriber active for the lifetime of the struct
pub struct LoggingTest {
    logs: Arc<Mutex<Vec<u8>>>,
    _guard: DefaultGuard,
}

impl Default for LoggingTest {
    fn default() -> Self {
        Self::new()
    }
}

impl LoggingTest {
    pub fn new() -> Self {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let logs_clone = logs.clone();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_writer(move || LogWriter(logs_clone.clone()))
                    .with_ansi(false),
            ),
        );
        Self { logs, _guard }
    }

    pub fn logs(&self) -> String {
        String::from_utf8(self.logs.lock().unwrap().clone()).unwrap()
    }
}
