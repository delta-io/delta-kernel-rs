use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use serde::Serialize;
use tempfile::TempDir;
use test_utils::{
    copy_directory, delta_path_for_version, load_test_data, modify_add_file_partition_keys,
    replace_array_row, AddFilePartitionKeyModify,
};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::util::SubscriberInitExt as _;
use url::Url;

use crate::actions::{get_all_actions_schema, Add, Cdc, CommitInfo, Metadata, Protocol, Remove};
use crate::arrow::array::{
    new_empty_array, new_null_array, ArrayRef, Int64Array, MapArray, RecordBatch, StringArray,
    StructArray,
};
use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use crate::arrow::compute::concat_batches;
use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_conversion::{parquet_field_id_metadata, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::metrics::{MetricEvent, MetricsReporter, WithMetricsReporterLayer as _};
use crate::object_store::local::LocalFileSystem;
use crate::object_store::memory::InMemory;
use crate::object_store::ObjectStoreExt as _;
use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use crate::path::ParsedLogPath;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    ColumnMappingMode, FeatureType, TableFeature, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::table_properties::COLUMN_MAPPING_MODE;
use crate::transaction::create_table::create_table;
use crate::transaction::{CreateTable, Transaction, BASE_ADD_FILES_SCHEMA};
use crate::{
    DeltaErrorCondition, DeltaResult, Engine, EngineData, Error, FileMeta, KernelError, Snapshot,
    SnapshotRef, Version,
};

/// Parses `path` (a full URL string) into a [`ParsedLogPath`] with zero size, for building
/// synthetic log-file listings in tests.
pub(crate) fn create_log_path(path: &str) -> ParsedLogPath<FileMeta> {
    create_log_path_with_size(path, 0)
}

/// [`create_log_path`] with an explicit file size.
pub(crate) fn create_log_path_with_size(path: &str, size: u64) -> ParsedLogPath<FileMeta> {
    ParsedLogPath::try_from(FileMeta {
        location: Url::parse(path).expect("Invalid file URL"),
        last_modified: 0,
        size,
    })
    .unwrap()
    .unwrap()
}

/// A metrics reporter that captures all events for test assertions.
#[derive(Debug, Default)]
pub(crate) struct CapturingReporter {
    events: Mutex<Vec<MetricEvent>>,
}

impl MetricsReporter for CapturingReporter {
    fn report(&self, event: MetricEvent) {
        self.events.lock().unwrap().push(event);
    }
}

impl CapturingReporter {
    /// Returns a copy of all captured events.
    pub(crate) fn events(&self) -> Vec<MetricEvent> {
        self.events.lock().unwrap().clone()
    }
}

/// Kernel-internal twin of [`test_utils::install_thread_local_metrics_reporter`].
///
/// Internal tests need their own helper because the trait identity of `MetricsReporter`
/// differs across the test_utils <-> kernel path-dep boundary. Both helpers wrap
/// [`test_utils::ensure_metrics_compatible_global_subscriber`] + a thread-local
/// `set_default` and serve the same purpose: install a metrics-collecting subscriber
/// in a way that is robust against tracing callsite-cache poisoning.
pub(crate) fn install_thread_local_metrics_reporter(
    reporter: Arc<dyn MetricsReporter>,
) -> DefaultGuard {
    test_utils::ensure_metrics_compatible_global_subscriber();
    tracing_subscriber::registry()
        .with_metrics_reporter_layer(reporter)
        .set_default()
}

#[derive(Serialize)]
pub(crate) enum Action {
    #[serde(rename = "add")]
    Add(Add),
    #[serde(rename = "remove")]
    Remove(Remove),
    #[serde(rename = "cdc")]
    Cdc(Cdc),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[allow(unused)]
    #[serde(rename = "commitInfo")]
    CommitInfo(CommitInfo),
}

use crate::schema::{
    schema, schema_ref, ArrayType, ColumnMetadataKey, DataType as KernelDataType, MapType,
    MetadataValue, SchemaRef, StructField, StructType,
};
#[cfg(feature = "geo-type-in-dev")]
use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType, PrimitiveType};

/// A mock table that writes commits to a local temporary delta log. This can be used to
/// construct a delta log used for testing.
pub(crate) struct LocalMockTable {
    commit_num: u64,
    store: Arc<LocalFileSystem>,
    dir: TempDir,
}

impl LocalMockTable {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        Self {
            commit_num: 0,
            store,
            dir,
        }
    }
    /// Writes all `actions` to a new commit in the log
    pub(crate) async fn commit(&mut self, actions: impl IntoIterator<Item = Action>) {
        let data = actions
            .into_iter()
            .map(|action| serde_json::to_string(&action).unwrap())
            .join("\n");

        let path = delta_path_for_version(self.commit_num, "json");
        self.commit_num += 1;

        self.store
            .put(&path, data.into())
            .await
            .expect("put log file in store");
    }

    /// Get the path to the root of the table.
    pub(crate) fn table_root(&self) -> &Path {
        self.dir.path()
    }
}

/// Try to convert an `EngineData` into a `RecordBatch`. Panics if not using `ArrowEngineData`
/// from the default module
fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
    ArrowEngineData::try_from_engine_data(engine_data)
        .unwrap()
        .into()
}

/// Checks that two `EngineData` objects are equal by converting them to `RecordBatch` and
/// comparing
pub(crate) fn assert_batch_matches(actual: Box<dyn EngineData>, expected: Box<dyn EngineData>) {
    assert_eq!(into_record_batch(actual), into_record_batch(expected));
}

/// Helper for building valid add-file batches.
///
/// Tests can generate add-file metadata without writing a Parquet file using this helper. All
/// required fields are populated. When `all_nullable` is true, every field in the batch
/// schema is nullable.
pub(crate) fn create_valid_add_file_batch(all_nullable: bool) -> RecordBatch {
    let schema = if all_nullable {
        let fields = BASE_ADD_FILES_SCHEMA
            .fields()
            .map(|f| StructField::nullable(f.name().clone(), f.data_type().clone()));
        StructType::try_new(fields).expect("nullable add-file schema")
    } else {
        BASE_ADD_FILES_SCHEMA.as_ref().clone()
    };
    let arrow_schema: ArrowSchema = (&schema).try_into_arrow().expect("arrow schema");
    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "path" => Arc::new(StringArray::from(vec!["dummy"])) as ArrayRef,
            "size" | "modificationTime" => Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
            "partitionValues" => {
                let DataType::Map(entries_field, sorted) = field.data_type() else {
                    panic!("partitionValues must be a map type");
                };
                let entries = new_empty_array(entries_field.data_type())
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("map entries struct")
                    .clone();
                // One row, empty partition map.
                let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0]));
                Arc::new(
                    MapArray::try_new(entries_field.clone(), offsets, entries, None, *sorted)
                        .expect("empty map array"),
                )
            }
            // Non-mandatory fields (e.g. `stats`) are left null.
            _ => new_null_array(field.data_type(), 1),
        })
        .collect();
    RecordBatch::try_new(Arc::new(arrow_schema), columns).expect("valid add-file batch")
}

/// Builds one valid add-file row with a fully nullable schema.
pub(crate) fn nullable_add_file() -> RecordBatch {
    create_valid_add_file_batch(true /* all_nullable */)
}

/// Builds `row_count` valid add-file rows with a fully nullable schema.
pub(crate) fn nullable_add_files(row_count: usize) -> RecordBatch {
    let batch = nullable_add_file();
    concat_batches(&batch.schema(), &vec![batch; row_count])
        .expect("failed to concatenate rows into a multi-row add-file batch")
}

pub(crate) fn replace_column(batch: &RecordBatch, field: &str, column: ArrayRef) -> RecordBatch {
    let schema = batch.schema();
    let index = schema.index_of(field).expect("field in schema");
    let mut columns = batch.columns().to_vec();
    columns[index] = column;
    RecordBatch::try_new(schema, columns).expect("failed to rebuild batch after replacing a column")
}

pub(crate) fn set_field_as_null(batch: &RecordBatch, field: &str, row: usize) -> RecordBatch {
    let schema = batch.schema();
    let index = schema.index_of(field).expect("field in schema");
    let mut columns = batch.columns().to_vec();
    let null = new_null_array(schema.field(index).data_type(), 1);
    columns[index] = replace_array_row(&columns[index], null, row);
    RecordBatch::try_new(schema, columns)
        .expect("failed to rebuild batch after replacing a field value with null")
}

/// Returns nullable add-file rows with `partitionValues` replaced by `partition_values`.
pub(crate) fn add_files_with_partition_values(
    partition_values: &[&[(&str, Option<&str>)]],
) -> RecordBatch {
    let batches: Vec<_> = partition_values
        .iter()
        .map(|entries| {
            let modifications: Vec<_> = entries
                .iter()
                .map(|(key, value)| AddFilePartitionKeyModify::Insert { key, value: *value })
                .collect();
            modify_add_file_partition_keys(nullable_add_file(), &modifications)
        })
        .collect();
    concat_batches(&batches[0].schema(), &batches)
        .expect("failed to concatenate rows with partition values")
}

pub(crate) fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
    let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
    let schema = Arc::new(ArrowSchema::new(vec![string_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
        .expect("Can't convert to record batch");
    Box::new(ArrowEngineData::new(batch))
}

pub(crate) fn parse_json_batch(json_strings: StringArray) -> Box<dyn EngineData> {
    let engine = SyncEngine::new();
    let json_handler = engine.json_handler();
    let output_schema = get_all_actions_schema().clone();
    json_handler
        .parse_json(string_array_to_engine_data(json_strings), output_schema)
        .unwrap()
}

pub(crate) fn action_batch() -> Box<dyn EngineData> {
    let json_strings: StringArray = vec![
        r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
        r#"{"remove":{"path":"part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","deletionTimestamp":1670892998135,"dataChange":true,"partitionValues":{"c1":"4","c2":"c"},"size":452}}"#,
        r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
        r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none", "delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}"#,
        r#"{"cdc":{"path":"_change_data/age=21/cdc-00000-93f7fceb-281a-446a-b221-07b88132d203.c000.snappy.parquet","partitionValues":{"age":"21"},"size":1033,"dataChange":false}}"#,
        r#"{"sidecar":{"path":"016ae953-37a9-438e-8683-9a9a4a79a395.parquet","sizeInBytes":9268,"modificationTime":1714496113961,"tags":{"tag_foo":"tag_bar"}}}"#,
        r#"{"txn":{"appId":"myApp","version": 3}}"#,
        r#"{"checkpointMetadata":{"version":2, "tags":{"tag_foo":"tag_bar"}}}"#,
    ]
    .into();
    parse_json_batch(json_strings)
}

/// A batch containing a single `checkpoint` action covering every element kind, including one
/// `txn`-typed and one `domainMetadata`-typed sidecar. The checkpoint row is surrounded by
/// non-checkpoint action rows so the outer `CheckpointVisitor` must skip other rows in the
/// batch and locate the checkpoint action among them.
#[cfg(feature = "adaptive-metadata-in-dev")]
pub(crate) fn checkpoint_action_batch() -> Box<dyn EngineData> {
    let json_strings: StringArray = vec![
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"checkpoint":[{"checkpointMetadata":{"version":42}},{"contentRoot":{"path":"s3://bucket/manifest","sizeInBytes":1024,"version":40}},{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["adaptiveMetadata-preview"],"writerFeatures":["adaptiveMetadata-preview"]}},{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}},{"txn":{"appId":"myApp","version":3}},{"domainMetadata":{"domain":"myDomain","configuration":"cfg","removed":false}},{"sidecar":{"type":"txn","path":"txn-sidecar.parquet","sizeInBytes":100,"modificationTime":1}},{"sidecar":{"type":"domainMetadata","path":"dm-sidecar.parquet","sizeInBytes":200,"modificationTime":2}}]}"#,
        r#"{"txn":{"appId":"otherApp","version":9}}"#,
    ]
    .into();
    parse_json_batch(json_strings)
}

// TODO: allow tests to pass in context (issue#1133)
#[track_caller]
pub(crate) fn assert_result_error_with_message<T, E: ToString>(res: Result<T, E>, message: &str) {
    match res {
        Ok(_) => panic!("Expected error with message {message}, but got Ok result"),
        Err(error) => {
            let error_str = error.to_string();
            assert!(
                error_str.contains(message),
                "Error message does not contain the expected message.\nExpected message:\t{message}\nActual message:\t\t{error_str}"
            );
        }
    }
}

/// Asserts the 2x2 matrix of (schema_has_feature, protocol_supports_feature) outcomes
/// for schema-level feature validators. The expected pattern is:
/// - schema + protocol => Ok
/// - no schema + no protocol => Ok
/// - no schema + protocol => Ok
/// - schema + no protocol => Err (orphaned schema presence)
///
/// Additional error schemas (e.g. nested) are also tested against `protocol_without`.
#[track_caller]
pub(crate) fn assert_schema_feature_validation(
    schema_with: &StructType,
    schema_without: &StructType,
    protocol_with: &Protocol,
    protocol_without: &Protocol,
    extra_err_schemas: &[&StructType],
    feature: &str,
) {
    let try_build = |schema: &StructType, protocol: &Protocol| {
        MockTableConfigurationBuilder::new()
            .with_schema(schema.clone())
            .with_protocol(protocol.clone())
            .try_build()
    };
    try_build(schema_with, protocol_with).expect("feature present + supported");
    try_build(schema_without, protocol_without).expect("feature absent + unsupported");
    try_build(schema_without, protocol_with).expect("feature absent + supported");
    let assert_mismatch = |result| match result {
        Err(Error::Delta(error)) => {
            assert_eq!(
                error.condition(),
                DeltaErrorCondition::DeltaFeaturesProtocolMetadataMismatch
            );
            assert_eq!(error.parameter("features"), Some(feature));
        }
        Err(error) => panic!("expected Delta feature mismatch, got {error}"),
        Ok(_) => panic!("expected Delta feature mismatch, got success"),
    };
    assert_mismatch(try_build(schema_with, protocol_without));
    for schema in extra_err_schemas {
        assert_mismatch(try_build(schema, protocol_without));
    }
}

// ==================== Mock TableConfiguration ====================

/// Builds a mock [`TableConfiguration`] for unit tests.
///
/// Defaults to a flat `value: INTEGER` schema, no partition columns, no table properties, and a
/// table-features protocol (reader 3, writer 7) listing no features, rooted at `file:///` at
/// version 0. Every axis is overridable.
pub(crate) struct MockTableConfigurationBuilder {
    schema: Option<SchemaRef>,
    partition_columns: Vec<String>,
    props: HashMap<String, String>,
    protocol: Protocol,
    table_root: Url,
    version: Version,
}

impl MockTableConfigurationBuilder {
    pub(crate) fn new() -> Self {
        Self {
            schema: None,
            partition_columns: vec![],
            props: HashMap::new(),
            protocol: MockProtocolBuilder::new().build(),
            table_root: Url::parse("file:///").unwrap(),
            version: 0,
        }
    }

    /// Sets the table schema, overriding the flat default.
    pub(crate) fn with_schema(mut self, schema: impl Into<SchemaRef>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub(crate) fn with_partition_columns(
        mut self,
        columns: impl IntoIterator<Item = impl ToString>,
    ) -> Self {
        self.partition_columns = columns.into_iter().map(|c| c.to_string()).collect();
        self
    }

    /// Adds table properties, keeping any previously set ones.
    pub(crate) fn with_properties<K: ToString, V: ToString>(
        mut self,
        props: impl IntoIterator<Item = impl Borrow<(K, V)>>,
    ) -> Self {
        self.props.extend(props.into_iter().map(|pair| {
            let (key, value) = pair.borrow();
            (key.to_string(), value.to_string())
        }));
        self
    }

    /// Sets the column-mapping mode property.
    ///
    /// Callers using `name` or `id` must provide a schema with column-mapping metadata through
    /// [`Self::with_schema`].
    pub(crate) fn with_column_mapping(self, mode: impl Into<Option<ColumnMappingMode>>) -> Self {
        let Some(mode) = mode.into() else {
            return self;
        };
        let mode = match mode {
            ColumnMappingMode::None => "none",
            ColumnMappingMode::Id => "id",
            ColumnMappingMode::Name => "name",
        };
        self.with_properties([(COLUMN_MAPPING_MODE, mode)])
    }

    /// Uses `protocol` verbatim.
    pub(crate) fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    pub(crate) fn with_table_root(mut self, table_root: impl AsRef<str>) -> Self {
        self.table_root = Url::parse(table_root.as_ref()).unwrap();
        self
    }

    pub(crate) fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Builds the [`TableConfiguration`], panicking if the configuration is invalid. Use
    /// [`Self::try_build`] to assert on construction errors.
    #[track_caller]
    pub(crate) fn build(self) -> TableConfiguration {
        self.try_build().unwrap()
    }

    pub(crate) fn try_build(self) -> DeltaResult<TableConfiguration> {
        let schema = self
            .schema
            .unwrap_or_else(|| schema_ref! { nullable "value": INTEGER });
        let metadata =
            Metadata::try_new(None, None, schema, self.partition_columns, 0, self.props)?;

        TableConfiguration::try_new(metadata, self.protocol, self.table_root, self.version)
    }
}

/// Builds a mock [`Protocol`] for unit tests.
///
/// Defaults to a table-features protocol (reader 3, writer 7) listing no features.
pub(crate) struct MockProtocolBuilder {
    reader_features: Vec<TableFeature>,
    writer_features: Vec<TableFeature>,
    reader_version: i32,
    writer_version: i32,
}

impl MockProtocolBuilder {
    pub(crate) fn new() -> Self {
        Self {
            reader_features: vec![],
            writer_features: vec![],
            reader_version: TABLE_FEATURES_MIN_READER_VERSION,
            writer_version: TABLE_FEATURES_MIN_WRITER_VERSION,
        }
    }

    /// Sets the protocol features, splitting them across the reader and writer lists by type.
    pub(crate) fn with_features(
        mut self,
        features: impl IntoIterator<Item = impl Borrow<TableFeature>>,
    ) -> Self {
        self.writer_features = features
            .into_iter()
            .map(|feature| feature.borrow().clone())
            .collect();
        assert!(
            self.writer_features
                .iter()
                .all(|feature| feature.feature_type() != FeatureType::Unknown),
            "unknown features require explicit reader/writer feature lists"
        );
        self.reader_features = self
            .writer_features
            .iter()
            .filter(|feature| feature.feature_type() == FeatureType::ReaderWriter)
            .cloned()
            .collect();
        self
    }

    /// Sets the protocol's reader features explicitly.
    pub(crate) fn with_reader_features(
        mut self,
        features: impl IntoIterator<Item = impl Borrow<TableFeature>>,
    ) -> Self {
        self.reader_features = features
            .into_iter()
            .map(|feature| feature.borrow().clone())
            .collect();
        self
    }

    /// Sets the protocol's writer features explicitly.
    pub(crate) fn with_writer_features(
        mut self,
        features: impl IntoIterator<Item = impl Borrow<TableFeature>>,
    ) -> Self {
        self.writer_features = features
            .into_iter()
            .map(|feature| feature.borrow().clone())
            .collect();
        self
    }

    /// Sets the protocol's minimum reader and writer versions.
    pub(crate) fn with_versions(mut self, reader: i32, writer: i32) -> Self {
        self.reader_version = reader;
        self.writer_version = writer;
        self
    }

    /// Builds the protocol, panicking if it is invalid.
    #[track_caller]
    pub(crate) fn build(self) -> Protocol {
        // The protocol permits feature lists if and only if the version is exactly the
        // table-features version, so legacy versions drop them.
        Protocol::try_new(
            self.reader_version,
            self.writer_version,
            (self.reader_version == TABLE_FEATURES_MIN_READER_VERSION)
                .then_some(self.reader_features),
            (self.writer_version == TABLE_FEATURES_MIN_WRITER_VERSION)
                .then_some(self.writer_features),
        )
        .unwrap()
    }
}

// ==================== Test schema helpers ====================
//
// Reusable test schemas
// Each variant exists with and without column mapping metadata.

/// Builds a nullable [`StructField`] carrying column-mapping id + physical name metadata.
fn cm_field(
    name: &str,
    id: i64,
    physical_name: &str,
    ty: impl Into<KernelDataType>,
) -> StructField {
    StructField::nullable(name, ty).with_metadata([
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(id),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String(physical_name.into()),
        ),
    ])
}

/// Shared fixture for nested field-id propagation tests.
pub(crate) struct NestedFieldIdFixture {
    pub(crate) kernel_schema: StructType,
    pub(crate) input_arrow_data: StructArray,
    pub(crate) expected_arrow_schema: ArrowSchema,
}

/// Recursively collect `(field_name, metadata_value)` pairs for the given metadata key
/// across all (nested) Arrow fields in `schema`.
pub(crate) fn collect_arrow_field_metadata(
    schema: &ArrowSchema,
    metadata_key: &str,
) -> Vec<(String, String)> {
    fn collect_from_fields(
        fields: &[Arc<Field>],
        metadata_key: &str,
        out: &mut Vec<(String, String)>,
    ) {
        for field in fields {
            collect_from_field(field, metadata_key, out);
        }
    }

    fn collect_from_field(field: &Field, metadata_key: &str, out: &mut Vec<(String, String)>) {
        if let Some(value) = field.metadata().get(metadata_key) {
            out.push((field.name().clone(), value.clone()));
        }

        match field.data_type() {
            DataType::Struct(fields) => collect_from_fields(fields, metadata_key, out),
            DataType::List(entry) | DataType::Map(entry, _) => {
                collect_from_field(entry, metadata_key, out)
            }
            _ => {}
        }
    }

    let mut out = Vec::new();
    collect_from_fields(schema.fields(), metadata_key, &mut out);
    out
}

/// Build the kernel schema for `array_in_map: map<int, array<int>>` with caller-provided
/// top-level field metadata.
pub(crate) fn array_in_map_kernel_schema(
    metadata: impl IntoIterator<Item = (String, MetadataValue)>,
) -> StructType {
    let array_in_map = StructField::nullable(
        "array_in_map",
        MapType::new(
            KernelDataType::INTEGER,
            ArrayType::new(KernelDataType::INTEGER, true),
            true,
        ),
    )
    .with_metadata(metadata);
    schema! {
        (array_in_map),
    }
}

/// Build an [`array_in_map_kernel_schema`] with `parquet.field.id` on the top-level field
/// and a nested-ids JSON map (key/value/element) under `nested_ids_meta_key`.
pub(crate) fn array_in_map_with_field_ids(nested_ids_meta_key: &str) -> StructType {
    let nested_ids = MetadataValue::Other(test_utils::nested_ids_json(&[
        ("array_in_map.key", 100),
        ("array_in_map.value", 101),
        ("array_in_map.value.element", 102),
    ]));
    array_in_map_kernel_schema([
        (
            ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
            MetadataValue::from(1i64),
        ),
        (nested_ids_meta_key.to_string(), nested_ids),
    ])
}

/// Build an empty Arrow `StructArray` matching [`array_in_map_kernel_schema`] with no field-id
/// metadata.
pub(crate) fn array_in_map_arrow_data_without_field_ids() -> StructArray {
    let kernel_schema = array_in_map_kernel_schema(std::iter::empty::<(String, MetadataValue)>());
    let arrow_schema: ArrowSchema = (&kernel_schema).try_into_arrow().unwrap();
    let batch = RecordBatch::new_empty(Arc::new(arrow_schema));
    StructArray::try_new(
        batch.schema().fields.clone(),
        batch.columns().to_vec(),
        None,
    )
    .unwrap()
}

/// Build a [`NestedFieldIdFixture`] covering Array+Map nested-id propagation through a
/// Struct boundary.
///
/// ## 1. Kernel schema
///
/// Two [`StructField`]s `top` and `inner` carry `parquet.field.id` (rewritten to
/// `PARQUET:field_id` in the Arrow output) plus a `<nested_ids_meta_key>` JSON map rooted
/// at each field's name. Each carries an array inside a map.
///
/// ```json
/// {
///   "type": "struct",
///   "fields": [{
///     "name": "top",
///     "type": {
///       "type": "map",
///       "keyType":   {"type": "array", "elementType": "integer"},
///       "valueType": {"type": "struct", "fields": [{
///         "name": "inner",
///         "type": {"type": "map", "keyType": "integer",
///                  "valueType": {"type": "array", "elementType": "integer"}},
///         "metadata": {
///           "parquet.field.id": 2,
///           "<nested_ids_meta_key>": {
///             "inner.key": 200, "inner.value": 201, "inner.value.element": 202
///           }
///         }
///       }]}
///     },
///     "metadata": {
///       "parquet.field.id": 1,
///       "<nested_ids_meta_key>": {
///         "top.key": 100, "top.key.element": 101, "top.value": 102
///       }
///     }
///   }]
/// }
/// ```
///
/// ## 2. Input Arrow schema
///
/// Same shape as kernel schema with no metadata anywhere, except a *stale*
/// `PARQUET:field_id=999` on the synthesized `top.key.element` field.
///
/// ## 3. Expected output Arrow schema
///
/// What `try_into_arrow(kernel schema)` and
/// `apply_schema(input arrow schema, kernel schema)` should both produce:
/// - `top` and `inner` carry `PARQUET:field_id` (rewritten from `parquet.field.id`).
/// - Synthesized list/map `key`/`value`/`element` fields each carry `PARQUET:field_id` pulled from
///   the corresponding nested-ids JSON entry. `top.key.element` has `101` (kernel's), not the stale
///   `999` from the input.
///
/// ```json
/// {
///   "fields": [{
///     "name": "top", "type": "map",
///     "metadata": {"PARQUET:field_id": "1"},
///     "entries": { "name": "key_value", "type": "struct", "fields": [
///       {
///         "name": "key", "type": "list",
///         "metadata": {"PARQUET:field_id": "100"},
///         "element": {
///           "name": "element", "type": "int32",
///           "metadata": {"PARQUET:field_id": "101"}
///         }
///       },
///       {
///         "name": "value", "type": "struct",
///         "metadata": {"PARQUET:field_id": "102"},
///         "fields": [{
///           "name": "inner", "type": "map",
///           "metadata": {"PARQUET:field_id": "2"},
///           "entries": { "name": "key_value", "type": "struct", "fields": [
///             {
///               "name": "key", "type": "int32",
///               "metadata": {"PARQUET:field_id": "200"}
///             },
///             {
///               "name": "value", "type": "list",
///               "metadata": {"PARQUET:field_id": "201"},
///               "element": {
///                 "name": "element", "type": "int32",
///                 "metadata": {"PARQUET:field_id": "202"}
///               }
///             }
///           ]}
///         }]
///       }
///     ]}
///   }]
/// }
/// ```
pub(crate) fn complex_nested_with_field_ids(nested_ids_meta_key: &str) -> NestedFieldIdFixture {
    NestedFieldIdFixture {
        kernel_schema: build_complex_nested_kernel_schema(nested_ids_meta_key),
        input_arrow_data: build_arrow_input_with_stale_element_id(),
        expected_arrow_schema: expected_complex_nested_arrow_schema(),
    }
}

/// Build the input Arrow data for [`complex_nested_with_field_ids`] by
/// striping the metadata from [`build_complex_nested_kernel_schema`], and
/// add one stale `PARQUET:field_id` to the `top.key.element` field.
fn build_arrow_input_with_stale_element_id() -> StructArray {
    // Get the no-meta Arrow shape from the kernel schema.
    let plain_inner = StructField::nullable("inner", complex_nested_inner_map_type());
    let plain_top = StructField::nullable(
        "top",
        complex_nested_outer_map_type(schema! {
            (plain_inner),
        }),
    );
    let plain_kernel_schema = schema! {
        (plain_top),
    };
    let plain_arrow_schema: ArrowSchema = (&plain_kernel_schema).try_into_arrow().unwrap();

    // Add stale `PARQUET:field_id` to the `top.key.element` field.
    let top = plain_arrow_schema.field(0);
    let DataType::Map(entries, sorted) = top.data_type() else {
        unreachable!("top is a Map by construction");
    };
    let DataType::Struct(entries_fields) = entries.data_type() else {
        unreachable!("map entries is a Struct by construction");
    };
    let outer_key = &entries_fields[0];
    let DataType::List(outer_element) = outer_key.data_type() else {
        unreachable!("outer map key is a List by construction");
    };
    let stale_element = outer_element
        .as_ref()
        .clone()
        .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), "999".to_string())].into());
    let new_outer_key = Field::new(
        outer_key.name(),
        DataType::List(Arc::new(stale_element)),
        outer_key.is_nullable(),
    );
    let new_entries = Field::new(
        entries.name(),
        DataType::Struct(vec![new_outer_key, entries_fields[1].as_ref().clone()].into()),
        entries.is_nullable(),
    );
    let new_top = Field::new(
        top.name(),
        DataType::Map(Arc::new(new_entries), *sorted),
        top.is_nullable(),
    );
    let arrow_input_schema = ArrowSchema::new(vec![new_top]);
    let batch = RecordBatch::new_empty(Arc::new(arrow_input_schema));
    StructArray::try_new(
        batch.schema().fields.clone(),
        batch.columns().to_vec(),
        None,
    )
    .unwrap()
}

fn complex_nested_inner_map_type() -> KernelDataType {
    KernelDataType::from(MapType::new(
        KernelDataType::INTEGER,
        ArrayType::new(KernelDataType::INTEGER, true),
        true,
    ))
}

fn complex_nested_outer_map_type(struct_value: StructType) -> KernelDataType {
    KernelDataType::from(MapType::new(
        ArrayType::new(KernelDataType::INTEGER, true),
        struct_value,
        true,
    ))
}

/// Build the kernel schema described by [`complex_nested_with_field_ids`].
pub(crate) fn build_complex_nested_kernel_schema(nested_ids_meta_key: &str) -> StructType {
    let top_nested_ids = test_utils::nested_ids_json(&[
        ("top.key", 100),
        ("top.key.element", 101),
        ("top.value", 102),
    ]);
    let inner_nested_ids = test_utils::nested_ids_json(&[
        ("inner.key", 200),
        ("inner.value", 201),
        ("inner.value.element", 202),
    ]);
    // Each `StructField` carries `parquet.field.id` plus the nested-ids JSON.
    let inner_field = StructField::nullable("inner", complex_nested_inner_map_type())
        .with_metadata([
            (
                ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                MetadataValue::from(2i64),
            ),
            (
                nested_ids_meta_key.to_string(),
                MetadataValue::Other(inner_nested_ids),
            ),
        ]);
    let top_field = StructField::nullable(
        "top",
        complex_nested_outer_map_type(schema! {
            (inner_field),
        }),
    )
    .with_metadata([
        (
            ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
            MetadataValue::from(1i64),
        ),
        (
            nested_ids_meta_key.to_string(),
            MetadataValue::Other(top_nested_ids),
        ),
    ]);
    schema! {
        (top_field),
    }
}

/// Build the expected output Arrow schema for [`complex_nested_with_field_ids`].
fn expected_complex_nested_arrow_schema() -> ArrowSchema {
    // top.value.inner.value.element: int (PARQUET:field_id=202).
    let inner_list_element = Field::new("element", DataType::Int32, true)
        .with_metadata(parquet_field_id_metadata(Some(202)));
    // top.value.inner.value: list<int> (PARQUET:field_id=201).
    let inner_value = Field::new("value", DataType::List(Arc::new(inner_list_element)), true)
        .with_metadata(parquet_field_id_metadata(Some(201)));
    // top.value.inner.key: int (PARQUET:field_id=200).
    let inner_key = Field::new("key", DataType::Int32, false)
        .with_metadata(parquet_field_id_metadata(Some(200)));
    // top.value.inner.key_value: synthesized map-entries struct (no field id).
    let inner_entries = Field::new(
        "key_value",
        DataType::Struct(vec![inner_key, inner_value].into()),
        false,
    );
    // top.value.inner: map<int, list<int>> (PARQUET:field_id=2).
    let inner_field = Field::new("inner", DataType::Map(Arc::new(inner_entries), false), true)
        .with_metadata(parquet_field_id_metadata(Some(2)));
    // top.value: struct<inner: ...> (PARQUET:field_id=102).
    let struct_value_field = Field::new("value", DataType::Struct(vec![inner_field].into()), true)
        .with_metadata(parquet_field_id_metadata(Some(102)));
    // top.key.element: int (PARQUET:field_id=101).
    let outer_key_element = Field::new("element", DataType::Int32, true)
        .with_metadata(parquet_field_id_metadata(Some(101)));
    // top.key: list<int> (PARQUET:field_id=100).
    let outer_key = Field::new("key", DataType::List(Arc::new(outer_key_element)), false)
        .with_metadata(parquet_field_id_metadata(Some(100)));
    // top.key_value: synthesized map-entries struct (no field id).
    let outer_entries = Field::new(
        "key_value",
        DataType::Struct(vec![outer_key, struct_value_field].into()),
        false,
    );
    // top: map<list<int>, struct<...>> (PARQUET:field_id=1).
    let top_field = Field::new("top", DataType::Map(Arc::new(outer_entries), false), true)
        .with_metadata(parquet_field_id_metadata(Some(1)));
    ArrowSchema::new(vec![top_field])
}

/// Flat schema: `[id: long, name: string]`
pub(crate) fn test_schema_flat() -> SchemaRef {
    schema_ref! {
        nullable "id": LONG,
        nullable "name": STRING,
    }
}

/// Flat schema with column mapping metadata.
pub(crate) fn test_schema_flat_with_column_mapping() -> SchemaRef {
    schema_ref! {
        (cm_field("id", 1, "phys_id", KernelDataType::LONG)),
        (cm_field("name", 2, "phys_name", KernelDataType::STRING)),
    }
}

/// Nested struct schema with array and map inside the struct
pub(crate) fn test_schema_nested() -> SchemaRef {
    schema_ref! {
        nullable "id": LONG,
        nullable "info": {
            nullable "name": STRING,
            nullable "age": INTEGER,
            nullable "tags": { STRING => nullable STRING },
            nullable "scores": [ nullable INTEGER ],
        },
    }
}

/// Nested struct schema with column mapping metadata.
pub(crate) fn test_schema_nested_with_column_mapping() -> SchemaRef {
    schema_ref! {
        (cm_field("id", 1, "phys_id", KernelDataType::LONG)),
        (cm_field("info", 2, "phys_info", schema! {
            (cm_field("name", 3, "phys_name", KernelDataType::STRING)),
            (cm_field("age", 4, "phys_age", KernelDataType::INTEGER)),
            (cm_field(
                "tags",
                5,
                "phys_tags",
                MapType::new(KernelDataType::STRING, KernelDataType::STRING, true),
            )),
            (cm_field(
                "scores",
                6,
                "phys_scores",
                ArrayType::new(KernelDataType::INTEGER, true),
            )),
        })),
    }
}

/// Schema with a map
pub(crate) fn test_schema_with_map() -> SchemaRef {
    schema_ref! {
        nullable "id": LONG,
        nullable "entries": { STRING => nullable {
            nullable "key": STRING,
            nullable "value": INTEGER,
        } },
        nullable "name": STRING,
    }
}

/// Schema with a map and column mapping metadata.
pub(crate) fn test_schema_with_map_and_column_mapping() -> SchemaRef {
    let value_struct = schema! {
        (cm_field("key", 4, "phys_key", KernelDataType::STRING)),
        (cm_field("value", 5, "phys_value", KernelDataType::INTEGER)),
    };
    schema_ref! {
        (cm_field("id", 1, "phys_id", KernelDataType::LONG)),
        (cm_field("entries", 2, "phys_entries",
            MapType::new(KernelDataType::STRING, value_struct, true))),
        (cm_field("name", 3, "phys_name", KernelDataType::STRING)),
    }
}

/// Schema with an array
pub(crate) fn test_schema_with_array() -> SchemaRef {
    schema_ref! {
        nullable "id": LONG,
        nullable "items": [ nullable {
            nullable "label": STRING,
            nullable "count": INTEGER,
        } ],
        nullable "name": STRING,
    }
}

/// Schema with an array and column mapping metadata.
pub(crate) fn test_schema_with_array_and_column_mapping() -> SchemaRef {
    let item_struct = schema! {
        (cm_field("label", 4, "phys_label", KernelDataType::STRING)),
        (cm_field("count", 5, "phys_count", KernelDataType::INTEGER)),
    };
    schema_ref! {
        (cm_field("id", 1, "phys_id", KernelDataType::LONG)),
        (cm_field("items", 2, "phys_items", ArrayType::new(item_struct, true))),
        (cm_field("name", 3, "phys_name", KernelDataType::STRING)),
    }
}

/// Deeply nested schema: struct -> array -> struct -> map(value) -> struct.
///
/// The leaf struct field is intentionally **not** annotated with column mapping metadata,
/// so this schema can be used to test error paths when column mapping is enabled.
pub(crate) fn test_deep_nested_schema_missing_leaf_cm() -> StructType {
    let map_type = MapType::new(
        KernelDataType::STRING,
        schema! { not_null "leaf": INTEGER },
        true,
    );
    let array_type = ArrayType::new(
        schema! {
            (cm_field("mid_field", 2, "phys_mid_field", map_type)),
        },
        true,
    );
    schema! {
        (cm_field("top", 1, "phys_top", array_type)),
    }
}

/// Build a create-table transaction with the given schema and column mapping mode.
/// Returns the engine and uncommitted transaction.
pub(crate) fn setup_column_mapping_txn(
    schema: SchemaRef,
    mode: ColumnMappingMode,
) -> DeltaResult<(Arc<dyn Engine>, Transaction<CreateTable>)> {
    let mode_str = match mode {
        ColumnMappingMode::Name => "name",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::None => "none",
    };
    let store = Arc::new(InMemory::new());
    let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store));

    let txn = create_table("memory:///test_table", schema, "DefaultEngine")
        .with_table_properties([("delta.columnMapping.mode", mode_str)])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    Ok((engine, txn))
}

/// Validate that a physical schema matches the logical schema's column mapping metadata.
/// For Name/Id modes, checks physicalName, columnMapping.id, and parquet.field.id on
/// each field. For None mode, only checks field names match.
pub(crate) fn validate_physical_schema_column_mapping(
    logical_schema: &StructType,
    physical_schema: &StructType,
    mode: ColumnMappingMode,
) {
    assert_eq!(
        physical_schema.fields().count(),
        logical_schema.fields().count()
    );

    // Collect expected (physical_name, field_id) from logical schema
    let expected: Vec<_> = logical_schema
        .fields()
        .map(|f| {
            let physical_name =
                match f.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
                    Some(MetadataValue::String(name)) => name.clone(),
                    _ if mode == ColumnMappingMode::None => f.name().to_string(),
                    _ => panic!("Logical field '{}' missing physicalName metadata", f.name()),
                };
            let field_id = match f.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
                Some(MetadataValue::Number(id)) => *id,
                _ if mode == ColumnMappingMode::None => -1,
                _ => panic!(
                    "Logical field '{}' missing columnMapping.id metadata",
                    f.name()
                ),
            };
            (physical_name, field_id)
        })
        .collect();

    // Validate each physical field against expected values
    for (physical_field, (expected_name, expected_id)) in
        physical_schema.fields().zip(expected.iter())
    {
        assert_eq!(
            physical_field.name(),
            expected_name,
            "Physical field name mismatch"
        );

        if mode == ColumnMappingMode::None {
            continue;
        }

        assert_eq!(
            physical_field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String(expected_name.clone())),
            "columnMapping.physicalName mismatch for '{}'",
            physical_field.name()
        );

        assert_eq!(
            physical_field.get_config_value(&ColumnMetadataKey::ColumnMappingId),
            Some(&MetadataValue::Number(*expected_id)),
            "columnMapping.id mismatch for '{}'",
            physical_field.name()
        );

        assert_eq!(
            physical_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
            Some(&MetadataValue::Number(*expected_id)),
            "parquet.field.id mismatch for '{}'",
            physical_field.name()
        );
    }
}

fn resolve_test_table_path(table_name: &str) -> DeltaResult<(PathBuf, Option<TempDir>)> {
    match load_test_data("tests/data", table_name) {
        Ok(test_dir) => {
            let test_path = test_dir.path().join(table_name);
            Ok((test_path, Some(test_dir)))
        }
        Err(_) => {
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/data")
                .join(table_name);
            let path = std::fs::canonicalize(&path).map_err(|source| {
                KernelError::from(source)
                    .with_context(crate::error::ErrorContext::Operation("canonicalize path"))
                    .with_context(crate::error::ErrorContext::File {
                        path: path.display().to_string(),
                    })
            })?;
            Ok((path, None))
        }
    }
}

/// Copies a test-table fixture into a writable temporary directory.
pub(crate) fn copy_test_table(table_name: &str) -> DeltaResult<(Url, TempDir)> {
    let (source, _source_tempdir) = resolve_test_table_path(table_name)?;
    let tempdir = tempfile::tempdir().map_err(KernelError::from)?;
    let table_path = tempdir.path().join(table_name);
    copy_directory(&source, &table_path).map_err(|error| {
        KernelError::from(error)
            .with_context(crate::error::ErrorContext::Operation("copy test table"))
            .with_context(crate::error::ErrorContext::File {
                path: source.display().to_string(),
            })
    })?;
    let url = Url::from_directory_path(&table_path)
        .map_err(|_| KernelError::invalid_table_location("Failed to create URL from path"))?;
    Ok((url, tempdir))
}

/// Load a test table from tests/data directory.
/// Tries compressed (tar.zst) first, falls back to extracted.
/// Returns (engine, snapshot, optional tempdir). The TempDir must be kept alive
/// for the duration of the test to prevent premature cleanup of extracted files.
pub(crate) fn load_test_table(
    table_name: &str,
) -> DeltaResult<(Arc<dyn Engine>, SnapshotRef, Option<TempDir>)> {
    let (path, tempdir) = resolve_test_table_path(table_name)?;

    let url = Url::from_directory_path(&path)
        .map_err(|_| KernelError::invalid_table_location("Failed to create URL from path"))?;

    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    Ok((engine, snapshot, tempdir))
}

pub(crate) mod column_mapping_physical_name_dedup_fixtures {
    use crate::schema::{
        schema, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
        StructType,
    };

    /// Two fields with the same physical name at different physical paths should be accepted.
    pub(crate) fn same_phy_name_different_paths() -> StructType {
        let nested = schema! {
            (cm_field("id", 3, "id", DataType::INTEGER)),
        };
        schema! {
            (cm_field("id", 1, "id", DataType::INTEGER)),
            (cm_field("nested", 2, "nested", nested)),
        }
    }

    /// Two nested fields with same physical path should be rejected.
    pub(crate) fn deeply_nested_repeat_physical_paths() -> StructType {
        let inner = schema! {
            (cm_field("a", 2, "x", DataType::INTEGER)),
            (cm_field("b", 3, "x", DataType::INTEGER)),
        };
        let arr_of_struct = ArrayType::new(inner, true);
        let map_to_arr = MapType::new(DataType::STRING, arr_of_struct, true);
        schema! {
            (cm_field("outer", 1, "outer", map_to_arr)),
        }
    }

    /// Full logical paths of the two colliding fields in
    /// [`deeply_nested_repeat_physical_paths`], in the order the walker visits them.
    pub(crate) fn deeply_nested_collider_paths() -> (&'static str, &'static str) {
        (
            "outer.`<map value>`.`<array element>`.a",
            "outer.`<map value>`.`<array element>`.b",
        )
    }

    /// Two collision sites in the same schema:
    /// - **shallower** (visited first by DFS): top-level siblings `a` and `b`, both have physical
    ///   name "p".
    /// - **deeper** (never reached): inside `nested` struct, siblings `x` and `y`, both have
    ///   physical name "q".
    ///
    /// Dedup must error at the shallower site and never report the deeper one.
    pub(crate) fn multiple_physical_name_collisions() -> StructType {
        schema! {
            (cm_field(
                "a",
                1,
                "p",
                schema! {
                    (cm_field("aa", 6, "aa", DataType::INTEGER)),
                },
            )),
            (cm_field(
                "b",
                2,
                "p",
                schema! {
                    (cm_field("bb", 7, "bb", DataType::INTEGER)),
                },
            )),
            (cm_field(
                "nested",
                3,
                "nested",
                schema! {
                    (cm_field("x", 4, "q", DataType::INTEGER)),
                    (cm_field("y", 5, "q", DataType::INTEGER)),
                },
            )),
        }
    }

    fn cm_field(name: &str, id: i64, phys: &str, ty: impl Into<DataType>) -> StructField {
        StructField::nullable(name, ty).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(id),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(phys.to_string()),
            ),
        ])
    }
}

#[cfg(feature = "geo-type-in-dev")]
pub(crate) fn geometry_type(crs: &str) -> KernelDataType {
    PrimitiveType::Geometry(Box::new(GeometryType::try_new(crs).unwrap())).into()
}

#[cfg(feature = "geo-type-in-dev")]
pub(crate) fn geography_type(crs: &str, algorithm: EdgeInterpolationAlgorithm) -> KernelDataType {
    PrimitiveType::Geography(Box::new(GeographyType::try_new(crs, algorithm).unwrap())).into()
}

/// A [`CancellationToken`](crate::CancellationToken) for kernel unit tests, cancellable on demand.
///
/// `test_utils::TestCancellationToken` cannot be used here: it implements the trait against
/// `test_utils`'s own `delta_kernel` dependency, a different crate instance than the
/// crate-under-test, so its impl does not satisfy the crate-self trait in unit tests. Integration
/// tests (which see one external `delta_kernel`) use the `test_utils` one instead.
#[derive(Default)]
pub(crate) struct TestCancellationToken(AtomicBool);

impl TestCancellationToken {
    /// A token that is already cancelled.
    pub(crate) fn cancelled() -> Self {
        let token = Self::default();
        token.cancel();
        token
    }

    /// Request cancellation.
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl crate::cancellation::CancellationToken for TestCancellationToken {
    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    fn cancelled_future(&self) -> crate::cancellation::CancelledFuture<'_> {
        Box::pin(std::future::ready(()))
    }
}
