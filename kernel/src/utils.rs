//! Various utility functions/macros used throughout the kernel
use std::borrow::Cow;
use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use delta_kernel_derive::internal_api;
use url::Url;

use crate::{DeltaResult, Error};

/// convenient way to return an error if a condition isn't true
macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}

pub(crate) use require;

/// Try to parse string uri into a URL for a table path. This will do it's best to handle things
/// like `/local/paths`, and even `../relative/paths`.
#[allow(unused)]
#[internal_api]
pub(crate) fn try_parse_uri(uri: impl AsRef<str>) -> DeltaResult<Url> {
    let uri = uri.as_ref();
    let uri_type = resolve_uri_type(uri)?;
    let url = match uri_type {
        UriType::LocalPath(path) => {
            if !path.exists() {
                // When we support writes, create a directory if we can
                return Err(Error::InvalidTableLocation(format!(
                    "Path does not exist: {path:?}"
                )));
            }
            if !path.is_dir() {
                return Err(Error::InvalidTableLocation(format!(
                    "{path:?} is not a directory"
                )));
            }
            let path = std::fs::canonicalize(path).map_err(|err| {
                let msg = format!("Invalid table location: {uri} Error: {err:?}");
                Error::InvalidTableLocation(msg)
            })?;
            Url::from_directory_path(path.clone()).map_err(|_| {
                let msg = format!(
                    "Could not construct a URL from canonicalized path: {path:?}.\n\
                     Something must be very wrong with the table path."
                );
                Error::InvalidTableLocation(msg)
            })?
        }
        UriType::Url(url) => url,
    };
    Ok(url)
}

#[allow(unused)]
#[derive(Debug)]
enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path is either local path or
/// some kind or URL.
///
/// Will return an error if the path is not valid.
#[allow(unused)]
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let table_uri = if table_uri.ends_with('/') {
        Cow::Borrowed(table_uri)
    } else {
        Cow::Owned(format!("{table_uri}/"))
    };
    if let Ok(url) = Url::parse(&table_uri) {
        let scheme = url.scheme().to_string();
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(
                url.to_file_path()
                    .map_err(|_| Error::invalid_table_location(table_uri))?,
            ))
        } else if scheme.len() == 1 {
            // NOTE this check is required to support absolute windows paths which may properly
            // parse as url we assume here that a single character scheme is a windows drive letter
            Ok(UriType::LocalPath(PathBuf::from(table_uri.as_ref())))
        } else {
            Ok(UriType::Url(url))
        }
    } else {
        Ok(UriType::LocalPath(table_uri.deref().into()))
    }
}

/// Returns the current time as a Duration since Unix epoch.
pub(crate) fn current_time_duration() -> DeltaResult<Duration> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::generic(format!("System time before Unix epoch: {e}")))
}

/// Returns the current time in milliseconds since Unix epoch.
pub(crate) fn current_time_ms() -> DeltaResult<i64> {
    let duration = current_time_duration()?;
    i64::try_from(duration.as_millis())
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))
}

/// Extension trait for adding completion callbacks to iterators.
pub(crate) trait IteratorExt: Iterator + Sized {
    /// Wraps this iterator to call a closure when fully exhausted.
    ///
    /// The closure is called only when `next()` returns `None`. If the iterator
    /// is dropped before exhaustion, a warning is logged but the closure is not called.
    fn on_complete<F: FnOnce()>(self, f: F) -> OnComplete<Self, F> {
        OnComplete {
            inner: self,
            on_complete: Some(f),
        }
    }
}

impl<I: Iterator> IteratorExt for I {}

/// Iterator adaptor that executes a closure when fully exhausted.
pub(crate) struct OnComplete<I, F: FnOnce()> {
    inner: I,
    on_complete: Option<F>,
}

impl<I, F: FnOnce()> Drop for OnComplete<I, F> {
    fn drop(&mut self) {
        if self.on_complete.is_some() {
            tracing::debug!(
                "OnComplete iterator dropped before exhaustion; completion callback not called"
            );
        }
    }
}

impl<I, F> Iterator for OnComplete<I, F>
where
    I: Iterator,
    F: FnOnce(),
{
    type Item = I::Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(item) => Some(item),
            None => {
                if let Some(f) = self.on_complete.take() {
                    f();
                }
                None
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use itertools::Itertools;
    use serde::Serialize;
    use tempfile::TempDir;
    use test_utils::{delta_path_for_version, load_test_data};
    use url::Url;

    use crate::actions::{
        get_all_actions_schema, Add, Cdc, CommitInfo, Metadata, Protocol, Remove,
    };
    use crate::arrow::array::{RecordBatch, StringArray, StructArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::{parquet_field_id_metadata, TryIntoArrow as _};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::DefaultEngineBuilder;
    use crate::engine::sync::SyncEngine;
    use crate::metrics::{MetricEvent, MetricsReporter};
    use crate::object_store::local::LocalFileSystem;
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStoreExt as _;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::table_features::ColumnMappingMode;
    use crate::transaction::create_table::create_table;
    use crate::transaction::{CreateTable, Transaction};
    use crate::{DeltaResult, Engine, EngineData, Error, Snapshot, SnapshotRef};

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
        ArrayType, ColumnMetadataKey, DataType as KernelDataType, MapType, MetadataValue,
        PrimitiveType, SchemaRef, StructField, StructType,
    };

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

    // TODO: allow tests to pass in context (issue#1133)
    #[track_caller]
    pub(crate) fn assert_result_error_with_message<T, E: ToString>(
        res: Result<T, E>,
        message: &str,
    ) {
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
        err_msg: &str,
    ) {
        make_test_tc(schema_with.clone(), protocol_with.clone(), [])
            .expect("feature present + supported");
        make_test_tc(schema_without.clone(), protocol_without.clone(), [])
            .expect("feature absent + unsupported");
        make_test_tc(schema_without.clone(), protocol_with.clone(), [])
            .expect("feature absent + supported");
        assert_result_error_with_message(
            make_test_tc(schema_with.clone(), protocol_without.clone(), []),
            err_msg,
        );
        for schema in extra_err_schemas {
            assert_result_error_with_message(
                make_test_tc((*schema).clone(), protocol_without.clone(), []),
                err_msg,
            );
        }
    }

    /// Creates a [`TableConfiguration`] from a schema, protocol, and table properties.
    /// Useful for testing validators that need a TC.
    pub(crate) fn make_test_tc(
        schema: StructType,
        protocol: Protocol,
        props: impl IntoIterator<Item = (String, String)>,
    ) -> crate::DeltaResult<crate::table_configuration::TableConfiguration> {
        let schema = std::sync::Arc::new(schema);
        let metadata =
            Metadata::try_new(None, None, schema, vec![], 0, props.into_iter().collect()).unwrap();
        let table_root = Url::try_from("file:///").unwrap();
        crate::table_configuration::TableConfiguration::try_new(metadata, protocol, table_root, 0)
    }

    /// Helper to get a field from a StructType by name, panicking if not found.
    pub(crate) fn get_schema_field(struct_type: &StructType, name: &str) -> StructField {
        struct_type
            .fields()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("Field '{name}' not found"))
            .clone()
    }

    /// Validates that a schema has the expected checkpoint structure with top-level action fields
    /// and proper nested types for add, metaData, and protocol actions.
    pub(crate) fn validate_checkpoint_schema(schema: &SchemaRef) {
        // Verify top-level action fields exist and are structs
        let top_level_fields = ["txn", "add", "remove", "metaData", "protocol"];
        for field_name in top_level_fields {
            let field = get_schema_field(schema, field_name);
            assert!(
                matches!(field.data_type(), KernelDataType::Struct(_)),
                "Field '{field_name}' should be a struct type"
            );
        }

        // Verify 'add' struct has expected fields with correct types
        let add_field = get_schema_field(schema, "add");
        let add_struct = match add_field.data_type() {
            KernelDataType::Struct(s) => s,
            _ => panic!("'add' should be a struct"),
        };
        assert_eq!(
            get_schema_field(add_struct, "path").data_type(),
            &KernelDataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            get_schema_field(add_struct, "size").data_type(),
            &KernelDataType::Primitive(PrimitiveType::Long)
        );
        assert!(
            matches!(
                get_schema_field(add_struct, "partitionValues").data_type(),
                KernelDataType::Map(_)
            ),
            "'partitionValues' should be a map type"
        );

        // Verify 'metaData' struct has nested 'format' struct
        let metadata_field = get_schema_field(schema, "metaData");
        let metadata_struct = match metadata_field.data_type() {
            KernelDataType::Struct(s) => s,
            _ => panic!("'metaData' should be a struct"),
        };
        let format_field = get_schema_field(metadata_struct, "format");
        let format_struct = match format_field.data_type() {
            KernelDataType::Struct(s) => s,
            _ => panic!("'format' should be a struct"),
        };
        assert_eq!(
            get_schema_field(format_struct, "provider").data_type(),
            &KernelDataType::Primitive(PrimitiveType::String)
        );

        // Verify 'protocol' struct has version fields
        let protocol_field = get_schema_field(schema, "protocol");
        let protocol_struct = match protocol_field.data_type() {
            KernelDataType::Struct(s) => s,
            _ => panic!("'protocol' should be a struct"),
        };
        assert_eq!(
            get_schema_field(protocol_struct, "minReaderVersion").data_type(),
            &KernelDataType::Primitive(PrimitiveType::Integer)
        );
        assert_eq!(
            get_schema_field(protocol_struct, "minWriterVersion").data_type(),
            &KernelDataType::Primitive(PrimitiveType::Integer)
        );
    }

    // ==================== Test schema helpers ====================
    //
    // Reusable test schemas
    // Each variant exists with and without column mapping metadata.

    /// Helper to add column mapping metadata to a [`StructField`].
    fn with_column_mapping(field: StructField, id: i64, physical_name: &str) -> StructField {
        field.with_metadata([
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
            KernelDataType::Map(Box::new(MapType::new(
                KernelDataType::INTEGER,
                KernelDataType::Array(Box::new(ArrayType::new(KernelDataType::INTEGER, true))),
                true,
            ))),
        )
        .with_metadata(metadata);
        StructType::try_new(vec![array_in_map]).unwrap()
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
        let kernel_schema =
            array_in_map_kernel_schema(std::iter::empty::<(String, MetadataValue)>());
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
    /// - `top` and `inner` carry `PARQUET:field_id` (rewritten from `parquet.field.id`) plus the
    ///   nested-ids JSON.
    /// - Synthesized list/map `key`/`value`/`element` fields each carry `PARQUET:field_id` pulled
    ///   from the corresponding nested-ids JSON entry. `top.key.element` has `101` (kernel's), not
    ///   the stale `999` from the input.
    pub(crate) fn complex_nested_with_field_ids(nested_ids_meta_key: &str) -> NestedFieldIdFixture {
        let (kernel_schema, top_nested_ids, inner_nested_ids) =
            build_complex_nested_kernel_schema(nested_ids_meta_key);
        let input_arrow_data = build_arrow_input_with_stale_element_id();
        let expected_arrow_schema = expected_complex_nested_arrow_schema(
            nested_ids_meta_key,
            top_nested_ids,
            inner_nested_ids,
        );
        NestedFieldIdFixture {
            kernel_schema,
            input_arrow_data,
            expected_arrow_schema,
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
            complex_nested_outer_map_type(StructType::try_new(vec![plain_inner]).unwrap()),
        );
        let plain_kernel_schema = StructType::try_new(vec![plain_top]).unwrap();
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
        KernelDataType::Map(Box::new(MapType::new(
            KernelDataType::INTEGER,
            KernelDataType::Array(Box::new(ArrayType::new(KernelDataType::INTEGER, true))),
            true,
        )))
    }

    fn complex_nested_outer_map_type(struct_value: StructType) -> KernelDataType {
        KernelDataType::Map(Box::new(MapType::new(
            KernelDataType::Array(Box::new(ArrayType::new(KernelDataType::INTEGER, true))),
            KernelDataType::Struct(Box::new(struct_value)),
            true,
        )))
    }

    /// Build the kernel schema described by [`complex_nested_with_field_ids`], returning the
    /// schema and the two nested-id JSON maps (top + inner) so the expected-Arrow-schema helper
    /// can reuse them without re-deriving.
    fn build_complex_nested_kernel_schema(
        nested_ids_meta_key: &str,
    ) -> (StructType, serde_json::Value, serde_json::Value) {
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
        // Each top-level field carries `parquet.field.id` plus the nested-ids JSON.
        let inner_field = StructField::nullable("inner", complex_nested_inner_map_type())
            .with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                    MetadataValue::from(2i64),
                ),
                (
                    nested_ids_meta_key.to_string(),
                    MetadataValue::Other(inner_nested_ids.clone()),
                ),
            ]);
        let top_field = StructField::nullable(
            "top",
            complex_nested_outer_map_type(StructType::try_new(vec![inner_field]).unwrap()),
        )
        .with_metadata([
            (
                ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                MetadataValue::from(1i64),
            ),
            (
                nested_ids_meta_key.to_string(),
                MetadataValue::Other(top_nested_ids.clone()),
            ),
        ]);
        let schema = StructType::try_new(vec![top_field]).unwrap();
        (schema, top_nested_ids, inner_nested_ids)
    }

    /// Build the expected output Arrow schema for [`complex_nested_with_field_ids`].
    fn expected_complex_nested_arrow_schema(
        nested_ids_meta_key: &str,
        top_nested_ids: serde_json::Value,
        inner_nested_ids: serde_json::Value,
    ) -> ArrowSchema {
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
        // top.value.inner: map<int, list<int>> (PARQUET:field_id=2 + nested-ids JSON).
        let inner_field_metadata = [
            (PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string()),
            (
                nested_ids_meta_key.to_string(),
                serde_json::to_string(&inner_nested_ids).unwrap(),
            ),
        ]
        .into();
        let inner_field = Field::new("inner", DataType::Map(Arc::new(inner_entries), false), true)
            .with_metadata(inner_field_metadata);
        // top.value: struct<inner: ...> (PARQUET:field_id=102).
        let struct_value_field =
            Field::new("value", DataType::Struct(vec![inner_field].into()), true)
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
        // top: map<list<int>, struct<...>> (PARQUET:field_id=1 + nested-ids JSON).
        let top_field_metadata = [
            (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            (
                nested_ids_meta_key.to_string(),
                serde_json::to_string(&top_nested_ids).unwrap(),
            ),
        ]
        .into();
        let top_field = Field::new("top", DataType::Map(Arc::new(outer_entries), false), true)
            .with_metadata(top_field_metadata);
        ArrowSchema::new(vec![top_field])
    }

    /// Flat schema: `[id: long, name: string]`
    pub(crate) fn test_schema_flat() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::new("id", KernelDataType::LONG, true),
            StructField::nullable("name", KernelDataType::STRING),
        ]))
    }

    /// Flat schema with column mapping metadata.
    pub(crate) fn test_schema_flat_with_column_mapping() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            with_column_mapping(
                StructField::new("id", KernelDataType::LONG, true),
                1,
                "phys_id",
            ),
            with_column_mapping(
                StructField::nullable("name", KernelDataType::STRING),
                2,
                "phys_name",
            ),
        ]))
    }

    /// Nested struct schema with array and map inside the struct
    pub(crate) fn test_schema_nested() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::new("id", KernelDataType::LONG, true),
            StructField::nullable(
                "info",
                StructType::new_unchecked([
                    StructField::nullable("name", KernelDataType::STRING),
                    StructField::nullable("age", KernelDataType::INTEGER),
                    StructField::nullable(
                        "tags",
                        MapType::new(KernelDataType::STRING, KernelDataType::STRING, true),
                    ),
                    StructField::nullable("scores", ArrayType::new(KernelDataType::INTEGER, true)),
                ]),
            ),
        ]))
    }

    /// Nested struct schema with column mapping metadata.
    pub(crate) fn test_schema_nested_with_column_mapping() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            with_column_mapping(
                StructField::new("id", KernelDataType::LONG, true),
                1,
                "phys_id",
            ),
            with_column_mapping(
                StructField::nullable(
                    "info",
                    StructType::new_unchecked([
                        with_column_mapping(
                            StructField::nullable("name", KernelDataType::STRING),
                            3,
                            "phys_name",
                        ),
                        with_column_mapping(
                            StructField::nullable("age", KernelDataType::INTEGER),
                            4,
                            "phys_age",
                        ),
                        with_column_mapping(
                            StructField::nullable(
                                "tags",
                                MapType::new(KernelDataType::STRING, KernelDataType::STRING, true),
                            ),
                            5,
                            "phys_tags",
                        ),
                        with_column_mapping(
                            StructField::nullable(
                                "scores",
                                ArrayType::new(KernelDataType::INTEGER, true),
                            ),
                            6,
                            "phys_scores",
                        ),
                    ]),
                ),
                2,
                "phys_info",
            ),
        ]))
    }

    /// Schema with a map
    pub(crate) fn test_schema_with_map() -> SchemaRef {
        let value_struct = StructType::new_unchecked([
            StructField::nullable("key", KernelDataType::STRING),
            StructField::nullable("value", KernelDataType::INTEGER),
        ]);
        Arc::new(StructType::new_unchecked([
            StructField::new("id", KernelDataType::LONG, true),
            StructField::nullable(
                "entries",
                MapType::new(
                    KernelDataType::STRING,
                    KernelDataType::Struct(Box::new(value_struct)),
                    true,
                ),
            ),
            StructField::nullable("name", KernelDataType::STRING),
        ]))
    }

    /// Schema with a map and column mapping metadata.
    pub(crate) fn test_schema_with_map_and_column_mapping() -> SchemaRef {
        let value_struct = StructType::new_unchecked([
            with_column_mapping(
                StructField::nullable("key", KernelDataType::STRING),
                4,
                "phys_key",
            ),
            with_column_mapping(
                StructField::nullable("value", KernelDataType::INTEGER),
                5,
                "phys_value",
            ),
        ]);
        Arc::new(StructType::new_unchecked([
            with_column_mapping(
                StructField::new("id", KernelDataType::LONG, true),
                1,
                "phys_id",
            ),
            with_column_mapping(
                StructField::nullable(
                    "entries",
                    MapType::new(
                        KernelDataType::STRING,
                        KernelDataType::Struct(Box::new(value_struct)),
                        true,
                    ),
                ),
                2,
                "phys_entries",
            ),
            with_column_mapping(
                StructField::nullable("name", KernelDataType::STRING),
                3,
                "phys_name",
            ),
        ]))
    }

    /// Schema with an array
    pub(crate) fn test_schema_with_array() -> SchemaRef {
        let item_struct = StructType::new_unchecked([
            StructField::nullable("label", KernelDataType::STRING),
            StructField::nullable("count", KernelDataType::INTEGER),
        ]);
        Arc::new(StructType::new_unchecked([
            StructField::new("id", KernelDataType::LONG, true),
            StructField::nullable(
                "items",
                ArrayType::new(KernelDataType::Struct(Box::new(item_struct)), true),
            ),
            StructField::nullable("name", KernelDataType::STRING),
        ]))
    }

    /// Schema with an array and column mapping metadata.
    pub(crate) fn test_schema_with_array_and_column_mapping() -> SchemaRef {
        let item_struct = StructType::new_unchecked([
            with_column_mapping(
                StructField::nullable("label", KernelDataType::STRING),
                4,
                "phys_label",
            ),
            with_column_mapping(
                StructField::nullable("count", KernelDataType::INTEGER),
                5,
                "phys_count",
            ),
        ]);
        Arc::new(StructType::new_unchecked([
            with_column_mapping(
                StructField::new("id", KernelDataType::LONG, true),
                1,
                "phys_id",
            ),
            with_column_mapping(
                StructField::nullable(
                    "items",
                    ArrayType::new(KernelDataType::Struct(Box::new(item_struct)), true),
                ),
                2,
                "phys_items",
            ),
            with_column_mapping(
                StructField::nullable("name", KernelDataType::STRING),
                3,
                "phys_name",
            ),
        ]))
    }

    /// Deeply nested schema: struct -> array -> struct -> map(value) -> struct.
    ///
    /// The leaf struct field is intentionally **not** annotated with column mapping metadata,
    /// so this schema can be used to test error paths when column mapping is enabled.
    pub(crate) fn test_deep_nested_schema_missing_leaf_cm() -> StructType {
        let leaf_struct =
            StructType::new_unchecked([StructField::new("leaf", KernelDataType::INTEGER, false)]);
        let map_type = MapType::new(
            KernelDataType::STRING,
            KernelDataType::Struct(Box::new(leaf_struct)),
            true,
        );
        let mid_struct = StructType::new_unchecked([with_column_mapping(
            StructField::nullable("mid_field", map_type),
            2,
            "phys_mid_field",
        )]);
        let array_type = ArrayType::new(KernelDataType::Struct(Box::new(mid_struct)), true);
        StructType::new_unchecked([with_column_mapping(
            StructField::nullable("top", array_type),
            1,
            "phys_top",
        )])
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
        let engine: Arc<dyn Engine> = Arc::new(DefaultEngineBuilder::new(store).build());

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

    /// Load a test table from tests/data directory.
    /// Tries compressed (tar.zst) first, falls back to extracted.
    /// Returns (engine, snapshot, optional tempdir). The TempDir must be kept alive
    /// for the duration of the test to prevent premature cleanup of extracted files.
    pub(crate) fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(Arc<dyn Engine>, SnapshotRef, Option<TempDir>)> {
        // Try loading compressed table first, fall back to extracted
        let (path, tempdir) = match load_test_data("tests/data", table_name) {
            Ok(test_dir) => {
                let test_path = test_dir.path().join(table_name);
                (test_path, Some(test_dir))
            }
            Err(_) => {
                // Fall back to already-extracted table
                let manifest_dir = env!("CARGO_MANIFEST_DIR");
                let mut path = PathBuf::from(manifest_dir);
                path.push("tests/data");
                path.push(table_name);
                let path = std::fs::canonicalize(path)
                    .map_err(|e| Error::Generic(format!("Failed to canonicalize path: {e}")))?;
                (path, None)
            }
        };

        // Create engine and snapshot from the resolved path
        let url = Url::from_directory_path(&path)
            .map_err(|_| Error::Generic("Failed to create URL from path".to_string()))?;

        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngineBuilder::new(store).build());
        let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
        Ok((engine, snapshot, tempdir))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_parsing() {
        for x in [
            // windows parsing of file:/// is... odd
            #[cfg(not(windows))]
            "file:///foo/bar",
            #[cfg(not(windows))]
            "file:///foo/bar/",
            "/foo/bar",
            "/foo/bar/",
            "../foo/bar",
            "../foo/bar/",
            "c:/foo/bar",
            "c:/",
            "file:///C:/",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::LocalPath(_)) => {}
                x => panic!("Should have parsed as a local path {x:?}"),
            }
        }

        for x in [
            "s3://foo/bar",
            "s3a://foo/bar",
            "memory://foo/bar",
            "gs://foo/bar",
            "https://foo/bar/",
            "unknown://foo/bar",
            "s2://foo/bar",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::Url(_)) => {}
                x => panic!("Should have parsed as a url {x:?}"),
            }
        }

        #[cfg(not(windows))]
        resolve_uri_type("file://foo/bar").expect_err("file://foo/bar should not have parsed");
    }

    #[test]
    fn try_from_uri_without_trailing_slash() {
        let location = "s3://foo/__unitystorage/catalogs/cid/tables/tid";
        let url = try_parse_uri(location).unwrap();

        assert_eq!(
            url.to_string(),
            "s3://foo/__unitystorage/catalogs/cid/tables/tid/"
        );
    }

    mod on_complete_tests {
        use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
        use std::sync::Arc;

        use super::*;

        #[test]
        fn test_calls_on_exhaustion() {
            let called = Arc::new(AtomicBool::new(false));
            let called_clone = called.clone();
            let mut iter = vec![1, 2].into_iter().on_complete(move || {
                called_clone.store(true, Ordering::SeqCst);
            });
            assert_eq!(iter.next(), Some(1));
            assert!(!called.load(Ordering::SeqCst));
            assert_eq!(iter.next(), Some(2));
            assert_eq!(iter.next(), None);
            assert!(called.load(Ordering::SeqCst));
        }

        #[test]
        fn test_does_not_call_on_early_drop() {
            let called = Arc::new(AtomicBool::new(false));
            let called_clone = called.clone();
            {
                let mut iter = vec![1, 2].into_iter().on_complete(move || {
                    called_clone.store(true, Ordering::SeqCst);
                });
                assert_eq!(iter.next(), Some(1));
                // Drop without exhausting - callback should NOT be called
            }
            assert!(!called.load(Ordering::SeqCst));
        }

        #[test]
        fn test_calls_only_once() {
            let count = Arc::new(AtomicU32::new(0));
            let count_clone = count.clone();
            {
                let mut iter = vec![1].into_iter().on_complete(move || {
                    count_clone.fetch_add(1, Ordering::SeqCst);
                });
                assert_eq!(iter.next(), Some(1));
                assert_eq!(iter.next(), None); // triggers callback
                assert_eq!(iter.next(), None); // should not trigger again
            } // drop should not trigger again
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }
    }
}
