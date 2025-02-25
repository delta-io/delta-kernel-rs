macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}

pub(crate) use require;

#[cfg(test)]
pub(crate) mod test_utils {
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde::Serialize;
    use std::sync::Mutex;
    use std::{collections::VecDeque, path::Path, sync::Arc};
    use tempfile::TempDir;
    use test_utils::delta_path_for_version;

    use crate::actions::{Add, Cdc, CommitInfo, Metadata, Protocol, Remove};
    use crate::{
        schema::SchemaRef, DeltaResult, ExpressionRef, FileDataReadResultIterator, FileMeta,
        ParquetHandler,
    };
    use crate::{Engine, ExpressionHandler, FileSystemClient, JsonHandler};
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

    /// A helper struct that bundles together all the components needed for mocking the engine in a test.
    /// It provides easy access to the mock engine and its handlers in a single convenient package.
    pub(crate) struct MockEngineContext {
        pub engine: MockEngine,
        pub json_handler: Arc<MockJsonHandler>,
        pub parquet_handler: Arc<MockParquetHandler>,
    }

    impl MockEngineContext {
        /// Creates a new MockEngineContext with all its components initialized.
        /// This provides a convenient way to set up all necessary mocks for testing.
        pub(crate) fn new() -> Self {
            let engine = MockEngine::new();

            // Extract and downcast the JSON handler from the engine
            // This gives us direct access to the mock handler for setting expectations
            let json_handler = engine
                .get_json_handler()
                .as_any()
                .downcast::<MockJsonHandler>()
                .expect("Expected MockJsonHandler");

            // Extract and downcast the Parquet handler from the engine
            // This gives us direct access to the mock handler for setting expectations
            let parquet_handler = engine
                .get_parquet_handler()
                .as_any()
                .downcast::<MockParquetHandler>()
                .expect("Expected MockParquetHandler");

            Self {
                engine,
                json_handler,
                parquet_handler,
            }
        }
    }

    /// A mock implementation of the `Engine` trait for unit testing purposes.
    ///
    /// Provides configurable handlers for testing file operations with controlled responses.
    pub(crate) struct MockEngine {
        json_handler: Arc<dyn JsonHandler>,
        parquet_handler: Arc<dyn ParquetHandler>,
    }

    impl MockEngine {
        /// Creates a new `MockEngine` instance with default mock handlers.
        pub(crate) fn new() -> Self {
            Self {
                json_handler: Arc::new(MockJsonHandler::new()),
                parquet_handler: Arc::new(MockParquetHandler::new()),
            }
        }
    }

    impl Engine for MockEngine {
        fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
            // TODO: Create a mock expression handler
            unimplemented!()
        }

        fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
            // TODO: Create a mock file system cliient
            unimplemented!()
        }

        fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            Arc::clone(&self.parquet_handler)
        }

        fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
            Arc::clone(&self.json_handler)
        }
    }

    /// Represents the expected parameters and result for a file read operation.
    /// Used by mock handlers to verify that file operations are called with expected parameters.
    struct ExpectedFileReadParams {
        /// List of files that should be read in this operation
        files: Vec<FileMeta>,

        /// Expected schema that should be used for reading the files
        schema: SchemaRef,

        /// Expected predicate filter that should be applied during reading
        predicate: Option<ExpressionRef>,

        /// Pre-configured result that should be returned when parameters match
        result: DeltaResult<FileDataReadResultIterator>,
    }

    /// Verifies that actual file read parameters match the expected parameters.
    /// Asserts if any discrepancy is found between expected and actual values.
    fn assert_parameters_match(
        expected_params: &ExpectedFileReadParams,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<ExpressionRef>,
        file_type: &str,
    ) {
        assert_eq!(
            expected_params.files, files,
            "Mismatch in {} file read: expected {:?}, got {:?}.",
            file_type, expected_params.files, files
        );

        assert_eq!(
            expected_params.schema, physical_schema,
            "Mismatch in {} file schema: expected {:?}, got {:?}.",
            file_type, expected_params.schema, physical_schema
        );

        assert_eq!(
            expected_params.predicate.is_some(),
            predicate.is_some(),
            "Mismatch in {} file predicate presence.",
            file_type
        );

        if let (Some(expected_predicate), Some(actual_predicate)) =
            (&expected_params.predicate, &predicate)
        {
            assert_eq!(
                expected_predicate.as_ref(),
                actual_predicate.as_ref(),
                "Mismatch in {} file predicate expressions.",
                file_type
            );
        }
    }

    /// A generic mock handler for testing file read operations.
    /// Maintains a queue of expected read operations and their results.
    struct MockHandler {
        /// Queue of expected file read operations
        expected_file_reads_params: Mutex<VecDeque<ExpectedFileReadParams>>,
    }

    impl MockHandler {
        /// Creates a new `MockHandler` with an empty queue.
        fn new() -> Self {
            Self {
                expected_file_reads_params: Mutex::new(VecDeque::new()),
            }
        }

        /// Registers an expected file read operation with specified parameters and result.
        /// These expectations will be checked in order when read operations occur.
        fn expect_read_files(
            &self,
            files: Vec<FileMeta>,
            schema: SchemaRef,
            predicate: Option<ExpressionRef>,
            result: DeltaResult<FileDataReadResultIterator>,
        ) {
            self.expected_file_reads_params
                .lock()
                .unwrap()
                .push_back(ExpectedFileReadParams {
                    files,
                    schema,
                    predicate,
                    result,
                });
        }

        /// Processes a file read operation by comparing against the next expected operation.
        /// Returns the pre-configured result if parameters match expectations.
        /// Panics if no expectations are queued or if parameters don't match.
        fn read_files(
            &self,
            files: &[FileMeta],
            schema: SchemaRef,
            predicate: Option<ExpressionRef>,
            file_type: &str,
        ) -> DeltaResult<FileDataReadResultIterator> {
            let mut queue = self.expected_file_reads_params.lock().unwrap();
            if let Some(expected_call) = queue.pop_front() {
                assert_parameters_match(&expected_call, files, schema, predicate, file_type);
                expected_call.result
            } else {
                panic!(
                    "Unexpected call to read_{}_files! No expected read call found.",
                    file_type
                );
            }
        }
    }

    /// A specialized mock handler for testing Parquet file operations.
    /// Wraps the generic MockHandler with Parquet-specific functionality.
    pub(crate) struct MockParquetHandler {
        handler: MockHandler,
    }

    impl MockParquetHandler {
        /// Creates a new `MockParquetHandler` with an empty expectation queue.
        pub(crate) fn new() -> Self {
            Self {
                handler: MockHandler::new(),
            }
        }

        /// Registers an expected Parquet file read operation.
        /// The operation will be expected to be called with exactly these parameters.
        pub(crate) fn expect_read_parquet_files(
            &self,
            files: Vec<FileMeta>,
            schema: SchemaRef,
            predicate: Option<ExpressionRef>,
            result: DeltaResult<FileDataReadResultIterator>,
        ) {
            self.handler
                .expect_read_files(files, schema, predicate, result);
        }
    }

    impl ParquetHandler for MockParquetHandler {
        /// Implements the Parquet file read operation for testing.
        /// Verifies the operation matches the next expected call and returns its result.
        fn read_parquet_files(
            &self,
            files: &[FileMeta],
            schema: SchemaRef,
            predicate: Option<ExpressionRef>,
        ) -> DeltaResult<FileDataReadResultIterator> {
            self.handler.read_files(files, schema, predicate, "parquet")
        }
    }

    /// Wraps the generic MockHandler with JSON-specific functionality.
    pub(crate) struct MockJsonHandler {
        handler: MockHandler,
    }

    impl MockJsonHandler {
        /// Creates a new `MockJsonHandler` with an empty expectation queue.
        pub(crate) fn new() -> Self {
            Self {
                handler: MockHandler::new(),
            }
        }

        /// Registers an expected JSON file read operation.
        /// The operation will be expected to be called with exactly these parameters.
        pub(crate) fn expect_read_json_files(
            &self,
            files: Vec<FileMeta>,
            schema: SchemaRef,
            predicate: Option<ExpressionRef>,
            result: DeltaResult<FileDataReadResultIterator>,
        ) {
            self.handler
                .expect_read_files(files, schema, predicate, result);
        }
    }

    impl JsonHandler for MockJsonHandler {
        /// Implements the JSON file read operation for testing.
        /// Verifies the operation matches the next expected call and returns its result.
        fn read_json_files(
            &self,
            files: &[FileMeta],
            schema: SchemaRef,
            predicate: Option<ExpressionRef>,
        ) -> DeltaResult<FileDataReadResultIterator> {
            self.handler.read_files(files, schema, predicate, "json")
        }

        /// Placeholder for JSON parsing operation.
        /// Not implemented in the mock as it's not needed for current tests.
        fn parse_json(
            &self,
            _json_strings: Box<dyn crate::EngineData>,
            _output_schema: SchemaRef,
        ) -> DeltaResult<Box<dyn crate::EngineData>> {
            unimplemented!()
        }

        /// Placeholder for JSON file writing operation.
        /// Not implemented in the mock as it's not needed for current tests.
        fn write_json_file(
            &self,
            _path: &url::Url,
            _data: Box<dyn Iterator<Item = DeltaResult<Box<dyn crate::EngineData>>> + Send + '_>,
            _overwrite: bool,
        ) -> DeltaResult<()> {
            unimplemented!()
        }
    }
}
