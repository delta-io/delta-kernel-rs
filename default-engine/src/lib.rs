//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt as _};
use itertools::Itertools as _;
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreStorageHandler;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use delta_kernel::engine::arrow_conversion::TryFromArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::metrics::MetricsReporter;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::schema::Schema;
use delta_kernel::transaction::WriteContext;
use delta_kernel::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandler, JsonHandler, ParquetHandler,
    StorageHandler,
};

pub mod executor;
pub mod file_stream;
pub mod filesystem;
pub mod json;
pub mod parquet;
pub mod stats;
pub mod storage;

/// Converts a Stream-producing future to a synchronous iterator.
///
/// This method performs the initial blocking call to extract the stream from the future, and each
/// subsequent call to `next` on the iterator translates to a blocking `stream.next()` call, using
/// the provided `task_executor`. Buffered streams allow concurrency in the form of prefetching,
/// because that initial call will attempt to populate the N buffer slots; every call to
/// `stream.next()` leaves an empty slot (out of N buffer slots) that the stream immediately
/// attempts to fill by launching another future that can make progress in the background while we
/// block on and consume each of the N-1 entries that precede it.
///
/// This is an internal utility for bridging object_store's async API to
/// Delta Kernel's synchronous handler traits.
pub(crate) fn stream_future_to_iter<T: Send + 'static, E: executor::TaskExecutor>(
    task_executor: Arc<E>,
    stream_future: impl Future<Output = DeltaResult<BoxStream<'static, T>>> + Send + 'static,
) -> DeltaResult<Box<dyn Iterator<Item = T> + Send>> {
    Ok(Box::new(BlockingStreamIterator {
        stream: Some(task_executor.block_on(stream_future)?),
        task_executor,
    }))
}

struct BlockingStreamIterator<T: Send + 'static, E: executor::TaskExecutor> {
    stream: Option<BoxStream<'static, T>>,
    task_executor: Arc<E>,
}

impl<T: Send + 'static, E: executor::TaskExecutor> Iterator for BlockingStreamIterator<T, E> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Move the stream into the future so we can block on it.
        let mut stream = self.stream.take()?;
        let (item, stream) = self
            .task_executor
            .block_on(async move { (stream.next().await, stream) });

        // We must not poll an exhausted stream after it returned None.
        if item.is_some() {
            self.stream = Some(stream);
        }

        item
    }
}

const DEFAULT_BUFFER_SIZE: usize = 1000;
const DEFAULT_BATCH_SIZE: usize = 1000;

#[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    storage: Arc<ObjectStoreStorageHandler<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
}

/// Builder for creating [`DefaultEngine`] instances.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use delta_kernel_default_engine::DefaultEngineBuilder;
/// # use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
/// # use delta_kernel::object_store::local::LocalFileSystem;
/// // Build a DefaultEngine with default executor
/// let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
///     .build();
///
/// // Build with a custom executor
/// let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
///     .with_task_executor(Arc::new(TokioBackgroundExecutor::new()))
///     .build();
/// ```
#[derive(Debug)]
pub struct DefaultEngineBuilder<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
}

impl DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
    /// Create a new [`DefaultEngineBuilder`] instance with the default executor.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            task_executor: Arc::new(executor::tokio::TokioBackgroundExecutor::new()),
            metrics_reporter: None,
        }
    }
}

impl<E: TaskExecutor> DefaultEngineBuilder<E> {
    /// Set the metrics reporter for the engine.
    pub fn with_metrics_reporter(mut self, reporter: Arc<dyn MetricsReporter>) -> Self {
        self.metrics_reporter = Some(reporter);
        self
    }

    /// Set a custom task executor for the engine.
    ///
    /// See [`executor::TaskExecutor`] for more details.
    pub fn with_task_executor<F: TaskExecutor>(
        self,
        task_executor: Arc<F>,
    ) -> DefaultEngineBuilder<F> {
        DefaultEngineBuilder {
            object_store: self.object_store,
            task_executor,
            metrics_reporter: self.metrics_reporter,
        }
    }

    /// Build the [`DefaultEngine`] instance.
    pub fn build(self) -> DefaultEngine<E> {
        DefaultEngine::new_with_opts(self.object_store, self.task_executor, self.metrics_reporter)
    }
}

impl DefaultEngine<executor::tokio::TokioBackgroundExecutor> {
    /// Create a [`DefaultEngineBuilder`] for constructing a [`DefaultEngine`] with custom options.
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store to use.
    pub fn builder(
        object_store: Arc<DynObjectStore>,
    ) -> DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(object_store)
    }
}

impl<E: TaskExecutor> DefaultEngine<E> {
    fn new_with_opts(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        metrics_reporter: Option<Arc<dyn MetricsReporter>>,
    ) -> Self {
        Self {
            storage: Arc::new(ObjectStoreStorageHandler::new(
                object_store.clone(),
                task_executor.clone(),
                None,
            )),
            json: Arc::new(DefaultJsonHandler::new(
                object_store.clone(),
                task_executor.clone(),
            )),
            parquet: Arc::new(DefaultParquetHandler::new(
                object_store.clone(),
                task_executor.clone(),
            )),
            object_store,
            task_executor,
            evaluation: Arc::new(ArrowEvaluationHandler {}),
            metrics_reporter,
        }
    }

    /// Enter the runtime context of the executor associated with this engine.
    ///
    /// # Panics
    ///
    /// When calling `enter` multiple times, the returned guards **must** be dropped in the reverse
    /// order that they were acquired.  Failure to do so will result in a panic and possible memory
    /// leaks.
    pub fn enter(&self) -> <E as TaskExecutor>::Guard<'_> {
        self.task_executor.enter()
    }

    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.object_store.clone())
    }

    /// Write `data` as a parquet file using the provided `write_context`.
    ///
    /// The `partition_values` keys should use **logical** column names. They will be
    /// automatically translated to physical names using the column mapping mode from
    /// `write_context`.
    pub async fn write_parquet(
        &self,
        data: &ArrowEngineData,
        write_context: &WriteContext,
        partition_values: HashMap<String, String>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // Validate partition columns exist in the schema and translate logical names to physical names.
        let physical_partition_values: HashMap<String, String> = partition_values
            .into_iter()
            .map(|(logical_name, value)| -> DeltaResult<(String, String)> {
                let field = write_context
                    .logical_schema()
                    .field(&logical_name)
                    .ok_or_else(|| {
                        Error::generic(format!(
                            "Partition column '{logical_name}' not found in table schema"
                        ))
                    })?;
                let physical_name = field
                    .physical_name(write_context.column_mapping_mode())
                    .to_string();
                Ok((physical_name, value))
            })
            .try_collect()?;

        let transform = write_context.logical_to_physical();
        let input_schema = Schema::try_from_arrow(data.record_batch().schema())?;
        let output_schema = write_context.physical_schema();
        let logical_to_physical_expr = self.evaluation_handler().new_expression_evaluator(
            input_schema.into(),
            transform.clone(),
            output_schema.clone().into(),
        )?;
        let physical_data = logical_to_physical_expr.evaluate(data)?;
        self.parquet
            .write_parquet_file(
                write_context.target_dir(),
                physical_data,
                physical_partition_values,
                Some(write_context.stats_columns()),
            )
            .await
    }
}

impl<E: TaskExecutor> Engine for DefaultEngine<E> {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    fn get_metrics_reporter(&self) -> Option<Arc<dyn MetricsReporter>> {
        self.metrics_reporter.clone()
    }
}

trait UrlExt {
    // Check if a given url is a presigned url and can be used
    // to access the object store via simple http requests
    fn is_presigned(&self) -> bool;
}

impl UrlExt for Url {
    fn is_presigned(&self) -> bool {
        matches!(self.scheme(), "http" | "https")
            && (
                // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
                // https://developers.cloudflare.com/r2/api/s3/presigned-urls/
                self
                .query_pairs()
                .any(|(k, _)| k.eq_ignore_ascii_case("X-Amz-Signature")) ||
                // https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#version-2020-12-06-and-later
                // note signed permission (sp) must always be present
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("sp")) ||
                // https://cloud.google.com/storage/docs/authentication/signatures
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("X-Goog-Credential")) ||
                // https://www.alibabacloud.com/help/en/oss/user-guide/upload-files-using-presigned-urls
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("X-OSS-Credential"))
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::arrow::array::StringArray;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::engine_data::FilteredEngineData;
    use delta_kernel::metrics::MetricEvent;
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::object_store::path::Path;
    use itertools::Itertools;
    use test_utils::delta_path_for_version;

    fn test_arrow_engine(engine: &dyn delta_kernel::Engine, base_url: &Url) {
        let get_data = || -> Box<dyn delta_kernel::EngineData> {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "dog",
                ArrowDataType::Utf8,
                true,
            )]));
            let data = delta_kernel::arrow::array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["remi", "wilson"]))],
            )
            .unwrap();
            Box::new(ArrowEngineData::new(data))
        };

        let json = engine.json_handler();
        let get_filtered = || {
            let data = get_data();
            let filtered_data = FilteredEngineData::with_all_rows_selected(data);
            Box::new(std::iter::once(Ok(filtered_data)))
        };

        let expected_names: Vec<Path> = (1..4)
            .map(|i| delta_path_for_version(i, "json"))
            .collect_vec();

        for i in expected_names.iter().rev() {
            let path = base_url.join(i.as_ref()).unwrap();
            json.write_json_file(&path, get_filtered(), false).unwrap();
        }
        let path = base_url.join("other").unwrap();
        json.write_json_file(&path, get_filtered(), false).unwrap();

        let storage = engine.storage_handler();

        let test_url = base_url.join(expected_names[0].as_ref()).unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len() - 1);
        for (file, expected) in files.iter().zip(expected_names.iter().skip(1)) {
            assert_eq!(file.location, base_url.join(expected.as_ref()).unwrap());
        }

        let test_url = base_url
            .join(delta_path_for_version(0, "json").as_ref())
            .unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len());

        let test_url = base_url.join("_delta_log/").unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len());
        for (file, expected) in files.iter().zip(expected_names.iter()) {
            assert_eq!(file.location, base_url.join(expected.as_ref()).unwrap());
        }
    }

    #[derive(Debug)]
    struct TestMetricsReporter;

    impl MetricsReporter for TestMetricsReporter {
        fn report(&self, _event: MetricEvent) {}
    }

    #[test]
    fn test_default_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(object_store).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_new_and_build() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(object_store).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_with_metrics_reporter() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let reporter = Arc::new(TestMetricsReporter);
        let engine = DefaultEngineBuilder::new(object_store)
            .with_metrics_reporter(reporter)
            .build();
        assert!(engine.get_metrics_reporter().is_some());
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_with_custom_executor() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(executor::tokio::TokioMultiThreadExecutor::new(
            rt.handle().clone(),
        ));
        let engine = DefaultEngineBuilder::new(object_store)
            .with_task_executor(executor)
            .build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_method() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::builder(object_store).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_all_options() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let reporter = Arc::new(TestMetricsReporter);
        let executor = Arc::new(executor::tokio::TokioBackgroundExecutor::new());
        let engine = DefaultEngineBuilder::new(object_store)
            .with_metrics_reporter(reporter)
            .with_task_executor(executor)
            .build();
        assert!(engine.get_metrics_reporter().is_some());
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_pre_signed_url() {
        let url = Url::parse("https://example.com?X-Amz-Signature=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?sp=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?X-Goog-Credential=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?X-OSS-Credential=foo").unwrap();
        assert!(url.is_presigned());

        // assert that query keys are case insensitive
        let url = Url::parse("https://example.com?x-gooG-credenTIAL=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com?x-oss-CREDENTIAL=foo").unwrap();
        assert!(url.is_presigned());

        let url = Url::parse("https://example.com").unwrap();
        assert!(!url.is_presigned());
    }
}
