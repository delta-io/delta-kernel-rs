//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::future::Future;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt as _};
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreStorageHandler;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use super::arrow_conversion::TryFromArrow as _;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowEvaluationHandler;
use crate::metrics::MetricsReporter;
use crate::object_store::path::Path;
use crate::object_store::DynObjectStore;
use crate::schema::Schema;
use crate::transaction::WriteContext;
use crate::{
    DeltaResult, Engine, EngineData, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
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

/// Wraps a [`FileDataReadResultIterator`] to emit a [`MetricEvent`] exactly once when the iterator
/// is either exhausted or dropped. Used by JSON and Parquet handlers to report the number of files
/// and bytes requested per `read_*_files` call.
pub(super) struct ReadMetricsIterator {
    inner: crate::FileDataReadResultIterator,
    reporter: Arc<dyn crate::metrics::MetricsReporter>,
    num_files: u64,
    bytes_read: u64,
    emitted: bool,
    make_event: fn(u64, u64) -> crate::metrics::MetricEvent,
}

impl ReadMetricsIterator {
    pub(super) fn new(
        inner: crate::FileDataReadResultIterator,
        reporter: Arc<dyn crate::metrics::MetricsReporter>,
        num_files: u64,
        bytes_read: u64,
        make_event: fn(u64, u64) -> crate::metrics::MetricEvent,
    ) -> Self {
        Self {
            inner,
            reporter,
            num_files,
            bytes_read,
            emitted: false,
            make_event,
        }
    }

    fn emit_once(&mut self) {
        if !self.emitted {
            self.emitted = true;
            self.reporter
                .report((self.make_event)(self.num_files, self.bytes_read));
        }
    }
}

impl Iterator for ReadMetricsIterator {
    type Item = crate::DeltaResult<Box<dyn crate::EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if item.is_none() {
            self.emit_once();
        }
        item
    }
}

impl Drop for ReadMetricsIterator {
    fn drop(&mut self) {
        self.emit_once();
    }
}

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
/// The canonical entry point is [`DefaultEngineBuilder::from_url`], which handles both
/// object store construction and URL path prefix derivation. Use [`DefaultEngineBuilder::new`]
/// directly only when you need to plug in a pre-built object store.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use url::Url;
/// # use delta_kernel::engine::default::DefaultEngineBuilder;
/// # use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
/// # use delta_kernel::DeltaResult;
/// # fn example() -> DeltaResult<()> {
/// let url = Url::parse("file:///path/to/table")?;
///
/// // Build a DefaultEngine from a URL (resolves store + prefix in one step).
/// let engine = DefaultEngineBuilder::from_url(&url)?.build();
///
/// // Or with a custom executor:
/// let engine = DefaultEngineBuilder::from_url(&url)?
///     .with_task_executor(Arc::new(TokioBackgroundExecutor::new()))
///     .build();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DefaultEngineBuilder<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
    url_path_prefix: Path,
}

impl DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
    /// Create a new [`DefaultEngineBuilder`] instance with the default executor.
    ///
    /// `url_path_prefix` is the prefix returned by [`storage::store_from_url`] /
    /// [`storage::store_from_url_opts`] and is required so that all handlers convert URLs
    /// to store-relative paths correctly. For schemes that encode the bucket/container in
    /// the URL authority (S3, ABFSS, local filesystem) pass `Path::from("")`. For Azure
    /// HTTPS URLs, pass the container name returned by the storage helpers.
    ///
    /// Prefer [`DefaultEngineBuilder::from_url`] when you don't already have a pre-built
    /// object store -- it computes the store and prefix in one step.
    pub fn new(object_store: Arc<DynObjectStore>, url_path_prefix: Path) -> Self {
        Self {
            object_store,
            task_executor: Arc::new(executor::tokio::TokioBackgroundExecutor::new()),
            metrics_reporter: None,
            url_path_prefix,
        }
    }

    /// Create a new [`DefaultEngineBuilder`] from a URL, resolving both the object store
    /// and the URL path prefix via [`storage::store_from_url`].
    ///
    /// This is the canonical entry point for most callers. It eliminates the possibility of
    /// forgetting to set the URL path prefix, which would silently break Azure HTTPS URLs.
    pub fn from_url(url: &Url) -> DeltaResult<Self> {
        let (store, prefix) = storage::store_from_url(url)?;
        Ok(Self::new(store, prefix))
    }

    /// Create a new [`DefaultEngineBuilder`] from a URL with custom options, via
    /// [`storage::store_from_url_opts`].
    pub fn from_url_opts<I, K, V>(url: &Url, options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, prefix) = storage::store_from_url_opts(url, options)?;
        Ok(Self::new(store, prefix))
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
            url_path_prefix: self.url_path_prefix,
        }
    }

    /// Build the [`DefaultEngine`] instance.
    pub fn build(self) -> DefaultEngine<E> {
        DefaultEngine::new_with_opts(
            self.object_store,
            self.task_executor,
            self.metrics_reporter,
            self.url_path_prefix,
        )
    }
}

impl DefaultEngine<executor::tokio::TokioBackgroundExecutor> {
    /// Create a [`DefaultEngineBuilder`] for constructing a [`DefaultEngine`] with custom options.
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store to use.
    /// - `url_path_prefix`: The URL path prefix returned by [`storage::store_from_url`] /
    ///   [`storage::store_from_url_opts`].
    pub fn builder(
        object_store: Arc<DynObjectStore>,
        url_path_prefix: Path,
    ) -> DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(object_store, url_path_prefix)
    }
}

impl<E: TaskExecutor> DefaultEngine<E> {
    fn new_with_opts(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        metrics_reporter: Option<Arc<dyn MetricsReporter>>,
        url_path_prefix: Path,
    ) -> Self {
        Self {
            storage: Arc::new(ObjectStoreStorageHandler::new(
                object_store.clone(),
                task_executor.clone(),
                metrics_reporter.clone(),
                url_path_prefix.clone(),
            )),
            json: Arc::new(
                DefaultJsonHandler::new(
                    object_store.clone(),
                    task_executor.clone(),
                    url_path_prefix.clone(),
                )
                .with_reporter(metrics_reporter.clone()),
            ),
            parquet: Arc::new(
                DefaultParquetHandler::new(
                    object_store.clone(),
                    task_executor.clone(),
                    url_path_prefix,
                )
                .with_reporter(metrics_reporter.clone()),
            ),
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
    /// The `write_context` must be created by [`Transaction::partitioned_write_context`] or
    /// [`Transaction::unpartitioned_write_context`], which handle partition value validation,
    /// serialization, and logical-to-physical key translation.
    ///
    /// [`Transaction::partitioned_write_context`]: crate::transaction::Transaction::partitioned_write_context
    /// [`Transaction::unpartitioned_write_context`]: crate::transaction::Transaction::unpartitioned_write_context
    pub async fn write_parquet(
        &self,
        data: &ArrowEngineData,
        write_context: &WriteContext,
    ) -> DeltaResult<Box<dyn EngineData>> {
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
            .write_parquet_file(physical_data, write_context)
            .await
    }
}

/// Converts [`DataFileMetadata`] into Add action [`EngineData`] using the partition values and
/// table root from the provided [`WriteContext`].
///
/// Paths in the returned Add action metadata are stored relative to the table root.
///
/// This is the public API for building Add action metadata from file write results. Custom
/// Arrow-based engines that write parquet files themselves (bypassing
/// [`DefaultEngine::write_parquet`]) should call this to produce the Add action metadata for
/// [`Transaction::add_files`].
///
/// [`DataFileMetadata`]: parquet::DataFileMetadata
/// [`Transaction::add_files`]: crate::transaction::Transaction::add_files
pub fn build_add_file_metadata(
    file_metadata: parquet::DataFileMetadata,
    write_context: &WriteContext,
) -> DeltaResult<Box<dyn EngineData>> {
    let add_path = write_context.resolve_file_path(file_metadata.location())?;
    file_metadata.as_record_batch(write_context.physical_partition_values(), &add_path)
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
        // We search a URL query string for these keys to see if we should consider it a presigned
        // URL:
        // - AWS: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        // - Cloudflare R2: https://developers.cloudflare.com/r2/api/s3/presigned-urls/
        // - Azure Blob (SAS): https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#version-2020-12-06-and-later
        // - Google Cloud Storage: https://cloud.google.com/storage/docs/authentication/signatures
        // - Alibaba Cloud OSS: https://www.alibabacloud.com/help/en/oss/user-guide/upload-files-using-presigned-urls
        // - Databricks presigned URLs
        const PRESIGNED_KEYS: &[&str] = &[
            "X-Amz-Signature",
            "sp",
            "X-Goog-Credential",
            "X-OSS-Credential",
            "X-Databricks-Signature",
        ];
        matches!(self.scheme(), "http" | "https")
            && self
                .query_pairs()
                .any(|(k, _)| PRESIGNED_KEYS.iter().any(|p| k.eq_ignore_ascii_case(p)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::test_arrow_engine;
    use crate::metrics::MetricEvent;
    use crate::object_store::local::LocalFileSystem;

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
        let engine = DefaultEngineBuilder::new(object_store, Path::from("")).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_new_and_build() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(object_store, Path::from("")).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_with_metrics_reporter() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let reporter = Arc::new(TestMetricsReporter);
        let engine = DefaultEngineBuilder::new(object_store, Path::from(""))
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
        let engine = DefaultEngineBuilder::new(object_store, Path::from(""))
            .with_task_executor(executor)
            .build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_method() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::builder(object_store, Path::from("")).build();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_all_options() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let reporter = Arc::new(TestMetricsReporter);
        let executor = Arc::new(executor::tokio::TokioBackgroundExecutor::new());
        let engine = DefaultEngineBuilder::new(object_store, Path::from(""))
            .with_metrics_reporter(reporter)
            .with_task_executor(executor)
            .build();
        assert!(engine.get_metrics_reporter().is_some());
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_default_engine_builder_from_url() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let engine = DefaultEngineBuilder::from_url(&url).unwrap().build();
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

        let url =
            Url::parse("https://example.com?X-Databricks-TTL=3599545&X-Databricks-Signature=bar")
                .unwrap();
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
