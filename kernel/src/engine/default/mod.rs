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

/// Wraps a [`crate::FileDataReadResultIterator`] to emit a metrics event exactly once when the
/// iterator is either exhausted or dropped.
///
/// Used by the JSON and Parquet handlers to report the number of files and bytes requested per
/// `read_*_files` call. The `emit_fn` is called with `(num_files, bytes_read)` and is expected
/// to create and immediately drop a tracing span, which triggers the `ReportGeneratorLayer` to
/// fire the appropriate [`crate::metrics::MetricEvent`] to any registered reporter.
pub(super) struct ReadMetricsIterator {
    inner: crate::FileDataReadResultIterator,
    num_files: u64,
    bytes_read: u64,
    emitted: bool,
    emit_fn: fn(u64, u64),
}

impl ReadMetricsIterator {
    pub(super) fn new(
        inner: crate::FileDataReadResultIterator,
        num_files: u64,
        bytes_read: u64,
        emit_fn: fn(u64, u64),
    ) -> Self {
        Self {
            inner,
            num_files,
            bytes_read,
            emitted: false,
            emit_fn,
        }
    }

    fn emit_once(&mut self) {
        if !self.emitted {
            self.emitted = true;
            (self.emit_fn)(self.num_files, self.bytes_read);
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
}

/// Builder for creating [`DefaultEngine`] instances.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use delta_kernel::engine::default::DefaultEngineBuilder;
/// # use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
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
}

impl DefaultEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
    /// Create a new [`DefaultEngineBuilder`] instance with the default executor.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            task_executor: Arc::new(executor::tokio::TokioBackgroundExecutor::new()),
        }
    }
}

impl<E: TaskExecutor> DefaultEngineBuilder<E> {
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
        }
    }

    /// Build the [`DefaultEngine`] instance.
    pub fn build(self) -> DefaultEngine<E> {
        DefaultEngine::new_with_opts(self.object_store, self.task_executor)
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
    fn new_with_opts(object_store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            storage: Arc::new(ObjectStoreStorageHandler::new(
                object_store.clone(),
                task_executor.clone(),
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
        // Random 2-char prefix for CM tables, Hive-style for partitioned, else table root.
        let write_dir = write_context.write_dir();
        self.parquet
            .write_parquet_file(
                &write_dir,
                physical_data,
                write_context.physical_partition_values(),
                Some(write_context.stats_columns()),
            )
            .await
    }
}

/// Converts [`DataFileMetadata`] into Add action [`EngineData`] using the partition values
/// from the provided [`WriteContext`].
///
/// This is the public API for building Add action metadata from file write results. Custom
/// Arrow-based engines that write parquet files themselves (bypassing [`DefaultEngine::write_parquet`])
/// should call this to produce the Add action metadata for [`Transaction::add_files`].
///
/// [`DataFileMetadata`]: parquet::DataFileMetadata
/// [`Transaction::add_files`]: crate::transaction::Transaction::add_files
pub fn build_add_file_metadata(
    file_metadata: parquet::DataFileMetadata,
    write_context: &WriteContext,
) -> DeltaResult<Box<dyn EngineData>> {
    file_metadata.as_record_batch(write_context.physical_partition_values())
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
}

trait UrlExt {
    // Check if a given url is a presigned url and can be used
    // to access the object store via simple http requests
    fn is_presigned(&self) -> bool;
}

impl UrlExt for Url {
    fn is_presigned(&self) -> bool {
        // We search a URL query string for these keys to see if we should consider it a presigned URL:
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
    use crate::object_store::local::LocalFileSystem;

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
        let executor = Arc::new(executor::tokio::TokioBackgroundExecutor::new());
        let engine = DefaultEngineBuilder::new(object_store)
            .with_task_executor(executor)
            .build();
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
