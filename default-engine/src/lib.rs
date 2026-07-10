//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::future::Future;
use std::num::NonZero;
use std::sync::Arc;

use delta_kernel::engine::arrow_conversion::TryFromArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::metrics::{MeteredJsonHandler, MeteredParquetHandler, MeteredStorageHandler};
use delta_kernel::object_store::list::PaginatedListStore;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::schema::Schema;
use delta_kernel::transaction::WriteContext;
use delta_kernel::{
    DeltaResult, Engine, EngineData, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
};
use futures::stream::{BoxStream, StreamExt as _};
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreStorageHandler;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;

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

/// Default file-level readahead depth for JSON and Parquet handlers.
pub(crate) const DEFAULT_READ_BUFFER_SIZE: NonZero<usize> =
    NonZero::new(DEFAULT_BUFFER_SIZE).unwrap();
/// Default row batch size for JSON and Parquet read streams.
pub(crate) const DEFAULT_READ_BATCH_SIZE: NonZero<usize> =
    NonZero::new(DEFAULT_BATCH_SIZE).unwrap();

#[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    storage: Arc<MeteredStorageHandler>,
    json: Arc<MeteredJsonHandler>,
    parquet: Arc<MeteredParquetHandler>,
    /// Concrete parquet handler retained so [`Self::write_parquet`] and
    /// [`Self::default_parquet_handler`] can reach the inherent `write_parquet_file`
    /// helper, which the [`ParquetHandler`] trait surface doesn't expose.
    raw_parquet: Arc<DefaultParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
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
pub struct DefaultEngineBuilder<E> {
    object_store: Arc<DynObjectStore>,
    /// Optional [`PaginatedListStore`] handle enabling single-directory log listing (delimiter
    /// pushdown) on backends that support it. `None` uses the client-side fallback.
    paginated: Option<Arc<dyn PaginatedListStore>>,
    /// The state is either [`DefaultTaskExecutor`] or `Arc<E>` with a custom task executor.
    task_executor: E,
    /// Read-path I/O concurrency config applied to the JSON and Parquet handlers. `None` fields
    /// fall back to the handlers' defaults.
    io_config: ReadIoConfig,
}

impl<E: std::fmt::Debug> std::fmt::Debug for DefaultEngineBuilder<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultEngineBuilder")
            .field("object_store", &self.object_store)
            .field("paginated", &self.paginated.is_some())
            .field("task_executor", &self.task_executor)
            .field("io_config", &self.io_config)
            .finish()
    }
}

/// Read-path I/O tuning for [`DefaultEngine`]'s JSON and Parquet handlers.
///
/// Both knobs default to the handlers' built-in defaults when left unset.
#[derive(Debug, Default, Clone, Copy)]
struct ReadIoConfig {
    /// Maximum number of files read concurrently (file-level readahead depth). See
    /// [`DefaultEngineBuilder::with_buffer_size`].
    buffer_size: Option<NonZero<usize>>,
    /// Maximum number of rows per yielded batch. See [`DefaultEngineBuilder::with_batch_size`].
    batch_size: Option<NonZero<usize>>,
}

/// Represents the default [`TaskExecutor`]. The executor is created lazily to avoid unnecessary
/// instantiations.
pub struct DefaultTaskExecutor;

impl DefaultEngineBuilder<DefaultTaskExecutor> {
    /// Create a new [`DefaultEngineBuilder`] instance with the default executor.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            paginated: None,
            task_executor: DefaultTaskExecutor,
            io_config: ReadIoConfig::default(),
        }
    }

    /// Build the [`DefaultEngine`] instance.
    pub fn build(self) -> DefaultEngine<executor::tokio::TokioBackgroundExecutor> {
        let task_executor = Arc::new(executor::tokio::TokioBackgroundExecutor::new());
        DefaultEngine::new_with_opts(
            self.object_store,
            self.paginated,
            task_executor,
            self.io_config,
        )
    }
}

impl<E> DefaultEngineBuilder<E> {
    /// Set a custom task executor for the engine.
    ///
    /// See [`executor::TaskExecutor`] for more details.
    pub fn with_task_executor<F: TaskExecutor>(
        self,
        task_executor: Arc<F>,
    ) -> DefaultEngineBuilder<Arc<F>> {
        DefaultEngineBuilder {
            object_store: self.object_store,
            paginated: self.paginated,
            task_executor,
            io_config: self.io_config,
        }
    }

    /// Enable single-level log listing (delimiter pushdown) for a store that also
    /// implements [`PaginatedListStore`]. This is typically the same concrete
    /// `Arc<AmazonS3>` / `Arc<GoogleCloudStorage>` / `Arc<MicrosoftAzure>` passed to [`Self::new`],
    /// which coerces to both trait objects independently.
    ///
    /// Without this, log listing falls back to a recursive listing filtered to direct children
    /// client-side, which is correct but pages through subdirectories.
    pub fn with_paginated_list_store(mut self, paginated: Arc<dyn PaginatedListStore>) -> Self {
        self.paginated = Some(paginated);
        self
    }

    /// Set the maximum number of files read concurrently by the JSON and Parquet handlers in their
    /// `read_*_files` paths. This is the file-level I/O readahead depth: higher values overlap more
    /// object-store requests to hide latency, at the cost of more in-flight memory.
    ///
    /// Defaults to the handlers' built-in value when unset. Ordering of returned data is preserved
    /// regardless of this value.
    pub fn with_buffer_size(mut self, buffer_size: NonZero<usize>) -> Self {
        self.io_config.buffer_size = Some(buffer_size);
        self
    }

    /// Set the maximum number of rows per batch yielded by the JSON and Parquet handlers in their
    /// `read_*_files` paths.
    ///
    /// Defaults to the handlers' built-in value when unset. Overall read memory usage is roughly
    /// proportional to `buffer_size * batch_size`.
    pub fn with_batch_size(mut self, batch_size: NonZero<usize>) -> Self {
        self.io_config.batch_size = Some(batch_size);
        self
    }
}

impl<E: TaskExecutor> DefaultEngineBuilder<Arc<E>> {
    /// Build the [`DefaultEngine`] instance.
    pub fn build(self) -> DefaultEngine<E> {
        DefaultEngine::new_with_opts(
            self.object_store,
            self.paginated,
            self.task_executor,
            self.io_config,
        )
    }
}

impl DefaultEngine<executor::tokio::TokioBackgroundExecutor> {
    /// Create a [`DefaultEngineBuilder`] for constructing a [`DefaultEngine`] with custom options.
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store to use.
    pub fn builder(object_store: Arc<DynObjectStore>) -> DefaultEngineBuilder<DefaultTaskExecutor> {
        DefaultEngineBuilder::new(object_store)
    }
}

impl<E: TaskExecutor> DefaultEngine<E> {
    fn new_with_opts(
        object_store: Arc<DynObjectStore>,
        paginated: Option<Arc<dyn PaginatedListStore>>,
        task_executor: Arc<E>,
        io_config: ReadIoConfig,
    ) -> Self {
        let raw_storage: Arc<dyn StorageHandler> = Arc::new(ObjectStoreStorageHandler::new(
            object_store.clone(),
            paginated,
            task_executor.clone(),
        ));

        let buffer_size = io_config.buffer_size.unwrap_or(DEFAULT_READ_BUFFER_SIZE);
        let batch_size = io_config.batch_size.unwrap_or(DEFAULT_READ_BATCH_SIZE);
        let json = DefaultJsonHandler::new(object_store.clone(), task_executor.clone())
            .with_buffer_size(buffer_size)
            .with_batch_size(batch_size);
        let parquet = DefaultParquetHandler::new(object_store.clone(), task_executor.clone())
            .with_buffer_size(buffer_size)
            .with_batch_size(batch_size);
        let raw_json: Arc<dyn JsonHandler> = Arc::new(json);
        let raw_parquet = Arc::new(parquet);
        Self {
            storage: Arc::new(MeteredStorageHandler::new(raw_storage)),
            json: Arc::new(MeteredJsonHandler::new(raw_json)),
            parquet: Arc::new(MeteredParquetHandler::new(raw_parquet.clone())),
            raw_parquet,
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

    /// Returns the concrete [`DefaultParquetHandler`] for callers that need the inherent
    /// async `write_parquet_file` helper not exposed by the [`ParquetHandler`] trait.
    /// For the metered trait surface used by reads, use [`Self::parquet_handler`].
    ///
    /// TODO(#2701): lift the inherent helper onto [`DefaultEngine`] so this accessor
    /// (and the `raw_parquet` field) can be removed.
    pub fn default_parquet_handler(&self) -> Arc<DefaultParquetHandler<E>> {
        self.raw_parquet.clone()
    }

    /// Write `data` as a parquet file using the provided `write_context`.
    ///
    /// `data` must not contain partition columns. If the table materializes partition columns (e.g.
    /// `materializePartitionColumns` or `icebergCompatV3`), this function automatically inserts
    /// them into the data.
    ///
    /// The `write_context` must be created by [`Transaction::partitioned_write_context`] or
    /// [`Transaction::unpartitioned_write_context`], which handle partition value validation,
    /// serialization, and logical-to-physical key translation.
    ///
    /// [`Transaction::partitioned_write_context`]: delta_kernel::transaction::Transaction::partitioned_write_context
    /// [`Transaction::unpartitioned_write_context`]: delta_kernel::transaction::Transaction::unpartitioned_write_context
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
        self.raw_parquet
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
/// [`Transaction::add_files`]: delta_kernel::transaction::Transaction::add_files
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
    use delta_kernel::object_store::local::LocalFileSystem;
    use test_utils::engine_contract::test_arrow_engine;

    use super::*;

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
            .with_buffer_size(NonZero::new(4).unwrap())
            .with_batch_size(NonZero::new(8).unwrap())
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
