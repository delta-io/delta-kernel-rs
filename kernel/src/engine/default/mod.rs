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
use object_store::DynObjectStore;
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreStorageHandler;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use super::arrow_conversion::TryFromArrow as _;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowEvaluationHandler;
use crate::metrics::MetricsReporter;
use crate::schema::Schema;
use crate::transaction::WriteContext;
use crate::{
    DeltaResult, Engine, EngineData, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
};
use delta_kernel_derive::internal_api;

pub mod executor;
pub mod file_stream;
pub mod filesystem;
pub mod json;
pub mod parquet;
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

#[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    storage: Arc<ObjectStoreStorageHandler<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
}

impl DefaultEngine<executor::tokio::TokioBackgroundExecutor> {
    /// Create a new [`DefaultEngine`] instance with the default executor.
    ///
    /// Uses `TokioBackgroundExecutor` as the default executor.
    /// For custom executors, use [`DefaultEngine::new_with_executor`].
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store to use.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self::new_with_executor(
            object_store,
            Arc::new(executor::tokio::TokioBackgroundExecutor::new()),
        )
    }

    /// Set a metrics reporter for the engine to collect events and metrics during operations.
    ///
    /// # Parameters
    ///
    /// - `reporter`: An implementation of the [`MetricsReporter`] trait which will be used to
    /// report metrics.
    #[allow(dead_code)]
    #[internal_api]
    pub(crate) fn set_metrics_reporter(&mut self, reporter: Arc<dyn MetricsReporter>) {
        self.metrics_reporter = Some(reporter);
    }
}

impl<E: TaskExecutor> DefaultEngine<E> {
    /// Create a new [`DefaultEngine`] instance with a custom executor.
    ///
    /// Most users should use [`DefaultEngine::new`] instead. This method is only
    /// needed for specialized testing scenarios (e.g., multi-threaded executors).
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store to use.
    /// - `task_executor`: Used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn new_with_executor(object_store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
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
                task_executor,
            )),
            object_store,
            evaluation: Arc::new(ArrowEvaluationHandler {}),
            metrics_reporter: None,
        }
    }

    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.object_store.clone())
    }

    pub async fn write_parquet(
        &self,
        data: &ArrowEngineData,
        write_context: &WriteContext,
        partition_values: HashMap<String, String>,
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
            .write_parquet_file(write_context.target_dir(), physical_data, partition_values)
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
    use crate::engine::tests::test_arrow_engine;
    use object_store::local::LocalFileSystem;

    #[test]
    fn test_default_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::new(object_store);
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
