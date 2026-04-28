//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::DynObjectStore;
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreStorageHandler;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use super::arrow_conversion::TryFromArrow as _;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowEvaluationHandler;
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
pub mod storage;

#[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    storage: Arc<ObjectStoreStorageHandler<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
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
        let output_schema = write_context.schema();
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
