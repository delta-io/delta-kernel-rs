//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::collections::HashMap;
use std::sync::Arc;

use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreFileSystemClient;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use self::storage::parse_url_opts;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowExpressionHandler;
use crate::schema::Schema;
use crate::transaction::WriteContext;
use crate::{
    DeltaResult, Engine, EngineData, ExpressionHandler, FileSystemClient, JsonHandler,
    ParquetHandler,
};

pub use registry::{DefaultObjectStoreRegistry, ObjectStoreRegistry};

pub mod executor;
pub mod file_stream;
pub mod filesystem;
pub mod json;
pub mod parquet;
pub mod registry;
pub mod storage;

#[derive(Debug, Clone)]
pub struct DefaultEngine<E: TaskExecutor> {
    file_system: Arc<ObjectStoreFileSystemClient<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    expression: Arc<ArrowExpressionHandler>,
}

impl<E: TaskExecutor> DefaultEngine<E> {
    /// Create a new [`DefaultEngine`] instance
    ///
    /// # Parameters
    ///
    /// - `table_root`: The URL of the table within storage.
    /// - `options`: key/value pairs of options to pass to the object store.
    /// - `task_executor`: Used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn try_new<K, V>(
        table_root: &Url,
        options: impl IntoIterator<Item = (K, V)>,
        task_executor: Arc<E>,
    ) -> DeltaResult<Self>
    where
        K: AsRef<str>,
        V: Into<String>,
    {
        // table root is the path of the table in the ObjectStore
        let (store, _table_root) = parse_url_opts(table_root, options)?;
        let mut registry = DefaultObjectStoreRegistry::new();
        registry.register_store(table_root, Arc::new(store));
        Ok(Self::new(Arc::new(registry), task_executor))
    }

    /// Create a new [`DefaultEngine`] instance
    ///
    /// # Parameters
    ///
    /// - `registry`: An object store registry. See [`ObjectStoreRegistry`].
    /// - `task_executor`: Used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn new(registry: Arc<dyn ObjectStoreRegistry>, task_executor: Arc<E>) -> Self {
        Self {
            file_system: Arc::new(ObjectStoreFileSystemClient::new(
                registry.clone(),
                task_executor.clone(),
            )),
            json: Arc::new(DefaultJsonHandler::new(
                registry.clone(),
                task_executor.clone(),
            )),
            parquet: Arc::new(DefaultParquetHandler::new(registry.clone(), task_executor)),
            expression: Arc::new(ArrowExpressionHandler {}),
        }
    }

    pub async fn write_parquet(
        &self,
        data: &ArrowEngineData,
        write_context: &WriteContext,
        partition_values: HashMap<String, String>,
        data_change: bool,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let transform = write_context.logical_to_physical();
        let input_schema: Schema = data.record_batch().schema().try_into()?;
        let output_schema = write_context.schema();
        let logical_to_physical_expr = self.get_expression_handler().get_evaluator(
            input_schema.into(),
            transform.clone(),
            output_schema.clone().into(),
        );
        let physical_data = logical_to_physical_expr.evaluate(data)?;
        self.parquet
            .write_parquet_file(
                write_context.target_dir(),
                physical_data,
                partition_values,
                data_change,
            )
            .await
    }
}

impl<E: TaskExecutor> Engine for DefaultEngine<E> {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        self.expression.clone()
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.file_system.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
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
    use super::executor::tokio::TokioBackgroundExecutor;
    use super::*;
    use crate::engine::tests::test_arrow_engine;

    #[test]
    fn test_default_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let exec = Arc::new(TokioBackgroundExecutor::new());
        let registry = Arc::new(DefaultObjectStoreRegistry::new_test());
        let engine = DefaultEngine::new(registry.clone(), exec.clone());
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
