//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::collections::HashMap;
use std::sync::Arc;

use self::storage::parse_url_opts;
use object_store::{path::Path, DynObjectStore};
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreFileSystemClient;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowExpressionHandler;
use crate::schema::Schema;
use crate::transaction::WriteContext;
use crate::{
    DeltaResult, Engine, EngineData, ExpressionHandler, FileSystemClient, JsonHandler,
    ParquetHandler,
};

pub mod executor;
pub mod file_stream;
pub mod filesystem;
pub mod json;
pub mod parquet;
pub mod storage;

#[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
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
        let (store, table_root) = parse_url_opts(table_root, options)?;
        Ok(Self::new(Arc::new(store), table_root, task_executor))
    }

    /// Create a new [`DefaultEngine`] instance
    ///
    /// # Parameters
    ///
    /// - `store`: The object store to use.
    /// - `table_root_path`: The root path of the table within storage.
    /// - `task_executor`: Used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn new(store: Arc<DynObjectStore>, table_root: Path, task_executor: Arc<E>) -> Self {
        // HACK to check if we're using a LocalFileSystem from ObjectStore. We need this because
        // local filesystem doesn't return a sorted list by default. Although the `object_store`
        // crate explicitly says it _does not_ return a sorted listing, in practice all the cloud
        // implementations actually do:
        // - AWS:
        //   [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
        //   states: "For general purpose buckets, ListObjectsV2 returns objects in lexicographical
        //   order based on their key names." (Directory buckets are out of scope for now)
        // - Azure: Docs state
        //   [here](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources):
        //   "A listing operation returns an XML response that contains all or part of the requested
        //   list. The operation returns entities in alphabetical order."
        // - GCP: The [main](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) doc
        //   doesn't indicate order, but [this
        //   page](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) does say: "This page
        //   shows you how to list the [objects](https://cloud.google.com/storage/docs/objects) stored
        //   in your Cloud Storage buckets, which are ordered in the list lexicographically by name."
        // So we just need to know if we're local and then if so, we sort the returned file list in
        // `filesystem.rs`
        let store_str = format!("{}", store);
        let is_local = store_str.starts_with("LocalFileSystem");
        Self {
            file_system: Arc::new(ObjectStoreFileSystemClient::new(
                store.clone(),
                !is_local,
                table_root,
                task_executor.clone(),
            )),
            json: Arc::new(DefaultJsonHandler::new(
                store.clone(),
                task_executor.clone(),
            )),
            parquet: Arc::new(DefaultParquetHandler::new(store.clone(), task_executor)),
            store,
            expression: Arc::new(ArrowExpressionHandler {}),
        }
    }

    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.store.clone())
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
