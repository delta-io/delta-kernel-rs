//! # Default TableClient
//!
//! The default implementation of [`TableClient`] is [`DefaultTableClient`].
//! This uses the [object_store], [parquet][::parquet], and [arrow_json] crates
//! to read and write data.
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::sync::Arc;

use object_store::{parse_url_opts, path::Path, DynObjectStore};
use url::Url;

use self::executor::TaskExecutor;
use self::expression::DefaultExpressionHandler;
use self::filesystem::ObjectStoreFileSystemClient;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use crate::{
    DeltaResult, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler, TableClient,
};

pub mod conversion;
pub mod executor;
pub mod expression;
pub mod file_handler;
pub mod filesystem;
pub mod json;
pub mod parquet;

#[derive(Debug)]
pub struct DefaultTableClient<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    file_system: Arc<ObjectStoreFileSystemClient<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    expression: Arc<DefaultExpressionHandler>,
}

impl<E: TaskExecutor> DefaultTableClient<E> {
    /// Create a new [`DefaultTableClient`] instance
    ///
    /// The `path` parameter is used to determine the type of storage used.
    ///
    /// The `task_executor` is used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn try_new<I, K, V>(path: &Url, options: I, task_executor: Arc<E>) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, prefix) = parse_url_opts(path, options)?;
        let store = Arc::new(store);
        Ok(Self {
            file_system: Arc::new(ObjectStoreFileSystemClient::new(
                store.clone(),
                prefix,
                task_executor.clone(),
            )),
            json: Arc::new(DefaultJsonHandler::new(
                store.clone(),
                task_executor.clone(),
            )),
            parquet: Arc::new(DefaultParquetHandler::new(store.clone(), task_executor)),
            store,
            expression: Arc::new(DefaultExpressionHandler {}),
        })
    }

    pub fn new(store: Arc<DynObjectStore>, prefix: Path, task_executor: Arc<E>) -> Self {
        Self {
            file_system: Arc::new(ObjectStoreFileSystemClient::new(
                store.clone(),
                prefix,
                task_executor.clone(),
            )),
            json: Arc::new(DefaultJsonHandler::new(
                store.clone(),
                task_executor.clone(),
            )),
            parquet: Arc::new(DefaultParquetHandler::new(store.clone(), task_executor)),
            store,
            expression: Arc::new(DefaultExpressionHandler {}),
        }
    }

    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.store.clone())
    }
}

impl<E: TaskExecutor> TableClient for DefaultTableClient<E> {
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
