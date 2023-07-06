//! # Default TableClient

use std::sync::Arc;

use object_store::{parse_url_opts, DynObjectStore};
use url::Url;

use self::filesystem::ObjectStoreFileSystemClient;
use self::json::{DefaultJsonHandler, JsonReadContext};
use self::parquet::{DefaultParquetHandler, ParquetReadContext};
use crate::{
    DeltaResult, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler, TableClient,
};

pub mod arrow;
pub mod filesystem;
pub mod json;
pub mod parquet;

#[derive(Debug)]
pub struct DefaultTableClient {
    store: Arc<DynObjectStore>,
    file_system: Arc<ObjectStoreFileSystemClient>,
    json: Arc<DefaultJsonHandler>,
    parquet: Arc<DefaultParquetHandler>,
}

impl DefaultTableClient {
    /// Create a new [`DefaultTableClient`] instance
    pub fn try_new<I, K, V>(path: &Url, options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, prefix) = parse_url_opts(path, options)?;
        let store = Arc::new(store);
        Ok(Self {
            file_system: Arc::new(ObjectStoreFileSystemClient::new(store.clone(), prefix)),
            json: Arc::new(DefaultJsonHandler::new(store.clone())),
            parquet: Arc::new(DefaultParquetHandler::new(store.clone())),
            store,
        })
    }
}

impl DefaultTableClient {
    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.store.clone())
    }
}

impl TableClient for DefaultTableClient {
    type JsonReadContext = JsonReadContext;
    type ParquetReadContext = ParquetReadContext;

    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        todo!()
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.file_system.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler<FileReadContext = Self::JsonReadContext>> {
        self.json.clone()
    }

    fn get_parquet_handler(
        &self,
    ) -> Arc<dyn ParquetHandler<FileReadContext = Self::ParquetReadContext>> {
        self.parquet.clone()
    }
}
