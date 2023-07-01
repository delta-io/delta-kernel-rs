use std::sync::Arc;

use object_store::{parse_url_opts, DynObjectStore, ObjectMeta};
use url::Url;

use super::filesystem::ObjectStoreFileSystemClient;
use super::json::{DefaultJsonHandler, JsonReadContext};
use super::{ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler, TableClient};
use crate::DeltaResult;

#[derive(Debug)]
pub struct DefaultTableClient {
    store: Arc<DynObjectStore>,
    file_system: Arc<ObjectStoreFileSystemClient>,
    json: Arc<DefaultJsonHandler>,
}

impl DefaultTableClient {
    /// Create a new [`DefaultTableClient`] instnance
    pub fn try_new<I, K, V>(path: impl AsRef<Url>, options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, prefix) = parse_url_opts(path.as_ref(), options)?;
        let store = Arc::new(store);
        let file_system = Arc::new(ObjectStoreFileSystemClient::new(store.clone(), prefix));
        let json = Arc::new(DefaultJsonHandler::new(store.clone()));
        Ok(Self {
            store,
            file_system,
            json,
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
    type ParquetReadContext = ObjectMeta;

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
        todo!()
    }
}
