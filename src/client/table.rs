use std::sync::Arc;

use object_store::{parse_url_opts, ObjectMeta};
use url::Url;

use super::filesystem::ObjectStoreFileSystemClient;
use super::{ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler, TableClient};
use crate::DeltaResult;

#[derive(Debug)]
pub struct DefaultTableClient {
    file_system: Arc<ObjectStoreFileSystemClient>,
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
        let client = ObjectStoreFileSystemClient::new(Arc::new(store), prefix);
        Ok(Self {
            file_system: Arc::new(client),
        })
    }
}

impl TableClient for DefaultTableClient {
    type JsonReadContext = ObjectMeta;
    type ParquetReadContext = ObjectMeta;

    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        todo!()
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.file_system.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler<FileReadContext = Self::JsonReadContext>> {
        todo!()
    }

    fn get_parquet_handler(
        &self,
    ) -> Arc<dyn ParquetHandler<FileReadContext = Self::ParquetReadContext>> {
        todo!()
    }
}
