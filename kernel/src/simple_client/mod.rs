//! This module implements a simple, single threaded, EngineInterface

use crate::{EngineInterface, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

use std::sync::Arc;

pub mod data;
mod fs_client;
mod get_data;
pub(crate) mod json;
mod parquet;

pub struct SimpleClient {
    fs_client: Arc<fs_client::SimpleFilesystemClient>,
    json_handler: Arc<json::SimpleJsonHandler>,
    parquet_handler: Arc<parquet::SimpleParquetHandler>,
}

impl SimpleClient {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        SimpleClient {
            fs_client: Arc::new(fs_client::SimpleFilesystemClient {}),
            json_handler: Arc::new(json::SimpleJsonHandler {}),
            parquet_handler: Arc::new(parquet::SimpleParquetHandler {}),
        }
    }
}

impl EngineInterface for SimpleClient {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        unimplemented!();
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.fs_client.clone()
    }

    /// Get the connector provided [`ParquetHandler`].
    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }
}
