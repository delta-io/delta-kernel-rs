//! A simple, single threaded, [`Engine`] that can only read from the local filesystem

use super::arrow_expression::ArrowExpressionHandler;
use crate::{Engine, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

use std::sync::Arc;

mod fs_client;
pub(crate) mod json;
mod parquet;

/// This is a simple implementation of [`Engine`]. It only supports reading data from the local
/// filesystem, and internally represents data using `Arrow`.
pub struct SyncEngine {
    fs_client: Arc<fs_client::SyncFilesystemClient>,
    json_handler: Arc<json::SyncJsonHandler>,
    parquet_handler: Arc<parquet::SyncParquetHandler>,
    expression_handler: Arc<ArrowExpressionHandler>,
}

impl SyncEngine {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        SyncEngine {
            fs_client: Arc::new(fs_client::SyncFilesystemClient {}),
            json_handler: Arc::new(json::SyncJsonHandler {}),
            parquet_handler: Arc::new(parquet::SyncParquetHandler {}),
            expression_handler: Arc::new(ArrowExpressionHandler {}),
        }
    }
}

impl Engine for SyncEngine {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        self.expression_handler.clone()
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
