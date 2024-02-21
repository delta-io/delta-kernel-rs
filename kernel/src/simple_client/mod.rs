//! This module implements a simple, single threaded, EngineClient

use crate::{EngineClient, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

use std::sync::Arc;

pub mod data;
mod fs_client;
mod get_data;
pub(crate) mod json;
mod parquet;

// #[derive(Debug)]
// pub(crate) struct SimpleDataExtractor {
//     expected_tag: data::SimpleDataTypeTag,
// }

// impl SimpleDataExtractor {
//     pub(crate) fn new() -> Self {
//         SimpleDataExtractor {
//             expected_tag: data::SimpleDataTypeTag,
//         }
//     }
// }

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

impl EngineClient for SimpleClient {
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
