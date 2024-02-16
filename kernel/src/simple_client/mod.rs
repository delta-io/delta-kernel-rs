//! This module implements a simple, single threaded, EngineClient

use crate::engine_data::{DataVisitor, EngineData, TypeTag};
use crate::schema::SchemaRef;
use crate::{
    DataExtractor, DeltaResult, EngineClient, ExpressionHandler, FileSystemClient, JsonHandler,
    ParquetHandler,
};

use std::sync::Arc;

pub mod data;
mod fs_client;
mod get_data_item;
pub(crate) mod json;
mod parquet;

#[derive(Debug)]
pub(crate) struct SimpleDataExtractor {
    expected_tag: data::SimpleDataTypeTag,
}

impl SimpleDataExtractor {
    pub(crate) fn new() -> Self {
        SimpleDataExtractor {
            expected_tag: data::SimpleDataTypeTag,
        }
    }
}

impl DataExtractor for SimpleDataExtractor {
    fn extract(
        &self,
        blob: &dyn EngineData,
        schema: SchemaRef,
        visitor: &mut dyn DataVisitor,
    ) -> DeltaResult<()> {
        assert!(self.expected_tag.eq(blob.type_tag()));
        let data: &data::SimpleData = blob
            .as_any()
            .downcast_ref::<data::SimpleData>()
            .expect("extract called on blob that isn't SimpleData");
        //data.extract(schema, visitor)
        let mut col_array = vec![];
        data.extract_columns(&schema, &mut col_array)?;
        visitor.visit(data.length(), &col_array);
        Ok(())
    }

    fn length(&self, blob: &dyn EngineData) -> usize {
        assert!(self.expected_tag.eq(blob.type_tag()));
        let data: &data::SimpleData = blob
            .as_any()
            .downcast_ref::<data::SimpleData>()
            .expect("length called on blob that isn't SimpleData");
        data.length()
    }
}

pub struct SimpleClient {
    data_extractor: Arc<SimpleDataExtractor>,
    fs_client: Arc<fs_client::SimpleFilesystemClient>,
    json_handler: Arc<json::SimpleJsonHandler>,
    parquet_handler: Arc<parquet::SimpleParquetHandler>,
}

impl SimpleClient {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        SimpleClient {
            data_extractor: Arc::new(SimpleDataExtractor::new()),
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

    fn get_data_extactor(&self) -> Arc<dyn DataExtractor> {
        self.data_extractor.clone()
    }
}
