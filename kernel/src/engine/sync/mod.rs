//! A simple, single threaded, test-only [`Engine`] that can only read from the local filesystem

use super::arrow_expression::ArrowEvaluationHandler;
use crate::engine::arrow_data::ArrowEngineData;
use crate::{
    DeltaResult, Engine, Error, EvaluationHandler, FileDataReadResultIterator, FileMeta,
    JsonHandler, ParquetHandler, PredicateRef, SchemaRef, StorageHandler,
};

use crate::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use crate::engine::arrow_conversion::TryFromKernel as _;
use itertools::Itertools;
use std::fs::File;
use std::sync::Arc;
use tracing::debug;

pub(crate) mod json;
mod parquet;
mod storage;

/// This is a simple (test-only) implementation of [`Engine`]. It only supports reading data from
/// the local filesystem, and internally represents data using `Arrow`.
pub(crate) struct SyncEngine {
    storage_handler: Arc<storage::SyncStorageHandler>,
    json_handler: Arc<json::SyncJsonHandler>,
    parquet_handler: Arc<parquet::SyncParquetHandler>,
    evaluation_handler: Arc<ArrowEvaluationHandler>,
}

impl SyncEngine {
    pub(crate) fn new() -> Self {
        SyncEngine {
            storage_handler: Arc::new(storage::SyncStorageHandler {}),
            json_handler: Arc::new(json::SyncJsonHandler {}),
            parquet_handler: Arc::new(parquet::SyncParquetHandler {}),
            evaluation_handler: Arc::new(ArrowEvaluationHandler {}),
        }
    }
}

impl Engine for SyncEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation_handler.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage_handler.clone()
    }

    /// Get the connector provided [`ParquetHandler`].
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }
}

fn read_files<F, I>(
    files: &[FileMeta],
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    mut try_create_from_file: F,
) -> DeltaResult<FileDataReadResultIterator>
where
    I: Iterator<Item = DeltaResult<ArrowEngineData>> + Send + 'static,
    F: FnMut(File, SchemaRef, ArrowSchemaRef, Option<PredicateRef>, String) -> DeltaResult<I>
        + Send
        + 'static,
{
    debug!("Reading files: {files:#?} with schema {schema:#?} and predicate {predicate:#?}");
    if files.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }
    let arrow_schema = Arc::new(ArrowSchema::try_from_kernel(schema.as_ref())?);
    let files = files.to_vec();
    let result = files
        .into_iter()
        // Produces Iterator<DeltaResult<Iterator<DeltaResult<ArrowEngineData>>>>
        .map(move |file| {
            let location_string = file.location.to_string();
            let location = file.location;
            debug!("Reading {location:#?} with schema {schema:#?} and predicate {predicate:#?}");
            let path = location
                .to_file_path()
                .map_err(|_| Error::generic("can only read local files"))?;
            try_create_from_file(
                File::open(path)?,
                schema.clone(),
                arrow_schema.clone(),
                predicate.clone(),
                location_string,
            )
        })
        // Flatten to Iterator<DeltaResult<DeltaResult<ArrowEngineData>>>
        .flatten_ok()
        // Double unpack and map Iterator<DeltaResult<Box<EngineData>>>
        .map(|data| Ok(Box::new(ArrowEngineData::new(data??.into())) as _));
    Ok(Box::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::test_arrow_engine;

    #[test]
    fn test_sync_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let url = url::Url::from_directory_path(tmp.path()).unwrap();
        let engine = SyncEngine::new();
        test_arrow_engine(&engine, &url);
    }
}
