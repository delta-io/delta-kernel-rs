//! A simple, single threaded, test-only [`Engine`].
//!
//! Supports both local filesystem and [`ObjectStore`]-backed reads. Use [`SyncEngine::new`] for
//! local-only access, or [`SyncEngine::new_with_store`] to read from any [`ObjectStore`]
//! implementation (e.g. `InMemory`).

use super::arrow_expression::ArrowEvaluationHandler;
use crate::engine::arrow_data::ArrowEngineData;
use crate::metrics::MetricsReporter;
use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
use crate::{
    DeltaResult, Engine, Error, EvaluationHandler, FileDataReadResultIterator, FileMeta,
    JsonHandler, ParquetHandler, PredicateRef, SchemaRef, StorageHandler,
};

use bytes::Bytes;
use itertools::Itertools;
use std::fs::File;
use std::sync::Arc;
use tracing::debug;

pub(crate) mod json;
mod parquet;
pub(crate) use parquet::SyncParquetHandler;
mod storage;

/// A simple (test-only) implementation of [`Engine`]. Supports both local filesystem reads
/// via [`SyncEngine::new`] and [`ObjectStore`]-backed reads via [`SyncEngine::new_with_store`].
pub(crate) struct SyncEngine {
    storage_handler: Arc<storage::SyncStorageHandler>,
    json_handler: Arc<json::SyncJsonHandler>,
    parquet_handler: Arc<parquet::SyncParquetHandler>,
    evaluation_handler: Arc<ArrowEvaluationHandler>,
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
}

impl SyncEngine {
    /// Create a SyncEngine that only reads from the local filesystem.
    pub(crate) fn new() -> Self {
        SyncEngine {
            storage_handler: Arc::new(storage::SyncStorageHandler::new()),
            json_handler: Arc::new(json::SyncJsonHandler::new()),
            parquet_handler: Arc::new(parquet::SyncParquetHandler::new()),
            evaluation_handler: Arc::new(ArrowEvaluationHandler {}),
            metrics_reporter: None,
        }
    }

    /// Create a SyncEngine backed by an [`ObjectStore`]. All I/O is performed synchronously
    /// via [`block_on_async`].
    pub(crate) fn new_with_store(store: Arc<DynObjectStore>) -> Self {
        SyncEngine {
            storage_handler: Arc::new(storage::SyncStorageHandler::with_store(store.clone())),
            json_handler: Arc::new(json::SyncJsonHandler::with_store(store.clone())),
            parquet_handler: Arc::new(parquet::SyncParquetHandler::with_store(store)),
            evaluation_handler: Arc::new(ArrowEvaluationHandler {}),
            metrics_reporter: None,
        }
    }

    /// Attach a metrics reporter to this engine.
    pub(crate) fn with_metrics_reporter(mut self, reporter: Arc<dyn MetricsReporter>) -> Self {
        self.metrics_reporter = Some(reporter);
        self
    }
}

impl Engine for SyncEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation_handler.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn get_metrics_reporter(&self) -> Option<Arc<dyn MetricsReporter>> {
        self.metrics_reporter.clone()
    }
}

/// Run an async future to completion on the current thread.
pub(super) fn block_on_async<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

/// Fetch the contents of a file from an [`ObjectStore`] synchronously.
pub(super) fn get_bytes_from_store(
    store: &DynObjectStore,
    location: &url::Url,
) -> DeltaResult<Bytes> {
    let path = crate::object_store::path::Path::from(location.path());
    let get_result = block_on_async(store.get(&path))?;
    let bytes = block_on_async(get_result.bytes())?;
    Ok(bytes)
}

fn read_files<F, I>(
    files: &[FileMeta],
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    mut try_create_from_file: F,
) -> DeltaResult<FileDataReadResultIterator>
where
    I: Iterator<Item = DeltaResult<ArrowEngineData>> + Send + 'static,
    F: FnMut(File, SchemaRef, Option<PredicateRef>, String) -> DeltaResult<I> + Send + 'static,
{
    debug!("Reading files: {files:#?} with schema {schema:#?} and predicate {predicate:#?}");
    if files.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }
    let files = files.to_vec();
    let result = files
        .into_iter()
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
                predicate.clone(),
                location_string,
            )
        })
        .flatten_ok()
        .map(|data| Ok(Box::new(ArrowEngineData::new(data??.into())) as _));
    Ok(Box::new(result))
}

/// Like [`read_files`] but fetches file contents from an [`ObjectStore`] as [`Bytes`].
fn read_files_from_store<F, I>(
    files: &[FileMeta],
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    store: Arc<DynObjectStore>,
    mut try_create_from_bytes: F,
) -> DeltaResult<FileDataReadResultIterator>
where
    I: Iterator<Item = DeltaResult<ArrowEngineData>> + Send + 'static,
    F: FnMut(Bytes, SchemaRef, Option<PredicateRef>, String) -> DeltaResult<I> + Send + 'static,
{
    debug!(
        "Reading files from store: {files:#?} with schema {schema:#?} and predicate {predicate:#?}"
    );
    if files.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }
    let files = files.to_vec();
    let result = files
        .into_iter()
        .map(move |file| {
            let location_string = file.location.to_string();
            debug!(
                "Reading {:#?} from store with schema {schema:#?} and predicate {predicate:#?}",
                file.location
            );
            let data = get_bytes_from_store(&store, &file.location)?;
            try_create_from_bytes(data, schema.clone(), predicate.clone(), location_string)
        })
        .flatten_ok()
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

    #[test]
    fn test_sync_engine_with_store() {
        let store = Arc::new(crate::object_store::memory::InMemory::new());
        let engine = SyncEngine::new_with_store(store);
        let url = url::Url::parse("memory:///test/").unwrap();
        test_arrow_engine(&engine, &url);
    }
}
