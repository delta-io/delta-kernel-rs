//! A simple, single threaded, test-only [`Engine`].
//!
//! Supports both local filesystem and [`ObjectStore`]-backed reads. Use [`SyncEngine::new`] for
//! local-only access, or [`SyncEngine::new_with_store`] to read from any [`ObjectStore`]
//! implementation (e.g. `InMemory`).
//!
//! Async work in `ObjectStore` calls is driven via [`futures::executor::block_on`], which is
//! sufficient for stores that do not require a tokio reactor (e.g. `LocalFileSystem`,
//! `InMemory`). Cloud-backed stores are NOT supported here -- they would deadlock because
//! `block_on` parks the calling thread with no reactor to wake it. That's acceptable for this
//! test-only engine; production code should use [`DefaultEngine`] instead.
//!
//! [`DefaultEngine`]: crate::engine::default::DefaultEngine

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use tracing::debug;

use super::arrow_expression::ArrowEvaluationHandler;
use crate::engine::arrow_data::ArrowEngineData;
use crate::object_store::DynObjectStore;
// `ObjectStoreExt` is needed for `store.get()` etc. in arrow-58 mode where these methods moved
// off the `ObjectStore` trait. In arrow-57 mode the compat shim makes the import a no-op, so
// silence the resulting unused-import warning.
#[allow(unused_imports)]
use crate::object_store::ObjectStoreExt as _;
use crate::{
    DeltaResult, Engine, Error, EvaluationHandler, FileDataReadResultIterator, FileMeta,
    JsonHandler, ParquetHandler, PredicateRef, SchemaRef, StorageHandler,
};

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
}

impl SyncEngine {
    /// Create a SyncEngine that only reads from the local filesystem.
    pub(crate) fn new() -> Self {
        SyncEngine {
            storage_handler: Arc::new(storage::SyncStorageHandler::new()),
            json_handler: Arc::new(json::SyncJsonHandler::new()),
            parquet_handler: Arc::new(parquet::SyncParquetHandler::new()),
            evaluation_handler: Arc::new(ArrowEvaluationHandler {}),
        }
    }

    /// Create a SyncEngine backed by an [`ObjectStore`]. All I/O is performed synchronously
    /// via [`futures::executor::block_on`]. See module docs for the deadlock caveat on
    /// reactor-dependent stores.
    pub(crate) fn new_with_store(store: Arc<DynObjectStore>) -> Self {
        SyncEngine {
            storage_handler: Arc::new(storage::SyncStorageHandler::with_store(store.clone())),
            json_handler: Arc::new(json::SyncJsonHandler::with_store(store.clone())),
            parquet_handler: Arc::new(parquet::SyncParquetHandler::with_store(store)),
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

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }
}

/// Fetch the contents of a file synchronously, either from an [`ObjectStore`] when present or
/// from the local filesystem otherwise.
pub(super) fn get_bytes(
    store: Option<&Arc<DynObjectStore>>,
    location: &url::Url,
) -> DeltaResult<Bytes> {
    if let Some(store) = store {
        let path = crate::object_store::path::Path::from(location.path());
        let get_result = futures::executor::block_on(store.get(&path))?;
        let bytes = futures::executor::block_on(get_result.bytes())?;
        return Ok(bytes);
    }
    let path = location
        .to_file_path()
        .map_err(|_| Error::generic("can only read local files"))?;
    Ok(std::fs::read(path)?.into())
}

/// Read each file as bytes (from store or local FS) and feed it to `try_create_from_bytes` to
/// produce data batches. Consolidates the dispatch so callers don't have to branch on the
/// presence of an [`ObjectStore`].
fn read_files<F, I>(
    store: Option<&Arc<DynObjectStore>>,
    files: &[FileMeta],
    schema: SchemaRef,
    predicate: Option<PredicateRef>,
    mut try_create_from_bytes: F,
) -> DeltaResult<FileDataReadResultIterator>
where
    I: Iterator<Item = DeltaResult<ArrowEngineData>> + Send + 'static,
    F: FnMut(Bytes, SchemaRef, Option<PredicateRef>, String) -> DeltaResult<I> + Send + 'static,
{
    debug!("Reading files: {files:#?} with schema {schema:#?} and predicate {predicate:#?}");
    if files.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }
    let files = files.to_vec();
    let store = store.cloned();
    let result = files
        .into_iter()
        .map(move |file| {
            let location_string = file.location.to_string();
            let bytes = get_bytes(store.as_ref(), &file.location)?;
            try_create_from_bytes(bytes, schema.clone(), predicate.clone(), location_string)
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
