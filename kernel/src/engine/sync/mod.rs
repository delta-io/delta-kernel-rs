//! A simple, single threaded, test-only [`Engine`].
//!
//! All I/O goes through an [`ObjectStore`]. [`SyncEngine::new`] uses a [`LocalFileSystem`]
//! built lazily per-URL (rooted at the URL's drive on Windows, or `/` on Unix), so any
//! `file://` URL is supported. [`SyncEngine::new_with_store`] takes any other store
//! (e.g. `InMemory`) and uses it directly for all URLs.
//!
//! Async work is driven via [`futures::executor::block_on`], which is sufficient for stores
//! that do not require a tokio reactor (`LocalFileSystem`, `InMemory`). Cloud-backed stores
//! are NOT supported here -- they would deadlock because `block_on` parks the calling thread
//! with no reactor to wake it. That's acceptable for this test-only engine; production code
//! should use [`DefaultEngine`] instead.
//!
//! [`DefaultEngine`]: crate::engine::default::DefaultEngine
//! [`LocalFileSystem`]: crate::object_store::local::LocalFileSystem

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use tracing::debug;
use url::Url;

use super::arrow_expression::ArrowEvaluationHandler;
use crate::engine::arrow_data::ArrowEngineData;
use crate::object_store::local::LocalFileSystem;
use crate::object_store::path::Path;
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
mod storage;

/// A simple (test-only) implementation of [`Engine`]. See module docs for supported stores.
pub(crate) struct SyncEngine {
    storage_handler: Arc<storage::SyncStorageHandler>,
    json_handler: Arc<json::SyncJsonHandler>,
    parquet_handler: Arc<parquet::SyncParquetHandler>,
    evaluation_handler: Arc<ArrowEvaluationHandler>,
}

impl SyncEngine {
    /// Create a SyncEngine that reads from the local filesystem via [`LocalFileSystem`].
    pub(crate) fn new() -> Self {
        Self::new_inner(None)
    }

    /// Create a SyncEngine backed by `store`. All I/O is performed synchronously via
    /// [`futures::executor::block_on`]. See module docs for the deadlock caveat on
    /// reactor-dependent stores.
    pub(crate) fn new_with_store(store: Arc<DynObjectStore>) -> Self {
        Self::new_inner(Some(store))
    }

    fn new_inner(store: Option<Arc<DynObjectStore>>) -> Self {
        SyncEngine {
            storage_handler: Arc::new(storage::SyncStorageHandler::new(store.clone())),
            json_handler: Arc::new(json::SyncJsonHandler::new(store.clone())),
            parquet_handler: Arc::new(parquet::SyncParquetHandler::new(store)),
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

/// Resolve the store, base URL, and path for `url`.
///
/// When `default_store` is `Some`, the user-provided store is returned with `url`'s decoded
/// path. The base URL is `url` with its path replaced by `/`, suitable for re-joining
/// listed [`Path`]s back into URLs.
///
/// When `default_store` is `None`, only `file://` URLs are accepted: a [`LocalFileSystem`]
/// is constructed rooted at the URL's resolved directory (the URL itself if it ends with
/// `/`, otherwise the parent), and the returned path is the simple filename (or empty for
/// directory URLs). Rooting at the parent avoids a url-crate quirk where path segments
/// containing reserved-looking characters (e.g. `:` in Windows drive letters or `~` in 8.3
/// short names like `RUNNER~1`) get URL-encoded by `path_segments_mut.extend(...)` but not
/// decoded back by `to_file_path`. By keeping such characters inside the store's base URL
/// (which is built once from a `PathBuf` and not re-processed), only simple filenames flow
/// through the broken round-trip.
pub(super) fn resolve_scope(
    default_store: Option<&Arc<DynObjectStore>>,
    url: &Url,
) -> DeltaResult<(Arc<DynObjectStore>, Url, Path)> {
    if let Some(store) = default_store {
        let mut base_url = url.clone();
        base_url.set_path("/");
        let path = Path::from_url_path(url.path())?;
        return Ok((store.clone(), base_url, path));
    }
    if url.scheme() != "file" {
        return Err(Error::generic(format!(
            "SyncEngine without an explicit store can only access file:// URLs, got: {url}"
        )));
    }
    let file_path = url
        .to_file_path()
        .map_err(|()| Error::generic(format!("Invalid file URL: {url}")))?;
    // Use the deepest existing ancestor of the URL's directory as the store prefix:
    // `LocalFileSystem::new_with_prefix` canonicalizes its argument (which requires the path
    // to exist), and operating on a non-existent path is a valid case for `list_from` on a
    // not-yet-written `_delta_log/` directory or `head` on a missing file.
    let target_dir = if url.path().ends_with('/') {
        file_path.clone()
    } else {
        file_path
            .parent()
            .ok_or_else(|| Error::generic(format!("File URL has no parent: {url}")))?
            .to_path_buf()
    };
    let mut prefix = target_dir.as_path();
    while !prefix.exists() {
        prefix = prefix
            .parent()
            .ok_or_else(|| Error::generic(format!("No existing ancestor for {target_dir:?}")))?;
    }
    let prefix = prefix.to_path_buf();
    let relative = file_path
        .strip_prefix(&prefix)
        .map_err(|e| Error::generic(format!("Failed to strip prefix: {e}")))?;
    let path = Path::from_iter(relative.components().filter_map(|c| match c {
        std::path::Component::Normal(s) => s.to_str().map(String::from),
        _ => None,
    }));
    let base_url = Url::from_directory_path(&prefix)
        .map_err(|()| Error::generic(format!("Could not URL-encode prefix {prefix:?}")))?;
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&prefix)?);
    Ok((store, base_url, path))
}

/// Fetch the contents of a file via [`resolve_scope`] and return them as bytes.
pub(super) fn get_bytes(
    default_store: Option<&Arc<DynObjectStore>>,
    location: &Url,
) -> DeltaResult<Bytes> {
    let (store, _, path) = resolve_scope(default_store, location)?;
    let get_result = futures::executor::block_on(store.get(&path))?;
    Ok(futures::executor::block_on(get_result.bytes())?)
}

/// Read each file as bytes and feed it to `try_create_from_bytes` to produce data batches.
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
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let engine = SyncEngine::new();
        test_arrow_engine(&engine, &url);
    }

    #[test]
    fn test_sync_engine_with_store() {
        let store = Arc::new(crate::object_store::memory::InMemory::new());
        let engine = SyncEngine::new_with_store(store);
        let url = Url::parse("memory:///test/").unwrap();
        test_arrow_engine(&engine, &url);
    }
}
