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
pub(crate) use parquet::SyncParquetHandler;
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
/// rooted at the URL's filesystem root (e.g. `/` on Unix, `C:\` on Windows) is created and
/// the returned path is relative to that root. This avoids a known url-crate quirk where
/// extending `file:///` with a Windows drive-letter segment (`["D:", "a", ...]`) drops the
/// drive letter on Windows.
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
    let raw_path = url
        .to_file_path()
        .map_err(|()| Error::generic(format!("Invalid file URL: {url}")))?;
    // On Windows the path may contain 8.3 short-name components such as `RUNNER~1`. The url
    // crate URL-encodes `~` to `%7E` when round-tripping through `path_segments_mut`, but
    // `to_file_path` does not decode it back, so `LocalFileSystem` ends up looking up the
    // wrong path. Canonicalize so short names get resolved to their long form.
    let file_path = canonicalize_for_localfs(&raw_path)?;
    // Every absolute path has at least one ancestor: itself. The last ancestor is the
    // filesystem root (`/` on Unix, drive root like `C:\` on Windows).
    let root = file_path
        .ancestors()
        .last()
        .expect("PathBuf::ancestors always yields at least one element");
    let base_url = Url::from_directory_path(root)
        .map_err(|()| Error::generic(format!("Could not URL-encode root {root:?}")))?;
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    let relative = file_path
        .strip_prefix(root)
        .map_err(|e| Error::generic(format!("Failed to strip root from path: {e}")))?;
    let path = Path::from_iter(relative.components().filter_map(|c| match c {
        std::path::Component::Normal(s) => s.to_str().map(String::from),
        _ => None,
    }));
    Ok((store, base_url, path))
}

/// On Windows, canonicalize `path` (or its parent if `path` itself does not exist) so 8.3
/// short-name components such as `RUNNER~1` are resolved to their long form. The url crate
/// URL-encodes `~` to `%7E` when round-tripping a path through `path_segments_mut`, but
/// `to_file_path` does not decode it back, so `LocalFileSystem` would otherwise look up the
/// wrong path. On non-Windows platforms canonicalization would resolve symlinks (e.g.
/// `/var` -> `/private/var` on macOS) and break the URL round-trip, so it is a no-op.
fn canonicalize_for_localfs(path: &std::path::Path) -> DeltaResult<std::path::PathBuf> {
    #[cfg(not(windows))]
    {
        Ok(path.to_path_buf())
    }
    #[cfg(windows)]
    {
        if path.exists() {
            return Ok(path.canonicalize()?);
        }
        let parent = path
            .parent()
            .ok_or_else(|| Error::generic(format!("Path has no parent: {path:?}")))?;
        let canonical_parent = parent.canonicalize()?;
        Ok(match path.file_name() {
            Some(name) => canonical_parent.join(name),
            None => canonical_parent,
        })
    }
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
