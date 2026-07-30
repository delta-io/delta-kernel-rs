use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::object_store::list::{PaginatedListOptions, PaginatedListStore};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{self, DynObjectStore, ObjectStoreExt as _, PutMode};
use delta_kernel::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use itertools::Itertools;
use url::Url;

use crate::executor::TaskExecutor;
use crate::UrlExt;

pub struct ObjectStoreStorageHandler<E: TaskExecutor> {
    inner: Arc<DynObjectStore>,
    /// Present only for backends implementing [`PaginatedListStore`] (S3/GCS/Azure), which push
    /// the single-directory delimiter into the listing request. `None` for local/memory/http and
    /// custom stores, which fall back to a client-side direct-children filter.
    paginated: Option<Arc<dyn PaginatedListStore>>,
    task_executor: Arc<E>,
    readahead: usize,
}

impl<E: TaskExecutor> std::fmt::Debug for ObjectStoreStorageHandler<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreStorageHandler")
            .field("inner", &self.inner)
            .field("paginated", &self.paginated.is_some())
            .field("readahead", &self.readahead)
            .finish_non_exhaustive()
    }
}

impl<E: TaskExecutor> ObjectStoreStorageHandler<E> {
    pub(crate) fn new(
        store: Arc<DynObjectStore>,
        paginated: Option<Arc<dyn PaginatedListStore>>,
        task_executor: Arc<E>,
    ) -> Self {
        Self {
            inner: store,
            paginated,
            task_executor,
            readahead: 10,
        }
    }

    /// Set the maximum number of files to read in parallel.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }
}

/// Derives the `(prefix, offset)` pair for a `list_from` request from `path`.
///
/// The offset is the list-after lower bound; the prefix restricts the listing to a single
/// directory. `Path` strips trailing slashes, so directory-vs-file is decided from the original
/// URL: a directory-like `path` (ends with `/`) lists its own contents, otherwise the parent
/// directory is listed after `path`.
fn list_scope(path: &Url) -> DeltaResult<(Path, Path)> {
    let offset = Path::from_url_path(path.path())?;
    let prefix = if path.path().ends_with('/') {
        offset.clone()
    } else {
        let mut parts = offset.parts().collect_vec();
        if parts.pop().is_none() {
            return Err(Error::Generic(format!(
                "Offset path must not be a root directory. Got: '{path}'",
            )));
        }
        Path::from_iter(parts)
    };
    Ok((prefix, offset))
}

/// Builds a [`FileMeta`] URL from a listed object's store-relative [`Path`], reusing `base`'s
/// scheme and authority.
fn file_meta_from_object(base: &Url, location: &Path, last_modified: i64, size: u64) -> FileMeta {
    let mut url = base.clone();
    url.set_path(&format!("/{}", location.as_ref()));
    FileMeta {
        location: url,
        last_modified,
        size,
    }
}

/// Native async implementation for list_from.
///
/// [`StorageHandler::list_from`] is single-directory: only the direct children of the listed
/// directory are returned. Two paths deliver this:
/// - Path A ([`list_one_level_paginated`]): when a [`PaginatedListStore`] handle is present, the
///   `/` delimiter is pushed into the request so the store never descends into subdirectories.
/// - Path B: otherwise, the recursive `list_with_offset` stream is filtered client-side to direct
///   children only.
///
/// Storage metrics are emitted by the outer [`MeteredStorageHandler`] wrapping this
/// handler (e.g. inside `DefaultEngine`'s `storage_handler()`), so this function just
/// returns the raw stream.
///
/// [`MeteredStorageHandler`]: delta_kernel::metrics::MeteredStorageHandler
async fn list_from_impl(
    store: Arc<DynObjectStore>,
    paginated: Option<Arc<dyn PaginatedListStore>>,
    path: Url,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    let (prefix, offset) = list_scope(&path)?;

    if let Some(paginated) = paginated {
        let files = list_one_level_paginated(paginated, path, prefix, offset).await?;
        return Ok(Box::pin(stream::iter(files.into_iter().map(Ok))));
    }

    let has_ordered_listing = supports_ordered_listing(&path);
    let filter_prefix = prefix.clone();
    let stream = store
        .list_with_offset(Some(&prefix), &offset)
        // Single-directory contract: drop entries nested below `prefix`. `prefix_match` yields the
        // path parts after `prefix`; a direct child has exactly one remaining part (its filename).
        .try_filter(move |meta| {
            let is_direct_child = meta
                .location
                .prefix_match(&filter_prefix)
                .is_some_and(|parts| parts.count() == 1);
            futures::future::ready(is_direct_child)
        })
        .map(move |meta| {
            let meta = meta?;
            Ok(file_meta_from_object(
                &path,
                &meta.location,
                meta.last_modified.timestamp_millis(),
                meta.size,
            ))
        });

    if !has_ordered_listing {
        // Local filesystem doesn't return sorted list - need to collect and sort
        let mut items: Vec<_> = stream.try_collect().await?;
        items.sort_unstable();
        Ok(Box::pin(stream::iter(
            items.into_iter().map(Ok::<FileMeta, delta_kernel::Error>),
        )))
    } else {
        Ok(Box::pin(stream))
    }
}

/// Path A: list a single directory level via [`PaginatedListStore`], starting after `offset`.
///
/// The `/` delimiter makes the store return direct children in `objects` and subdirectories in
/// `common_prefixes` (ignored here), so nested files are never fetched. `list_paginated` does not
/// guarantee ordering, so the collected results are sorted before returning to satisfy the
/// sorted-listing contract that [`StorageHandler::list_from`] callers rely on.
async fn list_one_level_paginated(
    paginated: Arc<dyn PaginatedListStore>,
    base_url: Url,
    prefix: Path,
    offset: Path,
) -> DeltaResult<Vec<FileMeta>> {
    // `list_paginated` does not append a trailing delimiter to the prefix (unlike
    // `ObjectStore::list`), so a slash-less prefix would roll every direct child into a
    // `common_prefix` and return no objects. Append it here, treating an empty prefix as a root
    // listing (no prefix).
    let prefix = (!prefix.as_ref().is_empty()).then(|| format!("{}/", prefix.as_ref()));
    let mut out = Vec::new();
    let mut page_token = None;
    loop {
        let result = paginated
            .list_paginated(
                prefix.as_deref(),
                PaginatedListOptions {
                    offset: Some(offset.to_string()),
                    delimiter: Some("/".into()),
                    page_token,
                    ..Default::default()
                },
            )
            .await?;
        for meta in result.result.objects {
            out.push(file_meta_from_object(
                &base_url,
                &meta.location,
                meta.last_modified.timestamp_millis(),
                meta.size,
            ));
        }
        match result.page_token {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Native async implementation for read_files
async fn read_files_impl(
    store: Arc<DynObjectStore>,
    files: Vec<FileSlice>,
    readahead: usize,
) -> DeltaResult<BoxStream<'static, DeltaResult<Bytes>>> {
    let files = stream::iter(files).map(move |(url, range)| {
        let store = store.clone();
        async move {
            // Wasn't checking the scheme before calling to_file_path causing the url path to
            // be eaten in a strange way. Now, if not a file scheme, just blindly convert to a path.
            // https://docs.rs/url/latest/url/struct.Url.html#method.to_file_path has more
            // details about why this check is necessary
            let path = if url.scheme() == "file" {
                let file_path = url
                    .to_file_path()
                    .map_err(|_| Error::InvalidTableLocation(format!("Invalid file URL: {url}")))?;
                Path::from_absolute_path(file_path)
                    .map_err(|e| Error::InvalidTableLocation(format!("Invalid file path: {e}")))?
            } else {
                Path::from(url.path())
            };
            if url.is_presigned() {
                // have to annotate type here or rustc can't figure it out
                Ok::<bytes::Bytes, Error>(reqwest::get(url).await?.bytes().await?)
            } else if let Some(rng) = range {
                Ok(store.get_range(&path, rng).await?)
            } else {
                let result = store.get(&path).await?;
                Ok(result.bytes().await?)
            }
        }
    });

    // We allow executing up to `readahead` futures concurrently and
    // buffer the results. This allows us to achieve async concurrency.
    Ok(Box::pin(files.buffered(readahead)))
}

/// Native async implementation for copy_atomic
async fn copy_atomic_impl(
    store: Arc<DynObjectStore>,
    src_path: Path,
    dest_path: Path,
) -> DeltaResult<()> {
    // Read source file then write atomically with PutMode::Create. Note that a GET/PUT is not
    // necessarily atomic, but since the source file is immutable, we aren't exposed to the
    // possibility of source file changing while we do the PUT.
    let data = store.get(&src_path).await?.bytes().await?;
    store
        .put_opts(&dest_path, data.into(), PutMode::Create.into())
        .await
        .map_err(|e| match e {
            object_store::Error::AlreadyExists { .. } => Error::FileAlreadyExists(dest_path.into()),
            e => e.into(),
        })?;
    Ok(())
}

/// Native async implementation for put
async fn put_impl(
    store: Arc<DynObjectStore>,
    path: Path,
    data: Bytes,
    overwrite: bool,
) -> DeltaResult<()> {
    let put_mode = if overwrite {
        PutMode::Overwrite
    } else {
        PutMode::Create
    };
    let result = store.put_opts(&path, data.into(), put_mode.into()).await;
    result.map_err(|e| match e {
        object_store::Error::AlreadyExists { .. } => Error::FileAlreadyExists(path.into()),
        e => e.into(),
    })?;
    Ok(())
}

/// Native async implementation for delete.
async fn delete_impl(store: Arc<DynObjectStore>, path: Path) -> DeltaResult<()> {
    match store.delete(&path).await {
        Ok(()) => Ok(()),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Native async implementation for head
async fn head_impl(store: Arc<DynObjectStore>, url: Url) -> DeltaResult<FileMeta> {
    let meta = store.head(&Path::from_url_path(url.path())?).await?;
    Ok(FileMeta {
        location: url,
        last_modified: meta.last_modified.timestamp_millis(),
        size: meta.size,
    })
}

impl<E: TaskExecutor> StorageHandler for ObjectStoreStorageHandler<E> {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let future = list_from_impl(self.inner.clone(), self.paginated.clone(), path.clone());
        let iter = super::stream_future_to_iter(self.task_executor.clone(), future)?;
        Ok(iter) // type coercion drops the unneeded Send bound
    }

    /// Read data specified by the start and end offset from the file.
    ///
    /// This will return the data in the same order as the provided file slices.
    ///
    /// Multiple reads may occur in parallel, depending on the configured readahead.
    /// See [`Self::with_readahead`].
    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let future = read_files_impl(self.inner.clone(), files, self.readahead);
        let iter = super::stream_future_to_iter(self.task_executor.clone(), future)?;
        Ok(iter) // type coercion drops the unneeded Send bound
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        let path = Path::from_url_path(path.path())?;
        self.task_executor
            .block_on(put_impl(self.inner.clone(), path, data, overwrite))
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        let src_path = Path::from_url_path(src.path())?;
        let dest_path = Path::from_url_path(dest.path())?;
        let future = copy_atomic_impl(self.inner.clone(), src_path, dest_path);
        self.task_executor.block_on(future)
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        let future = head_impl(self.inner.clone(), path.clone());
        self.task_executor.block_on(future)
    }

    fn delete(&self, path: &Url) -> DeltaResult<()> {
        let path = Path::from_url_path(path.path())?;
        self.task_executor
            .block_on(delete_impl(self.inner.clone(), path))
    }
}

/// Returns whether or not the [Url] can support ordered listing.
///
/// When this returns false the default engine will need to collect a stream before returning,
/// which has a performance impact
///
/// The current known situations where there are unordered listings are with filesystems and AWS S3
/// Express One Zone directory buckets
///
/// Although the `object_store` crate explicitly says it _does not_ return a sorted listing, in
/// practice many implementations actually do:
/// - AWS: [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
///   states: "For general purpose buckets, ListObjectsV2 returns objects in lexicographical order
///   based on their key names."
/// - Azure: Docs state [here](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources):
///   "A listing operation returns an XML response that contains all or part of the requested list.
///   The operation returns entities in alphabetical order."
/// - GCP: The [main](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) doc doesn't indicate
///   order, but [this page](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) does say:
///   "This page shows you how to list the [objects](https://cloud.google.com/storage/docs/objects)
///   stored in your Cloud Storage buckets, which are ordered in the list lexicographically by
///   name."
fn supports_ordered_listing(url: &Url) -> bool {
    !((url.scheme() == "file")
        // S3 Directory Buckets
        || url.domain().map(|d| d.contains("--x-s3")).unwrap_or(false)
        // S3 Directory Bucket Access Points
        || url.domain().map(|d| d.contains("-xa-s3")).unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::time::Duration;

    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::Engine as _;
    use delta_kernel_default_engine_test_utils::current_time_duration;
    use itertools::Itertools;
    use test_utils::delta_path_for_version;

    use super::*;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::DefaultEngineBuilder;

    fn setup_test() -> (
        tempfile::TempDir,
        Arc<LocalFileSystem>,
        ObjectStoreStorageHandler<TokioBackgroundExecutor>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), None, executor);
        (tmp, store, handler)
    }

    #[test]
    fn test_ordered_listing_for_url() {
        for (u, expected) in &[
            (Url::parse("file:///dev/null").unwrap(), false),
            (Url::parse("s3://robbert").unwrap(), true),
            (Url::parse("s3://robbert/likes/paths").unwrap(), true),
            (Url::parse("s3://robbie-one-zone--x-s3").unwrap(), false),
            (
                Url::parse("https://robbie-one-zone-xa-s3.us-east-2.amazonaws.biz").unwrap(),
                false,
            ),
        ] {
            assert_eq!(
                *expected,
                supports_ordered_listing(u),
                "expected {expected} on {u:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_read_files() {
        let tmp = tempfile::tempdir().unwrap();
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();

        let data = Bytes::from("kernel-data");
        tmp_store
            .put(&Path::from("a"), data.clone().into())
            .await
            .unwrap();
        tmp_store
            .put(&Path::from("b"), data.clone().into())
            .await
            .unwrap();
        tmp_store
            .put(&Path::from("c"), data.clone().into())
            .await
            .unwrap();

        let mut url = Url::from_directory_path(tmp.path()).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, None, executor);

        let mut slices: Vec<FileSlice> = Vec::new();

        let mut url1 = url.clone();
        url1.set_path(&format!("{}/b", url.path()));
        slices.push((url1.clone(), Some(Range { start: 0, end: 6 })));
        slices.push((url1, Some(Range { start: 7, end: 11 })));

        url.set_path(&format!("{}/c", url.path()));
        slices.push((url, Some(Range { start: 4, end: 9 })));
        dbg!("Slices are: {}", &slices);
        let data: Vec<Bytes> = storage.read_files(slices).unwrap().try_collect().unwrap();

        assert_eq!(data.len(), 3);
        assert_eq!(data[0], Bytes::from("kernel"));
        assert_eq!(data[1], Bytes::from("data"));
        assert_eq!(data[2], Bytes::from("el-da"));
    }

    #[tokio::test]
    async fn test_file_meta_is_correct() {
        let store = Arc::new(InMemory::new());

        let begin_time = current_time_duration().unwrap();

        let data = Bytes::from("kernel-data");
        let name = delta_path_for_version(1, "json");
        store.put(&name, data.clone().into()).await.unwrap();

        let table_root = Url::parse("memory:///").expect("valid url");
        let engine = DefaultEngineBuilder::new(store).build();
        let files: Vec<_> = engine
            .storage_handler()
            .list_from(&table_root.join("_delta_log/").unwrap().join("0").unwrap())
            .unwrap()
            .try_collect()
            .unwrap();

        assert!(!files.is_empty());
        for meta in files.into_iter() {
            let meta_time = Duration::from_millis(meta.last_modified.try_into().unwrap());
            assert!(meta_time.abs_diff(begin_time) < Duration::from_secs(10));
        }
    }
    #[tokio::test]
    async fn test_default_engine_listing() {
        let tmp = tempfile::tempdir().unwrap();
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
        let data = Bytes::from("kernel-data");

        let expected_names: Vec<Path> =
            (0..10).map(|i| delta_path_for_version(i, "json")).collect();

        // put them in in reverse order
        for name in expected_names.iter().rev() {
            tmp_store.put(name, data.clone().into()).await.unwrap();
        }

        let url = Url::from_directory_path(tmp.path()).unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let files = engine
            .storage_handler()
            .list_from(&url.join("_delta_log/").unwrap().join("0").unwrap())
            .unwrap();
        let mut len = 0;
        for (file, expected) in files.zip(expected_names.iter()) {
            assert!(
                file.as_ref()
                    .unwrap()
                    .location
                    .path()
                    .ends_with(expected.as_ref()),
                "{} does not end with {}",
                file.unwrap().location.path(),
                expected
            );
            len += 1;
        }
        assert_eq!(len, 10, "list_from should have returned 10 files");
    }

    #[tokio::test]
    async fn test_copy() {
        let (tmp, store, handler) = setup_test();

        // basic
        let data = Bytes::from("test-data");
        let src_path = Path::from_absolute_path(tmp.path().join("src.txt")).unwrap();
        store.put(&src_path, data.clone().into()).await.unwrap();
        let src_url = Url::from_file_path(tmp.path().join("src.txt")).unwrap();
        let dest_url = Url::from_file_path(tmp.path().join("dest.txt")).unwrap();
        assert!(handler.copy_atomic(&src_url, &dest_url).is_ok());
        let dest_path = Path::from_absolute_path(tmp.path().join("dest.txt")).unwrap();
        assert_eq!(
            store.get(&dest_path).await.unwrap().bytes().await.unwrap(),
            data
        );

        // copy to existing fails
        assert!(matches!(
            handler.copy_atomic(&src_url, &dest_url),
            Err(Error::FileAlreadyExists(_))
        ));

        // copy from non-existing fails
        let missing_url = Url::from_file_path(tmp.path().join("missing.txt")).unwrap();
        let new_dest_url = Url::from_file_path(tmp.path().join("new_dest.txt")).unwrap();
        assert!(handler.copy_atomic(&missing_url, &new_dest_url).is_err());
    }

    #[tokio::test]
    async fn test_head() {
        let (tmp, store, handler) = setup_test();

        let data = Bytes::from("test-content");
        let file_path = Path::from_absolute_path(tmp.path().join("test.txt")).unwrap();
        let write_time = current_time_duration().unwrap();
        store.put(&file_path, data.clone().into()).await.unwrap();

        let file_url = Url::from_file_path(tmp.path().join("test.txt")).unwrap();
        let file_meta = handler.head(&file_url).unwrap();

        assert_eq!(file_meta.location, file_url);
        assert_eq!(file_meta.size, data.len() as u64);

        // Verify timestamp is within the expected range
        let meta_time = Duration::from_millis(file_meta.last_modified as u64);
        assert!(
            meta_time.abs_diff(write_time) < Duration::from_millis(100),
            "last_modified timestamp should be around {} ms, but was {} ms",
            write_time.as_millis(),
            meta_time.as_millis()
        );
    }

    #[tokio::test]
    async fn test_head_non_existent() {
        let (tmp, _store, handler) = setup_test();

        let missing_url = Url::from_file_path(tmp.path().join("missing.txt")).unwrap();
        let result = handler.head(&missing_url);

        assert!(matches!(result, Err(Error::FileNotFound(_))));
    }

    #[test]
    fn test_put() {
        let (tmp, _store, handler) = setup_test();

        let data = Bytes::from("put-test-data");
        let file_url = Url::from_file_path(tmp.path().join("put.txt")).unwrap();
        handler.put(&file_url, data.clone(), false).unwrap();

        // Read back via read_files and verify content
        let read_back: Vec<Bytes> = handler
            .read_files(vec![(file_url, None)])
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0], data);
    }

    #[test]
    fn test_put_already_exists() {
        let (tmp, _store, handler) = setup_test();

        let data = Bytes::from("original");
        let file_url = Url::from_file_path(tmp.path().join("put.txt")).unwrap();
        handler.put(&file_url, data, false).unwrap();

        // Second put with overwrite=false should fail
        let new_data = Bytes::from("updated");
        assert!(matches!(
            handler.put(&file_url, new_data.clone(), false),
            Err(Error::FileAlreadyExists(_))
        ));

        // Put with overwrite=true should succeed
        handler.put(&file_url, new_data.clone(), true).unwrap();

        // Verify the content was overwritten
        let read_back: Vec<Bytes> = handler
            .read_files(vec![(file_url, None)])
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0], new_data);
    }

    #[test]
    fn test_delete() {
        let (tmp, _store, handler) = setup_test();

        let data = Bytes::from("delete-test-data");
        let file_url = Url::from_file_path(tmp.path().join("delete.txt")).unwrap();
        handler.put(&file_url, data, false).unwrap();

        handler.delete(&file_url).unwrap();

        assert!(matches!(
            handler.head(&file_url),
            Err(Error::FileNotFound(_))
        ));
    }

    #[test]
    fn test_delete_nonexistent_is_ok() {
        let (tmp, _store, handler) = setup_test();

        let missing_url = Url::from_file_path(tmp.path().join("missing.txt")).unwrap();
        assert!(matches!(
            handler.head(&missing_url),
            Err(Error::FileNotFound(_))
        ));
        handler.delete(&missing_url).unwrap();
    }

    // === single-directory listing ===

    /// A [`PaginatedListStore`] over an in-memory store that reproduces real cloud (S3/GCS/Azure)
    /// semantics: raw-string prefix matching plus `/` delimiter grouping, where every key with a
    /// further `/` after the prefix rolls into a `common_prefix` and is excluded from `objects`.
    /// This differs from [`InMemory::list_with_delimiter`], which matches on `Path` segments and so
    /// would tolerate a prefix missing its trailing slash. Returns objects reverse-sorted to verify
    /// the paginated path sorts its results.
    #[derive(Debug)]
    struct ReverseSortedPaginatedStore(Arc<InMemory>);

    #[async_trait::async_trait]
    impl PaginatedListStore for ReverseSortedPaginatedStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            opts: PaginatedListOptions,
        ) -> object_store::Result<delta_kernel::object_store::list::PaginatedListResult> {
            use delta_kernel::object_store::ObjectStore as _;
            use futures::TryStreamExt as _;
            assert_eq!(opts.delimiter.as_deref(), Some("/"), "Path A must pass '/'");
            let prefix = prefix.unwrap_or("");
            let all: Vec<_> = self.0.list(None).try_collect().await?;
            let mut objects: Vec<_> = all
                .into_iter()
                .filter(|meta| {
                    let key = meta.location.as_ref();
                    // Raw-string prefix match, then exclude keys nested below a delimiter (they
                    // roll into a common prefix on real cloud stores).
                    key.strip_prefix(prefix)
                        .is_some_and(|rest| !rest.contains('/'))
                })
                .filter(|meta| {
                    opts.offset
                        .as_deref()
                        .is_none_or(|offset| meta.location.as_ref() > offset)
                })
                .collect();
            objects.sort_unstable_by(|a, b| b.location.cmp(&a.location));
            let result = delta_kernel::object_store::ListResult {
                objects,
                common_prefixes: Vec::new(),
            };
            Ok(delta_kernel::object_store::list::PaginatedListResult {
                result,
                page_token: None,
            })
        }
    }

    /// Seeds a `_delta_log/` with three commits, a checkpoint at v2, and `staged_count` staged
    /// commits under `_staged_commits/`. Returns the store and the table root URL.
    async fn seed_log_with_staged_commits(staged_count: u64) -> (Arc<InMemory>, Url) {
        let store = Arc::new(InMemory::new());
        let data = Bytes::from("x");
        for v in 0..3 {
            store
                .put(&delta_path_for_version(v, "json"), data.clone().into())
                .await
                .unwrap();
        }
        store
            .put(
                &delta_path_for_version(2, "checkpoint.parquet"),
                data.clone().into(),
            )
            .await
            .unwrap();
        for v in 0..staged_count {
            let uuid = uuid::Uuid::new_v4();
            let path = Path::from(format!("_delta_log/_staged_commits/{v:020}.{uuid}.json"));
            store.put(&path, data.clone().into()).await.unwrap();
        }
        (store, Url::parse("memory:///").unwrap())
    }

    #[rstest::rstest]
    #[case::path_a_paginated(true)]
    #[case::path_b_fallback(false)]
    #[tokio::test]
    async fn list_from_single_directory_omits_staged_commits(#[case] use_paginated: bool) {
        let (store, table_root) = seed_log_with_staged_commits(20).await;
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let paginated: Option<Arc<dyn PaginatedListStore>> =
            use_paginated.then(|| Arc::new(ReverseSortedPaginatedStore(store.clone())) as _);
        let handler = ObjectStoreStorageHandler::new(store, paginated, executor);

        let start = table_root.join("_delta_log/").unwrap().join("0").unwrap();
        let files: Vec<_> = handler.list_from(&start).unwrap().try_collect().unwrap();

        let names: Vec<_> = files
            .iter()
            .map(|f| f.location.path().to_string())
            .collect();
        // Only direct children of _delta_log/ are listed, sorted; no _staged_commits/ files.
        assert_eq!(
            names,
            vec![
                "/_delta_log/00000000000000000000.json",
                "/_delta_log/00000000000000000001.json",
                "/_delta_log/00000000000000000002.checkpoint.parquet",
                "/_delta_log/00000000000000000002.json",
            ]
        );
        assert!(
            names.iter().all(|n| !n.contains("_staged_commits")),
            "staged commits must never be listed: {names:?}"
        );
    }
}
