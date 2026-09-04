use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::object_store::list::{PaginatedListOptions, PaginatedListStore};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{self, DynObjectStore, ObjectMeta, ObjectStoreExt as _, PutMode};
use delta_kernel::{CancellationTokenRef, DeltaResult, Error, FileMeta, FileSlice, StorageHandler};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use itertools::Itertools;
use url::Url;

use crate::executor::TaskExecutor;
use crate::UrlExt;

pub struct ObjectStoreStorageHandler<E: TaskExecutor> {
    inner: Arc<DynObjectStore>,
    /// `Some` for S3/GCS/Azure (delimiter pushdown), `None` elsewhere (client-side filter).
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

/// Returns the `(prefix, offset)` to list: a trailing-`/` `path` lists itself, otherwise its
/// parent is listed after `path`.
///
/// - `s3://bucket/_delta_log/` -> (`_delta_log`, `_delta_log`)
/// - `s3://bucket/_delta_log/00000000000000000005.json` -> (`_delta_log`,
///   `_delta_log/00000000000000000005.json`)
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

/// Builds a [`FileMeta`], taking the scheme and authority from `base`.
fn file_meta(base: &Url, meta: ObjectMeta) -> FileMeta {
    let mut location = base.clone();
    location.set_path(&format!("/{}", meta.location.as_ref()));
    FileMeta {
        location,
        last_modified: meta.last_modified.timestamp_millis(),
        size: meta.size,
    }
}

/// Options for a follow-up page: continuation token only, no offset (offset is first-page only).
fn next_page_opts(token: String) -> PaginatedListOptions {
    PaginatedListOptions {
        delimiter: Some("/".into()),
        page_token: Some(token),
        ..Default::default()
    }
}

/// Sorts and drops entries at or before `offset`, for out-of-order or offset-ignoring backends.
/// Compares in the store's decoded key space, not the encoded URL path.
fn sort_and_bound(
    mut items: Vec<ObjectMeta>,
    base: &Url,
    offset: &Path,
) -> BoxStream<'static, DeltaResult<FileMeta>> {
    if !offset.as_ref().is_empty() {
        items.retain(|m| m.location.as_ref() > offset.as_ref());
    }
    items.sort_unstable_by(|a, b| a.location.cmp(&b.location));
    let base = base.clone();
    Box::pin(stream::iter(
        items.into_iter().map(move |m| Ok(file_meta(&base, m))),
    ))
}

/// Single-directory `list_from`, dispatching by whether the store supports delimiter pushdown.
async fn list_from_impl(
    store: Arc<DynObjectStore>,
    paginated: Option<Arc<dyn PaginatedListStore>>,
    path: Url,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    let (prefix, offset) = list_scope(&path)?;
    let ordered = supports_ordered_listing(&path);
    match paginated {
        Some(p) => list_one_level_paginated(p, path, prefix, offset, ordered).await,
        None => list_one_level_delimited(store, path, prefix, offset).await,
    }
}

/// Fallback for stores without a [`PaginatedListStore`] handle. `list_with_delimiter` is one level
/// but takes no offset, so bound and sort client-side.
async fn list_one_level_delimited(
    store: Arc<DynObjectStore>,
    base_url: Url,
    prefix: Path,
    offset: Path,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    let result = store.list_with_delimiter(Some(&prefix)).await?;
    Ok(sort_and_bound(result.objects, &base_url, &offset))
}

/// List one directory level via [`PaginatedListStore`]'s `/` delimiter. Ordered backends stream
/// pages lazily so a caller can stop early. S3 Express is unordered and rejects `start-after`, so
/// it gets no offset and is bounded client-side.
async fn list_one_level_paginated(
    paginated: Arc<dyn PaginatedListStore>,
    base_url: Url,
    prefix: Path,
    offset: Path,
    ordered: bool,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    // `list_paginated` needs the trailing slash. An empty prefix lists the root.
    let req_prefix = (!prefix.as_ref().is_empty()).then(|| format!("{}/", prefix.as_ref()));
    // A directory-like path has offset == prefix, which is not a lower bound worth sending.
    let req_offset = (ordered && offset != prefix).then(|| offset.to_string());

    let first_opts = PaginatedListOptions {
        offset: req_offset,
        delimiter: Some("/".into()),
        ..Default::default()
    };

    let pages = stream::try_unfold(Some(first_opts), move |opts| {
        let (paginated, req_prefix) = (paginated.clone(), req_prefix.clone());
        async move {
            let Some(opts) = opts else {
                return Ok::<_, object_store::Error>(None);
            };
            let result = paginated
                .list_paginated(req_prefix.as_deref(), opts)
                .await?;
            Ok(Some((
                result.result.objects,
                result.page_token.map(next_page_opts),
            )))
        }
    });

    if ordered {
        Ok(pages
            .map_ok(move |objects| {
                let base_url = base_url.clone();
                stream::iter(
                    objects
                        .into_iter()
                        .map(move |m| Ok::<_, object_store::Error>(file_meta(&base_url, m))),
                )
            })
            .try_flatten()
            .err_into()
            .boxed())
    } else {
        let objects: Vec<ObjectMeta> = pages
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(sort_and_bound(objects, &base_url, &offset))
    }
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
            // File URLs need OS path conversion. Other schemes need object-store URL decoding so
            // already escaped path segments do not get escaped again.
            let path = if url.scheme() == "file" {
                let file_path = url
                    .to_file_path()
                    .map_err(|_| Error::InvalidTableLocation(format!("Invalid file URL: {url}")))?;
                Path::from_absolute_path(file_path)
                    .map_err(|e| Error::InvalidTableLocation(format!("Invalid file path: {e}")))?
            } else {
                Path::from_url_path(url.path())?
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
        self.list_from_with_cancellation(path, None)
    }

    fn list_from_with_cancellation(
        &self,
        path: &Url,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let future = list_from_impl(self.inner.clone(), self.paginated.clone(), path.clone());
        let iter = super::stream_future_to_cancellable_iter(
            self.task_executor.clone(),
            future,
            cancellation_token,
        )?;
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
        self.read_files_with_cancellation(files, None)
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let future = read_files_impl(self.inner.clone(), files, self.readahead);
        let iter = super::stream_future_to_cancellable_iter(
            self.task_executor.clone(),
            future,
            cancellation_token,
        )?;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use delta_kernel::object_store::list::{
        PaginatedListOptions, PaginatedListResult, PaginatedListStore,
    };
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::{ListResult, ObjectMeta, ObjectStore, PutPayload};
    use delta_kernel::Engine as _;
    use delta_kernel_default_engine_test_utils::current_time_duration;
    use itertools::Itertools;
    use test_utils::delta_path_for_version;

    use super::*;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::storage::EngineStore;
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
    async fn read_files_decodes_non_file_url_paths_once() {
        let store = Arc::new(InMemory::new());

        let data = Bytes::from("kernel-data");
        store
            .put(&Path::from("hello, world!"), data.clone().into())
            .await
            .unwrap();

        let engine = DefaultEngineBuilder::new(EngineStore::plain(store)).build();
        let file_url = Url::parse("memory:///hello%2C%20world%21").unwrap();

        let read_back: Vec<Bytes> = engine
            .storage_handler()
            .read_files(vec![(file_url, None)])
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(read_back, vec![data]);
    }

    #[tokio::test]
    async fn test_file_meta_is_correct() {
        let store = Arc::new(InMemory::new());

        let begin_time = current_time_duration().unwrap();

        let data = Bytes::from("kernel-data");
        let name = delta_path_for_version(1, "json");
        store.put(&name, data.clone().into()).await.unwrap();

        let table_root = Url::parse("memory:///").expect("valid url");
        let engine = DefaultEngineBuilder::new(EngineStore::plain(store)).build();
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
        let engine = DefaultEngineBuilder::new(EngineStore::plain(store)).build();
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
    /// [`PaginatedListStore`] over [`InMemory`] mimicking cloud `list_paginated`: `/` grouping,
    /// `offset` start-after, one `page_size` chunk per call. `reverse` lists descending (exercises
    /// the sort path). `honors_offset = false` models S3 Express dropping `start-after`.
    /// `fail_after = Some(n)` errors on page `n`.
    struct MockPaginatedStore {
        inner: Arc<InMemory>,
        page_size: usize,
        reverse: bool,
        honors_offset: bool,
        fail_after: Option<usize>,
        pages_fetched: Arc<AtomicUsize>,
    }

    impl MockPaginatedStore {
        fn new(inner: Arc<InMemory>, page_size: usize, reverse: bool) -> Self {
            Self {
                inner,
                page_size,
                reverse,
                honors_offset: true,
                fail_after: None,
                pages_fetched: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn ignoring_offset(mut self) -> Self {
            self.honors_offset = false;
            self
        }

        fn failing_after(mut self, pages: usize) -> Self {
            self.fail_after = Some(pages);
            self
        }
    }

    #[async_trait::async_trait]
    impl PaginatedListStore for MockPaginatedStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            opts: PaginatedListOptions,
        ) -> delta_kernel::object_store::Result<PaginatedListResult> {
            assert_eq!(
                opts.delimiter.as_deref(),
                Some("/"),
                "expected `/` delimiter"
            );
            let fetched = self.pages_fetched.fetch_add(1, Ordering::Relaxed);
            if self.fail_after == Some(fetched) {
                return Err(delta_kernel::object_store::Error::Generic {
                    store: "MockPaginatedStore",
                    source: "injected mid-stream failure".into(),
                });
            }
            let prefix = prefix.unwrap_or("");

            // Direct children of the prefix, in key order. Offset-independent so the page cursor
            // stays valid across pages (the offset only arrives on page one).
            let mut objects: Vec<ObjectMeta> = self
                .inner
                .list(None)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|m| m.unwrap())
                .filter(|m| {
                    m.location
                        .as_ref()
                        .strip_prefix(prefix)
                        .is_some_and(|rest| !rest.contains('/'))
                })
                .collect();
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            if self.reverse {
                objects.reverse();
            }

            // Page one `page_size` chunk per call, keyed by an integer start cursor. A first-page
            // offset advances the cursor past every key at or before it (start-after is exclusive).
            let mut start: usize = opts
                .page_token
                .as_deref()
                .map(|t| t.parse().unwrap())
                .unwrap_or(0);
            if self.honors_offset {
                if let Some(offset) = &opts.offset {
                    start = objects.partition_point(|m| m.location.as_ref() <= offset.as_str());
                }
            }
            let end = (start + self.page_size).min(objects.len());
            let page: Vec<ObjectMeta> = objects[start..end].to_vec();
            let page_token = (end < objects.len()).then(|| end.to_string());
            Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: Vec::new(),
                    objects: page,
                },
                page_token,
            })
        }
    }

    async fn put_key(store: &InMemory, key: &str) {
        store
            .put(&Path::from(key), PutPayload::from_static(b"x"))
            .await
            .unwrap();
    }

    fn collect_names<E: TaskExecutor>(
        handler: &ObjectStoreStorageHandler<E>,
        url: &str,
    ) -> Vec<String> {
        handler
            .list_from(&Url::parse(url).unwrap())
            .unwrap()
            .map(|m| m.unwrap().location.path().to_string())
            .collect()
    }

    /// Commits 0-2, a checkpoint at 2, and `staged` commits under `_staged_commits/`.
    async fn seed_log(store: &InMemory, staged: usize) {
        for v in 0..3 {
            put_key(store, &format!("_delta_log/{v:020}.json")).await;
        }
        put_key(store, "_delta_log/00000000000000000002.checkpoint.parquet").await;
        for v in 0..staged {
            put_key(
                store,
                &format!("_delta_log/_staged_commits/{v:020}.{v}-uuid.json"),
            )
            .await;
        }
    }

    /// Both paths return only the direct children of `_delta_log/`, not the staged commits.
    #[rstest::rstest]
    #[case::path_a_paginated(true)]
    #[case::path_b_fallback(false)]
    #[tokio::test]
    async fn list_from_single_directory_omits_staged_commits(#[case] paginated: bool) {
        let store = Arc::new(InMemory::new());
        seed_log(&store, 20).await;

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let paginated: Option<Arc<dyn PaginatedListStore>> =
            paginated.then(|| Arc::new(MockPaginatedStore::new(store.clone(), 100, false)) as _);
        let handler = ObjectStoreStorageHandler::new(store.clone(), paginated, executor);

        // `s3://` is an ordered backend.
        let names = collect_names(&handler, "s3://bucket/_delta_log/0");

        assert!(
            names.iter().all(|n| !n.contains("_staged_commits")),
            "single-directory listing must exclude staged commits: {names:?}"
        );
        assert_eq!(
            names.len(),
            4,
            "expected 3 commits + 1 checkpoint: {names:?}"
        );
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "results must be sorted");
    }

    /// An unordered backend sorts, so descending input still comes back sorted.
    #[tokio::test]
    async fn list_from_paginated_unordered_sorts() {
        let store = Arc::new(InMemory::new());
        seed_log(&store, 5).await;
        let mock = Arc::new(MockPaginatedStore::new(store.clone(), 2, true));

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), Some(mock as _), executor);

        // `--x-s3` is an S3 Express (unordered) bucket.
        let names = collect_names(&handler, "s3://bucket--x-s3/_delta_log/0");

        assert_eq!(names.len(), 4);
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(
            names, sorted,
            "unordered results must be sorted by the caller"
        );
    }

    /// The lazy paginated path fetches only the pages a caller consumes.
    #[tokio::test]
    async fn list_from_paginated_ordered_is_lazy() {
        let store = Arc::new(InMemory::new());
        for v in 0..50 {
            put_key(&store, &format!("_delta_log/{v:020}.json")).await;
        }
        let mock = Arc::new(MockPaginatedStore::new(store.clone(), 5, false));
        let pages_fetched = mock.pages_fetched.clone();

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), Some(mock as _), executor);

        let start = Url::parse("s3://bucket/_delta_log/0").unwrap();
        let first_three: Vec<_> = handler.list_from(&start).unwrap().take(3).collect();

        assert_eq!(first_three.len(), 3);
        // Taking 3 from 5-per-page fetches only the first page.
        assert_eq!(pages_fetched.load(Ordering::Relaxed), 1);
    }

    /// The ordered path threads the page token across many pages, in global order, with no dropped
    /// or duplicated entries.
    #[tokio::test]
    async fn list_from_paginated_ordered_threads_pages_in_order() {
        let store = Arc::new(InMemory::new());
        for v in 0..50 {
            put_key(&store, &format!("_delta_log/{v:020}.json")).await;
        }
        let mock = Arc::new(MockPaginatedStore::new(store.clone(), 5, false)); // 10 pages
        let pages_fetched = mock.pages_fetched.clone();
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), Some(mock as _), executor);

        let names = collect_names(&handler, "s3://bucket/_delta_log/0");

        assert_eq!(
            names.len(),
            50,
            "every direct child returned across all pages"
        );
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(
            names, sorted,
            "ordered path stays sorted across page boundaries"
        );
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(
            unique.len(),
            50,
            "no dropped or duplicated entries at boundaries"
        );
        assert!(
            pages_fetched.load(Ordering::Relaxed) >= 10,
            "continuation threaded"
        );
    }

    /// A first-page offset that excludes entries must stay bounded across page boundaries.
    #[tokio::test]
    async fn list_from_paginated_ordered_bounds_offset_across_pages() {
        let store = Arc::new(InMemory::new());
        for v in 0..20 {
            put_key(&store, &format!("_delta_log/{v:020}.json")).await;
        }
        let mock = Arc::new(MockPaginatedStore::new(store.clone(), 5, false));

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), Some(mock as _), executor);

        // Listing after version 4 spans multiple pages. Versions 0-4 must never appear.
        let names = collect_names(&handler, "s3://bucket/_delta_log/00000000000000000004.json");

        assert_eq!(names.len(), 15, "only versions 5-19: {names:?}");
        assert!(names
            .iter()
            .all(|n| n.as_str() > "/_delta_log/00000000000000000004.json"));
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    /// An S3 Express bucket ignores `start-after`, so the offset must be enforced client-side.
    #[tokio::test]
    async fn list_from_paginated_s3_express_bounds_offset_client_side() {
        let store = Arc::new(InMemory::new());
        for v in 0..6 {
            put_key(&store, &format!("_delta_log/{v:020}.json")).await;
        }
        // Unordered, and ignores the offset the engine sends.
        let mock = Arc::new(MockPaginatedStore::new(store.clone(), 2, true).ignoring_offset());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), Some(mock as _), executor);

        // Listing after version 2 must still exclude versions 0-2.
        let names = collect_names(
            &handler,
            "s3://bucket--x-s3/_delta_log/00000000000000000002.json",
        );

        assert_eq!(
            names.len(),
            3,
            "only versions 3-5 are after the offset: {names:?}"
        );
        assert!(names
            .iter()
            .all(|n| n.as_str() > "/_delta_log/00000000000000000002.json"));
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    /// A failure on any page surfaces as an error, not a silently truncated listing.
    #[rstest::rstest]
    #[case::ordered("s3://bucket/_delta_log/0", false)]
    #[case::unordered("s3://bucket--x-s3/_delta_log/0", true)]
    #[tokio::test]
    async fn list_from_paginated_surfaces_mid_stream_error(
        #[case] url: &str,
        #[case] reverse: bool,
    ) {
        let store = Arc::new(InMemory::new());
        for v in 0..20 {
            put_key(&store, &format!("_delta_log/{v:020}.json")).await;
        }
        // page_size 5, fail on the 2nd page (index 1).
        let mock = Arc::new(MockPaginatedStore::new(store.clone(), 5, reverse).failing_after(1));
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), Some(mock as _), executor);

        let outcome = handler.list_from(&Url::parse(url).unwrap());
        let saw_err = match outcome {
            Err(_) => true,
            Ok(iter) => iter.into_iter().any(|r| r.is_err()),
        };
        assert!(
            saw_err,
            "a mid-stream page failure must surface, not silently truncate"
        );
    }

    /// Listing the root directory (empty prefix) via the delimited fallback returns only top-level
    /// files, not entries nested under a subdirectory.
    #[tokio::test]
    async fn list_from_root_delimited_returns_top_level_only() {
        let store = Arc::new(InMemory::new());
        put_key(&store, "00000000000000000000.json").await;
        put_key(&store, "sub/nested.json").await;
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), None, executor);

        let names = collect_names(&handler, "memory:///");

        assert_eq!(
            names,
            vec!["/00000000000000000000.json"],
            "only top-level files: {names:?}"
        );
    }

    #[test]
    fn list_scope_rejects_authority_only_url() {
        // No path segments and not directory-like, thus no parent to list after.
        let url = Url::parse("s3://bucket").unwrap();
        assert!(matches!(
            list_scope(&url),
            Err(Error::Generic(message))
                if message == "Offset path must not be a root directory. Got: 's3://bucket'"
        ));
    }
    // The cancellation-aware overrides feed the racing helper, so an already-cancelled token stops
    // the operation instead of performing I/O.
    #[test]
    fn precancelled_token_short_circuits_list_and_read() {
        let (tempdir, _store, handler) = setup_test();
        let url = Url::from_directory_path(tempdir.path()).unwrap();
        let token: CancellationTokenRef = Arc::new(test_utils::TestCancellationToken::cancelled());

        let listed = handler.list_from_with_cancellation(&url, Some(token.clone()));
        assert!(matches!(listed, Err(Error::Cancelled)));

        let read = handler.read_files_with_cancellation(vec![(url, None)], Some(token));
        assert!(matches!(read, Err(Error::Cancelled)));
    }
}
