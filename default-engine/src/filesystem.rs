use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::object_store::list::{PaginatedListOptions, PaginatedListStore};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{self, DynObjectStore, ObjectMeta, ObjectStoreExt as _, PutMode};
use delta_kernel::{
    CancellationTokenRef, DeltaResult, DeltaResultIteratorStatic, Error, FileMeta, FileSlice,
    StorageHandler,
};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use itertools::Itertools;
use url::Url;

use crate::executor::TaskExecutor;
use crate::storage::PaginatedListing;
use crate::UrlExt;

pub struct ObjectStoreStorageHandler<E: TaskExecutor> {
    inner: Arc<DynObjectStore>,
    paginated: Option<PaginatedListing>,
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
        paginated: Option<PaginatedListing>,
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

/// Native async implementation for list_from.
///
/// Storage metrics are emitted by the outer [`MeteredStorageHandler`] wrapping this
/// handler (e.g. inside `DefaultEngine`'s `storage_handler()`), so this function just
/// returns the raw stream.
///
/// [`MeteredStorageHandler`]: delta_kernel::metrics::MeteredStorageHandler
async fn list_from_impl(
    store: Arc<DynObjectStore>,
    paginated: Option<PaginatedListing>,
    path: Url,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    // The offset is used for list-after; the prefix is used to restrict the listing to a specific
    // directory. Unfortunately, `Path` provides no easy way to check whether a name is
    // directory-like, because it strips trailing /, so we're reduced to manually checking the
    // original URL.
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

    if let Some(paginated) = paginated {
        return list_paginated(paginated.store, path, prefix, offset, paginated.ordered).await;
    }

    let has_ordered_listing = supports_ordered_listing(&path);

    // `list_with_offset` lets capable stores push down the offset but recursively lists
    // descendants.
    let stream = store
        .list_with_offset(Some(&prefix), &offset)
        // Filtering descendants hides them from Kernel's lexical stopping rule. Without an
        // earlier direct entry such as _last_checkpoint, reaching the end can consume the entire
        // _staged_commits or _sidecars tail. Sorted paths can interleave direct children and
        // descendants: a, b/x, c. Encountering b/x does not mean all direct children were listed.
        .try_filter(move |meta| {
            futures::future::ready(
                meta.location
                    .prefix_match(&prefix)
                    .is_some_and(|parts| parts.count() == 1),
            )
        })
        .map(move |meta| {
            let meta = meta?;
            let mut location = path.clone();
            location.set_path(&format!("/{}", meta.location.as_ref()));
            Ok(FileMeta {
                location,
                last_modified: meta.last_modified.timestamp_millis(),
                size: meta.size,
            })
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

/// Lists one directory level, pushing the offset only for globally ordered stores.
async fn list_paginated(
    store: Arc<dyn PaginatedListStore>,
    base_url: Url,
    prefix: Path,
    offset: Path,
    ordered: bool,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    let request_prefix = (!prefix.as_ref().is_empty()).then(|| format!("{}/", prefix.as_ref()));
    let request_offset = (ordered && offset != prefix).then(|| offset.to_string());
    let first_options = PaginatedListOptions {
        offset: request_offset,
        delimiter: Some("/".into()),
        ..Default::default()
    };

    let pages: BoxStream<'static, object_store::Result<Vec<ObjectMeta>>> =
        stream::try_unfold(Some(first_options), move |options| {
            let store = store.clone();
            let request_prefix = request_prefix.clone();
            async move {
                let Some(options) = options else {
                    return Ok::<_, object_store::Error>(None);
                };
                let result = store
                    .list_paginated(request_prefix.as_deref(), options.clone())
                    .await?;
                let next_options = result.page_token.map(|page_token| PaginatedListOptions {
                    page_token: Some(page_token),
                    offset: None,
                    ..options
                });
                Ok(Some((result.result.objects, next_options)))
            }
        })
        .boxed();

    let filtered_pages = pages.map_ok(move |objects| {
        let base_url = base_url.clone();
        let offset = offset.clone();
        stream::iter(
            objects
                .into_iter()
                .filter(move |meta| meta.location.as_ref() > offset.as_ref())
                .map(move |meta| Ok::<_, object_store::Error>(file_meta(&base_url, meta))),
        )
    });
    let stream = filtered_pages
        .try_flatten()
        .err_into::<delta_kernel::Error>();

    if ordered {
        Ok(stream.boxed())
    } else {
        let mut items: Vec<_> = stream.try_collect().await?;
        items.sort_unstable();
        Ok(stream::iter(items.into_iter().map(Ok)).boxed())
    }
}

fn file_meta(base_url: &Url, meta: ObjectMeta) -> FileMeta {
    let mut location = base_url.clone();
    location.set_path(&format!("/{}", meta.location.as_ref()));
    FileMeta {
        location,
        last_modified: meta.last_modified.timestamp_millis(),
        size: meta.size,
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
    fn list_from(&self, path: &Url) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
        self.list_from_with_cancellation(path, None)
    }

    fn list_from_with_cancellation(
        &self,
        path: &Url,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
        let future = list_from_impl(self.inner.clone(), self.paginated.clone(), path.clone());
        let iter = super::stream_future_to_cancellable_iter(
            self.task_executor.clone(),
            future,
            cancellation_token,
        )?;
        Ok(iter)
    }

    /// Read data specified by the start and end offset from the file.
    ///
    /// This will return the data in the same order as the provided file slices.
    ///
    /// Multiple reads may occur in parallel, depending on the configured readahead.
    /// See [`Self::with_readahead`].
    fn read_files(&self, files: Vec<FileSlice>) -> DeltaResult<DeltaResultIteratorStatic<Bytes>> {
        self.read_files_with_cancellation(files, None)
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<DeltaResultIteratorStatic<Bytes>> {
        let future = read_files_impl(self.inner.clone(), files, self.readahead);
        let iter = super::stream_future_to_cancellable_iter(
            self.task_executor.clone(),
            future,
            cancellation_token,
        )?;
        Ok(iter)
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
pub(crate) fn supports_ordered_listing(url: &Url) -> bool {
    let path_style_bucket =
        if url.scheme() == "https" && url.host_str().is_some_and(|host| host.starts_with("s3.")) {
            url.path_segments().and_then(|mut segments| segments.next())
        } else {
            None
        };
    !((url.scheme() == "file")
        // S3 Directory Buckets
        || url.domain().map(|d| d.contains("--x-s3")).unwrap_or(false)
        // S3 Directory Bucket Access Points
        || url.domain().map(|d| d.contains("-xa-s3")).unwrap_or(false)
        || path_style_bucket.is_some_and(|bucket| bucket.contains("--x-s3") || bucket.contains("-xa-s3")))
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use delta_kernel::log_segment_files::list_delta_log_from_storage;
    use delta_kernel::object_store::list::PaginatedListResult;
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use delta_kernel::Engine as _;
    use delta_kernel_default_engine_test_utils::current_time_duration;
    use itertools::Itertools;
    use rstest::rstest;
    use test_utils::delta_path_for_version;
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::storage::EngineStore;
    use crate::DefaultEngineBuilder;

    #[derive(Debug)]
    struct RecordingOffsetStore {
        inner: InMemory,
        list_requests: Mutex<Vec<(Option<Path>, Path)>>,
        upstream_pulls: Arc<AtomicUsize>,
        pages: Vec<Vec<&'static str>>,
        page_requests: AtomicUsize,
    }

    impl RecordingOffsetStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                list_requests: Mutex::new(Vec::new()),
                upstream_pulls: Arc::new(AtomicUsize::new(0)),
                pages: Vec::new(),
                page_requests: AtomicUsize::new(0),
            }
        }
    }

    #[derive(Debug, PartialEq)]
    struct PaginatedRequest {
        prefix: Option<String>,
        offset: Option<String>,
        delimiter: Option<String>,
        page_token: Option<String>,
    }

    #[derive(Debug, Default)]
    struct UnorderedPaginatedStore {
        requests: Mutex<Vec<PaginatedRequest>>,
    }

    #[async_trait::async_trait]
    impl PaginatedListStore for UnorderedPaginatedStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> object_store::Result<PaginatedListResult> {
            self.requests.lock().unwrap().push(PaginatedRequest {
                prefix: prefix.map(ToOwned::to_owned),
                offset: options.offset.clone(),
                delimiter: options.delimiter.as_deref().map(ToOwned::to_owned),
                page_token: options.page_token.clone(),
            });
            let (locations, page_token) = match options.page_token.as_deref() {
                None => (
                    vec![
                        "table/_delta_log/00000000000000000012.json",
                        "table/_delta_log/00000000000000000010.json",
                        "table/_delta_log/00000000000000000009.json",
                    ],
                    Some("second-page".to_string()),
                ),
                Some("second-page") => (vec!["table/_delta_log/00000000000000000011.json"], None),
                Some(token) => panic!("Unexpected page token: {token}"),
            };
            let objects = locations
                .into_iter()
                .map(|location| ObjectMeta {
                    location: Path::from(location),
                    last_modified: chrono::Utc::now(),
                    size: 1,
                    e_tag: None,
                    version: None,
                })
                .collect();
            Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: vec![Path::from("table/_delta_log/_sidecars")],
                    objects,
                },
                page_token,
            })
        }
    }

    impl std::fmt::Display for RecordingOffsetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingOffsetStore")
        }
    }

    #[async_trait::async_trait]
    impl PaginatedListStore for RecordingOffsetStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> object_store::Result<PaginatedListResult> {
            assert_eq!(prefix, Some("_delta_log/"));
            assert_eq!(options.delimiter.as_deref(), Some("/"));
            assert!(options.offset.is_none());
            self.page_requests.fetch_add(1, Ordering::Relaxed);
            let index = options
                .page_token
                .as_deref()
                .map(|token| token.parse::<usize>().unwrap())
                .unwrap_or(0);
            let mut objects = Vec::new();
            for key in &self.pages[index] {
                objects.push(self.inner.head(&Path::from(*key)).await?);
            }
            Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: Vec::new(),
                    objects,
                },
                page_token: (index + 1 < self.pages.len()).then(|| (index + 1).to_string()),
            })
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RecordingOffsetStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.list_requests
                .lock()
                .unwrap()
                .push((prefix.cloned(), offset.clone()));
            let upstream_pulls = self.upstream_pulls.clone();
            self.inner
                .list_with_offset(prefix, offset)
                .inspect(move |_| {
                    upstream_pulls.fetch_add(1, Ordering::Relaxed);
                })
                .boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

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

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, None, executor);
        let file_url = Url::parse("memory:///hello%2C%20world%21").unwrap();

        let read_back: Vec<Bytes> = storage
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
    async fn list_from_applies_offset_and_excludes_nested_files() {
        let store = Arc::new(RecordingOffsetStore::new());
        for key in [
            "_delta_log/00000000000000000000.json",
            "_delta_log/00000000000000000001.json",
            "_delta_log/00000000000000000001.json/child",
            "_delta_log/00000000000000000002.json",
            "_delta_log/_staged_commits/00000000000000000003.uuid.json",
            "_delta_log/z.txt",
        ] {
            store
                .put(&Path::from(key), Bytes::from_static(b"x").into())
                .await
                .unwrap();
        }

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), None, executor);
        let start = Url::parse("memory:///_delta_log/00000000000000000001.json").unwrap();

        let locations: Vec<_> = handler
            .list_from(&start)
            .unwrap()
            .map(|result| result.unwrap().location.path().to_string())
            .collect();

        assert_eq!(
            locations,
            vec!["/_delta_log/00000000000000000002.json", "/_delta_log/z.txt"]
        );
        assert_eq!(
            *store.list_requests.lock().unwrap(),
            vec![(
                Some(Path::from("_delta_log")),
                Path::from("_delta_log/00000000000000000001.json")
            )]
        );
    }

    #[rstest]
    #[case::without_hint(false, 1003)]
    #[case::with_hint(true, 4)]
    #[tokio::test]
    async fn fallback_log_discovery_consumption_depends_on_direct_stopping_entry(
        #[case] include_hint: bool,
        #[case] expected_pulls: usize,
    ) {
        let store = Arc::new(RecordingOffsetStore::new());
        let mut keys: Vec<_> = (0..3)
            .map(|version| format!("_delta_log/{version:020}.json"))
            .collect();
        keys.extend(
            (0..1000).map(|version| format!("_delta_log/_staged_commits/{version:020}.uuid.json")),
        );
        if include_hint {
            keys.push("_delta_log/_last_checkpoint".into());
        }
        for key in keys {
            store
                .put(&Path::from(key), Bytes::from_static(b"x").into())
                .await
                .unwrap();
        }
        let engine = DefaultEngineBuilder::new(store.clone()).build();
        let storage = engine.storage_handler();
        let log_root = Url::parse("s3://bucket/_delta_log/").unwrap();
        let versions: Vec<_> =
            list_delta_log_from_storage(storage.as_ref(), &log_root, 0, u64::MAX, None)
                .unwrap()
                .map(|result| result.unwrap().version)
                .collect();
        assert_eq!(versions, vec![0, 1, 2]);
        assert_eq!(store.upstream_pulls.load(Ordering::Relaxed), expected_pulls);
        assert_eq!(store.page_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn custom_paginated_store_sorts_across_pages_before_kernel_stops_listing() {
        let store = Arc::new(RecordingOffsetStore {
            pages: vec![
                vec![
                    "_delta_log/00000000000000000000.json",
                    "_delta_log/_last_checkpoint",
                ],
                vec!["_delta_log/00000000000000000001.json"],
            ],
            ..RecordingOffsetStore::new()
        });
        for key in store.pages.iter().flatten() {
            store
                .put(&Path::from(*key), Bytes::from_static(b"x").into())
                .await
                .unwrap();
        }
        let engine = DefaultEngineBuilder::new(EngineStore::from_paginated(store.clone())).build();
        let storage = engine.storage_handler();
        let log_root = Url::parse("s3://bucket/_delta_log/").unwrap();
        let versions: Vec<_> =
            list_delta_log_from_storage(storage.as_ref(), &log_root, 0, u64::MAX, None)
                .unwrap()
                .map(|result| result.unwrap().version)
                .collect();
        assert_eq!(versions, vec![0, 1]);
        assert_eq!(store.page_requests.load(Ordering::Relaxed), 2);
    }

    #[rstest]
    #[case::ordinary_bucket("bucket")]
    #[case::directory_bucket("bucket--usw2-az1--x-s3")]
    #[case::directory_bucket_access_point("access-point-usw2-az1-xa-s3")]
    #[tokio::test]
    async fn unordered_paginated_listing_filters_offset_and_sorts_all_pages(#[case] bucket: &str) {
        let paginated = Arc::new(UnorderedPaginatedStore::default());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(
            Arc::new(InMemory::new()),
            Some(PaginatedListing {
                store: paginated.clone(),
                ordered: false,
            }),
            executor,
        );
        let start = Url::parse(&format!(
            "s3://{bucket}/table/_delta_log/00000000000000000010.json"
        ))
        .unwrap();

        let locations: Vec<_> = handler
            .list_from(&start)
            .unwrap()
            .map(|result| result.unwrap().location.path().to_string())
            .collect();

        assert_eq!(
            locations,
            vec![
                "/table/_delta_log/00000000000000000011.json",
                "/table/_delta_log/00000000000000000012.json",
            ]
        );
        assert_eq!(
            *paginated.requests.lock().unwrap(),
            vec![
                PaginatedRequest {
                    prefix: Some("table/_delta_log/".to_string()),
                    offset: None,
                    delimiter: Some("/".to_string()),
                    page_token: None,
                },
                PaginatedRequest {
                    prefix: Some("table/_delta_log/".to_string()),
                    offset: None,
                    delimiter: Some("/".to_string()),
                    page_token: Some("second-page".to_string()),
                },
            ]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn azure_listing_pushes_directory_and_offset_and_remains_lazy(
        #[values(true, false)] use_url_factory: bool,
    ) {
        const OFFSET: &str = "table/_delta_log/00000000000000000010.json";
        const NEXT: &str = "table/_delta_log/00000000000000000011.json";
        const NEXT_PAGE: &str = "table/_delta_log/00000000000000000012.json";

        let server = MockServer::start().await;
        let body = format!(
            "<EnumerationResults><Blobs>\
             <Blob><Name>table/_delta_log/00000000000000000009.json</Name><Properties>\
             <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>\
             <Content-Length>1</Content-Length><Content-Type>application/json</Content-Type>\
             </Properties></Blob>\
             <Blob><Name>{OFFSET}</Name><Properties>\
             <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>\
             <Content-Length>1</Content-Length><Content-Type>application/json</Content-Type>\
             </Properties></Blob>\
             <Blob><Name>{NEXT}</Name><Properties>\
             <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>\
             <Content-Length>1</Content-Length><Content-Type>application/json</Content-Type>\
             </Properties></Blob>\
             <BlobPrefix><Name>table/_delta_log/_staged_commits/</Name></BlobPrefix>\
             </Blobs><NextMarker>page-2</NextMarker></EnumerationResults>"
        );
        Mock::given(method("GET"))
            .and(path("/container"))
            .and(query_param("prefix", "table/_delta_log/"))
            .and(query_param("delimiter", "/"))
            .and(query_param("startFrom", OFFSET))
            .and(query_param_is_missing("marker"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/xml"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/container"))
            .and(query_param("prefix", "table/_delta_log/"))
            .and(query_param("delimiter", "/"))
            .and(query_param("marker", "page-2"))
            .and(query_param_is_missing("startFrom"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                format!(
                    "<EnumerationResults><Blobs>\
                     <Blob><Name>{NEXT_PAGE}</Name><Properties>\
                     <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>\
                     <Content-Length>1</Content-Length><Content-Type>application/json</Content-Type>\
                     </Properties></Blob>\
                     </Blobs></EnumerationResults>"
                ),
                "application/xml",
            ))
            .expect(1)
            .mount(&server)
            .await;

        let table_url =
            Url::parse("abfss://container@account.dfs.core.windows.net/table/").unwrap();
        let store = if use_url_factory {
            let options = vec![
                ("endpoint", server.uri()),
                ("allow_http", "true".to_string()),
                ("skip_signature", "true".to_string()),
            ];
            EngineStore::from_url_opts(&table_url, options).unwrap()
        } else {
            EngineStore::from_ordered_paginated(Arc::new(
                object_store::azure::MicrosoftAzureBuilder::new()
                    .with_url(table_url.as_str())
                    .with_endpoint(server.uri())
                    .with_allow_http(true)
                    .with_skip_signature(true)
                    .build()
                    .unwrap(),
            ))
        };
        let engine = DefaultEngineBuilder::new(store).build();
        let start = table_url
            .join("_delta_log/00000000000000000010.json")
            .unwrap();
        let mut files = engine.storage_handler().list_from(&start).unwrap();

        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(
            files.next().unwrap().unwrap().location,
            table_url.join(&format!("/{NEXT}")).unwrap()
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        assert_eq!(
            files.next().unwrap().unwrap().location,
            table_url.join(&format!("/{NEXT_PAGE}")).unwrap()
        );
        assert!(files.next().is_none());
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
        server.verify().await;
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
