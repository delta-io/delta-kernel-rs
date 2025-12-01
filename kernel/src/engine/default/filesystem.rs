use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use delta_kernel_derive::internal_api;
use futures::stream::StreamExt;
use itertools::Itertools;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore, PutMode};
use url::Url;

use super::UrlExt;
use crate::engine::default::executor::TaskExecutor;
use crate::metrics::{MetricEvent, MetricsReporter};
use crate::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};

/// Iterator wrapper that emits metrics when exhausted
///
/// Generic over the inner iterator type and item type.
/// The `event_fn` receives (duration, num_files, bytes_read) to construct the appropriate MetricEvent.
/// Metrics are emitted either when the iterator is exhausted or when dropped.
struct MetricsIterator<I, T>
where
    I: Iterator<Item = DeltaResult<T>>,
{
    inner: I,
    reporter: Option<Arc<dyn MetricsReporter>>,
    start: Instant,
    num_files: u64,
    bytes_read: u64,
    metrics_emitted: bool,
    event_fn: fn(Duration, u64, u64) -> MetricEvent,
}

impl<I, T> MetricsIterator<I, T>
where
    I: Iterator<Item = DeltaResult<T>>,
{
    fn new(
        inner: I,
        reporter: Option<Arc<dyn MetricsReporter>>,
        start: Instant,
        event_fn: fn(Duration, u64, u64) -> MetricEvent,
    ) -> Self {
        Self {
            inner,
            reporter,
            start,
            num_files: 0,
            bytes_read: 0,
            metrics_emitted: false,
            event_fn,
        }
    }

    fn emit_metrics_once(&mut self) {
        if !self.metrics_emitted {
            self.reporter.as_ref().inspect(|r| {
                r.report((self.event_fn)(
                    self.start.elapsed(),
                    self.num_files,
                    self.bytes_read,
                ));
            });
            self.metrics_emitted = true;
        }
    }
}

impl<I> Iterator for MetricsIterator<I, FileMeta>
where
    I: Iterator<Item = DeltaResult<FileMeta>>,
{
    type Item = DeltaResult<FileMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(item) => {
                if item.is_ok() {
                    self.num_files += 1;
                }
                Some(item)
            }
            None => {
                self.emit_metrics_once();
                None
            }
        }
    }
}

impl<I> Iterator for MetricsIterator<I, Bytes>
where
    I: Iterator<Item = DeltaResult<Bytes>>,
{
    type Item = DeltaResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(item) => {
                if let Ok(ref bytes) = item {
                    self.num_files += 1;
                    self.bytes_read += bytes.len() as u64;
                }
                Some(item)
            }
            None => {
                self.emit_metrics_once();
                None
            }
        }
    }
}

impl<I, T> Drop for MetricsIterator<I, T>
where
    I: Iterator<Item = DeltaResult<T>>,
{
    fn drop(&mut self) {
        self.emit_metrics_once();
    }
}

#[derive(Debug)]
pub struct ObjectStoreStorageHandler<E: TaskExecutor> {
    inner: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    reporter: Option<Arc<dyn MetricsReporter>>,
    readahead: usize,
}

impl<E: TaskExecutor> ObjectStoreStorageHandler<E> {
    #[internal_api]
    pub(crate) fn new(
        store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        reporter: Option<Arc<dyn MetricsReporter>>,
    ) -> Self {
        Self {
            inner: store,
            task_executor,
            reporter,
            readahead: 10,
        }
    }

    /// Set the maximum number of files to read in parallel.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }
}

impl<E: TaskExecutor> StorageHandler for ObjectStoreStorageHandler<E> {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let start = Instant::now();
        // The offset is used for list-after; the prefix is used to restrict the listing to a specific directory.
        // Unfortunately, `Path` provides no easy way to check whether a name is directory-like,
        // because it strips trailing /, so we're reduced to manually checking the original URL.
        let offset = Path::from_url_path(path.path())?;
        let prefix = if path.path().ends_with('/') {
            offset.clone()
        } else {
            let mut parts = offset.parts().collect_vec();
            if parts.pop().is_none() {
                return Err(Error::Generic(format!(
                    "Offset path must not be a root directory. Got: '{}'",
                    path.as_str()
                )));
            }
            Path::from_iter(parts)
        };

        let store = self.inner.clone();

        // HACK to check if we're using a LocalFileSystem from ObjectStore. We need this because
        // local filesystem doesn't return a sorted list by default. Although the `object_store`
        // crate explicitly says it _does not_ return a sorted listing, in practice all the cloud
        // implementations actually do:
        // - AWS:
        //   [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
        //   states: "For general purpose buckets, ListObjectsV2 returns objects in lexicographical
        //   order based on their key names." (Directory buckets are out of scope for now)
        // - Azure: Docs state
        //   [here](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources):
        //   "A listing operation returns an XML response that contains all or part of the requested
        //   list. The operation returns entities in alphabetical order."
        // - GCP: The [main](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) doc
        //   doesn't indicate order, but [this
        //   page](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) does say: "This page
        //   shows you how to list the [objects](https://cloud.google.com/storage/docs/objects) stored
        //   in your Cloud Storage buckets, which are ordered in the list lexicographically by name."
        // So we just need to know if we're local and then if so, we sort the returned file list
        let has_ordered_listing = path.scheme() != "file";

        // This channel will become the iterator
        let (sender, receiver) = std::sync::mpsc::sync_channel(4_000);
        let url = path.clone();
        self.task_executor.spawn(async move {
            let mut stream = store.list_with_offset(Some(&prefix), &offset);

            while let Some(meta) = stream.next().await {
                match meta {
                    Ok(meta) => {
                        let mut location = url.clone();
                        location.set_path(&format!("/{}", meta.location.as_ref()));
                        sender
                            .send(Ok(FileMeta {
                                location,
                                last_modified: meta.last_modified.timestamp_millis(),
                                size: meta.size,
                            }))
                            .ok();
                    }
                    Err(e) => {
                        sender.send(Err(e.into())).ok();
                    }
                }
            }
        });

        let reporter = self.reporter.clone();

        if !has_ordered_listing {
            // This FS doesn't return things in the order we require
            let mut fms: Vec<FileMeta> = receiver.into_iter().try_collect()?;
            fms.sort_unstable();

            let num_files = fms.len() as u64;
            let storage_list_duration = start.elapsed();
            reporter.as_ref().inspect(|r| {
                r.report(MetricEvent::StorageListCompleted {
                    duration: storage_list_duration,
                    num_files,
                });
            });

            Ok(Box::new(fms.into_iter().map(Ok)))
        } else {
            Ok(Box::new(MetricsIterator::new(
                receiver.into_iter(),
                reporter,
                start,
                |duration, num_files, _bytes_read| MetricEvent::StorageListCompleted {
                    duration,
                    num_files,
                },
            )))
        }
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
        let start = Instant::now();
        let store = self.inner.clone();

        // This channel will become the output iterator.
        // Because there will already be buffering in the stream, we set the
        // buffer size to 0.
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);

        self.task_executor.spawn(
            futures::stream::iter(files)
                .map(move |(url, range)| {
                    let store = store.clone();
                    async move {
                        // Wasn't checking the scheme before calling to_file_path causing the url path to
                        // be eaten in a strange way. Now, if not a file scheme, just blindly convert to a path.
                        // https://docs.rs/url/latest/url/struct.Url.html#method.to_file_path has more
                        // details about why this check is necessary
                        let path = if url.scheme() == "file" {
                            let file_path = url.to_file_path().map_err(|_| {
                                Error::InvalidTableLocation(format!("Invalid file URL: {url}"))
                            })?;
                            Path::from_absolute_path(file_path).map_err(|e| {
                                Error::InvalidTableLocation(format!("Invalid file path: {e}"))
                            })?
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
                })
                // We allow executing up to `readahead` futures concurrently and
                // buffer the results. This allows us to achieve async concurrency
                // within a synchronous method.
                .buffered(self.readahead)
                .for_each(move |res| {
                    sender.send(res).ok();
                    futures::future::ready(())
                }),
        );

        Ok(Box::new(MetricsIterator::new(
            receiver.into_iter(),
            self.reporter.clone(),
            start,
            |duration, num_files, bytes_read| MetricEvent::StorageReadCompleted {
                duration,
                num_files,
                bytes_read,
            },
        )))
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        let start = Instant::now();
        let src_path = Path::from_url_path(src.path())?;
        let dest_path = Path::from_url_path(dest.path())?;
        let dest_path_str = dest_path.to_string();
        let store = self.inner.clone();

        // Read source file then write atomically with PutMode::Create. Note that a GET/PUT is not
        // necessarily atomic, but since the source file is immutable, we aren't exposed to the
        // possiblilty of source file changing while we do the PUT.
        let result = self.task_executor.block_on(async move {
            let data = store.get(&src_path).await?.bytes().await?;

            store
                .put_opts(&dest_path, data.into(), PutMode::Create.into())
                .await
                .map_err(|e| match e {
                    object_store::Error::AlreadyExists { .. } => {
                        Error::FileAlreadyExists(dest_path_str)
                    }
                    e => e.into(),
                })?;
            Ok(())
        });
        let copy_atomic_duration = start.elapsed();

        self.reporter.as_ref().inspect(|r| {
            r.report(MetricEvent::StorageCopyCompleted {
                duration: copy_atomic_duration,
            });
        });

        result
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        let store = self.inner.clone();
        let url = path.clone();
        let path = Path::from_url_path(path.path())?;
        self.task_executor.block_on(async move {
            store
                .head(&path)
                .await
                .map_err(Into::into)
                .map(|meta| FileMeta {
                    location: url,
                    last_modified: meta.last_modified.timestamp_millis(),
                    size: meta.size,
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::time::Duration;

    use itertools::Itertools;
    use object_store::memory::InMemory;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use test_utils::delta_path_for_version;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::utils::current_time_duration;
    use crate::Engine as _;

    use super::*;

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
        let storage = ObjectStoreStorageHandler::new(store, executor, None);

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
        let engine = DefaultEngine::new(store);
        let files: Vec<_> = engine
            .storage_handler()
            .list_from(&table_root.join("_delta_log").unwrap().join("0").unwrap())
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
        let engine = DefaultEngine::new(store);
        let files = engine
            .storage_handler()
            .list_from(&url.join("_delta_log").unwrap().join("0").unwrap())
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
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), executor, None);

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
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store.clone(), executor, None);

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
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let handler = ObjectStoreStorageHandler::new(store, executor, None);

        let missing_url = Url::from_file_path(tmp.path().join("missing.txt")).unwrap();
        let result = handler.head(&missing_url);

        assert!(matches!(result, Err(Error::FileNotFound(_))));
    }
}
