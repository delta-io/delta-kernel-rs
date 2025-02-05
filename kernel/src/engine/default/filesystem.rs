use std::sync::Arc;

use bytes::Bytes;
use futures::stream::StreamExt;
use itertools::Itertools;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use url::Url;

use crate::engine::default::executor::TaskExecutor;
use crate::{DeltaResult, Error, FileMeta, FileSlice, FileSystemClient};

#[derive(Debug)]
pub struct ObjectStoreFileSystemClient<E: TaskExecutor> {
    inner: Arc<DynObjectStore>,
    has_ordered_listing: bool,
    table_root: Path,
    task_executor: Arc<E>,
    readahead: usize,
}

impl<E: TaskExecutor> ObjectStoreFileSystemClient<E> {
    pub(crate) fn new(
        store: Arc<DynObjectStore>,
        has_ordered_listing: bool,
        table_root: Path,
        task_executor: Arc<E>,
    ) -> Self {
        Self {
            inner: store,
            has_ordered_listing,
            table_root,
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

impl<E: TaskExecutor> FileSystemClient for ObjectStoreFileSystemClient<E> {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let url = path.clone();
        let offset = Path::from(path.path());
        // TODO properly handle table prefix
        let prefix = self.table_root.child("_delta_log");

        let store = self.inner.clone();

        // This channel will become the iterator
        let (sender, receiver) = std::sync::mpsc::sync_channel(4_000);

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

        if !self.has_ordered_listing {
            // This FS doesn't return things in the order we require
            let mut fms: Vec<FileMeta> = receiver.into_iter().try_collect()?;
            fms.sort_unstable();
            Ok(Box::new(fms.into_iter().map(Ok)))
        } else {
            Ok(Box::new(receiver.into_iter()))
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
        let store = self.inner.clone();

        // This channel will become the output iterator.
        // Because there will already be buffering in the stream, we set the
        // buffer size to 0.
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);

        self.task_executor.spawn(
            futures::stream::iter(files)
                .map(move |(url, range)| {
                    // Wasn't checking the scheme before calling to_file_path causing the url path to
                    // be eaten in a strange way. Now, if not a file scheme, just blindly convert to a path.
                    // https://docs.rs/url/latest/url/struct.Url.html#method.to_file_path has more
                    // details about why this check is necessary
                    let path = if url.scheme() == "file" {
                        let file_path = url.to_file_path().expect("Not a valid file path");
                        Path::from_absolute_path(file_path).expect("Not able to be made into Path")
                    } else {
                        Path::from(url.path())
                    };
                    let store = store.clone();
                    async move {
                        match url.scheme() {
                            "http" | "https" => {
                                // have to annotate type here or rustc can't figure it out
                                Ok::<bytes::Bytes, Error>(reqwest::get(url).await?.bytes().await?)
                            }
                            _ => {
                                if let Some(rng) = range {
                                    Ok(store.get_range(&path, rng).await?)
                                } else {
                                    let result = store.get(&path).await?;
                                    Ok(result.bytes().await?)
                                }
                            }
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

        Ok(Box::new(receiver.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use object_store::memory::InMemory;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use test_utils::{abs_diff, delta_path_for_version};

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::Engine;

    use itertools::Itertools;

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
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(
            store,
            false, // don't have ordered listing
            prefix,
            Arc::new(TokioBackgroundExecutor::new()),
        );

        let mut slices: Vec<FileSlice> = Vec::new();

        let mut url1 = url.clone();
        url1.set_path(&format!("{}/b", url.path()));
        slices.push((url1.clone(), Some(Range { start: 0, end: 6 })));
        slices.push((url1, Some(Range { start: 7, end: 11 })));

        url.set_path(&format!("{}/c", url.path()));
        slices.push((url, Some(Range { start: 4, end: 9 })));
        dbg!("Slices are: {}", &slices);
        let data: Vec<Bytes> = client.read_files(slices).unwrap().try_collect().unwrap();

        assert_eq!(data.len(), 3);
        assert_eq!(data[0], Bytes::from("kernel"));
        assert_eq!(data[1], Bytes::from("data"));
        assert_eq!(data[2], Bytes::from("el-da"));
    }

    #[tokio::test]
    async fn test_file_meta_is_correct() {
        let store = Arc::new(InMemory::new());

        let begin_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let data = Bytes::from("kernel-data");
        let name = delta_path_for_version(1, "json");
        store.put(&name, data.clone().into()).await.unwrap();

        let table_root = Url::parse("memory:///").expect("valid url");
        let prefix = Path::from_url_path(table_root.path()).expect("Couldn't get path");
        let engine = DefaultEngine::new(store, prefix, Arc::new(TokioBackgroundExecutor::new()));
        let files: Vec<_> = engine
            .get_file_system_client()
            .list_from(&table_root)
            .unwrap()
            .try_collect()
            .unwrap();

        assert!(!files.is_empty());
        for meta in files.into_iter() {
            let meta_time = Duration::from_millis(meta.last_modified.try_into().unwrap());
            assert!(abs_diff(meta_time, begin_time) < Duration::from_secs(10));
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
        let prefix = Path::from_url_path(url.path()).expect("Couldn't get path");
        let engine = DefaultEngine::new(store, prefix, Arc::new(TokioBackgroundExecutor::new()));
        let client = engine.get_file_system_client();

        let files = client.list_from(&Url::parse("file://").unwrap()).unwrap();
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
}
