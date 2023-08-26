use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use url::Url;

use crate::{executor::TaskExecutor, DeltaResult, FileMeta, FileSlice, FileSystemClient};

#[derive(Debug)]
pub struct ObjectStoreFileSystemClient<E: TaskExecutor> {
    inner: Arc<DynObjectStore>,
    table_root: Path,
    task_executor: Arc<E>,
}

impl<E: TaskExecutor> ObjectStoreFileSystemClient<E> {
    pub fn new(store: Arc<DynObjectStore>, table_root: Path, task_executor: Arc<E>) -> Self {
        Self {
            inner: store,
            table_root,
            task_executor,
        }
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

        // This channel will report whether initialization succeeded
        let (res_sender, res_receiver) =
            std::sync::mpsc::channel::<Result<(), object_store::Error>>();
        // This channel will become the iterator
        let (sender, receiver) = std::sync::mpsc::sync_channel(4_000);

        self.task_executor.spawn(async move {
            let mut stream = match store.list_with_offset(Some(&prefix), &offset).await {
                Ok(stream) => {
                    res_sender.send(Ok(())).ok();
                    stream
                }
                Err(e) => {
                    res_sender.send(Err(e)).ok();
                    return;
                }
            };

            while let Some(meta) = stream.next().await {
                match meta {
                    Ok(meta) => {
                        let mut location = url.clone();
                        location.set_path(&format!("/{}", meta.location.as_ref()));
                        sender
                            .send(Ok(FileMeta {
                                location,
                                last_modified: meta.last_modified.timestamp(),
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

        // Wait for successful init
        res_receiver.recv().unwrap()?;

        Ok(Box::new(receiver.into_iter()))
    }

    /// Read data specified by the start and end offset from the file.
    fn read_files(&self, files: Vec<FileSlice>) -> DeltaResult<Vec<Bytes>> {
        let store = self.inner.clone();
        let fut = futures::stream::iter(files)
            .map(move |(url, range)| {
                let path = Path::from(url.path());
                let store = store.clone();
                async move {
                    if let Some(rng) = range {
                        store.get_range(&path, rng).await
                    } else {
                        let result = store.get(&path).await?;
                        result.bytes().await
                    }
                }
            })
            // Read up to 10 files concurrently.
            .buffered(10)
            .try_collect::<Vec<Bytes>>();

        Ok(self.task_executor.block_on(fut)?)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::executor::tokio::TokioBackgroundExecutor;

    use super::*;

    #[tokio::test]
    async fn test_read_files() {
        let tmp = tempfile::tempdir().unwrap();
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();

        let data = Bytes::from("kernel-data");
        tmp_store.put(&Path::from("a"), data.clone()).await.unwrap();
        tmp_store.put(&Path::from("b"), data.clone()).await.unwrap();
        tmp_store.put(&Path::from("c"), data.clone()).await.unwrap();

        let mut url = Url::from_directory_path(tmp.path()).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let prefix = Path::from(url.path());
        let client = ObjectStoreFileSystemClient::new(
            store,
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

        let data = client.read_files(slices).unwrap();

        assert_eq!(data.len(), 3);
        assert_eq!(data[0], Bytes::from("kernel"));
        assert_eq!(data[1], Bytes::from("data"));
        assert_eq!(data[2], Bytes::from("el-da"));
    }
}
