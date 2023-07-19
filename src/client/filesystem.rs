use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use url::Url;

use crate::{DeltaResult, Error, FileMeta, FileSlice, FileSystemClient};

#[derive(Debug)]
pub struct ObjectStoreFileSystemClient {
    inner: Arc<DynObjectStore>,
    table_root: Path,
}

impl ObjectStoreFileSystemClient {
    pub fn new(store: Arc<DynObjectStore>, table_root: Path) -> Self {
        Self {
            inner: store,
            table_root,
        }
    }
}

#[async_trait::async_trait]
impl FileSystemClient for ObjectStoreFileSystemClient {
    async fn list_from(&self, path: &Url) -> DeltaResult<BoxStream<'_, DeltaResult<FileMeta>>> {
        let url = path.clone();
        let offset = Path::from(path.path());
        // TODO properly handle table prefix
        let prefix = self.table_root.child("_delta_log");
        Ok(self
            .inner
            .list_with_offset(Some(&prefix), &offset)
            .await?
            .map_err(Error::from)
            .map_ok(move |meta| {
                let mut location = url.clone();
                location.set_path(&format!("/{}", meta.location.as_ref()));
                FileMeta {
                    location,
                    last_modified: meta.last_modified.timestamp(),
                    size: meta.size,
                }
            })
            .boxed())
    }

    /// Read data specified by the start and end offset from the file.
    async fn read_files(&self, files: Vec<FileSlice>) -> DeltaResult<Vec<Bytes>> {
        let mut bytes = Vec::new();
        for (url, range) in files {
            let path = Path::from(url.path());
            let data = if let Some(rng) = range {
                self.inner.get_range(&path, rng).await?
            } else {
                self.inner.get(&path).await?.bytes().await?
            };
            bytes.push(data);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use object_store::{local::LocalFileSystem, ObjectStore};

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
        let client = ObjectStoreFileSystemClient::new(store, prefix);

        let mut slices: Vec<FileSlice> = Vec::new();

        let mut url1 = url.clone();
        url1.set_path(&format!("{}/b", url.path()));
        slices.push((url1.clone(), Some(Range { start: 0, end: 6 })));
        slices.push((url1, Some(Range { start: 7, end: 11 })));

        url.set_path(&format!("{}/c", url.path()));
        slices.push((url, Some(Range { start: 4, end: 9 })));

        let data = client.read_files(slices).await.unwrap();

        assert_eq!(data.len(), 3);
        assert_eq!(data[0], Bytes::from("kernel"));
        assert_eq!(data[1], Bytes::from("data"));
        assert_eq!(data[2], Bytes::from("el-da"));
    }
}
