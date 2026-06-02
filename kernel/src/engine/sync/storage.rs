use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use url::Url;

use super::{put_bytes, resolve_scope};
use crate::object_store::path::Path;
use crate::object_store::DynObjectStore;
// `ObjectStoreExt` is needed for `store.get()` etc. in arrow-58 mode where these methods moved
// off the `ObjectStore` trait. In arrow-57 mode the compat shim makes the import a no-op, so
// silence the resulting unused-import warning.
#[allow(unused_imports)]
use crate::object_store::ObjectStoreExt as _;
use crate::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};

pub(crate) struct SyncStorageHandler {
    store: Option<Arc<DynObjectStore>>,
}

impl SyncStorageHandler {
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self { store }
    }
}

impl StorageHandler for SyncStorageHandler {
    fn list_from(
        &self,
        url_path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let (store, base_url, offset) = resolve_scope(self.store.as_ref(), url_path)?;

        // For directory URLs, prefix == offset and the offset acts as a lower bound that still
        // includes everything underneath (since `dir/a` > `dir` lexicographically). For file
        // URLs, the prefix is the parent directory and the offset filters out files at-or-below.
        let prefix = if url_path.path().ends_with('/') {
            offset.clone()
        } else {
            let mut parts: Vec<_> = offset.parts().collect();
            parts.pop();
            Path::from_iter(parts)
        };

        // LocalFileSystem and InMemory do not return sorted listings, so we collect and sort
        // to give callers a deterministic order.
        let mut metas: Vec<_> = futures::executor::block_on(
            store
                .list_with_offset(Some(&prefix), &offset)
                .collect::<Vec<_>>(),
        )
        .into_iter()
        .collect::<Result<_, _>>()?;
        metas.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        let iter = metas.into_iter().map(move |meta| {
            let location = base_url
                .join(meta.location.as_ref())
                .map_err(|e| Error::generic(format!("Failed to construct URL: {e}")))?;
            Ok(FileMeta {
                location,
                last_modified: meta.last_modified.timestamp_millis(),
                size: meta.size,
            })
        });
        Ok(Box::new(iter))
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let store = self.store.clone();
        let results: Vec<DeltaResult<Bytes>> = files
            .into_iter()
            .map(|(url, _range_opt)| {
                let (s, _, path) = resolve_scope(store.as_ref(), &url)?;
                let get_result = futures::executor::block_on(s.get(&path))?;
                Ok(futures::executor::block_on(get_result.bytes())?)
            })
            .collect();
        Ok(Box::new(results.into_iter()))
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        put_bytes(self.store.as_ref(), path, data, overwrite)
    }

    fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
        unimplemented!("SyncStorageHandler does not implement copy");
    }

    fn head(&self, url: &Url) -> DeltaResult<FileMeta> {
        let (store, _, path) = resolve_scope(self.store.as_ref(), url)?;
        let meta = futures::executor::block_on(store.head(&path))?;
        Ok(FileMeta {
            location: url.clone(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;

    use bytes::{BufMut, BytesMut};
    use itertools::Itertools;
    use url::Url;

    use super::SyncStorageHandler;
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStoreExt as _;
    use crate::utils::current_time_duration;
    use crate::{Error, StorageHandler};

    /// generate json filenames that follow the spec (numbered padded to 20 chars)
    fn get_json_filename(index: usize) -> String {
        format!("{index:020}.json")
    }

    #[test]
    fn test_file_meta_is_correct() -> Result<(), Box<dyn std::error::Error>> {
        let storage = SyncStorageHandler::new(None);
        let tmp_dir = tempfile::tempdir().unwrap();

        let begin_time = current_time_duration()?;

        let path = tmp_dir.path().join(get_json_filename(1));
        let mut f = File::create(path)?;
        writeln!(f, "null")?;
        f.flush()?;

        let url_path = tmp_dir.path().join(get_json_filename(0));
        let url = Url::from_file_path(url_path).unwrap();
        let files: Vec<_> = storage.list_from(&url)?.try_collect()?;

        assert!(!files.is_empty());
        for meta in files.iter() {
            let meta_time = Duration::from_millis(meta.last_modified.try_into()?);
            assert!(meta_time.abs_diff(begin_time) < Duration::from_secs(10));
        }
        Ok(())
    }

    #[test]
    fn test_list_from() -> Result<(), Box<dyn std::error::Error>> {
        let storage = SyncStorageHandler::new(None);
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut expected = vec![];
        for i in 0..3 {
            let path = tmp_dir.path().join(get_json_filename(i));
            expected.push(path.clone());
            let mut f = File::create(path)?;
            writeln!(f, "null")?;
        }
        let url_path = tmp_dir.path().join(get_json_filename(1));
        let url = Url::from_file_path(url_path).unwrap();
        let list = storage.list_from(&url)?;
        let mut file_count = 0;
        for (i, file) in list.enumerate() {
            // i+1 in index because we started at 0001 in the listing
            assert_eq!(
                file?.location.to_file_path().unwrap().to_str().unwrap(),
                expected[i + 2].to_str().unwrap()
            );
            file_count += 1;
        }
        assert_eq!(file_count, 1);

        // Listing a directory URL: the URL must end with `/` so list_from recognizes it as a
        // directory rather than as a file whose parent should be listed.
        let url = Url::from_directory_path(tmp_dir.path()).unwrap();
        let list = storage.list_from(&url)?;
        file_count = list.count();
        assert_eq!(file_count, 3);

        let url_path = tmp_dir.path().join(format!("{:020}", 1));
        let url = Url::from_file_path(url_path).unwrap();
        let list = storage.list_from(&url)?;
        file_count = list.count();
        assert_eq!(file_count, 2);
        Ok(())
    }

    #[test]
    fn test_read_files() -> Result<(), Box<dyn std::error::Error>> {
        let storage = SyncStorageHandler::new(None);
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join(get_json_filename(1));
        let mut f = File::create(path.clone())?;
        writeln!(f, "null")?;
        let url = Url::from_file_path(path).unwrap();
        let file_slice = (url.clone(), None);
        let read = storage.read_files(vec![file_slice])?;
        let mut file_count = 0;
        let mut buf = BytesMut::with_capacity(16);
        buf.put(&b"null\n"[..]);
        let a = buf.split();
        for result in read {
            let result = result?;
            assert_eq!(result, a);
            file_count += 1;
        }
        assert_eq!(file_count, 1);
        Ok(())
    }

    /// `list_from` against an [`ObjectStore`] must walk subdirectories, matching the local FS
    /// implementation that uses `read_dir` recursively.
    #[tokio::test]
    async fn list_from_store_is_recursive() {
        let store = std::sync::Arc::new(InMemory::new());
        store
            .put(
                &crate::object_store::path::Path::from("a/file1.json"),
                bytes::Bytes::from("x").into(),
            )
            .await
            .unwrap();
        store
            .put(
                &crate::object_store::path::Path::from("a/sub/file2.json"),
                bytes::Bytes::from("y").into(),
            )
            .await
            .unwrap();

        let storage = SyncStorageHandler::new(Some(store));
        let url = Url::parse("memory:///a/").unwrap();
        let listed: Vec<_> = storage
            .list_from(&url)
            .unwrap()
            .map(Result::unwrap)
            .collect();

        let names: Vec<_> = listed
            .iter()
            .map(|f| f.location.path().to_string())
            .collect();
        assert_eq!(names, vec!["/a/file1.json", "/a/sub/file2.json"]);
    }

    #[test]
    fn head_local_returns_metadata() {
        let storage = SyncStorageHandler::new(None);
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join("file.json");
        std::fs::write(&path, b"hello").unwrap();
        let url = Url::from_file_path(&path).unwrap();

        let meta = storage.head(&url).unwrap();
        assert_eq!(meta.location, url);
        assert_eq!(meta.size, 5);
    }

    #[test]
    fn head_local_missing_file_errors() {
        let storage = SyncStorageHandler::new(None);
        let tmp_dir = tempfile::tempdir().unwrap();
        let url = Url::from_file_path(tmp_dir.path().join("missing.json")).unwrap();
        assert!(matches!(
            storage.head(&url).unwrap_err(),
            Error::FileNotFound(_)
        ));
    }

    #[tokio::test]
    async fn head_store_returns_metadata() {
        let store = std::sync::Arc::new(InMemory::new());
        let path = crate::object_store::path::Path::from("file.json");
        store
            .put(&path, bytes::Bytes::from("hello").into())
            .await
            .unwrap();

        let storage = SyncStorageHandler::new(Some(store));
        let url = Url::parse("memory:///file.json").unwrap();
        let meta = storage.head(&url).unwrap();
        assert_eq!(meta.location, url);
        assert_eq!(meta.size, 5);
    }

    #[tokio::test]
    async fn put_store_creates_and_blocks_overwrite() {
        let store = std::sync::Arc::new(InMemory::new());
        let storage = SyncStorageHandler::new(Some(store.clone()));
        let url = Url::parse("memory:///file.json").unwrap();

        storage
            .put(&url, bytes::Bytes::from("first"), false)
            .unwrap();
        let path = crate::object_store::path::Path::from("file.json");
        let bytes = futures::executor::block_on(async {
            store.get(&path).await.unwrap().bytes().await.unwrap()
        });
        assert_eq!(bytes.as_ref(), b"first");

        // Without overwrite, a second put must fail with FileAlreadyExists.
        let err = storage
            .put(&url, bytes::Bytes::from("second"), false)
            .unwrap_err();
        assert!(matches!(err, Error::FileAlreadyExists(_)));

        // With overwrite, it should succeed.
        storage
            .put(&url, bytes::Bytes::from("third"), true)
            .unwrap();
        let bytes = futures::executor::block_on(async {
            store.get(&path).await.unwrap().bytes().await.unwrap()
        });
        assert_eq!(bytes.as_ref(), b"third");
    }
}
