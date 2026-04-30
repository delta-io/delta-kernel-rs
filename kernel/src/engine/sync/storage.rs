use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use itertools::Itertools;
use url::Url;

use crate::object_store::path::Path;
use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
use crate::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};

pub(crate) struct SyncStorageHandler {
    store: Option<Arc<DynObjectStore>>,
}

impl SyncStorageHandler {
    pub(crate) fn new() -> Self {
        Self { store: None }
    }

    pub(crate) fn with_store(store: Arc<DynObjectStore>) -> Self {
        Self { store: Some(store) }
    }
}

impl StorageHandler for SyncStorageHandler {
    fn list_from(
        &self,
        url_path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        if let Some(store) = &self.store {
            return list_from_store(store, url_path);
        }
        list_from_local(url_path)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        if let Some(store) = &self.store {
            return read_files_store(store, files);
        }
        read_files_local(files)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        if let Some(store) = &self.store {
            return put_store(store, path, data, overwrite);
        }
        put_local(path, data, overwrite)
    }

    fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
        unimplemented!("SyncStorageHandler does not implement copy");
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        if let Some(store) = &self.store {
            return head_store(store, path);
        }
        head_local(path)
    }
}

// === ObjectStore-backed implementations ===

fn list_from_store(
    store: &DynObjectStore,
    url_path: &Url,
) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
    let path = Path::from(url_path.path());
    let url_is_directory = url_path.path().ends_with('/');

    // If the URL is a directory, list everything underneath. Otherwise treat the final segment as
    // a lower-bound filename and list its parent. We use recursive `list` (not
    // `list_with_delimiter`) to match the local implementation, which walks into subdirectories.
    let (list_prefix, filter_path) = if url_is_directory {
        (path, None)
    } else {
        let parent = path
            .parts()
            .take(path.parts().count().saturating_sub(1))
            .collect::<Path>();
        (parent, Some(path.to_string()))
    };

    let mut metas: Vec<_> =
        futures::executor::block_on(store.list(Some(&list_prefix)).collect::<Vec<_>>())
            .into_iter()
            .filter_map(|res| match res {
                Ok(meta) => filter_path
                    .as_ref()
                    .is_none_or(|fp| meta.location.to_string() > *fp)
                    .then_some(Ok(meta)),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<_, _>>()?;
    metas.sort_by(|a, b| a.location.cmp(&b.location));

    let base_url = {
        let mut u = url_path.clone();
        u.set_path("/");
        u
    };

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

fn read_files_store(
    store: &DynObjectStore,
    files: Vec<FileSlice>,
) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
    // Collect eagerly since we need sync access to the store
    let results: Vec<DeltaResult<Bytes>> = files
        .into_iter()
        .map(|(url, _range_opt)| {
            let path = Path::from(url.path());
            let get_result = futures::executor::block_on(store.get(&path))?;
            let bytes = futures::executor::block_on(get_result.bytes())?;
            Ok(bytes)
        })
        .collect();
    Ok(Box::new(results.into_iter()))
}

fn put_store(store: &DynObjectStore, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
    use crate::object_store::PutMode;

    let object_path = Path::from(path.path());
    let opts = if overwrite {
        crate::object_store::PutOptions::default()
    } else {
        crate::object_store::PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        }
    };
    futures::executor::block_on(store.put_opts(&object_path, data.into(), opts)).map_err(|e| {
        match e {
            crate::object_store::Error::AlreadyExists { .. } => {
                Error::FileAlreadyExists(path.to_string())
            }
            other => Error::generic(other.to_string()),
        }
    })?;
    Ok(())
}

fn head_store(store: &DynObjectStore, url: &Url) -> DeltaResult<FileMeta> {
    let path = Path::from(url.path());
    let meta = futures::executor::block_on(store.head(&path))?;
    Ok(FileMeta {
        location: url.clone(),
        last_modified: meta.last_modified.timestamp_millis(),
        size: meta.size,
    })
}

// === Local filesystem implementations (original behavior) ===

fn list_from_local(url_path: &Url) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
    if url_path.scheme() == "file" {
        let path = url_path
            .to_file_path()
            .map_err(|_| Error::Generic(format!("Invalid path for list_from: {url_path:?}")))?;

        let (path_to_read, min_file_name) = if path.is_dir() {
            (path, None)
        } else {
            let parent = path
                .parent()
                .ok_or_else(|| Error::Generic(format!("Invalid path for list_from: {path:?}")))?
                .to_path_buf();
            let file_name = path
                .file_name()
                .ok_or_else(|| Error::Generic(format!("Invalid path for list_from: {path:?}")))?;
            (parent, Some(file_name))
        };

        let all_ents: Vec<_> = std::fs::read_dir(path_to_read)?
            .filter(|ent_res| {
                match (ent_res, min_file_name) {
                    (Ok(ent), Some(min_file_name)) => ent.file_name() > *min_file_name,
                    _ => true, // Keep unfiltered and/or error entries
                }
            })
            .try_collect()?;
        let it = all_ents
            .into_iter()
            .sorted_by_key(|ent| ent.path())
            .map(TryFrom::try_from);
        Ok(Box::new(it))
    } else {
        Err(Error::generic("Can only read local filesystem"))
    }
}

fn read_files_local(
    files: Vec<FileSlice>,
) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
    let iter = files.into_iter().map(|(url, _range_opt)| {
        if url.scheme() == "file" {
            if let Ok(file_path) = url.to_file_path() {
                let bytes_vec_res = std::fs::read(file_path);
                let bytes: std::io::Result<Bytes> = bytes_vec_res.map(|bytes_vec| bytes_vec.into());
                return bytes.map_err(|_| Error::file_not_found(url.path()));
            }
        }
        Err(Error::generic("Can only read local filesystem"))
    });
    Ok(Box::new(iter))
}

fn put_local(path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
    if path.scheme() != "file" {
        return Err(Error::generic("Can only write to local filesystem"));
    }
    let file_path = path
        .to_file_path()
        .map_err(|_| Error::generic(format!("Invalid path for put: {path:?}")))?;
    if !overwrite && file_path.exists() {
        return Err(Error::FileAlreadyExists(file_path.to_string_lossy().into()));
    }
    std::fs::write(&file_path, &data)
        .map_err(|e| Error::generic(format!("Failed to write {}: {e}", file_path.display())))
}

fn head_local(path: &Url) -> DeltaResult<FileMeta> {
    if path.scheme() != "file" {
        return Err(Error::generic("Can only read local filesystem"));
    }
    let file_path = path
        .to_file_path()
        .map_err(|_| Error::generic(format!("Invalid path for head: {path:?}")))?;
    let metadata = std::fs::metadata(&file_path)
        .map_err(|_| Error::file_not_found(file_path.to_string_lossy()))?;
    let last_modified = metadata
        .modified()
        .map_err(|e| Error::generic(format!("Failed to get modified time: {e}")))?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::generic(format!("Invalid modified time: {e}")))?
        .as_millis() as i64;
    Ok(FileMeta {
        location: path.clone(),
        last_modified,
        size: metadata.len(),
    })
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
        let storage = SyncStorageHandler::new();
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
        let storage = SyncStorageHandler::new();
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

        let url_path = tmp_dir.path().join("");
        let url = Url::from_file_path(url_path).unwrap();
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
        let storage = SyncStorageHandler::new();
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

        let storage = SyncStorageHandler::with_store(store);
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
        let storage = SyncStorageHandler::new();
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
        let storage = SyncStorageHandler::new();
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

        let storage = SyncStorageHandler::with_store(store);
        let url = Url::parse("memory:///file.json").unwrap();
        let meta = storage.head(&url).unwrap();
        assert_eq!(meta.location, url);
        assert_eq!(meta.size, 5);
    }

    #[tokio::test]
    async fn put_store_creates_and_blocks_overwrite() {
        let store = std::sync::Arc::new(InMemory::new());
        let storage = SyncStorageHandler::with_store(store.clone());
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
