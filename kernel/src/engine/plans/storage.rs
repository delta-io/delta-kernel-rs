//! A [`StorageHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools as _;
use url::Url;

use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult};
use crate::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};

/// A [`StorageHandler`] that delegates to a [`PlanExecutor`].
pub struct PlanBasedStorageHandler {
    executor: Arc<dyn PlanExecutor>,
}

impl PlanBasedStorageHandler {
    pub fn new(executor: Arc<dyn PlanExecutor>) -> Self {
        Self { executor }
    }

    fn execute_io(&self, op: IoOperation) -> DeltaResult<PlanResult> {
        self.executor.execute_op(Operation::IoOperation(op))
    }
}

impl StorageHandler for PlanBasedStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        Ok(self
            .execute_io(IoOperation::file_listing(path.clone()))?
            .into_file_meta()?)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        Ok(self
            .execute_io(IoOperation::read_bytes(files))?
            .into_bytes()?)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        self.execute_io(IoOperation::atomic_copy(src.clone(), dest.clone()))?
            .into_unit()
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        self.execute_io(IoOperation::write_bytes(path.clone(), data, overwrite))?
            .into_unit()
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        self.execute_io(IoOperation::head_file(path.clone()))?
            .into_file_meta()?
            .exactly_one()
            .map_err(|e| Error::generic(format!("Expected exactly one file meta: {e}")))?
    }

    fn delete(&self, _path: &Url) -> DeltaResult<()> {
        // TODO(#2820): implement here once supported as IoOperation.
        unimplemented!("PlanBasedStorageHandler does not yet implement delete")
    }
}

#[cfg(test)]
mod tests {
    // TODO(#2618): Refactor and share a test suite with sync engine tests.

    use std::fs::File;
    use std::io::Write as _;
    use std::sync::Arc;

    use itertools::Itertools as _;
    use tempfile::tempdir;
    use url::Url;

    use super::PlanBasedStorageHandler;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::{Error, StorageHandler as _};

    fn make_handler() -> PlanBasedStorageHandler {
        PlanBasedStorageHandler::new(Arc::new(SyncPlanExecutor::default()))
    }

    #[test]
    fn test_list_from() {
        let tmp = tempdir().unwrap();
        for i in 0..3 {
            let path = tmp.path().join(format!("{i:020}.json"));
            let mut f = File::create(&path).unwrap();
            writeln!(f, "null").unwrap();
        }

        let storage = make_handler();

        // Listing from a file URL is exclusive of the offset itself, so listing from index 1
        // returns only index 2.
        let from = Url::from_file_path(tmp.path().join("00000000000000000001.json")).unwrap();
        let files: Vec<_> = storage.list_from(&from).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), 1);
        let expected = Url::from_file_path(tmp.path().join("00000000000000000002.json")).unwrap();
        assert_eq!(files[0].location, expected);

        // Listing the directory returns all 3 files.
        let dir = Url::from_directory_path(tmp.path()).unwrap();
        let all: Vec<_> = storage.list_from(&dir).unwrap().try_collect().unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_put_then_read_round_trip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("blob.bin");
        let url = Url::from_file_path(&path).unwrap();

        let storage = make_handler();

        storage
            .put(&url, bytes::Bytes::from_static(b"hello"), false)
            .unwrap();

        let files = vec![(url.clone(), None)];
        let bytes: Vec<_> = storage.read_files(files).unwrap().try_collect().unwrap();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0].as_ref(), b"hello");
    }

    #[test]
    fn test_put_without_overwrite() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("blob.bin");
        let url = Url::from_file_path(&path).unwrap();

        let storage = make_handler();

        storage
            .put(&url, bytes::Bytes::from_static(b"first"), false)
            .unwrap();
        let err = storage
            .put(&url, bytes::Bytes::from_static(b"second"), false)
            .unwrap_err();
        assert!(matches!(err, Error::FileAlreadyExists(_)));

        // With `overwrite = true`, the second write succeeds.
        storage
            .put(&url, bytes::Bytes::from_static(b"second"), true)
            .unwrap();
    }

    #[test]
    fn test_head() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.json");
        std::fs::write(&path, b"hello").unwrap();
        let url = Url::from_file_path(&path).unwrap();

        let meta = make_handler().head(&url).unwrap();
        assert_eq!(meta.location, url);
        assert_eq!(meta.size, 5);

        // Errors on missing file
        let url = Url::from_file_path(tmp.path().join("missing.json")).unwrap();
        let err = make_handler().head(&url).unwrap_err();
        assert!(matches!(err, Error::FileNotFound(_)));
    }
}
