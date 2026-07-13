//! A [`StorageHandler`] implementation backed by a [`PlanExecutor`].

use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::plans::{IoOperation, Operation, PlanExecutor, PlanResult};
use delta_kernel::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};
use itertools::Itertools as _;
use url::Url;

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
