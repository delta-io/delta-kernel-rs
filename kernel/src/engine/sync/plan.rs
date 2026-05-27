//! A synchronous, test-only [`PlanExecutor`] backed by [`SyncEngine`] handlers.
//!
//! [`SyncEngine`]: super::SyncEngine
//
// TODO: Not used yet, but will eventually be used to replace SyncEngine with a
// PlanBasedEngine (backed by this PlanExecutor)
#![allow(dead_code)]

use std::sync::Arc;

use bytes::Bytes;

use super::json::SyncJsonHandler;
use super::parquet::SyncParquetHandler;
use super::storage::SyncStorageHandler;
use crate::object_store::DynObjectStore;
use crate::plans::{IoOperation, Plan, PlanExecutor, PlanResult, QueryPlanNode};
use crate::{DeltaResult, FileMeta, JsonHandler as _, ParquetHandler as _, StorageHandler as _};

/// A synchronous, test-only [`PlanExecutor`] that delegates to [`SyncStorageHandler`],
/// [`SyncJsonHandler`], and [`SyncParquetHandler`].
///
/// All I/O is performed synchronously via [`futures::executor::block_on`]; cloud-backed
/// stores are not supported (see [`super`] module docs).
pub(crate) struct SyncPlanExecutor {
    storage: SyncStorageHandler,
    json: SyncJsonHandler,
    parquet: SyncParquetHandler,
}

impl SyncPlanExecutor {
    /// Create a `SyncPlanExecutor` that resolves URLs against LocalFileSystem
    pub(crate) fn new() -> Self {
        Self::new_inner(None)
    }

    /// Create a `SyncPlanExecutor` backed by an explicit object store.
    pub(crate) fn new_with_store(store: Arc<DynObjectStore>) -> Self {
        Self::new_inner(Some(store))
    }

    fn new_inner(store: Option<Arc<DynObjectStore>>) -> Self {
        Self {
            storage: SyncStorageHandler::new(store.clone()),
            json: SyncJsonHandler::new(store.clone()),
            parquet: SyncParquetHandler::new(store),
        }
    }
}

impl PlanExecutor for SyncPlanExecutor {
    fn execute_plan(&self, plan: Plan) -> DeltaResult<PlanResult> {
        match plan {
            Plan::IoOperation(op) => self.execute_io(op),
            Plan::QueryPlan(query) => self.execute_query(query),
        }
    }
}

impl SyncPlanExecutor {
    fn execute_io(&self, op: IoOperation) -> DeltaResult<PlanResult> {
        match op {
            IoOperation::FileListing { url } => {
                // `StorageHandler::list_from` returns a non-`Send` iterator, so we collect into
                // a `Vec` first to convert into a `Send` iterator.
                // TODO(#2619): Evaluate whether StorageHandler should just return `Send` iterators
                let metas: Vec<DeltaResult<FileMeta>> = self.storage.list_from(&url)?.collect();
                Ok(PlanResult::FileMeta(Box::new(metas.into_iter())))
            }
            IoOperation::ReadBytes { files } => {
                // `StorageHandler::read_files` returns a non-`Send` iterator, so we collect into
                // a `Vec` first to convert into a `Send` iterator.
                // TODO(#2619): Evaluate whether StorageHandler should just return `Send` iterators
                let bytes: Vec<DeltaResult<Bytes>> = self.storage.read_files(files)?.collect();
                Ok(PlanResult::Bytes(Box::new(bytes.into_iter())))
            }
            IoOperation::WriteBytes {
                url,
                data,
                overwrite,
            } => {
                self.storage.put(&url, data, overwrite)?;
                Ok(PlanResult::Unit)
            }
            IoOperation::HeadFile { url } => {
                let meta = self.storage.head(&url)?;
                Ok(PlanResult::FileMeta(Box::new(std::iter::once(Ok(meta)))))
            }
            IoOperation::AtomicCopy {
                source,
                destination,
            } => {
                self.storage.copy_atomic(&source, &destination)?;
                Ok(PlanResult::Unit)
            }
        }
    }

    fn execute_query(&self, query: QueryPlanNode) -> DeltaResult<PlanResult> {
        match query {
            QueryPlanNode::ScanJson {
                files,
                physical_schema,
                predicate,
            } => {
                let iter = self
                    .json
                    .read_json_files(&files, physical_schema, predicate)?;
                Ok(PlanResult::Data(iter))
            }
            QueryPlanNode::ScanParquet {
                files,
                physical_schema,
                predicate,
            } => {
                let iter = self
                    .parquet
                    .read_parquet_files(&files, physical_schema, predicate)?;
                Ok(PlanResult::Data(iter))
            }
        }
    }
}
