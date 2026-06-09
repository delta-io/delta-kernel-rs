//! A synchronous, test-only [`PlanExecutor`] backed by [`SyncEngine`] handlers.
//!
//! [`SyncEngine`]: super::SyncEngine
//
// TODO: Not used yet, but will eventually be used to replace SyncEngine with an
// PlanBasedEngine (backed by this PlanExecutor)
#![allow(dead_code)]

use std::sync::Arc;

use bytes::Bytes;

use super::json::SyncJsonHandler;
use super::parquet::SyncParquetHandler;
use super::storage::SyncStorageHandler;
use crate::object_store::DynObjectStore;
use crate::plans::ir::nodes::{NodeKind, ScanJson, ScanParquet};
use crate::plans::ir::plan::{Plan, PlanNode};
use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult};
use crate::{
    DeltaResult, Error, FileMeta, JsonHandler as _, ParquetHandler as _, StorageHandler as _,
};

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
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
        match op {
            Operation::IoOperation(io_op) => self.execute_io(io_op),
            Operation::QueryPlan(query) => self.execute_query(query),
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
            IoOperation::ParquetFooter { file } => {
                let footer = self.parquet.read_parquet_footer(&file)?;
                Ok(PlanResult::ParquetFooter(footer))
            }
        }
    }

    fn execute_query(&self, query: Plan) -> DeltaResult<PlanResult> {
        let node = into_single_node_plan(query)?;
        match node.kind {
            NodeKind::ScanJson(ScanJson { files, schema, .. }) => {
                let files: Vec<FileMeta> = files.into_iter().map(|f| f.meta).collect();
                let iter = self.json.read_json_files(&files, schema, None)?;
                Ok(PlanResult::Data(iter))
            }
            NodeKind::ScanParquet(ScanParquet { files, schema, .. }) => {
                let files: Vec<FileMeta> = files.into_iter().map(|f| f.meta).collect();
                let iter = self.parquet.read_parquet_files(&files, schema, None)?;
                Ok(PlanResult::Data(iter))
            }
            other => Err(Error::generic(format!(
                "SyncPlanExecutor only supports ScanJson / ScanParquet, got NodeKind::{other}",
            ))),
        }
    }
}

/// Returns the plan's sole [`PlanNode`], erroring if it is not a single-node plan.
fn into_single_node_plan(plan: Plan) -> DeltaResult<PlanNode> {
    let [node] = <[_; 1]>::try_from(plan.nodes).map_err(|nodes: Vec<_>| {
        Error::generic(format!(
            "expected single-node plan, got {} nodes",
            nodes.len()
        ))
    })?;
    Ok(node)
}
