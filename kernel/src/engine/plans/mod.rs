//! This module contains an implementation of the Engine trait that is
//! backed by a PlanExecutor. The engine delegates handler operations
//! (e.g. storage, JSON, parquet) to declarative plan execution rather than implementing
//! each handler independently.
//!
//! This allows a PlanExecutor to become the single surface for connector optimizations,
//! while still allowing kernel to use existing Engine trait APIs.

use std::sync::Arc;

pub mod json;
pub mod parquet;
pub mod storage;

use json::PlanBasedJsonHandler;
use parquet::PlanBasedParquetHandler;
use storage::PlanBasedStorageHandler;

use crate::plans::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// An [`Engine`] that routes operations through a [`PlanExecutor`].
///
/// Storage, JSON file reads, and Parquet file reads are converted into
/// [`Operation`](crate::plans::Operation)s and delegated to the plan executor.
///
/// EvaluationHandler capabilities are not supported under plan execution,
/// so the engine must provide an EvaluationHandler as well.
pub struct PlanBasedEngine {
    executor: Arc<dyn PlanExecutor>,
    evaluation: Arc<dyn EvaluationHandler>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<PlanBasedJsonHandler>,
    parquet: Arc<PlanBasedParquetHandler>,
}

impl PlanBasedEngine {
    pub fn new(
        evaluation_handler: Arc<dyn EvaluationHandler>,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> Self {
        Self {
            evaluation: evaluation_handler,
            storage: Arc::new(PlanBasedStorageHandler::new(plan_executor.clone())),
            json: Arc::new(PlanBasedJsonHandler::new(plan_executor.clone())),
            parquet: Arc::new(PlanBasedParquetHandler::new(plan_executor.clone())),
            executor: plan_executor,
        }
    }
}

impl Engine for PlanBasedEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    fn plan_executor(&self) -> Arc<dyn PlanExecutor> {
        self.executor.clone()
    }
}
