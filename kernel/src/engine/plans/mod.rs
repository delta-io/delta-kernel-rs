//! This module contains an implementation of the Engine trait that is
//! backed by a PlanExecutor. The engine delegates handler operations
//! (e.g. storage, JSON, parquet) to declarative plan execution rather than implementing
//! each handler independently.
//!
//! This allows a PlanExecutor to become the single surface for connector optimizations,
//! while still allowing kernel to use existing Engine trait APIs.
//!
//! ### Arrow Requirement:
//! The PlanBasedEngine implementation assumes the use of ArrowEngineData during JSON parsing, so it
//! is only compatible with `PlanExecutor`'s which return ArrowEngineData.

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
/// Operations not yet implemented on the plan-execution path (e.g. `write_json_file`,
/// `write_parquet_file`) and evaluation are delegated to the required `fallback` [`Engine`].
pub struct PlanBasedEngine {
    executor: Arc<dyn PlanExecutor>,
    evaluation: Arc<dyn EvaluationHandler>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<PlanBasedJsonHandler>,
    parquet: Arc<PlanBasedParquetHandler>,
}

impl PlanBasedEngine {
    /// Construct a `PlanBasedEngine` backed by `plan_executor`, delegating operations not yet
    /// implemented on the plan-execution path to `fallback`.
    ///
    /// The fallback's [`EvaluationHandler`] is used for evaluation, and each plan-based handler
    /// falls back to the fallback's corresponding handler for operations the plan-execution path
    /// does not yet implement.
    pub fn new(fallback: Arc<dyn Engine>, plan_executor: Arc<dyn PlanExecutor>) -> Self {
        Self {
            evaluation: fallback.evaluation_handler(),
            storage: Arc::new(PlanBasedStorageHandler::new(plan_executor.clone())),
            json: Arc::new(PlanBasedJsonHandler::new(
                plan_executor.clone(),
                fallback.json_handler(),
            )),
            parquet: Arc::new(PlanBasedParquetHandler::new(
                plan_executor.clone(),
                fallback.parquet_handler(),
            )),
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
