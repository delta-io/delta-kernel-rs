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
/// EvaluationHandler capabilities are not supported under plan execution, so the engine is always
/// constructed with an [`EvaluationHandler`].
pub struct PlanBasedEngine {
    executor: Arc<dyn PlanExecutor>,
    evaluation: Arc<dyn EvaluationHandler>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<PlanBasedJsonHandler>,
    parquet: Arc<PlanBasedParquetHandler>,
}

impl PlanBasedEngine {
    /// Construct a `PlanBasedEngine` with no fallback engine.
    ///
    /// Not-yet-implemented operations return an unsupported error.
    /// Use [`PlanBasedEngine::with_fallback`] to delegate them to another engine instead.
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

    /// Construct a `PlanBasedEngine` that delegates not-yet-implemented operations to `fallback`.
    ///
    /// The fallback's [`EvaluationHandler`] is used for evaluation, and each plan-based handler
    /// falls back to the fallback's corresponding handler for operations the plan-execution path
    /// does not yet implement.
    pub fn with_fallback(fallback: Arc<dyn Engine>, plan_executor: Arc<dyn PlanExecutor>) -> Self {
        Self {
            evaluation: fallback.evaluation_handler(),
            storage: Arc::new(PlanBasedStorageHandler::new(plan_executor.clone())),
            json: Arc::new(PlanBasedJsonHandler::with_fallback(
                plan_executor.clone(),
                fallback.json_handler(),
            )),
            parquet: Arc::new(PlanBasedParquetHandler::with_fallback(
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
