//! A plan-based [`Engine`] implementation.
//!
//! [`PlanBasedEngine`] delegates operations to a [`PlanExecutor`] when possible, otherwise
//! falling back to using DefaultEngine implementations.

use std::fmt;
use std::sync::Arc;

use super::storage::PlanBasedStorageHandler;
use crate::engine::arrow_expression::ArrowEvaluationHandler;
use crate::engine::default::executor::TaskExecutor;
use crate::engine::default::json::DefaultJsonHandler;
use crate::engine::default::parquet::DefaultParquetHandler;
use crate::object_store::DynObjectStore;
use crate::plan::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// An [`Engine`] that routes storage operations through a [`PlanExecutor`].
///
/// The storage handler converts [`StorageHandler`] calls into [`DeclarativePlanNode`]s and
/// delegates them to the plan executor. JSON, Parquet, and evaluation handlers use the same
/// default implementations as [`DefaultEngine`].
///
/// [`DeclarativePlanNode`]: crate::plan::DeclarativePlanNode
/// [`DefaultEngine`]: crate::engine::default::DefaultEngine
pub struct PlanBasedEngine<E: TaskExecutor> {
    executor: Arc<dyn PlanExecutor>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<DefaultParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
}

impl<E: TaskExecutor> fmt::Debug for PlanBasedEngine<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanBasedEngine")
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}

impl<E: TaskExecutor> PlanBasedEngine<E> {
    /// Create a new `PlanBasedEngine`.
    ///
    /// Storage operations are delegated to `plan_executor`. The JSON, Parquet, and evaluation
    /// handlers are constructed from the given `object_store` and `task_executor`, identically
    /// to [`DefaultEngine`](crate::engine::default::DefaultEngine).
    pub fn new(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> Self {
        Self {
            storage: Arc::new(PlanBasedStorageHandler::new(plan_executor.clone())),
            json: Arc::new(DefaultJsonHandler::new(
                object_store.clone(),
                task_executor.clone(),
            )),
            parquet: Arc::new(DefaultParquetHandler::new(object_store, task_executor)),
            executor: plan_executor,
            evaluation: Arc::new(ArrowEvaluationHandler {}),
        }
    }
}

impl<E: TaskExecutor> Engine for PlanBasedEngine<E> {
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
