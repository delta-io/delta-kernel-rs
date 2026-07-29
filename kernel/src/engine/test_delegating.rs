//! A test [`Engine`] that forwards to an inner engine, overriding individual handlers.

use std::sync::Arc;

#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// A test [`Engine`] that forwards every handler to `inner`, except those overridden via the
/// `with_*` setters. Lets a test swap a single handler without re-typing the forwards for the
/// rest.
#[allow(dead_code)]
pub(crate) struct DelegatingEngine {
    inner: Arc<dyn Engine>,
    evaluation: Option<Arc<dyn EvaluationHandler>>,
    storage: Option<Arc<dyn StorageHandler>>,
    json: Option<Arc<dyn JsonHandler>>,
    parquet: Option<Arc<dyn ParquetHandler>>,
    #[cfg(feature = "declarative-plans")]
    plan_executor: Option<Arc<dyn PlanExecutor>>,
}

#[allow(dead_code)]
impl DelegatingEngine {
    pub(crate) fn new(inner: Arc<dyn Engine>) -> Self {
        Self {
            inner,
            evaluation: None,
            storage: None,
            json: None,
            parquet: None,
            #[cfg(feature = "declarative-plans")]
            plan_executor: None,
        }
    }

    pub(crate) fn with_evaluation_handler(mut self, handler: Arc<dyn EvaluationHandler>) -> Self {
        self.evaluation = Some(handler);
        self
    }

    pub(crate) fn with_storage_handler(mut self, handler: Arc<dyn StorageHandler>) -> Self {
        self.storage = Some(handler);
        self
    }

    pub(crate) fn with_json_handler(mut self, handler: Arc<dyn JsonHandler>) -> Self {
        self.json = Some(handler);
        self
    }

    pub(crate) fn with_parquet_handler(mut self, handler: Arc<dyn ParquetHandler>) -> Self {
        self.parquet = Some(handler);
        self
    }

    #[cfg(feature = "declarative-plans")]
    pub(crate) fn with_plan_executor(mut self, executor: Arc<dyn PlanExecutor>) -> Self {
        self.plan_executor = Some(executor);
        self
    }
}

impl Engine for DelegatingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation
            .clone()
            .unwrap_or_else(|| self.inner.evaluation_handler())
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage
            .clone()
            .unwrap_or_else(|| self.inner.storage_handler())
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json
            .clone()
            .unwrap_or_else(|| self.inner.json_handler())
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet
            .clone()
            .unwrap_or_else(|| self.inner.parquet_handler())
    }

    #[cfg(feature = "declarative-plans")]
    fn plan_executor(&self) -> Arc<dyn PlanExecutor> {
        self.plan_executor
            .clone()
            .unwrap_or_else(|| self.inner.plan_executor())
    }
}
