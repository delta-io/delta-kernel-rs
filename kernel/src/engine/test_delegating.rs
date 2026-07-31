//! Test [`Engine`] that delegates handlers to an inner engine.

use std::sync::Arc;

#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// A test [`Engine`] that forwards every handler to `inner`, except those overridden via the
/// `with_*` setters. Lets a test swap a single handler without re-typing the forwards for the
/// rest.
pub(crate) struct DelegatingEngine {
    inner: Arc<dyn Engine>,
    storage: Option<Arc<dyn StorageHandler>>,
    json: Option<Arc<dyn JsonHandler>>,
    parquet: Option<Arc<dyn ParquetHandler>>,
    #[cfg(feature = "declarative-plans")]
    plan_executor: PlanExecutorOverride,
}

/// How [`DelegatingEngine`] resolves `plan_executor`. Unlike the other handlers, `None` is
/// itself a valid override (forcing an engine to report no executor), so it needs a distinct
/// variant from inheriting the inner engine's executor.
#[cfg(feature = "declarative-plans")]
enum PlanExecutorOverride {
    Inherit,
    None,
    Some(Arc<dyn PlanExecutor>),
}

impl DelegatingEngine {
    pub(crate) fn new(inner: Arc<dyn Engine>) -> Self {
        Self {
            inner,
            storage: None,
            json: None,
            parquet: None,
            #[cfg(feature = "declarative-plans")]
            plan_executor: PlanExecutorOverride::Inherit,
        }
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
        self.plan_executor = PlanExecutorOverride::Some(executor);
        self
    }

    #[cfg(feature = "declarative-plans")]
    pub(crate) fn without_plan_executor(mut self) -> Self {
        self.plan_executor = PlanExecutorOverride::None;
        self
    }
}

impl Engine for DelegatingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
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
    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        match &self.plan_executor {
            PlanExecutorOverride::Inherit => self.inner.plan_executor(),
            PlanExecutorOverride::None => None,
            PlanExecutorOverride::Some(executor) => Some(executor.clone()),
        }
    }
}
