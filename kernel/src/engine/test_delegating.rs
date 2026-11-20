//! Test engines for selected handler overrides and assertions against unexpected handler access.

use std::sync::Arc;

#[cfg(feature = "declarative-plans")]
use crate::plans::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// A test [`Engine`] that returns supplied handler overrides and delegates other accessors.
///
/// Each accessor returns its override if one is set, and the inner engine's handler otherwise.
///
/// With `declarative-plans`, the plan executor is inherited unless explicitly replaced or disabled.
pub struct DelegatingEngine {
    inner: Arc<dyn Engine>,
    evaluation: Option<Arc<dyn EvaluationHandler>>,
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
    /// Creates an engine that inherits handlers and the optional plan executor from `inner`.
    pub fn new(inner: Arc<dyn Engine>) -> Self {
        Self {
            inner,
            evaluation: None,
            storage: None,
            json: None,
            parquet: None,
            #[cfg(feature = "declarative-plans")]
            plan_executor: PlanExecutorOverride::Inherit,
        }
    }

    /// Overrides the evaluation handler, replacing any previous override.
    pub fn with_evaluation_handler(mut self, handler: Arc<dyn EvaluationHandler>) -> Self {
        self.evaluation = Some(handler);
        self
    }

    /// Overrides the storage handler, replacing any previous override.
    pub fn with_storage_handler(mut self, handler: Arc<dyn StorageHandler>) -> Self {
        self.storage = Some(handler);
        self
    }

    /// Overrides the JSON handler, replacing any previous override.
    pub fn with_json_handler(mut self, handler: Arc<dyn JsonHandler>) -> Self {
        self.json = Some(handler);
        self
    }

    /// Overrides the Parquet handler, replacing any previous override.
    pub fn with_parquet_handler(mut self, handler: Arc<dyn ParquetHandler>) -> Self {
        self.parquet = Some(handler);
        self
    }

    /// Overrides the plan executor, including any previous selection of no executor.
    #[cfg(feature = "declarative-plans")]
    pub fn with_plan_executor(mut self, executor: Arc<dyn PlanExecutor>) -> Self {
        self.plan_executor = PlanExecutorOverride::Some(executor);
        self
    }

    /// Disables the plan executor, replacing any previous executor override.
    ///
    /// Without `declarative-plans`, returns the engine unchanged so callers need no matching gate.
    pub fn without_plan_executor(self) -> Self {
        #[cfg(feature = "declarative-plans")]
        {
            let mut engine = self;
            engine.plan_executor = PlanExecutorOverride::None;
            engine
        }
        #[cfg(not(feature = "declarative-plans"))]
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
    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        match &self.plan_executor {
            PlanExecutorOverride::Inherit => self.inner.plan_executor(),
            PlanExecutorOverride::None => None,
            PlanExecutorOverride::Some(executor) => Some(executor.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "declarative-plans")]
    use rstest::rstest;
    use url::Url;

    use super::*;
    #[cfg(feature = "declarative-plans")]
    use crate::KernelError;
    use crate::engine::CountingEngine;
    use crate::engine::sync::SyncEngine;
    use crate::schema::StructType;

    /// Runs one operation through each required handler of `engine`, ignoring the results.
    fn run_one_operation_per_handler(engine: &dyn Engine) {
        let schema = Arc::new(StructType::new_unchecked([]));
        let url = Url::parse("memory:///missing").unwrap();
        let _ = engine
            .evaluation_handler()
            .create_many(schema.clone(), vec![]);
        let _ = engine.storage_handler().head(&url);
        let _ = engine
            .json_handler()
            .read_json_files(&[], schema.clone(), None);
        let _ = engine
            .parquet_handler()
            .read_parquet_files(&[], schema, None);
    }

    #[test]
    fn overrides_bypass_inner_and_retain_handler_allocations() {
        let base_engine = Arc::new(SyncEngine::in_memory());
        let evaluation_handler = base_engine.evaluation_handler();
        let storage_handler = base_engine.storage_handler();
        let json_handler = base_engine.json_handler();
        let parquet_handler = base_engine.parquet_handler();
        let inner = Arc::new(CountingEngine::new_no_io(base_engine));
        let engine = DelegatingEngine::new(inner.clone())
            .with_evaluation_handler(evaluation_handler.clone())
            .with_storage_handler(storage_handler.clone())
            .with_json_handler(json_handler.clone())
            .with_parquet_handler(parquet_handler.clone())
            .without_plan_executor();

        assert!(Arc::ptr_eq(
            &engine.evaluation_handler(),
            &evaluation_handler
        ));
        assert!(Arc::ptr_eq(&engine.storage_handler(), &storage_handler));
        assert!(Arc::ptr_eq(&engine.json_handler(), &json_handler));
        assert!(Arc::ptr_eq(&engine.parquet_handler(), &parquet_handler));
        run_one_operation_per_handler(&engine);
    }

    #[test]
    fn unconfigured_handlers_delegate_operations_to_inner() {
        let inner = Arc::new(CountingEngine::new(Arc::new(SyncEngine::in_memory())));
        let engine = DelegatingEngine::new(inner.clone());

        run_one_operation_per_handler(&engine);

        assert_eq!(inner.evaluation.op_count(), 1);
        assert_eq!(inner.storage.op_count(), 1);
        assert_eq!(inner.json.op_count(), 1);
        assert_eq!(inner.parquet.op_count(), 1);
    }

    #[test]
    fn unconfigured_handlers_delegate_to_inner_allocations() {
        let inner = Arc::new(SyncEngine::new());
        let engine = DelegatingEngine::new(inner.clone());
        assert!(Arc::ptr_eq(
            &engine.evaluation_handler(),
            &inner.evaluation_handler()
        ));
        assert!(Arc::ptr_eq(
            &engine.storage_handler(),
            &inner.storage_handler()
        ));
        assert!(Arc::ptr_eq(&engine.json_handler(), &inner.json_handler()));
        assert!(Arc::ptr_eq(
            &engine.parquet_handler(),
            &inner.parquet_handler()
        ));
    }

    #[cfg(feature = "declarative-plans")]
    #[rstest]
    #[case::present(Arc::new(SyncEngine::new()))]
    #[case::absent(Arc::new(
        DelegatingEngine::new(Arc::new(SyncEngine::new())).without_plan_executor()
    ))]
    fn executor_is_inherited(#[case] inner: Arc<dyn Engine>) {
        let expected = inner.plan_executor();
        let engine = DelegatingEngine::new(inner);
        match expected {
            Some(expected) => {
                assert!(Arc::ptr_eq(&engine.plan_executor().unwrap(), &expected));
                assert!(Arc::ptr_eq(
                    &engine.require_plan_executor().unwrap(),
                    &expected
                ));
            }
            None => {
                assert!(engine.plan_executor().is_none());
                assert!(matches!(
                    engine.require_plan_executor(),
                    Err(KernelError::Unsupported(_))
                ));
            }
        }
    }

    #[cfg(feature = "declarative-plans")]
    #[rstest]
    fn last_executor_override_wins(#[values(false, true)] supplied_last: bool) {
        let inner = Arc::new(SyncEngine::new());
        let replacement_executor = SyncEngine::new().plan_executor().unwrap();
        let engine = DelegatingEngine::new(inner);
        let engine = if supplied_last {
            engine
                .without_plan_executor()
                .with_plan_executor(replacement_executor.clone())
        } else {
            engine
                .with_plan_executor(replacement_executor.clone())
                .without_plan_executor()
        };
        if supplied_last {
            assert!(Arc::ptr_eq(
                &engine.plan_executor().unwrap(),
                &replacement_executor
            ));
            assert!(Arc::ptr_eq(
                &engine.require_plan_executor().unwrap(),
                &replacement_executor
            ));
        } else {
            assert!(engine.plan_executor().is_none());
            assert!(matches!(
                engine.require_plan_executor(),
                Err(KernelError::Unsupported(_))
            ));
        }
    }
}
