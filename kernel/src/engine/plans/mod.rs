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

use crate::engine::arrow_expression::ArrowEvaluationHandler;
use crate::plans::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// An [`Engine`] that routes operations through a [`PlanExecutor`].
///
/// Storage, JSON file reads, and Parquet file reads are converted into
/// [`Operation`](crate::plans::Operation)s and delegated to the plan executor.
///
/// Operations not yet implemented on the plan-execution path (e.g. `write_json_file`,
/// `write_parquet_file`) are delegated to `fallback` when one is configured, and otherwise return
/// an unsupported error. A fallback is optional because a connector may be unable to construct one:
/// an FFI-backed engine resolves table paths itself, which fails for a path only the connector can
/// resolve.
pub struct PlanBasedEngine {
    executor: Arc<dyn PlanExecutor>,
    evaluation: Arc<dyn EvaluationHandler>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<PlanBasedJsonHandler>,
    parquet: Arc<PlanBasedParquetHandler>,
}

impl PlanBasedEngine {
    /// Construct a `PlanBasedEngine` backed by `plan_executor`, delegating operations not yet
    /// implemented on the plan-execution path to `fallback` when it is provided.
    ///
    /// When `fallback` is `None`, those operations return an unsupported error. Evaluation uses the
    /// fallback's [`EvaluationHandler`] when one is available and otherwise uses the Arrow
    /// implementation.
    pub fn new(fallback: Option<Arc<dyn Engine>>, plan_executor: Arc<dyn PlanExecutor>) -> Self {
        let evaluation: Arc<dyn EvaluationHandler> = fallback.as_ref().map_or_else(
            || Arc::new(ArrowEvaluationHandler) as Arc<dyn EvaluationHandler>,
            |engine| engine.evaluation_handler(),
        );
        Self {
            evaluation,
            storage: Arc::new(PlanBasedStorageHandler::new(plan_executor.clone())),
            json: Arc::new(PlanBasedJsonHandler::new(
                plan_executor.clone(),
                fallback.as_ref().map(|engine| engine.json_handler()),
            )),
            parquet: Arc::new(PlanBasedParquetHandler::new(
                plan_executor.clone(),
                fallback.as_ref().map(|engine| engine.parquet_handler()),
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

    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        Some(self.executor.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use url::Url;

    use super::PlanBasedEngine;
    use crate::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::{Engine as _, EngineData, Error};

    fn plan_engine(fallback: Option<Arc<dyn crate::Engine>>) -> PlanBasedEngine {
        PlanBasedEngine::new(fallback, Arc::new(SyncPlanExecutor::default()))
    }

    #[test]
    fn uses_fallback_evaluation_handler_when_available() {
        let fallback: Arc<dyn crate::Engine> = Arc::new(SyncEngine::new());
        let expected_evaluation_handler = fallback.evaluation_handler();
        let engine = plan_engine(Some(fallback));

        assert!(Arc::ptr_eq(
            &engine.evaluation_handler(),
            &expected_evaluation_handler
        ));
    }

    #[test]
    fn uses_arrow_evaluation_handler_without_fallback() {
        let engine = plan_engine(None);

        assert!(engine
            .evaluation_handler()
            .as_ref()
            .any_ref()
            .is::<ArrowEvaluationHandler>());
    }

    #[test]
    fn unimplemented_writes_delegate_to_fallback() {
        let dir = tempdir().unwrap();
        let engine = plan_engine(Some(Arc::new(SyncEngine::new())));
        let json_location = Url::from_file_path(dir.path().join("out.json")).unwrap();
        let json_batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(StringArray::from(vec!["one"])) as Arc<dyn Array>,
        )])
        .unwrap();
        let json_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(json_batch));

        engine
            .json_handler()
            .write_json_file(
                &json_location,
                Box::new(std::iter::once(Ok(
                    FilteredEngineData::with_all_rows_selected(json_data),
                ))),
                false,
            )
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(json_location.to_file_path().unwrap()).unwrap(),
            "{\"value\":\"one\"}\n"
        );

        let parquet_location = Url::from_file_path(dir.path().join("out.parquet")).unwrap();
        let parquet_batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![1])) as Arc<dyn Array>,
        )])
        .unwrap();
        let parquet_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(parquet_batch));

        engine
            .parquet_handler()
            .write_parquet_file(
                parquet_location.clone(),
                Box::new(std::iter::once(Ok(parquet_data))),
            )
            .unwrap();
        assert!(
            std::fs::metadata(parquet_location.to_file_path().unwrap())
                .unwrap()
                .len()
                > 0
        );
    }

    #[test]
    fn unimplemented_writes_are_unsupported_without_fallback() {
        let engine = plan_engine(None);
        let location = Url::parse("memory:///table/out").unwrap();

        let json_err = engine
            .json_handler()
            .write_json_file(&location, Box::new(std::iter::empty()), false)
            .expect_err("no fallback is configured");
        assert!(matches!(json_err, Error::Unsupported(_)));

        let parquet_err = engine
            .parquet_handler()
            .write_parquet_file(location, Box::new(std::iter::empty()))
            .expect_err("no fallback is configured");
        assert!(matches!(parquet_err, Error::Unsupported(_)));
    }
}
