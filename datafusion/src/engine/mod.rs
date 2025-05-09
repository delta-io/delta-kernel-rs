use std::sync::Arc;

use datafusion::execution::SessionState;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};
use parking_lot::RwLock;

use self::evaluation::DataFusionEvaluationHandler;
use self::json::DataFusionJsonHandler;
use self::storage::DataFusionStorageHandler;
mod evaluation;
mod json;
mod storage;

pub struct DataFusionEngine<E: TaskExecutor> {
    evaluation_handler: Arc<DataFusionEvaluationHandler>,
    storage_handler: Arc<DataFusionStorageHandler<E>>,
    json_handler: Arc<DataFusionJsonHandler<E>>,
}

impl<E: TaskExecutor> DataFusionEngine<E> {
    /// Create a new [`DataFusionEngine`].
    pub fn new(task_executor: Arc<E>, state: Arc<RwLock<SessionState>>) -> Self {
        let evaluation_handler = Arc::new(DataFusionEvaluationHandler {
            state: state.clone(),
        });
        let json_handler = Arc::new(DataFusionJsonHandler::new(
            state.clone(),
            task_executor.clone(),
        ));
        let storage_handler = Arc::new(DataFusionStorageHandler::new(
            state.read().runtime_env().object_store_registry.clone(),
            task_executor.clone(),
        ));
        Self {
            evaluation_handler,
            json_handler,
            storage_handler,
        }
    }
}

impl<E: TaskExecutor> Engine for DataFusionEngine<E> {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation_handler.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage_handler.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        todo!()
    }
}
