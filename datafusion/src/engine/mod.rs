use std::sync::Arc;

use datafusion::execution::SessionState;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};
use parking_lot::RwLock;

use self::evaluation::DataFusionEvaluationHandler;
use self::file_format::DataFusionFileFormatHandler;
use self::storage::DataFusionStorageHandler;

mod evaluation;
mod file_format;
mod storage;

pub struct DataFusionEngine<E: TaskExecutor> {
    evaluation_handler: Arc<DataFusionEvaluationHandler>,
    storage_handler: Arc<DataFusionStorageHandler<E>>,
    file_format_handler: Arc<DataFusionFileFormatHandler<E>>,
}

impl<E: TaskExecutor> DataFusionEngine<E> {
    /// Create a new [`DataFusionEngine`].
    pub fn new(task_executor: Arc<E>, state: Arc<RwLock<SessionState>>) -> Self {
        let evaluation_handler = Arc::new(DataFusionEvaluationHandler {
            state: state.clone(),
        });
        let file_format_handler = Arc::new(DataFusionFileFormatHandler::new(
            state.clone(),
            task_executor.clone(),
        ));
        let storage_handler = Arc::new(DataFusionStorageHandler::new(
            state.read().runtime_env().object_store_registry.clone(),
            task_executor.clone(),
        ));
        Self {
            evaluation_handler,
            file_format_handler,
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
        self.file_format_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.file_format_handler.clone()
    }
}
