use std::sync::Arc;

use datafusion::execution::SessionState;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};
use parking_lot::RwLock;

use self::evaluation::DataFusionEvaluationHandler;
use self::json::DataFusionJsonHandler;

mod evaluation;
mod json;

pub struct DataFusionEngine<E: TaskExecutor> {
    evaluation_handler: Arc<DataFusionEvaluationHandler>,

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
        Self {
            evaluation_handler,
            json_handler,
        }
    }
}

impl<E: TaskExecutor> Engine for DataFusionEngine<E> {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation_handler.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        todo!()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        todo!()
    }
}
