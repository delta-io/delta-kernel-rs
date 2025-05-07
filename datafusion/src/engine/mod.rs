use std::sync::Arc;

use datafusion_execution::runtime_env::RuntimeEnv;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

pub struct DataFusionEngine<E: TaskExecutor> {
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// Datafusion [`RuntimeEnv`] that manages system resources
    /// such as memory, disk, cache and storage.
    runtime_env: Arc<RuntimeEnv>,
}

impl<E: TaskExecutor> DataFusionEngine<E> {
    /// Create a new [`DataFusionEngine`].
    pub fn new(task_executor: Arc<E>, runtime_env: Arc<RuntimeEnv>) -> Self {
        Self {
            task_executor,
            runtime_env,
        }
    }
}

impl<E: TaskExecutor> Engine for DataFusionEngine<E> {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        todo!()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        todo!()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        todo!()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        todo!()
    }
}
