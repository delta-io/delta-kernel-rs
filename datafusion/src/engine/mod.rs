use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_session::SessionStore;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

pub use self::evaluation::DataFusionEvaluationHandler;
pub use self::file_format::DataFusionFileFormatHandler;
pub use self::storage::DataFusionStorageHandler;

mod evaluation;
mod file_format;
mod storage;

pub struct DataFusionEngine<E: TaskExecutor> {
    pub(crate) evaluation_handler: Arc<DataFusionEvaluationHandler>,
    pub(crate) storage_handler: Arc<DataFusionStorageHandler<E>>,
    pub(crate) file_format_handler: Arc<DataFusionFileFormatHandler<E>>,
}

impl<E: TaskExecutor> DataFusionEngine<E> {
    /// Create a new [`DataFusionEngine`].
    pub fn new(task_executor: Arc<E>, ctx: &SessionContext) -> Self {
        let session_store = Arc::new(SessionStore::new());
        let evaluation_handler = Arc::new(DataFusionEvaluationHandler::new(session_store.clone()));
        let file_format_handler = Arc::new(DataFusionFileFormatHandler::new(
            task_executor.clone(),
            session_store.clone(),
        ));
        let storage_handler = Arc::new(DataFusionStorageHandler::new(
            session_store.clone(),
            task_executor.clone(),
        ));
        session_store.with_state(ctx.state_weak_ref());
        Self {
            evaluation_handler,
            file_format_handler,
            storage_handler,
        }
    }

    /// Create a new [`DataFusionEngine`] with a session store.
    ///
    /// The caller is responsible to update the session reference in the session store
    /// and assure it is not dropped prematurely.
    pub fn new_with_session_store(task_executor: Arc<E>, session_store: Arc<SessionStore>) -> Self {
        let evaluation_handler = Arc::new(DataFusionEvaluationHandler::new(session_store.clone()));
        let file_format_handler = Arc::new(DataFusionFileFormatHandler::new(
            task_executor.clone(),
            session_store.clone(),
        ));
        let storage_handler = Arc::new(DataFusionStorageHandler::new(
            session_store.clone(),
            task_executor.clone(),
        ));
        Self {
            evaluation_handler,
            file_format_handler,
            storage_handler,
        }
    }

    /// Create a new [`DataFusionEngine`] with a background executor.
    pub fn new_background(ctx: &SessionContext) -> Arc<dyn Engine> {
        let executor = Arc::new(TokioBackgroundExecutor::new());
        Arc::new(DataFusionEngine::new(executor, ctx))
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
