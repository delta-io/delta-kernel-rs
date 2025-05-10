use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_session::SessionStore;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::Engine;
use tokio::runtime::RuntimeFlavor;

use crate::engine::{
    DataFusionEngine, DataFusionEvaluationHandler, DataFusionFileFormatHandler,
    DataFusionStorageHandler,
};

pub(crate) struct EngineExtension {
    pub(crate) engine: Arc<dyn Engine>,
}

pub trait KernelSession {
    /// Create a new delta kernel engine for this session.
    ///
    /// The created engine shares the session state with the session
    /// it is generated with. Specifically:
    /// * Object store instances are acquired via the session's object store registry
    /// * Expressions can access resources registered with the session.
    ///
    /// Any new stores registered via the session will also be available to the engine.
    /// However the engine does maintain also maintain internal state, thus it is recommened
    /// to create an engine from the session context only once.
    ///
    /// The current tokio runtime flavor determines the type of task executor
    /// used by the engine. This requires a runtime is active when this method
    /// is called.
    ///
    /// * `CurrentThread` - A dedciated background runtime is used.
    /// * `MultiThread` - A shared haldle is used to create a multi-threaded executor.
    fn kernel_engine(&self) -> Arc<dyn Engine>;

    fn enable_kernel_engine(self) -> DFResult<SessionContext>;
}

impl KernelSession for SessionContext {
    fn kernel_engine(&self) -> Arc<dyn Engine> {
        self.state()
            .config()
            .get_extension::<EngineExtension>()
            .as_ref()
            .map(|ext| ext.engine.clone())
            .unwrap_or_else(|| {
                tracing::warn!(
                    "No engine extension found, creating a new one. This is not recommended."
                );
                let handle = tokio::runtime::Handle::current();
                match handle.runtime_flavor() {
                    RuntimeFlavor::CurrentThread => {
                        let executor = Arc::new(TokioBackgroundExecutor::new());
                        Arc::new(DataFusionEngine::new(executor, self))
                    }
                    RuntimeFlavor::MultiThread => {
                        let executor = Arc::new(TokioMultiThreadExecutor::new(handle));
                        Arc::new(DataFusionEngine::new(executor, self))
                    }
                    _ => panic!("unsupported runtime flavor"),
                }
            })
    }

    fn enable_kernel_engine(self) -> DFResult<SessionContext> {
        let handle = tokio::runtime::Handle::current();
        match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => enable_kernel_engine_multi_thread(self, handle),
            RuntimeFlavor::CurrentThread => enable_kernel_engine_background(self),
            _ => panic!("unsupported runtime flavor"),
        }
    }
}

fn enable_kernel_engine_multi_thread(
    ctx: SessionContext,
    handle: tokio::runtime::Handle,
) -> DFResult<SessionContext> {
    let executor = match handle.runtime_flavor() {
        RuntimeFlavor::MultiThread => Arc::new(TokioMultiThreadExecutor::new(handle)),
        _ => {
            return Err(DataFusionError::Execution(
                "unsuppoerted runtime flavor".into(),
            ))
        }
    };

    let runtime_env = ctx.runtime_env().object_store_registry.clone();
    let evaluation_handler = Arc::new(DataFusionEvaluationHandler::new(SessionStore::new()));
    let storage_handler = Arc::new(DataFusionStorageHandler::new(
        runtime_env.clone(),
        executor.clone(),
    ));
    let file_format_handler = Arc::new(DataFusionFileFormatHandler::new(
        executor.clone(),
        SessionStore::new(),
    ));
    let engine = Arc::new(DataFusionEngine {
        evaluation_handler: evaluation_handler.clone(),
        storage_handler,
        file_format_handler: file_format_handler.clone(),
    });

    let ctx = with_engine(ctx, engine);

    evaluation_handler
        .session_store()
        .with_state(ctx.state_weak_ref());
    file_format_handler
        .session_store()
        .with_state(ctx.state_weak_ref());

    Ok(ctx)
}

fn enable_kernel_engine_background(ctx: SessionContext) -> DFResult<SessionContext> {
    let executor = Arc::new(TokioBackgroundExecutor::new());

    let runtime_env = ctx.runtime_env().object_store_registry.clone();
    let evaluation_handler = Arc::new(DataFusionEvaluationHandler::new(SessionStore::new()));
    let storage_handler = Arc::new(DataFusionStorageHandler::new(
        runtime_env.clone(),
        executor.clone(),
    ));
    let file_format_handler = Arc::new(DataFusionFileFormatHandler::new(
        executor.clone(),
        SessionStore::new(),
    ));
    let engine = Arc::new(DataFusionEngine {
        evaluation_handler: evaluation_handler.clone(),
        storage_handler,
        file_format_handler: file_format_handler.clone(),
    });

    let ctx = with_engine(ctx, engine);

    evaluation_handler
        .session_store()
        .with_state(ctx.state_weak_ref());
    file_format_handler
        .session_store()
        .with_state(ctx.state_weak_ref());

    Ok(ctx)
}

fn with_engine(ctx: SessionContext, engine: Arc<dyn Engine>) -> SessionContext {
    let session_id = ctx.session_id().clone();
    let mut new_config = ctx.copied_config();
    new_config.set_extension(Arc::new(EngineExtension { engine }));

    let ctx: SessionContext = ctx
        .into_state_builder()
        .with_session_id(session_id)
        .with_config(new_config)
        .build()
        .into();
    ctx
}
