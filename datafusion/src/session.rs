use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_session::SessionStore;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::Engine;
use tokio::runtime::RuntimeFlavor;

use crate::engine::DataFusionEngine;

pub(crate) struct EngineExtension {
    pub(crate) engine: Arc<dyn Engine>,
}

pub trait KernelSessionExt {
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
    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>>;

    fn enable_kernel_engine(self) -> SessionContext;
}

impl KernelSessionExt for SessionContext {
    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>> {
        get_engine(self.state().config())
    }

    fn enable_kernel_engine(self) -> SessionContext {
        let handle = tokio::runtime::Handle::current();
        let session_store = Arc::new(SessionStore::new());

        let engine: Arc<dyn Engine> = match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => Arc::new(DataFusionEngine::new_with_session_store(
                Arc::new(TokioMultiThreadExecutor::new(handle)),
                session_store.clone(),
            )),
            RuntimeFlavor::CurrentThread => Arc::new(DataFusionEngine::new_with_session_store(
                Arc::new(TokioBackgroundExecutor::new()),
                session_store.clone(),
            )),
            _ => panic!("unsupported runtime flavor"),
        };

        let ctx = with_engine(self, engine);
        session_store.with_state(ctx.state_weak_ref());
        ctx
    }
}

pub fn get_engine(config: &SessionConfig) -> Result<Arc<dyn Engine>, DataFusionError> {
    config
        .get_extension::<EngineExtension>()
        .as_ref()
        .map(|ext| ext.engine.clone())
        .ok_or_else(|| DataFusionError::Execution("no engine extension found".into()))
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
