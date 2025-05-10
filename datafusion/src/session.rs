use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::TableReference;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_execution::object_store::ObjectStoreRegistry;
use datafusion_session::Session;
use datafusion_session::SessionStore;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::object_store::ObjectStore;
use delta_kernel::{Engine, Table};
use parking_lot::RwLock;
use tokio::runtime::RuntimeFlavor;
use url::Url;

use crate::engine::DataFusionEngine;
use crate::DeltaTableProvider;

pub struct EngineExtension {
    pub(crate) engine: Arc<dyn Engine>,
    object_store_factory: Option<Arc<dyn ObjectStoreFactory>>,
}

#[async_trait::async_trait]
impl ObjectStoreFactory for EngineExtension {
    async fn create_object_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        self.object_store_factory
            .as_ref()
            .ok_or_else(|| DataFusionError::Execution("no object store factory registered".into()))?
            .create_object_store(url)
            .await
    }
}

#[async_trait::async_trait]
impl<S: Session + ?Sized> KernelSessionExt for S {
    fn kernel_ext(&self) -> DFResult<Arc<EngineExtension>> {
        self.config()
            .get_extension::<EngineExtension>()
            .ok_or_else(|| DataFusionError::Execution("no engine extension found".into()))
    }

    async fn ensure_object_store(&self, url: &Url) -> DFResult<()> {
        let mut root_url = url.clone();
        root_url.set_path("/");
        let registry = self.runtime_env().object_store_registry.clone();
        let ext = self.kernel_ext()?;
        ensure_object_store(url, registry, ext).await
    }
}

#[async_trait::async_trait]
pub trait ObjectStoreFactory: Send + Sync + 'static {
    async fn create_object_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>>;
}

#[async_trait::async_trait]
pub trait KernelContextExt: private::KernelContextExtInner {
    fn enable_delta_kernel(self) -> SessionContext;

    async fn register_delta(
        &self,
        table_ref: impl Into<TableReference> + Send,
        url: &Url,
    ) -> DFResult<()>;
}

#[async_trait::async_trait]
impl KernelContextExt for SessionContext {
    fn enable_delta_kernel(self) -> SessionContext {
        if self
            .state_ref()
            .read()
            .config()
            .get_extension::<EngineExtension>()
            .is_none()
        {
            return enable_kernel_engine(self, None);
        }
        self
    }

    async fn register_delta(
        &self,
        table_ref: impl Into<TableReference> + Send,
        url: &Url,
    ) -> DFResult<()> {
        let table =
            Table::try_from_uri(url).map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.ensure_object_store(table.location()).await?;

        let engine = self.kernel_engine()?;

        // NB: Engine needs to list all fiels and read some log,
        // so we need to run it in a blocking thread.
        let snapshot = tokio::task::spawn_blocking(move || table.snapshot(engine.as_ref(), None))
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let provider = DeltaTableProvider::try_new(snapshot.into())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.register_table(table_ref, Arc::new(provider))?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait KernelSessionExt: Send + Sync {
    /// Get the engine extension for this session.
    ///
    /// Tries to get the kernel extension from the extension registry
    /// on [`SessionConfig`].
    fn kernel_ext(&self) -> DFResult<Arc<EngineExtension>>;

    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>> {
        Ok(self.kernel_ext()?.engine.clone())
    }

    async fn ensure_object_store(&self, url: &Url) -> DFResult<()>;
}

use private::KernelContextExtInner as _;
mod private {
    use super::*;

    #[async_trait::async_trait]
    pub trait KernelContextExtInner {
        /// Ensure that an object store is available for the given url.
        ///
        /// If no object store is currently registered, attempts to discover
        /// suitable credentials to construct an object store.
        async fn ensure_object_store(&self, url: &Url) -> DFResult<()>;

        fn kernel(&self) -> Arc<RwLock<dyn KernelSessionExt>>;

        fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>>;
    }
}

#[async_trait::async_trait]
impl private::KernelContextExtInner for SessionContext {
    async fn ensure_object_store(&self, url: &Url) -> DFResult<()> {
        let ext = self.kernel().read().kernel_ext()?;
        let registry = self.runtime_env().object_store_registry.clone();
        ensure_object_store(url, registry, ext).await
    }

    fn kernel(&self) -> Arc<RwLock<dyn KernelSessionExt>> {
        self.state_ref()
    }

    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>> {
        Ok(self.kernel().read().kernel_engine()?)
    }
}

async fn ensure_object_store(
    url: &Url,
    registry: Arc<dyn ObjectStoreRegistry>,
    kernel: Arc<EngineExtension>,
) -> DFResult<()> {
    let mut root_url = url.clone();
    root_url.set_path("/");

    if registry.get_store(&root_url).is_err() {
        let store = kernel.create_object_store(&root_url).await?;
        registry.register_store(url, store);
    }

    Ok(())
}

pub fn get_engine(config: &SessionConfig) -> Result<Arc<dyn Engine>, DataFusionError> {
    config
        .get_extension::<EngineExtension>()
        .as_ref()
        .map(|ext| ext.engine.clone())
        .ok_or_else(|| DataFusionError::Execution("no engine extension found".into()))
}

fn with_engine(
    ctx: SessionContext,
    engine: Arc<dyn Engine>,
    object_store_factory: Option<Arc<dyn ObjectStoreFactory>>,
) -> SessionContext {
    let session_id = ctx.session_id().clone();
    let mut new_config = ctx.copied_config();
    new_config.set_extension(Arc::new(EngineExtension {
        engine,
        object_store_factory,
    }));

    let ctx: SessionContext = ctx
        .into_state_builder()
        .with_session_id(session_id)
        .with_config(new_config)
        .build()
        .into();
    ctx
}

fn enable_kernel_engine(
    ctx: SessionContext,
    object_store_factory: Option<Arc<dyn ObjectStoreFactory>>,
) -> SessionContext {
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

    let ctx = with_engine(ctx, engine, object_store_factory);
    session_store.with_state(ctx.state_weak_ref());

    ctx
}
