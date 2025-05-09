use std::sync::Arc;

use bytes::Bytes;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use datafusion_execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::engine::default::filesystem::ObjectStoreStorageHandler;
use delta_kernel::{DeltaResult, Error as DeltaError, FileMeta, FileSlice, StorageHandler};
use itertools::Itertools;
use url::Url;

use crate::utils::{group_by_store, AsObjectStoreUrl};

pub struct DataFusionStorageHandler<E: TaskExecutor> {
    /// Object store registry shared with datafusion session
    stores: Arc<dyn ObjectStoreRegistry>,
    /// Registry of object store handlers
    registry: Arc<DashMap<ObjectStoreUrl, Arc<ObjectStoreStorageHandler<E>>>>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
}

impl<E: TaskExecutor> DataFusionStorageHandler<E> {
    /// Create a new [`DataFusionStorageHandler`] instance.
    pub fn new(stores: Arc<dyn ObjectStoreRegistry>, task_executor: Arc<E>) -> Self {
        Self {
            stores,
            registry: DashMap::new().into(),
            task_executor,
        }
    }

    fn get_or_create(
        &self,
        url: ObjectStoreUrl,
    ) -> DeltaResult<Ref<'_, ObjectStoreUrl, Arc<ObjectStoreStorageHandler<E>>>> {
        if let Some(handler) = self.registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .stores
            .get_store(url.as_ref())
            .map_err(DeltaError::generic_err)?;
        self.registry.insert(
            url.clone(),
            Arc::new(ObjectStoreStorageHandler::new(
                store,
                self.task_executor.clone(),
            )),
        );
        Ok(self.registry.get(&url).unwrap())
    }
}

impl<E: TaskExecutor> StorageHandler for DataFusionStorageHandler<E> {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        self.get_or_create(path.as_object_store_url())?
            .list_from(path)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let grouped_files = group_by_store(files);
        Ok(Box::new(
            grouped_files
                .into_iter()
                .map(|(url, files)| self.get_or_create(url)?.read_files(files))
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }
}
