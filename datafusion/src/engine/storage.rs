use std::sync::Arc;

use bytes::Bytes;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use datafusion_execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use datafusion_session::SessionStore;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::engine::default::filesystem::ObjectStoreStorageHandler;
use delta_kernel::{DeltaResult, Error as DeltaError, FileMeta, FileSlice, StorageHandler};
use itertools::Itertools;
use url::Url;

use crate::utils::{group_by_store, AsObjectStoreUrl};

pub struct DataFusionStorageHandler<E: TaskExecutor> {
    /// Object store registry shared with datafusion session
    session_store: Arc<SessionStore>,
    /// Registry of object store handlers
    registry: Arc<DashMap<ObjectStoreUrl, Arc<ObjectStoreStorageHandler<E>>>>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
}

impl<E: TaskExecutor> DataFusionStorageHandler<E> {
    /// Create a new [`DataFusionStorageHandler`] instance.
    pub fn new(session_store: Arc<SessionStore>, task_executor: Arc<E>) -> Self {
        Self {
            session_store,
            registry: DashMap::new().into(),
            task_executor,
        }
    }

    fn registry(&self) -> DeltaResult<Arc<dyn ObjectStoreRegistry>> {
        self.session_store
            .get_session()
            .upgrade()
            .ok_or_else(|| DeltaError::generic_err("no session found"))
            .map(|session| session.read().runtime_env().object_store_registry.clone())
    }

    fn get_or_create(
        &self,
        url: ObjectStoreUrl,
    ) -> DeltaResult<Ref<'_, ObjectStoreUrl, Arc<ObjectStoreStorageHandler<E>>>> {
        if let Some(handler) = self.registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .registry()?
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

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use datafusion::prelude::SessionContext;
    use delta_kernel::{
        object_store::{local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore},
        Engine,
    };
    use rstest::*;

    use super::*;
    use crate::tests::df_engine;

    pub fn delta_path_for_version(version: u64, suffix: &str) -> Path {
        let path = format!("_delta_log/{version:020}.{suffix}");
        Path::from(path.as_str())
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_files(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let tmp = tempfile::tempdir().unwrap();
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();

        let data = Bytes::from("kernel-data");
        tmp_store
            .put(&Path::from("a"), data.clone().into())
            .await
            .unwrap();
        tmp_store
            .put(&Path::from("b"), data.clone().into())
            .await
            .unwrap();
        tmp_store
            .put(&Path::from("c"), data.clone().into())
            .await
            .unwrap();

        let mut url = Url::from_directory_path(tmp.path()).unwrap();

        let (engine, _ctx) = df_engine;
        let storage = engine.storage_handler();

        let mut slices: Vec<FileSlice> = Vec::new();

        let mut url1 = url.clone();
        url1.set_path(&format!("{}/b", url.path()));
        slices.push((url1.clone(), Some(Range { start: 0, end: 6 })));
        slices.push((url1, Some(Range { start: 7, end: 11 })));

        url.set_path(&format!("{}/c", url.path()));
        slices.push((url, Some(Range { start: 4, end: 9 })));
        let data: Vec<Bytes> = storage.read_files(slices).unwrap().try_collect().unwrap();

        assert_eq!(data.len(), 3);
        assert_eq!(data[0], Bytes::from("kernel"));
        assert_eq!(data[1], Bytes::from("data"));
        assert_eq!(data[2], Bytes::from("el-da"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_default_engine_listing(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let tmp = tempfile::tempdir().unwrap();
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
        let data = Bytes::from("kernel-data");

        let expected_names: Vec<Path> =
            (0..10).map(|i| delta_path_for_version(i, "json")).collect();

        // put them in in reverse order
        for name in expected_names.iter().rev() {
            tmp_store.put(name, data.clone().into()).await.unwrap();
        }

        let url = Url::from_directory_path(tmp.path()).unwrap();
        let (engine, _ctx) = df_engine;
        let files = engine
            .storage_handler()
            .list_from(&url.join("_delta_log").unwrap().join("0").unwrap())
            .unwrap();
        let mut len = 0;
        for (file, expected) in files.zip(expected_names.iter()) {
            assert!(
                file.as_ref()
                    .unwrap()
                    .location
                    .path()
                    .ends_with(expected.as_ref()),
                "{} does not end with {}",
                file.unwrap().location.path(),
                expected
            );
            len += 1;
        }
        assert_eq!(len, 10, "list_from should have returned 10 files");
    }

    #[rstest]
    #[tokio::test]
    async fn test_register_store(df_engine: (Arc<dyn Engine>, SessionContext)) {
        let store = Arc::new(InMemory::new());
        let file = Path::from("a");
        store
            .put(&file, Bytes::from("kernel-data").into())
            .await
            .unwrap();

        let (engine, ctx) = df_engine;

        // The in memory store is not registered by default on the session.
        // so we can register our own.
        ctx.register_object_store(&Url::parse("memory:///").unwrap(), store);

        // assert that stores registered on the session are available to the engine.
        let url = Url::parse("memory:///a").unwrap();
        let data: Vec<Bytes> = engine
            .storage_handler()
            .read_files(vec![(url, None)])
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(data, vec![Bytes::from("kernel-data")]);
    }
}
