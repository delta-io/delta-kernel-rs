use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::{DynObjectStore, ObjectStore};
use url::Url;

use crate::{DeltaResult, Error};

pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
    fn get_store(&self, url: &Url) -> DeltaResult<(Arc<DynObjectStore>, bool)>;
}

/// The default [`ObjectStoreRegistry`]
pub struct DefaultObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    object_stores: HashMap<String, (Arc<dyn ObjectStore>, bool)>,
}

impl std::fmt::Debug for DefaultObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultObjectStoreRegistry")
            .field(
                "schemes",
                &self.object_stores.keys().cloned().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultObjectStoreRegistry {
    /// This will register [`LocalFileSystem`] to handle `file://` paths
    pub fn new() -> Self {
        let mut object_stores: HashMap<String, (Arc<dyn ObjectStore>, bool)> = HashMap::new();
        object_stores.insert(
            "file://".to_string(),
            (Arc::new(LocalFileSystem::new()), false),
        );
        Self { object_stores }
    }

    /// This will register [`LocalFileSystem`] to handle `file://` and `memory://` paths
    #[cfg(test)]
    pub fn new_test() -> Self {
        use object_store::memory::InMemory;
        let mut object_stores: HashMap<String, (Arc<dyn ObjectStore>, bool)> = HashMap::new();
        object_stores.insert(
            "file://".to_string(),
            (Arc::new(LocalFileSystem::new()), false),
        );
        object_stores.insert("memory://".to_string(), (Arc::new(InMemory::new()), false));
        Self { object_stores }
    }

    #[cfg(test)]
    pub fn get_memory(&self) -> Arc<dyn ObjectStore> {
        self.object_stores
            .get("memory://")
            .map(|(s, _flag)| Arc::clone(s))
            .expect("memory store not found")
    }

    #[cfg(test)]
    pub fn get_local(&self) -> Arc<dyn ObjectStore> {
        self.object_stores
            .get("file://")
            .map(|(s, _flag)| Arc::clone(s))
            .expect("file store not found")
    }

    pub fn register_store(
        &mut self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        // HACK to check if we're using a LocalFileSystem from ObjectStore. We need this because
        // local filesystem doesn't return a sorted list by default. Although the `object_store`
        // crate explicitly says it _does not_ return a sorted listing, in practice all the cloud
        // implementations actually do:
        // - AWS:
        //   [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
        //   states: "For general purpose buckets, ListObjectsV2 returns objects in lexicographical
        //   order based on their key names." (Directory buckets are out of scope for now)
        // - Azure: Docs state
        //   [here](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources):
        //   "A listing operation returns an XML response that contains all or part of the requested
        //   list. The operation returns entities in alphabetical order."
        // - GCP: The [main](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) doc
        //   doesn't indicate order, but [this
        //   page](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) does say: "This page
        //   shows you how to list the [objects](https://cloud.google.com/storage/docs/objects) stored
        //   in your Cloud Storage buckets, which are ordered in the list lexicographically by name."
        // So we just need to know if we're local and then if so, we wrap the store in an OrderedObjectStore
        let store_str = format!("{}", store);
        let is_local = store_str.starts_with("LocalFileSystem");
        self.object_stores
            .insert(s, (store, !is_local))
            .map(|(s, _)| s)
    }
}

/// Stores are registered based on the scheme, host and port of the provided URL
/// with a [`LocalFileSystem::new`] automatically registered for `file://` (if the
/// target arch is not `wasm32`).
///
/// For example:
///
/// - `file:///my_path` will return the default LocalFS store
/// - `s3://bucket/path` will return a store registered with `s3://bucket` if any
/// - `hdfs://host:port/path` will return a store registered with `hdfs://host:port` if any
impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn get_store(&self, url: &Url) -> DeltaResult<(Arc<dyn ObjectStore>, bool)> {
        let s = get_url_key(url);
        self.object_stores
            .get(&s)
            .map(|(s, flag)| (Arc::clone(s), *flag))
            .ok_or_else(|| Error::Generic(format!("No object store registered for {}", url)))
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}
