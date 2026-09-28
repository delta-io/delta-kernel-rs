use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use delta_kernel::object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use delta_kernel::object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use delta_kernel::object_store::gcp::{
    GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey,
};
use delta_kernel::object_store::list::PaginatedListStore;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{self, DynObjectStore, Error, ObjectStore, ObjectStoreScheme};
use delta_kernel::{DeltaResult, Error as DeltaError};
use url::Url;

/// Object-store handles used by a [`DefaultEngine`](crate::DefaultEngine).
///
/// The optional paginated handle lets cloud backends apply both the directory delimiter and the
/// starting offset in the storage request. Without it, listing uses
/// [`ObjectStore::list_with_offset`] and removes nested descendants client-side.
pub struct EngineStore {
    pub(crate) object_store: Arc<DynObjectStore>,
    pub(crate) paginated: Option<Arc<dyn PaginatedListStore>>,
}

impl std::fmt::Debug for EngineStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStore")
            .field("object_store", &self.object_store)
            .field("paginated", &self.paginated.is_some())
            .finish()
    }
}

impl EngineStore {
    /// Create a store without provider-specific paginated listing support.
    ///
    /// Offset pushdown remains available through [`ObjectStore::list_with_offset`], but stores
    /// whose offset listing is recursive may still retrieve nested descendants.
    pub fn plain(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            paginated: None,
        }
    }

    /// Create a store that supports provider-specific paginated listing.
    ///
    /// The same store handles ordinary object operations and paginated listing, preventing the two
    /// handles from referring to different backends.
    pub fn with_paginated<S: ObjectStore + PaginatedListStore + 'static>(store: Arc<S>) -> Self {
        Self {
            object_store: store.clone(),
            paginated: Some(store),
        }
    }

    /// Create a store for `url` using the provider-specific `options`.
    ///
    /// Built-in S3, GCS, and Azure stores retain paginated listing support. Registered custom URL
    /// handlers and other built-in stores use [`Self::plain`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URL, storage configuration, or custom URL handler is invalid.
    pub fn from_url_opts<I, K, V>(url: &Url, options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let has_custom_handler = URL_REGISTRY
            .read()
            .map(|handlers| handlers.contains_key(url.scheme()))
            .unwrap_or(false);
        if has_custom_handler {
            return Ok(Self::plain(store_from_url_opts(url, options)?));
        }

        let (scheme, _path) = ObjectStoreScheme::parse(url).map_err(object_store::Error::from)?;
        let options = options
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_string(), value.into()));
        macro_rules! paginated {
            ($builder:expr) => {
                Self::with_paginated(Arc::new(build_cloud_store($builder, url, options)?))
            };
        }
        Ok(match scheme {
            ObjectStoreScheme::AmazonS3 => paginated!(AmazonS3Builder::new()),
            ObjectStoreScheme::GoogleCloudStorage => paginated!(GoogleCloudStorageBuilder::new()),
            ObjectStoreScheme::MicrosoftAzure => paginated!(MicrosoftAzureBuilder::new()),
            _ => Self::plain(store_from_url_opts(url, options)?),
        })
    }
}

impl From<Arc<DynObjectStore>> for EngineStore {
    fn from(object_store: Arc<DynObjectStore>) -> Self {
        Self::plain(object_store)
    }
}

impl<S: ObjectStore + 'static> From<Arc<S>> for EngineStore {
    fn from(object_store: Arc<S>) -> Self {
        Self::plain(object_store)
    }
}

/// Alias for convenience
type ClosureReturn = Result<(Box<dyn ObjectStore>, Path), Error>;
/// This type alias makes it easier to reference the handler closure(s)
///
/// It uses a HashMap<String, String> which _must_ be converted in [store_from_url_opts]
/// because we cannot use generics in this scenario.
type HandlerClosure = Arc<dyn Fn(&Url, HashMap<String, String>) -> ClosureReturn + Send + Sync>;
/// hashmap containing scheme => handler fn mappings to allow consumers of delta-kernel-rs provide
/// their own url opts parsers for different scemes
type Handlers = HashMap<String, HandlerClosure>;
/// The URL_REGISTRY contains the custom URL scheme handlers that will parse URL options
static URL_REGISTRY: LazyLock<RwLock<Handlers>> = LazyLock::new(|| RwLock::new(HashMap::default()));

/// Insert a new URL handler for [store_from_url_opts] with the given `scheme`. This allows
/// users to provide their own custom URL handler to plug new
/// [delta_kernel::object_store::ObjectStore] instances into delta-kernel, which is used by
/// [store_from_url_opts] to parse the URL.
pub fn insert_url_handler(
    scheme: impl AsRef<str>,
    handler_closure: HandlerClosure,
) -> Result<(), DeltaError> {
    let Ok(mut registry) = URL_REGISTRY.write() else {
        return Err(DeltaError::generic(
            "failed to acquire lock for adding a URL handler!",
        ));
    };
    registry.insert(scheme.as_ref().into(), handler_closure);
    Ok(())
}

/// Create an [`ObjectStore`] from a URL.
///
/// Returns an `Arc<dyn ObjectStore>` ready to use with [`crate::DefaultEngine`].
/// For cloud engines, use [`EngineStore::from_url_opts`] to retain non-recursive offset listing.
///
/// This function checks for custom URL handlers registered via [`insert_url_handler`]
/// before falling back to [`object_store`]'s default behavior.
///
/// # Example
///
/// ```rust
/// # use url::Url;
/// # use delta_kernel_default_engine::storage::store_from_url;
/// # use delta_kernel::DeltaResult;
/// # fn example() -> DeltaResult<()> {
/// let url = Url::parse("file:///path/to/table")?;
/// let store = store_from_url(&url)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url(url: &Url) -> delta_kernel::DeltaResult<Arc<dyn ObjectStore>> {
    store_from_url_opts(url, std::iter::empty::<(&str, &str)>())
}

/// Create an [`ObjectStore`] from a URL with custom options.
///
/// Returns an `Arc<dyn ObjectStore>` ready to use with [`crate::DefaultEngine`].
/// For cloud engines, use [`EngineStore::from_url_opts`] to retain non-recursive offset listing.
///
/// This function checks for custom URL handlers registered via [`insert_url_handler`]
/// before falling back to [`object_store`]'s default behavior.
///
/// # Example
///
/// ```rust
/// # use url::Url;
/// # use std::collections::HashMap;
/// # use delta_kernel_default_engine::storage::store_from_url_opts;
/// # use delta_kernel::DeltaResult;
/// # fn example() -> DeltaResult<()> {
/// let url = Url::parse("s3://my-bucket/path/to/table")?;
/// let options = HashMap::from([("region", "us-west-2")]);
/// let store = store_from_url_opts(&url, options)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url_opts<I, K, V>(
    url: &Url,
    options: I,
) -> delta_kernel::DeltaResult<Arc<dyn ObjectStore>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    // First attempt to use any schemes registered via insert_url_handler,
    // falling back to the default behavior of delta_kernel::object_store::parse_url_opts
    let (store, _path) = if let Ok(handlers) = URL_REGISTRY.read() {
        if let Some(handler) = handlers.get(url.scheme()) {
            let options = options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.into()))
                .collect();
            handler(url, options)?
        } else {
            object_store::parse_url_opts(url, options)?
        }
    } else {
        object_store::parse_url_opts(url, options)?
    };

    Ok(Arc::new(store))
}

/// Builds a concrete cloud store without erasing its [`PaginatedListStore`] implementation.
fn build_cloud_store<B: CloudBuilder>(
    builder: B,
    url: &Url,
    options: impl IntoIterator<Item = (String, String)>,
) -> DeltaResult<B::Store> {
    let builder = options.into_iter().fold(
        builder.with_url(url.to_string()),
        |builder, (key, value)| match key.to_ascii_lowercase().parse() {
            Ok(config_key) => builder.with_config(config_key, value),
            Err(_) => builder,
        },
    );
    Ok(builder.build()?)
}

trait CloudBuilder: Sized {
    type ConfigKey: std::str::FromStr;
    type Store: ObjectStore + PaginatedListStore;

    fn with_url(self, url: String) -> Self;
    fn with_config(self, key: Self::ConfigKey, value: String) -> Self;
    fn build(self) -> object_store::Result<Self::Store>;
}

macro_rules! impl_cloud_builder {
    ($builder:ty, $key:ty, $store:ty) => {
        impl CloudBuilder for $builder {
            type ConfigKey = $key;
            type Store = $store;

            fn with_url(self, url: String) -> Self {
                self.with_url(url)
            }

            fn with_config(self, key: Self::ConfigKey, value: String) -> Self {
                self.with_config(key, value)
            }

            fn build(self) -> object_store::Result<Self::Store> {
                self.build()
            }
        }
    };
}

impl_cloud_builder!(AmazonS3Builder, AmazonS3ConfigKey, AmazonS3);
impl_cloud_builder!(
    GoogleCloudStorageBuilder,
    GoogleConfigKey,
    GoogleCloudStorage
);
impl_cloud_builder!(MicrosoftAzureBuilder, AzureConfigKey, MicrosoftAzure);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::{self, ObjectStore};
    use hdfs_native_object_store::HdfsObjectStoreBuilder;

    use super::{insert_url_handler, store_from_url_opts, EngineStore, URL_REGISTRY};
    use crate::*;

    /// Example funciton of doing testing of a custom [HdfsObjectStore] construction
    fn parse_url_opts_hdfs_native<I, K, V>(
        url: &Url,
        options: I,
    ) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let options_map = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()));
        let store = HdfsObjectStoreBuilder::new()
            .with_url(url.as_str())
            .with_config(options_map)
            .build()?;
        let path = Path::parse(url.path())?;
        Ok((Box::new(store), path))
    }

    #[test]
    fn test_add_hdfs_scheme() {
        let scheme = "hdfs";
        if let Ok(handlers) = URL_REGISTRY.read() {
            assert!(handlers.get(scheme).is_none());
        } else {
            panic!("Failed to read the RwLock for the registry");
        }
        insert_url_handler(scheme, Arc::new(parse_url_opts_hdfs_native))
            .expect("Failed to add new URL scheme handler");

        if let Ok(handlers) = URL_REGISTRY.read() {
            assert!(handlers.get(scheme).is_some());
        } else {
            panic!("Failed to read the RwLock for the registry");
        }

        let url: Url = Url::parse("hdfs://example").expect("Failed to parse URL");
        let options: HashMap<String, String> = HashMap::default();
        // Currently constructing an [HdfsObjectStore] won't work if there isn't an actual HDFS
        // to connect to, so the only way to really verify that we got the object store we
        // expected is to inspect the `store` on the error v_v
        match store_from_url_opts(&url, options) {
            Err(delta_kernel::Error::ObjectStore(object_store::Error::Generic {
                store,
                source: _,
            })) => {
                assert_eq!(store, "HdfsObjectStore");
            }
            Err(unexpected) => panic!("Unexpected error happened: {unexpected:?}"),
            Ok(_) => {
                panic!("Expected to get an error when constructing an HdfsObjectStore, but something didn't work as expected! Either the parse_url_opts_hdfs_native function didn't get called, or the hdfs-native-object-store no longer errors when it cannot connect to HDFS");
            }
        }
    }

    #[test]
    fn cloud_url_factory_preserves_paginated_listing() {
        for url in [
            "s3://bucket/table",
            "gs://bucket/table",
            "abfss://container@account.dfs.core.windows.net/table",
        ] {
            let url = Url::parse(url).unwrap();
            let store = EngineStore::from_url_opts(&url, HashMap::<String, String>::new()).unwrap();
            assert!(store.paginated.is_some(), "missing pagination for {url}");
        }
    }
}
