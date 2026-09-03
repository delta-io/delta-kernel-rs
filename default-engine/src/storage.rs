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

/// The backing store for a [`DefaultEngine`](crate::DefaultEngine).
///
/// The optional [`PaginatedListStore`] lets cloud storage apply the directory and offset bounds.
/// Without it, the engine retrieves direct children from the start of the directory and applies
/// the offset bound client-side.
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
    pub fn plain(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            paginated: None,
        }
    }

    /// The one `Arc` backs both reads and listing, so they cannot diverge.
    pub fn with_paginated<S: ObjectStore + PaginatedListStore + 'static>(store: Arc<S>) -> Self {
        Self {
            object_store: store.clone(),
            paginated: Some(store),
        }
    }

    /// Delimiter-pushdown capable for S3, GCS, and Azure, plain otherwise.
    pub fn from_url_opts<I, K, V>(url: &Url, options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        // A registered custom handler takes precedence and is not listing-capable. Drop the read
        // guard before delegating, since store_from_url_opts locks URL_REGISTRY again.
        let is_custom = URL_REGISTRY
            .read()
            .map(|handlers| handlers.contains_key(url.scheme()))
            .unwrap_or(false);
        if is_custom {
            return Ok(Self::plain(store_from_url_opts(url, options)?));
        }

        let (scheme, _path) = ObjectStoreScheme::parse(url).map_err(object_store::Error::from)?;
        let opts = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()));
        macro_rules! listing {
            ($builder:expr) => {
                Self::with_paginated(Arc::new(build_cloud_store($builder, url, opts)?))
            };
        }
        Ok(match scheme {
            ObjectStoreScheme::AmazonS3 => listing!(AmazonS3Builder::new()),
            ObjectStoreScheme::GoogleCloudStorage => listing!(GoogleCloudStorageBuilder::new()),
            ObjectStoreScheme::MicrosoftAzure => listing!(MicrosoftAzureBuilder::new()),
            _ => Self::plain(store_from_url_opts(url, opts)?),
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

/// Builds a concrete cloud store from `url` and `options`, mirroring
/// `object_store::parse_url_opts`, which returns `Box<dyn ObjectStore>` and erases the
/// `PaginatedListStore` capability.
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

/// Builder surface [`build_cloud_store`] needs.
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

    use delta_kernel::object_store::aws::AmazonS3Builder;
    use delta_kernel::object_store::azure::MicrosoftAzureBuilder;
    use delta_kernel::object_store::gcp::GoogleCloudStorageBuilder;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::{self, ObjectStore};
    use hdfs_native_object_store::HdfsObjectStoreBuilder;

    use super::{
        build_cloud_store, insert_url_handler, store_from_url_opts, ClosureReturn, EngineStore,
        URL_REGISTRY,
    };
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
    fn engine_store_is_paginated_for_cloud_schemes() {
        for url in [
            "s3://bucket/table",
            "gs://bucket/table",
            "abfss://container@account.dfs.core.windows.net/table",
        ] {
            let url = Url::parse(url).unwrap();
            let opts: HashMap<String, String> = HashMap::default();
            let store = EngineStore::from_url_opts(&url, opts).unwrap();
            assert!(
                store.paginated.is_some(),
                "expected a paginated store for {url}"
            );
        }
    }

    #[test]
    fn engine_store_is_plain_for_local_or_memory() {
        for url in ["memory:///table", "file:///tmp/table"] {
            let url = Url::parse(url).unwrap();
            let opts: HashMap<String, String> = HashMap::default();
            let store = EngineStore::from_url_opts(&url, opts).unwrap();
            assert!(
                store.paginated.is_none(),
                "expected a plain store for {url}"
            );
        }
    }

    /// A registered custom handler yields a plain store even for a cloud scheme.
    #[test]
    fn engine_store_is_plain_for_registered_custom_handler() {
        fn memory_handler<I, K, V>(url: &Url, _opts: I) -> ClosureReturn
        where
            I: IntoIterator<Item = (K, V)>,
            K: AsRef<str>,
            V: Into<String>,
        {
            Ok((Box::new(InMemory::new()), Path::parse(url.path()).unwrap()))
        }
        let scheme = "custom-listing-test";
        insert_url_handler(scheme, Arc::new(memory_handler)).unwrap();

        let url = Url::parse(&format!("{scheme}://bucket/table")).unwrap();
        let store = EngineStore::from_url_opts(&url, HashMap::<String, String>::default()).unwrap();
        assert!(store.paginated.is_none());
    }

    #[test]
    fn engine_store_from_url_opts_rejects_unknown_scheme() {
        let url = Url::parse("ftp://host/table").unwrap();
        let result = EngineStore::from_url_opts(&url, HashMap::<String, String>::default());
        assert!(result.is_err(), "unknown scheme must not build a store");
    }

    // Guards against drift: build_cloud_store and parse_url_opts must handle the same keys, both
    // tolerating an unknown one. Covers all three cloud builders.
    #[test]
    fn build_cloud_store_matches_parse_url_opts_option_handling() {
        let s3 = Url::parse("s3://bucket/table").unwrap();
        let s3_opts = [
            ("region".to_string(), "us-west-2".to_string()),
            ("bogus_unknown_key".to_string(), "x".to_string()),
        ];
        assert!(object_store::parse_url_opts(&s3, s3_opts.clone()).is_ok());
        assert!(build_cloud_store(AmazonS3Builder::new(), &s3, s3_opts).is_ok());

        let gcs = Url::parse("gs://bucket/table").unwrap();
        let gcs_opts = [("bogus_unknown_key".to_string(), "x".to_string())];
        assert!(build_cloud_store(GoogleCloudStorageBuilder::new(), &gcs, gcs_opts).is_ok());

        let azure = Url::parse("abfss://c@a.dfs.core.windows.net/table").unwrap();
        let azure_opts = [
            ("skip_signature".to_string(), "true".to_string()),
            ("bogus_unknown_key".to_string(), "x".to_string()),
        ];
        assert!(build_cloud_store(MicrosoftAzureBuilder::new(), &azure, azure_opts).is_ok());
    }
}
