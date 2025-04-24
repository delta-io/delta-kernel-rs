use object_store::parse_url_opts as parse_url_opts_object_store;
use object_store::path::Path;
use object_store::{Error, ObjectStore};
use url::Url;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

/// Alias for convenience
type ClosureReturn = Result<(Box<dyn ObjectStore>, Path), Error>;
/// This type alias makes it easier to reference the handler closure(s)
///
/// It uses a HashMap<String, String> which _must_ be converted in our [parse_url_opts] because we
/// cannot use generics in this scenario.
type HandlerClosure = Arc<dyn Fn(&Url, HashMap<String, String>) -> ClosureReturn + Send + Sync>;
/// hashmap containing scheme => handler fn mappings to allow consumers of delta-kernel-rs provide
/// their own url opts parsers for different scemes
type Handlers = HashMap<String, HandlerClosure>;
/// The URL_REGISTRY contains the custom URL scheme handlers that will parse URL options
static URL_REGISTRY: LazyLock<RwLock<Handlers>> = LazyLock::new(|| RwLock::new(HashMap::default()));

/// Insert a new URL handler for [parse_url_opts] with the given `scheme`. This allows users to
/// provide their own custom URL handler to plug new [object_store::ObjectStore] instances into
/// delta-kernel
pub fn insert_url_handler(
    scheme: impl AsRef<str>,
    handler_closure: HandlerClosure,
) -> Result<(), Error> {
    if let Ok(mut lock) = URL_REGISTRY.write() {
        (*lock).insert(scheme.as_ref().into(), handler_closure);
    } else {
        panic!("Failed to acquire lock for adding a URL handler!");
    }
    Ok(())
}

/// Parse the given URL options to produce a valid and configured [ObjectStore]
///
/// This function will first attempt to use any schemes registered via [insert_url_handler],
/// falling back to the default behavior of [object_store::parse_url_opts]
pub fn parse_url_opts<I, K, V>(url: &Url, options: I) -> Result<(Box<dyn ObjectStore>, Path), Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    if let Ok(handlers) = URL_REGISTRY.read() {
        if let Some(handler) = handlers.get(url.scheme()) {
            let options: HashMap<String, String> = HashMap::from_iter(
                options
                    .into_iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.into())),
            );

            return handler(url, options);
        }
    }
    parse_url_opts_object_store(url, options)
}

#[cfg(all(test, feature = "cloud"))]
mod tests {
    use super::*;

    use hdfs_native_object_store::HdfsObjectStore;
    use object_store::path::Path;

    /// Example funciton of doing testing of a custom [HdfsObjectStore] construction
    fn parse_url_opts_hdfs_native<I, K, V>(
        url: &Url,
        options: I,
    ) -> Result<(Box<dyn ObjectStore>, Path), Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let options_map = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()))
            .collect();
        let store = HdfsObjectStore::with_config(url.as_str(), options_map)?;
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
        // jxpected is to inspect the `store` on the error v_v
        if let Err(store_error) = parse_url_opts(&url, options) {
            match store_error {
                object_store::Error::Generic { store, source: _ } => {
                    assert_eq!(store, "HdfsObjectStore");
                }
                unexpected => panic!("Unexpected error happened: {unexpected:?}"),
            }
        } else {
            panic!("Expected to get an error when constructing an HdfsObjectStore, but something didn't work as expected! Either the parse_url_opts_hdfs_native function didn't get called, or the hdfs-native-object-store no longer errors when it cannot connect to HDFS");
        }
    }
}
