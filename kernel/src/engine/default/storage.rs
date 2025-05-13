use crate::object_store::parse_url_opts as parse_url_opts_object_store;
use crate::object_store::path::Path;
use crate::object_store::{Error, ObjectStore};
use url::Url;

use crate::Error as DeltaError;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

// NB: tests for this module (testing registering a custom URL handler) are in the
// hdfs-integration-test crate. Otherwise (if they were included here) we would need to support
// multiple versions of hdfs crates since they are compatible with specific versions of
// object_store (of which we support multiple versions).

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
/// provide their own custom URL handler to plug new [crate::object_store::ObjectStore] instances into
/// delta-kernel
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

/// Parse the given URL options to produce a valid and configured [ObjectStore]
///
/// This function will first attempt to use any schemes registered via [insert_url_handler],
/// falling back to the default behavior of [crate::object_store::parse_url_opts]
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
