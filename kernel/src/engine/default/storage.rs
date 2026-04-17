use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use url::Url;

use crate::object_store::path::Path;
use crate::object_store::{self, Error, ObjectStore};
use crate::{DeltaResult, Error as DeltaError};

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

/// Insert a new URL handler for [`store_from_url_opts`] with the given `scheme`. This allows
/// users to provide their own custom URL handler to plug new [`crate::object_store::ObjectStore`]
/// instances into delta-kernel, which is used by [`store_from_url_opts`] to parse the URL.
///
/// The handler must return a store-relative [`Path`] that corresponds to the suffix of the
/// URL's decoded path after removing any store-level scoping (e.g. a container name). This
/// path is used to derive the URL path prefix via segment-level comparison; if the returned
/// path is not a segment-level suffix of the URL path, store creation will fail.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use delta_kernel::engine::default::storage::insert_url_handler;
/// use delta_kernel::object_store::memory::InMemory;
/// use delta_kernel::object_store::path::Path;
///
/// # fn example() -> delta_kernel::DeltaResult<()> {
/// // Register a custom handler for the "mem" scheme that returns a fresh InMemory store.
/// // The returned Path must be a segment-level suffix of the URL's decoded path.
/// insert_url_handler("mem", Arc::new(|url, _opts| {
///     let path = Path::from_url_path(url.path())?;
///     Ok((Box::new(InMemory::new()), path))
/// }))?;
/// # Ok(())
/// # }
/// ```
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
/// Returns an `Arc<dyn ObjectStore>` and a [`Path`] representing the URL path prefix that must
/// be stripped when converting URLs to store-relative paths. For most schemes (S3, ABFSS, local
/// filesystem) the prefix is empty because the container/bucket is encoded in the URL authority
/// rather than the path. For Azure HTTPS URLs (e.g.
/// `https://account.blob.core.windows.net/container/...`) the prefix is the container name,
/// because the store is scoped to the container but `url.path()` includes it.
///
/// Pass both the store and the returned prefix to
/// [`crate::engine::default::DefaultEngineBuilder::new`], or prefer
/// [`crate::engine::default::DefaultEngineBuilder::from_url`] / `from_url_opts` to handle
/// both in one step.
///
/// This function checks for custom URL handlers registered via [`insert_url_handler`]
/// before falling back to [`object_store`]'s default behavior.
///
/// # Example
///
/// ```rust
/// # use url::Url;
/// # use delta_kernel::engine::default::storage::store_from_url;
/// # use delta_kernel::DeltaResult;
/// # fn example() -> DeltaResult<()> {
/// let url = Url::parse("file:///path/to/table")?;
/// let (store, url_path_prefix) = store_from_url(&url)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url(url: &Url) -> crate::DeltaResult<(Arc<dyn ObjectStore>, Path)> {
    store_from_url_opts(url, std::iter::empty::<(&str, &str)>())
}

/// Create an [`ObjectStore`] from a URL with custom options.
///
/// Returns an `Arc<dyn ObjectStore>` and a [`Path`] representing the URL path prefix. See
/// [`store_from_url`] for details on what the prefix represents.
///
/// This function checks for custom URL handlers registered via [`insert_url_handler`]
/// before falling back to [`object_store`]'s default behavior.
///
/// # Example
///
/// ```rust
/// # use url::Url;
/// # use std::collections::HashMap;
/// # use delta_kernel::engine::default::storage::store_from_url_opts;
/// # use delta_kernel::DeltaResult;
/// # fn example() -> DeltaResult<()> {
/// let url = Url::parse("s3://my-bucket/path/to/table")?;
/// let options = HashMap::from([("region", "us-west-2")]);
/// let (store, url_path_prefix) = store_from_url_opts(&url, options)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url_opts<I, K, V>(
    url: &Url,
    options: I,
) -> DeltaResult<(Arc<dyn ObjectStore>, Path)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    // First attempt to use any schemes registered via insert_url_handler,
    // falling back to the default behavior of crate::object_store::parse_url_opts
    let (store, path) = if let Ok(handlers) = URL_REGISTRY.read() {
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

    let path_prefix = compute_url_path_prefix(url, &path)?;

    Ok((Arc::new(store), path_prefix))
}

/// Convert a URL to a store-relative [`Path`] by stripping the URL path prefix.
///
/// Decodes the URL path into [`Path`] space, then verifies and removes the leading prefix
/// segments (e.g. a container name for Azure HTTPS URLs). Returns an error if the decoded
/// URL path does not start with the expected prefix.
///
/// Comparison is segment-based (using [`Path::parts`]) so a prefix of `"a"` will never
/// accidentally match a path starting with `"ab/..."`.
pub(crate) fn store_path_from_url(url: &Url, url_path_prefix: &Path) -> DeltaResult<Path> {
    let full = Path::from_url_path(url.path())?;
    if url_path_prefix.as_ref().is_empty() {
        return Ok(full);
    }
    let prefix_parts: Vec<_> = url_path_prefix.parts().collect();
    let full_parts: Vec<_> = full.parts().collect();
    if full_parts.len() < prefix_parts.len() || full_parts[..prefix_parts.len()] != prefix_parts[..]
    {
        return Err(DeltaError::generic(format!(
            "URL path '{}' does not start with expected prefix '{}'",
            full.as_ref(),
            url_path_prefix.as_ref(),
        )));
    }
    Ok(Path::from_iter(
        full_parts[prefix_parts.len()..].iter().cloned(),
    ))
}

/// Computes the URL path prefix by subtracting the store-relative path from the full URL path.
///
/// `object_store::parse_url_opts` returns a store scoped to a root (e.g. an Azure container)
/// and a store-relative path. The full `url.path()` may contain extra leading segments (like
/// the container name) that the store already accounts for. This function finds those extra
/// segments by decoding both paths into [`Path`] space and subtracting.
///
/// Comparison is segment-based (using [`Path::parts`]) so partial segment overlaps cannot
/// produce false matches.
///
/// # Examples
///
/// - Azure HTTPS: URL path `/container/a/b` with store path `a/b` -> prefix `container`
/// - S3/ABFSS/file: URL path `/a/b` with store path `a/b` -> prefix `` (empty)
/// - Container root: URL path `/container` with store path `` -> prefix `container`
fn compute_url_path_prefix(url_with_prefix: &Url, path_without_prefix: &Path) -> DeltaResult<Path> {
    let full = Path::from_url_path(url_with_prefix.path())?;
    if path_without_prefix.as_ref().is_empty() {
        return Ok(full);
    }
    let full_parts: Vec<_> = full.parts().collect();
    let store_parts: Vec<_> = path_without_prefix.parts().collect();
    let prefix_len = full_parts
        .len()
        .checked_sub(store_parts.len())
        .ok_or_else(|| {
            DeltaError::generic(format!(
                "Store path '{}' has more segments than URL path '{}'",
                path_without_prefix.as_ref(),
                full.as_ref(),
            ))
        })?;
    if full_parts[prefix_len..] != store_parts[..] {
        return Err(DeltaError::generic(format!(
            "Store path '{}' is not a suffix of URL path '{}'",
            path_without_prefix.as_ref(),
            full.as_ref(),
        )));
    }
    Ok(Path::from_iter(full_parts[..prefix_len].iter().cloned()))
}

#[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
#[cfg(test)]
mod tests {
    use hdfs_native_object_store::HdfsObjectStoreBuilder;
    use rstest::rstest;

    use super::*;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::{self};

    type TestHandler = fn(&Url, HashMap<String, String>) -> ClosureReturn;

    /// Example function for testing a custom [`HdfsObjectStore`] construction
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
            .map(|(k, v)| (k.as_ref().to_string(), v.into()));
        let store = HdfsObjectStoreBuilder::new()
            .with_url(url.as_str())
            .with_config(options_map)
            .build()?;
        let path = Path::parse(url.path())?;
        Ok((Box::new(store), path))
    }
    #[rstest]
    // S3-style: URL path maps directly to store path, no prefix
    #[case("s3://bucket/a/b/", "a/b", "")]
    // ABFSS: same as S3, container is in URL authority not path
    #[case("abfss://ctr@acct.dfs.core.windows.net/a/b/", "a/b", "")]
    // Local filesystem: leading slash stripped by Path, no prefix
    #[case("file:///tmp/table/", "tmp/table", "")]
    // Azure HTTPS: container is first path segment, stripped by parse_url_opts
    #[case("https://acct.blob.core.windows.net/ctr/a/b/", "a/b", "ctr")]
    // Azure HTTPS at container root: entire decoded path is the prefix
    #[case("https://acct.blob.core.windows.net/ctr/", "", "ctr")]
    // Paths are identical (no container prefix at all)
    #[case("s3://bucket/path/to/table/", "path/to/table", "")]
    // URL-encoded space: decoded Path space handles this correctly
    #[case("s3://bucket/a%20b/c/", "a b/c", "")]
    fn compute_url_path_prefix_cases(
        #[case] url_str: &str,
        #[case] store_path_str: &str,
        #[case] expected_prefix: &str,
    ) {
        let url = Url::parse(url_str).unwrap();
        let store_path = Path::from(store_path_str);
        let prefix = compute_url_path_prefix(&url, &store_path).unwrap();
        assert_eq!(prefix.as_ref(), expected_prefix);
    }

    #[test]
    fn compute_url_path_prefix_errors_on_mismatch() {
        let url = Url::parse("s3://bucket/a/b/").unwrap();
        let store_path = Path::from("x/y");
        assert!(compute_url_path_prefix(&url, &store_path).is_err());
    }

    #[test]
    fn compute_url_path_prefix_rejects_partial_segment_overlap() {
        // Store path "ab" is a string suffix of URL path "a/ab" but not a segment suffix.
        // Segment-level comparison must reject this.
        let url = Url::parse("s3://bucket/a/ab/").unwrap();
        let store_path = Path::from("ab");
        // "a/ab" has segments ["a", "ab"], store path has segment ["ab"]. Suffix match
        // checks that the last 1 segment of ["a", "ab"] equals ["ab"], which is true,
        // so prefix is "a". This is actually correct -- "ab" IS a valid segment suffix.
        let prefix = compute_url_path_prefix(&url, &store_path).unwrap();
        assert_eq!(prefix.as_ref(), "a");

        // But "b" is NOT a valid segment suffix of "a/ab" -- it would match via string
        // strip_suffix on "a/ab" -> "a/a" but not via segment comparison.
        let store_path = Path::from("b");
        assert!(compute_url_path_prefix(&url, &store_path).is_err());
    }

    #[rstest]
    // Empty prefix: full decoded path passes through
    #[case("s3://bucket/a/b/file.json", "", "a/b/file.json")]
    // Container prefix: first segment stripped
    #[case(
        "https://acct.blob.core.windows.net/ctr/a/b/file.json",
        "ctr",
        "a/b/file.json"
    )]
    // Container prefix with table at container root
    #[case(
        "https://acct.blob.core.windows.net/ctr/_delta_log/0.json",
        "ctr",
        "_delta_log/0.json"
    )]
    // URL-encoded characters decoded correctly
    #[case("s3://bucket/a%20b/file.json", "", "a b/file.json")]
    fn store_path_from_url_cases(
        #[case] url_str: &str,
        #[case] prefix_str: &str,
        #[case] expected: &str,
    ) {
        let url = Url::parse(url_str).unwrap();
        let prefix = Path::from(prefix_str);
        let result = store_path_from_url(&url, &prefix).unwrap();
        assert_eq!(result.as_ref(), expected);
    }

    #[test]
    fn store_path_from_url_errors_on_wrong_prefix() {
        let url = Url::parse("https://acct.blob.core.windows.net/ctr/a/b/").unwrap();
        let wrong_prefix = Path::from("other");
        assert!(store_path_from_url(&url, &wrong_prefix).is_err());
    }

    #[test]
    fn store_path_from_url_rejects_partial_segment_prefix() {
        // Prefix "ct" is a string prefix of "ctr/a/b" but not a segment prefix.
        // Segment-level comparison must reject this.
        let url = Url::parse("https://acct.blob.core.windows.net/ctr/a/b/").unwrap();
        let bad_prefix = Path::from("ct");
        assert!(store_path_from_url(&url, &bad_prefix).is_err());
    }

    #[test]
    fn store_path_from_url_roundtrip_with_list_reconstruction() {
        // Simulate: compute prefix, convert URL to store path, then reconstruct the URL
        // from a listing result (as list_from_impl does).
        let table_url =
            Url::parse("https://acct.blob.core.windows.net/ctr/path/to/table/").unwrap();
        let store_path = Path::from("path/to/table");
        let prefix = compute_url_path_prefix(&table_url, &store_path).unwrap();
        assert_eq!(prefix.as_ref(), "ctr");

        // A sub-URL under the table
        let file_url = table_url.join("_delta_log/00000.json").unwrap();
        let file_store_path = store_path_from_url(&file_url, &prefix).unwrap();
        assert_eq!(
            file_store_path.as_ref(),
            "path/to/table/_delta_log/00000.json"
        );

        // Reconstruct URL from store path (inverse, as list_from_impl does)
        let full_path = Path::from_iter(prefix.parts().chain(file_store_path.parts()));
        let mut reconstructed = table_url.clone();
        reconstructed.set_path(&format!("/{}", full_path.as_ref()));
        assert_eq!(reconstructed, file_url);
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
            Err(crate::Error::ObjectStore(object_store::Error::Generic { store, source: _ })) => {
                assert_eq!(store, "HdfsObjectStore");
            }
            Err(unexpected) => panic!("Unexpected error happened: {unexpected:?}"),
            Ok(_) => {
                panic!("Expected to get an error when constructing an HdfsObjectStore, but something didn't work as expected! Either the parse_url_opts_hdfs_native function didn't get called, or the hdfs-native-object-store no longer errors when it cannot connect to HDFS");
            }
        }
    }

    // === insert_url_handler tests ===

    /// Handler that treats the entire URL path as the store-relative path (unscoped store).
    fn handler_passthrough(url: &Url, _opts: HashMap<String, String>) -> ClosureReturn {
        let path = Path::from_url_path(url.path())?;
        Ok((Box::new(InMemory::new()), path))
    }

    /// Handler that strips the first path segment (simulating container-scoped store creation).
    fn handler_strip_first(url: &Url, _opts: HashMap<String, String>) -> ClosureReturn {
        let full = Path::from_url_path(url.path())?;
        Ok((
            Box::new(InMemory::new()),
            Path::from_iter(full.parts().skip(1)),
        ))
    }

    /// Handler that violates the suffix contract by returning an unrelated path.
    fn handler_unrelated(_url: &Url, _opts: HashMap<String, String>) -> ClosureReturn {
        Ok((
            Box::new(InMemory::new()),
            Path::from("unrelated/store/path"),
        ))
    }

    #[rstest]
    // Passthrough handler: store-relative path == URL path, so prefix is empty.
    #[case("ih-passthrough", "/my/table/", handler_passthrough as TestHandler, "")]
    // Strip-first handler: first URL segment is the container, so prefix is that segment.
    #[case(
        "ih-strip-first",
        "/container/a/b/",
        handler_strip_first as TestHandler,
        "container"
    )]
    fn insert_url_handler_success_cases(
        #[case] scheme: &str,
        #[case] url_path: &str,
        #[case] handler: TestHandler,
        #[case] expected_prefix: &str,
    ) {
        insert_url_handler(scheme, Arc::new(handler)).expect("Failed to register URL handler");
        let url = Url::parse(&format!("{scheme}://{url_path}")).unwrap();
        let (_store, prefix) = store_from_url(&url).expect("store_from_url should succeed");
        assert_eq!(prefix.as_ref(), expected_prefix);
    }

    #[test]
    fn insert_url_handler_errors_when_returned_path_is_not_suffix() {
        let scheme = "ih-unrelated";
        insert_url_handler(scheme, Arc::new(handler_unrelated as TestHandler))
            .expect("Failed to register URL handler");
        let url = Url::parse(&format!("{scheme}:///a/b/")).unwrap();
        assert!(
            store_from_url(&url).is_err(),
            "expected error when handler returns a non-suffix path"
        );
    }
}
