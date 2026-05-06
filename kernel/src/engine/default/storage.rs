use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use url::Url;

use crate::object_store::path::Path;
use crate::object_store::{self, DynObjectStore};
use crate::{DeltaResult, Error as DeltaError};

/// An object store together with the URL path prefix that must be stripped when converting
/// URLs to store-relative paths.
///
/// Produced by [`store_from_url`] / [`store_from_url_opts`] and consumed by
/// [`crate::engine::default::DefaultEngineBuilder::new`]. Coupling the two into a single
/// type ensures callers cannot accidentally pair a store with the wrong prefix (or forget
/// the prefix altogether, which would silently break Azure HTTPS URLs).
#[derive(Debug, Clone)]
pub struct PrefixedStore {
    /// The object store to read from / write to.
    pub store: Arc<DynObjectStore>,
    /// The leading URL path segments that identify the store's scope. Must be stripped
    /// from URLs before passing paths to the store. For schemes that encode the
    /// bucket/container in the URL authority (S3, ABFSS, local filesystem) this is empty.
    /// For Azure HTTPS URLs (`https://account.blob.core.windows.net/container/...`) it
    /// contains the container name that the store is scoped to.
    pub url_path_prefix: Path,
}

impl PrefixedStore {
    /// Construct a [`PrefixedStore`] from an object store and its scoping URL path prefix.
    ///
    /// Pass `Path::from("")` for `url_path_prefix` when the URL scheme encodes the
    /// bucket/container in the authority (S3, ABFSS, GCS, local filesystem).
    pub fn new(store: Arc<DynObjectStore>, url_path_prefix: Path) -> Self {
        Self {
            store,
            url_path_prefix,
        }
    }
}

/// Handler closures return a [`PrefixedStore`] built from a freshly constructed object store
/// and the store-scoped path prefix (typically a bucket or container name). Errors use the
/// kernel's [`DeltaResult`] so handlers can surface both `object_store` errors and
/// prefix-computation errors via the `?` operator.
type ClosureReturn = DeltaResult<PrefixedStore>;
/// Closure type for custom URL handlers registered via [`insert_url_handler`].
///
/// Uses `HashMap<String, String>` for options because trait objects cannot be generic;
/// [`store_from_url_opts`] converts the caller's options into this concrete type before
/// dispatching to a registered handler.
type HandlerClosure = Arc<dyn Fn(&Url, HashMap<String, String>) -> ClosureReturn + Send + Sync>;
/// HashMap of `scheme => handler fn` mappings, allowing consumers of delta-kernel-rs to
/// provide their own URL opts parsers for different schemes.
type Handlers = HashMap<String, HandlerClosure>;
/// The URL_REGISTRY contains the custom URL scheme handlers that will parse URL options
static URL_REGISTRY: LazyLock<RwLock<Handlers>> = LazyLock::new(|| RwLock::new(HashMap::default()));

/// Insert a new URL handler for [`store_from_url_opts`] with the given `scheme`. This allows
/// users to provide their own custom URL handler to plug new [`crate::object_store::ObjectStore`]
/// instances into delta-kernel, which is used by [`store_from_url_opts`] to parse the URL.
///
/// The handler must return a [`PrefixedStore`] whose `url_path_prefix` is a segment-level
/// prefix of the URL's decoded path (typically empty, or a container/bucket segment).
/// Handlers that only know the store-relative suffix can compute the prefix with
/// [`compute_url_path_prefix`].
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
/// // Register a custom handler for the "mem" scheme that returns a fresh InMemory store
/// // with no URL path prefix (the URL path maps directly to store-relative paths).
/// insert_url_handler("mem", Arc::new(|_url, _opts| {
///     Ok(PrefixedStore::new(Arc::new(InMemory::new()), Path::from("")))
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

/// Create a [`PrefixedStore`] from a URL.
///
/// Returns a [`PrefixedStore`] that pairs the constructed object store with the URL path
/// prefix that must be stripped when converting URLs to store-relative paths. For most
/// schemes (S3, ABFSS, local filesystem) the prefix is empty because the container/bucket is
/// encoded in the URL authority rather than the path. For Azure HTTPS URLs (e.g.
/// `https://account.blob.core.windows.net/container/...`) the prefix is the container name,
/// because the store is scoped to the container but `url.path()` includes it.
///
/// Pass the returned [`PrefixedStore`] directly to
/// [`crate::engine::default::DefaultEngineBuilder::new`].
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
/// let prefixed = store_from_url(&url)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url(url: &Url) -> DeltaResult<PrefixedStore> {
    store_from_url_opts(url, std::iter::empty::<(&str, &str)>())
}

/// Create a [`PrefixedStore`] from a URL with custom options.
///
/// See [`store_from_url`] for details on the returned [`PrefixedStore`] and its path prefix.
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
/// let prefixed = store_from_url_opts(&url, options)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url_opts<I, K, V>(url: &Url, options: I) -> DeltaResult<PrefixedStore>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    // First attempt to use any schemes registered via insert_url_handler,
    // falling back to the default behavior of crate::object_store::parse_url_opts
    if let Ok(handlers) = URL_REGISTRY.read() {
        if let Some(handler) = handlers.get(url.scheme()) {
            let options = options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.into()))
                .collect();
            let prefixed = handler(url, options)?;
            // Verify the handler-provided prefix is actually a segment-level prefix of the
            // URL. `store_path_from_url` returns an error if not, which is the same
            // guarantee the default fallback gives via `compute_url_path_prefix`.
            store_path_from_url(url, &prefixed.url_path_prefix)?;
            return Ok(prefixed);
        }
    }

    let (store, path) = object_store::parse_url_opts(url, options)?;
    let url_path_prefix = compute_url_path_prefix(url, &path)?;
    Ok(PrefixedStore::new(Arc::new(store), url_path_prefix))
}

/// Convert a URL to a store-relative [`Path`] by stripping the URL path prefix.
///
/// Decodes the URL path into [`Path`] space, then verifies and removes the leading prefix
/// segments (e.g. a container name for Azure HTTPS URLs). Returns an error if the decoded
/// URL path does not start with the expected prefix.
///
/// Delegates to [`Path::prefix_match`] for segment-aware matching, so a prefix of `"a"`
/// will never accidentally match a path starting with `"ab/..."`.
pub(crate) fn store_path_from_url(url: &Url, url_path_prefix: &Path) -> DeltaResult<Path> {
    let full = Path::from_url_path(url.path())?;
    full.prefix_match(url_path_prefix)
        .map(Path::from_iter)
        .ok_or_else(|| {
            DeltaError::generic(format!(
                "URL path '{}' does not start with expected prefix '{}'",
                full.as_ref(),
                url_path_prefix.as_ref(),
            ))
        })
}

/// Computes the URL path prefix by subtracting the store-relative path from the full URL path.
///
/// `object_store::parse_url_opts` returns a store scoped to a root (e.g. an Azure container)
/// and a store-relative path. The full `url.path()` may contain extra leading segments (like
/// the container name) that the store already accounts for. This function finds those extra
/// segments by decoding both paths into [`Path`] space and subtracting.
///
/// Exposed publicly so custom URL handlers (registered via [`insert_url_handler`]) can reuse
/// the same prefix-derivation logic used by the default `parse_url_opts` fallback.
///
/// Comparison is segment-based (using [`Path::parts`]) so partial segment overlaps cannot
/// produce false matches.
///
/// # Examples
///
/// - Azure HTTPS: URL path `/container/a/b` with store path `a/b` -> prefix `container`
/// - S3/ABFSS/file: URL path `/a/b` with store path `a/b` -> prefix `` (empty)
/// - Container root: URL path `/container` with store path `` -> prefix `container`
pub fn compute_url_path_prefix(
    url_with_prefix: &Url,
    path_without_prefix: &Path,
) -> DeltaResult<Path> {
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

    /// Example function for testing a custom [`HdfsObjectStore`] construction.
    ///
    /// Since the handler returns a fully-resolved [`PrefixedStore`], this helper derives
    /// the prefix from the URL via [`compute_url_path_prefix`].
    fn parse_url_opts_hdfs_native(
        url: &Url,
        options: HashMap<String, String>,
    ) -> DeltaResult<PrefixedStore> {
        let store = HdfsObjectStoreBuilder::new()
            .with_url(url.as_str())
            .with_config(options)
            .build()?;
        let path = Path::parse(url.path())?;
        let url_path_prefix = compute_url_path_prefix(url, &path)?;
        Ok(PrefixedStore::new(Arc::new(store), url_path_prefix))
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

    /// End-to-end coverage of [`store_from_url`] for each URL scheme we support. Unlike
    /// [`compute_url_path_prefix_cases`] (which feeds in a hand-rolled store-relative
    /// path), this exercises the real `object_store::parse_url_opts` fallback so a wrong
    /// assumption about how the underlying crate parses a scheme would surface here.
    #[rstest]
    // S3: bucket is in URL authority, not path -> empty prefix.
    #[case::s3("s3://my-bucket/path/to/table/", "")]
    // ABFSS: container is in `container@account.dfs.core.windows.net` authority -> empty
    // prefix.
    #[case::abfss("abfss://ctr@acct.dfs.core.windows.net/path/to/table/", "")]
    // GCS: bucket is in URL authority -> empty prefix.
    #[case::gcs("gs://my-bucket/path/to/table/", "")]
    // Local filesystem: leading slash is the path root, not a scoping segment -> empty
    // prefix.
    #[case::file("file:///tmp/table/", "")]
    // Azure HTTPS: container is the first path segment -> prefix is the container name.
    #[case::azure_https("https://acct.blob.core.windows.net/container/path/", "container")]
    // Azure HTTPS at container root: prefix is the container, store-relative path is empty.
    #[case::azure_https_container_root(
        "https://acct.blob.core.windows.net/container/",
        "container"
    )]
    fn store_from_url_returns_expected_prefix(
        #[case] url_str: &str,
        #[case] expected_prefix: &str,
    ) {
        let url = Url::parse(url_str).unwrap();
        let prefixed = store_from_url(&url)
            .unwrap_or_else(|e| panic!("store_from_url({url_str}) failed: {e}"));
        assert_eq!(prefixed.url_path_prefix.as_ref(), expected_prefix);
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
    /// The derived prefix is empty.
    fn handler_passthrough(_url: &Url, _opts: HashMap<String, String>) -> ClosureReturn {
        Ok(PrefixedStore::new(
            Arc::new(InMemory::new()),
            Path::from(""),
        ))
    }

    /// Handler that strips the first path segment (simulating container-scoped store creation).
    fn handler_strip_first(url: &Url, _opts: HashMap<String, String>) -> ClosureReturn {
        let full = Path::from_url_path(url.path())?;
        let url_path_prefix = full
            .parts()
            .next()
            .map(|part| Path::from(part.as_ref().to_string()))
            .unwrap_or(Path::from(""));
        Ok(PrefixedStore::new(
            Arc::new(InMemory::new()),
            url_path_prefix,
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
        let prefixed = store_from_url(&url).expect("store_from_url should succeed");
        assert_eq!(prefixed.url_path_prefix.as_ref(), expected_prefix);
    }

    /// Handler that returns a prefix that is not actually a segment-level prefix of the URL.
    fn handler_bad_prefix(_url: &Url, _opts: HashMap<String, String>) -> ClosureReturn {
        Ok(PrefixedStore::new(
            Arc::new(InMemory::new()),
            Path::from("not-a-real-prefix"),
        ))
    }

    #[test]
    fn insert_url_handler_errors_when_prefix_does_not_match_url() {
        let scheme = "ih-bad-prefix";
        insert_url_handler(scheme, Arc::new(handler_bad_prefix as TestHandler))
            .expect("Failed to register URL handler");
        let url = Url::parse(&format!("{scheme}:///a/b/")).unwrap();
        assert!(
            store_from_url(&url).is_err(),
            "expected error when handler returns a non-matching prefix"
        );
    }
}
