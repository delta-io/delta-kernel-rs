//! A generic REST/HTTP-backed [`ObjectStore`].
//!
//! [`RestObjectStore`] implements [`ObjectStore`] over a REST "file API" -- a service that
//! exposes list/read/write of files behind authenticated HTTP endpoints rather than as a plain
//! blob store -- so the default engine can read and write Delta tables through it with no
//! service-specific logic in the kernel.
//!
//! The caller supplies everything service-specific through two extension points:
//!
//! - [`AuthHeaderProvider`] -- headers attached to every request (auth, identity). Consulted per
//!   request, so an implementation can refresh short-lived credentials.
//! - [`RestFileApiContract`] -- the REST dialect: path-to-URL mapping, list and write query
//!   parameters, list-response parsing, and HTTP-status-to-[`ObjectStoreError`] mapping.
//!
//! Only the operations kernel needs are implemented (read, list, write, delete); the rest
//! return [`ObjectStoreError::NotSupported`].

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use delta_kernel::object_store::path::Path;
// object_store 0.13 (arrow-58) routes copy through copy_opts(CopyOptions); 0.12 (arrow-57)
// uses copy/copy_if_not_exists. Import CopyOptions only where it exists.
#[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
use delta_kernel::object_store::CopyOptions;
use delta_kernel::object_store::{
    Attributes, Error as ObjectStoreError, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult,
};
use futures::stream::BoxStream;
// `delete_stream` (.then) exists only on the object_store 0.13 (arrow-58) code path.
#[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
use futures::StreamExt as _;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Certificate, Client, Identity};

/// Supplies the HTTP headers attached to every request a [`RestObjectStore`] makes.
///
/// Called once per request, so implementations own any caching and refresh of credentials.
/// A static set of headers can use [`StaticHeaderProvider`]; a provider backed by a
/// short-lived, refreshable token should return the current headers on each call.
pub trait AuthHeaderProvider: std::fmt::Debug + Send + Sync {
    /// Return the headers to attach to the next request.
    fn headers(&self) -> ObjectStoreResult<HeaderMap>;
}

/// An [`AuthHeaderProvider`] that always returns the same fixed headers.
///
/// Suitable when credentials do not expire within the store's lifetime, or when refresh is
/// handled out of band by swapping in a new store.
#[derive(Debug, Clone)]
pub struct StaticHeaderProvider {
    headers: HeaderMap,
}

impl StaticHeaderProvider {
    /// Create a provider that returns `headers` on every request.
    pub fn new(headers: HeaderMap) -> Self {
        Self { headers }
    }

    /// Build a provider from `(name, value)` header pairs. Errors if a name or value is not a
    /// valid HTTP header. Lets a caller that can't construct a [`HeaderMap`] (e.g. across an FFI
    /// boundary) supply headers as plain strings.
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> ObjectStoreResult<Self> {
        Ok(Self {
            headers: headers_from_pairs(pairs)?,
        })
    }
}

impl AuthHeaderProvider for StaticHeaderProvider {
    fn headers(&self) -> ObjectStoreResult<HeaderMap> {
        Ok(self.headers.clone())
    }
}

/// Build a [`HeaderMap`] from `(name, value)` pairs, erroring if a name or value is not a valid
/// HTTP header.
pub fn headers_from_pairs(
    pairs: impl IntoIterator<Item = (String, String)>,
) -> ObjectStoreResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(generic_err)?;
        let value = HeaderValue::from_str(&value).map_err(generic_err)?;
        headers.insert(name, value);
    }
    Ok(headers)
}

/// Safety margin before TTL expiry at which cached headers are refreshed, so a request never
/// races expiry.
const HEADER_REFRESH_SKEW: Duration = Duration::from_secs(30);

/// An [`AuthHeaderProvider`] that produces headers via a closure returning `(headers,
/// Option<ttl>)`. `Some(ttl)` caches the headers until [`HEADER_REFRESH_SKEW`] before `ttl`
/// elapses, then produces fresh ones; `None` re-runs the closure on every request.
pub struct RefreshingHeaderProvider {
    produce: Box<dyn Fn() -> ObjectStoreResult<(HeaderMap, Option<Duration>)> + Send + Sync>,
    cached: std::sync::Mutex<Option<(HeaderMap, std::time::Instant)>>,
}

impl std::fmt::Debug for RefreshingHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshingHeaderProvider")
            .finish_non_exhaustive()
    }
}

impl RefreshingHeaderProvider {
    /// Create a provider that calls `produce` to obtain `(headers, ttl)`; see the type docs for how
    /// the optional `ttl` drives caching.
    pub fn new(
        produce: impl Fn() -> ObjectStoreResult<(HeaderMap, Option<Duration>)> + Send + Sync + 'static,
    ) -> Self {
        Self {
            produce: Box::new(produce),
            cached: std::sync::Mutex::new(None),
        }
    }
}

impl AuthHeaderProvider for RefreshingHeaderProvider {
    fn headers(&self) -> ObjectStoreResult<HeaderMap> {
        // Hold the lock across the (rare) produce call for single-flight refresh; recover from a
        // poisoned lock rather than panic.
        let mut cached = self.cached.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((headers, deadline)) = cached.as_ref() {
            if deadline.saturating_duration_since(std::time::Instant::now()) > HEADER_REFRESH_SKEW {
                return Ok(headers.clone());
            }
        }
        let (headers, ttl) = (self.produce)()?;
        // Skip caching if an absurd TTL overflows `Instant`, rather than panicking.
        *cached = ttl
            .and_then(|ttl| Some((headers.clone(), std::time::Instant::now().checked_add(ttl)?)));
        Ok(headers)
    }
}

/// One page of a list response: the objects on this page plus an optional token for the next.
pub struct RestListPage {
    /// The (file) objects on this page. Directories should already be filtered out by the
    /// contract, since [`ObjectStore::list`] yields only objects.
    pub objects: Vec<ObjectMeta>,
    /// Opaque token to fetch the next page, or `None` when this is the last page.
    pub next_page_token: Option<String>,
}

/// Describes the REST dialect a [`RestObjectStore`] speaks. Holds no credentials -- those come
/// from the [`AuthHeaderProvider`] -- only the request/response shape of the target API.
pub trait RestFileApiContract: std::fmt::Debug + Send + Sync {
    /// Build the URL for file (object) operations on `path`.
    fn file_url(&self, base_url: &str, path: &str) -> String;

    /// Build the URL for directory (list) operations on `path`.
    fn directory_url(&self, base_url: &str, path: &str) -> String;

    /// Query parameters for a list request. `page_token` drives pagination; `start_from` is an
    /// optional offset (see [`ObjectStore::list_with_offset`]); `recursive` requests a
    /// recursive listing.
    fn list_query(
        &self,
        page_token: Option<&str>,
        start_from: Option<&str>,
        recursive: bool,
    ) -> Vec<(String, String)>;

    /// Parse a list-response body into a [`RestListPage`].
    ///
    /// Entries **must** be ordered by ascending full path, and that order must hold *across*
    /// pages (a page's first entry sorts after the previous page's last). [`RestObjectStore`]
    /// relies on this for Delta log replay and does not re-sort: it checks the order as it
    /// streams pages and fails the listing with an error if an entry arrives out of order,
    /// rather than silently producing a wrong snapshot.
    fn parse_list(&self, body: &[u8]) -> ObjectStoreResult<RestListPage>;

    /// Query parameters for a `PUT`, given the put options. Only [`PutMode::Overwrite`] and
    /// [`PutMode::Create`] reach here; [`PutMode::Update`] is rejected by the store.
    fn put_query(&self, overwrite: bool) -> Vec<(String, String)>;

    /// Map a non-success HTTP status to an [`ObjectStoreError`], or `None` to fall back to the
    /// generic status handling.
    ///
    /// A 404 on a file **must** map to [`ObjectStoreError::NotFound`], and a `Create` collision
    /// **should** map 409 -> [`ObjectStoreError::AlreadyExists`].
    fn map_status(&self, status: reqwest::StatusCode, path: &str) -> Option<ObjectStoreError>;
}

/// A data-driven [`RestFileApiContract`]: path-prefix templates and configurable JSON field
/// names describe the request/response shape, so a backend needs no bespoke Rust impl.
#[derive(Debug, Clone)]
pub struct RestEndpointConfig {
    /// Path prefix for file (object) operations; the URL is `{base_url}/{files_prefix}/{path}`.
    pub files_prefix: String,
    /// Path prefix for directory (list) operations.
    pub directories_prefix: String,
    /// Query-param name carrying the pagination token.
    pub page_token_param: String,
    /// Query-param name carrying the list start offset.
    pub start_from_param: String,
    /// Query-param name (value `"true"`) requesting a recursive listing.
    pub recursive_param: String,
    /// Query-param name controlling overwrite-vs-create on `PUT` (value `"true"`/`"false"`).
    pub overwrite_param: String,
    /// List-response JSON field holding the array of entries.
    pub contents_field: String,
    /// List-response JSON field holding the next-page token.
    pub next_page_token_field: String,
    /// Per-entry JSON field holding the path.
    pub entry_path_field: String,
    /// Per-entry JSON field holding the size in bytes.
    pub entry_size_field: String,
    /// Per-entry JSON field holding the directory flag; truthy entries are skipped, since
    /// [`ObjectStore::list`] yields only objects.
    pub entry_is_directory_field: String,
    /// Per-entry JSON field holding the last-modified time as epoch milliseconds.
    pub entry_last_modified_field: String,
    /// If set, every list-entry path must start with this prefix; it is stripped to yield the
    /// store-relative path, and an entry outside the prefix is rejected (a scope check). Use when
    /// the backend returns absolute keys but the store is rooted at a sub-path.
    pub entry_strip_prefix: Option<String>,
}

impl Default for RestEndpointConfig {
    /// Field-name and query-param defaults shared by all callers; `files_prefix` and
    /// `directories_prefix` default to empty (no prefix) since they have no sensible default.
    fn default() -> Self {
        Self {
            files_prefix: String::new(),
            directories_prefix: String::new(),
            page_token_param: "page_token".to_string(),
            start_from_param: "start_from".to_string(),
            recursive_param: "recursive".to_string(),
            overwrite_param: "overwrite".to_string(),
            contents_field: "contents".to_string(),
            next_page_token_field: "nextPageToken".to_string(),
            entry_path_field: "path".to_string(),
            entry_size_field: "fileSize".to_string(),
            entry_is_directory_field: "isDirectory".to_string(),
            entry_last_modified_field: "lastModified".to_string(),
            entry_strip_prefix: None,
        }
    }
}

impl RestEndpointConfig {
    fn join_url(&self, base_url: &str, prefix: &str, path: &str) -> String {
        format!(
            "{}/{}/{}",
            base_url.trim_end_matches('/'),
            prefix.trim_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

impl RestFileApiContract for RestEndpointConfig {
    fn file_url(&self, base_url: &str, path: &str) -> String {
        self.join_url(base_url, &self.files_prefix, path)
    }

    fn directory_url(&self, base_url: &str, path: &str) -> String {
        self.join_url(base_url, &self.directories_prefix, path)
    }

    fn list_query(
        &self,
        page_token: Option<&str>,
        start_from: Option<&str>,
        recursive: bool,
    ) -> Vec<(String, String)> {
        let mut q = Vec::new();
        if let Some(t) = page_token {
            q.push((self.page_token_param.clone(), t.to_string()));
        }
        if let Some(s) = start_from {
            q.push((self.start_from_param.clone(), s.to_string()));
        }
        if recursive {
            q.push((self.recursive_param.clone(), "true".to_string()));
        }
        q
    }

    fn parse_list(&self, body: &[u8]) -> ObjectStoreResult<RestListPage> {
        let root: serde_json::Value = serde_json::from_slice(body).map_err(generic_err)?;
        let mut objects = Vec::new();
        if let Some(entries) = root.get(&self.contents_field).and_then(|v| v.as_array()) {
            for entry in entries {
                let is_dir = entry
                    .get(&self.entry_is_directory_field)
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_dir {
                    continue;
                }
                let Some(path) = entry.get(&self.entry_path_field).and_then(|v| v.as_str()) else {
                    continue;
                };
                let path = match &self.entry_strip_prefix {
                    Some(prefix) => match path.strip_prefix(prefix.as_str()) {
                        Some(rest) => rest.trim_start_matches('/'),
                        None => {
                            return Err(generic_msg(format!(
                                "list entry `{path}` is outside the configured prefix `{prefix}`"
                            )))
                        }
                    },
                    None => path,
                };
                let size = entry
                    .get(&self.entry_size_field)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let last_modified = entry
                    .get(&self.entry_last_modified_field)
                    .and_then(|v| v.as_u64())
                    .map(|ms| {
                        (std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(ms))
                            .into()
                    })
                    .unwrap_or(chrono::DateTime::UNIX_EPOCH);
                // Skip unparseable paths rather than failing the whole page.
                let Ok(location) = Path::parse(path) else {
                    continue;
                };
                objects.push(ObjectMeta {
                    location,
                    last_modified,
                    size,
                    e_tag: None,
                    version: None,
                });
            }
        }
        let next_page_token = root
            .get(&self.next_page_token_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(RestListPage {
            objects,
            next_page_token,
        })
    }

    fn put_query(&self, overwrite: bool) -> Vec<(String, String)> {
        vec![(self.overwrite_param.clone(), overwrite.to_string())]
    }

    fn map_status(&self, status: reqwest::StatusCode, path: &str) -> Option<ObjectStoreError> {
        match status {
            reqwest::StatusCode::NOT_FOUND => Some(ObjectStoreError::NotFound {
                path: path.to_string(),
                source: "HTTP 404".into(),
            }),
            reqwest::StatusCode::CONFLICT => Some(ObjectStoreError::AlreadyExists {
                path: path.to_string(),
                source: "HTTP 409".into(),
            }),
            _ => None,
        }
    }
}

/// A generic REST/HTTP-backed [`ObjectStore`]. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct RestObjectStore {
    base_url: String,
    client: Client,
    auth: Arc<dyn AuthHeaderProvider>,
    contract: Arc<dyn RestFileApiContract>,
    /// Retries (beyond the first attempt) for transient failures on idempotent requests. 0
    /// disables.
    max_retries: u32,
    /// Verify a `Create` put by reading it back when the outcome is ambiguous (5xx / dropped
    /// connection), so a write that landed despite the error is not mistaken for a conflict.
    verify_on_ambiguous: bool,
}

impl std::fmt::Display for RestObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RestObjectStore({})", self.base_url)
    }
}

impl RestObjectStore {
    /// Create a store targeting `base_url`, using `client` for transport, `auth` for per-request
    /// headers, and `contract` for the REST dialect.
    pub fn new(
        base_url: impl Into<String>,
        client: Client,
        auth: Arc<dyn AuthHeaderProvider>,
        contract: Arc<dyn RestFileApiContract>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            client,
            auth,
            contract,
            max_retries: 0,
            verify_on_ambiguous: false,
        }
    }

    /// Retry transient failures (5xx, connect/timeout) on idempotent requests up to `n` times.
    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Verify a `Create` put by reading it back on an ambiguous outcome.
    pub fn with_verify_on_ambiguous(mut self, verify: bool) -> Self {
        self.verify_on_ambiguous = verify;
        self
    }

    /// Fetch the current auth headers from the provider.
    fn headers(&self) -> ObjectStoreResult<HeaderMap> {
        self.auth.headers()
    }

    /// Send an idempotent request, retrying transient failures (5xx, connect/timeout) up to
    /// [`Self::max_retries`]. Returns the response for the caller to map via
    /// [`Self::check_status`].
    async fn send_idempotent(
        &self,
        make: impl Fn(&Client, HeaderMap) -> reqwest::RequestBuilder,
    ) -> ObjectStoreResult<reqwest::Response> {
        let mut retry = 0;
        loop {
            let last = retry >= self.max_retries;
            // Fetch headers per attempt so a refreshable provider can produce a fresh token.
            let headers = self.headers()?;
            match make(&self.client, headers).send().await {
                Ok(resp) if !last && resp.status().is_server_error() => {}
                Ok(resp) => return Ok(resp),
                Err(e) if !last && is_transient(&e) => {}
                Err(e) => return Err(generic_err(e)),
            }
            retry += 1;
            backoff(retry).await;
        }
    }

    /// PUT a `Create`, verifying the result on an ambiguous outcome (5xx or transient transport
    /// error). Reads the object back to distinguish a write that landed (success) from a real
    /// conflict, retrying only while the write is confirmed absent.
    ///
    /// Assumes the backend writes objects verbatim and atomically, and that commit bodies carry
    /// writer-unique content -- so a byte-for-byte match implies we wrote it, not a competitor.
    async fn put_create_verified(
        &self,
        path: &str,
        url: &str,
        query: &[(String, String)],
        body: Bytes,
    ) -> ObjectStoreResult<PutResult> {
        let mut retry = 0;
        loop {
            // Fetch headers per attempt so a refreshable provider can produce a fresh token.
            match self
                .client
                .put(url)
                .query(query)
                .headers(self.headers()?)
                .body(body.clone())
                .send()
                .await
            {
                Ok(resp) => {
                    if let Some(err) = self.contract.map_status(resp.status(), path) {
                        return Err(err);
                    }
                    if !resp.status().is_server_error() {
                        resp.error_for_status().map_err(generic_err)?;
                        return Ok(put_result());
                    }
                }
                Err(e) if is_transient(&e) => {}
                Err(e) => return Err(generic_err(e)),
            }
            // Ambiguous outcome -- read back to tell a landed write from a real conflict.
            match self.read_back(path, &body).await? {
                WriteState::Matches => return Ok(put_result()),
                WriteState::Differs => {
                    return Err(ObjectStoreError::AlreadyExists {
                        path: path.to_string(),
                        source: "verified conflicting write".into(),
                    })
                }
                WriteState::Absent => {}
            }
            if retry >= self.max_retries {
                return Err(generic_msg(format!(
                    "put could not confirm write for `{path}`"
                )));
            }
            retry += 1;
            backoff(retry).await;
        }
    }

    /// Read `path` back and compare its bytes with `expected`.
    async fn read_back(&self, path: &str, expected: &Bytes) -> ObjectStoreResult<WriteState> {
        match self.get_file(path, None).await {
            Ok((bytes, _)) if bytes == *expected => Ok(WriteState::Matches),
            Ok(_) => Ok(WriteState::Differs),
            Err(ObjectStoreError::NotFound { .. }) => Ok(WriteState::Absent),
            Err(e) => Err(e),
        }
    }

    /// Delete a single object via HTTP `DELETE`.
    async fn delete_one(&self, location: &Path) -> ObjectStoreResult<()> {
        let path = location.as_ref().trim_end_matches('/');
        let url = self.contract.file_url(&self.base_url, path);
        let response = self
            .client
            .delete(&url)
            .headers(self.headers()?)
            .send()
            .await
            .map_err(generic_err)?;
        self.check_status(response, path)?;
        Ok(())
    }

    /// Apply the contract's status mapping, then reqwest's default error-for-status, returning
    /// the response unchanged on success.
    fn check_status(
        &self,
        response: reqwest::Response,
        path: &str,
    ) -> ObjectStoreResult<reqwest::Response> {
        if let Some(err) = self.contract.map_status(response.status(), path) {
            return Err(err);
        }
        response.error_for_status().map_err(generic_err)
    }

    async fn get_file(
        &self,
        path: &str,
        range_header: Option<&str>,
    ) -> ObjectStoreResult<(Bytes, HeaderMap)> {
        let url = self.contract.file_url(&self.base_url, path);
        let range = range_header
            .map(reqwest::header::HeaderValue::from_str)
            .transpose()
            .map_err(generic_err)?;
        let response = self
            .send_idempotent(|c, mut h| {
                if let Some(v) = &range {
                    h.insert(reqwest::header::RANGE, v.clone());
                }
                c.get(&url).headers(h)
            })
            .await?;
        let response = self.check_status(response, path)?;
        let resp_headers = response.headers().clone();
        let body = response.bytes().await.map_err(generic_err)?;
        Ok((body, resp_headers))
    }

    /// Issue an HTTP `HEAD` and build [`ObjectMeta`] from the response headers, without
    /// downloading the body. Used to serve `get_opts(head = true)` / `head()`.
    async fn head_meta(&self, path: &str, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let url = self.contract.file_url(&self.base_url, path);
        let response = self.send_idempotent(|c, h| c.head(&url).headers(h)).await?;
        let response = self.check_status(response, path)?;
        let headers = response.headers();
        let size = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let last_modified = parse_last_modified(headers);
        let e_tag = parse_etag(headers);
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified,
            size,
            e_tag,
            version: None,
        })
    }

    /// Stream a paginated listing of `prefix`. When `exclusive_offset` is set, entries at or
    /// before it are dropped client-side so the [`ObjectStore::list_with_offset`] exclusive-offset
    /// contract holds regardless of how the backend interprets its own offset parameter.
    fn list_paginated(
        &self,
        prefix: String,
        start_from: Option<String>,
        exclusive_offset: Option<Path>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let stream = async_stream::stream! {
            let mut page_token: Option<String> = None;
            // start_from applies only to the first request; page_token drives later pages.
            let mut start_from = start_from;
            // A `start_from` offset implies a flat (non-recursive) listing; a plain list with no
            // offset recurses to honor the ObjectStore::list contract.
            let recursive = start_from.is_none();
            // The contract requires ascending paths across the whole listing; verified here so a
            // misordered backend fails loudly instead of corrupting log replay.
            let mut last_path: Option<Path> = None;
            'pages: loop {
                let url = store.contract.directory_url(&store.base_url, &prefix);
                let query = store.contract.list_query(
                    page_token.as_deref(),
                    start_from.as_deref(),
                    recursive,
                );
                start_from = None;
                let response = match store
                    .send_idempotent(|c, h| c.get(url.as_str()).query(&query).headers(h))
                    .await
                {
                    Ok(r) => r,
                    Err(e) => { yield Err(e); break; }
                };
                if let Some(err) = store.contract.map_status(response.status(), &prefix) {
                    // A missing directory lists as empty -- but only on the first page. A NotFound
                    // mid-pagination (page_token set) means the listing was truncated, so surface it
                    // rather than silently returning a partial result.
                    if !matches!(err, ObjectStoreError::NotFound { .. }) || page_token.is_some() {
                        yield Err(err);
                    }
                    break;
                }
                let response = match response.error_for_status() {
                    Ok(r) => r,
                    Err(e) => { yield Err(generic_err(e)); break; }
                };
                let body = match response.bytes().await {
                    Ok(b) => b,
                    Err(e) => { yield Err(generic_err(e)); break; }
                };
                let page = match store.contract.parse_list(&body) {
                    Ok(p) => p,
                    Err(e) => { yield Err(e); break; }
                };
                for meta in page.objects {
                    // Enforce the exclusive-offset contract client-side.
                    if let Some(off) = &exclusive_offset {
                        if meta.location <= *off {
                            continue;
                        }
                    }
                    if let Some(last) = &last_path {
                        if meta.location < *last {
                            yield Err(generic_msg(format!(
                                "REST listing returned out-of-order entry `{}` after `{}`; \
                                 RestFileApiContract must return lexicographically sorted paths",
                                meta.location, last
                            )));
                            break 'pages;
                        }
                    }
                    last_path = Some(meta.location.clone());
                    yield Ok(meta);
                }
                match page.next_page_token {
                    Some(token) => page_token = Some(token),
                    None => break,
                }
            }
        };
        Box::pin(stream)
    }
}

/// Convert an HTTP `GetRange` into a `Range` header value.
fn get_range_to_header(range: &GetRange) -> String {
    match range {
        GetRange::Bounded(r) => format!("bytes={}-{}", r.start, r.end.saturating_sub(1)),
        GetRange::Offset(n) => format!("bytes={}-", n),
        GetRange::Suffix(n) => format!("bytes=-{}", n),
    }
}

/// Parse a `Content-Range: bytes start-end/total` header into `(range, total_size)`.
///
/// Errors on a malformed header rather than guessing, matching `object_store`'s own HTTP client:
/// a server that sends a partial response with a bogus `Content-Range` should surface as an error,
/// not silently degrade the reported range/size.
fn parse_content_range(header: &str) -> ObjectStoreResult<(std::ops::Range<u64>, u64)> {
    let invalid = || generic_msg(format!("malformed Content-Range header: `{header}`"));
    let (range_part, total_part) = header
        .strip_prefix("bytes ")
        .and_then(|inner| inner.split_once('/'))
        .ok_or_else(invalid)?;
    let total = total_part.parse::<u64>().map_err(|_| invalid())?;
    let (start, end) = range_part.split_once('-').ok_or_else(invalid)?;
    let start = start.parse::<u64>().map_err(|_| invalid())?;
    let end = end.parse::<u64>().map_err(|_| invalid())?;
    Ok((start..end.saturating_add(1), total))
}

fn generic_err<E: std::error::Error + Send + Sync + 'static>(err: E) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "RestObjectStore",
        source: Box::new(err),
    }
}

/// Build a generic [`ObjectStoreError`] from a message.
fn generic_msg(msg: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "RestObjectStore",
        source: msg.into(),
    }
}

/// Build the [`ObjectStoreError::NotSupported`] returned for an operation Delta never issues.
fn not_supported(op: &str) -> ObjectStoreError {
    ObjectStoreError::NotSupported {
        source: format!("RestObjectStore does not support {op}").into(),
    }
}

/// A successful PUT result; this store surfaces no etag or version.
fn put_result() -> PutResult {
    PutResult {
        e_tag: None,
        version: None,
    }
}

/// Outcome of reading a file back to compare against bytes we tried to write.
enum WriteState {
    Matches,
    Differs,
    Absent,
}

/// Transport-level failures worth retrying for an idempotent request.
fn is_transient(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

/// Exponential backoff for retry `n` (1-based): 100ms doubling, capped at 2s. `n.min(6)` bounds
/// the shift (`50 << 6` already exceeds the ceiling).
async fn backoff(n: u32) {
    let ms = (50u64 << n.min(6)).min(2_000);
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Parse the `Last-Modified` response header (RFC 2822), defaulting to the Unix epoch when it is
/// absent or unparseable.
fn parse_last_modified(headers: &HeaderMap) -> chrono::DateTime<chrono::Utc> {
    headers
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or(chrono::DateTime::UNIX_EPOCH)
}

/// Extract the `ETag` response header, if present and valid UTF-8.
fn parse_etag(headers: &HeaderMap) -> Option<String> {
    headers
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Options for the [`reqwest::Client`] backing a [`RestObjectStore`]. All fields are optional;
/// the default value yields a plain client.
#[derive(Debug, Clone, Default)]
pub struct RestClientOptions {
    /// PEM client-certificate path. mTLS is enabled only when cert, key, and CA are all set.
    pub cert_path: Option<String>,
    /// PEM client-private-key path.
    pub key_path: Option<String>,
    /// PEM CA-bundle path used to verify the server.
    pub ca_path: Option<String>,
    /// DNS overrides as `host=ip:port` entries: connect to the given address for `host`,
    /// bypassing the system resolver. TLS SNI/hostname verification is unchanged.
    pub dns_overrides: Vec<String>,
    /// Request timeout in seconds; `None` uses reqwest's default.
    pub timeout_secs: Option<u64>,
}

/// Build a [`reqwest::Client`] from [`RestClientOptions`]. Enables mTLS when `cert_path`,
/// `key_path`, and `ca_path` are all present.
pub fn build_rest_client(opts: &RestClientOptions) -> ObjectStoreResult<Client> {
    let mut builder = Client::builder().use_rustls_tls();
    if let Some(secs) = opts.timeout_secs {
        builder = builder.timeout(Duration::from_secs(secs));
    }
    match (&opts.cert_path, &opts.key_path, &opts.ca_path) {
        (Some(cert), Some(key), Some(ca)) => {
            let mut identity_pem = std::fs::read(cert).map_err(generic_err)?;
            identity_pem.extend_from_slice(&std::fs::read(key).map_err(generic_err)?);
            let identity = Identity::from_pem(&identity_pem).map_err(generic_err)?;
            let ca_cert = Certificate::from_pem(&std::fs::read(ca).map_err(generic_err)?)
                .map_err(generic_err)?;
            builder = builder.identity(identity).add_root_certificate(ca_cert);
        }
        (None, None, None) => {}
        // A partial mTLS config is an error, not a silent fall-through to a plain client.
        _ => {
            return Err(generic_msg(
                "partial mTLS config: cert_path, key_path, and ca_path must be set \
                 together (or all omitted)",
            ));
        }
    }
    for entry in &opts.dns_overrides {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (host, addr) = entry.split_once('=').ok_or_else(|| {
            generic_msg(format!(
                "invalid DNS override (expected `host=ip:port`): {entry}"
            ))
        })?;
        let addr: SocketAddr = addr.parse().map_err(generic_err)?;
        builder = builder.resolve(host, addr);
    }
    builder.build().map_err(generic_err)
}

#[async_trait]
impl ObjectStore for RestObjectStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let path_str = location.as_ref().trim_end_matches('/');

        // A head-only request resolves metadata via HTTP HEAD and returns an empty body, so we
        // don't download the object just to read its size/etag (e.g. parquet footer probes).
        if options.head {
            let meta = self.head_meta(path_str, location).await?;
            let size = meta.size;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..size,
                meta,
                attributes: Attributes::new(),
            });
        }

        let range_header = options.range.as_ref().map(get_range_to_header);
        let (content, headers) = self.get_file(path_str, range_header.as_deref()).await?;

        // Derive byte range + total size from Content-Range (partial responses) or the body length.
        let (range, total_size) = match headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
        {
            Some(cr) => parse_content_range(cr)?,
            None => (0..content.len() as u64, content.len() as u64),
        };

        let last_modified = parse_last_modified(&headers);
        let e_tag = parse_etag(&headers);

        let stream = Box::pin(futures::stream::once(futures::future::ready(Ok(content))));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified,
                size: total_size,
                e_tag,
                version: None,
            },
            range,
            attributes: Attributes::new(),
        })
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let overwrite = match opts.mode {
            PutMode::Overwrite => true,
            PutMode::Create => false,
            PutMode::Update(_) => return Err(not_supported("PutMode::Update")),
        };
        let path_str = location.as_ref().trim_end_matches('/');
        let url = self.contract.file_url(&self.base_url, path_str);
        let query = self.contract.put_query(overwrite);
        let body: Bytes = payload.into();
        // A non-idempotent PUT is only safe to retry when we can verify the write landed, so the
        // verify path is gated on `Create` + verification enabled.
        if !overwrite && self.verify_on_ambiguous {
            return self.put_create_verified(path_str, &url, &query, body).await;
        }
        let response = self
            .client
            .put(&url)
            .query(&query)
            .headers(self.headers()?)
            .body(body)
            .send()
            .await
            .map_err(generic_err)?;
        self.check_status(response, path_str)?;
        Ok(put_result())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        self.list_paginated(prefix, None, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        // `offset` is a full path beginning with `prefix`; the REST list offset is relative to
        // the directory being listed, so send only the leaf portion. The full `offset` is kept
        // to enforce exclusivity client-side.
        let offset_str = {
            let raw = offset.as_ref();
            if !prefix.is_empty() && raw.starts_with(&prefix) {
                raw[prefix.len()..].trim_start_matches('/').to_string()
            } else {
                raw.to_string()
            }
        };
        self.list_paginated(prefix, Some(offset_str), Some(offset.clone()))
    }

    // object_store 0.12 (arrow-57) has `delete` on the trait; 0.13 (arrow-58) replaced it with the
    // required `delete_stream` (and `delete` moved to ObjectStoreExt) -- both route to delete_one.
    #[cfg(all(feature = "arrow-57", not(feature = "arrow-58")))]
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.delete_one(location).await
    }

    #[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let store = self.clone();
        Box::pin(locations.then(move |location| {
            let store = store.clone();
            async move {
                let location = location?;
                store.delete_one(&location).await?;
                Ok(location)
            }
        }))
    }

    // === Operations Delta never issues against a REST file store ===
    // These return NotSupported. object_store's copy API differs across backends: 0.13 (arrow-58)
    // has copy_opts, 0.12 (arrow-57) has copy / copy_if_not_exists -- cfg-gated to match.

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        Err(not_supported("list_with_delimiter"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Err(not_supported("multipart upload"))
    }

    #[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        Err(not_supported("copy"))
    }

    #[cfg(all(feature = "arrow-57", not(feature = "arrow-58")))]
    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Err(not_supported("copy"))
    }

    #[cfg(all(feature = "arrow-57", not(feature = "arrow-58")))]
    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Err(not_supported("copy_if_not_exists"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // 0.13 puts the get/put/copy convenience methods on ObjectStoreExt; 0.12 has them on
    // ObjectStore directly, where importing the Ext trait too would make the calls ambiguous.
    #[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
    use delta_kernel::object_store::ObjectStoreExt;
    use delta_kernel::object_store::{
        GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
    };
    use futures::StreamExt as _;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn token_producer(
        calls: Arc<AtomicUsize>,
        ttl: Option<std::time::Duration>,
    ) -> RefreshingHeaderProvider {
        RefreshingHeaderProvider::new(move || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            let headers =
                headers_from_pairs([("authorization".to_string(), format!("Bearer token-{n}"))])?;
            Ok((headers, ttl))
        })
    }

    /// Without a TTL, [`RefreshingHeaderProvider`] produces on every call -- refreshed per request.
    #[test]
    fn refreshing_header_provider_without_ttl_produces_each_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = token_producer(calls.clone(), None);
        let first = provider.headers().unwrap();
        let second = provider.headers().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(first.get("authorization").unwrap(), "Bearer token-0");
        assert_eq!(second.get("authorization").unwrap(), "Bearer token-1");
    }

    /// With a TTL, headers are cached and the closure runs once until the TTL nears expiry.
    #[test]
    fn refreshing_header_provider_with_ttl_caches() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = token_producer(calls.clone(), Some(std::time::Duration::from_secs(3600)));
        let first = provider.headers().unwrap();
        let second = provider.headers().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.get("authorization").unwrap(), "Bearer token-0");
        assert_eq!(second.get("authorization").unwrap(), "Bearer token-0");
    }

    /// A minimal REST dialect for tests: files at `/files/{path}`, directories at
    /// `/dirs/{path}`, a `{ "contents": [{path,size}], "nextPageToken" }` list body, and the
    /// canonical 404 -> NotFound / 409 -> AlreadyExists status mapping.
    #[derive(Debug)]
    struct TestContract;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestFile {
        path: String,
        size: Option<u64>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestList {
        #[serde(default)]
        contents: Vec<TestFile>,
        next_page_token: Option<String>,
    }

    impl RestFileApiContract for TestContract {
        fn file_url(&self, base_url: &str, path: &str) -> String {
            format!("{base_url}/files/{}", path.trim_start_matches('/'))
        }
        fn directory_url(&self, base_url: &str, path: &str) -> String {
            format!("{base_url}/dirs/{}", path.trim_start_matches('/'))
        }
        fn list_query(
            &self,
            page_token: Option<&str>,
            start_from: Option<&str>,
            recursive: bool,
        ) -> Vec<(String, String)> {
            let mut q = Vec::new();
            if let Some(t) = page_token {
                q.push(("page_token".into(), t.into()));
            }
            if let Some(s) = start_from {
                q.push(("start_from".into(), s.into()));
            }
            if recursive {
                q.push(("recursive".into(), "true".into()));
            }
            q
        }
        fn parse_list(&self, body: &[u8]) -> ObjectStoreResult<RestListPage> {
            let list: TestList = serde_json::from_slice(body).map_err(generic_err)?;
            let objects = list
                .contents
                .into_iter()
                .map(|f| ObjectMeta {
                    location: Path::from(f.path),
                    last_modified: chrono::DateTime::UNIX_EPOCH,
                    size: f.size.unwrap_or(0),
                    e_tag: None,
                    version: None,
                })
                .collect();
            Ok(RestListPage {
                objects,
                next_page_token: list.next_page_token,
            })
        }
        fn put_query(&self, overwrite: bool) -> Vec<(String, String)> {
            vec![("overwrite".into(), overwrite.to_string())]
        }
        fn map_status(&self, status: StatusCode, path: &str) -> Option<ObjectStoreError> {
            match status {
                StatusCode::NOT_FOUND => Some(ObjectStoreError::NotFound {
                    path: path.to_string(),
                    source: "404".into(),
                }),
                StatusCode::CONFLICT => Some(ObjectStoreError::AlreadyExists {
                    path: path.to_string(),
                    source: "409".into(),
                }),
                _ => None,
            }
        }
    }

    fn store_for(server: &MockServer, headers: HeaderMap) -> RestObjectStore {
        RestObjectStore::new(
            server.uri(),
            Client::new(),
            Arc::new(StaticHeaderProvider::new(headers)),
            Arc::new(TestContract),
        )
    }

    #[tokio::test]
    async fn get_returns_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".as_slice()))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let bytes = store
            .get(&Path::from("a.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"hello");
    }

    /// A malformed `Content-Range` surfaces as an error rather than silently degrading the
    /// reported range/size (matches `object_store`'s own client).
    #[tokio::test]
    async fn get_malformed_content_range_yields_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-range", "bytes not-a-range")
                    .set_body_bytes(b"hello".as_slice()),
            )
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let err = store.get(&Path::from("a.txt")).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::Generic { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn head_uses_http_head_and_returns_meta_without_body() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/files/a.txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "42")
                    .insert_header("etag", "\"abc\""),
            )
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let res = store
            .get_opts(
                &Path::from("a.txt"),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(res.meta.size, 42);
        assert_eq!(res.meta.e_tag.as_deref(), Some("\"abc\""));
        // head must not stream a body.
        let body = res.bytes().await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn put_overwrite_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        store
            .put(&Path::from("a.txt"), PutPayload::from_static(b"hi"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_create_conflict_maps_to_already_exists() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(409))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let err = store
            .put_opts(
                &Path::from("a.txt"),
                PutPayload::from_static(b"hi"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn put_update_is_not_supported() {
        let server = MockServer::start().await;
        let store = store_for(&server, HeaderMap::new());
        let err = store
            .put_opts(
                &Path::from("a.txt"),
                PutPayload::from_static(b"hi"),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some("v1".to_string()),
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotSupported { .. }));
    }

    #[tokio::test]
    async fn list_paginates_across_pages() {
        let server = MockServer::start().await;
        // First page (no page_token) returns one file + a token; second page returns two.
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .and(wiremock::matchers::query_param_is_missing("page_token"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"contents":[{"path":"d/1","size":1}],"nextPageToken":"p2"}"#,
                ),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .and(wiremock::matchers::query_param("page_token", "p2"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/2","size":2},{"path":"d/3","size":3}]}"#,
            ))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let metas: Vec<_> = store
            .list(Some(&Path::from("d")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        let paths: Vec<_> = metas
            .iter()
            .map(|m| m.location.as_ref().to_string())
            .collect();
        assert_eq!(paths, vec!["d/1", "d/2", "d/3"]);
    }

    #[tokio::test]
    async fn list_missing_directory_is_empty() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dirs/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let metas: Vec<_> = store
            .list(Some(&Path::from("missing")))
            .collect::<Vec<_>>()
            .await;
        assert!(metas.is_empty());
    }

    /// A first-page 404 lists as empty, but a 404 *mid-pagination* (page_token set) means the
    /// listing was truncated -- it must surface as an error, not a silent partial result.
    #[tokio::test]
    async fn list_not_found_mid_pagination_yields_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .and(wiremock::matchers::query_param_is_missing("page_token"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"contents":[{"path":"d/1","size":1}],"nextPageToken":"p2"}"#,
                ),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .and(wiremock::matchers::query_param("page_token", "p2"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
        // First-page entry is yielded, then the mid-pagination 404 surfaces as an error.
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(
            matches!(results[1], Err(ObjectStoreError::NotFound { .. })),
            "got {:?}",
            results[1]
        );
    }

    /// `list_with_offset` must exclude the offset entry itself even if the backend echoes it
    /// back, so log replay never re-ingests the offset commit.
    #[tokio::test]
    async fn list_with_offset_excludes_offset_entry() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/2","size":2},{"path":"d/3","size":3}]}"#,
            ))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let paths: Vec<_> = store
            .list_with_offset(Some(&Path::from("d")), &Path::from("d/2"))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap().location.as_ref().to_string())
            .collect();
        assert_eq!(paths, vec!["d/3"]);
    }

    /// An out-of-order page violates the sorted-listing contract and must surface as an error
    /// rather than feeding log replay a misordered listing.
    #[tokio::test]
    async fn list_out_of_order_entries_yields_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/3","size":3},{"path":"d/1","size":1}]}"#,
            ))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().location.as_ref(), "d/3");
        assert!(matches!(results[1], Err(ObjectStoreError::Generic { .. })));
    }

    #[tokio::test]
    async fn auth_headers_are_sent() {
        let server = MockServer::start().await;
        // Only matches when the provider's header is present.
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .and(header("x-test-auth", "secret"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok".as_slice()))
            .mount(&server)
            .await;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-test-auth",
            reqwest::header::HeaderValue::from_static("secret"),
        );
        let store = store_for(&server, headers);
        let bytes = store
            .get(&Path::from("a.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"ok");
    }

    #[tokio::test]
    async fn unsupported_operations_error_not_panic() {
        let server = MockServer::start().await;
        let store = store_for(&server, HeaderMap::new());
        assert!(matches!(
            store
                .copy(&Path::from("a"), &Path::from("b"))
                .await
                .unwrap_err(),
            ObjectStoreError::NotSupported { .. }
        ));
        assert!(matches!(
            store
                .list_with_delimiter(Some(&Path::from("d")))
                .await
                .unwrap_err(),
            ObjectStoreError::NotSupported { .. }
        ));
    }

    #[tokio::test]
    async fn config_driven_contract_parses_list_and_reads() {
        let server = MockServer::start().await;
        let config = RestEndpointConfig {
            files_prefix: "api/2.0/fs/files".into(),
            directories_prefix: "api/2.0/fs/directories".into(),
            ..Default::default()
        };
        // Listing with one file and one subdirectory; the directory entry must be filtered out.
        Mock::given(method("GET"))
            .and(path("/api/2.0/fs/directories/d"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/f1","fileSize":7,"isDirectory":false,"lastModified":1000},{"path":"d/sub","isDirectory":true}]}"#,
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/fs/files/d/f1"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"content".as_slice()))
            .mount(&server)
            .await;
        let store = RestObjectStore::new(
            server.uri(),
            Client::new(),
            Arc::new(StaticHeaderProvider::new(HeaderMap::new())),
            Arc::new(config),
        );
        let metas: Vec<_> = store
            .list(Some(&Path::from("d")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].location.as_ref(), "d/f1");
        assert_eq!(metas[0].size, 7);
        assert_eq!(metas[0].last_modified.timestamp_millis(), 1000);
        let bytes = store
            .get(&Path::from("d/f1"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"content");
    }

    #[tokio::test]
    async fn delete_issues_http_delete() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new());
        store.delete(&Path::from("a.txt")).await.unwrap();
    }

    fn store_with_strip_prefix(server: &MockServer, prefix: &str) -> RestObjectStore {
        let config = RestEndpointConfig {
            directories_prefix: "dirs".into(),
            entry_strip_prefix: Some(prefix.into()),
            ..Default::default()
        };
        RestObjectStore::new(
            server.uri(),
            Client::new(),
            Arc::new(StaticHeaderProvider::new(HeaderMap::new())),
            Arc::new(config),
        )
    }

    #[tokio::test]
    async fn list_strips_entry_prefix() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"contents":[{"path":"/TablesById/u/d/f1","fileSize":1}]}"#,
                ),
            )
            .mount(&server)
            .await;
        let store = store_with_strip_prefix(&server, "/TablesById/u");
        let metas: Vec<_> = store
            .list(Some(&Path::from("d")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].location.as_ref(), "d/f1");
    }

    #[tokio::test]
    async fn list_rejects_entry_outside_strip_prefix() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dirs/d"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(r#"{"contents":[{"path":"/other/x"}]}"#),
            )
            .mount(&server)
            .await;
        let store = store_with_strip_prefix(&server, "/TablesById/u");
        let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
        assert!(matches!(
            results.as_slice(),
            [Err(ObjectStoreError::Generic { .. })]
        ));
    }

    #[tokio::test]
    async fn get_retries_transient_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok".as_slice()))
            .with_priority(2)
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new()).with_max_retries(1);
        let bytes = store
            .get(&Path::from("a.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"ok");
    }

    /// PUT returns 500; the verified Create then reads back via `read_back`, with the read-back
    /// response supplied by `get`. `max_retries` is 0, so an absent read-back fails immediately.
    async fn put_create_with_readback(
        server: &MockServer,
        get: ResponseTemplate,
    ) -> ObjectStoreResult<()> {
        Mock::given(method("PUT"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(500))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(get)
            .mount(server)
            .await;
        store_for(server, HeaderMap::new())
            .with_verify_on_ambiguous(true)
            .put_opts(
                &Path::from("a.txt"),
                PutPayload::from_static(b"hi"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
    }

    /// An ambiguous 5xx whose read-back matches what we wrote is treated as a success.
    #[tokio::test]
    async fn put_create_verified_matching_write_succeeds() {
        let server = MockServer::start().await;
        let get = ResponseTemplate::new(200).set_body_bytes(b"hi".as_slice());
        put_create_with_readback(&server, get).await.unwrap();
    }

    /// An ambiguous 5xx whose read-back differs is a real conflict.
    #[tokio::test]
    async fn put_create_verified_conflicting_write_is_already_exists() {
        let server = MockServer::start().await;
        let get = ResponseTemplate::new(200).set_body_bytes(b"other".as_slice());
        let err = put_create_with_readback(&server, get).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::AlreadyExists { .. }));
    }

    /// An ambiguous 5xx whose read-back is absent (write never landed) fails to confirm.
    #[tokio::test]
    async fn put_create_verified_absent_write_fails_to_confirm() {
        let server = MockServer::start().await;
        let err = put_create_with_readback(&server, ResponseTemplate::new(404))
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::Generic { .. }));
    }

    /// Exhausting retries on persistent 5xx surfaces an error rather than looping or succeeding.
    #[tokio::test]
    async fn get_retry_exhaustion_surfaces_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let store = store_for(&server, HeaderMap::new()).with_max_retries(1);
        let err = store.get(&Path::from("a.txt")).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::Generic { .. }));
    }

    /// A verified Create retries while the read-back is absent, then succeeds once it lands.
    #[tokio::test]
    async fn put_create_verified_retries_until_write_lands() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(404))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hi".as_slice()))
            .with_priority(2)
            .mount(&server)
            .await;
        store_for(&server, HeaderMap::new())
            .with_verify_on_ambiguous(true)
            .with_max_retries(1)
            .put_opts(
                &Path::from("a.txt"),
                PutPayload::from_static(b"hi"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_missing_maps_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/files/a.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let err = store_for(&server, HeaderMap::new())
            .delete(&Path::from("a.txt"))
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound { .. }));
    }

    #[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
    #[tokio::test]
    async fn delete_stream_deletes_each_path() {
        let server = MockServer::start().await;
        for p in ["a", "b"] {
            Mock::given(method("DELETE"))
                .and(path(format!("/files/{p}")))
                .respond_with(ResponseTemplate::new(204))
                .mount(&server)
                .await;
        }
        let store = store_for(&server, HeaderMap::new());
        let paths = futures::stream::iter([Ok(Path::from("a")), Ok(Path::from("b"))]).boxed();
        let deleted: Vec<_> = store.delete_stream(paths).collect::<Vec<_>>().await;
        assert_eq!(deleted.len(), 2);
        assert!(deleted.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn build_rest_client_rejects_partial_mtls() {
        let opts = RestClientOptions {
            cert_path: Some("/x/cert.pem".into()),
            ..Default::default()
        };
        assert!(build_rest_client(&opts).is_err());
    }

    #[test]
    fn build_rest_client_rejects_malformed_dns_override() {
        let missing_eq = RestClientOptions {
            dns_overrides: vec!["no-equals".into()],
            ..Default::default()
        };
        assert!(build_rest_client(&missing_eq).is_err());
        let bad_addr = RestClientOptions {
            dns_overrides: vec!["host=not-an-addr".into()],
            ..Default::default()
        };
        assert!(build_rest_client(&bad_addr).is_err());
    }

    #[test]
    fn build_rest_client_plain_and_valid_dns_override_succeed() {
        assert!(build_rest_client(&RestClientOptions::default()).is_ok());
        let opts = RestClientOptions {
            // Empty entries are skipped; a valid `host=ip:port` is accepted.
            dns_overrides: vec!["".into(), "example.com=127.0.0.1:8443".into()],
            timeout_secs: Some(5),
            ..Default::default()
        };
        assert!(build_rest_client(&opts).is_ok());
    }
}
