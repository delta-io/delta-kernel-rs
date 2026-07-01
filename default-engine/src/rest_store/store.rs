//! [`RestObjectStore`]: a generic REST/HTTP-backed
//! [`ObjectStore`](delta_kernel::object_store::ObjectStore). See the [module docs](super).

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
use reqwest::header::HeaderMap;
use reqwest::Client;

use super::auth::AuthHeaderProvider;
use super::contract::RestEndpointConfig;
use super::{generic_err, generic_msg};

/// A generic REST/HTTP-backed [`ObjectStore`]. See the [module docs](super).
#[derive(Debug, Clone)]
pub struct RestObjectStore {
    base_url: String,
    client: Client,
    auth: Arc<dyn AuthHeaderProvider>,
    config: Arc<RestEndpointConfig>,
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
    /// headers, and `config` for the REST dialect.
    pub fn new(
        base_url: impl Into<String>,
        client: Client,
        auth: Arc<dyn AuthHeaderProvider>,
        config: Arc<RestEndpointConfig>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            client,
            auth,
            config,
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
    /// A `409` (conflict) on the *first* attempt is a genuine pre-existing object and returns
    /// `AlreadyExists` immediately. On a *retry*, a `409` is most likely the writer's own
    /// already-landed write (the prior attempt succeeded despite an ambiguous error), so it is
    /// routed through the read-back instead of being trusted verbatim.
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
                    let status = resp.status();
                    if let Some(err) = self.status_error(status, path) {
                        // A conflict on a retry may be our own landed write, so reconcile it via
                        // read-back rather than trusting it; on the first attempt it is a genuine
                        // pre-existing conflict and is returned as-is.
                        let conflict = matches!(err, ObjectStoreError::AlreadyExists { .. });
                        if !(conflict && retry > 0) {
                            return Err(err);
                        }
                    } else if !status.is_server_error() {
                        resp.error_for_status().map_err(generic_err)?;
                        return Ok(put_result());
                    }
                }
                Err(e) if is_transient(&e) => {}
                Err(e) => return Err(generic_err(e)),
            }
            // Ambiguous outcome -- read back to tell a landed write from a real conflict. A
            // transient read-back failure leaves the outcome ambiguous, so treat it like an absent
            // write and consume a retry rather than making it terminal.
            match self.read_back(path, &body).await {
                Ok(WriteState::Matches) => return Ok(put_result()),
                Ok(WriteState::Differs) => {
                    return Err(ObjectStoreError::AlreadyExists {
                        path: path.to_string(),
                        source: "verified conflicting write".into(),
                    })
                }
                Ok(WriteState::Absent) => {}
                Err(e) if retry >= self.max_retries => return Err(e),
                Err(_) => {}
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
            Ok((bytes, _, _)) if bytes == *expected => Ok(WriteState::Matches),
            Ok(_) => Ok(WriteState::Differs),
            Err(ObjectStoreError::NotFound { .. }) => Ok(WriteState::Absent),
            Err(e) => Err(e),
        }
    }

    /// Delete a single object via HTTP `DELETE`. DELETE is idempotent, so transient failures are
    /// retried via [`Self::send_idempotent`].
    async fn delete_one(&self, location: &Path) -> ObjectStoreResult<()> {
        let path = location.as_ref().trim_end_matches('/');
        let url = self.config.file_url(&self.base_url, path);
        let response = self
            .send_idempotent(|c, h| c.delete(&url).headers(h))
            .await?;
        self.check_status(response, path)?;
        Ok(())
    }

    /// Map a non-success HTTP status to an error. `404 -> NotFound` is enforced here -- a universal
    /// HTTP semantic that must not depend on the contract config -- and the remaining codes are
    /// delegated to the config's mapping (e.g. `409 -> AlreadyExists`). Returns `None` on success
    /// or a status the config does not claim.
    fn status_error(&self, status: reqwest::StatusCode, path: &str) -> Option<ObjectStoreError> {
        if status == reqwest::StatusCode::NOT_FOUND {
            return Some(ObjectStoreError::NotFound {
                path: path.to_string(),
                source: "HTTP 404".into(),
            });
        }
        self.config.map_status(status, path)
    }

    /// Apply [`Self::status_error`], then reqwest's default error-for-status, returning the
    /// response unchanged on success.
    fn check_status(
        &self,
        response: reqwest::Response,
        path: &str,
    ) -> ObjectStoreResult<reqwest::Response> {
        if let Some(err) = self.status_error(response.status(), path) {
            return Err(err);
        }
        response.error_for_status().map_err(generic_err)
    }

    /// GET `path` (optionally with a `Range` header) and return the body, response headers, and
    /// HTTP status. The status lets a ranged caller distinguish a partial (`206`) response from a
    /// full-body `200`.
    async fn get_file(
        &self,
        path: &str,
        range_header: Option<&str>,
    ) -> ObjectStoreResult<(Bytes, HeaderMap, reqwest::StatusCode)> {
        let url = self.config.file_url(&self.base_url, path);
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
        let status = response.status();
        let resp_headers = response.headers().clone();
        let body = response.bytes().await.map_err(generic_err)?;
        Ok((body, resp_headers, status))
    }

    /// Issue an HTTP `HEAD` and build [`ObjectMeta`] from the response headers, without
    /// downloading the body. Used to serve `get_opts(head = true)` / `head()`.
    async fn head_meta(&self, path: &str, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let url = self.config.file_url(&self.base_url, path);
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
        recursive: bool,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let stream = async_stream::stream! {
            let mut page_token: Option<String> = None;
            // start_from applies only to the first request; page_token drives later pages.
            let mut start_from = start_from;
            // The contract requires ascending paths across the whole listing; verified here so a
            // misordered backend fails loudly instead of corrupting log replay.
            let mut last_path: Option<Path> = None;
            'pages: loop {
                let url = store.config.directory_url(&store.base_url, &prefix);
                let query = store.config.list_query(
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
                if let Some(err) = store.status_error(response.status(), &prefix) {
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
                let page = match store.config.parse_list(&body) {
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
                                 RestEndpointConfig must return lexicographically sorted paths",
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
    // Reject a reversed or out-of-bounds range: `end < start` would underflow in
    // `GetResult::bytes()`, and `start > total` is nonsensical for a partial response.
    if end < start || start > total {
        return Err(invalid());
    }
    Ok((start..end.saturating_add(1), total))
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

#[async_trait]
impl ObjectStore for RestObjectStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let path_str = location.as_ref().trim_end_matches('/');

        // A head-only request resolves metadata via HTTP HEAD and returns an empty body, so we
        // don't download the object just to read its size/etag (e.g. parquet footer probes).
        if options.head {
            let meta = self.head_meta(path_str, location).await?;
            // Enforce client-side conditional preconditions against the HEAD metadata.
            options.check_preconditions(&meta)?;
            let size = meta.size;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..size,
                meta,
                attributes: Attributes::new(),
            });
        }

        let ranged = options.range.is_some();
        let range_header = options.range.as_ref().map(get_range_to_header);
        let (content, headers, status) = self.get_file(path_str, range_header.as_deref()).await?;

        // A ranged request that comes back non-partial (a 200 with the full body) must not be
        // silently treated as the requested slice. Mirror object_store's `NotPartial` behavior and
        // surface an error rather than handing back the wrong bytes.
        if ranged && status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(generic_msg(format!(
                "ranged GET for `{path_str}` returned a non-partial response (HTTP {status}); \
                 expected 206 Partial Content"
            )));
        }

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

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified,
            size: total_size,
            e_tag,
            version: None,
        };
        // Enforce client-side conditional preconditions (if_match / if_none_match /
        // if_modified_since / if_unmodified_since) against the resolved metadata.
        options.check_preconditions(&meta)?;

        let stream = Box::pin(futures::stream::once(futures::future::ready(Ok(content))));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
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
        // Update (a conditional/compare-and-set put) is not supported: Delta's commit path never
        // issues it, and the REST file API has no compare-and-set primitive to back it.
        let overwrite = match opts.mode {
            PutMode::Overwrite => true,
            PutMode::Create => false,
            PutMode::Update(_) => return Err(not_supported("PutMode::Update")),
        };
        let path_str = location.as_ref().trim_end_matches('/');
        let url = self.config.file_url(&self.base_url, path_str);
        let query = self.config.put_query(overwrite);
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
        // Both `list` and `list_with_offset` recurse; only `list_with_delimiter` is non-recursive.
        self.list_paginated(prefix, None, None, true)
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
        self.list_paginated(prefix, Some(offset_str), Some(offset.clone()), true)
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
    use super::*;

    #[test]
    fn parse_content_range_accepts_valid() {
        let (range, total) = parse_content_range("bytes 2-5/10").unwrap();
        assert_eq!(range, 2..6);
        assert_eq!(total, 10);
    }

    #[test]
    fn parse_content_range_rejects_reversed_range() {
        // `end < start` would underflow in GetResult::bytes(); it must be an error.
        assert!(parse_content_range("bytes 5-2/10").is_err());
    }

    #[test]
    fn parse_content_range_rejects_start_past_total() {
        assert!(parse_content_range("bytes 20-25/10").is_err());
    }
}
