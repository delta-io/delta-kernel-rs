//! Auth header providers for [`RestObjectStore`](super::RestObjectStore): the headers attached to
//! every request, including refresh of short-lived credentials.

use std::time::Duration;

use delta_kernel::object_store::Result as ObjectStoreResult;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use super::generic_err;

/// Supplies the HTTP headers attached to every request a
/// [`RestObjectStore`](super::RestObjectStore) makes.
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

/// How far before a header set's TTL expires to refresh it, so a request never uses a credential
/// that expires in flight.
const HEADER_REFRESH_BUFFER: Duration = Duration::from_secs(30);

/// An [`AuthHeaderProvider`] that produces headers via a closure returning `(headers,
/// Option<ttl>)`. `Some(ttl)` caches the headers until a fixed safety margin before `ttl`
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
        // Hold the lock across the (rare) produce call so concurrent refreshes are single-flight.
        let mut cached = self.cached.lock().unwrap();
        if let Some((headers, deadline)) = cached.as_ref() {
            if deadline.saturating_duration_since(std::time::Instant::now()) > HEADER_REFRESH_BUFFER
            {
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
