//! REST [`RestObjectStore`] support for [`EngineBuilder`](crate::EngineBuilder).
//!
//! Call [`set_builder_rest_object_store`](crate::set_builder_rest_object_store) to select the REST
//! backend, then configure the REST file API dialect (the [`RestEndpointConfig`] field names),
//! request headers (`header.<Name>`), TLS (`tls.*`), and resilience (`retry.max_retries`,
//! `put.verify_on_ambiguous`) via [`set_builder_option`](crate::set_builder_option). The builder
//! `url` is the REST service base URL (not a Delta table path).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use delta_kernel::object_store::{Error as ObjectStoreError, ObjectStore};
use delta_kernel::{DeltaResult, Error};
use delta_kernel_default_engine::rest_store::{
    build_rest_client, headers_from_pairs, AuthHeaderProvider, RefreshingHeaderProvider,
    RestClientOptions, RestEndpointConfig, RestObjectStore, StaticHeaderProvider,
};
use url::Url;

use crate::{KernelStringSlice, NullableCvoid, TryFromStringSlice};

/// [`set_builder_option`](crate::set_builder_option) key for the legacy REST backend selector.
/// Use [`set_builder_rest_object_store`](crate::set_builder_rest_object_store) instead.
pub(crate) const STORE_BACKEND_KEY: &str = "store.backend";

/// Max `(name, value)` pairs in a [`CAuthHeaders`] struct.
pub const AUTH_MAX_HEADERS: usize = 8;

/// One HTTP header name/value pair borrowed for the duration of a [`CAuthHeaderCallback`] call.
#[repr(C)]
pub struct CAuthHeaderPair {
    pub name: KernelStringSlice,
    pub value: KernelStringSlice,
}

/// Output buffer for a [`CAuthHeaderCallback`] invocation.
///
/// Set `count`, fill `headers[0..count]`, and optionally set `ttl_ms`. `name` and `value` slices
/// must remain valid until the callback returns; the kernel copies them before the next refresh.
#[repr(C)]
pub struct CAuthHeaders {
    pub count: u32,
    pub headers: [CAuthHeaderPair; AUTH_MAX_HEADERS],
    /// How long (in milliseconds) the headers stay valid. `0` means refresh on every request.
    pub ttl_ms: u64,
}

/// Supplies auth headers for a REST-backed engine.
///
/// The kernel invokes this when it needs fresh headers. Fill `out` with `headers[0..count]` and
/// set `ttl_ms` to report how long those headers stay valid. With a non-zero TTL the kernel caches
/// them and re-invokes near expiry; with `ttl_ms = 0` it invokes the callback on every request.
///
/// `context` is the opaque pointer registered via
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store). Both pointers are only
/// valid for the duration of the call and must not be retained.
pub type CAuthHeaderCallback = extern "C" fn(context: NullableCvoid, out: *mut CAuthHeaders);

/// Rust-side adapter pairing a [`CAuthHeaderCallback`] with its opaque `context`. Marked `Send +
/// Sync` so the per-request closure can be shared across threads.
///
/// Safety: the FFI caller guarantees `callback` and `context` are safe to invoke from multiple
/// threads concurrently -- the kernel consults the header provider from async request tasks that
/// may run on any worker thread.
#[derive(Clone, Copy)]
pub(crate) struct FfiAuthHeaderProvider {
    callback: CAuthHeaderCallback,
    context: NullableCvoid,
}
unsafe impl Send for FfiAuthHeaderProvider {}
unsafe impl Sync for FfiAuthHeaderProvider {}

impl FfiAuthHeaderProvider {
    pub(crate) fn new(callback: CAuthHeaderCallback, context: NullableCvoid) -> Self {
        Self { callback, context }
    }

    fn collect(&self) -> DeltaResult<(Vec<(String, String)>, Option<u64>)> {
        let mut headers = empty_c_auth_headers();
        (self.callback)(self.context, &mut headers);
        Ok((auth_pairs_from_c(&headers)?, ttl_ms_from_c(headers.ttl_ms)))
    }
}

fn empty_c_auth_headers() -> CAuthHeaders {
    static EMPTY: &str = "";
    CAuthHeaders {
        count: 0,
        headers: std::array::from_fn(|_| CAuthHeaderPair {
            name: unsafe { KernelStringSlice::new_unsafe(EMPTY) },
            value: unsafe { KernelStringSlice::new_unsafe(EMPTY) },
        }),
        ttl_ms: 0,
    }
}

fn ttl_ms_from_c(ttl_ms: u64) -> Option<u64> {
    if ttl_ms == 0 {
        None
    } else {
        Some(ttl_ms)
    }
}

/// Copy `(name, value)` pairs from borrowed slices in `headers`.
pub(crate) fn auth_pairs_from_c(headers: &CAuthHeaders) -> DeltaResult<Vec<(String, String)>> {
    let count = headers.count as usize;
    if count > AUTH_MAX_HEADERS {
        return Err(Error::generic(format!(
            "auth header count {count} exceeds max {AUTH_MAX_HEADERS}"
        )));
    }

    let mut pairs = Vec::with_capacity(count);
    for slot in &headers.headers[..count] {
        // SAFETY: callback keeps slice memory valid until it returns.
        let name = unsafe { String::try_from_slice(&slot.name) }?;
        let value = unsafe { String::try_from_slice(&slot.value) }?;
        pairs.push((name, value));
    }
    Ok(pairs)
}

/// Reject the legacy `store.backend` builder option; REST is enabled via
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
pub(crate) fn reject_legacy_store_backend_option(
    options: &HashMap<String, String>,
) -> DeltaResult<()> {
    if options.contains_key(STORE_BACKEND_KEY) {
        return Err(Error::generic(format!(
            "do not set `{STORE_BACKEND_KEY}` via set_builder_option; \
             call set_builder_rest_object_store instead"
        )));
    }
    Ok(())
}

/// Build a [`RestObjectStore`] from an engine builder's URL, options, and optional auth callback.
pub(crate) fn build_rest_object_store(
    base_url: &Url,
    options: &HashMap<String, String>,
    auth_callback: Option<FfiAuthHeaderProvider>,
) -> DeltaResult<Arc<dyn ObjectStore>> {
    let config = rest_endpoint_config_from_options(options)?;

    let auth: Arc<dyn AuthHeaderProvider> = match auth_callback {
        Some(cb) => {
            let provider = cb;
            Arc::new(RefreshingHeaderProvider::new(move || {
                let (pairs, ttl_ms) =
                    provider.collect().map_err(|e| ObjectStoreError::Generic {
                        store: "RestObjectStore",
                        source: e.into(),
                    })?;
                Ok((
                    headers_from_pairs(pairs)?,
                    ttl_ms.map(Duration::from_millis),
                ))
            }))
        }
        None => {
            let header_pairs = options.iter().filter_map(|(k, v)| {
                k.strip_prefix("header.")
                    .map(|name| (name.to_string(), v.clone()))
            });
            Arc::new(StaticHeaderProvider::from_pairs(header_pairs)?)
        }
    };

    let tls = RestClientOptions {
        cert_path: options.get("tls.cert_path").cloned(),
        key_path: options.get("tls.key_path").cloned(),
        ca_path: options.get("tls.ca_path").cloned(),
        dns_overrides: options
            .get("tls.dns_override")
            .map(|s| s.split(',').map(str::to_string).collect())
            .unwrap_or_default(),
        timeout_secs: options
            .get("tls.timeout_secs")
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|e| Error::generic(format!("invalid tls.timeout_secs `{s}`: {e}")))
            })
            .transpose()?,
    };
    let client = build_rest_client(&tls)?;

    let max_retries = options
        .get("retry.max_retries")
        .map(|s| {
            s.parse::<u32>()
                .map_err(|e| Error::generic(format!("invalid retry.max_retries `{s}`: {e}")))
        })
        .transpose()?
        .unwrap_or(0);
    let verify = parse_bool_option(
        "put.verify_on_ambiguous",
        options.get("put.verify_on_ambiguous"),
    )?;

    Ok(Arc::new(
        RestObjectStore::new(base_url.to_string(), client, auth, Arc::new(config))
            .with_max_retries(max_retries)
            .with_verify_on_ambiguous(verify),
    ))
}

fn require_option(options: &HashMap<String, String>, key: &str) -> DeltaResult<String> {
    options
        .get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| Error::generic(format!("missing required REST engine option `{key}`")))
}

fn optional_prefix(options: &HashMap<String, String>, key: &str) -> String {
    options.get(key).cloned().unwrap_or_default()
}

fn rest_endpoint_config_from_options(
    options: &HashMap<String, String>,
) -> DeltaResult<RestEndpointConfig> {
    Ok(RestEndpointConfig {
        files_prefix: optional_prefix(options, "files_prefix"),
        directories_prefix: optional_prefix(options, "directories_prefix"),
        page_token_param: require_option(options, "page_token_param")?,
        start_from_param: require_option(options, "start_from_param")?,
        recursive_param: require_option(options, "recursive_param")?,
        overwrite_param: require_option(options, "overwrite_param")?,
        contents_field: require_option(options, "contents_field")?,
        next_page_token_field: require_option(options, "next_page_token_field")?,
        entry_path_field: require_option(options, "entry_path_field")?,
        entry_size_field: require_option(options, "entry_size_field")?,
        entry_is_directory_field: require_option(options, "entry_is_directory_field")?,
        entry_last_modified_field: require_option(options, "entry_last_modified_field")?,
        entry_strip_prefix: options.get("entry_strip_prefix").cloned(),
    })
}

fn parse_bool_option(key: &str, value: Option<&String>) -> DeltaResult<bool> {
    match value {
        None => Ok(false),
        Some(v) => match v.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            other => Err(Error::generic(format!(
                "invalid {key} `{other}`: expected `true` or `false`"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    use crate::kernel_string_slice;

    fn test_base_url() -> Url {
        Url::parse("http://localhost/").unwrap()
    }

    fn test_dialect_options() -> HashMap<String, String> {
        [
            ("page_token_param", "page_token"),
            ("start_from_param", "start_from"),
            ("recursive_param", "recursive"),
            ("overwrite_param", "overwrite"),
            ("contents_field", "contents"),
            ("next_page_token_field", "next_page_token"),
            ("entry_path_field", "path"),
            ("entry_size_field", "size"),
            ("entry_is_directory_field", "is_directory"),
            ("entry_last_modified_field", "last_modified"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    static EMPTY: &str = "";

    fn test_auth_headers(pairs: &[(&str, &str)]) -> CAuthHeaders {
        let mut headers = CAuthHeaders {
            count: pairs.len() as u32,
            headers: std::array::from_fn(|_| CAuthHeaderPair {
                name: kernel_string_slice!(EMPTY),
                value: kernel_string_slice!(EMPTY),
            }),
            ttl_ms: 0,
        };
        for (i, (name, value)) in pairs.iter().enumerate() {
            headers.headers[i] = CAuthHeaderPair {
                name: kernel_string_slice!(name),
                value: kernel_string_slice!(value),
            };
        }
        headers
    }

    #[test]
    fn reject_legacy_store_backend_option_errors() {
        let mut options = HashMap::new();
        options.insert(STORE_BACKEND_KEY.into(), "rest".into());
        assert!(reject_legacy_store_backend_option(&options).is_err());
    }

    #[test]
    fn auth_pairs_from_c_copies_slices() {
        let headers = test_auth_headers(&[
            ("Authorization", "Bearer t"),
            ("X-Databricks-Org-Id", "123"),
        ]);
        let pairs = auth_pairs_from_c(&headers).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("Authorization".to_string(), "Bearer t".to_string()),
                ("X-Databricks-Org-Id".to_string(), "123".to_string()),
            ]
        );
    }

    #[test]
    fn auth_pairs_from_c_rejects_excess_count() {
        let mut headers = test_auth_headers(&[]);
        headers.count = (AUTH_MAX_HEADERS + 1) as u32;
        assert!(auth_pairs_from_c(&headers).is_err());
    }

    extern "C" fn fill_auth_direct(_: NullableCvoid, out: *mut CAuthHeaders) {
        let mut headers = test_auth_headers(&[("authorization", "Bearer t")]);
        headers.ttl_ms = 60_000;
        unsafe { *out = headers };
    }

    #[test]
    fn auth_callback_collects_direct_fill_and_ttl() {
        let provider = FfiAuthHeaderProvider {
            callback: fill_auth_direct,
            context: None,
        };
        let (pairs, ttl_ms) = provider.collect().unwrap();
        assert_eq!(
            pairs,
            vec![("authorization".to_string(), "Bearer t".to_string())]
        );
        assert_eq!(ttl_ms, Some(60_000));
    }

    #[test]
    fn build_succeeds_with_refreshing_auth_callback() {
        let provider = FfiAuthHeaderProvider::new(fill_auth_direct, None);
        assert!(
            build_rest_object_store(&test_base_url(), &test_dialect_options(), Some(provider))
                .is_ok()
        );
    }

    #[test]
    fn build_succeeds_with_header_options_fallback() {
        let mut options = test_dialect_options();
        options.insert("header.Authorization".into(), "Bearer x".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_ok());
    }

    #[test]
    fn build_succeeds_with_defaults_and_options() {
        let mut options = test_dialect_options();
        options.insert("header.Authorization".into(), "Bearer x".into());
        options.insert("files_prefix".into(), "/TablesById/u".into());
        options.insert("entry_strip_prefix".into(), "/TablesById/u".into());
        options.insert("retry.max_retries".into(), "3".into());
        options.insert("put.verify_on_ambiguous".into(), "true".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_ok());
    }

    #[test]
    fn build_rejects_missing_dialect_option() {
        assert!(build_rest_object_store(&test_base_url(), &HashMap::new(), None).is_err());
    }

    #[test]
    fn build_rejects_invalid_timeout() {
        let mut options = test_dialect_options();
        options.insert("tls.timeout_secs".into(), "nope".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_err());
    }

    #[test]
    fn build_rejects_invalid_max_retries() {
        let mut options = test_dialect_options();
        options.insert("retry.max_retries".into(), "nope".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_err());
    }

    #[test]
    fn build_rejects_invalid_verify_on_ambiguous() {
        let mut options = test_dialect_options();
        options.insert("put.verify_on_ambiguous".into(), "nope".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_err());
    }

    #[test]
    fn build_rejects_invalid_header_value() {
        let mut options = test_dialect_options();
        options.insert("header.X".into(), "bad\nvalue".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_err());
    }

    #[test]
    fn build_rejects_partial_mtls() {
        let mut options = test_dialect_options();
        options.insert("tls.cert_path".into(), "/x/cert.pem".into());
        assert!(build_rest_object_store(&test_base_url(), &options, None).is_err());
    }
}
