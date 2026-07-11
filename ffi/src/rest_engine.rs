//! REST [`RestObjectStore`] wiring for [`EngineBuilder`](crate::EngineBuilder).
//!
//! Call [`set_builder_rest_object_store`] with a [`CRestEndpointConfig`] to select the REST
//! backend. The builder `url` must be the REST service base URL, not a Delta table path.
//!
//! TLS, retries, and static auth use [`set_builder_option`](crate::set_builder_option) with the
//! `REST_BUILDER_OPTION_*` keys below. Pass a [`CAuthHeaderCallback`] when headers expire; with
//! `callback = NULL`, set static headers via `header.<Name>` options instead.
//!
//! When `callback` is non-null, `context` must remain valid for the engine lifetime and be safe
//! to invoke from any thread.

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

use crate::{KernelStringSlice, NullableCvoid, OptionalValue, TryFromStringSlice};

/// Max `(name, value)` pairs in a [`CAuthHeaders`] struct.
pub const AUTH_MAX_HEADERS: usize = 8;

// === `set_builder_option` keys for REST engines ===
//
// Each `REST_BUILDER_OPTION_*` is the Rust `&str` key. The matching `*_KEY` static is a
// null-terminated byte array exported to C via cbindgen (`extern const uint8_t ...`).

/// Prefix for static auth header options: `header.<Name>` (append the HTTP header name).
///
/// Example: `header.Authorization` with value `Bearer <token>`. Ignored when a
/// [`CAuthHeaderCallback`] is passed to
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
pub const REST_BUILDER_OPTION_HEADER_PREFIX: &str = "header.";
#[no_mangle]
pub static REST_BUILDER_OPTION_HEADER_PREFIX_KEY: [u8; 8] = *b"header.\0";

/// PEM client-certificate path. mTLS is enabled only when cert, key, and CA paths are all set.
pub const REST_BUILDER_OPTION_TLS_CERT_PATH: &str = "tls.cert_path";
#[no_mangle]
pub static REST_BUILDER_OPTION_TLS_CERT_PATH_KEY: [u8; 14] = *b"tls.cert_path\0";

/// PEM client-private-key path (required with [`REST_BUILDER_OPTION_TLS_CERT_PATH`] and
/// [`REST_BUILDER_OPTION_TLS_CA_PATH`] for mTLS).
pub const REST_BUILDER_OPTION_TLS_KEY_PATH: &str = "tls.key_path";
#[no_mangle]
pub static REST_BUILDER_OPTION_TLS_KEY_PATH_KEY: [u8; 13] = *b"tls.key_path\0";

/// PEM CA-bundle path used to verify the server (required with cert and key for mTLS).
pub const REST_BUILDER_OPTION_TLS_CA_PATH: &str = "tls.ca_path";
#[no_mangle]
pub static REST_BUILDER_OPTION_TLS_CA_PATH_KEY: [u8; 12] = *b"tls.ca_path\0";

/// Comma-separated DNS overrides as `host=ip:port` entries (bypasses system resolver; SNI
/// unchanged).
pub const REST_BUILDER_OPTION_TLS_DNS_OVERRIDE: &str = "tls.dns_override";
#[no_mangle]
pub static REST_BUILDER_OPTION_TLS_DNS_OVERRIDE_KEY: [u8; 17] = *b"tls.dns_override\0";

/// HTTP request timeout in seconds; unset uses the reqwest default.
pub const REST_BUILDER_OPTION_TLS_TIMEOUT_SECS: &str = "tls.timeout_secs";
#[no_mangle]
pub static REST_BUILDER_OPTION_TLS_TIMEOUT_SECS_KEY: [u8; 17] = *b"tls.timeout_secs\0";

/// Maximum automatic retries for ambiguous REST failures; defaults to `0` when unset.
pub const REST_BUILDER_OPTION_RETRY_MAX_RETRIES: &str = "retry.max_retries";
#[no_mangle]
pub static REST_BUILDER_OPTION_RETRY_MAX_RETRIES_KEY: [u8; 18] = *b"retry.max_retries\0";

/// After an ambiguous PUT, verify object existence before treating the write as successful.
/// Value: `true` or `false`; defaults to `false` when unset.
pub const REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS: &str = "put.verify_on_ambiguous";
#[no_mangle]
pub static REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS_KEY: [u8; 24] =
    *b"put.verify_on_ambiguous\0";

/// REST file API dialect passed to
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
///
/// Each [`KernelStringSlice`] must remain valid for the duration of that call; the kernel copies
/// the strings into the built engine. Prefix fields may be empty. `entry_strip_prefix` uses
/// [`OptionalValue::None`] when unset.
#[repr(C)]
pub struct CRestEndpointConfig {
    pub files_prefix: KernelStringSlice,
    pub directories_prefix: KernelStringSlice,
    pub page_token_param: KernelStringSlice,
    pub start_from_param: KernelStringSlice,
    pub recursive_param: KernelStringSlice,
    pub overwrite_param: KernelStringSlice,
    pub contents_field: KernelStringSlice,
    pub next_page_token_field: KernelStringSlice,
    pub entry_path_field: KernelStringSlice,
    pub entry_size_field: KernelStringSlice,
    pub entry_is_directory_field: KernelStringSlice,
    pub entry_last_modified_field: KernelStringSlice,
    pub entry_strip_prefix: OptionalValue<KernelStringSlice>,
}

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

/// State for [`crate::ObjectStoreBackend::Rest`], stored on [`EngineBuilder`](crate::EngineBuilder)
/// after [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
pub(crate) struct RestBuilderState {
    endpoint_config: RestEndpointConfig,
    auth_callback: Option<FfiAuthHeaderProvider>,
}

/// Upcalls a [`CAuthHeaderCallback`] whenever the REST client needs fresh auth headers.
///
/// Registered via [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
#[derive(Clone, Copy)]
pub(crate) struct FfiAuthHeaderProvider {
    callback: CAuthHeaderCallback,
    context: NullableCvoid,
}
// SAFETY: see [`set_builder_rest_object_store`]: `context` and `callback` must be safe to invoke
// from any thread concurrently.
unsafe impl Send for FfiAuthHeaderProvider {}
unsafe impl Sync for FfiAuthHeaderProvider {}

type AuthHeaderPairs = Vec<(String, String)>;
type CollectedAuthHeaders = (AuthHeaderPairs, Option<u64>);

impl FfiAuthHeaderProvider {
    pub(crate) fn new(callback: CAuthHeaderCallback, context: NullableCvoid) -> Self {
        Self { callback, context }
    }

    fn collect(&self) -> DeltaResult<CollectedAuthHeaders> {
        let mut headers = empty_c_auth_headers();
        (self.callback)(self.context, &mut headers);
        Ok((auth_pairs_from_c(&headers)?, ttl_ms_from_c(headers.ttl_ms)))
    }
}

/// Build REST builder state from FFI endpoint config and optional auth callback.
pub(crate) fn rest_builder_state_from_ffi(
    endpoint_config: &CRestEndpointConfig,
    callback: Option<CAuthHeaderCallback>,
    context: NullableCvoid,
) -> DeltaResult<RestBuilderState> {
    Ok(RestBuilderState {
        endpoint_config: rest_endpoint_config_from_c(endpoint_config)?,
        auth_callback: callback.map(|cb| FfiAuthHeaderProvider::new(cb, context)),
    })
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

/// Copy a [`CRestEndpointConfig`] into an owned [`RestEndpointConfig`].
pub(crate) fn rest_endpoint_config_from_c(
    config: &CRestEndpointConfig,
) -> DeltaResult<RestEndpointConfig> {
    Ok(RestEndpointConfig {
        files_prefix: copy_optional_string(&config.files_prefix)?,
        directories_prefix: copy_optional_string(&config.directories_prefix)?,
        page_token_param: copy_required_string(&config.page_token_param, "page_token_param")?,
        start_from_param: copy_required_string(&config.start_from_param, "start_from_param")?,
        recursive_param: copy_required_string(&config.recursive_param, "recursive_param")?,
        overwrite_param: copy_required_string(&config.overwrite_param, "overwrite_param")?,
        contents_field: copy_required_string(&config.contents_field, "contents_field")?,
        next_page_token_field: copy_required_string(
            &config.next_page_token_field,
            "next_page_token_field",
        )?,
        entry_path_field: copy_required_string(&config.entry_path_field, "entry_path_field")?,
        entry_size_field: copy_required_string(&config.entry_size_field, "entry_size_field")?,
        entry_is_directory_field: copy_required_string(
            &config.entry_is_directory_field,
            "entry_is_directory_field",
        )?,
        entry_last_modified_field: copy_required_string(
            &config.entry_last_modified_field,
            "entry_last_modified_field",
        )?,
        entry_strip_prefix: copy_optional_entry_strip_prefix(&config.entry_strip_prefix)?,
    })
}

fn copy_optional_string(slice: &KernelStringSlice) -> DeltaResult<String> {
    // SAFETY: caller keeps slice memory valid until `set_builder_rest_object_store` returns.
    unsafe { String::try_from_slice(slice) }
}

fn copy_required_string(slice: &KernelStringSlice, field: &str) -> DeltaResult<String> {
    let value = copy_optional_string(slice)?;
    if value.is_empty() {
        return Err(Error::generic(format!("`{field}` must be non-empty")));
    }
    Ok(value)
}

fn copy_optional_entry_strip_prefix(
    prefix: &OptionalValue<KernelStringSlice>,
) -> DeltaResult<Option<String>> {
    match prefix {
        OptionalValue::None => Ok(None),
        OptionalValue::Some(slice) => {
            let value = copy_optional_string(slice)?;
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
    }
}

/// Build a [`RestObjectStore`] from an engine builder's URL, options, and REST state.
pub(crate) fn build_rest_object_store(
    base_url: &Url,
    options: &HashMap<String, String>,
    rest: &RestBuilderState,
) -> DeltaResult<Arc<dyn ObjectStore>> {
    let config = rest.endpoint_config.clone();

    let auth: Arc<dyn AuthHeaderProvider> = match rest.auth_callback {
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
                k.strip_prefix(REST_BUILDER_OPTION_HEADER_PREFIX)
                    .map(|name| (name.to_string(), v.clone()))
            });
            Arc::new(StaticHeaderProvider::from_pairs(header_pairs)?)
        }
    };

    let tls = RestClientOptions {
        cert_path: options.get(REST_BUILDER_OPTION_TLS_CERT_PATH).cloned(),
        key_path: options.get(REST_BUILDER_OPTION_TLS_KEY_PATH).cloned(),
        ca_path: options.get(REST_BUILDER_OPTION_TLS_CA_PATH).cloned(),
        dns_overrides: options
            .get(REST_BUILDER_OPTION_TLS_DNS_OVERRIDE)
            .map(|s| s.split(',').map(str::to_string).collect())
            .unwrap_or_default(),
        timeout_secs: options
            .get(REST_BUILDER_OPTION_TLS_TIMEOUT_SECS)
            .map(|s| {
                s.parse::<u64>().map_err(|e| {
                    Error::generic(format!(
                        "invalid {} `{s}`: {e}",
                        REST_BUILDER_OPTION_TLS_TIMEOUT_SECS
                    ))
                })
            })
            .transpose()?,
    };
    let client = build_rest_client(&tls)?;

    let max_retries = options
        .get(REST_BUILDER_OPTION_RETRY_MAX_RETRIES)
        .map(|s| {
            s.parse::<u32>().map_err(|e| {
                Error::generic(format!(
                    "invalid {} `{s}`: {e}",
                    REST_BUILDER_OPTION_RETRY_MAX_RETRIES
                ))
            })
        })
        .transpose()?
        .unwrap_or(0);
    let verify = parse_bool_option(
        REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS,
        options.get(REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS),
    )?;

    Ok(Arc::new(
        RestObjectStore::new(base_url.to_string(), client, auth, Arc::new(config))
            .with_max_retries(max_retries)
            .with_verify_on_ambiguous(verify),
    ))
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

    fn option_key_bytes_match_const(key: &[u8], expected: &str) {
        let without_nul = &key[..key.len() - 1];
        assert_eq!(
            std::str::from_utf8(without_nul).expect("option key bytes should be valid UTF-8"),
            expected
        );
    }

    fn test_base_url() -> Url {
        Url::parse("http://localhost/").unwrap()
    }

    static EMPTY: &str = "";

    fn test_c_rest_endpoint_config() -> CRestEndpointConfig {
        let page_token = "page_token";
        let start_from = "start_from";
        let recursive = "recursive";
        let overwrite = "overwrite";
        let contents = "contents";
        let next_page_token = "next_page_token";
        let path = "path";
        let size = "size";
        let is_directory = "is_directory";
        let last_modified = "last_modified";
        CRestEndpointConfig {
            files_prefix: kernel_string_slice!(EMPTY),
            directories_prefix: kernel_string_slice!(EMPTY),
            page_token_param: kernel_string_slice!(page_token),
            start_from_param: kernel_string_slice!(start_from),
            recursive_param: kernel_string_slice!(recursive),
            overwrite_param: kernel_string_slice!(overwrite),
            contents_field: kernel_string_slice!(contents),
            next_page_token_field: kernel_string_slice!(next_page_token),
            entry_path_field: kernel_string_slice!(path),
            entry_size_field: kernel_string_slice!(size),
            entry_is_directory_field: kernel_string_slice!(is_directory),
            entry_last_modified_field: kernel_string_slice!(last_modified),
            entry_strip_prefix: OptionalValue::None,
        }
    }

    fn test_rest_builder_state() -> RestBuilderState {
        rest_builder_state_from_ffi(&test_c_rest_endpoint_config(), None, None).unwrap()
    }

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
    fn rest_builder_option_keys_match_c_exports() {
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_HEADER_PREFIX_KEY,
            REST_BUILDER_OPTION_HEADER_PREFIX,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_TLS_CERT_PATH_KEY,
            REST_BUILDER_OPTION_TLS_CERT_PATH,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_TLS_KEY_PATH_KEY,
            REST_BUILDER_OPTION_TLS_KEY_PATH,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_TLS_CA_PATH_KEY,
            REST_BUILDER_OPTION_TLS_CA_PATH,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_TLS_DNS_OVERRIDE_KEY,
            REST_BUILDER_OPTION_TLS_DNS_OVERRIDE,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_TLS_TIMEOUT_SECS_KEY,
            REST_BUILDER_OPTION_TLS_TIMEOUT_SECS,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_RETRY_MAX_RETRIES_KEY,
            REST_BUILDER_OPTION_RETRY_MAX_RETRIES,
        );
        option_key_bytes_match_const(
            &REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS_KEY,
            REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS,
        );
    }

    #[test]
    fn rest_endpoint_config_from_c_copies_fields() {
        let config = rest_endpoint_config_from_c(&test_c_rest_endpoint_config()).unwrap();
        assert_eq!(config.page_token_param, "page_token");
        assert_eq!(config.entry_path_field, "path");
        assert!(config.entry_strip_prefix.is_none());
    }

    #[test]
    fn rest_endpoint_config_from_c_rejects_empty_required_field() {
        let mut config = test_c_rest_endpoint_config();
        config.page_token_param = kernel_string_slice!(EMPTY);
        assert!(rest_endpoint_config_from_c(&config).is_err());
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
        let rest = rest_builder_state_from_ffi(
            &test_c_rest_endpoint_config(),
            Some(fill_auth_direct),
            None,
        )
        .unwrap();
        assert!(build_rest_object_store(&test_base_url(), &HashMap::new(), &rest).is_ok());
    }

    #[test]
    fn build_succeeds_with_header_options_fallback() {
        let mut options = HashMap::new();
        options.insert(
            format!("{}Authorization", REST_BUILDER_OPTION_HEADER_PREFIX),
            "Bearer x".into(),
        );
        assert!(
            build_rest_object_store(&test_base_url(), &options, &test_rest_builder_state()).is_ok()
        );
    }

    #[test]
    fn build_succeeds_with_prefixes_and_resilience_options() {
        let prefix = "/TablesById/u";
        let mut config = test_c_rest_endpoint_config();
        config.files_prefix = kernel_string_slice!(prefix);
        config.entry_strip_prefix = OptionalValue::Some(kernel_string_slice!(prefix));
        let rest = rest_builder_state_from_ffi(&config, None, None).unwrap();

        let mut options = HashMap::new();
        options.insert(
            format!("{}Authorization", REST_BUILDER_OPTION_HEADER_PREFIX),
            "Bearer x".into(),
        );
        options.insert(REST_BUILDER_OPTION_RETRY_MAX_RETRIES.into(), "3".into());
        options.insert(
            REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS.into(),
            "true".into(),
        );
        assert!(build_rest_object_store(&test_base_url(), &options, &rest).is_ok());
    }

    #[test]
    fn build_rejects_invalid_timeout() {
        let mut options = HashMap::new();
        options.insert(REST_BUILDER_OPTION_TLS_TIMEOUT_SECS.into(), "nope".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &test_rest_builder_state())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_invalid_max_retries() {
        let mut options = HashMap::new();
        options.insert(REST_BUILDER_OPTION_RETRY_MAX_RETRIES.into(), "nope".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &test_rest_builder_state())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_invalid_verify_on_ambiguous() {
        let mut options = HashMap::new();
        options.insert(
            REST_BUILDER_OPTION_PUT_VERIFY_ON_AMBIGUOUS.into(),
            "nope".into(),
        );
        assert!(
            build_rest_object_store(&test_base_url(), &options, &test_rest_builder_state())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_invalid_header_value() {
        let mut options = HashMap::new();
        options.insert(
            format!("{}X", REST_BUILDER_OPTION_HEADER_PREFIX),
            "bad\nvalue".into(),
        );
        assert!(
            build_rest_object_store(&test_base_url(), &options, &test_rest_builder_state())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_partial_mtls() {
        let mut options = HashMap::new();
        options.insert(
            REST_BUILDER_OPTION_TLS_CERT_PATH.into(),
            "/x/cert.pem".into(),
        );
        assert!(
            build_rest_object_store(&test_base_url(), &options, &test_rest_builder_state())
                .is_err()
        );
    }
}
