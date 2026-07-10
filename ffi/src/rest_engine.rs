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

use delta_kernel::object_store::ObjectStore;
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

/// Output buffer for a [`CAuthHeaderCallback`] invocation.
///
/// Opaque to callers: populate it by calling [`rest_engine_emit_auth_header`] and
/// [`rest_engine_emit_auth_ttl`] with the `out` pointer your callback receives. Do not read fields
/// or keep the pointer after the callback returns.
#[repr(C)]
pub struct CAuthHeaderCollector {
    // Not ZST: cbindgen emits `[u8; 0]` as a zero-length array, which ISO C rejects under
    // -Wpedantic.
    _private: u8,
}

/// Supplies auth headers for a REST-backed engine.
///
/// The kernel invokes this when it needs fresh headers. The implementation writes into `out` by
/// calling [`rest_engine_emit_auth_header`] once per header and may call
/// [`rest_engine_emit_auth_ttl`] once to report how long those headers stay valid. With a TTL the
/// kernel caches them and re-invokes near expiry; without one it invokes the callback on every
/// request.
///
/// `context` is the opaque pointer registered via
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store). Both pointers are only
/// valid for the duration of the call and must not be retained.
///
/// Named emit exports (rather than function-pointer arguments) allow FFI runtimes that cannot
/// invoke arbitrary function pointers (e.g. JNR-FFI) to emit headers via bound symbols.
pub type CAuthHeaderCallback =
    extern "C" fn(context: NullableCvoid, out: *mut CAuthHeaderCollector);

/// Kernel-side storage for what a [`CAuthHeaderCallback`] emits in one invocation.
#[derive(Default)]
struct AuthHeaderEmission {
    headers: Vec<(String, String)>,
    ttl_ms: Option<u64>,
}

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

    fn collect(&self) -> AuthHeaderEmission {
        let mut emission = AuthHeaderEmission::default();
        (self.callback)(
            self.context,
            &mut emission as *mut AuthHeaderEmission as *mut CAuthHeaderCollector,
        );
        emission
    }
}

fn auth_header_emission_from_out(out: *mut CAuthHeaderCollector) -> *mut AuthHeaderEmission {
    out.cast()
}

/// Emit one `(name, value)` header during a [`CAuthHeaderCallback`] invocation.
///
/// Pass the `out` pointer your callback received from the kernel. `name` and `value` are borrowed
/// for this call; the kernel copies them. Non-UTF-8 names or values are skipped.
///
/// # Safety
/// `out` must be the collector pointer passed to the current [`CAuthHeaderCallback`] invocation.
/// `name` and `value` must remain valid for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn rest_engine_emit_auth_header(
    out: *mut CAuthHeaderCollector,
    name: KernelStringSlice,
    value: KernelStringSlice,
) {
    // SAFETY: `out` is the `&mut AuthHeaderEmission` from `collect`.
    let emission = unsafe { &mut *auth_header_emission_from_out(out) };
    let name = unsafe { String::try_from_slice(&name) };
    let value = unsafe { String::try_from_slice(&value) };
    if let (Ok(name), Ok(value)) = (name, value) {
        emission.headers.push((name, value));
    }
}

/// Report how long (in milliseconds) the headers emitted in this [`CAuthHeaderCallback`] invocation
/// stay valid.
///
/// # Safety
/// `out` must be the collector pointer passed to the current [`CAuthHeaderCallback`] invocation.
#[no_mangle]
pub unsafe extern "C" fn rest_engine_emit_auth_ttl(out: *mut CAuthHeaderCollector, ttl_ms: u64) {
    // SAFETY: `out` is the `&mut AuthHeaderEmission` from `collect`.
    let emission = unsafe { &mut *auth_header_emission_from_out(out) };
    emission.ttl_ms = Some(ttl_ms);
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
        Some(cb) => Arc::new(RefreshingHeaderProvider::new(move || {
            let emitted = cb.collect();
            Ok((
                headers_from_pairs(emitted.headers)?,
                emitted.ttl_ms.map(Duration::from_millis),
            ))
        })),
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

    #[test]
    fn reject_legacy_store_backend_option_errors() {
        let mut options = HashMap::new();
        options.insert(STORE_BACKEND_KEY.into(), "rest".into());
        assert!(reject_legacy_store_backend_option(&options).is_err());
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

    extern "C" fn emit_header_and_ttl(_context: NullableCvoid, out: *mut CAuthHeaderCollector) {
        let name = "authorization";
        let value = "Bearer t";
        unsafe {
            rest_engine_emit_auth_header(
                out,
                kernel_string_slice!(name),
                kernel_string_slice!(value),
            );
            rest_engine_emit_auth_ttl(out, 60_000);
        };
    }

    #[test]
    fn auth_callback_collects_emitted_headers_and_ttl() {
        let ctx = FfiAuthHeaderProvider {
            callback: emit_header_and_ttl,
            context: None,
        };
        let emitted = ctx.collect();
        assert_eq!(
            emitted.headers,
            vec![("authorization".to_string(), "Bearer t".to_string())]
        );
        assert_eq!(emitted.ttl_ms, Some(60_000));
    }
}
