//! FFI for a default engine backed by a generic REST file API ([`RestObjectStore`]).
//!
//! Mirrors the object-store engine builder (`get_engine_builder` / `set_builder_option` /
//! `builder_build`): create a builder for a base URL, set string options, optionally register a
//! per-request auth callback, then build. Options describe the REST endpoint config (the
//! [`RestEndpointConfig`] field names), request headers (`header.<Name>`), TLS material
//! (`tls.cert_path` / `tls.key_path` / `tls.ca_path` / `tls.dns_override` / `tls.timeout_secs`),
//! and resilience (`retry.max_retries`, `put.verify_on_ambiguous`), so a backend is configured
//! entirely from the caller -- no per-backend Rust.

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;
use std::time::Duration;

use delta_kernel::{DeltaResult, Error};
use delta_kernel_default_engine::rest_store::{
    build_rest_client, headers_from_pairs, AuthHeaderProvider, RefreshingHeaderProvider,
    RestClientOptions, RestEndpointConfig, RestObjectStore, StaticHeaderProvider,
};

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    build_engine_from_store, IoConcurrencyConfig, KernelStringSlice, MultithreadedExecutorConfig,
    SharedExternEngine, TryFromStringSlice,
};

/// Supplies the current auth/identity headers for a REST engine. The implementation calls the
/// named [`rest_engine_emit_auth_header`] export once per header it wants attached, and may call
/// [`rest_engine_emit_auth_ttl`] once to report how long those headers stay valid -- with a TTL
/// the kernel caches them and only re-invokes the callback near expiry; without one it invokes the
/// callback every request. Both take back the `emit_state` passed in. `context` is the opaque
/// pointer registered via [`set_rest_engine_builder_auth_callback`].
///
/// The callback must emit synchronously, on the calling thread, before it returns; `emit_state`
/// must not be retained past the call.
///
/// A named emit export (rather than an emit function pointer argument) is used so callers on FFI
/// runtimes that cannot invoke an arbitrary function pointer (e.g. JNR-FFI) can still emit headers
/// by calling a bound symbol.
pub type AuthHeaderCallback = extern "C" fn(context: *mut c_void, emit_state: *mut c_void);

/// What an [`AuthHeaderCallback`] emits in one invocation: the header pairs plus an optional TTL.
#[derive(Default)]
struct AuthCollector {
    headers: Vec<(String, String)>,
    ttl_ms: Option<u64>,
}

/// Pairs an [`AuthHeaderCallback`] with its opaque `context` and marks the pair `Send + Sync` so
/// the per-request closure can be shared across threads.
///
/// Safety: the FFI caller guarantees `callback` and `context` are safe to invoke from multiple
/// threads concurrently -- the kernel consults the header provider from async request tasks that
/// may run on any worker thread.
#[derive(Clone, Copy)]
struct AuthCallbackContext {
    callback: AuthHeaderCallback,
    context: *mut c_void,
}
unsafe impl Send for AuthCallbackContext {}
unsafe impl Sync for AuthCallbackContext {}

impl AuthCallbackContext {
    /// Invoke the callback and return what it emitted. Taking `&self` makes the enclosing closure
    /// capture the whole (`Send + Sync`) struct rather than the bare `*mut c_void` field (Rust 2021
    /// disjoint closure capture would otherwise grab the non-`Send` field).
    fn collect(&self) -> AuthCollector {
        let mut collected = AuthCollector::default();
        (self.callback)(self.context, &mut collected as *mut _ as *mut c_void);
        collected
    }
}

/// Emit one `(name, value)` header into the kernel's collector during an [`AuthHeaderCallback`]
/// invocation. `emit_state` is the opaque pointer the callback received; `name`/`value` are
/// borrowed for the call (the kernel copies them). Non-UTF-8 names/values are skipped.
///
/// # Safety
/// `emit_state` must be the pointer the kernel passed to the active [`AuthHeaderCallback`], and the
/// `name`/`value` slices must be valid for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn rest_engine_emit_auth_header(
    emit_state: *mut c_void,
    name: KernelStringSlice,
    value: KernelStringSlice,
) {
    // SAFETY: `emit_state` is the `&mut AuthCollector` from `collect`.
    let collected = unsafe { &mut *(emit_state as *mut AuthCollector) };
    let name = unsafe { String::try_from_slice(&name) };
    let value = unsafe { String::try_from_slice(&value) };
    if let (Ok(name), Ok(value)) = (name, value) {
        collected.headers.push((name, value));
    }
}

/// Report how long (in milliseconds) the headers emitted in this [`AuthHeaderCallback`] invocation
/// stay valid. The kernel caches them for that long and re-invokes the callback near expiry;
/// callbacks that omit this are consulted on every request.
///
/// # Safety
/// `emit_state` must be the pointer the kernel passed to the active [`AuthHeaderCallback`].
#[no_mangle]
pub unsafe extern "C" fn rest_engine_emit_auth_ttl(emit_state: *mut c_void, ttl_ms: u64) {
    // SAFETY: `emit_state` is the `&mut AuthCollector` from `collect`.
    let collected = unsafe { &mut *(emit_state as *mut AuthCollector) };
    collected.ttl_ms = Some(ttl_ms);
}

/// Builder for a REST-backed default engine. Created by [`get_rest_engine_builder`], configured
/// via [`set_rest_engine_builder_option`], and consumed by [`rest_engine_builder_build`] (or
/// discarded with [`free_rest_engine_builder`]).
pub struct RestEngineBuilder {
    base_url: String,
    allocate_fn: AllocateErrorFn,
    options: HashMap<String, String>,
    auth_callback: Option<AuthCallbackContext>,
    multithreaded_executor_config: Option<MultithreadedExecutorConfig>,
    io_config: IoConcurrencyConfig,
}

impl RestEngineBuilder {
    fn require_option(&self, key: &str) -> DeltaResult<String> {
        self.options
            .get(key)
            .filter(|v| !v.is_empty())
            .cloned()
            .ok_or_else(|| Error::generic(format!("missing required REST engine option `{key}`")))
    }

    fn optional_prefix(&self, key: &str) -> String {
        self.options.get(key).cloned().unwrap_or_default()
    }
}

/// Get a builder for a REST-backed engine targeting `base_url`.
///
/// # Safety
/// Caller must pass a valid string slice for `base_url`.
#[no_mangle]
pub unsafe extern "C" fn get_rest_engine_builder(
    base_url: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*mut RestEngineBuilder> {
    get_rest_engine_builder_impl(base_url, allocate_error).into_extern_result(&allocate_error)
}

fn get_rest_engine_builder_impl(
    base_url: KernelStringSlice,
    allocate_fn: AllocateErrorFn,
) -> DeltaResult<*mut RestEngineBuilder> {
    let base_url = unsafe { String::try_from_slice(&base_url) }?;
    Ok(Box::into_raw(Box::new(RestEngineBuilder {
        base_url,
        allocate_fn,
        options: HashMap::new(),
        auth_callback: None,
        multithreaded_executor_config: None,
        io_config: IoConcurrencyConfig::default(),
    })))
}

/// Set a string option on the builder. See the [module docs](self) for recognized keys.
///
/// # Safety
/// Caller must pass a valid builder pointer and valid slices for `key` and `value`.
#[no_mangle]
pub unsafe extern "C" fn set_rest_engine_builder_option(
    builder: &mut RestEngineBuilder,
    key: KernelStringSlice,
    value: KernelStringSlice,
) -> ExternResult<bool> {
    set_rest_engine_builder_option_impl(builder, key, value)
        .into_extern_result(&builder.allocate_fn)
}

fn set_rest_engine_builder_option_impl(
    builder: &mut RestEngineBuilder,
    key: KernelStringSlice,
    value: KernelStringSlice,
) -> DeltaResult<bool> {
    let key = unsafe { String::try_from_slice(&key) }?;
    let value = unsafe { String::try_from_slice(&value) }?;
    builder.options.insert(key, value);
    Ok(true)
}

/// Configure the builder to use a multi-threaded executor instead of the default
/// single-threaded background executor.
///
/// # Safety
/// Caller must pass a valid builder pointer.
#[no_mangle]
pub unsafe extern "C" fn set_rest_engine_builder_with_multithreaded_executor(
    builder: &mut RestEngineBuilder,
    worker_threads: usize,
    max_blocking_threads: usize,
) {
    let worker_threads = (worker_threads != 0).then_some(worker_threads);
    let max_blocking_threads = (max_blocking_threads != 0).then_some(max_blocking_threads);

    builder.multithreaded_executor_config = Some(MultithreadedExecutorConfig {
        worker_threads,
        max_blocking_threads,
    });
}

/// Configure read-path I/O concurrency for the engine's JSON and Parquet handlers.
///
/// # Safety
/// Caller must pass a valid builder pointer.
#[no_mangle]
pub unsafe extern "C" fn set_rest_engine_builder_with_io_concurrency(
    builder: &mut RestEngineBuilder,
    buffer_size: usize,
    batch_size: usize,
) {
    use std::num::NonZero;

    builder.io_config = IoConcurrencyConfig {
        buffer_size: NonZero::new(buffer_size),
        batch_size: NonZero::new(batch_size),
    };
}

/// Register an [`AuthHeaderCallback`] supplying the current headers (for short-lived, refreshable
/// credentials); with a reported TTL the kernel caches them and re-invokes the callback only near
/// expiry, otherwise it is consulted per request. When set, the callback supplies the full header
/// set and the static `header.<Name>` options are ignored. `context` is passed back on each
/// invocation and must remain valid until the engine produced by [`rest_engine_builder_build`] is
/// freed.
///
/// # Safety
/// Caller must pass a valid builder pointer; `callback` and `context` must stay valid for the
/// lifetime of the built engine.
#[no_mangle]
pub unsafe extern "C" fn set_rest_engine_builder_auth_callback(
    builder: &mut RestEngineBuilder,
    callback: AuthHeaderCallback,
    context: *mut c_void,
) {
    builder.auth_callback = Some(AuthCallbackContext { callback, context });
}

/// Consume the builder and return a REST-backed default engine. This frees the builder, so the
/// pointer must not be used again.
///
/// # Safety
/// Caller must pass a valid builder pointer and not use it again afterwards.
#[no_mangle]
pub unsafe extern "C" fn rest_engine_builder_build(
    builder: *mut RestEngineBuilder,
) -> ExternResult<Handle<SharedExternEngine>> {
    let builder = unsafe { Box::from_raw(builder) };
    rest_engine_builder_build_impl(&builder).into_extern_result(&builder.allocate_fn)
}

fn rest_endpoint_config_from_builder(
    builder: &RestEngineBuilder,
) -> DeltaResult<RestEndpointConfig> {
    Ok(RestEndpointConfig {
        files_prefix: builder.optional_prefix("files_prefix"),
        directories_prefix: builder.optional_prefix("directories_prefix"),
        page_token_param: builder.require_option("page_token_param")?,
        start_from_param: builder.require_option("start_from_param")?,
        recursive_param: builder.require_option("recursive_param")?,
        overwrite_param: builder.require_option("overwrite_param")?,
        contents_field: builder.require_option("contents_field")?,
        next_page_token_field: builder.require_option("next_page_token_field")?,
        entry_path_field: builder.require_option("entry_path_field")?,
        entry_size_field: builder.require_option("entry_size_field")?,
        entry_is_directory_field: builder.require_option("entry_is_directory_field")?,
        entry_last_modified_field: builder.require_option("entry_last_modified_field")?,
        entry_strip_prefix: builder.options.get("entry_strip_prefix").cloned(),
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

fn rest_engine_builder_build_impl(
    builder: &RestEngineBuilder,
) -> DeltaResult<Handle<SharedExternEngine>> {
    let config = rest_endpoint_config_from_builder(builder)?;

    // When an auth callback is registered, the kernel pulls headers from it (caching for the TTL
    // it reports, if any). Otherwise use the static `header.<Name>` options.
    let auth: Arc<dyn AuthHeaderProvider> = match &builder.auth_callback {
        Some(cb) => {
            let cb = *cb;
            Arc::new(RefreshingHeaderProvider::new(move || {
                let emitted = cb.collect();
                Ok((
                    headers_from_pairs(emitted.headers)?,
                    emitted.ttl_ms.map(Duration::from_millis),
                ))
            }))
        }
        None => {
            let header_pairs = builder.options.iter().filter_map(|(k, v)| {
                k.strip_prefix("header.")
                    .map(|name| (name.to_string(), v.clone()))
            });
            Arc::new(StaticHeaderProvider::from_pairs(header_pairs)?)
        }
    };

    let tls = RestClientOptions {
        cert_path: builder.options.get("tls.cert_path").cloned(),
        key_path: builder.options.get("tls.key_path").cloned(),
        ca_path: builder.options.get("tls.ca_path").cloned(),
        dns_overrides: builder
            .options
            .get("tls.dns_override")
            .map(|s| s.split(',').map(str::to_string).collect())
            .unwrap_or_default(),
        timeout_secs: builder
            .options
            .get("tls.timeout_secs")
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|e| Error::generic(format!("invalid tls.timeout_secs `{s}`: {e}")))
            })
            .transpose()?,
    };
    let client = build_rest_client(&tls)?;

    let max_retries = builder
        .options
        .get("retry.max_retries")
        .map(|s| {
            s.parse::<u32>()
                .map_err(|e| Error::generic(format!("invalid retry.max_retries `{s}`: {e}")))
        })
        .transpose()?
        .unwrap_or(0);
    let verify = parse_bool_option(
        "put.verify_on_ambiguous",
        builder.options.get("put.verify_on_ambiguous"),
    )?;

    let store = Arc::new(
        RestObjectStore::new(builder.base_url.clone(), client, auth, Arc::new(config))
            .with_max_retries(max_retries)
            .with_verify_on_ambiguous(verify),
    );
    build_engine_from_store(
        store,
        builder.multithreaded_executor_config.clone(),
        builder.io_config.clone(),
        builder.allocate_fn,
    )
}

/// Discard a builder without building it.
///
/// # Safety
/// Caller must pass a valid builder pointer and not use it again afterwards.
#[no_mangle]
pub unsafe extern "C" fn free_rest_engine_builder(builder: *mut RestEngineBuilder) {
    if !builder.is_null() {
        drop(unsafe { Box::from_raw(builder) });
    }
}

/// Minimal dialect options for unit tests and examples.
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_test_utils::allocate_err;
    use crate::kernel_string_slice;

    // Building the engine spawns a Tokio background thread (DefaultEngine's executor) that the test
    // does not join, which Miri's leak check rejects. The `build_rejects_*` tests cover the build
    // logic's error paths without spawning a thread.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn build_succeeds_with_defaults_and_options() {
        let mut options = test_dialect_options();
        options.insert("header.Authorization".into(), "Bearer x".into());
        options.insert("files_prefix".into(), "/TablesById/u".into());
        options.insert("entry_strip_prefix".into(), "/TablesById/u".into());
        options.insert("retry.max_retries".into(), "3".into());
        options.insert("put.verify_on_ambiguous".into(), "true".into());
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options,
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_ok());
    }

    #[test]
    fn build_rejects_missing_dialect_option() {
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options: HashMap::new(),
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_err());
    }

    #[test]
    fn build_rejects_invalid_timeout() {
        let mut options = test_dialect_options();
        options.insert("tls.timeout_secs".into(), "nope".into());
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options,
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_err());
    }

    #[test]
    fn build_rejects_invalid_max_retries() {
        let mut options = test_dialect_options();
        options.insert("retry.max_retries".into(), "nope".into());
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options,
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_err());
    }

    #[test]
    fn build_rejects_invalid_verify_on_ambiguous() {
        let mut options = test_dialect_options();
        options.insert("put.verify_on_ambiguous".into(), "nope".into());
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options,
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_err());
    }

    #[test]
    fn build_rejects_invalid_header_value() {
        let mut options = test_dialect_options();
        options.insert("header.X".into(), "bad\nvalue".into());
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options,
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_err());
    }

    #[test]
    fn build_rejects_partial_mtls() {
        let mut options = test_dialect_options();
        options.insert("tls.cert_path".into(), "/x/cert.pem".into());
        let b = RestEngineBuilder {
            base_url: "http://localhost".to_string(),
            allocate_fn: allocate_err,
            options,
            auth_callback: None,
            multithreaded_executor_config: None,
            io_config: IoConcurrencyConfig::default(),
        };
        assert!(rest_engine_builder_build_impl(&b).is_err());
    }

    extern "C" fn emit_header_and_ttl(_context: *mut c_void, emit_state: *mut c_void) {
        let name = "authorization";
        let value = "Bearer t";
        unsafe {
            rest_engine_emit_auth_header(
                emit_state,
                kernel_string_slice!(name),
                kernel_string_slice!(value),
            );
            rest_engine_emit_auth_ttl(emit_state, 60_000);
        };
    }

    #[test]
    fn auth_callback_collects_emitted_headers_and_ttl() {
        let ctx = AuthCallbackContext {
            callback: emit_header_and_ttl,
            context: std::ptr::null_mut(),
        };
        let emitted = ctx.collect();
        assert_eq!(
            emitted.headers,
            vec![("authorization".to_string(), "Bearer t".to_string())]
        );
        assert_eq!(emitted.ttl_ms, Some(60_000));
    }
}
