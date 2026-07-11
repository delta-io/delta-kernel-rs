//! REST [`RestObjectStore`] support for [`EngineBuilder`](crate::EngineBuilder).
//!
//! Call [`set_builder_rest_object_store`](crate::set_builder_rest_object_store) to select the REST
//! backend, then configure the REST file API dialect (the [`RestEndpointConfig`] field names),
//! TLS (`tls.*`), and resilience (`retry.max_retries`, `put.verify_on_ambiguous`) via
//! [`set_builder_option`](crate::set_builder_option). Pass auth headers via
//! [`set_builder_rest_object_store`](crate::set_builder_rest_object_store) or legacy
//! `header.<Name>` options. The builder `url` is the REST service base URL (not a Delta table
//! path).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::object_store::ObjectStore;
use delta_kernel::{DeltaResult, Error};
use delta_kernel_default_engine::rest_store::{
    build_rest_client, AuthHeaderProvider, RestClientOptions, RestEndpointConfig, RestObjectStore,
    StaticHeaderProvider,
};
use url::Url;

use crate::{KernelStringSlice, TryFromStringSlice};

/// [`set_builder_option`](crate::set_builder_option) key for the legacy REST backend selector.
/// Use [`set_builder_rest_object_store`](crate::set_builder_rest_object_store) instead.
pub(crate) const STORE_BACKEND_KEY: &str = "store.backend";

/// Max `(name, value)` pairs in a [`CAuthHeaders`] struct (FSS typically uses ~3–5).
pub const AUTH_MAX_HEADERS: usize = 8;

/// One HTTP header name/value pair borrowed for the duration of
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
#[repr(C)]
pub struct CAuthHeaderPair {
    pub name: KernelStringSlice,
    pub value: KernelStringSlice,
}

/// Static auth headers for a REST-backed engine.
///
/// Set `count` and fill `headers[0..count]`. When passed to
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store), each slice must remain
/// valid until that call returns; kernel copies the strings.
#[repr(C)]
pub struct CAuthHeaders {
    pub count: u32,
    pub headers: [CAuthHeaderPair; AUTH_MAX_HEADERS],
}

/// REST-specific state stored on [`EngineBuilder`](crate::EngineBuilder) after
/// [`set_builder_rest_object_store`](crate::set_builder_rest_object_store).
#[derive(Default)]
pub(crate) struct RestBuilderState {
    /// Headers from [`set_builder_rest_object_store`](crate::set_builder_rest_object_store). When
    /// `None` at build time, [`build_rest_object_store`] falls back to `header.<Name>` builder
    /// options.
    auth_headers: Option<Vec<(String, String)>>,
}

/// Build REST builder state. `headers == NULL` selects REST with no bulk auth headers.
pub(crate) fn rest_builder_state_from_optional_headers(
    headers: *const CAuthHeaders,
) -> DeltaResult<RestBuilderState> {
    let mut rest = RestBuilderState::default();
    if let Some(headers) = unsafe { headers.as_ref() } {
        rest.set_auth_headers(headers)?;
    }
    Ok(rest)
}

impl RestBuilderState {
    /// Copies `headers` into this builder state.
    pub(crate) fn set_auth_headers(&mut self, headers: &CAuthHeaders) -> DeltaResult<()> {
        self.auth_headers = Some(auth_pairs_from_c(headers)?);
        Ok(())
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
        // SAFETY: caller keeps slice memory valid until `set_builder_rest_object_store` returns.
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

/// Build a [`RestObjectStore`] from an engine builder's URL, options, and REST auth state.
pub(crate) fn build_rest_object_store(
    base_url: &Url,
    options: &HashMap<String, String>,
    rest: &RestBuilderState,
) -> DeltaResult<Arc<dyn ObjectStore>> {
    let config = rest_endpoint_config_from_options(options)?;

    let auth: Arc<dyn AuthHeaderProvider> = match &rest.auth_headers {
        Some(pairs) => Arc::new(StaticHeaderProvider::from_pairs(pairs.clone())?),
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
    fn rest_builder_state_from_null_headers() {
        let rest = rest_builder_state_from_optional_headers(std::ptr::null()).unwrap();
        assert!(rest.auth_headers.is_none());
    }

    #[test]
    fn rest_builder_state_from_auth_headers() {
        let headers = test_auth_headers(&[("Authorization", "Bearer x")]);
        let rest = rest_builder_state_from_optional_headers(&headers).unwrap();
        assert_eq!(
            rest.auth_headers,
            Some(vec![("Authorization".to_string(), "Bearer x".to_string())])
        );
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

    #[test]
    fn build_succeeds_with_c_auth_headers() {
        let mut rest = RestBuilderState::default();
        let headers = test_auth_headers(&[("Authorization", "Bearer x")]);
        rest.set_auth_headers(&headers).unwrap();

        assert!(build_rest_object_store(&test_base_url(), &test_dialect_options(), &rest).is_ok());
    }

    #[test]
    fn build_succeeds_with_header_options_fallback() {
        let mut options = test_dialect_options();
        options.insert("header.Authorization".into(), "Bearer x".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &RestBuilderState::default())
                .is_ok()
        );
    }

    #[test]
    fn build_succeeds_with_defaults_and_options() {
        let mut options = test_dialect_options();
        options.insert("header.Authorization".into(), "Bearer x".into());
        options.insert("files_prefix".into(), "/TablesById/u".into());
        options.insert("entry_strip_prefix".into(), "/TablesById/u".into());
        options.insert("retry.max_retries".into(), "3".into());
        options.insert("put.verify_on_ambiguous".into(), "true".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &RestBuilderState::default())
                .is_ok()
        );
    }

    #[test]
    fn build_rejects_missing_dialect_option() {
        assert!(build_rest_object_store(
            &test_base_url(),
            &HashMap::new(),
            &RestBuilderState::default()
        )
        .is_err());
    }

    #[test]
    fn build_rejects_invalid_timeout() {
        let mut options = test_dialect_options();
        options.insert("tls.timeout_secs".into(), "nope".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &RestBuilderState::default())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_invalid_max_retries() {
        let mut options = test_dialect_options();
        options.insert("retry.max_retries".into(), "nope".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &RestBuilderState::default())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_invalid_verify_on_ambiguous() {
        let mut options = test_dialect_options();
        options.insert("put.verify_on_ambiguous".into(), "nope".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &RestBuilderState::default())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_partial_mtls() {
        let mut options = test_dialect_options();
        options.insert("tls.cert_path".into(), "/x/cert.pem".into());
        assert!(
            build_rest_object_store(&test_base_url(), &options, &RestBuilderState::default())
                .is_err()
        );
    }

    #[test]
    fn build_rejects_invalid_header_value() {
        let mut rest = RestBuilderState::default();
        let headers = test_auth_headers(&[("X", "bad\nvalue")]);
        rest.set_auth_headers(&headers).unwrap();
        assert!(build_rest_object_store(&test_base_url(), &test_dialect_options(), &rest).is_err());
    }
}
