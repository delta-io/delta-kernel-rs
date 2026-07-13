//! HTTP response status handling for [`RestObjectStore`](super::store::RestObjectStore).
//!
//! Three separate concerns show up across REST calls:
//!
//! 1. **Retry classification** ([`is_retryable_http_status`], [`is_transient`]) -- 5xx / dropped
//!    connections may succeed on another attempt. These are *not* terminal errors yet.
//! 2. **Typed mapping** ([`typed_http_error`]) -- select status codes become specific
//!    [`ObjectStoreError`] variants (`404 -> NotFound`, `409 -> AlreadyExists`). Returns `None` for
//!    2xx and for statuses with no typed mapping.
//! 3. **Generic non-2xx** ([`reject_remaining_non_success`]) -- reqwest's `error_for_status` turns
//!    any remaining failure into [`ObjectStoreError::Generic`].
//!
//! Most callers use [`ensure_success_response`], which applies (2) then (3). List and
//! verified-create encode extra rules in [`ensure_list_response`] and
//! [`classify_put_create_response`].

use delta_kernel::object_store::{Error as ObjectStoreError, Result as ObjectStoreResult};

use super::generic_error;

/// The last failure that triggered a retry.
pub(crate) enum RetryFailure {
    ServerError(reqwest::StatusCode),
    Transport(String),
}

impl std::fmt::Display for RetryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServerError(status) => write!(f, "HTTP {status}"),
            Self::Transport(msg) => write!(f, "{msg}"),
        }
    }
}

/// How verified-create PUT should treat an HTTP response.
pub(crate) enum PutCreateDisposition {
    Success,
    Terminal(ObjectStoreError),
    Ambiguous { failure: RetryFailure },
}

/// HTTP 5xx may succeed on retry; callers classify it before mapping to a terminal error.
pub(crate) fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error()
}

/// Transport-level failures worth retrying for an idempotent request.
pub(crate) fn is_transient(err: &reqwest::Error) -> bool {
    err.is_timeout()
        || err.is_connect()
        || ((err.is_request() || err.is_body()) && err.status().is_none())
}

/// If `status` maps to a typed [`ObjectStoreError`] (`404`, `409`, ...), return it. Returns `None`
/// for 2xx and for statuses with no typed mapping (see [`reject_remaining_non_success`]).
pub(crate) fn typed_http_error(
    status: reqwest::StatusCode,
    path: &str,
) -> Option<ObjectStoreError> {
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

/// Apply reqwest's catch-all check for any remaining non-2xx after typed mapping.
pub(crate) fn reject_remaining_non_success(
    response: reqwest::Response,
) -> ObjectStoreResult<reqwest::Response> {
    response.error_for_status().map_err(generic_error)
}

/// Typed status mapping, then generic non-2xx rejection. Returns the response on success.
pub(crate) fn ensure_success_response(
    response: reqwest::Response,
    path: &str,
) -> ObjectStoreResult<reqwest::Response> {
    if let Some(err) = typed_http_error(response.status(), path) {
        return Err(err);
    }
    reject_remaining_non_success(response)
}

/// Like [`ensure_success_response`], but a first-page `NotFound` yields `Ok(None)` so an absent
/// directory lists as empty. A `NotFound` mid-pagination is an error.
pub(crate) fn ensure_list_response(
    response: reqwest::Response,
    path: &str,
    page_token: Option<&str>,
) -> ObjectStoreResult<Option<reqwest::Response>> {
    if let Some(err) = typed_http_error(response.status(), path) {
        if matches!(err, ObjectStoreError::NotFound { .. }) && page_token.is_none() {
            return Ok(None);
        }
        return Err(err);
    }
    Ok(Some(reject_remaining_non_success(response)?))
}

/// Classify a verified-create PUT attempt: success, a terminal error, or an ambiguous outcome that
/// requires read-back reconciliation.
///
/// - **Success** -- 2xx after [`reject_remaining_non_success`] validation.
/// - **Terminal** -- definite failure (`404`, first-attempt `409`, other non-retryable 4xx, or
///   non-transient transport error).
/// - **Ambiguous** -- retryable 5xx, `409` on a retry (may be our own landed write), or transient
///   transport error.
pub(crate) fn classify_put_create_response(
    response: Result<reqwest::Response, reqwest::Error>,
    path: &str,
    retries: u32,
) -> ObjectStoreResult<PutCreateDisposition> {
    let response = match response {
        Ok(resp) => resp,
        Err(e) if is_transient(&e) => {
            return Ok(PutCreateDisposition::Ambiguous {
                failure: RetryFailure::Transport(e.to_string()),
            });
        }
        Err(e) => return Err(generic_error(e)),
    };
    let status = response.status();
    if status.is_success() {
        reject_remaining_non_success(response)?;
        return Ok(PutCreateDisposition::Success);
    }
    if let Some(err) = typed_http_error(status, path) {
        if matches!(err, ObjectStoreError::AlreadyExists { .. }) && retries > 0 {
            return Ok(PutCreateDisposition::Ambiguous {
                failure: RetryFailure::ServerError(status),
            });
        }
        return Ok(PutCreateDisposition::Terminal(err));
    }
    if is_retryable_http_status(status) {
        return Ok(PutCreateDisposition::Ambiguous {
            failure: RetryFailure::ServerError(status),
        });
    }
    reject_remaining_non_success(response).map(|_| PutCreateDisposition::Success)
}
