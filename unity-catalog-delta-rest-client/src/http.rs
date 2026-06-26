use std::future::Future;

use reqwest::{header, Client, Response, StatusCode};
use tracing::warn;

use crate::config::ClientConfig;
use crate::error::{Error, Result};

/// Build a configured HTTP client from the given config.
pub fn build_http_client(config: &ClientConfig) -> Result<Client> {
    let headers = header::HeaderMap::from_iter([
        (
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", config.token))?,
        ),
        (
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        ),
        // The UC REST API rejects requests without a User-Agent header (HTTP 400).
        (
            header::USER_AGENT,
            header::HeaderValue::from_str(&config.user_agent)?,
        ),
    ]);

    let client = Client::builder()
        .default_headers(headers)
        .timeout(config.timeout)
        .connect_timeout(config.connect_timeout)
        .build()?;

    Ok(client)
}

/// Whether a response status warrants a retry: server errors (5xx) and rate limiting (429).
fn is_retryable(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

/// Execute a request with retry logic for retryable responses (5xx, 429) and request failures.
/// Retries up to `max_retries` times with linear backoff: the nth retry waits
/// `retry_base_delay * n`, clamped to `retry_max_delay`.
pub async fn execute_with_retry<F, Fut>(config: &ClientConfig, f: F) -> Result<Response>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<Response, reqwest::Error>>,
{
    // Non-final attempts retry on a retryable status or request failure; the final attempt
    // returns its real result, so there is no sentinel "max retries" error.
    for retry in 0..config.max_retries {
        match f().await {
            Ok(response) if !is_retryable(response.status()) => return Ok(response),
            Ok(response) => warn!(
                "Retryable status {}, retrying (attempt {}/{})",
                response.status(),
                retry + 1,
                config.max_retries
            ),
            Err(e) => warn!(
                "Request failed, retrying (attempt {}/{}): {}",
                retry + 1,
                config.max_retries,
                e
            ),
        }

        let delay = (config.retry_base_delay * (retry + 1)).min(config.retry_max_delay);
        tokio::time::sleep(delay).await;
    }

    let response = f().await?;
    if is_retryable(response.status()) {
        let status = response.status();
        let message = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(Error::HttpStatusError {
            status: status.as_u16(),
            message,
        });
    }
    Ok(response)
}

/// Handle HTTP response and deserialize.
pub async fn handle_response<T>(response: Response) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();

    if status.is_success() {
        response.json::<T>().await.map_err(Error::from)
    } else {
        let error_body = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());

        match status {
            StatusCode::UNAUTHORIZED => {
                Err(unity_catalog_delta_client_api::Error::AuthenticationFailed.into())
            }
            StatusCode::CONFLICT => {
                Err(unity_catalog_delta_client_api::Error::CommitConflict.into())
            }
            StatusCode::NOT_FOUND => Err(Error::HttpStatusError {
                status: status.as_u16(),
                message: format!("Resource not found: {error_body}"),
            }),
            _ => Err(Error::HttpStatusError {
                status: status.as_u16(),
                message: error_body,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_retryable_classifies_statuses() {
        for code in [500u16, 502, 503, 429] {
            let status = StatusCode::from_u16(code).unwrap();
            assert!(is_retryable(status), "{code} should be retryable");
        }
        for code in [200u16, 400, 401, 404] {
            let status = StatusCode::from_u16(code).unwrap();
            assert!(!is_retryable(status), "{code} should not be retryable");
        }
    }
}
