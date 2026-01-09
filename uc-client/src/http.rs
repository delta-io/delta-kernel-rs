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
    ]);

    let client = Client::builder()
        .default_headers(headers)
        .timeout(config.timeout)
        .connect_timeout(config.connect_timeout)
        .build()?;

    Ok(client)
}

/// Execute a request with retry logic for server errors and request failures.
/// Retries up to `max_retries` times with linear backoff: delay = `retry_base_delay * attempt`.
pub async fn execute_with_retry<F, Fut>(config: &ClientConfig, f: F) -> Result<Response>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<Response, reqwest::Error>>,
{
    for retry in 0..=config.max_retries {
        match f().await {
            Ok(response) if !response.status().is_server_error() => return Ok(response),
            Ok(response) if retry < config.max_retries => {
                warn!(
                    "Server error {}, retrying (attempt {}/{})",
                    response.status(),
                    retry + 1,
                    config.max_retries
                );
            }
            Ok(response) => {
                return Err(Error::ApiError {
                    status: response.status().as_u16(),
                    message: "Server error".to_string(),
                })
            }
            Err(e) if retry < config.max_retries => {
                warn!(
                    "Request failed, retrying (attempt {}/{}): {}",
                    retry + 1,
                    config.max_retries,
                    e
                );
            }
            Err(e) => return Err(Error::from(e)),
        }

        tokio::time::sleep(config.retry_base_delay * (retry + 1)).await;
    }

    // this is actually unreachable since we return in the loop for Ok/Err after all retries
    Err(Error::MaxRetriesExceeded)
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
            StatusCode::UNAUTHORIZED => Err(Error::AuthenticationFailed),
            StatusCode::NOT_FOUND => Err(Error::ApiError {
                status: status.as_u16(),
                message: format!("Resource not found: {}", error_body),
            }),
            _ => Err(Error::ApiError {
                status: status.as_u16(),
                message: error_body,
            }),
        }
    }
}
