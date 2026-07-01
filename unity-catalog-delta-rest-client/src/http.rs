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
                return Err(Error::HttpStatusError {
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

        Err(error_for_errored_status(status, error_body))
    }
}

/// Map a non-success HTTP status and response body to a client [`Error`].
///
/// Some commit-semantic status codes UC returns become UC delta client API errors
/// that are preserved as rest client errors. Anything else stays a generic
/// [`Error::HttpStatusError`].
fn error_for_errored_status(status: StatusCode, body: String) -> Error {
    use unity_catalog_delta_client_api::Error as ApiError;
    match status {
        StatusCode::UNAUTHORIZED => ApiError::AuthenticationFailed.into(),
        StatusCode::NOT_FOUND => ApiError::TableNotFound(body).into(),
        StatusCode::CONFLICT => ApiError::CommitConflict.into(),
        StatusCode::BAD_REQUEST => ApiError::InvalidCommit(body).into(),
        StatusCode::TOO_MANY_REQUESTS => ApiError::RateLimited.into(),
        _ => Error::HttpStatusError {
            status: status.as_u16(),
            message: body,
        },
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use unity_catalog_delta_client_api::Error as ApiError;

    use super::*;

    /// Each response status maps to its distinct UC delta client API error (the
    /// trait error implementations see), and response bodies are preserved.
    #[rstest]
    #[case(StatusCode::UNAUTHORIZED, ApiError::AuthenticationFailed)]
    #[case(StatusCode::NOT_FOUND, ApiError::TableNotFound("boom".into()))]
    #[case(StatusCode::CONFLICT, ApiError::CommitConflict)]
    #[case(StatusCode::BAD_REQUEST, ApiError::InvalidCommit("boom".into()))]
    #[case(StatusCode::TOO_MANY_REQUESTS, ApiError::RateLimited)]
    fn error_for_status_maps_commit_semantic_codes_to_distinct_api_errors(
        #[case] status: StatusCode,
        #[case] expected: ApiError,
    ) {
        let api_err = ApiError::from(error_for_errored_status(status, "boom".into()));
        assert_eq!(format!("{api_err:?}"), format!("{expected:?}"));
    }

    #[test]
    fn error_for_status_maps_unrecognized_status_to_http_status_error() {
        let err = error_for_errored_status(StatusCode::IM_A_TEAPOT, "teapot".into());
        assert!(
            matches!(err, Error::HttpStatusError { status: 418, ref message } if message == "teapot"),
            "unexpected: {err:?}"
        );
    }
}
