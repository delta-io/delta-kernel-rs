use reqwest::StatusCode;
use serde::Deserialize;
use tracing::instrument;
use unity_catalog_delta_client_api::{TableIdentifier, UpdateTableClient, UpdateTableRequest};
use url::Url;

use crate::config::ClientConfig;
use crate::error::Error;
use crate::http::{build_http_client, handle_empty_response};

/// REST implementation of [`UpdateTableClient`].
#[derive(Debug)]
pub struct UCUpdateTableRestClient {
    http_client: reqwest::Client,
    base_url: Url,
}

impl UCUpdateTableRestClient {
    /// Create from config.
    pub fn new(config: ClientConfig) -> crate::error::Result<Self> {
        Ok(Self {
            http_client: build_http_client(&config)?,
            base_url: config.workspace_url.clone(),
        })
    }

    /// Create from an existing reqwest client and config.
    pub fn with_http_client(http_client: reqwest::Client, config: ClientConfig) -> Self {
        Self {
            base_url: config.workspace_url.clone(),
            http_client,
        }
    }
}

impl UpdateTableClient for UCUpdateTableRestClient {
    #[instrument(skip(self))]
    async fn update_table(
        &self,
        target: &TableIdentifier,
        request: UpdateTableRequest,
    ) -> unity_catalog_delta_client_api::Result<()> {
        let result: crate::error::Result<()> = async {
            let path = format!(
                "delta/v1/catalogs/{}/schemas/{}/tables/{}",
                target.catalog, target.schema, target.table
            );
            let url = self.base_url.join(&path)?;

            // Single attempt. UC rejects a resubmit of an already-ratified version with a version
            // conflict. Retries belong in the transaction layer.
            let response = self.http_client.post(url).json(&request).send().await?;
            handle_empty_response(response).await
        }
        .await;
        result.map_err(|error| match &error {
            // The update-table API uses HTTP 429 ResourceExhaustedException for a full
            // unpublished-commit queue; ordinary throttling uses TooManyRequestsException.
            // https://github.com/unitycatalog/unitycatalog/blob/main/api/delta.yaml
            Error::HttpStatusError { status, message }
                if *status == StatusCode::TOO_MANY_REQUESTS.as_u16()
                    && serde_json::from_str::<DeltaErrorResponse>(message).is_ok_and(|body| {
                        body.error.error_type == "ResourceExhaustedException"
                    }) =>
            {
                unity_catalog_delta_client_api::Error::MaxUnpublishedCommitsExceeded(None)
            }
            _ => error.into(),
        })
    }
}

#[derive(Deserialize)]
struct DeltaErrorResponse {
    error: DeltaError,
}

#[derive(Deserialize)]
struct DeltaError {
    #[serde(rename = "type")]
    error_type: String,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use unity_catalog_delta_client_api::{Commit, DeltaTableUpdate, Error};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn update_table_reports_full_commit_queue() {
        let error = update_table_error(
            429,
            r#"{"error":{"type":"ResourceExhaustedException","code":429,"message":"Full"}}"#,
        )
        .await;

        assert!(matches!(error, Error::MaxUnpublishedCommitsExceeded(None)));
    }

    #[rstest]
    #[case::rate_limit(
        429,
        r#"{"error":{"type":"TooManyRequestsException","code":429,"message":"Slow down"}}"#
    )]
    #[case::unknown_type(429, r#"{"error":{"type":"UnknownException"}}"#)]
    #[case::missing_type(429, r#"{"error":{"message":"ResourceExhaustedException"}}"#)]
    #[case::invalid_type(429, r#"{"error":{"type":429}}"#)]
    #[case::non_json(429, "Too many requests")]
    #[case::malformed_json(429, "{\"error\":")]
    #[case::wrong_status(400, r#"{"error":{"type":"ResourceExhaustedException"}}"#)]
    #[tokio::test]
    async fn update_table_preserves_unrecognized_error(#[case] status: u16, #[case] body: &str) {
        let error = update_table_error(status, body).await;

        assert!(matches!(
            error,
            Error::Generic(message) if message == format!("HTTP error (status {status}): {body}")
        ));
    }

    async fn update_table_error(status: u16, body: &str) -> Error {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/table",
            ))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;

        let config = ClientConfig::build(server.uri(), "test-token")
            .build()
            .unwrap();
        let client = UCUpdateTableRestClient::new(config).unwrap();
        let request = UpdateTableRequest::new(
            vec![],
            vec![DeltaTableUpdate::AddCommit {
                commit: Commit::new(1, 100, "commit.json", 10, 100),
            }],
        )
        .unwrap();

        client
            .update_table(&TableIdentifier::new("main", "default", "table"), request)
            .await
            .unwrap_err()
    }
}
