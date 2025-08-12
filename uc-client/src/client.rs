use std::future::Future;
use std::time::Duration;

use reqwest::{header, Client, Response, StatusCode};
use tracing::{instrument, warn};
use url::Url;

use crate::config::{ClientConfig, ClientConfigBuilder};
use crate::error::{Error, Result};
use crate::models::commits::{CommitsRequest, CommitsResponse};
use crate::models::credentials::{CredentialsRequest, Operation, TemporaryTableCredentials};
use crate::models::tables::TablesResponse;

#[derive(Debug, Clone)]
pub struct UCClient {
    client: Client,
    config: ClientConfig,
    base_url: Url,
}

impl UCClient {
    pub fn new(config: ClientConfig) -> Result<Self> {
        // default headers with authorization and content type
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", config.token))?,
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(config.timeout)
            .connect_timeout(config.connect_timeout)
            .build()?;

        Ok(Self {
            client,
            base_url: config.workspace_url.clone(),
            config,
        })
    }

    pub fn builder(workspace: impl Into<String>, token: impl Into<String>) -> UCClientBuilder {
        UCClientBuilder::new(workspace, token)
    }

    #[instrument(skip(self))]
    pub async fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse> {
        let url = self.base_url.join("delta/preview/commits")?;

        let response = self
            .execute_with_retry(|| {
                self.client
                    .request(reqwest::Method::GET, url.clone())
                    .json(&request)
                    .send()
            })
            .await?;

        self.handle_response(response).await
    }

    #[instrument(skip(self))]
    pub async fn get_table(&self, table_name: &str) -> Result<TablesResponse> {
        let url = self.base_url.join(&format!("tables/{}", table_name))?;

        let response = self
            .execute_with_retry(|| self.client.get(url.clone()).send())
            .await?;

        match response.status() {
            StatusCode::NOT_FOUND => Err(Error::TableNotFound(table_name.to_string())),
            _ => self.handle_response(response).await,
        }
    }

    #[instrument(skip(self))]
    pub async fn get_credentials(
        &self,
        table_id: &str,
        operation: Operation,
    ) -> Result<TemporaryTableCredentials> {
        let url = self.base_url.join("temporary-table-credentials")?;

        let request_body = CredentialsRequest::new(table_id, operation);
        let response = self
            .execute_with_retry(|| self.client.post(url.clone()).json(&request_body).send())
            .await?;

        self.handle_response(response).await
    }

    async fn execute_with_retry<F, Fut>(&self, f: F) -> Result<Response>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = std::result::Result<Response, reqwest::Error>>,
    {
        let mut retries = 0;
        let max_retries = self.config.max_retries;

        loop {
            match f().await {
                Ok(response) => {
                    if response.status().is_server_error() && retries < max_retries {
                        retries += 1;
                        warn!(
                            "Server error {}, retrying (attempt {}/{})",
                            response.status(),
                            retries,
                            max_retries
                        );
                        tokio::time::sleep(self.config.retry_base_delay * retries).await;
                        continue;
                    }
                    return Ok(response);
                }
                Err(e) => {
                    if retries >= max_retries {
                        return Err(Error::from(e));
                    }
                    retries += 1;
                    warn!(
                        "Request failed, retrying (attempt {}/{}): {}",
                        retries, max_retries, e
                    );
                    tokio::time::sleep(self.config.retry_base_delay * retries).await;
                }
            }
        }
    }

    async fn handle_response<T>(&self, response: Response) -> Result<T>
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
}

pub struct UCClientBuilder {
    config_builder: ClientConfigBuilder,
}

impl UCClientBuilder {
    pub fn new(workspace: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            config_builder: ClientConfig::builder(workspace, token),
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config_builder = self.config_builder.timeout(timeout);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config_builder = self.config_builder.connect_timeout(timeout);
        self
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config_builder = self.config_builder.max_retries(retries);
        self
    }

    pub fn retry_delays(mut self, base: Duration, max: Duration) -> Self {
        self.config_builder = self.config_builder.retry_delays(base, max);
        self
    }

    pub fn build(self) -> Result<UCClient> {
        let config = self.config_builder.build()?;
        UCClient::new(config)
    }
}
