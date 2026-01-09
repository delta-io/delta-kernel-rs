use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;
use tracing::instrument;
use url::Url;

use crate::config::{ClientConfig, ClientConfigBuilder};
use crate::error::{Error, Result};
use crate::http::{build_http_client, execute_with_retry, handle_response};
use crate::models::commits::{CommitRequest, CommitsRequest, CommitsResponse};
use crate::models::credentials::{CredentialsRequest, Operation, TemporaryTableCredentials};
use crate::models::tables::TablesResponse;

/// An HTTP client for interacting with the Unity Catalog API.
#[derive(Debug, Clone)]
pub struct UCClient {
    client: reqwest::Client,
    config: ClientConfig,
    base_url: Url,
}

impl UCClient {
    /// Create a new client from [ClientConfig].
    pub fn new(config: ClientConfig) -> Result<Self> {
        Ok(Self {
            client: build_http_client(&config)?,
            base_url: config.workspace_url.clone(),
            config,
        })
    }

    /// Create a new [UCClientBuilder] to configure and build a [UCClient].
    pub fn builder(workspace: impl Into<String>, token: impl Into<String>) -> UCClientBuilder {
        UCClientBuilder::new(workspace, token)
    }

    /// Get the latest commits for the table.
    #[instrument(skip(self))]
    pub async fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse> {
        let url = self.base_url.join("delta/preview/commits")?;
        let response = execute_with_retry(&self.config, || {
            self.client
                .request(reqwest::Method::GET, url.clone())
                .json(&request)
                .send()
        })
        .await?;

        handle_response(response).await
    }

    /// Commit a new version to the table.
    #[instrument(skip(self))]
    pub async fn commit(&self, request: CommitRequest) -> Result<()> {
        let url = self.base_url.join("delta/preview/commits")?;
        let response = execute_with_retry(&self.config, || {
            self.client
                .request(reqwest::Method::POST, url.clone())
                .json(&request)
                .send()
        })
        .await?;

        #[derive(Deserialize)]
        struct EmptyResponse {}
        let _: EmptyResponse = handle_response(response).await?;
        Ok(())
    }

    /// Resolve the table by name.
    #[instrument(skip(self))]
    pub async fn get_table(&self, table_name: &str) -> Result<TablesResponse> {
        let url = self.base_url.join(&format!("tables/{}", table_name))?;

        let response =
            execute_with_retry(&self.config, || self.client.get(url.clone()).send()).await?;

        match response.status() {
            StatusCode::NOT_FOUND => Err(Error::TableNotFound(table_name.to_string())),
            _ => handle_response(response).await,
        }
    }

    /// Get temporary cloud storage credentials for accessing a table.
    #[instrument(skip(self))]
    pub async fn get_credentials(
        &self,
        table_id: &str,
        operation: Operation,
    ) -> Result<TemporaryTableCredentials> {
        let url = self.base_url.join("temporary-table-credentials")?;

        let request_body = CredentialsRequest::new(table_id, operation);
        let response = execute_with_retry(&self.config, || {
            self.client.post(url.clone()).json(&request_body).send()
        })
        .await?;

        handle_response(response).await
    }
}

/// A builder for configuring and creating a [UCClient].
pub struct UCClientBuilder {
    config_builder: ClientConfigBuilder,
}

impl UCClientBuilder {
    pub fn new(workspace: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            config_builder: ClientConfig::build(workspace, token),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config_builder = self.config_builder.with_timeout(timeout);
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.config_builder = self.config_builder.with_connect_timeout(timeout);
        self
    }

    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.config_builder = self.config_builder.with_max_retries(retries);
        self
    }

    pub fn with_retry_delays(mut self, base: Duration, max: Duration) -> Self {
        self.config_builder = self.config_builder.with_retry_delays(base, max);
        self
    }

    pub fn build(self) -> Result<UCClient> {
        let config = self.config_builder.build()?;
        UCClient::new(config)
    }
}
