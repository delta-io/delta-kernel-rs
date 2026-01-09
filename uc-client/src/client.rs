use reqwest::StatusCode;
use tracing::instrument;
use url::Url;

use crate::config::ClientConfig;
use crate::error::{Error, Result};
use crate::http::{build_http_client, execute_with_retry, handle_response};
use crate::models::credentials::{CredentialsRequest, Operation, TemporaryTableCredentials};
use crate::models::tables::TablesResponse;

/// An HTTP client for interacting with the Unity Catalog API.
#[derive(Debug, Clone)]
pub struct UCClient {
    http_client: reqwest::Client,
    config: ClientConfig,
    base_url: Url,
}

impl UCClient {
    /// Create a new client from [ClientConfig].
    pub fn new(config: ClientConfig) -> Result<Self> {
        Ok(Self {
            http_client: build_http_client(&config)?,
            base_url: config.workspace_url.clone(),
            config,
        })
    }

    /// Create from existing reqwest Client.
    pub fn with_http_client(http_client: reqwest::Client, config: ClientConfig) -> Self {
        Self {
            base_url: config.workspace_url.clone(),
            http_client,
            config,
        }
    }

    /// Resolve the table by name.
    #[instrument(skip(self))]
    pub async fn get_table(&self, table_name: &str) -> Result<TablesResponse> {
        let url = self.base_url.join(&format!("tables/{}", table_name))?;

        let response =
            execute_with_retry(&self.config, || self.http_client.get(url.clone()).send()).await?;

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
            self.http_client
                .post(url.clone())
                .json(&request_body)
                .send()
        })
        .await?;

        handle_response(response).await
    }
}
