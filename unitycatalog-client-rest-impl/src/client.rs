use reqwest::StatusCode;
use tracing::instrument;
use url::Url;

use unitycatalog_client_api::{GetTableClient, TableInfo, UCGetStagingTableClient};

use crate::config::ClientConfig;
use crate::error::{Error, Result};
use crate::http::{build_http_client, execute_with_retry, handle_response};
use unitycatalog_client_api::{Operation, TemporaryTableCredentials};

use crate::models::credentials::CredentialsRequest;
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
        let url = self.base_url.join(&format!("tables/{table_name}"))?;

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

impl GetTableClient for UCClient {
    async fn get_table(&self, table_name: &str) -> unitycatalog_client_api::Result<TableInfo> {
        UCClient::get_table(self, table_name)
            .await
            .map_err(unitycatalog_client_api::Error::from)
            .map(|resp| TableInfo {
                table_id: resp.table_id,
                storage_location: resp.storage_location,
            })
    }
}

impl UCGetStagingTableClient for UCClient {
    async fn get_credentials(
        &self,
        table_id: &str,
        operation: unitycatalog_client_api::Operation,
    ) -> unitycatalog_client_api::Result<unitycatalog_client_api::TemporaryTableCredentials> {
        UCClient::get_credentials(self, table_id, operation)
            .await
            .map_err(unitycatalog_client_api::Error::from)
    }
}
