// TODO(https://github.com/delta-io/delta-kernel-rs/issues/2251): Replace UCClient with
// trait-based clients (GetTableClient, GetCredentialsClient) once those traits are added
// to unity-catalog-delta-client-api.
use reqwest::StatusCode;
use tracing::instrument;
use unity_catalog_delta_client_api::{
    CatalogConfig, LoadTableResponse, Operation, TemporaryTableCredentials,
};
use url::Url;

use crate::config::ClientConfig;
use crate::error::Result;
use crate::http::{build_http_client, execute_with_retry, handle_response};
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
            StatusCode::NOT_FOUND => Err(unity_catalog_delta_client_api::Error::TableNotFound(
                table_name.to_string(),
            )
            .into()),
            _ => handle_response(response).await,
        }
    }

    /// `GET /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}` (`load_table`): reads a
    /// catalog-managed table's full metadata plus any unpublished commits. Read-only.
    #[instrument(skip(self))]
    pub async fn load_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<LoadTableResponse> {
        let path = format!("delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}");
        let url = self.base_url.join(&path)?;

        let response =
            execute_with_retry(&self.config, || self.http_client.get(url.clone()).send()).await?;
        match response.status() {
            StatusCode::NOT_FOUND => Err(unity_catalog_delta_client_api::Error::TableNotFound(
                format!("{catalog}.{schema}.{table}"),
            )
            .into()),
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

    /// `GET /delta/v1/config?catalog={catalog}&protocol-versions={csv}`: session-start handshake.
    /// `protocol_versions` is a list of version strings such as `["1.1", "2.3"]` indicating the
    /// highest version per major version the client supports.
    #[instrument(skip(self))]
    pub async fn get_config(
        &self,
        catalog: &str,
        protocol_versions: &[&str],
    ) -> Result<CatalogConfig> {
        let mut url = self.base_url.join("delta/v1/config")?;
        url.query_pairs_mut().append_pair("catalog", catalog);
        url.query_pairs_mut()
            .append_pair("protocol-versions", &protocol_versions.join(","));

        let response =
            execute_with_retry(&self.config, || self.http_client.get(url.clone()).send()).await?;
        handle_response(response).await
    }
}
