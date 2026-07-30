use reqwest::StatusCode;
use tracing::instrument;
use unity_catalog_delta_client_api::{
    CatalogConfig, CredentialsResponse, LoadTableResponse, Operation,
};
use url::Url;

use crate::config::ClientConfig;
use crate::error::Result;
use crate::http::{build_http_client, execute_with_retry, handle_response};

/// Builds the Delta-Tables per-table resource path
/// (`delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}`) that the `load_table` and
/// credential-vending endpoints share.
fn table_path(catalog: &str, schema: &str, table: &str) -> String {
    format!("delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
}

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

    /// `GET /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}`:
    /// fetch the table's metadata plus inline unpublished commits.
    #[instrument(skip(self))]
    pub async fn load_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<LoadTableResponse> {
        let url = self.base_url.join(&table_path(catalog, schema, table))?;

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

    /// Vend temporary cloud-storage credentials for the table via the Delta-Tables
    /// `GET .../catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials?operation=...`
    /// endpoint.
    #[instrument(skip(self))]
    pub async fn get_table_credentials(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        operation: Operation,
    ) -> Result<CredentialsResponse> {
        let path = format!("{}/credentials", table_path(catalog, schema, table));
        let mut url = self.base_url.join(&path)?;
        url.query_pairs_mut()
            .append_pair("operation", &operation.to_string());

        let response =
            execute_with_retry(&self.config, || self.http_client.get(url.clone()).send()).await?;
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
