use tracing::instrument;
use unity_catalog_delta_client_api::{TableIdentifier, UpdateTableClient, UpdateTableRequest};
use url::Url;

use crate::config::ClientConfig;
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
        result.map_err(Into::into)
    }
}
