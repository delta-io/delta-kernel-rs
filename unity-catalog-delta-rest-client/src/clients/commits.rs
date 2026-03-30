use serde::Deserialize;
use tracing::instrument;
use url::Url;

use crate::config::ClientConfig;
use crate::http::{build_http_client, execute_with_retry, handle_response};
use unity_catalog_delta_client_api::{
    CommitClient, CommitRequest, CommitsRequest, CommitsResponse, GetCommitsClient,
};

/// Placeholder for deserializing empty JSON responses from void-returning endpoints.
#[derive(Deserialize)]
struct EmptyResponse {}

/// REST implementation of [CommitClient] and [GetCommitsClient].
#[derive(Debug, Clone)]
pub struct UCCommitsRestClient {
    http_client: reqwest::Client,
    config: ClientConfig,
    base_url: Url,
}

impl UCCommitsRestClient {
    /// Create from config.
    pub fn new(config: ClientConfig) -> crate::error::Result<Self> {
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
}

impl GetCommitsClient for UCCommitsRestClient {
    #[instrument(skip(self))]
    async fn get_commits(
        &self,
        request: CommitsRequest,
    ) -> unity_catalog_delta_client_api::Result<CommitsResponse> {
        let result: crate::error::Result<CommitsResponse> = async {
            let url = self.base_url.join("delta/preview/commits")?;
            let response = execute_with_retry(&self.config, || {
                self.http_client
                    .request(reqwest::Method::GET, url.clone())
                    .json(&request)
                    .send()
            })
            .await?;
            handle_response(response).await
        }
        .await;
        result.map_err(Into::into)
    }
}

impl CommitClient for UCCommitsRestClient {
    #[instrument(skip(self))]
    async fn commit(&self, request: CommitRequest) -> unity_catalog_delta_client_api::Result<()> {
        let result: crate::error::Result<()> = async {
            let url = self.base_url.join("delta/preview/commits")?;
            let response = execute_with_retry(&self.config, || {
                self.http_client
                    .request(reqwest::Method::POST, url.clone())
                    .json(&request)
                    .send()
            })
            .await?;

            let _: EmptyResponse = handle_response(response).await?;
            Ok(())
        }
        .await;
        result.map_err(Into::into)
    }
}
