use serde::Deserialize;
use tracing::instrument;
use url::Url;

use crate::config::ClientConfig;
use crate::error::Result;
use crate::http::{build_http_client, execute_with_retry, handle_response};
use unitycatalog_client_api::{CommitRequest, CommitsRequest, CommitsResponse};

pub use unitycatalog_client_api::commits_client::{UCCommitClient, UCGetCommitsClient};

#[cfg(any(test, feature = "test-utils"))]
pub use unitycatalog_client_api::{InMemoryCommitsClient, TableData};

/// REST implementation of [UCCommitClient] and [UCGetCommitsClient].
#[derive(Debug, Clone)]
pub struct UCCommitsRestClient {
    http_client: reqwest::Client,
    config: ClientConfig,
    base_url: Url,
}

impl UCCommitsRestClient {
    /// Create from config.
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

    #[instrument(skip(self))]
    async fn get_commits_impl(&self, request: CommitsRequest) -> Result<CommitsResponse> {
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

    #[instrument(skip(self))]
    async fn commit_impl(&self, request: CommitRequest) -> Result<()> {
        let url = self.base_url.join("delta/preview/commits")?;
        let response = execute_with_retry(&self.config, || {
            self.http_client
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
}

fn uc_err_to_api_err(e: crate::Error) -> unitycatalog_client_api::Error {
    unitycatalog_client_api::Error::Generic(e.to_string())
}

impl UCGetCommitsClient for UCCommitsRestClient {
    async fn get_commits(
        &self,
        request: CommitsRequest,
    ) -> unitycatalog_client_api::Result<CommitsResponse> {
        self.get_commits_impl(request)
            .await
            .map_err(uc_err_to_api_err)
    }
}

impl UCCommitClient for UCCommitsRestClient {
    async fn commit(&self, request: CommitRequest) -> unitycatalog_client_api::Result<()> {
        self.commit_impl(request).await.map_err(uc_err_to_api_err)
    }
}
