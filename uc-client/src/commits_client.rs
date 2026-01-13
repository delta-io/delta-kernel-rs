use serde::Deserialize;
use tracing::instrument;
use url::Url;

use crate::config::ClientConfig;
use crate::error::Result;
use crate::http::{build_http_client, execute_with_retry, handle_response};
use crate::models::commits::{CommitRequest, CommitsRequest, CommitsResponse};

/// Trait for UC commits API operations.
///
/// Implementations of this trait are responsible for performing any necessary retries on transient
/// failures. This trait is designed to be injected into a `uc_catalog::committer::UCCommitter`,
/// which itself does not perform any retries and relies on the underlying client implementation to
/// handle retry logic.
#[allow(async_fn_in_trait)]
pub trait UCCommitsClient: Send + Sync {
    /// Get the latest commits for the table.
    async fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse>;

    /// Commit a new version to the table.
    async fn commit(&self, request: CommitRequest) -> Result<()>;
}

/// REST implementation of [UCCommitsClient].
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
}

impl UCCommitsClient for UCCommitsRestClient {
    #[instrument(skip(self))]
    async fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse> {
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
    async fn commit(&self, request: CommitRequest) -> Result<()> {
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
