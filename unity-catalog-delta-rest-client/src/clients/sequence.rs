use tracing::instrument;
use unity_catalog_delta_client_api::{
    CreateIdentitySequences, DropIdentitySequences, DropIdentitySequencesResponse,
    ReserveIdentityRanges, ReserveIdentityRangesResponse, SequenceClient,
};
use url::Url;

use crate::config::ClientConfig;
use crate::http::{build_http_client, execute_with_retry, handle_empty_response, handle_response};

/// REST implementation of [`SequenceClient`] for the UC Identity Sequence Service.
///
/// Targets the `identity/sequence` (create via POST, drop via DELETE) and
/// `identity/sequence/reserve` (reserve via POST) endpoints of the Unity Catalog Delta Tables
/// API.
#[derive(Debug, Clone)]
pub struct UCSequenceRestClient {
    http_client: reqwest::Client,
    config: ClientConfig,
    base_url: Url,
}

impl UCSequenceRestClient {
    /// Create from config.
    pub fn new(config: ClientConfig) -> crate::error::Result<Self> {
        Ok(Self {
            http_client: build_http_client(&config)?,
            base_url: config.workspace_url.clone(),
            config,
        })
    }

    /// Create from existing reqwest client.
    pub fn with_http_client(http_client: reqwest::Client, config: ClientConfig) -> Self {
        Self {
            base_url: config.workspace_url.clone(),
            http_client,
            config,
        }
    }
}

impl SequenceClient for UCSequenceRestClient {
    #[instrument(skip(self))]
    async fn create_identity_sequences(
        &self,
        req: CreateIdentitySequences,
    ) -> unity_catalog_delta_client_api::Result<()> {
        let result: crate::error::Result<()> = async {
            let url = self.base_url.join("identity/sequence")?;
            let response = execute_with_retry(&self.config, || {
                self.http_client
                    .request(reqwest::Method::POST, url.clone())
                    .json(&req)
                    .send()
            })
            .await?;
            // The service returns 200 with no body on success.
            handle_empty_response(response).await
        }
        .await;
        result.map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn reserve_identity_ranges(
        &self,
        req: ReserveIdentityRanges,
    ) -> unity_catalog_delta_client_api::Result<ReserveIdentityRangesResponse> {
        let result: crate::error::Result<ReserveIdentityRangesResponse> = async {
            let url = self.base_url.join("identity/sequence/reserve")?;
            let response = execute_with_retry(&self.config, || {
                self.http_client
                    .request(reqwest::Method::POST, url.clone())
                    .json(&req)
                    .send()
            })
            .await?;
            handle_response(response).await
        }
        .await;
        result.map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn drop_identity_sequences(
        &self,
        req: DropIdentitySequences,
    ) -> unity_catalog_delta_client_api::Result<DropIdentitySequencesResponse> {
        let result: crate::error::Result<DropIdentitySequencesResponse> = async {
            let url = self.base_url.join("identity/sequence")?;
            let response = execute_with_retry(&self.config, || {
                self.http_client
                    .request(reqwest::Method::DELETE, url.clone())
                    .json(&req)
                    .send()
            })
            .await?;
            handle_response(response).await
        }
        .await;
        result.map_err(Into::into)
    }
}
