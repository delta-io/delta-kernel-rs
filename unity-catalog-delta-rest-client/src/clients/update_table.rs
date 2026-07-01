//! REST implementation of the `UpdateTableClient` trait.
//!
//! Wraps `POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}` with
//! the typed `requirements + updates` payload. The catalog, schema, and
//! table name are carried on the `UpdateTableRequest` itself (as
//! `#[serde(skip)]` fields) so the trait method stays single-arg while the
//! impl can build the URL from per-call routing rather than client state.

use tracing::instrument;
use unity_catalog_delta_client_api::{UpdateTableClient, UpdateTableRequest, UpdateTableResponse};
use url::Url;

use crate::config::ClientConfig;
use crate::http::{build_http_client, handle_response};

/// REST implementation of [`UpdateTableClient`].
///
/// One client instance can serve any number of UC tables: each `update_table`
/// call carries its own `(catalog, schema, table)` routing on the
/// `UpdateTableRequest`. The client shares a connection pool internally.
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
    /// An `Err` from the commit POST leaves the commit state UNKNOWN: the request may have reached
    /// and been applied by the server before the connection failed. Since AddCommit is
    /// non-idempotent, callers must reconcile by re-reading the table rather than blindly retrying.
    #[instrument(skip(self))]
    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> unity_catalog_delta_client_api::Result<UpdateTableResponse> {
        let result: crate::error::Result<UpdateTableResponse> = async {
            let path = format!(
                "delta/v1/catalogs/{}/schemas/{}/tables/{}",
                request.catalog, request.schema, request.table_name
            );
            let url = self.base_url.join(&path)?;

            // Single attempt, no retry: AddCommit is non-idempotent, so a retried POST could
            // double-register a commit.
            let response = self.http_client.post(url).json(&request).send().await?;
            handle_response(response).await
        }
        .await;
        result.map_err(Into::into)
    }
}
