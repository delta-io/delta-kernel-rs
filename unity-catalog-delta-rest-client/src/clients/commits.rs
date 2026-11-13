use tracing::instrument;
use unity_catalog_delta_client_api::{
    CommitState, LoadTableResponse, TableIdentifier, UpdateTableClient, UpdateTableRequest,
};
use url::Url;

use super::uc_client::table_path;
use crate::config::ClientConfig;
use crate::http::{build_http_client, handle_empty_response, handle_response};

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
    async fn get_commits(
        &self,
        target: &TableIdentifier,
        _table_uuid: &str,
    ) -> unity_catalog_delta_client_api::Result<CommitState> {
        let result: crate::error::Result<LoadTableResponse> = async {
            let path = table_path(&target.catalog, &target.schema, &target.table);
            let response = self
                .http_client
                .get(self.base_url.join(&path)?)
                .send()
                .await?;
            handle_response(response).await
        }
        .await;
        let response = result.map_err(unity_catalog_delta_client_api::Error::from)?;
        let latest_version = response.latest_table_version.ok_or_else(|| {
            unity_catalog_delta_client_api::Error::Generic(
                "Catalog commit-history response omitted latest-table-version".to_string(),
            )
        })?;
        Ok(CommitState {
            table_uuid: response.metadata.table_uuid,
            latest_version,
            commits: response.commits,
        })
    }

    #[instrument(skip(self))]
    async fn update_table(
        &self,
        target: &TableIdentifier,
        request: UpdateTableRequest,
    ) -> unity_catalog_delta_client_api::Result<()> {
        let result: crate::error::Result<()> = async {
            let path = table_path(&target.catalog, &target.schema, &target.table);
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpSocket};
    use tokio::sync::oneshot;
    use unity_catalog_delta_client_api::{DeltaTableRequirement, Error};

    use super::*;

    fn request() -> UpdateTableRequest {
        UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertTableUuid {
                uuid: "test".into(),
            }],
            vec![],
        )
        .unwrap()
    }

    fn client(address: std::net::SocketAddr) -> UCUpdateTableRestClient {
        let config = ClientConfig::build(format!("http://{address}"), "unused")
            .build()
            .unwrap();
        let http_client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        UCUpdateTableRestClient::with_http_client(http_client, config)
    }

    #[tokio::test]
    async fn connection_refused_before_sending_the_request_is_retryable() {
        // Reserve the port without listening so another test cannot claim it before connect.
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let result = client(socket.local_addr().unwrap())
            .update_table(
                &TableIdentifier::new("catalog", "schema", "table"),
                request(),
            )
            .await;
        drop(socket);
        assert!(matches!(result, Err(Error::Retryable(_))));
    }

    #[tokio::test]
    async fn timeout_after_sending_the_request_does_not_claim_a_retryable_rejection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen, received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.read_u8().await.unwrap();
            request_seen.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(stream);
        });
        let result = client(address)
            .update_table(
                &TableIdentifier::new("catalog", "schema", "table"),
                request(),
            )
            .await;
        let request_was_sent = received.await.is_ok();
        server.abort();
        let _ = server.await;
        assert!(request_was_sent);
        assert!(matches!(result, Err(Error::Generic(_))));
    }
}
