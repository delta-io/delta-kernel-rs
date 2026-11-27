use unity_catalog_delta_client_api::{CreateStagingTableRequest, Operation};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::{ClientConfig, Error, UCDeltaTableClient};

#[test]
fn test_operation_display() {
    assert_eq!(Operation::Read.to_string(), "READ");
    assert_eq!(Operation::ReadWrite.to_string(), "READ_WRITE");
}

#[tokio::test]
async fn create_staging_table_preserves_resource_exhaustion_error() {
    let server = MockServer::start().await;
    let body = r#"{"error":{"type":"ResourceExhaustedException","code":429,"message":"Full"}}"#;
    Mock::given(method("POST"))
        .and(path(
            "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/staging-tables",
        ))
        .respond_with(ResponseTemplate::new(429).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let config = ClientConfig::build(server.uri(), "test-token")
        .build()
        .unwrap();
    let client = UCDeltaTableClient::new(config).unwrap();
    let error = client
        .create_staging_table(
            "main",
            "default",
            CreateStagingTableRequest {
                name: "table".to_string(),
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        Error::HttpStatusError { status: 429, message } if message == body
    ));
}
