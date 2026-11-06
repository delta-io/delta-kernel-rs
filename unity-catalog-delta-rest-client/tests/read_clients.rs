use rstest::rstest;
use serde_json::json;
use unity_catalog_delta_client_api::{
    Error, GetCredentialsClient, LoadTableClient, Operation, TableIdentifier,
};
use unity_catalog_delta_rest_client::{ClientConfig, UCDeltaTableClient};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TABLE_PATH: &str = concat!(
    "/api/2.1/unity-catalog/delta/v1/catalogs/main%2Fcatalog/",
    "schemas/my%20schema/tables/t%3F%25",
);

#[tokio::test]
async fn load_table_trait_preserves_metadata_and_unpublished_commits() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(TABLE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "metadata": {
                "etag": "etag-1",
                "table-type": "MANAGED",
                "table-uuid": "table-id",
                "location": "s3://bucket/table/",
                "created-time": 1,
                "updated-time": 2,
                "columns": {"type": "struct", "fields": []},
                "properties": {"delta.enableDeletionVectors": "true"},
                "last-commit-version": 4
            },
            "commits": [{
                "version": 5,
                "timestamp": 3,
                "file-name": "commit.json",
                "file-size": 100,
                "file-modification-timestamp": 3
            }],
            "latest-table-version": 5
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = client(&server);
    let target = TableIdentifier::new("main/catalog", "my schema", "t?%");
    let response = LoadTableClient::load_table(&client, &target).await.unwrap();

    assert_eq!(response.metadata.table_uuid, "table-id");
    assert_eq!(response.metadata.location, "s3://bucket/table/");
    assert_eq!(response.metadata.last_commit_version, Some(4));
    assert_eq!(
        response.metadata.properties["delta.enableDeletionVectors"],
        "true"
    );
    assert_eq!(response.latest_table_version, Some(5));
    assert_eq!(response.commits.len(), 1);
    assert_eq!(response.commits[0].version, 5);
    assert_eq!(response.commits[0].file_name, "commit.json");
}

#[rstest]
#[case::read(Operation::Read, "READ")]
#[case::read_write(Operation::ReadWrite, "READ_WRITE")]
#[tokio::test]
async fn credentials_trait_preserves_operation_and_storage_scope(
    #[case] operation: Operation,
    #[case] wire_operation: &str,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{TABLE_PATH}/credentials")))
        .and(query_param("operation", wire_operation))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "storage-credentials": [{
                "prefix": "s3://bucket/table/",
                "operation": wire_operation,
                "expiration-time-ms": 123,
                "config": {"s3.region": "us-west-2"}
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = client(&server);
    let target = TableIdentifier::new("main/catalog", "my schema", "t?%");
    let response = GetCredentialsClient::get_table_credentials(&client, &target, operation)
        .await
        .unwrap();

    assert_eq!(response.storage_credentials.len(), 1);
    let credential = &response.storage_credentials[0];
    assert_eq!(credential.operation, operation);
    assert_eq!(credential.prefix, "s3://bucket/table/");
    assert_eq!(credential.expiration_time_ms, Some(123));
    assert_eq!(credential.config["s3.region"], "us-west-2");
}

#[rstest]
#[case::load_table(false)]
#[case::credentials(true)]
#[tokio::test]
async fn read_traits_return_api_errors(
    #[case] credentials: bool,
    #[values(200, 401, 404, 500)] status: u16,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status).set_body_string("not JSON"))
        .expect(1)
        .mount(&server)
        .await;

    let client = client(&server);
    let target = TableIdentifier::new("main", "default", "table");
    let result = if credentials {
        GetCredentialsClient::get_table_credentials(&client, &target, Operation::Read)
            .await
            .map(|_| ())
    } else {
        LoadTableClient::load_table(&client, &target)
            .await
            .map(|_| ())
    };
    let error = result.unwrap_err();

    match (status, credentials) {
        (401, _) => assert!(matches!(error, Error::AuthenticationFailed)),
        (404, false) => {
            assert!(matches!(error, Error::TableNotFound(name) if name == "main.default.table"));
        }
        _ => assert!(matches!(error, Error::Generic(message) if !message.is_empty())),
    }
}

fn client(server: &MockServer) -> UCDeltaTableClient {
    let config = ClientConfig::build(server.uri(), "test-token")
        .with_max_retries(0)
        .build()
        .unwrap();
    UCDeltaTableClient::new(config).unwrap()
}
