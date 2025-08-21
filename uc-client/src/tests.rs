use crate::models::commits::CommitsRequest;
use crate::models::credentials::Operation;

#[test]
fn test_commits_request_builder() {
    let request = CommitsRequest::new("test-id", "test-uri")
        .with_start_version(1)
        .with_end_version(10);

    assert_eq!(request.table_id, "test-id");
    assert_eq!(request.table_uri, "test-uri");
    assert_eq!(request.start_version, Some(1));
    assert_eq!(request.end_version, Some(10));
}

#[test]
fn test_operation_display() {
    assert_eq!(Operation::Read.to_string(), "READ");
    assert_eq!(Operation::Write.to_string(), "WRITE");
    assert_eq!(Operation::ReadWrite.to_string(), "READ_WRITE");
}

#[test]
fn test_table_response_helpers() {
    use crate::models::tables::TablesResponse;
    use std::collections::HashMap;

    let table = TablesResponse {
        name: "my_table".to_string(),
        catalog_name: "catalog".to_string(),
        schema_name: "schema".to_string(),
        table_type: "MANAGED".to_string(),
        data_source_format: "DELTA".to_string(),
        storage_location: "/path/to/table".to_string(),
        owner: "user".to_string(),
        properties: HashMap::new(),
        securable_kind: "TABLE".to_string(),
        metastore_id: "metastore-id".to_string(),
        table_id: "table-id".to_string(),
        schema_id: "schema-id".to_string(),
        catalog_id: "catalog-id".to_string(),
    };

    assert_eq!(table.full_name(), "catalog.schema.my_table");
    assert!(table.is_delta_table());
    assert!(table.is_managed_table());
    assert!(!table.is_external_table());
}
