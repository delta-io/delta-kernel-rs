use unity_catalog_delta_client_api::Operation;

#[test]
fn test_operation_display() {
    assert_eq!(Operation::Read.to_string(), "READ");
    assert_eq!(Operation::ReadWrite.to_string(), "READ_WRITE");
}
