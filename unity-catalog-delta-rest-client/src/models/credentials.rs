use serde::Serialize;
use unity_catalog_delta_client_api::Operation;

/// The HTTP request body for the temporary credentials endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct CredentialsRequest {
    pub table_id: String,
    pub operation: Operation,
}

impl CredentialsRequest {
    /// Create a new credentials request for the given table and operation.
    pub fn new(table_id: impl Into<String>, operation: Operation) -> Self {
        Self {
            table_id: table_id.into(),
            operation,
        }
    }
}
