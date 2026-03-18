use serde::Serialize;
use unitycatalog_client_api::Operation;

/// The HTTP request body for the temporary credentials endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct CredentialsRequest {
    pub table_id: String,
    pub operation: Operation,
}

impl CredentialsRequest {
    pub fn new(table_id: impl Into<String>, operation: Operation) -> Self {
        Self {
            table_id: table_id.into(),
            operation,
        }
    }
}
