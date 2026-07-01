//! Credential-vending wire types for the UC API.
//!
//! These mirror the `CredentialsResponse` / `StorageCredential` /
//! `Operation` schemas in the OpenAPI spec.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Operation level for a vended storage credential. Sent as the `operation`
/// query parameter to credential-vending endpoints and echoed in
/// `StorageCredential::operation`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum Operation {
    /// Read-only access.
    #[serde(rename = "READ")]
    #[default]
    Read,
    /// Read + write access.
    #[serde(rename = "READ_WRITE")]
    ReadWrite,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Read => write!(f, "READ"),
            Operation::ReadWrite => write!(f, "READ_WRITE"),
        }
    }
}

/// Response from a credential-vending endpoint.
///
/// Always returns a list of `StorageCredential`s; the client picks the one
/// with the longest matching prefix when multiple are returned.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CredentialsResponse {
    /// Temporary cloud-storage credentials. Wire field: `storage-credentials`.
    #[serde(rename = "storage-credentials", default)]
    pub storage_credentials: Vec<StorageCredential>,
}

/// A single temporary cloud-storage credential scoped to a storage prefix.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageCredential {
    /// Storage path prefix this credential applies to, e.g.
    /// `"s3://bucket/path/"`.
    pub prefix: String,
    /// Permission level granted by this credential.
    pub operation: Operation,
    /// Credential expiration time in epoch milliseconds, or `None` if the credential does not
    /// expire (e.g. local `file://` credentials vended by a dev/OSS server). Wire field:
    /// `expiration-time-ms`.
    #[serde(rename = "expiration-time-ms", default)]
    pub expiration_time_ms: Option<i64>,
    /// Cloud provider-specific credential configuration (e.g.
    /// `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`,
    /// `azure.sas-token`, `gcs.oauth-token`).
    #[serde(default)]
    pub config: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_credential_decodes_null_expiration_as_none() {
        let json =
            r#"{"prefix":"file:///tmp/t/","operation":"READ_WRITE","expiration-time-ms":null}"#;
        let cred: StorageCredential = serde_json::from_str(json).unwrap();
        assert_eq!(cred.expiration_time_ms, None);
        assert_eq!(cred.operation, Operation::ReadWrite);
    }
}
