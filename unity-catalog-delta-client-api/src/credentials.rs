use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporaryTableCredentials {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_temp_credentials: Option<AwsTempCredentials>,
    pub expiration_time: i64,
    pub url: String,
}

impl TemporaryTableCredentials {
    pub fn expiration_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.expiration_time)
    }

    pub fn is_expired(&self) -> bool {
        // If we can't parse the timestamp, consider it expired for safety
        self.expiration_as_datetime()
            .is_none_or(|exp| exp < chrono::Utc::now())
    }

    pub fn time_until_expiry(&self) -> Option<chrono::Duration> {
        self.expiration_as_datetime()
            .map(|exp| exp - chrono::Utc::now())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsTempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Operation {
    Read,
    Write,
    #[serde(rename = "READ_WRITE")]
    ReadWrite,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Read => write!(f, "READ"),
            Operation::Write => write!(f, "WRITE"),
            Operation::ReadWrite => write!(f, "READ_WRITE"),
        }
    }
}

// === Delta-Tables API credential-vending wire types ===
//
// Served by `GET .../tables/{table}/credentials`.
// TODO: delete the legacy Delta-Commits `TemporaryTableCredentials` / `AwsTempCredentials` types
// above once the read path swaps onto the Delta-Tables API.

/// Response from a credential-vending endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialsResponse {
    #[serde(rename = "storage-credentials")]
    pub storage_credentials: Vec<StorageCredential>,
}

/// A single temporary cloud-storage credential scoped to a storage prefix.
#[derive(Clone, Serialize, Deserialize)]
pub struct StorageCredential {
    /// Storage path prefix this credential applies to, e.g. `"s3://bucket/path/"`.
    pub prefix: String,
    /// Permission level granted by this credential.
    pub operation: Operation,
    /// Credential expiration time in epoch milliseconds, or `None` when the server omits it.
    #[serde(rename = "expiration-time-ms")]
    pub expiration_time_ms: Option<i64>,
    /// Cloud provider-specific credential configuration (e.g. `s3.access-key-id`,
    /// `s3.secret-access-key`, `s3.session-token`, `azure.sas-token`, `gcs.oauth-token`).
    pub config: HashMap<String, String>,
}

// Manual `Debug` that redacts `config`: it holds live secrets (`s3.secret-access-key`,
// `azure.sas-token`, `gcs.oauth-token`) that must not leak into logs or error output.
impl std::fmt::Debug for StorageCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCredential")
            .field("prefix", &self.prefix)
            .field("operation", &self.operation)
            .field("expiration_time_ms", &self.expiration_time_ms)
            .field(
                "config",
                &format_args!("<{} redacted entries>", self.config.len()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_credential_decodes_empty_config_and_absent_expiration() {
        // Local `file://` storage: the server sends an empty `config` object (no cloud keys
        // apply) and omits the nullable `expiration-time-ms`.
        let json = r#"{"prefix":"file:///tmp/t/","operation":"READ_WRITE","config":{}}"#;
        let cred: StorageCredential = serde_json::from_str(json).unwrap();
        assert_eq!(cred.operation, Operation::ReadWrite);
        assert_eq!(cred.expiration_time_ms, None);
        assert!(cred.config.is_empty());
    }

    #[test]
    fn storage_credential_debug_redacts_config_secrets() {
        let cred = StorageCredential {
            prefix: "s3://b/t/".to_string(),
            operation: Operation::Read,
            expiration_time_ms: Some(123),
            config: HashMap::from([(
                "s3.secret-access-key".to_string(),
                "supersecret".to_string(),
            )]),
        };
        let debug = format!("{cred:?}");
        assert!(
            !debug.contains("supersecret"),
            "secret leaked in Debug: {debug}"
        );
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn credentials_response_decodes_populated_body() {
        let json = r#"{"storage-credentials":[{
            "prefix":"s3://b/t/",
            "operation":"READ",
            "expiration-time-ms":123,
            "config":{"s3.access-key-id":"ak","s3.secret-access-key":"sk"}
        }]}"#;
        let resp: CredentialsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.storage_credentials.len(), 1);
        let cred = &resp.storage_credentials[0];
        assert_eq!(cred.prefix, "s3://b/t/");
        assert_eq!(cred.operation, Operation::Read);
        assert_eq!(cred.expiration_time_ms, Some(123));
        assert_eq!(cred.config["s3.access-key-id"], "ak");
        assert_eq!(cred.config["s3.secret-access-key"], "sk");
    }
}
