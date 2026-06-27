//! Credential-vending wire types for the UC API.
//!
//! These mirror the `CredentialsResponse` / `StorageCredential` /
//! `CredentialOperation` schemas in the OpenAPI spec.

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

impl StorageCredential {
    /// Returns the expiration time as a `chrono::DateTime`, or `None` if the credential has no
    /// expiration or the timestamp does not parse.
    pub fn expiration_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.expiration_time_ms
            .and_then(chrono::DateTime::from_timestamp_millis)
    }

    /// Returns whether this credential has expired. A credential with no expiration never expires;
    /// a present-but-unparseable timestamp is conservatively treated as expired.
    pub fn is_expired(&self) -> bool {
        match self.expiration_time_ms {
            None => false,
            Some(_) => self
                .expiration_as_datetime()
                .is_none_or(|exp| exp < chrono::Utc::now()),
        }
    }

    /// Returns the duration until this credential expires, if computable.
    pub fn time_until_expiry(&self) -> Option<chrono::Duration> {
        self.expiration_as_datetime()
            .map(|exp| exp - chrono::Utc::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_credential_decodes_null_expiration_as_none_and_not_expired() {
        let json =
            r#"{"prefix":"file:///tmp/t/","operation":"READ_WRITE","expiration-time-ms":null}"#;
        let cred: StorageCredential = serde_json::from_str(json).unwrap();
        assert_eq!(cred.expiration_time_ms, None);
        assert_eq!(cred.expiration_as_datetime(), None);
        assert!(!cred.is_expired());
        assert_eq!(cred.time_until_expiry(), None);
    }

    #[test]
    fn storage_credential_decodes_missing_expiration_as_none() {
        let json = r#"{"prefix":"file:///tmp/t/","operation":"READ_WRITE"}"#;
        let cred: StorageCredential = serde_json::from_str(json).unwrap();
        assert_eq!(cred.expiration_time_ms, None);
        assert!(!cred.is_expired());
    }

    #[test]
    fn storage_credential_past_expiration_is_expired() {
        let json = r#"{"prefix":"s3://b/","operation":"READ_WRITE","expiration-time-ms":1}"#;
        let cred: StorageCredential = serde_json::from_str(json).unwrap();
        assert_eq!(cred.expiration_time_ms, Some(1));
        assert!(cred.is_expired());
    }

    #[test]
    fn storage_credential_future_expiration_is_not_expired() {
        let cred = StorageCredential {
            expiration_time_ms: Some(
                (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp_millis(),
            ),
            ..Default::default()
        };
        assert!(!cred.is_expired());
        assert!(cred
            .time_until_expiry()
            .is_some_and(|d| d > chrono::Duration::zero()));
        assert!(cred.expiration_as_datetime().is_some());
    }

    #[test]
    fn storage_credential_unparseable_expiration_is_expired() {
        let cred = StorageCredential {
            expiration_time_ms: Some(i64::MAX),
            ..Default::default()
        };
        assert_eq!(cred.expiration_as_datetime(), None);
        assert!(cred.is_expired());
    }
}
