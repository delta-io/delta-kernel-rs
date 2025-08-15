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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
