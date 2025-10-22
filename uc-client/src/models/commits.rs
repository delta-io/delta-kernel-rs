use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitsRequest {
    pub table_id: String,
    pub table_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_version: Option<i64>,
}

impl CommitsRequest {
    pub fn new(table_id: impl Into<String>, table_uri: impl Into<String>) -> Self {
        Self {
            table_id: table_id.into(),
            table_uri: table_uri.into(),
            start_version: None,
            end_version: None,
        }
    }

    pub fn with_start_version(mut self, version: i64) -> Self {
        self.start_version = Some(version);
        self
    }

    pub fn with_end_version(mut self, version: i64) -> Self {
        self.end_version = Some(version);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commits: Option<Vec<Commit>>,
    pub latest_table_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub version: i64,
    pub timestamp: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
}

impl Commit {
    /// Create a new commit to send to UC with the specified version and timestamp.
    pub fn new(
        version: i64,
        timestamp: i64,
        file_name: impl Into<String>,
        file_size: i64,
        file_modification_timestamp: i64,
    ) -> Self {
        Self {
            version,
            timestamp,
            file_name: file_name.into(),
            file_size,
            file_modification_timestamp,
        }
    }

    pub fn timestamp_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.timestamp)
    }

    pub fn file_modification_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.file_modification_timestamp)
    }
}

/// Request to commit a new version to the table. It must include either a `commit_info` or
/// `latest_backfilled_version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    pub table_id: String,
    pub table_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_info: Option<Commit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_backfilled_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<serde_json::Value>,
}

impl CommitRequest {
    pub fn new(
        table_id: impl Into<String>,
        table_uri: impl Into<String>,
        commit_info: Commit,
        latest_backfilled_version: Option<i64>,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            table_uri: table_uri.into(),
            commit_info: Some(commit_info),
            latest_backfilled_version,
            metadata: None,
            protocol: None,
        }
    }

    pub fn with_latest_backfilled_version(mut self, version: i64) -> Self {
        self.latest_backfilled_version = Some(version);
        self
    }

    // TODO: expose metadata/protocol (with_metadata, with_protocol)
}
