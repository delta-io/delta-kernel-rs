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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_disown_commit: Option<bool>,
}

impl Commit {
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
            is_disown_commit: Some(false),
        }
    }

    pub fn with_disown_commit(mut self, disown: bool) -> Self {
        self.is_disown_commit = Some(disown);
        self
    }

    pub fn timestamp_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.timestamp)
    }

    pub fn file_modification_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.file_modification_timestamp)
    }
}

// Structs for creating a new commit
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

    pub fn ack_publish(
        table_id: impl Into<String>,
        table_uri: impl Into<String>,
        last_backfilled_version: i64,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            table_uri: table_uri.into(),
            commit_info: None,
            latest_backfilled_version: Some(last_backfilled_version),
            metadata: None,
            protocol: None,
        }
    }

    pub fn with_latest_backfilled_version(mut self, version: i64) -> Self {
        self.latest_backfilled_version = Some(version);
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_protocol(mut self, protocol: serde_json::Value) -> Self {
        self.protocol = Some(protocol);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResponse {}
