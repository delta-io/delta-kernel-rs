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
    pub fn timestamp_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.timestamp)
    }

    pub fn file_modification_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.file_modification_timestamp)
    }
}
