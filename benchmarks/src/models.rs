//! Data models for workload specifications

use delta_kernel::actions::{Metadata, Protocol};
use serde::Deserialize;
use std::path::{Path, PathBuf};

// ── Table info ──────────────────────────────────────────────────────────────

/// Table metadata loaded from `table_info.json`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    pub name: String,
    pub description: Option<String>,
    #[serde(alias = "table_root_path")]
    pub table_path: Option<String>,
    #[serde(skip, default)]
    pub table_info_dir: PathBuf,
}

impl TableInfo {
    pub fn resolved_table_root(&self) -> String {
        self.table_path.clone().unwrap_or_else(|| {
            self.table_info_dir
                .join("delta")
                .to_string_lossy()
                .to_string()
        })
    }

    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        let mut table_info: TableInfo = serde_json::from_str(&content)?;
        if let Some(parent) = path.as_ref().parent() {
            table_info.table_info_dir = parent.to_path_buf();
        }
        Ok(table_info)
    }
}

// ── Time travel ─────────────────────────────────────────────────────────────

/// Mutually exclusive version or timestamp for time travel queries.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum TimeTravel {
    Version { version: u64 },
    Timestamp { timestamp: String },
}

// ── Workload specification ──────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Spec {
    Read(ReadSpec),
    #[serde(alias = "snapshot_construction")]
    Snapshot(Box<SnapshotSpec>),
}

impl Spec {
    pub fn type_name(&self) -> &'static str {
        match self {
            Spec::Read(_) => "read",
            Spec::Snapshot(_) => "snapshot_construction",
        }
    }

    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        serde_json::from_str(&content)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSpec {
    #[serde(flatten)]
    pub time_travel: Option<TimeTravel>,
    #[serde(default)]
    pub predicate: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(flatten)]
    pub expected: Option<ReadExpected>,
}

impl ReadSpec {
    pub fn type_name(&self) -> &'static str {
        "read"
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSpec {
    #[serde(flatten)]
    pub time_travel: Option<TimeTravel>,
    #[serde(flatten)]
    pub expected: Option<SnapshotExpected>,
}

impl SnapshotSpec {
    pub fn type_name(&self) -> &'static str {
        "snapshot_construction"
    }
}

// ── Expected-value types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedError {
    pub error_code: String,
    #[serde(default)]
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSuccess {
    pub row_count: u64,
}

/// Expected snapshot values using kernel's Protocol and Metadata types.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSuccess {
    pub protocol: Protocol,
    pub metadata: Metadata,
}

/// Expected outcome: either success with expected values, or an expected error.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ReadExpected {
    Success { expected: ReadSuccess },
    Error { error: ExpectedError },
}

/// Expected outcome for snapshot: either success or error.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SnapshotExpected {
    Success { expected: Box<SnapshotSuccess> },
    Error { error: ExpectedError },
}

// ── Benchmark-specific types ────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ReadConfig {
    pub name: String,
    pub parallel_scan: ParallelScan,
}

pub fn default_read_configs() -> Vec<ReadConfig> {
    vec![ReadConfig {
        name: "serial".into(),
        parallel_scan: ParallelScan::Disabled,
    }]
}

#[derive(Clone, Debug)]
pub enum ParallelScan {
    Disabled,
    Enabled { num_threads: usize },
}

#[derive(Clone, Copy, Debug)]
pub enum ReadOperation {
    ReadData,
    ReadMetadata,
}

impl ReadOperation {
    pub fn as_str(&self) -> &str {
        match self {
            ReadOperation::ReadData => "read_data",
            ReadOperation::ReadMetadata => "read_metadata",
        }
    }
}

/// A workload combining table info, case name, and spec.
#[derive(Clone, Debug)]
pub struct Workload {
    pub table_info: TableInfo,
    pub case_name: String,
    pub spec: Spec,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        r#"{"name": "basic_append", "description": "A basic table with two append writes"}"#,
        "basic_append",
        Some("A basic table with two append writes")
    )]
    #[case(
        r#"{"name": "table_without_description"}"#,
        "table_without_description",
        None
    )]
    #[case(
       r#"{"name": "table_with_extra_fields", "description": "A table with extra fields", "extra_field": "should be ignored"}"#,
       "table_with_extra_fields",
       Some("A table with extra fields")
   )]
    fn test_deserialize_table_info(
        #[case] json: &str,
        #[case] expected_name: &str,
        #[case] expected_description: Option<&str>,
    ) {
        let table_info: TableInfo =
            serde_json::from_str(json).expect("Failed to deserialize table info");

        assert_eq!(table_info.name, expected_name);
        assert_eq!(table_info.description.as_deref(), expected_description);
    }

    #[rstest]
    #[case(
        r#"{"description": "A table missing the required name field"}"#,
        "missing field"
    )]
    fn test_deserialize_table_info_errors(#[case] json: &str, #[case] expected_msg: &str) {
        let error = serde_json::from_str::<TableInfo>(json).unwrap_err();
        assert!(error.to_string().contains(expected_msg));
    }

    #[rstest]
    #[case(r#"{"type": "read", "version": 5}"#, "read", Some(5))]
    #[case(r#"{"type": "read"}"#, "read", None)]
    #[case(
        r#"{"type": "read", "version": 7, "extra_field": "should be ignored"}"#,
        "read",
        Some(7)
    )]
    #[case(
        r#"{"type": "snapshot_construction", "version": 5}"#,
        "snapshot_construction",
        Some(5)
    )]
    #[case(r#"{"type": "snapshot_construction"}"#, "snapshot_construction", None)]
    #[case(
        r#"{"type": "snapshot_construction", "version": 7, "extra_field": "should be ignored"}"#,
        "snapshot_construction",
        Some(7)
    )]
    #[case(
        r#"{"type": "snapshot", "version": 3}"#,
        "snapshot_construction",
        Some(3)
    )]
    fn test_deserialize_spec(
        #[case] json: &str,
        #[case] expected_type: &str,
        #[case] expected_version: Option<u64>,
    ) {
        let spec: Spec = serde_json::from_str(json).expect("Failed to deserialize spec");
        assert_eq!(spec.type_name(), expected_type);
        let version = match &spec {
            Spec::Read(read_spec) => match &read_spec.time_travel {
                Some(TimeTravel::Version { version }) => Some(*version),
                _ => None,
            },
            Spec::Snapshot(snapshot_spec) => match &snapshot_spec.time_travel {
                Some(TimeTravel::Version { version }) => Some(*version),
                _ => None,
            },
        };

        assert_eq!(version, expected_version);
    }

    #[rstest]
    #[case(r#"{"version": 10}"#, "missing field")]
    #[case(r#"{"type": "write", "version": 3}"#, "unknown variant")]
    fn test_deserialize_spec_errors(#[case] json: &str, #[case] expected_msg: &str) {
        let error = serde_json::from_str::<Spec>(json).unwrap_err();
        assert!(error.to_string().contains(expected_msg));
    }

    #[rstest]
    #[case(
        r#"{"type": "read", "timestamp": "2024-01-01 00:00:00"}"#,
        "2024-01-01 00:00:00"
    )]
    fn test_deserialize_timestamp(#[case] json: &str, #[case] expected_ts: &str) {
        let spec: Spec = serde_json::from_str(json).expect("Failed to deserialize spec");
        match &spec {
            Spec::Read(read_spec) => match &read_spec.time_travel {
                Some(TimeTravel::Timestamp { timestamp }) => {
                    assert_eq!(timestamp, expected_ts);
                }
                _ => panic!("Expected timestamp time travel"),
            },
            _ => panic!("Expected read spec"),
        }
    }
}
