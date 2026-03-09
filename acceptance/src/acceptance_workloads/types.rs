//! Unified types for Delta workload specifications.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

// ── Table & test metadata ───────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TableInfo {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(alias = "table_root_path", default)]
    pub table_path: Option<String>,
}

impl TableInfo {
    /// Resolve table root path, falling back to `<dir>/delta` if `table_path` is absent.
    pub fn resolved_table_root(&self, dir: &Path) -> String {
        self.table_path
            .clone()
            .unwrap_or_else(|| dir.join("delta").to_string_lossy().to_string())
    }
}

// ── Workload specification ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkloadSpec {
    Read {
        #[serde(default)]
        version: Option<i64>,
        #[serde(default)]
        predicate: Option<String>,
        #[serde(default)]
        timestamp: Option<String>,
        #[serde(default)]
        columns: Option<Vec<String>>,
        #[serde(default)]
        error: Option<ExpectedError>,
        #[serde(default)]
        expected: Option<ReadExpected>,
    },
    #[serde(alias = "snapshot_construction")]
    Snapshot {
        #[serde(default)]
        version: Option<i64>,
        #[serde(default)]
        timestamp: Option<String>,
        #[serde(default)]
        error: Option<ExpectedError>,
        #[serde(default)]
        expected: Option<SnapshotExpected>,
    },
    /// Catch-all for workload types not supported in this build (txn, cdf, domain_metadata, etc.)
    #[serde(other)]
    Unsupported,
}

impl WorkloadSpec {
    pub fn expected_error(&self) -> Option<&ExpectedError> {
        match self {
            Self::Read { error, .. } | Self::Snapshot { error, .. } => error.as_ref(),
            Self::Unsupported => None,
        }
    }
}

// ── Expected-value types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedError {
    #[serde(alias = "errorCode", alias = "error_code")]
    pub error_code: String,
    #[serde(alias = "errorMessage", alias = "error_message", default)]
    pub error_message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExpectedSummary {
    pub actual_row_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadExpected {
    pub row_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotExpected {
    #[serde(default)]
    pub protocol: Option<Protocol>,
    #[serde(default)]
    pub metadata: Option<Metadata>,
}

// ── Snapshot validation types ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ProtocolWrapper {
    pub protocol: Protocol,
}

/// Custom `PartialEq` treats features as sets (order-independent).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    #[serde(default)]
    pub reader_features: Option<Vec<String>>,
    #[serde(default)]
    pub writer_features: Option<Vec<String>>,
}

impl PartialEq for Protocol {
    fn eq(&self, other: &Self) -> bool {
        self.min_reader_version == other.min_reader_version
            && self.min_writer_version == other.min_writer_version
            && sorted_features(&self.reader_features) == sorted_features(&other.reader_features)
            && sorted_features(&self.writer_features) == sorted_features(&other.writer_features)
    }
}

fn sorted_features(features: &Option<Vec<String>>) -> Vec<&str> {
    let mut v: Vec<&str> = features
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|s| s.as_str())
        .collect();
    v.sort();
    v
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataWrapper {
    pub meta_data: Metadata,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MetadataFormat {
    pub provider: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Custom `PartialEq` compares `schema_string` as parsed JSON (ignoring formatting),
/// skips `created_time`, and treats `None` schema as matching anything.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    pub format: MetadataFormat,
    #[serde(default)]
    pub schema_string: Option<String>,
    #[serde(default)]
    pub partition_columns: Vec<String>,
    #[serde(default)]
    pub configuration: HashMap<String, String>,
    #[serde(default)]
    pub created_time: Option<i64>,
}

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.format == other.format
            && schemas_match(&self.schema_string, &other.schema_string)
            && self.partition_columns == other.partition_columns
            && self.configuration == other.configuration
    }
}

fn schemas_match(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => {
            let parsed_a: Result<serde_json::Value, _> = serde_json::from_str(a);
            let parsed_b: Result<serde_json::Value, _> = serde_json::from_str(b);
            match (parsed_a, parsed_b) {
                (Ok(va), Ok(vb)) => va == vb,
                _ => a == b,
            }
        }
        (None, _) | (_, None) => true,
    }
}
