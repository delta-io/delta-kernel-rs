//! Unified types for Delta workload specifications.

use serde::Deserialize;
use std::path::PathBuf;

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
#[serde(rename_all = "camelCase")]
pub struct ExpectedError {
    pub error_code: String,
    #[serde(default)]
    pub error_message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedSummary {
    pub actual_row_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadExpected {
    pub row_count: u64,
}

/// Expected snapshot values. Uses `serde_json::Value` for protocol/metadata
/// to allow flexible comparison (set-based feature ordering, optional schema, etc.)
/// without duplicating kernel's Protocol/Metadata structs.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotExpected {
    #[serde(default)]
    pub protocol: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}
