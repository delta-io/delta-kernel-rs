//! Unified types for Delta workload specifications.
//!
//! These types support both correctness testing (improved_dat) and benchmarking workloads.
//! All fields that only apply to one use case are optional with serde defaults.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

// ── Table & test metadata ───────────────────────────────────────────────────

/// Table definition from `table_info.json`.
///
/// Shared between correctness tests and benchmarking. Fields specific to one
/// use-case are optional.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Table description
    #[serde(default)]
    pub description: String,

    // ── Correctness-test fields ──
    /// Final version of the table after all operations
    #[serde(default)]
    pub version: Option<i64>,
    /// List of SQL operations (CREATE, INSERT, ALTER, etc.)
    #[serde(default)]
    pub sql_operations: Vec<String>,

    // ── Benchmarking fields ──
    /// Engine that created the table (e.g. "Apache-Spark/3.5.1 Delta-Lake/3.1.0")
    #[serde(default)]
    pub engine_info: Option<String>,
    /// Explicit table path (local or S3). Also accepts `table_root_path` from
    /// the Java WorkloadSpecGenerator.
    #[serde(alias = "table_root_path", default)]
    pub table_path: Option<String>,
    /// Whether this table is managed by Unity Catalog
    #[serde(default)]
    pub is_catalog_managed: bool,
    /// Fully-qualified UC table name (e.g. "catalog.schema.table")
    #[serde(default)]
    pub uc_table_name: Option<String>,

    // ── Runtime state (not serialized) ──
    /// Directory containing this table_info.json (set at load time)
    #[serde(skip)]
    pub table_info_dir: PathBuf,
}

impl TableInfo {
    /// Resolve the table root path. Uses `table_path` if set, otherwise
    /// falls back to `<table_info_dir>/delta`.
    pub fn resolved_table_root(&self) -> String {
        self.table_path.clone().unwrap_or_else(|| {
            self.table_info_dir
                .join("delta")
                .to_string_lossy()
                .to_string()
        })
    }

    /// Load from a `table_info.json` file, setting `table_info_dir` to its parent.
    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content =
            std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        let mut info: Self = serde_json::from_str(&content)?;
        if let Some(parent) = path.as_ref().parent() {
            info.table_info_dir = parent.to_path_buf();
        }
        Ok(info)
    }
}

// ── Workload specification ──────────────────────────────────────────────────

/// Workload specification from `specs/*.json`.
///
/// The `type` field in JSON selects the variant. Both correctness and
/// benchmarking specs deserialize into this enum. Unsupported variants are
/// simply never matched by the consumer.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkloadSpec {
    /// Read workload — execute a scan and (optionally) validate data
    Read {
        #[serde(default)]
        version: Option<i64>,
        /// SQL predicate filter (e.g. "id = 2")
        #[serde(default)]
        predicate: Option<String>,
        /// Timestamp for time travel (format: "YYYY-MM-DD HH:MM:SS.mmm")
        #[serde(default)]
        timestamp: Option<String>,
        /// Column projection (e.g. ["id", "name"])
        #[serde(default)]
        columns: Option<Vec<String>>,
        /// Expected error if this workload should fail
        #[serde(default)]
        error: Option<ExpectedError>,
        /// Benchmarking: operation sub-type (e.g. "read_metadata", "read_data")
        #[serde(default)]
        operation_type: Option<String>,
    },
    /// Snapshot workload — construct a snapshot and validate metadata.
    /// Also accepts `"type": "snapshot_construction"` from benchmarking specs.
    #[serde(alias = "snapshot_construction")]
    Snapshot {
        #[serde(default)]
        version: Option<i64>,
        #[serde(default)]
        timestamp: Option<String>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        error: Option<ExpectedError>,
    },
    /// Transaction workload — validate SetTransaction
    Txn {
        #[serde(default)]
        version: Option<i64>,
        expected: ExpectedTxn,
        name: String,
        description: String,
    },
    /// Domain metadata workload
    DomainMetadata {
        #[serde(default)]
        version: Option<i64>,
        expected: ExpectedDomainMetadata,
        name: String,
        description: String,
    },
    /// CDF (Change Data Feed) workload — read table changes
    Cdf {
        start_version: i64,
        #[serde(default)]
        end_version: Option<i64>,
        #[serde(default)]
        predicate: Option<String>,
        #[serde(default)]
        error: Option<ExpectedError>,
    },
}

impl WorkloadSpec {
    /// Whether this workload expects an error outcome
    pub fn expects_error(&self) -> bool {
        self.expected_error().is_some()
    }

    /// The expected error, if any
    pub fn expected_error(&self) -> Option<&ExpectedError> {
        match self {
            Self::Read { error, .. }
            | Self::Snapshot { error, .. }
            | Self::Cdf { error, .. } => error.as_ref(),
            Self::Txn { .. } | Self::DomainMetadata { .. } => None,
        }
    }
}

// ── Expected-value types (correctness testing) ──────────────────────────────

/// Expected error specification
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedError {
    /// Error code (e.g. "DELTA_CDC_NOT_ALLOWED_ON_NON_CDC_TABLE")
    pub error_code: String,
    /// Optional error message pattern
    #[serde(default)]
    pub error_message: Option<String>,
}

/// Expected transaction information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedTxn {
    pub app_id: String,
    pub txn_version: i64,
    #[serde(default)]
    pub last_updated: Option<i64>,
}

/// Expected domain metadata
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedDomainMetadata {
    pub domain: String,
    pub configuration: String,
    pub removed: bool,
}

// ── Read-result validation types ────────────────────────────────────────────

/// Summary of expected read results from `summary.json`
#[derive(Debug, Deserialize)]
pub struct ExpectedSummary {
    pub actual_row_count: u64,
    #[serde(default)]
    pub file_count: Option<u32>,
    #[serde(default)]
    pub expected_row_count: Option<u64>,
    #[serde(default)]
    pub matches_expected: Option<bool>,
}

// ── Snapshot validation types ───────────────────────────────────────────────

/// Wrapper for `protocol.json`
#[derive(Debug, Deserialize)]
pub struct ProtocolWrapper {
    pub protocol: ExpectedProtocol,
}

/// Expected protocol definition
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedProtocol {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    #[serde(default)]
    pub reader_features: Option<Vec<String>>,
    #[serde(default)]
    pub writer_features: Option<Vec<String>>,
}

/// Wrapper for `metadata.json`
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataWrapper {
    pub meta_data: ExpectedMetadata,
}

/// Format specification within metadata
#[derive(Debug, Deserialize)]
pub struct MetadataFormat {
    pub provider: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Expected metadata definition
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedMetadata {
    pub id: String,
    pub format: MetadataFormat,
    pub schema_string: String,
    #[serde(default)]
    pub partition_columns: Vec<String>,
    #[serde(default)]
    pub configuration: HashMap<String, String>,
    #[serde(default)]
    pub created_time: Option<i64>,
}

/// Actual metadata match result from `actual_meta.json`
#[derive(Debug, Deserialize)]
pub struct ActualMeta {
    pub actual_row_count: u64,
    pub matches_expected: bool,
}
