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
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
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
        /// Inline expected values (row_count, file_count, files_skipped)
        #[serde(default, skip_serializing)]
        expected: Option<ReadExpected>,
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
        /// Inline expected protocol + metadata
        #[serde(default, skip_serializing)]
        expected: Option<SnapshotExpected>,
    },
    /// Transaction workload — validate SetTransaction
    Txn {
        #[serde(default)]
        version: Option<i64>,
        expected: ExpectedTxn,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        description: Option<String>,
    },
    /// Domain metadata workload
    DomainMetadata {
        #[serde(default)]
        version: Option<i64>,
        expected: ExpectedDomainMetadata,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        description: Option<String>,
    },
    /// CDF (Change Data Feed) workload — read table changes
    Cdf {
        #[serde(alias = "startVersion", alias = "start_version", default)]
        start_version: Option<i64>,
        #[serde(alias = "endVersion", alias = "end_version", default)]
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
            Self::Read { error, .. } | Self::Snapshot { error, .. } | Self::Cdf { error, .. } => {
                error.as_ref()
            }
            Self::Txn { .. } | Self::DomainMetadata { .. } => None,
        }
    }

    /// Whether this workload has a predicate filter
    pub fn has_predicate(&self) -> bool {
        match self {
            Self::Read { predicate, .. } => predicate.is_some(),
            _ => false,
        }
    }
}

// ── Expected-value types (correctness testing) ──────────────────────────────

/// Expected error specification
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedError {
    /// Error code (e.g. "DELTA_CDC_NOT_ALLOWED_ON_NON_CDC_TABLE")
    #[serde(alias = "errorCode", alias = "error_code")]
    pub error_code: String,
    /// Optional error message pattern
    #[serde(alias = "errorMessage", alias = "error_message", default)]
    pub error_message: Option<String>,
}

/// Expected transaction information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedTxn {
    #[serde(alias = "appId")]
    pub app_id: String,
    #[serde(alias = "txnVersion")]
    pub txn_version: i64,
    #[serde(alias = "lastUpdated", default)]
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

// ── Inline expected values (self-contained in spec JSON) ────────────────────

/// Inline expected values for read specs
#[derive(Debug, Clone, Deserialize)]
pub struct ReadExpected {
    pub row_count: u64,
    #[serde(default)]
    pub file_count: Option<u64>,
    #[serde(default)]
    pub files_skipped: Option<u64>,
}

/// Inline expected values for snapshot specs
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotExpected {
    #[serde(default)]
    pub protocol: Option<Protocol>,
    #[serde(default)]
    pub metadata: Option<Metadata>,
}

// ── Snapshot validation types ───────────────────────────────────────────────

/// Wrapper for `protocol.json`
#[derive(Debug, Deserialize)]
pub struct ProtocolWrapper {
    pub protocol: Protocol,
}

/// Protocol definition — shared between expected (from spec JSON) and actual (from kernel).
///
/// Custom `PartialEq` treats features as sets (order-independent) and
/// `None` == empty vec.
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

/// Wrapper for `metadata.json`
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataWrapper {
    pub meta_data: Metadata,
}

/// Format specification within metadata
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MetadataFormat {
    pub provider: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Metadata definition — shared between expected (from spec JSON) and actual (from kernel).
///
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

/// Compare schema strings as parsed JSON. `None` matches anything.
fn schemas_match(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => {
            let parsed_a: Result<serde_json::Value, _> = serde_json::from_str(a);
            let parsed_b: Result<serde_json::Value, _> = serde_json::from_str(b);
            match (parsed_a, parsed_b) {
                (Ok(va), Ok(vb)) => va == vb,
                _ => a == b, // fallback to string comparison if parse fails
            }
        }
        (None, _) | (_, None) => true, // None means "don't check"
    }
}
