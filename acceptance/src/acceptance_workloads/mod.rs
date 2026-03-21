//! Delta workload specification framework.
//!
//! Provides shared types and loading logic for correctness testing and benchmarking
//! workloads. All workload specs follow a flat file layout:
//!
//! ```text
//! <test_case>/
//!   table_info.json           # Table metadata (optional)
//!   delta/                    # The Delta table
//!     _delta_log/
//!   specs/                    # Workload specifications (flat files)
//!     workload_a.json
//!     workload_b.json
//!   expected/                 # Expected results
//!     workload_a/
//!       expected_data/        # Expected output as Parquet files
//!         part-00000.parquet
//!         part-00001.parquet
//!       expected_metadata/    # Expected add files after data skipping as Parquet files
//!         part-aaaaa.parquet
//!         part-bbbbb.parquet
//!
//! ```
//!
//! ## Expected Data Format
//!
//! The `expected_data/` directory contains one or more Parquet files representing the
//! expected table content after executing the workload. Files starting with `.` or `_`
//! are ignored (e.g., `_SUCCESS`, `.crc` files). The data is compared order-independently
//! against the actual scan output by sorting rows before comparison.
//!
//! ## Read Spec Format
//!
//! Read workloads execute a scan and compare results against expected data:
//!
//! ```json
//! {
//!   "type": "read",
//!   "version": 5,                // optional: time travel to version
//!   "predicate": "id > 10",      // optional: filter predicate
//!   "columns": ["id", "name"],   // optional: column projection
//!   "expected": { "rowCount": 100 }
//! }
//! ```
//!
//! For error cases, use `"error"` instead of `"expected"`:
//!
//! ```json
//! {
//!   "type": "read",
//!   "error": { "errorCode": "TABLE_NOT_FOUND" }
//! }
//! ```
//!
//! ## Snapshot Construction Spec Format
//!
//! Snapshot workloads verify that a snapshot can be constructed and its metadata matches:
//!
//! ```json
//! {
//!   "type": "snapshotConstruction",
//!   "version": 5,                // optional: time travel to version
//!   "expected": {
//!     "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 },
//!     "metadata": { "id": "...", "schemaString": "...", ... }
//!   }
//! }
//! ```
//!
//! For error cases:
//!
//! ```json
//! {
//!   "type": "snapshotConstruction",
//!   "error": { "errorCode": "INVALID_TABLE" }
//! }
//! ```

pub mod validation;
pub mod workload;

use std::path::{Path, PathBuf};

use delta_kernel_benchmarks::models::TableInfo;
use url::Url;

/// A fully resolved test case ready for execution.
#[derive(Debug)]
pub struct TestCase {
    /// Table metadata (absent if `table_info.json` doesn't exist). This
    /// occurs when the table is corrupt and used for error testing.
    pub table_info: Option<TableInfo>,
    /// Root directory of the test case
    pub root_dir: PathBuf,
}

impl TestCase {
    /// Load a test case from a directory. `table_info.json` is loaded if present
    /// but not required.
    pub fn from_dir(root_dir: impl AsRef<Path>) -> Result<Self, String> {
        let root_dir = root_dir.as_ref().to_path_buf();

        let path = root_dir.join("table_info.json");

        let table_info = if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read table_info.json: {}", e))?;
            let mut info: TableInfo = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse table_info.json: {}", e))?;
            info.table_info_dir = root_dir.clone();
            Some(info)
        } else {
            None
        };

        Ok(Self {
            table_info,
            root_dir,
        })
    }

    /// URL to the Delta table directory.
    ///
    /// Uses `table_info.table_path` if available, otherwise defaults to
    /// `<root_dir>/delta`.
    pub fn table_root(&self) -> Result<Url, String> {
        match self.table_info.as_ref() {
            Some(table_info) => Ok(table_info.resolved_table_root()),
            None => Url::from_directory_path(self.root_dir.join("delta"))
                .map_err(|_| format!("Failed to construct table root for {:?}", self.root_dir)),
        }
    }

    /// Path to the expected results directory for a given workload.
    pub fn expected_dir(&self, workload_name: &str) -> PathBuf {
        self.root_dir.join("expected").join(workload_name)
    }
}

/// Resolve a test case from a spec file path (for datatest_stable integration).
///
/// Given a path like `.../workloads/<test_case>/specs/<workload>.json`,
/// walks up to find the test case root directory.
pub fn test_case_from_spec_path(spec_path: &Path) -> Result<(TestCase, String), String> {
    let spec_path = std::fs::canonicalize(spec_path).unwrap_or_else(|_| spec_path.to_path_buf());

    let workload_name = spec_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Walk up: <workload>.json -> specs/ -> <workload_directory>
    let test_case_dir = spec_path
        .parent() // specs/
        .and_then(|p| p.parent()) // worklod_directory/
        .ok_or_else(|| format!("Cannot find test case dir from: {}", spec_path.display()))?;

    let test_case = TestCase::from_dir(test_case_dir)?;
    Ok((test_case, workload_name))
}
