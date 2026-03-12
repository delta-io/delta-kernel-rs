//! Delta workload specification framework.
//!
//! Provides shared types and loading logic for both correctness testing
//! and benchmarking workloads. Workload specs follow a common
//! directory layout:
//!
//! ```text
//! <test_case>/
//!   table_info.json
//!   delta/                    # The Delta table
//!     _delta_log/
//!   specs/                    # Workload specifications
//!     workload_a.json         # flat file layout (correctness)
//!     workload_b/spec.json    # subdirectory layout (benchmarking)
//!   expected/                 # Expected results (correctness only)
//!     workload_a/
//!       summary.json
//!       expected_data/
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
    /// occurs when a table is corrupt table used for testing.
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

        let mut table_info = None;
        if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read table_info.json: {}", e))?;
            let mut info: TableInfo = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse table_info.json: {}", e))?;
            info.table_info_dir = root_dir.clone();
            table_info = Some(info)
        }

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
        let table_path = self
            .table_info
            .as_ref()
            .map(|ti| ti.resolved_table_root())
            .unwrap_or_else(|| self.root_dir.join("delta").to_string_lossy().to_string());
        // If it looks like a URL already (s3://, etc.), parse directly
        if table_path.contains("://") {
            Url::parse(&table_path).map_err(|e| format!("Invalid table URL: {}", e))
        } else {
            let abs = if Path::new(&table_path).is_absolute() {
                PathBuf::from(&table_path)
            } else {
                self.root_dir.join(&table_path)
            };
            Url::from_directory_path(&abs)
                .map_err(|_| format!("Failed to create URL from path: {}", abs.display()))
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

    // Walk up: <workload>.json -> specs/ -> <test_case>/
    let test_case_dir = spec_path
        .parent() // specs/
        .and_then(|p| p.parent()) // test_case/
        .ok_or_else(|| format!("Cannot find test case dir from: {}", spec_path.display()))?;

    let test_case = TestCase::from_dir(test_case_dir)?;
    Ok((test_case, workload_name))
}
