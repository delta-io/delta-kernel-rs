//! Delta workload specification framework.
//!
//! Provides shared types and loading logic for both correctness testing
//! (improved_dat) and benchmarking workloads. Workload specs follow a common
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

pub mod types;
pub mod validation;
pub mod workload;

use std::path::{Path, PathBuf};

use url::Url;

use types::{TableInfo, WorkloadSpec};

/// A loaded workload: its name, parsed spec, and where to find expected results.
#[derive(Debug)]
pub struct LoadedWorkload {
    /// Workload name (derived from spec filename)
    pub name: String,
    /// Parsed workload specification
    pub spec: WorkloadSpec,
    /// Path to the spec file on disk
    pub spec_path: PathBuf,
}

/// A fully resolved test case ready for execution.
#[derive(Debug)]
pub struct TestCase {
    /// Table metadata (absent if `table_info.json` doesn't exist)
    pub table_info: Option<TableInfo>,
    /// Root directory of the test case
    pub root_dir: PathBuf,
}

impl TestCase {
    /// Load a test case from a directory. `table_info.json` is loaded if present
    /// but not required.
    pub fn from_dir(root_dir: impl AsRef<Path>) -> Result<Self, String> {
        let root_dir = root_dir.as_ref().to_path_buf();

        let table_info = {
            let path = root_dir.join("table_info.json");
            if path.exists() {
                Some(
                    TableInfo::from_json_path(&path)
                        .map_err(|e| format!("Failed to load table_info.json: {}", e))?,
                )
            } else {
                None
            }
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
        let table_path = self
            .table_info
            .as_ref()
            .map(|ti| ti.resolved_table_root())
            .unwrap_or_else(|| {
                self.root_dir
                    .join("delta")
                    .to_string_lossy()
                    .to_string()
            });
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

    /// Discover and load all workload specs in this test case.
    ///
    /// Supports both layouts:
    /// - Flat files: `specs/<name>.json`
    /// - Subdirectories: `specs/<name>/spec.json`
    pub fn load_workloads(&self) -> Result<Vec<LoadedWorkload>, String> {
        let specs_dir = self.root_dir.join("specs");
        if !specs_dir.exists() {
            return Ok(Vec::new());
        }

        let mut workloads = Vec::new();
        let entries = std::fs::read_dir(&specs_dir)
            .map_err(|e| format!("Cannot read specs dir: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Dir entry error: {}", e))?;
            let path = entry.path();

            let (name, spec_path) = if path.is_file()
                && path.extension().map_or(false, |e| e == "json")
            {
                // Flat layout: specs/<name>.json
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                (name, path)
            } else if path.is_dir() {
                // Subdirectory layout: specs/<name>/spec.json
                let spec_file = path.join("spec.json");
                if !spec_file.exists() {
                    continue;
                }
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                (name, spec_file)
            } else {
                continue;
            };

            let content = std::fs::read_to_string(&spec_path)
                .map_err(|e| format!("Cannot read {}: {}", spec_path.display(), e))?;
            let spec: WorkloadSpec = serde_json::from_str(&content)
                .map_err(|e| format!("Cannot parse {}: {}", spec_path.display(), e))?;

            workloads.push(LoadedWorkload {
                name,
                spec,
                spec_path,
            });
        }

        workloads.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(workloads)
    }
}

/// Discover all test cases under a root directory.
///
/// Scans for subdirectories containing `table_info.json`.
pub fn discover_test_cases(root: impl AsRef<Path>) -> Result<Vec<TestCase>, String> {
    let root = root.as_ref();
    if !root.is_dir() {
        return Err(format!("Not a directory: {}", root.display()));
    }

    let mut cases = Vec::new();
    let entries = std::fs::read_dir(root)
        .map_err(|e| format!("Cannot read {}: {}", root.display(), e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Dir entry error: {}", e))?;
        let path = entry.path();
        if path.is_dir() && path.join("table_info.json").exists() {
            cases.push(TestCase::from_dir(&path)?);
        }
    }

    cases.sort_by(|a, b| {
        let a_name = a.table_info.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        let b_name = b.table_info.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        a_name.cmp(b_name)
    });
    Ok(cases)
}

/// Resolve a test case from a spec file path (for datatest_stable integration).
///
/// Given a path like `.../improved_dat/<test_case>/specs/<workload>.json`,
/// walks up to find the test case root directory.
pub fn test_case_from_spec_path(spec_path: &Path) -> Result<(TestCase, String), String> {
    let spec_path = std::fs::canonicalize(spec_path)
        .unwrap_or_else(|_| spec_path.to_path_buf());

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
