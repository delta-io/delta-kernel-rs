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

use delta_kernel_workloads::models::{Spec, TableInfo};
use url::Url;

/// Result of loading a workload specification.
#[derive(Debug)]
pub enum LoadedTestCase {
    /// A read or snapshot workload supported by this harness.
    Supported(Box<TestCase>),
    /// A workload operation that this harness does not execute.
    Unsupported(String),
}

/// A fully resolved test case ready for execution.
#[derive(Debug)]
pub struct TestCase {
    /// Table metadata (absent if `table_info.json` doesn't exist). This
    /// occurs when the table is corrupt and used for error testing.
    pub table_info: Option<TableInfo>,
    /// Root directory of the test case
    pub root_dir: PathBuf,
    /// The workload specification
    pub spec: Spec,
    /// Name of the workload (spec filename without extension)
    pub workload_name: String,
}

impl TestCase {
    /// Load a test case from a spec file path.
    ///
    /// Given a path like `.../workloads/<test_case>/specs/<workload>.json`,
    /// reads the JSON once and resolves the test case root directory. Unsupported workload
    /// operations are returned without attempting to deserialize them as read or snapshot specs.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is malformed, the file cannot be read, the JSON does not have
    /// a string `type`, or a supported workload cannot be deserialized.
    pub fn load(spec_path: impl AsRef<Path>) -> Result<LoadedTestCase, String> {
        let spec_path = spec_path.as_ref();

        let workload_name = spec_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or("Invalid spec path: missing filename")?
            .to_string();

        let root_dir = spec_path
            .parent() // specs/
            .and_then(|p| p.parent()) // test_case/
            .ok_or("Invalid spec path: must be <test_case>/specs/<workload>.json")?
            .to_path_buf();

        let content = std::fs::read_to_string(spec_path).map_err(|error| {
            format!("Failed to read spec file {}: {error}", spec_path.display())
        })?;
        let value: serde_json::Value = serde_json::from_str(&content).map_err(|error| {
            format!("Failed to parse spec file {}: {error}", spec_path.display())
        })?;
        let spec_type = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                format!(
                    "Workload spec '{}' is missing a string 'type' field",
                    spec_path.display()
                )
            })?;
        if !is_supported_spec_type(spec_type) {
            return Ok(LoadedTestCase::Unsupported(spec_type.to_string()));
        }
        let spec: Spec = serde_json::from_value(value).map_err(|error| {
            format!(
                "Failed to deserialize spec file {}: {error}",
                spec_path.display()
            )
        })?;

        Ok(LoadedTestCase::Supported(Box::new(Self {
            table_info: None,
            root_dir,
            spec,
            workload_name,
        })))
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

    /// Path to the expected results directory for this workload.
    pub fn expected_dir(&self) -> PathBuf {
        self.root_dir.join("expected").join(&self.workload_name)
    }
}

fn is_supported_spec_type(spec_type: &str) -> bool {
    matches!(
        spec_type,
        "read" | "snapshot" | "snapshotConstruction" | "snapshot_construction"
    )
}

/// Return a spec's extension-free path relative to the corpus root.
///
/// # Errors
///
/// Returns an error if `spec_path` is outside `corpus_root`, has no extension, or is not valid
/// UTF-8.
pub fn corpus_relative_spec_id(spec_path: &Path, corpus_root: &Path) -> Result<String, String> {
    let relative = spec_path.strip_prefix(corpus_root).map_err(|_| {
        format!(
            "Spec path '{}' is outside corpus root '{}'",
            spec_path.display(),
            corpus_root.display()
        )
    })?;
    let without_extension = relative.with_extension("");
    if without_extension == relative {
        return Err(format!(
            "Spec path '{}' has no extension",
            spec_path.display()
        ));
    }
    without_extension
        .to_str()
        .map(|path| path.replace('\\', "/"))
        .ok_or_else(|| format!("Spec path '{}' is not valid UTF-8", spec_path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use delta_kernel_workloads::models::Spec;

    use super::{corpus_relative_spec_id, is_supported_spec_type};

    #[test]
    fn corpus_spec_id_is_exact_and_extension_free() {
        let root = Path::new("/tmp/corpus");
        let spec = root.join("table/specs/read_all.json");

        assert_eq!(
            corpus_relative_spec_id(&spec, root).unwrap(),
            "table/specs/read_all"
        );
        assert_ne!(
            corpus_relative_spec_id(&root.join("table/specs/read_all_extra.json"), root).unwrap(),
            "table/specs/read_all"
        );
    }

    #[test]
    fn corpus_spec_id_rejects_paths_outside_root() {
        assert!(corpus_relative_spec_id(
            Path::new("/tmp/other/table/specs/read.json"),
            Path::new("/tmp/corpus")
        )
        .is_err());
    }

    #[test]
    fn loader_accepts_all_snapshot_spec_aliases() {
        for spec_type in ["snapshot", "snapshotConstruction", "snapshot_construction"] {
            assert!(is_supported_spec_type(spec_type));
            let spec: Spec = serde_json::from_value(serde_json::json!({ "type": spec_type }))
                .expect("snapshot alias should deserialize");
            assert!(matches!(spec, Spec::SnapshotConstruction(_)));
        }
    }

    #[test]
    fn loader_does_not_treat_other_operations_as_supported() {
        for spec_type in ["cdf", "checkpoint", "crc", "write", "unknown"] {
            assert!(!is_supported_spec_type(spec_type));
        }
    }
}
