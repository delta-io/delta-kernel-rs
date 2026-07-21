//! Benchmark harness-config registry.
//!
//! Defines benchmark harness parameters and maps workloads to the configurations used to run them.
//! These parameters control how a benchmark runs rather than what operation it performs.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use delta_kernel_workloads::models::{Spec, Workload};
use serde::Deserialize;

// === Harness configs ===

/// A read-operation config: benchmark parameters not carried in the spec JSON. Deserialized
/// directly from a `bench-registry.json` entry.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadConfig {
    pub name: String,
    pub parallel_scan: ParallelScan,
}

/// The built-in default read configs (a single serial config).
pub fn default_read_configs() -> Vec<ReadConfig> {
    vec![ReadConfig {
        name: "serial".into(),
        parallel_scan: ParallelScan::Disabled,
    }]
}

/// Scan parallelism configuration.
///
/// In registry JSON, disabled scans use the string `"disabled"`. Enabled scans use an object that
/// specifies the thread count: `{ "enabled": { "numThreads": <n> } }`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ParallelScan {
    Disabled,
    Enabled { num_threads: usize },
}

// === Registry ===

/// Error type for registry loading and lookup.
pub type RegistryResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Read harness configs keyed by table directory name and workload spec filename.
///
/// For example, this entry runs `readMetadataLatest` against the table in the
/// `10kAdds0CommitsSinceChkpt1V2Chkpt` directory once serially and once with two scan threads:
///
/// ```json
/// {
///   "10kAdds0CommitsSinceChkpt1V2Chkpt": {
///     "readMetadataLatest": [
///       { "name": "serial", "parallelScan": "disabled" },
///       { "name": "parallel2", "parallelScan": { "enabled": { "numThreads": 2 } } }
///     ]
///   }
/// }
/// ```
///
/// Each configuration creates a separate Criterion benchmark whose final name segment is the
/// configuration `name`. Read workloads absent from the registry use the built-in serial config.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(transparent)]
pub struct BenchRegistry {
    tables: HashMap<String, HashMap<String, Vec<ReadConfig>>>,
}

impl BenchRegistry {
    /// Loads a registry from `path`.
    ///
    /// Returns an error when the file cannot be read or deserialized. Call [`Self::validate`]
    /// before using the registry.
    pub fn load_from_path(path: &Path) -> RegistryResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("reading registry {}: {e}", path.display()))?;
        serde_json::from_str(&content)
            .map_err(|e| format!("parsing registry {}: {e}", path.display()).into())
    }

    /// Returns the registry configs for a read workload, or the built-in serial config when the
    /// workload is not registered.
    ///
    /// Returns an error when the workload's table directory has no valid registry key.
    pub fn read_configs(&self, workload: &Workload) -> RegistryResult<Vec<ReadConfig>> {
        let table = workload.table_info.registry_table_key().ok_or_else(|| {
            format!(
                "could not derive a registry table key for workload '{}'",
                workload.case_name
            )
        })?;
        Ok(self
            .lookup(table, &workload.case_name)
            .map_or_else(default_read_configs, ToOwned::to_owned))
    }

    /// Validates the registry structure and its relationship to `workloads`.
    ///
    /// Only registry entries matching a workload in `workloads` receive workload-type validation,
    /// allowing callers to validate a tag-filtered workload set. Returns an error for empty config
    /// lists, duplicate config names, invalid workload registry keys, or read configs targeting a
    /// non-read workload.
    pub fn validate(&self, workloads: &[Workload]) -> RegistryResult<()> {
        for (table, cases) in &self.tables {
            for (case, configs) in cases {
                let key = format!("{table}/{case}");
                if configs.is_empty() {
                    return Err(format!("registry entry '{key}' has an empty config list").into());
                }
                let names: HashSet<_> = configs.iter().map(|config| config.name.as_str()).collect();
                if names.len() != configs.len() {
                    return Err(format!("registry entry '{key}' has duplicate config names").into());
                }
            }
        }

        for workload in workloads {
            let table = workload.table_info.registry_table_key().ok_or_else(|| {
                format!(
                    "could not derive a registry table key for workload '{}'",
                    workload.case_name
                )
            })?;
            if self.lookup(table, &workload.case_name).is_some() {
                if let Spec::SnapshotConstruction(_) = &workload.spec {
                    return Err(format!(
                        "registry entry '{table}/{}' provides read configs for a {} workload",
                        workload.case_name,
                        workload.spec.as_str()
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    /// All `(table, case)` keys in the registry. Lets the harness cross-check every entry against
    /// the loaded workloads so a stale/typo'd key (which would otherwise silently fall back to
    /// defaults) is caught.
    pub fn keys(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tables.iter().flat_map(|(table, cases)| {
            cases
                .keys()
                .map(move |case| (table.as_str(), case.as_str()))
        })
    }

    /// Looks up the config list for `"{table}/{case}"`, if the registry lists it.
    fn lookup(&self, table: &str, case: &str) -> Option<&[ReadConfig]> {
        self.tables
            .get(table)
            .and_then(|cases| cases.get(case))
            .map(Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use delta_kernel_workloads::models::{Spec, TableInfo, Workload};

    use super::*;

    fn registry_from_str(json: &str) -> BenchRegistry {
        let registry: BenchRegistry = serde_json::from_str(json).expect("failed to parse registry");
        registry.validate(&[]).expect("registry should validate");
        registry
    }

    fn read_workload(table: &str, case: &str) -> Workload {
        let mut table_info: TableInfo = serde_json::from_str(
            r#"{
                "name": "test", "description": "test", "tablePath": "s3://bucket/table",
                "schema": {"type": "struct", "fields": []},
                "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
                "logInfo": {"numAddFiles": 0, "numRemoveFiles": 0, "sizeInBytes": 0,
                    "numCommits": 1, "numActions": 1},
                "properties": {}, "dataLayout": {}, "tags": []
            }"#,
        )
        .unwrap();
        table_info.table_info_dir = PathBuf::from(table);
        Workload {
            table_info,
            case_name: case.to_string(),
            spec: serde_json::from_str::<Spec>(r#"{"type": "read"}"#).unwrap(),
        }
    }

    const FULL_REGISTRY: &str = r#"{
        "v2": {
            "readMetadataLatest": [
                { "name": "serial",    "parallelScan": "disabled" },
                { "name": "parallel2", "parallelScan": { "enabled": { "numThreads": 2 } } }
            ]
        }
    }"#;

    #[test]
    fn parses_full_registry() {
        let registry = registry_from_str(FULL_REGISTRY);
        assert_eq!(registry.tables.len(), 1);
        assert_eq!(registry.tables["v2"]["readMetadataLatest"].len(), 2);
    }

    #[test]
    fn read_configs_use_explicit_entry() {
        let registry = registry_from_str(FULL_REGISTRY);
        let configs = registry
            .read_configs(&read_workload("v2", "readMetadataLatest"))
            .unwrap();
        let names: Vec<_> = configs.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["serial", "parallel2"]);
        assert_eq!(configs[0].parallel_scan, ParallelScan::Disabled);
        assert_eq!(
            configs[1].parallel_scan,
            ParallelScan::Enabled { num_threads: 2 }
        );
    }

    #[test]
    fn unlisted_read_benchmark_falls_back_to_serial() {
        // An unlisted read benchmark in a populated registry and any lookup against an empty
        // registry both fall back to the built-in serial config.
        for registry in [registry_from_str(FULL_REGISTRY), registry_from_str("{}")] {
            let read = registry
                .read_configs(&read_workload("other", "readMetadataLatest"))
                .unwrap();
            assert_eq!(read.len(), 1);
            assert_eq!(read[0].name, "serial");
        }
    }

    #[test]
    fn read_configs_errors_without_registry_table_key() {
        let registry = registry_from_str(FULL_REGISTRY);
        let mut workload = read_workload("v2", "readMetadataLatest");
        workload.table_info.table_info_dir.clear();

        let error = registry.read_configs(&workload).unwrap_err().to_string();
        assert!(error.contains("could not derive a registry table key"));
    }

    #[test]
    fn workload_validation_accepts_read_configs_for_read_workload() {
        let registry = registry_from_str(FULL_REGISTRY);
        registry
            .validate(&[read_workload("v2", "readMetadataLatest")])
            .unwrap();
    }

    #[test]
    fn workload_validation_rejects_read_configs_for_snapshot_workload() {
        let registry = registry_from_str(FULL_REGISTRY);
        let mut workload = read_workload("v2", "readMetadataLatest");
        workload.spec = serde_json::from_str(r#"{"type":"snapshotConstruction"}"#).unwrap();

        let error = registry.validate(&[workload]).unwrap_err().to_string();
        assert!(
            error.contains("provides read configs for a snapshotConstruction workload"),
            "got: {error}"
        );
    }

    #[test]
    fn duplicate_config_names_error_at_validation() {
        let json = r#"{
            "t": { "readMetadataLatest": [
                { "name": "dup", "parallelScan": "disabled" },
                { "name": "dup", "parallelScan": "disabled" }
            ] }
        }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate(&[]).unwrap_err().to_string();
        assert!(err.contains("duplicate config names"), "got: {err}");
    }

    #[test]
    fn empty_config_list_errors_at_validation() {
        // An explicit `[]` would expand to zero benchmarks, silently dropping the benchmark, so it
        // is rejected at load rather than accepted like a missing key (which falls back to
        // default).
        let json = r#"{ "t": { "readMetadataLatest": [] } }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate(&[]).unwrap_err().to_string();
        assert!(err.contains("empty config list"), "got: {err}");
    }

    #[test]
    fn malformed_entry_errors_at_load() {
        // `parallelScn` is a typo, so the entry is not a valid read config.
        let json = r#"{
            "t": { "c": [ { "name": "x", "parallelScn": "disabled" } ] }
        }"#;
        assert!(serde_json::from_str::<BenchRegistry>(json).is_err());
    }

    #[test]
    fn non_object_table_entry_errors() {
        // Each table key must map to an object of case -> config list, not a scalar.
        let result: Result<BenchRegistry, _> = serde_json::from_str(r#"{ "t": 1 }"#);
        assert!(result.is_err());
    }

    #[test]
    fn load_from_path_missing_file_errors() {
        let result = BenchRegistry::load_from_path(Path::new("/nonexistent/bench-registry.json"));
        assert!(result.is_err());
    }
}
