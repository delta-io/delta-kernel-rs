//! Benchmark harness-config registry.
//!
//! Defines benchmark harness parameters and maps workloads to the configurations used to run them.
//! These parameters control how a benchmark runs rather than what operation it performs.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use delta_kernel_workloads::models::{Spec, Workload};
use serde::Deserialize;

// === Harness configs ===

/// A read-operation config: benchmark parameters not carried in the spec JSON.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadConfig {
    /// Name appended to the configured benchmark identifier.
    pub name: String,
    /// Whether metadata scanning runs serially or in parallel.
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
    /// Run the metadata scan serially.
    Disabled,
    /// Run the metadata scan with the configured number of worker threads.
    Enabled {
        /// Number of worker threads.
        num_threads: usize,
    },
}

/// A snapshot-construction config: snapshot builder and CRC replay parameters.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotConstructionConfig {
    /// Name appended to the configured benchmark identifier.
    pub name: String,
    /// Whether to build from the table root or from a prebuilt snapshot.
    pub snapshot_builder: SnapshotBuilderConfig,
}

/// The built-in fresh snapshot-construction config.
pub fn default_snapshot_construction_config() -> SnapshotConstructionConfig {
    SnapshotConstructionConfig {
        name: "fresh".into(),
        snapshot_builder: SnapshotBuilderConfig::For,
    }
}

/// Selects the starting point for snapshot construction.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum SnapshotBuilderConfig {
    /// Build a fresh snapshot from the table root.
    For,
    /// Build from a snapshot preconstructed at `version` outside the timed loop.
    From {
        /// Version of the preconstructed base snapshot.
        version: u64,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum HarnessConfig {
    Read(ReadConfig),
    SnapshotConstruction(SnapshotConstructionConfig),
}

impl HarnessConfig {
    fn name(&self) -> &str {
        match self {
            Self::Read(config) => &config.name,
            Self::SnapshotConstruction(config) => &config.name,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::SnapshotConstruction(_) => "snapshot-construction",
        }
    }
}

// === Registry ===

/// Error type for registry loading and lookup.
pub type RegistryResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Benchmark harness configs keyed by table directory name and workload spec filename.
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
/// configuration `name`. Unregistered read workloads use the built-in serial config, while
/// unregistered snapshot-construction workloads retain their default fresh-snapshot behavior.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(transparent)]
pub struct BenchRegistry {
    tables: HashMap<String, HashMap<String, Vec<HarnessConfig>>>,
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
        let (table, case) = workload_registry_key(workload)?;
        let Some(configs) = self.lookup(table, case) else {
            return Ok(default_read_configs());
        };
        configs
            .iter()
            .map(|config| match config {
                HarnessConfig::Read(config) => Ok(config.clone()),
                other => Err(config_kind_error(table, case, "read", other.kind())),
            })
            .collect()
    }

    /// Returns configs for a snapshot-construction workload, or `None` when it is unlisted.
    ///
    /// Returns an error when the workload's table directory has no valid registry key or the
    /// registered configs are for a different operation type.
    pub fn snapshot_configs(
        &self,
        workload: &Workload,
    ) -> RegistryResult<Option<Vec<SnapshotConstructionConfig>>> {
        let (table, case) = workload_registry_key(workload)?;
        let Some(configs) = self.lookup(table, case) else {
            return Ok(None);
        };
        configs
            .iter()
            .map(|config| match config {
                HarnessConfig::SnapshotConstruction(config) => Ok(config.clone()),
                other => Err(config_kind_error(
                    table,
                    case,
                    "snapshot-construction",
                    other.kind(),
                )),
            })
            .collect::<RegistryResult<Vec<_>>>()
            .map(Some)
    }

    /// Validates the registry structure and its relationship to `workloads`.
    ///
    /// Only registry entries matching a workload in `workloads` receive workload-type validation,
    /// allowing callers to validate a tag-filtered workload set. Returns an error for empty config
    /// lists or names, duplicate names, mixed config types, invalid workload registry keys, or
    /// configs targeting the wrong workload type.
    pub fn validate(&self, workloads: &[Workload]) -> RegistryResult<()> {
        for (table, cases) in &self.tables {
            for (case, configs) in cases {
                let key = format!("{table}/{case}");
                if configs.is_empty() {
                    return Err(format!("registry entry '{key}' has an empty config list").into());
                }
                if configs.iter().any(|config| config.name().is_empty()) {
                    return Err(format!("empty config name in '{key}'").into());
                }
                let names: HashSet<_> = configs.iter().map(HarnessConfig::name).collect();
                if names.len() != configs.len() {
                    return Err(format!("registry entry '{key}' has duplicate config names").into());
                }
                let expected_kind = configs[0].kind();
                if let Some(other) = configs.iter().find(|config| config.kind() != expected_kind) {
                    return Err(format!(
                        "registry entry '{key}' mixes {expected_kind} and {} configs",
                        other.kind()
                    )
                    .into());
                }
            }
        }

        for workload in workloads {
            let (table, case) = workload_registry_key(workload)?;
            let Some(configs) = self.lookup(table, case) else {
                continue;
            };
            let expected_kind = match &workload.spec {
                Spec::Read(_) => "read",
                Spec::SnapshotConstruction(_) => "snapshot-construction",
            };
            let actual_kind = configs[0].kind();
            if actual_kind != expected_kind {
                return Err(format!(
                    "registry entry '{table}/{case}' provides {actual_kind} configs for a \
                     {expected_kind} workload"
                )
                .into());
            }
        }
        Ok(())
    }

    /// All `(table, case)` keys in the registry.
    pub fn keys(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tables.iter().flat_map(|(table, cases)| {
            cases
                .keys()
                .map(move |case| (table.as_str(), case.as_str()))
        })
    }

    fn lookup(&self, table: &str, case: &str) -> Option<&[HarnessConfig]> {
        self.tables
            .get(table)
            .and_then(|cases| cases.get(case))
            .map(Vec::as_slice)
    }
}

fn workload_registry_key(workload: &Workload) -> RegistryResult<(&str, &str)> {
    let table = workload.table_info.registry_table_key().ok_or_else(|| {
        format!(
            "could not derive a registry table key for workload '{}'",
            workload.case_name
        )
    })?;
    Ok((table, &workload.case_name))
}

fn config_kind_error(
    table: &str,
    case: &str,
    expected: &str,
    actual: &str,
) -> Box<dyn std::error::Error> {
    format!("registry entry '{table}/{case}' contains {actual} configs, expected {expected}").into()
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
            ],
            "snapshotLatest": [
                {
                    "name": "fresh",
                    "snapshotBuilder": "for"
                },
                {
                    "name": "from199",
                    "snapshotBuilder": { "from": { "version": 199 } }
                }
            ]
        }
    }"#;

    #[test]
    fn parses_full_registry() {
        let registry = registry_from_str(FULL_REGISTRY);
        assert_eq!(registry.tables.len(), 1);
        assert_eq!(registry.tables["v2"]["readMetadataLatest"].len(), 2);
        assert_eq!(registry.tables["v2"]["snapshotLatest"].len(), 2);
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
    fn snapshot_configs_use_explicit_entry() {
        let registry = registry_from_str(FULL_REGISTRY);
        let configs = registry
            .snapshot_configs(&read_workload("v2", "snapshotLatest"))
            .unwrap()
            .unwrap();
        assert_eq!(configs[0].snapshot_builder, SnapshotBuilderConfig::For);
        assert_eq!(
            configs[1].snapshot_builder,
            SnapshotBuilderConfig::From { version: 199 }
        );
    }

    #[test]
    fn unlisted_benchmarks_use_operation_defaults() {
        for registry in [registry_from_str(FULL_REGISTRY), registry_from_str("{}")] {
            let read = registry
                .read_configs(&read_workload("other", "readMetadataLatest"))
                .unwrap();
            assert_eq!(read.len(), 1);
            assert_eq!(read[0].name, "serial");
            assert!(registry
                .snapshot_configs(&read_workload("other", "snapshotLatest"))
                .unwrap()
                .is_none());
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
            error.contains("provides read configs for a snapshot-construction workload"),
            "got: {error}"
        );
    }

    #[test]
    fn wrong_config_type_errors_at_lookup() {
        let registry = registry_from_str(FULL_REGISTRY);
        let read_err = registry
            .read_configs(&read_workload("v2", "snapshotLatest"))
            .unwrap_err()
            .to_string();
        assert!(read_err.contains("expected read"), "got: {read_err}");
        let snapshot_err = registry
            .snapshot_configs(&read_workload("v2", "readMetadataLatest"))
            .unwrap_err()
            .to_string();
        assert!(
            snapshot_err.contains("expected snapshot-construction"),
            "got: {snapshot_err}"
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
    fn mixed_config_types_error_at_validation() {
        let json = r#"{
            "t": { "c": [
                { "name": "read", "parallelScan": "disabled" },
                {
                    "name": "snapshot",
                    "snapshotBuilder": "for"
                }
            ] }
        }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate(&[]).unwrap_err().to_string();
        assert!(
            err.contains("mixes read and snapshot-construction"),
            "got: {err}"
        );
    }

    #[test]
    fn empty_config_list_errors_at_validation() {
        let json = r#"{ "t": { "readMetadataLatest": [] } }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate(&[]).unwrap_err().to_string();
        assert!(err.contains("empty config list"), "got: {err}");
    }

    #[test]
    fn empty_config_name_errors_at_validation() {
        let json = r#"{
            "t": { "c": [ { "name": "", "parallelScan": "disabled" } ] }
        }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate(&[]).unwrap_err().to_string();
        assert!(err.contains("empty config name"), "got: {err}");
    }

    #[test]
    fn malformed_entry_errors_at_load() {
        let json = r#"{
            "t": { "c": [ { "name": "x", "parallelScn": "disabled" } ] }
        }"#;
        assert!(serde_json::from_str::<BenchRegistry>(json).is_err());
    }

    #[test]
    fn non_object_table_entry_errors() {
        let result: Result<BenchRegistry, _> = serde_json::from_str(r#"{ "t": 1 }"#);
        assert!(result.is_err());
    }

    #[test]
    fn load_from_path_missing_file_errors() {
        let result = BenchRegistry::load_from_path(Path::new("/nonexistent/bench-registry.json"));
        assert!(result.is_err());
    }
}
