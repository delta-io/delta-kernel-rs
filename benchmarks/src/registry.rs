//! Benchmark harness-config registry.
//!
//! Defines the read harness config types (`ReadConfig`, `ParallelScan`) and the registry that maps
//! read benchmarks to them. These are benchmark-harness concepts (how to run a benchmark, not what
//! the workload is).
//!
//! The registry is a checked-in JSON file (`benchmarks/bench-registry.json`) nesting each
//! benchmark's harness configs by table name then case name: `{ table: { case: [configs] } }`.
//! The table key is the table's directory name and the case key is the spec filename. Benchmark
//! output uses the human-readable table name from `tableInfo.json` instead of the directory key.
//!
//! Each registered read benchmark expands into one Criterion benchmark per config; the config
//! `name` becomes the trailing path segment of the benchmark name. A read benchmark whose
//! `(table, case)` key is absent from the registry falls back to the built-in serial config.
//!
//! The `parallelScan` config field uses serde's externally-tagged form: the fieldless variant is
//! a bare string (`"disabled"`) and the parameterized variant a single-key object
//! (`{"enabled": {"numThreads": 2}}`).

use std::collections::{HashMap, HashSet};
use std::path::Path;

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

/// Scan parallelism. Serialized externally-tagged: `"disabled"` or
/// `{ "enabled": { "numThreads": <n> } }`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ParallelScan {
    Disabled,
    Enabled { num_threads: usize },
}

// === Registry ===

/// Error type for registry loading and lookup.
pub type RegistryResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Registry deserialized from `benchmarks/bench-registry.json`: read harness configs nested by
/// table name then case name.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(transparent)]
pub struct BenchRegistry {
    tables: HashMap<String, HashMap<String, Vec<ReadConfig>>>,
}

impl BenchRegistry {
    /// Load and validate a registry from `path`.
    pub fn load_from_path(path: &Path) -> RegistryResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("reading registry {}: {e}", path.display()))?;
        let registry: BenchRegistry = serde_json::from_str(&content)
            .map_err(|e| format!("parsing registry {}: {e}", path.display()))?;
        registry.validate()?;
        Ok(registry)
    }

    /// Configs for the read benchmark `"{table}/{case}"`: the registry entry if present, else the
    /// built-in serial config.
    pub fn read_configs(&self, table: &str, case: &str) -> Vec<ReadConfig> {
        self.lookup(table, case)
            .map_or_else(default_read_configs, ToOwned::to_owned)
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

    /// Validate every read config list independently of which benchmarks are later looked up.
    fn validate(&self) -> RegistryResult<()> {
        for (table, cases) in &self.tables {
            for (case, configs) in cases {
                let key = format!("{table}/{case}");
                check_config_names(configs.iter().map(|c| c.name.as_str()), &key)?;
            }
        }
        Ok(())
    }
}

/// Errors if `names` is empty or contains a repeat. `ctx` identifies the offending list. An empty
/// list would otherwise expand to zero benchmarks, silently dropping the benchmark.
fn check_config_names<'a>(names: impl Iterator<Item = &'a str>, ctx: &str) -> RegistryResult<()> {
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(format!("duplicate config name '{name}' in '{ctx}'").into());
        }
    }
    if seen.is_empty() {
        return Err(format!("registry entry '{ctx}' has an empty config list").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry_from_str(json: &str) -> BenchRegistry {
        let registry: BenchRegistry = serde_json::from_str(json).expect("failed to parse registry");
        registry.validate().expect("registry should validate");
        registry
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
        let configs = registry.read_configs("v2", "readMetadataLatest");
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
            let read = registry.read_configs("other", "readMetadataLatest");
            assert_eq!(read.len(), 1);
            assert_eq!(read[0].name, "serial");
        }
    }

    #[test]
    fn duplicate_config_names_error_at_load() {
        let json = r#"{
            "t": { "readMetadataLatest": [
                { "name": "dup", "parallelScan": "disabled" },
                { "name": "dup", "parallelScan": "disabled" }
            ] }
        }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate().unwrap_err().to_string();
        assert!(err.contains("duplicate config name 'dup'"), "got: {err}");
    }

    #[test]
    fn empty_config_list_errors_at_load() {
        // An explicit `[]` would expand to zero benchmarks, silently dropping the benchmark, so it
        // is rejected at load rather than accepted like a missing key (which falls back to
        // default).
        let json = r#"{ "t": { "readMetadataLatest": [] } }"#;
        let registry: BenchRegistry = serde_json::from_str(json).unwrap();
        let err = registry.validate().unwrap_err().to_string();
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
