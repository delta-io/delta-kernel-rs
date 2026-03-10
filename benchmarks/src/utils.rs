//! Utility functions for loading workload specifications

use crate::models::{Spec, TableInfo, Workload};
use std::path::{Path, PathBuf};

// Environment variable used to filter benchmarks by tag (e.g. `BENCH_TAGS=base,feature_x`).
pub const BENCH_TAGS_ENV_VAR: &str = "BENCH_TAGS";

const OUTPUT_FOLDER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/workloads");
const TABLE_INFO_FILE_NAME: &str = "tableInfo.json";
const SPECS_DIR_NAME: &str = "specs";
const BENCHMARKS_DIR_NAME: &str = "benchmarks";
const DELTA_DIR_NAME: &str = "delta";

/// Loads all workload specifications from `OUTPUT_FOLDER`, optionally filtered by `BENCH_TAGS`.
///
/// If `KERNEL_BENCH_WORKLOAD_DIR` is set, loads from that directory instead (for remote/S3 tables).
///
/// Workloads are downloaded and extracted into `OUTPUT_FOLDER` at build time by `build.rs`.
///
/// If the `BENCH_TAGS` environment variable is set (e.g. `BENCH_TAGS=base`),
/// only workloads whose `table_info.json` has at least one matching tag are returned.
/// If `BENCH_TAGS` is unset or empty, all workloads are returned.
pub fn load_all_workloads() -> Result<Vec<Workload>, Box<dyn std::error::Error>> {
    let (base_dir, required_tags) = if let Ok(dir) = std::env::var("KERNEL_BENCH_WORKLOAD_DIR") {
        (PathBuf::from(dir), None)
    } else {
        let benchmarks_dir = PathBuf::from(OUTPUT_FOLDER).join(BENCHMARKS_DIR_NAME);
        (benchmarks_dir, get_required_tags())
    };

    let table_directories = find_table_directories(&base_dir)?;

    let mut all_workloads = Vec::new();

    for table_dir in table_directories {
        all_workloads.extend(load_specs_from_table(&table_dir, required_tags.as_ref())?);
    }

    Ok(all_workloads)
}

/// Reads the `BENCH_TAGS` environment variable and returns the set of tags
/// Returns `None` if unset or empty (meaning we should run all workloads)
fn get_required_tags() -> Option<Vec<String>> {
    std::env::var(BENCH_TAGS_ENV_VAR)
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
}

/// Returns all subdirectories of `base_dir`. In practice this is called with `base_dir` = `OUTPUT_FOLDER`/`BENCHMARKS_DIR_NAME`,
/// Each subdirectory returned represents a table to be benchmarked and contains the table itself, specs, and table info
fn find_table_directories(base_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let entries = std::fs::read_dir(base_dir)
        .map_err(|e| format!("Cannot read directory {}: {}", base_dir.display(), e))?;

    let table_dirs: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();

    if table_dirs.is_empty() {
        return Err(format!("No table directories found in {}", base_dir.display()).into());
    }

    Ok(table_dirs)
}

/// Loads all workload specs for a single table, or returns an empty vec if required_tags is set
/// and the table has no matching tags.
///
/// Reads table info from `TABLE_INFO_FILE_NAME` at the root of `table_dir`,
/// then loads each JSON spec from `table_dir`/`SPECS_DIR_NAME`.
///
/// If `required_tags` is `None`, all tables are included (no tables will be skipped in this function)
/// Otherwise, a specific table is included (not skipped by this function) if any of its tags appear in `required_tags` (uses union semantics)
fn load_specs_from_table(
    table_dir: &Path,
    required_tags: Option<&Vec<String>>,
) -> Result<Vec<Workload>, Box<dyn std::error::Error>> {
    let specs_dir = table_dir.join(SPECS_DIR_NAME);

    if !specs_dir.is_dir() {
        return Err(format!("Specs directory not found: {}", specs_dir.display()).into());
    }

    let table_info_path = table_dir.join(TABLE_INFO_FILE_NAME);
    let table_info = TableInfo::from_json_path(&table_info_path).map_err(|e| {
        format!(
            "Failed to parse table_info.json at {}: {}",
            table_info_path.display(),
            e
        )
    })?;

    // Skip this table if BENCH_TAGS is set and none of its tags match
    if let Some(tags) = required_tags {
        if !table_info.matches_tags(tags) {
            return Ok(vec![]);
        }
    }

    // Remote tables (table_path or catalog_managed_info set) don't need local data.
    // Local tables must have a delta/ subdirectory next to tableInfo.json.
    let is_remote = table_info.table_path.is_some() || table_info.catalog_managed_info.is_some();
    if !is_remote {
        let delta_dir = table_dir.join(DELTA_DIR_NAME);
        if !delta_dir.is_dir() {
            return Err(format!(
                "Table data not found for '{}'. Expected a 'delta' directory in {}",
                table_info.name,
                table_dir.display()
            )
            .into());
        }
    }

    let spec_files = find_spec_files(&specs_dir)?;

    let mut workloads = Vec::new();
    for spec_file in spec_files {
        workloads.push(load_single_spec(&spec_file, table_info.clone())?);
    }

    Ok(workloads)
}

/// Returns all JSON files in `specs_dir`, where each file is a benchmark spec for the table
fn find_spec_files(specs_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let entries = std::fs::read_dir(specs_dir)
        .map_err(|e| format!("Cannot read directory {}: {}", specs_dir.display(), e))?;

    let spec_files: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();

    if spec_files.is_empty() {
        return Err(format!("No JSON spec files found in {}", specs_dir.display()).into());
    }

    Ok(spec_files)
}

/// Parses a single spec JSON file and builds a Workload from it, combining the spec with the
/// provided table info and using the spec file's name (without extension) as the case name
fn load_single_spec(
    spec_file: &Path,
    table_info: TableInfo,
) -> Result<Workload, Box<dyn std::error::Error>> {
    let case_name = spec_file
        .file_stem()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("Invalid spec file name: {}", spec_file.display()))?
        .to_string();

    let spec = Spec::from_json_path(spec_file)
        .map_err(|e| format!("Failed to parse spec file {}: {}", spec_file.display(), e))?;

    Ok(Workload {
        table_info,
        case_name,
        spec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_required_tags() {
        // These cases must run sequentially b/c env vars conflict when these tests are separate (as they run in parallel)
        std::env::remove_var(BENCH_TAGS_ENV_VAR);
        assert!(get_required_tags().is_none());

        std::env::set_var(BENCH_TAGS_ENV_VAR, "");
        assert!(get_required_tags().is_none());

        std::env::set_var(BENCH_TAGS_ENV_VAR, "ci");
        assert_eq!(get_required_tags().unwrap(), vec!["ci"]);

        std::env::set_var(BENCH_TAGS_ENV_VAR, "ci,checkpoints,v2");
        assert_eq!(
            get_required_tags().unwrap(),
            vec!["ci", "checkpoints", "v2"]
        );

        std::env::set_var(BENCH_TAGS_ENV_VAR, " ci , checkpoints ");
        assert_eq!(get_required_tags().unwrap(), vec!["ci", "checkpoints"]);

        std::env::remove_var(BENCH_TAGS_ENV_VAR);
    }
}
