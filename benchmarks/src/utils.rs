//! Utility functions for loading workload specifications

use crate::models::{Spec, TableInfo, Workload};
use flate2::read::GzDecoder;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use tar::Archive;

// Environment variable used to filter benchmarks by tag (e.g. `BENCH_TAGS=base,feature_x`).
pub const BENCH_TAGS_ENV_VAR: &str = "BENCH_TAGS";

// Workload extraction configuration
const WORKLOAD_TAR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/workloads.tar.gz");
const OUTPUT_FOLDER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/workloads");
const DONE_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/workloads/.done");
const TABLE_INFO_FILE_NAME: &str = "tableInfo.json";
const SPECS_DIR_NAME: &str = "specs";
const BENCHMARKS_DIR_NAME: &str = "benchmarks";
const DELTA_DIR_NAME: &str = "delta";

/// Loads all workload specifications from OUTPUT_FOLDER, optionally filtered by BENCH_TAGS
///
/// On first run, extracts from WORKLOAD_TAR if it exists.
/// Uses a .done file to avoid re-extracting on subsequent runs
///
/// If the `BENCH_TAGS` environment variable is set (e.g. `BENCH_TAGS=base`),
/// only workloads whose `table_info.json` has at least one matching tag are returned
/// If `BENCH_TAGS` is unset or empty, all workloads are returned
pub fn load_all_workloads() -> Result<Vec<Workload>, Box<dyn std::error::Error>> {
    if !workload_specs_exist() {
        extract_workload_specs()?;
    }

    let spec_dir = PathBuf::from(OUTPUT_FOLDER);
    let benchmarks_dir = spec_dir.join(BENCHMARKS_DIR_NAME);
    let table_directories = find_table_directories(&benchmarks_dir)?;

    let required_tags = get_required_tags();
    let mut all_workloads = Vec::new();

    for table_dir in table_directories {
        all_workloads.extend(load_specs_from_table(&table_dir, required_tags.as_ref())?);
    }

    Ok(all_workloads)
}

/// Reads the BENCH_TAGS environment variable and returns the set of tags
/// Returns None if unset or empty (meaning we should run all workloads)
fn get_required_tags() -> Option<Vec<String>> {
    std::env::var(BENCH_TAGS_ENV_VAR)
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
}

/// Returns true if table_tags has ANY tag in common with required, where required is the set of tags specified with BENCH_TAGS
fn matches_tags(table_tags: &[String], required: &[String]) -> bool {
    required.iter().any(|r| table_tags.contains(r))
}

/// Checks if workload specs have already been extracted by looking for the .done file
/// If the .done file exists, we don't need to extract the workload specs again
///
/// TODO(#1939): the usage of this function is a naive check;
/// currently, the .done file must be manually deleted to force re-extraction of workload specs
fn workload_specs_exist() -> bool {
    Path::new(DONE_FILE).exists()
}

/// Extracts workload specs from WORKLOAD_TAR into OUTPUT_FOLDER and writes a .done file on success
fn extract_workload_specs() -> Result<(), Box<dyn std::error::Error>> {
    let tar_path = Path::new(WORKLOAD_TAR);

    if !tar_path.exists() {
        return Err(format!("Workload tarball not found at {}", WORKLOAD_TAR).into());
    }

    extract_tarball(tar_path)?;
    write_done_file()?;

    Ok(())
}

/// Extracts a tarball at path into OUTPUT_FOLDER
/// This is used to extract WORKLOAD_TAR into OUTPUT_FOLDER
fn extract_tarball(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let tarball = GzDecoder::new(BufReader::new(file));
    let mut archive = Archive::new(tarball);

    std::fs::create_dir_all(OUTPUT_FOLDER)
        .map_err(|e| format!("Failed to create output directory: {}", e))?;

    archive
        .unpack(OUTPUT_FOLDER)
        .map_err(|e| format!("Failed to unpack tarball: {}", e))?;

    Ok(())
}

/// Writes DONE_FILE to mark that workload specs have been successfully extracted
/// See TODO(#1939) for `workload_specs_exist` above; this file must be manually deleted to force re-extraction
fn write_done_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut done_file = std::fs::File::create(DONE_FILE)
        .map_err(|e| format!("Failed to create .done file: {}", e))?;

    write!(done_file, "done").map_err(|e| format!("Failed to write .done file: {}", e))?;

    Ok(())
}

/// Returns all subdirectories of `base_dir`. In practice this is called with base_dir = OUTPUT_FOLDER/BENCHMARKS_DIR_NAME,
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
/// Reads table info from TABLE_INFO_FILE_NAME at the root of table_dir,
/// then loads each JSON spec from table_dir/SPECS_DIR_NAME
/// If required_tags is None, all tables are included
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
        if !matches_tags(&table_info.tags, tags) {
            return Ok(vec![]);
        }
    }

    // If the table path is not provided, assume that the Delta table is in a DELTA_DIR_NAME/ subdirectory at the same level as table_info.json
    if table_info.table_path.is_none() {
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
    fn test_matches_tags_or_semantics() {
        let table_tags = vec!["ci".to_string(), "checkpoints".to_string()];

        assert!(matches_tags(&table_tags, &["ci".to_string()]));
        assert!(matches_tags(&table_tags, &["checkpoints".to_string()]));
        assert!(matches_tags(
            &table_tags,
            &["ci".to_string(), "large".to_string()]
        ));
    }

    #[test]
    fn test_matches_tags_no_match() {
        let table_tags = vec!["large".to_string()];
        assert!(!matches_tags(
            &table_tags,
            &["ci".to_string(), "checkpoints".to_string()]
        ));
    }

    #[test]
    fn test_matches_tags_empty_table_tags() {
        let table_tags: Vec<String> = vec![];
        assert!(!matches_tags(&table_tags, &["ci".to_string()]));
    }

    #[test]
    fn test_get_required_tags_empty() {
        std::env::set_var(BENCH_TAGS_ENV_VAR, "");
        assert!(get_required_tags().is_none());
        std::env::remove_var(BENCH_TAGS_ENV_VAR);
    }

    #[test]
    fn test_get_required_tags_single() {
        std::env::set_var(BENCH_TAGS_ENV_VAR, "ci");
        let tags = get_required_tags().unwrap();
        assert_eq!(tags, vec!["ci"]);
        std::env::remove_var(BENCH_TAGS_ENV_VAR);
    }

    #[test]
    fn test_get_required_tags_multiple() {
        std::env::set_var(BENCH_TAGS_ENV_VAR, "ci,checkpoints,v2");
        let tags = get_required_tags().unwrap();
        assert_eq!(tags, vec!["ci", "checkpoints", "v2"]);
        std::env::remove_var(BENCH_TAGS_ENV_VAR);
    }

    #[test]
    fn test_get_required_tags_with_whitespace() {
        std::env::set_var(BENCH_TAGS_ENV_VAR, " ci , checkpoints ");
        let tags = get_required_tags().unwrap();
        assert_eq!(tags, vec!["ci", "checkpoints"]);
        std::env::remove_var(BENCH_TAGS_ENV_VAR);
    }
}
