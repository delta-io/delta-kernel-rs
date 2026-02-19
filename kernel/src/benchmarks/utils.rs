//! Utility functions for loading workload specifications

use crate::benchmarks::models::{Spec, TableInfo, Workload};
use std::io::Write;
use std::path::{Path, PathBuf};

// Workload extraction configuration
const WORKLOAD_TAR: &str = "tests/data/workloads.tar.gz";
const OUTPUT_FOLDER: &str = "tests/data/workloads";
const DONE_FILE: &str = "tests/data/workloads/.done";
const TABLE_INFO_FILE: &str = "table_info.json";
const SPECS_DIR: &str = "specs";

/// Loads all workload specifications from OUTPUT_FOLDER
/// On first run, extracts from WORKLOAD_TAR if it exists.
/// Uses a .done file to avoid re-extracting on subsequent runs
pub fn load_all_workloads() -> Result<Vec<Workload>, Box<dyn std::error::Error>> {
    if !workload_specs_exist() {
        extract_workload_specs()?;
    }

    let spec_dir = PathBuf::from(OUTPUT_FOLDER);
    let benchmarks_dir = spec_dir.join("benchmarks");
    let table_directories = find_table_directories(&benchmarks_dir)?;

    let mut all_specs = Vec::new();

    for table_dir in table_directories {
        all_specs.extend(load_specs_from_table(&table_dir)?);
    }

    Ok(all_specs)
}

fn workload_specs_exist() -> bool {
    Path::new(DONE_FILE).exists()
}

fn extract_workload_specs() -> Result<(), Box<dyn std::error::Error>> {
    let tar_path = Path::new(WORKLOAD_TAR);

    if !tar_path.exists() {
        return Err(format!(
            "Workload tarball not found at: {}. Please ensure {} exists.",
            WORKLOAD_TAR, WORKLOAD_TAR
        )
        .into());
    }

    let tarball_data = std::fs::read(tar_path)?;
    extract_tarball(tarball_data)?;
    write_done_file()?;

    Ok(())
}

fn extract_tarball(tarball_data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::BufReader;

    let tarball = flate2::read::GzDecoder::new(BufReader::new(&tarball_data[..]));
    let mut archive = tar::Archive::new(tarball);

    std::fs::create_dir_all(OUTPUT_FOLDER)
        .map_err(|e| format!("Failed to create output directory: {}", e))?;

    archive
        .unpack(OUTPUT_FOLDER)
        .map_err(|e| format!("Failed to unpack tarball: {}", e))?;

    Ok(())
}

fn write_done_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut done_file = std::fs::File::create(DONE_FILE)
        .map_err(|e| format!("Failed to create .done file: {}", e))?;

    write!(done_file, "done").map_err(|e| format!("Failed to write .done file: {}", e))?;

    Ok(())
}

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

fn load_specs_from_table(
    table_dir: &Path,
) -> Result<Vec<Workload>, Box<dyn std::error::Error>> {
    let specs_dir = table_dir.join(SPECS_DIR);

    if !specs_dir.exists() || !specs_dir.is_dir() {
        return Err(format!("Specs directory not found: {}", specs_dir.display()).into());
    }

    let table_info_path = table_dir.join(TABLE_INFO_FILE);
    let table_info = TableInfo::from_json_path(&table_info_path).map_err(|e| {
        format!(
            "Failed to parse table_info.json at {}: {}",
            table_info_path.display(),
            e
        )
    })?;

    if table_info.table_path.is_none() {
        let delta_dir = table_dir.join("delta");
        if !delta_dir.exists() || !delta_dir.is_dir() {
            return Err(format!(
                "Table data not found for '{}'. Expected a 'delta' directory in {}",
                table_info.name,
                table_dir.display()
            )
            .into());
        }
    }

    let spec_files = find_spec_files(&specs_dir)?;

    let mut all_specs = Vec::new();
    for spec_file in spec_files {
        all_specs.push(load_single_spec(&spec_file, table_info.clone())?);
    }

    Ok(all_specs)
}

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

fn load_single_spec(
    spec_file: &Path,
    table_info: TableInfo,
) -> Result<Workload, Box<dyn std::error::Error>> {
    if !spec_file.exists() || !spec_file.is_file() {
        return Err(format!("Spec file not found: {}", spec_file.display()).into());
    }

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
