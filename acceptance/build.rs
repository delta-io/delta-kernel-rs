//! Build script for DAT and improved_dat

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;
use ureq::{Agent, Proxy};

const DAT_EXISTS_FILE_CHECK: &str = "tests/dat/.done";
const OUTPUT_FOLDER: &str = "tests/dat";
const VERSION: &str = "0.0.3";

fn main() {
    if !dat_exists() {
        let tarball_data = download_dat_files();
        extract_tarball(tarball_data);
        write_done_file();
    }

    extract_improved_dat();
}

fn dat_exists() -> bool {
    Path::new(DAT_EXISTS_FILE_CHECK).exists()
}

fn download_dat_files() -> Vec<u8> {
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v{VERSION}/deltalake-dat-v{VERSION}.tar.gz"
    );

    let response = if let Ok(proxy_url) = env::var("HTTPS_PROXY") {
        let proxy = Proxy::new(&proxy_url).unwrap();
        let config = Agent::config_builder().proxy(proxy.into()).build();
        let agent = Agent::new_with_config(config);
        agent.get(&tarball_url).call().unwrap()
    } else {
        ureq::get(&tarball_url).call().unwrap()
    };

    let mut tarball_data: Vec<u8> = Vec::new();
    response
        .into_body()
        .as_reader()
        .read_to_end(&mut tarball_data)
        .unwrap();

    tarball_data
}

fn extract_tarball(tarball_data: Vec<u8>) {
    let tarball = GzDecoder::new(BufReader::new(&tarball_data[..]));
    let mut archive = Archive::new(tarball);
    std::fs::create_dir_all(OUTPUT_FOLDER).expect("Failed to create output directory");
    archive
        .unpack(OUTPUT_FOLDER)
        .expect("Failed to unpack tarball");
}

fn write_done_file() {
    let mut done_file =
        BufWriter::new(File::create(DAT_EXISTS_FILE_CHECK).expect("Failed to create .done file"));
    write!(done_file, "done").expect("Failed to write .done file");
}

/// Get the repo root directory (parent of CARGO_MANIFEST_DIR for the acceptance crate).
fn repo_root() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    Path::new(&manifest_dir)
        .parent()
        .expect("CARGO_MANIFEST_DIR has no parent")
        .to_path_buf()
}

/// Extract improved_dat specs from the local tar.gz if not already done.
fn extract_improved_dat() {
    let root = repo_root();
    let tarball_path = root.join("kernel_benchmark_specs.tar.gz");
    let output_dir = root.join("improved_dat");
    let done_marker = output_dir.join(".done");

    // Tell Cargo to re-run if the tarball changes
    println!("cargo::rerun-if-changed={}", tarball_path.display());

    if done_marker.exists() {
        return;
    }

    if !tarball_path.exists() {
        // Tarball not present — skip silently (tests will just find no spec files)
        return;
    }

    let tarball_file = File::open(&tarball_path).expect("Failed to open improved_dat tarball");
    let decoder = GzDecoder::new(BufReader::new(tarball_file));
    let mut archive = Archive::new(decoder);
    std::fs::create_dir_all(&output_dir)
        .expect("Failed to create improved_dat output directory");
    archive
        .unpack(&output_dir)
        .expect("Failed to unpack improved_dat tarball");

    // Rename DV bin files: strip "test%dv%prefix-" from filenames.
    // Delta test resources use this prefix but the delta log references the unprefixed name.
    rename_dv_bin_files(&output_dir);

    // Write .done marker
    let mut done_file = BufWriter::new(
        File::create(&done_marker).expect("Failed to create improved_dat .done file"),
    );
    write!(done_file, "done").expect("Failed to write improved_dat .done file");
}

/// Recursively rename files with "test%dv%prefix-" to strip that prefix.
fn rename_dv_bin_files(dir: &Path) {
    const DV_PREFIX: &str = "test%dv%prefix-";

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rename_dv_bin_files(&path);
        } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if let Some(stripped) = name.strip_prefix(DV_PREFIX) {
                let new_path = path.with_file_name(stripped);
                std::fs::rename(&path, &new_path).unwrap_or_else(|e| {
                    eprintln!(
                        "Warning: failed to rename {:?} -> {:?}: {}",
                        path, new_path, e
                    );
                });
            }
        }
    }
}
