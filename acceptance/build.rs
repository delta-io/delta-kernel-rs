//! Build script for DAT and acceptance workload specs

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;
use ureq::{Agent, Proxy};

const DAT_EXISTS_FILE_CHECK: &str = "tests/dat/.done";
const DAT_OUTPUT_FOLDER: &str = "tests/dat";
const DAT_VERSION: &str = "0.0.3";
const ACCEPTANCE_WORKLOADS_VERSION: &str = "0.0.4";

/// Workloads to skip on Windows due to invalid filename characters.
/// Windows does not support these characters in filenames: < > : " | ? *
/// Additionally, some percent-encoded characters cause issues.
#[cfg(windows)]
const WINDOWS_SKIP_WORKLOADS: &[&str] = &[
    // Contains files with #, %, and ? in filenames
    "fpe_special_chars_path/",
];

fn main() {
    if !dat_exists() {
        let tarball_data = download_dat_files();
        extract_dat_tarball(tarball_data);
        write_done_file();
    }

    extract_acceptance_workloads();
}

fn dat_exists() -> bool {
    Path::new(DAT_EXISTS_FILE_CHECK).exists()
}

fn download_dat_files() -> Vec<u8> {
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v{DAT_VERSION}/deltalake-dat-v{DAT_VERSION}.tar.gz"
    );

    download_tarball(&tarball_url)
}

fn download_tarball(url: &str) -> Vec<u8> {
    let response = if let Ok(proxy_url) = env::var("HTTPS_PROXY") {
        let proxy = Proxy::new(&proxy_url).unwrap();
        let config = Agent::config_builder().proxy(proxy.into()).build();
        let agent = Agent::new_with_config(config);
        agent.get(url).call().unwrap()
    } else {
        ureq::get(url).call().unwrap()
    };

    let mut tarball_data: Vec<u8> = Vec::new();
    response
        .into_body()
        .as_reader()
        .read_to_end(&mut tarball_data)
        .unwrap();

    tarball_data
}

fn extract_dat_tarball(tarball_data: Vec<u8>) {
    let tarball = GzDecoder::new(BufReader::new(&tarball_data[..]));
    let mut archive = Archive::new(tarball);
    std::fs::create_dir_all(DAT_OUTPUT_FOLDER).expect("Failed to create output directory");
    archive
        .unpack(DAT_OUTPUT_FOLDER)
        .expect("Failed to unpack tarball");
}

fn write_done_file() {
    let mut done_file =
        BufWriter::new(File::create(DAT_EXISTS_FILE_CHECK).expect("Failed to create .done file"));
    write!(done_file, "done").expect("Failed to write .done file");
}

/// Download and extract acceptance workload specs if not already done.
/// Downloads from GitHub releases at `v{ACCEPTANCE_WORKLOADS_VERSION}_dat` tag.
/// Extracts to `acceptance/workloads/`.
fn extract_acceptance_workloads() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let dir = PathBuf::from(manifest_dir);

    let output_dir = dir.join("workloads");
    let done_marker = output_dir.join(".done");

    // Tell Cargo to re-run if the done marker changes
    println!("cargo::rerun-if-changed={}", done_marker.display());

    if done_marker.exists() {
        return;
    }

    // Download from GitHub releases
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v0.04-preview/v{ACCEPTANCE_WORKLOADS_VERSION}_dat_workloads.tar.gz"
    );

    let tarball_data = download_tarball(&tarball_url);

    // Extract tarball, skipping files with invalid Windows filenames
    let decoder = GzDecoder::new(BufReader::new(&tarball_data[..]));
    let mut archive = Archive::new(decoder);
    std::fs::create_dir_all(&output_dir).expect("Failed to create acceptance output directory");

    // Extract workloads one-by-one to skip tests that contain delta logs with special characters
    // on Windows machines. This is because Windows does not support such files in the filesystem.
    for entry in archive.entries().expect("Failed to read tarball entries") {
        let mut entry = entry.expect("Failed to read tarball entry");

        // On Windows, skip workloads that contain files with invalid filename characters
        #[cfg(windows)]
        {
            let path = entry.path().expect("Failed to get entry path");
            let path_str = path.to_string_lossy();

            // Skip entire workloads known to have problematic filenames
            let should_skip = WINDOWS_SKIP_WORKLOADS
                .iter()
                .any(|skip| path_str.contains(skip));
            if should_skip {
                eprintln!("Skipping Windows-incompatible workload file: {}", path_str);
                continue;
            }
        }

        entry
            .unpack_in(&output_dir)
            .expect("Failed to unpack entry");
    }

    // Write .done marker
    let mut done_file = BufWriter::new(
        File::create(&done_marker).expect("Failed to create acceptance workloads .done file"),
    );
    write!(done_file, "done").expect("Failed to write acceptance workloads .done file");
}
