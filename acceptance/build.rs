//! Build script for DAT and acceptance workload specs

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

    extract_acceptance_workloads();
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

/// Extract acceptance workload specs from the local tar.gz if not already done.
/// Tarball lives at `acceptance/acceptance_workloads.tar.gz` and extracts to
/// `acceptance/workloads/`.
fn extract_acceptance_workloads() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let dir = PathBuf::from(manifest_dir);

    let tarball_path = dir.join("acceptance_workloads.tar.gz");
    let output_dir = dir.join("workloads");
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

    let tarball_file =
        File::open(&tarball_path).expect("Failed to open acceptance_workloads tarball");
    let decoder = GzDecoder::new(BufReader::new(tarball_file));
    let mut archive = Archive::new(decoder);
    std::fs::create_dir_all(&dir).expect("Failed to create acceptance output directory");
    archive
        .unpack(&dir)
        .expect("Failed to unpack acceptance_workloads tarball");

    // Write .done marker
    let mut done_file = BufWriter::new(
        File::create(&done_marker).expect("Failed to create acceptance workloads .done file"),
    );
    write!(done_file, "done").expect("Failed to write acceptance workloads .done file");
}
