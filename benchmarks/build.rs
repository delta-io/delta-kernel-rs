//! Build script for benchmark workloads

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tar::Archive;
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};
use ureq::{Agent, Proxy};

const VERSION: &str = "0.0.5-preview"; // release tag
const WORKLOADS_VERSION: &str = "0.0.5"; // version in filename
const WORKLOAD_CHECKSUM: &str = "ddac5359eca42e7ec65b4a7cfc6f4bc1d629d9c204ba714ec15e4ae830c37ba2"; // benchmarks checksum

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let output_dir = PathBuf::from(manifest_dir).join("workloads");
    let done_marker = output_dir.join(".done");

    println!("cargo::rerun-if-changed={}", done_marker.display());

    if done_marker.exists() {
        return;
    }

    let tarball_data = download_workloads();
    extract_tarball(tarball_data, &output_dir);
    write_done_file(&done_marker);
}

fn download_workloads() -> Vec<u8> {
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v{VERSION}/v{WORKLOADS_VERSION}_benchmark_workloads.tar.gz"
    );
    download_tarball(&tarball_url, WORKLOAD_CHECKSUM)
}

fn download_tarball(url: &str, expected_checksum: &str) -> Vec<u8> {
    let response = build_agent().get(url).call().unwrap();

    let mut data: Vec<u8> = Vec::new();
    response
        .into_body()
        .as_reader()
        .read_to_end(&mut data)
        .unwrap();
    verify_checksum(&data, expected_checksum);
    data
}

/// Panic unless the SHA-256 of `data` equals `expected` (lowercase hex). Called before any
/// extraction, so a download whose digest doesn't match the pinned value fails the build instead
/// of being unpacked to disk.
fn verify_checksum(data: &[u8], expected: &str) {
    let actual: String = Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if actual != expected {
        panic!("tarball checksum mismatch: expected {expected}, got {actual}");
    }
}

fn build_agent() -> Agent {
    let tls_config = TlsConfig::builder()
        .provider(TlsProvider::NativeTls)
        .root_certs(RootCerts::PlatformVerifier)
        .build();
    let config = Agent::config_builder().tls_config(tls_config).proxy(Proxy::try_from_env())
    Agent::new_with_config(config.build())
}

fn extract_tarball(tarball_data: Vec<u8>, output_dir: &Path) {
    let decoder = GzDecoder::new(BufReader::new(&tarball_data[..]));
    let mut archive = Archive::new(decoder);
    std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    archive
        .unpack(output_dir)
        .expect("Failed to unpack tarball");
}

fn write_done_file(done_marker: &Path) {
    let mut done_file =
        BufWriter::new(File::create(done_marker).expect("Failed to create .done file"));
    write!(done_file, "done").expect("Failed to write .done file");
}
