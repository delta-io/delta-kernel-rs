//! Build script for benchmark workloads

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;
use ureq::{Agent, Proxy};

const VERSION: &str = "0.04-preview"; // release tag
const WORKLOADS_VERSION: &str = "0.0.4"; // version in filename

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
    download_tarball(&tarball_url)
}

fn download_tarball(url: &str) -> Vec<u8> {
    let response = if let Ok(proxy_url) = env::var("HTTPS_PROXY") {
        let proxy = Proxy::new(&proxy_url).unwrap();
        let config = Agent::config_builder().proxy(proxy.into()).build();
        Agent::new_with_config(config).get(url).call().unwrap()
    } else {
        ureq::get(url).call().unwrap()
    };

    let mut data: Vec<u8> = Vec::new();
    response
        .into_body()
        .as_reader()
        .read_to_end(&mut data)
        .unwrap();
    data
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
