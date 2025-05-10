//! Build script for DAT

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use flate2::read::GzDecoder;
use tar::Archive;
use ureq::{Agent, Proxy};

const DAT_EXISTS_FILE_CHECK: &str = "dat/.done";
const OUTPUT_FOLDER: &str = "dat";
const VERSION: &str = "0.0.3";

fn main() {
    if dat_exists() {
        return;
    }

    let tarball_data = download_dat_files();
    extract_tarball(tarball_data);
    write_done_file();
}

fn dat_exists() -> bool {
    Path::new(DAT_EXISTS_FILE_CHECK).exists()
}

fn download_dat_files() -> Vec<u8> {
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v{version}/deltalake-dat-v{version}.tar.gz",
        version = VERSION
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
