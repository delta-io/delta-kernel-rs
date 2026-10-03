//! Build script for DAT and acceptance workload specs

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tar::Archive;
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};
use ureq::{Agent, Proxy};

const DAT_EXISTS_FILE_CHECK: &str = "tests/dat/.done";
const DAT_OUTPUT_FOLDER: &str = "tests/dat";
const DAT_VERSION: &str = "0.0.3";
const ACCEPTANCE_WORKLOADS_RELEASE: &str = "0.0.7-preview";
const ACCEPTANCE_WORKLOADS_VERSION: &str = "0.0.7";
const ACCEPTANCE_WORKLOADS_ARCHIVE_ENV: &str = "DELTA_ACCEPTANCE_WORKLOADS_ARCHIVE";
const DOWNLOAD_ATTEMPTS: usize = 3;

// SHA-256 of the release assets. Each download is otherwise trusted purely on TLS; verifying these
// digests before extraction stops a tampered or MITM'd tarball from being unpacked to disk. Update
// alongside the version constants above.
const DAT_CHECKSUM: &str = "19c045bc6f4e8531d1985d0f7bb156d788e65078b435d613c8e4a9c753b4a982";
const WORKLOAD_CHECKSUM: &str = "6d86cada9aa6070cc23a77e3704af095a2ed838c3c198f3a5c39c7d9868f6eeb";

/// Workloads to skip on Windows due to invalid filename characters.
/// Windows does not support these characters in filenames: < > : " | ? *
/// Additionally, some percent-encoded characters cause issues.
#[cfg(windows)]
const WINDOWS_SKIP_WORKLOADS: &[&str] = &[
    // Contains files with #, %, and ? in filenames
    "fpe_special_chars_path/",
];

fn main() {
    // The FFI example tests still use these generated table fixtures even though the legacy DAT
    // acceptance runner has been removed.
    if !Path::new(DAT_EXISTS_FILE_CHECK).exists() {
        let tarball_url = format!(
            "https://github.com/delta-incubator/dat/releases/download/v{DAT_VERSION}/deltalake-dat-v{DAT_VERSION}.tar.gz"
        );
        let tarball_data = download_tarball(&tarball_url, DAT_CHECKSUM);
        extract_dat_tarball(&tarball_data);
        let mut done_file = BufWriter::new(
            File::create(DAT_EXISTS_FILE_CHECK).expect("Failed to create DAT fixture marker"),
        );
        write!(done_file, "done").expect("Failed to write DAT fixture marker");
    }
    extract_acceptance_workloads();
}

fn extract_dat_tarball(tarball_data: &[u8]) {
    let tarball = GzDecoder::new(BufReader::new(tarball_data));
    let mut archive = Archive::new(tarball);
    std::fs::create_dir_all(DAT_OUTPUT_FOLDER).expect("Failed to create DAT fixture directory");
    archive
        .unpack(DAT_OUTPUT_FOLDER)
        .expect("Failed to unpack DAT fixtures");
}

fn download_tarball(url: &str, expected_checksum: &str) -> Vec<u8> {
    let agent = build_agent();
    let mut attempt = 1;

    loop {
        match download(&agent, url) {
            Ok(tarball_data) => {
                verify_checksum(&tarball_data, expected_checksum);
                return tarball_data;
            }
            Err(error) if attempt < DOWNLOAD_ATTEMPTS => {
                eprintln!("Download attempt {attempt} failed: {error}. Retrying...");
                std::thread::sleep(Duration::from_secs(1));
                attempt += 1;
            }
            Err(error) => panic!("Failed to download {url}: {error}"),
        }
    }
}

fn download(agent: &Agent, url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let response = agent.get(url).call()?;
    let mut tarball_data: Vec<u8> = Vec::new();
    response
        .into_body()
        .as_reader()
        .read_to_end(&mut tarball_data)?;

    Ok(tarball_data)
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

/// Build a `ureq` agent that validates TLS against the OS trust store (native-tls).
fn build_agent() -> Agent {
    let tls_config = TlsConfig::builder()
        .provider(TlsProvider::NativeTls)
        .root_certs(RootCerts::PlatformVerifier)
        .build();
    let config = Agent::config_builder()
        .tls_config(tls_config)
        .proxy(Proxy::try_from_env())
        .build();
    Agent::new_with_config(config)
}

/// Download and extract acceptance workload specs if not already done.
/// Downloads from the preview release matching [`ACCEPTANCE_WORKLOADS_RELEASE`].
/// Extracts to `acceptance/workloads/`.
fn extract_acceptance_workloads() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let dir = PathBuf::from(manifest_dir);

    let output_dir = dir.join("workloads");

    // if DELTA_ACCEPTANCE_WORKLOADS_PATH is set, point `workloads/` at that locally-generated
    // corpus instead of downloading the pinned release, so a dev can iterate against their own
    // corpus.
    println!("cargo::rerun-if-env-changed=DELTA_ACCEPTANCE_WORKLOADS_PATH");
    if let Ok(local) = env::var("DELTA_ACCEPTANCE_WORKLOADS_PATH") {
        link_local_workloads(&local, &output_dir);
        return;
    }
    println!("cargo::rerun-if-env-changed={ACCEPTANCE_WORKLOADS_ARCHIVE_ENV}");
    // Drop a stale override symlink (from a prior run) so we download into a real directory.
    if output_dir.is_symlink() {
        std::fs::remove_file(&output_dir).expect("Failed to remove stale workloads symlink");
    }

    let done_marker = output_dir.join(".done");

    // Tell Cargo to re-run if the done marker changes
    println!("cargo::rerun-if-changed={}", done_marker.display());

    if std::fs::read_to_string(&done_marker).is_ok_and(|identity| identity == WORKLOAD_CHECKSUM) {
        return;
    }

    // Download from GitHub releases
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v{ACCEPTANCE_WORKLOADS_RELEASE}/v{ACCEPTANCE_WORKLOADS_VERSION}_dat_workloads.tar.gz"
    );

    let tarball_data = if let Ok(archive) = env::var(ACCEPTANCE_WORKLOADS_ARCHIVE_ENV) {
        let data = std::fs::read(&archive)
            .unwrap_or_else(|error| panic!("Failed to read workload archive {archive}: {error}"));
        verify_checksum(&data, WORKLOAD_CHECKSUM);
        data
    } else {
        download_tarball(&tarball_url, WORKLOAD_CHECKSUM)
    };

    if output_dir.exists() {
        std::fs::remove_dir_all(&output_dir).expect("Failed to remove stale acceptance workloads");
    }
    std::fs::create_dir_all(&output_dir).expect("Failed to create acceptance workloads directory");
    extract_workloads(&tarball_data, &output_dir).expect("Failed to extract acceptance workloads");
    std::fs::write(done_marker, WORKLOAD_CHECKSUM)
        .expect("Failed to write acceptance workloads marker");
}

fn extract_workloads(tarball_data: &[u8], output_dir: &Path) -> Result<(), String> {
    let decoder = GzDecoder::new(BufReader::new(tarball_data));
    let mut archive = Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|error| format!("Failed to read tarball entries: {error}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|error| format!("Failed to read tarball entry: {error}"))?;

        #[cfg(windows)]
        {
            let path = entry
                .path()
                .map_err(|error| format!("Failed to get entry path: {error}"))?;
            let path_str = path.to_string_lossy();
            if WINDOWS_SKIP_WORKLOADS
                .iter()
                .any(|skip| path_str.contains(skip))
            {
                eprintln!("Skipping Windows-incompatible workload file: {path_str}");
                continue;
            }
        }

        entry
            .unpack_in(output_dir)
            .map_err(|error| format!("Failed to unpack entry: {error}"))?;
    }
    Ok(())
}

/// Point `workloads/` at a locally-generated corpus (`DELTA_ACCEPTANCE_WORKLOADS_PATH`) via a
/// symlink, replacing any prior download.
fn link_local_workloads(local: &str, output_dir: &Path) {
    let local_path = std::fs::canonicalize(local).unwrap_or_else(|e| {
        panic!("DELTA_ACCEPTANCE_WORKLOADS_PATH '{local}' is not accessible: {e}")
    });
    assert!(
        local_path.is_dir(),
        "DELTA_ACCEPTANCE_WORKLOADS_PATH '{}' is not a directory",
        local_path.display()
    );

    // Replace whatever is at workloads/ (a prior download dir, or an old symlink).
    if output_dir.is_symlink() || output_dir.is_file() {
        std::fs::remove_file(output_dir).expect("Failed to remove existing workloads path");
    } else if output_dir.is_dir() {
        std::fs::remove_dir_all(output_dir).expect("Failed to remove existing workloads dir");
    }

    #[cfg(unix)]
    std::os::unix::fs::symlink(&local_path, output_dir).unwrap_or_else(|e| {
        panic!(
            "Failed to symlink workloads -> {}: {e}",
            local_path.display()
        )
    });
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&local_path, output_dir).unwrap_or_else(|e| {
        panic!(
            "Failed to symlink workloads -> {}: {e}",
            local_path.display()
        )
    });

    println!(
        "cargo::warning=acceptance: using local workloads from {}",
        local_path.display()
    );
}
