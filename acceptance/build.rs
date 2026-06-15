//! Build script for DAT and acceptance workload specs

use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tar::Archive;
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};
use ureq::{Agent, Proxy};

const DAT_EXISTS_FILE_CHECK: &str = "tests/dat/.done";
const DAT_OUTPUT_FOLDER: &str = "tests/dat";
const DAT_VERSION: &str = "0.0.3";
const ACCEPTANCE_WORKLOADS_VERSION: &str = "0.0.4";

// SHA-256 of the release assets. Each download is otherwise trusted purely on TLS; verifying these
// digests before extraction stops a tampered or MITM'd tarball from being unpacked to disk. Update
// alongside the version constants above.
const DAT_CHECKSUM: &str = "19c045bc6f4e8531d1985d0f7bb156d788e65078b435d613c8e4a9c753b4a982";
const WORKLOAD_CHECKSUM: &str = "c129283e152239c810a6cf2e35571268bd7452fb379f6294abcee340f2436312";

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

    download_tarball(&tarball_url, DAT_CHECKSUM)
}

fn download_tarball(url: &str, expected_checksum: &str) -> Vec<u8> {
    let response = build_agent().get(url).call().unwrap();

    let mut tarball_data: Vec<u8> = Vec::new();
    response
        .into_body()
        .as_reader()
        .read_to_end(&mut tarball_data)
        .unwrap();

    verify_checksum(&tarball_data, expected_checksum);

    tarball_data
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

<<<<<<< HEAD
=======
/// Build a `ureq` agent that validates TLS against the OS trust store (native-tls) rather than
/// ureq's default rustls + bundled webpki-roots.
///
/// ureq defaults to `RootCerts::WebPki` to resist MITM proxies, but behind a sanctioned
/// TLS-intercepting corporate proxy the interception CA lives in the system trust store and not
/// in the Mozilla bundle, so the default backend rejects the chain with `UnknownIssuer`.
/// `RootCerts::PlatformVerifier` makes native-tls use the system roots (like `curl`). Honors
/// `HTTPS_PROXY` if set.
>>>>>>> 72cc7707 (add workload checksum verification)
fn build_agent() -> Agent {
    let tls_config = TlsConfig::builder()
        .provider(TlsProvider::NativeTls)
        .root_certs(RootCerts::PlatformVerifier)
        .build();
    let mut config = Agent::config_builder().tls_config(tls_config);
    if let Ok(proxy_url) = env::var("HTTPS_PROXY") {
        let proxy = Proxy::new(&proxy_url).unwrap();
        config = config.proxy(Some(proxy));
    }
    Agent::new_with_config(config.build())
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

    // if DELTA_ACCEPTANCE_WORKLOADS_PATH is set, point `workloads/` at that locally-generated
    // corpus instead of downloading the pinned release, so a dev can iterate against their own
    // corpus.
    println!("cargo::rerun-if-env-changed=DELTA_ACCEPTANCE_WORKLOADS_PATH");
    if let Ok(local) = env::var("DELTA_ACCEPTANCE_WORKLOADS_PATH") {
        link_local_workloads(&local, &output_dir);
        return;
    }
    // Drop a stale override symlink (from a prior run) so we download into a real directory.
    if output_dir.is_symlink() {
        std::fs::remove_file(&output_dir).expect("Failed to remove stale workloads symlink");
    }

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

    let tarball_data = download_tarball(&tarball_url, WORKLOAD_CHECKSUM);

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
