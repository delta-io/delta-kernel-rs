//! The delta-kernel crate provides a query engine-agnostic interface for reading (and soon
//! writing) Delta tables.

#![warn(
    unreachable_pub,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    rust_2018_idioms,
    rust_2021_compatibility,
    missing_debug_implementations
)]

use object_store::path::Path;
use tracing::*;

use std::io::{Error, ErrorKind};

/// Includes top-level DeltaTable type which can construct Snapshots
pub mod delta_table;

/// defines a common expression language for use in data skipping predicates
pub mod expressions;

/// generic parquet interface
pub mod parquet_reader;

/// Implements snapshots reads/scans via iterator API yielding arrow record batches
pub mod scan;

/// In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
/// has schema etc.) pub mod snapshot;
pub mod snapshot;

pub mod error;
pub use error::DeltaResult;

/// delta_log module for defining schema of log files, actions, etc.
mod delta_log;
pub use delta_log::LogFile;

/// Delta table version is 8 byte unsigned int
pub type Version = u64;

/**
 * Parse the given [object_store::path::Path] to identify a commit log version
 */
fn version_from_path(path: &Path) -> Result<Version, object_store::Error> {
    if let Some(filename) = path.filename() {
        if let Some(part) = filename.split('.').next() {
            return part.parse().map_err(|source: std::num::ParseIntError| {
                object_store::Error::NotFound {
                    path: part.to_string(),
                    source: source.into(),
                }
            });
        }
        Ok(0)
    } else {
        let e = format!(
            "Provided path does not have a filename at the end: {:?}",
            path
        );
        error!("{}", &e);
        let err = Error::new(ErrorKind::Other, e);
        Err(object_store::Error::Generic {
            store: "invalid",
            source: Box::new(err),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_from_path() {
        let path = Path::from("/tmp/test/_delta_log/00000000000000000001.json");
        let version = version_from_path(&path).unwrap();

        assert_eq!(1, version);
    }

    #[test]
    fn test_version_from_path_invalid_stem() {
        let path = Path::from("/tmp/test/_delta_log/some-garbage");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }

    #[test]
    fn test_version_from_path_invalid_extension() {
        let path = Path::from("/tmp/test/_delta_log/some-garbage.txt");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }

    #[test]
    fn test_version_from_path_without_filename() {
        let path = Path::from("/");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }

    #[test]
    fn test_version_from_path_with_dir() {
        let path = Path::from("/tmp/test/_delta_log/");
        let result = version_from_path(&path);
        assert!(result.is_err())
    }
}
