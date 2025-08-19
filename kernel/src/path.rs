//! Utilities to make working with directory and file paths easier

use std::slice;
use std::str::FromStr;

use crate::actions::visitors::InCommitTimestampVisitor;
use crate::{DeltaResult, Engine, Error, FileMeta, RowVisitor, Version};
use delta_kernel_derive::internal_api;

use url::Url;
use uuid::Uuid;

/// How many characters a version tag has
const VERSION_LEN: usize = 20;

/// How many characters a part specifier on a multipart checkpoint has
const MULTIPART_PART_LEN: usize = 10;

/// The number of characters in the uuid part of a uuid checkpoint
const UUID_PART_LEN: usize = 36;

#[derive(Debug, Clone, PartialEq, Eq)]
#[internal_api]
pub(crate) enum LogPathFileType {
    Commit,
    SinglePartCheckpoint,
    #[allow(unused)]
    UuidCheckpoint(String),
    // NOTE: Delta spec doesn't actually say, but checkpoint part numbers are effectively 31-bit
    // unsigned integers: Negative values are never allowed, but Java integer types are always
    // signed. Approximate that as u32 here.
    #[allow(unused)]
    MultiPartCheckpoint {
        part_num: u32,
        num_parts: u32,
    },
    #[allow(unused)]
    CompactedCommit {
        hi: Version,
    },
    Crc,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[internal_api]
pub(crate) struct ParsedLogPath<Location: AsUrl = FileMeta> {
    pub location: Location,
    #[allow(unused)]
    pub filename: String,
    #[allow(unused)]
    pub extension: String,
    pub version: Version,
    pub file_type: LogPathFileType,
}

// Internal helper used by TryFrom<FileMeta> below. It parses a fixed-length string into the numeric
// type expected by the caller. A wrong length produces an error, even if the parse succeeded.
fn parse_path_part<T: FromStr>(value: &str, expect_len: usize, location: &Url) -> DeltaResult<T> {
    match value.parse() {
        Ok(result) if value.len() == expect_len => Ok(result),
        _ => Err(Error::invalid_log_path(location)),
    }
}

// We normally construct ParsedLogPath from FileMeta, but in testing it's convenient to use
// a Url directly instead. This trait decouples the two.
#[internal_api]
pub(crate) trait AsUrl {
    fn as_url(&self) -> &Url;
}

impl AsUrl for FileMeta {
    fn as_url(&self) -> &Url {
        &self.location
    }
}

impl AsUrl for Url {
    fn as_url(&self) -> &Url {
        self
    }
}

impl<Location: AsUrl> ParsedLogPath<Location> {
    // NOTE: We can't actually impl TryFrom because Option<T> is a foreign struct even if T is local.
    #[internal_api]
    pub(crate) fn try_from(location: Location) -> DeltaResult<Option<ParsedLogPath<Location>>> {
        let url = location.as_url();
        #[allow(clippy::unwrap_used)]
        let filename = url
            .path_segments()
            .ok_or_else(|| Error::invalid_log_path(url))?
            .next_back()
            .unwrap() // "the iterator always contains at least one string (which may be empty)"
            .to_string();
        if filename.is_empty() {
            return Err(Error::invalid_log_path(url));
        }

        let mut split = filename.split('.');

        // NOTE: str::split always returns at least one item, even for the empty string.
        #[allow(clippy::unwrap_used)]
        let version = split.next().unwrap();

        // Every valid log path starts with a numeric version part. If version parsing fails, it
        // must not be a log path and we simply return None. However, it is an error if version
        // parsing succeeds for a wrong-length numeric string.
        let version = match version.parse().ok() {
            Some(v) if version.len() == VERSION_LEN => v,
            Some(_) => return Err(Error::invalid_log_path(url)),
            None => return Ok(None),
        };

        // Every valid log path has a file extension as its last part. Return None if it's missing.
        let split: Vec<_> = split.collect();
        let extension = match split.last() {
            Some(extension) => extension.to_string(),
            None => return Ok(None),
        };

        // Parse the file type, based on the number of remaining parts
        let file_type = match split.as_slice() {
            ["json"] => LogPathFileType::Commit,
            ["crc"] => LogPathFileType::Crc,
            ["checkpoint", "parquet"] => LogPathFileType::SinglePartCheckpoint,
            ["checkpoint", uuid, "json" | "parquet"] => {
                let uuid = parse_path_part(uuid, UUID_PART_LEN, url)?;
                LogPathFileType::UuidCheckpoint(uuid)
            }
            [hi, "compacted", "json"] => {
                let hi = parse_path_part(hi, VERSION_LEN, url)?;
                LogPathFileType::CompactedCommit { hi }
            }
            ["checkpoint", part_num, num_parts, "parquet"] => {
                let part_num = parse_path_part(part_num, MULTIPART_PART_LEN, url)?;
                let num_parts = parse_path_part(num_parts, MULTIPART_PART_LEN, url)?;

                // A valid part_num must be in the range [1, num_parts]
                if !(0 < part_num && part_num <= num_parts) {
                    return Err(Error::invalid_log_path(url));
                }
                LogPathFileType::MultiPartCheckpoint {
                    part_num,
                    num_parts,
                }
            }

            // Unrecognized log paths are allowed, so long as they have a valid version.
            _ => LogPathFileType::Unknown,
        };
        Ok(Some(ParsedLogPath {
            location,
            filename,
            extension,
            version,
            file_type,
        }))
    }

    #[internal_api]
    pub(crate) fn is_commit(&self) -> bool {
        matches!(self.file_type, LogPathFileType::Commit)
    }

    #[internal_api]
    pub(crate) fn is_checkpoint(&self) -> bool {
        matches!(
            self.file_type,
            LogPathFileType::SinglePartCheckpoint
                | LogPathFileType::MultiPartCheckpoint { .. }
                | LogPathFileType::UuidCheckpoint(_)
        )
    }

    #[internal_api]
    #[allow(dead_code)] // currently only used in tests, which don't "count"
    pub(crate) fn is_unknown(&self) -> bool {
        matches!(self.file_type, LogPathFileType::Unknown)
    }
}

impl ParsedLogPath<Url> {
    const DELTA_LOG_DIR: &'static str = "_delta_log/";

    /// Helper method to create a path with the given filename generator
    fn create_path(table_root: &Url, filename: String) -> DeltaResult<Self> {
        let location = table_root.join(Self::DELTA_LOG_DIR)?.join(&filename)?;
        Self::try_from(location)?.ok_or_else(|| {
            Error::internal_error(format!("Attempted to create an invalid path: {filename}"))
        })
    }

    /// Create a new ParsedCommitPath<Url> for a new json commit file
    pub(crate) fn new_commit(table_root: &Url, version: Version) -> DeltaResult<Self> {
        let filename = format!("{version:020}.json");
        let path = Self::create_path(table_root, filename)?;
        if !path.is_commit() {
            return Err(Error::internal_error(
                "ParsedLogPath::new_commit created a non-commit path",
            ));
        }
        Ok(path)
    }

    /// Create a new ParsedCheckpointPath<Url> for a classic parquet checkpoint file
    #[allow(dead_code)] // TODO: Remove this once we have a use case for it
    pub(crate) fn new_classic_parquet_checkpoint(
        table_root: &Url,
        version: Version,
    ) -> DeltaResult<Self> {
        let filename = format!("{version:020}.checkpoint.parquet");
        let path = Self::create_path(table_root, filename)?;
        if !path.is_checkpoint() {
            return Err(Error::internal_error(
                "ParsedLogPath::new_classic_parquet_checkpoint created a non-checkpoint path",
            ));
        }
        Ok(path)
    }

    /// Create a new ParsedCheckpointPath<Url> for a UUID-based parquet checkpoint file
    #[allow(dead_code)] // TODO: Remove this once we have a use case for it
    pub(crate) fn new_uuid_parquet_checkpoint(
        table_root: &Url,
        version: Version,
    ) -> DeltaResult<Self> {
        let filename = format!("{:020}.checkpoint.{}.parquet", version, Uuid::new_v4());
        let path = Self::create_path(table_root, filename)?;
        if !path.is_checkpoint() {
            return Err(Error::internal_error(
                "ParsedLogPath::new_uuid_parquet_checkpoint created a non-checkpoint path",
            ));
        }
        Ok(path)
    }

    // TODO: remove after support for writing CRC files
    #[allow(unused)]
    /// Create a new ParsedCommitPath<Url> for a new CRC file
    pub(crate) fn new_crc(table_root: &Url, version: Version) -> DeltaResult<Self> {
        let filename = format!("{version:020}.crc");
        let path = Self::create_path(table_root, filename)?;
        if path.file_type != LogPathFileType::Crc {
            return Err(Error::internal_error(
                "ParsedLogPath::new_crc created a non-crc path",
            ));
        }
        Ok(path)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InCommitTimestampError {
    #[error("No In-commit timestamp found for commit at version {version}")]
    NotFound { version: Version },
    #[error("Failed to read the in commit timestamp for a commit at version {version}: {source}")]
    FailedToReadTimestamp {
        version: Version,
        source: Box<dyn std::error::Error>,
    },
}

impl ParsedLogPath<FileMeta> {
    /// Reads the in-commit timestamp for the given `commit_file`.
    ///
    /// This returns a [`InCommitTimestampError::FailedToReadTimestamp`] if this encounters an
    /// error while reading the file or visiting the rows.
    ///
    /// This returns a [`InCommitTimestampError::NotFound`] if the in-commit timestamp
    /// is not present in the commit file, or if the CommitInfo is not the first action in the
    /// commit.
    #[allow(unused)]
    pub(crate) fn read_in_commit_timestamp(
        &self,
        engine: &dyn Engine,
    ) -> Result<i64, InCommitTimestampError> {
        debug_assert!(self.is_commit(), "File should be a commit");
        let wrap_err = |error: Error| InCommitTimestampError::FailedToReadTimestamp {
            version: self.version,
            source: Box::new(error),
        };

        // Get an iterator over the actions in the commit file
        let mut action_iter = engine
            .json_handler()
            .read_json_files(
                slice::from_ref(&self.location),
                InCommitTimestampVisitor::schema(),
                None,
            )
            .map_err(wrap_err)?;

        let not_found = || InCommitTimestampError::NotFound {
            version: self.version,
        };

        // Take the first non-empty engine data batch
        match action_iter.next() {
            Some(Ok(batch)) => {
                // Visit the rows and get the in-commit timestamp if present
                let mut visitor = InCommitTimestampVisitor::default();
                visitor.visit_rows_of(batch.as_ref()).map_err(wrap_err)?;
                visitor.in_commit_timestamp.ok_or_else(not_found)
            }
            Some(Err(err)) => Err(wrap_err(err)),
            None => Err(not_found()),
        }
    }

    /// Returns the file modification timestamp. This makes no guarantee on whether
    /// in-commit timestamps is enabled for this commit or not
    #[allow(unused)]
    pub(crate) fn file_modification_timestamp(&self) -> i64 {
        debug_assert!(self.is_commit(), "File should be a commit");
        self.location.last_modified
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::num::NonZero;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    use crate::actions::{CommitInfo, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::snapshot::Snapshot;
    use crate::table_features::WriterFeature;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::Version;

    use test_utils::delta_path_for_version;
    use url::Url;

    use super::*;

    fn table_log_dir_url() -> Url {
        let path = PathBuf::from("./tests/data/table-with-dv-small/_delta_log/");
        let path = std::fs::canonicalize(path).unwrap();
        assert!(path.is_dir());
        let url = url::Url::from_directory_path(path).unwrap();
        assert!(url.path().ends_with('/'));
        url
    }

    #[test]
    fn test_unknown_invalid_patterns() {
        let table_log_dir = table_log_dir_url();

        // invalid -- not a file
        let log_path = table_log_dir.join("subdir/").unwrap();
        assert!(log_path
            .path()
            .ends_with("/tests/data/table-with-dv-small/_delta_log/subdir/"));
        ParsedLogPath::try_from(log_path).expect_err("directory path");

        // ignored - not versioned
        let log_path = table_log_dir.join("_last_checkpoint").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap();
        assert!(log_path.is_none());

        // ignored - no extension
        let log_path = table_log_dir.join("00000000000000000010").unwrap();
        let result = ParsedLogPath::try_from(log_path);
        assert!(
            matches!(result, Ok(None)),
            "Expected Ok(None) for missing file extension"
        );

        // empty extension - should be treated as unknown file type
        let log_path = table_log_dir.join("00000000000000000011.").unwrap();
        let result = ParsedLogPath::try_from(log_path);
        assert!(
            matches!(
                result,
                Ok(Some(ParsedLogPath {
                    file_type: LogPathFileType::Unknown,
                    ..
                }))
            ),
            "Expected Unknown file type, got {result:?}"
        );

        // ignored - version fails to parse
        let log_path = table_log_dir.join("abc.json").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap();
        assert!(log_path.is_none());

        // invalid - version has too many digits
        let log_path = table_log_dir.join("000000000000000000010.json").unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too many digits");

        // invalid - version has too few digits
        let log_path = table_log_dir.join("0000000000000000010.json").unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too few digits");

        // unknown - two parts
        let log_path = table_log_dir.join("00000000000000000010.foo").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000010.foo");
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 10);
        assert!(matches!(log_path.file_type, LogPathFileType::Unknown));
        assert!(log_path.is_unknown());

        // unknown - many parts
        let log_path = table_log_dir
            .join("00000000000000000010.a.b.c.foo")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000010.a.b.c.foo");
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 10);
        assert!(log_path.is_unknown());
    }

    #[test]
    fn test_commit_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir.join("00000000000000000000.json").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000000.json");
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 0);
        assert!(matches!(log_path.file_type, LogPathFileType::Commit));
        assert!(log_path.is_commit());
        assert!(!log_path.is_checkpoint());

        let log_path = table_log_dir.join("00000000000000000005.json").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.version, 5);
        assert!(log_path.is_commit());
    }

    #[test]
    fn test_crc_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir.join("00000000000000000000.crc").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000000.crc");
        assert_eq!(log_path.extension, "crc");
        assert_eq!(log_path.version, 0);
        assert!(matches!(log_path.file_type, LogPathFileType::Crc));
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());

        let log_path = table_log_dir.join("00000000000000000005.crc").unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.version, 5);
        assert!(log_path.file_type == LogPathFileType::Crc);
    }

    #[test]
    fn test_single_part_checkpoint_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000002.checkpoint.parquet");
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 2);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::SinglePartCheckpoint
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        // invalid file extension
        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000002.checkpoint.json");
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 2);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());
    }

    #[test]
    fn test_uuid_checkpoint_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 2);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::UuidCheckpoint(ref u) if u == "3a0d65cd-4056-49b8-937b-95f9e3ee90e5",
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json"
        );
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 2);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::UuidCheckpoint(ref u) if u == "3a0d65cd-4056-49b8-937b-95f9e3ee90e5",
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.foo")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000002.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.foo"
        );
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 2);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.foo.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("not a uuid");

        // invalid file extension
        let log_path = table_log_dir
            .join("00000000000000000002.checkpoint.foo")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(log_path.filename, "00000000000000000002.checkpoint.foo");
        assert_eq!(log_path.extension, "foo");
        assert_eq!(log_path.version, 2);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        // Boundary test - UUID with exactly 35 characters (one too short)
        let log_path = table_log_dir
            .join("00000000000000000010.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e.parquet")
            .unwrap();
        let result = ParsedLogPath::try_from(log_path);
        assert!(
            matches!(result, Err(Error::InvalidLogPath(_))),
            "Expected an error for UUID with exactly 35 characters"
        );
    }

    #[test]
    fn test_multi_part_checkpoint_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000000.0000000002.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.checkpoint.0000000000.0000000002.json"
        );
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 8);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000000.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part 0");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000001.0000000002.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.checkpoint.0000000001.0000000002.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 8);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 1,
                num_parts: 2
            }
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000002.0000000002.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.checkpoint.0000000002.0000000002.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 8);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 2,
                num_parts: 2
            }
        ));
        assert!(!log_path.is_commit());
        assert!(log_path.is_checkpoint());

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000003.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part 3");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.000000001.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part_num");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000001.000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid num_parts");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.00000000x1.0000000002.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid part_num");

        let log_path = table_log_dir
            .join("00000000000000000008.checkpoint.0000000001.00000000x2.parquet")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("invalid num_parts");
    }

    #[test]
    fn test_compacted_delta_patterns() {
        let table_log_dir = table_log_dir_url();

        let log_path = table_log_dir
            .join("00000000000000000008.00000000000000000015.compacted.json")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.00000000000000000015.compacted.json"
        );
        assert_eq!(log_path.extension, "json");
        assert_eq!(log_path.version, 8);
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::CompactedCommit { hi: 15 },
        ));
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());

        // invalid extension
        let log_path = table_log_dir
            .join("00000000000000000008.00000000000000000015.compacted.parquet")
            .unwrap();
        let log_path = ParsedLogPath::try_from(log_path).unwrap().unwrap();
        assert_eq!(
            log_path.filename,
            "00000000000000000008.00000000000000000015.compacted.parquet"
        );
        assert_eq!(log_path.extension, "parquet");
        assert_eq!(log_path.version, 8);
        assert!(!log_path.is_commit());
        assert!(!log_path.is_checkpoint());
        assert!(log_path.is_unknown());

        let log_path = table_log_dir
            .join("00000000000000000008.0000000000000000015.compacted.json")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too few digits in hi");

        let log_path = table_log_dir
            .join("00000000000000000008.000000000000000000015.compacted.json")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("too many digits in hi");

        let log_path = table_log_dir
            .join("00000000000000000008.00000000000000000a15.compacted.json")
            .unwrap();
        ParsedLogPath::try_from(log_path).expect_err("non-numeric hi");
    }

    #[test]
    fn test_new_commit() {
        let table_log_dir = table_log_dir_url();
        let log_path = ParsedLogPath::new_commit(&table_log_dir, 10).unwrap();
        assert_eq!(log_path.version, 10);
        assert!(log_path.is_commit());
        assert_eq!(log_path.extension, "json");
        assert!(matches!(log_path.file_type, LogPathFileType::Commit));
        assert_eq!(log_path.filename, "00000000000000000010.json");
    }

    #[test]
    fn test_new_uuid_parquet_checkpoint() {
        let table_log_dir = table_log_dir_url();
        let log_path = ParsedLogPath::new_uuid_parquet_checkpoint(&table_log_dir, 10).unwrap();

        assert_eq!(log_path.version, 10);
        assert!(log_path.is_checkpoint());
        assert_eq!(log_path.extension, "parquet");
        if let LogPathFileType::UuidCheckpoint(uuid) = &log_path.file_type {
            assert_eq!(uuid.len(), UUID_PART_LEN);
        } else {
            panic!("Expected UuidCheckpoint file type");
        }

        let filename = log_path.filename.to_string();
        let filename_parts: Vec<&str> = filename.split('.').collect();
        assert_eq!(filename_parts.len(), 4);
        assert_eq!(filename_parts[0], "00000000000000000010");
        assert_eq!(filename_parts[1], "checkpoint");
        assert_eq!(filename_parts[2].len(), UUID_PART_LEN);
        assert_eq!(filename_parts[3], "parquet");
    }

    #[test]
    fn test_new_classic_parquet_checkpoint() {
        let table_log_dir = table_log_dir_url();
        let log_path = ParsedLogPath::new_classic_parquet_checkpoint(&table_log_dir, 10).unwrap();

        assert_eq!(log_path.version, 10);
        assert!(log_path.is_checkpoint());
        assert_eq!(log_path.extension, "parquet");
        assert!(matches!(
            log_path.file_type,
            LogPathFileType::SinglePartCheckpoint
        ));
        assert_eq!(log_path.filename, "00000000000000000010.checkpoint.parquet");
    }

    //-------------------------//
    //     Timestamp tests     //
    //-------------------------//

    fn timestamp_conversion_segment_from_snapshot(
        engine: &dyn Engine,
        snapshot: &Snapshot,
        limit: Option<NonZero<usize>>,
    ) -> DeltaResult<LogSegment> {
        LogSegment::for_timestamp_conversion(
            engine.storage_handler().as_ref(),
            snapshot.log_segment().log_root.clone(),
            snapshot.version(),
            limit,
        )
    }

    // Helper to set the file modification timestamp of a file
    fn set_mod_time(mock_table: &LocalMockTable, commit_version: Version, timestamp: i64) {
        let file_name = delta_path_for_version(commit_version, "json")
            .filename()
            .unwrap()
            .to_string();
        let path = mock_table.table_root().join("_delta_log/").join(file_name);
        let file = OpenOptions::new().write(true).open(path).unwrap();

        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp.try_into().unwrap());
        file.set_modified(time).unwrap();
    }

    async fn timestamp_test_mock_table() -> LocalMockTable {
        let mut mock_table = LocalMockTable::new();

        // 0: Has file modification timestamp 50
        mock_table.commit([Action::Metadata(Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        }),
            Action::Protocol(Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(vec![WriterFeature::InCommitTimestamp])).unwrap())
        ]).await;
        set_mod_time(&mock_table, 0, 50);

        // 1: Has file modification timestamp 150
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 1, 150);

        // 2: Has file modification timestamp 250
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 2, 250);

        // 3: Has in-commit timestamp 300, file modification timestamp 350
        mock_table.commit([
            Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(300),
                ..Default::default()
            }),
            Action::Metadata(Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            configuration: HashMap::from_iter([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            ),
                ("delta.inCommitTimestampEnablementVersion".to_string(), "3".to_string()),
                ("delta.inCommitTimestampEnablementTimestamp".to_string(), "300".to_string())]),
            ..Default::default()
        }),
            Action::Protocol(Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(vec![WriterFeature::InCommitTimestamp])).unwrap())
        ]).await;
        set_mod_time(&mock_table, 3, 350);

        // 4: Has in-commit timestamp 400, file modification timestamp 450
        mock_table
            .commit([Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(400),
                ..Default::default()
            })])
            .await;
        set_mod_time(&mock_table, 4, 450);

        mock_table
    }

    #[tokio::test]
    async fn reading_in_commit_timestamp() {
        let mock_table = timestamp_test_mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let log_segment =
            timestamp_conversion_segment_from_snapshot(&engine, &snapshot, None).unwrap();
        let commits = log_segment.ascending_commit_files;

        // File that has no In-commit timestamps
        let mut res = commits[0].read_in_commit_timestamp(&engine);
        assert!(
            matches!(res, Err(InCommitTimestampError::NotFound { version: 0 })),
            "{res:?} failed"
        );

        // File that doesn't exist
        let mut fake_log_path = commits[0].clone();

        let failing_path = if cfg!(windows) {
            "C:\\phony\\path"
        } else {
            "/phony/path"
        };

        fake_log_path.location.location = Url::from_file_path(failing_path).unwrap();
        res = fake_log_path.read_in_commit_timestamp(&engine);
        assert!(
            matches!(
                res,
                Err(InCommitTimestampError::FailedToReadTimestamp {
                    version: 0,
                    source: _
                })
            ),
            "{res:?} failed"
        );

        // Files with In-commit timestamps
        res = commits[3].read_in_commit_timestamp(&engine);
        assert!(matches!(res, Ok(300)), "{res:?}");
        res = commits[4].read_in_commit_timestamp(&engine);
        assert!(matches!(res, Ok(400)), "{res:?}");
    }

    #[tokio::test]
    async fn file_modification_conversion() {
        let mock_table = timestamp_test_mock_table().await;
        let engine = SyncEngine::new();
        let path = Url::from_directory_path(mock_table.table_root()).unwrap();
        let snapshot = Snapshot::try_new(path, &engine, None).unwrap();
        let log_segment =
            timestamp_conversion_segment_from_snapshot(&engine, &snapshot, None).unwrap();
        let commits = log_segment.ascending_commit_files;

        // Read the file modification timestamps
        let ts = commits[0].file_modification_timestamp();
        assert!(matches!(ts, 50));

        let ts = commits[1].file_modification_timestamp();
        assert!(matches!(ts, 150));

        let ts = commits[2].file_modification_timestamp();
        assert!(matches!(ts, 250));

        // Read the in-commit timestamps
        let ts = commits[3].read_in_commit_timestamp(&engine);
        assert!(matches!(ts, Ok(300)));
        let ts = commits[4].read_in_commit_timestamp(&engine);
        assert!(matches!(ts, Ok(400)));
    }
}
