//! Public-facing [`LogPath`] type for representing paths to delta log files.

use crate::path::ParsedLogPath;
use crate::utils::require;
use crate::{DeltaResult, Error, FileMeta, FileSize};

use url::Url;

/// A path to a valid delta log file. You can parse a given `FileMeta` into a `LogPath` using
/// [`LogPath::try_new`].
///
/// Today, a `LogPath` is a file in the `_delta_log` directory of a Delta table; in the future,
/// this will expand to support providing inline data in the log path itself.
#[derive(Debug, Clone, PartialEq)]
pub struct LogPath(ParsedLogPath);

impl From<LogPath> for ParsedLogPath {
    fn from(p: LogPath) -> Self {
        p.0
    }
}

impl LogPath {
    /// Attempt to create a `LogPath` from `FileMeta`. This returns an error if the path isn't a
    /// valid log path.
    pub fn try_new(file_meta: FileMeta) -> DeltaResult<Self> {
        // TODO: we should avoid the clone
        let parsed = ParsedLogPath::try_from(file_meta.clone())?
            .ok_or_else(|| Error::invalid_log_path(&file_meta.location))?;

        require!(
            !parsed.is_unknown(),
            Error::invalid_log_path(&file_meta.location)
        );

        Ok(Self(parsed))
    }

    /// Create a new staged commit log path given the table root and filename and metadata. The
    /// table_root must point to the root of the table and end with a '/'.
    pub fn staged_commit(
        table_root: Url,
        filename: &str,
        last_modified: i64,
        size: FileSize,
    ) -> DeltaResult<LogPath> {
        // TODO: we should introduce TablePath/LogPath types which enforce checks like ending '/'
        if !table_root.path().ends_with('/') {
            return Err(Error::invalid_table_location(table_root));
        }

        let commit_path = table_root
            .join("_delta_log/")
            .and_then(|url| url.join("_staged_commits/"))
            .and_then(|url| url.join(filename))
            .map_err(|_| Error::invalid_table_location(table_root))?;

        let file_meta = FileMeta {
            location: commit_path,
            last_modified,
            size,
        };
        LogPath::try_new(file_meta)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::str::FromStr;

    #[test]
    fn test_staged_commit_path_creation() {
        let table_root = Url::from_str("s3://my-bucket/my-table/").unwrap();
        let filename = "00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json";
        let last_modified = 1234567890i64;
        let size = 1024u64;

        let log_path = LogPath::staged_commit(table_root.clone(), filename, last_modified, size)
            .expect("Failed to create staged commit log path");

        let expected_path =
            Url::from_str("s3://my-bucket/my-table/_delta_log/_staged_commits/00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json")
                .unwrap();
        let expected = FileMeta {
            location: expected_path,
            last_modified,
            size,
        };

        let path = log_path.0;
        assert_eq!(path.location, expected);
    }

    #[test]
    fn test_staged_commit_path_creation_failures() {
        let last_modified = 1234567890i64;
        let size = 1024u64;

        // table root not ending with '/'
        let table_root = Url::from_str("s3://my-bucket/my-table").unwrap();
        let filename = "00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json";
        let err =
            LogPath::staged_commit(table_root.clone(), filename, last_modified, size).unwrap_err();
        assert!(matches!(err, Error::InvalidTableLocation(_)));

        // filename with path separators
        let table_root = Url::from_str("s3://my-bucket/my-table/").unwrap();
        let filename = "subdir/00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json";
        LogPath::staged_commit(table_root.clone(), filename, last_modified, size).unwrap_err();

        // incorrect filenames
        let table_root = Url::from_str("s3://my-bucket/my-table/").unwrap();
        let filename = "00000000000000000010.not-a-uuid.json";
        LogPath::staged_commit(table_root.clone(), filename, last_modified, size).unwrap_err();
        let filename = "000000000000000000aa.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json";
        LogPath::staged_commit(table_root.clone(), filename, last_modified, size).unwrap_err();
        let filename = "00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.parquet";
        LogPath::staged_commit(table_root.clone(), filename, last_modified, size).unwrap_err();
    }
}
