//! Public-facing [`LogPath`] type for representing paths to delta log files.

use url::Url;

use crate::path::{ParsedLogPath, DELTA_LOG_DIR_WITH_SLASH, STAGED_COMMITS_DIR};
use crate::utils::require;
use crate::{FileMeta, FileSize, KernelError, KernelResult, Version};

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
    pub fn try_new(file_meta: FileMeta) -> KernelResult<Self> {
        // TODO: we should avoid the clone
        let parsed = ParsedLogPath::try_from(file_meta.clone())?
            .ok_or_else(|| KernelError::invalid_log_path(&file_meta.location))?;

        require!(
            !parsed.is_unknown(),
            KernelError::invalid_log_path(&file_meta.location)
        );

        Ok(Self(parsed))
    }

    /// Create a commit log path from a commit filename.
    ///
    /// The filename's form selects the directory: `00000000000000000010.json` resolves to
    /// `_delta_log/00000000000000000010.json`, and `00000000000000000010.<uuid>.json` resolves to
    /// `_delta_log/_staged_commits/00000000000000000010.<uuid>.json`. `table_root` points to the
    /// table's root directory. `version` is the catalog's version for the commit and must match
    /// the filename's. Performs no I/O.
    ///
    /// A published filename asserts that this version and all lower versions are already published.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_root` cannot be a base URL, or if `filename` is not a bare
    /// published or staged commit filename for `version`.
    pub fn from_commit_filename(
        table_root: Url,
        version: Version,
        filename: &str,
        last_modified: i64,
        size: FileSize,
    ) -> KernelResult<LogPath> {
        let invalid_commit_filename = || {
            KernelError::invalid_log_path(format!(
                "{filename} is not a published or staged commit filename for version {version}"
            ))
        };
        // A published commit has exactly one valid name for `version`. Any other name must be a
        // staged commit, which `ParsedLogPath` validates against the staged-commit directory.
        let subdir = if filename == format!("{version:020}.json") {
            ""
        } else {
            STAGED_COMMITS_DIR
        };
        let location = Self::log_file_url(table_root, subdir, filename).map_err(|e| match e {
            KernelError::InvalidLogPath(_) => invalid_commit_filename(),
            e => e,
        })?;
        let log_path = Self::try_new(FileMeta {
            location,
            last_modified,
            size,
        })
        .map_err(|e| match e {
            KernelError::InvalidLogPath(_) => invalid_commit_filename(),
            e => e,
        })?;
        require!(log_path.0.version == version, invalid_commit_filename());
        Ok(log_path)
    }

    /// Create a new staged commit log path given the table root, filename, and metadata.
    ///
    /// Use [`Self::from_commit_filename`] for catalog responses, which may contain published
    /// commits.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_root` cannot be a base URL, or if `filename` is not a bare
    /// staged commit filename.
    pub fn staged_commit(
        table_root: Url,
        filename: &str,
        last_modified: i64,
        size: FileSize,
    ) -> KernelResult<LogPath> {
        let commit_path = Self::staged_commit_url(table_root, filename)?;
        let file_meta = FileMeta {
            location: commit_path,
            last_modified,
            size,
        };
        LogPath::try_new(file_meta)
    }

    /// Create the URL for a staged commit file given the table root and filename.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_root` cannot be a base URL, or if `filename` is empty, contains
    /// '%', or does not remain the final path segment after URL resolution.
    pub fn staged_commit_url(table_root: Url, filename: &str) -> KernelResult<Url> {
        Self::log_file_url(table_root, STAGED_COMMITS_DIR, filename)
    }

    /// Resolve the bare `filename` in `subdir` (empty, or ending with '/') of the table's
    /// `_delta_log/` directory.
    ///
    /// Returns an error if `table_root` cannot be a base URL, or if the resolved URL would not
    /// name `filename` itself.
    fn log_file_url(mut table_root: Url, subdir: &str, filename: &str) -> KernelResult<Url> {
        if !table_root.path().ends_with('/') {
            table_root.set_path(&format!("{}/", table_root.path()));
        }
        let dir = table_root
            .join(DELTA_LOG_DIR_WITH_SLASH)
            .and_then(|url| url.join(subdir))
            .map_err(|_| KernelError::invalid_table_location(table_root))?;
        let invalid_log_filename =
            || KernelError::invalid_log_path(format!("not a bare log file name: {filename}"));
        // `Url::join` resolves separators, queries, fragments, and dot segments, strips tabs and
        // newlines, and trims spaces. Require the resolved name to match the supplied filename.
        let url = dir.join(filename).map_err(|_| invalid_log_filename())?;
        // Engines percent-decode URL paths, so an escape would name a different object than
        // `ParsedLogPath` validated.
        require!(
            !filename.is_empty()
                && !filename.contains('%')
                && url.path_segments().and_then(|mut s| s.next_back()) == Some(filename),
            invalid_log_filename()
        );
        Ok(url)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;
    use crate::path::LogPathFileType;

    const TABLE_ROOT: &str = "s3://my-bucket/my-table/";
    const UUID: &str = "3a0d65cd-4a56-49a8-937b-95f9e3ee90e5";

    #[rstest]
    #[case::published(
        "00000000000000000010.json",
        "_delta_log/00000000000000000010.json",
        LogPathFileType::Commit
    )]
    #[case::staged(
        "00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json",
        "_delta_log/_staged_commits/00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json",
        LogPathFileType::StagedCommit
    )]
    fn from_commit_filename_resolves_directory_by_form(
        #[values(TABLE_ROOT, "s3://my-bucket/my-table")] table_root: &str,
        #[case] filename: &str,
        #[case] expected_rel_path: &str,
        #[case] expected_type: LogPathFileType,
    ) {
        let table_root = Url::from_str(table_root).unwrap();
        let log_path =
            LogPath::from_commit_filename(table_root.clone(), 10, filename, 1234, 1024).unwrap();

        let expected = FileMeta {
            location: Url::from_str(TABLE_ROOT)
                .unwrap()
                .join(expected_rel_path)
                .unwrap(),
            last_modified: 1234,
            size: 1024,
        };
        assert_eq!(log_path.0.location, expected);
        assert_eq!(log_path.0.file_type, expected_type);
        assert_eq!(log_path.0.version, 10);
    }

    #[rstest]
    #[case::empty(10, "")]
    #[case::dot(10, ".")]
    #[case::dot_dot(10, "..")]
    #[case::subdir(10, "subdir/00000000000000000010.json")]
    #[case::parent_dir(10, "../00000000000000000010.json")]
    #[case::backslash(10, "..\\00000000000000000010.json")]
    #[case::query(10, "00000000000000000010.json?x")]
    #[case::fragment(10, "00000000000000000010.json#x")]
    #[case::percent_encoded(10, "%2E%2E%2F00000000000000000010.json")]
    // The middle part has a UUID's length, so it parses as a staged commit. Engines
    // percent-decode it into a nested path under `_staged_commits/`.
    #[case::percent_encoded_uuid(
        10,
        "00000000000000000010.%2Fother%2F_delta_log%2Fzzzzzzzzzzzz.json"
    )]
    #[case::absolute_url(10, "http://[bad")]
    // `Url::join` strips tabs and newlines and trims spaces, so these would otherwise resolve to
    // `00000000000000000010.json`.
    #[case::embedded_tab(10, "0000000000000000001\t0.json")]
    #[case::embedded_newline(10, "0000000000000000001\n0.json")]
    #[case::leading_space(10, " 00000000000000000010.json")]
    #[case::invalid_uuid(10, "00000000000000000010.not-a-uuid.json")]
    #[case::non_numeric_version(10, "000000000000000000aa.json")]
    #[case::short_version(10, "10.json")]
    #[case::staged_parquet(
        10,
        "00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.parquet"
    )]
    #[case::crc(10, "00000000000000000010.crc")]
    #[case::checkpoint(10, "00000000000000000010.checkpoint.parquet")]
    #[case::compaction(10, "00000000000000000010.00000000000000000011.compacted.json")]
    #[case::published_version_mismatch(11, "00000000000000000010.json")]
    #[case::staged_version_mismatch(
        11,
        "00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json"
    )]
    fn from_commit_filename_rejects_invalid_filenames(
        #[case] version: Version,
        #[case] filename: &str,
    ) {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let err = LogPath::from_commit_filename(table_root, version, filename, 0, 0).unwrap_err();
        assert!(matches!(err, KernelError::InvalidLogPath(_)), "{err:?}");
    }

    #[rstest]
    fn test_staged_commit_path_creation(
        #[values(TABLE_ROOT, "s3://my-bucket/my-table")] table_root: &str,
    ) {
        let table_root = Url::from_str(table_root).unwrap();
        let filename = format!("00000000000000000010.{UUID}.json");

        let log_path = LogPath::staged_commit(table_root.clone(), &filename, 1234, 1024)
            .expect("Failed to create staged commit log path");

        let expected = FileMeta {
            location: Url::from_str(TABLE_ROOT)
                .unwrap()
                .join(&format!("_delta_log/_staged_commits/{filename}"))
                .unwrap(),
            last_modified: 1234,
            size: 1024,
        };
        assert_eq!(log_path.0.location, expected);
    }

    #[rstest]
    #[case::subdir("subdir/00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json")]
    #[case::invalid_uuid("00000000000000000010.not-a-uuid.json")]
    #[case::non_numeric_version("000000000000000000aa.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json")]
    #[case::parquet("00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.parquet")]
    #[case::published("00000000000000000010.json")]
    #[case::empty("")]
    #[case::query("00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json?x")]
    #[case::percent_encoded_uuid("00000000000000000010.%2Fother%2F_delta_log%2Fzzzzzzzzzzzz.json")]
    fn test_staged_commit_path_creation_failures(#[case] filename: &str) {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let err = LogPath::staged_commit(table_root, filename, 0, 0).unwrap_err();
        assert!(matches!(err, KernelError::InvalidLogPath(_)), "{err:?}");
    }

    #[test]
    fn test_staged_commit_url_rejects_empty_filename() {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let err = LogPath::staged_commit_url(table_root, "").unwrap_err();
        assert!(matches!(err, KernelError::InvalidLogPath(_)), "{err:?}");
    }
}
