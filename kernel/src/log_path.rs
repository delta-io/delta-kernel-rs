//! Public-facing [`LogPath`] type for representing paths to delta log files.

use url::Url;

use crate::path::ParsedLogPath;
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

    /// Create a commit log path from a catalog-reported commit filename, which may name a staged
    /// commit or one the catalog already published.
    ///
    /// The filename's form selects the directory: `00000000000000000010.json` resolves to
    /// `_delta_log/00000000000000000010.json`, and `00000000000000000010.<uuid>.json` resolves to
    /// `_delta_log/_staged_commits/00000000000000000010.<uuid>.json`. `table_root` must point to
    /// the root of the table and end with a '/'. `version` is the catalog's version for this
    /// commit, and `last_modified` and `size` describe the referenced file. Performs no I/O.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_root` does not end with '/', if `filename` is not a bare
    /// published or staged commit filename, or if the filename's version differs from `version`.
    pub fn from_commit_filename(
        table_root: Url,
        version: Version,
        filename: &str,
        last_modified: i64,
        size: FileSize,
    ) -> KernelResult<LogPath> {
        // `Url::join` interprets separators, queries, fragments, and percent-escapes, so such a
        // filename could resolve outside the chosen directory or to a different file.
        require!(
            !filename.is_empty() && !filename.contains(['/', '\\', '?', '#', '%']),
            KernelError::invalid_log_path(format!("invalid commit filename: {filename}"))
        );
        // Only the published form `<version>.json` has two dot-separated parts. `ParsedLogPath`
        // validates the remaining grammar against the chosen directory.
        let subdir = if filename.split('.').count() == 2 {
            ""
        } else {
            "_staged_commits/"
        };
        let location = Self::log_dir_url(table_root, subdir)?
            .join(filename)
            .map_err(|_| KernelError::invalid_log_path(filename))?;
        let log_path = Self::try_new(FileMeta {
            location,
            last_modified,
            size,
        })?;
        let parsed = &log_path.0;
        // `Url::join` normalizes dot segments, so also require that the parsed path kept the
        // caller's filename.
        require!(
            parsed.is_commit() && parsed.filename == filename,
            KernelError::invalid_log_path(format!("not a commit filename: {filename}"))
        );
        require!(
            parsed.version == version,
            KernelError::invalid_log_path(format!(
                "commit filename {filename} does not match expected version {version}"
            ))
        );
        Ok(log_path)
    }

    /// Create a new staged commit log path given the table root and filename and metadata. The
    /// table_root must point to the root of the table and end with a '/'.
    ///
    /// Use [`Self::from_commit_filename`] for catalog responses, which may contain published
    /// commits.
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

    /// Create the URL for a staged commit file given the table root and filename. The table_root
    /// must point to the root of the table and end with a '/'.
    pub fn staged_commit_url(table_root: Url, filename: &str) -> KernelResult<Url> {
        Self::log_dir_url(table_root.clone(), "_staged_commits/")?
            .join(filename)
            .map_err(|_| KernelError::invalid_table_location(table_root))
    }

    /// Resolve `subdir` (empty, or ending with '/') under the table's `_delta_log/` directory.
    /// Returns an error if `table_root` does not end with '/'.
    fn log_dir_url(table_root: Url, subdir: &str) -> KernelResult<Url> {
        // TODO: we should introduce TablePath/LogPath types which enforce checks like ending '/'
        if !table_root.path().ends_with('/') {
            return Err(KernelError::invalid_table_location(table_root));
        }
        table_root
            .join("_delta_log/")
            .and_then(|url| url.join(subdir))
            .map_err(|_| KernelError::invalid_table_location(table_root))
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
        #[case] filename: &str,
        #[case] expected_rel_path: &str,
        #[case] expected_type: LogPathFileType,
    ) {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let log_path =
            LogPath::from_commit_filename(table_root.clone(), 10, filename, 1234, 1024).unwrap();

        let expected = FileMeta {
            location: table_root.join(expected_rel_path).unwrap(),
            last_modified: 1234,
            size: 1024,
        };
        assert_eq!(log_path.0.location, expected);
        assert_eq!(log_path.0.file_type, expected_type);
        assert_eq!(log_path.0.version, 10);
    }

    #[rstest]
    #[case::empty("")]
    #[case::subdir("subdir/00000000000000000010.json")]
    #[case::parent_dir("../00000000000000000010.json")]
    #[case::backslash("..\\00000000000000000010.json")]
    #[case::query("00000000000000000010.json?x")]
    #[case::fragment("00000000000000000010.json#x")]
    #[case::percent_encoded("%2E%2E%2F00000000000000000010.json")]
    #[case::invalid_uuid("00000000000000000010.not-a-uuid.json")]
    #[case::non_numeric_version("000000000000000000aa.json")]
    #[case::short_version("10.json")]
    #[case::staged_parquet("00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.parquet")]
    #[case::crc("00000000000000000010.crc")]
    #[case::checkpoint("00000000000000000010.checkpoint.parquet")]
    #[case::compaction("00000000000000000010.00000000000000000011.compacted.json")]
    fn from_commit_filename_rejects_non_commit_filenames(#[case] filename: &str) {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let err = LogPath::from_commit_filename(table_root, 10, filename, 0, 0).unwrap_err();
        assert!(matches!(err, KernelError::InvalidLogPath(_)), "{err:?}");
    }

    #[rstest]
    #[case::published("00000000000000000010.json")]
    #[case::staged("00000000000000000010.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json")]
    fn from_commit_filename_rejects_version_mismatch(#[case] filename: &str) {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let err = LogPath::from_commit_filename(table_root, 11, filename, 0, 0).unwrap_err();
        assert!(err
            .to_string()
            .contains("does not match expected version 11"));
    }

    #[test]
    fn from_commit_filename_rejects_table_root_without_trailing_slash() {
        let table_root = Url::from_str("s3://my-bucket/my-table").unwrap();
        let err = LogPath::from_commit_filename(table_root, 10, "00000000000000000010.json", 0, 0)
            .unwrap_err();
        assert!(matches!(err, KernelError::InvalidTableLocation(_)));
    }

    #[test]
    fn test_staged_commit_path_creation() {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        let filename = format!("00000000000000000010.{UUID}.json");

        let log_path = LogPath::staged_commit(table_root.clone(), &filename, 1234, 1024)
            .expect("Failed to create staged commit log path");

        let expected = FileMeta {
            location: table_root
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
    fn test_staged_commit_path_creation_failures(#[case] filename: &str) {
        let table_root = Url::from_str(TABLE_ROOT).unwrap();
        LogPath::staged_commit(table_root, filename, 0, 0).unwrap_err();
    }

    #[test]
    fn test_staged_commit_rejects_table_root_without_trailing_slash() {
        let table_root = Url::from_str("s3://my-bucket/my-table").unwrap();
        let filename = format!("00000000000000000010.{UUID}.json");
        let err = LogPath::staged_commit(table_root, &filename, 0, 0).unwrap_err();
        assert!(matches!(err, KernelError::InvalidTableLocation(_)));
    }
}
