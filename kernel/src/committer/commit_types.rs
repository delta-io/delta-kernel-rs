//! Commit metadata types for the committer module.

use url::Url;

use crate::path::LogRoot;
use crate::{DeltaResult, Version};

/// `CommitMetadata` bundles the metadata about a commit operation. This currently includes the
/// commit path and version but will expand to things like `Protocol`, `Metadata`, etc. to allow
/// for catalogs to understand/cache/persist more information about the table at commit time.
///
/// Note that this struct cannot be constructed. It is handed to the [`Committer`] (in the
/// [`commit`] method) by the kernel when a transaction is being committed.
///
/// See the [module-level documentation] for more details.
///
/// [`Committer`]: super::Committer
/// [`commit`]: super::Committer::commit
/// [module-level documentation]: crate::committer
#[derive(Debug)]
pub struct CommitMetadata {
    pub(crate) log_root: LogRoot,
    pub(crate) version: Version,
    pub(crate) in_commit_timestamp: i64,
    pub(crate) max_published_version: Option<Version>,
    // in the future this will include Protocol, Metadata, CommitInfo, Domain Metadata, etc.
}

impl CommitMetadata {
    pub(crate) fn new(
        log_root: LogRoot,
        version: Version,
        in_commit_timestamp: i64,
        max_published_version: Option<Version>,
    ) -> Self {
        Self {
            log_root,
            version,
            in_commit_timestamp,
            max_published_version,
        }
    }

    /// The commit path is the absolute path (e.g. s3://bucket/table/_delta_log/{version}.json) to
    /// the published delta file for this commit.
    pub fn published_commit_path(&self) -> DeltaResult<Url> {
        self.log_root
            .new_commit_path(self.version)
            .map(|p| p.location)
    }

    /// The staged commit path is the absolute path (e.g.
    /// s3://bucket/table/_delta_log/{version}.{uuid}.json) to the staged commit file.
    pub fn staged_commit_path(&self) -> DeltaResult<Url> {
        self.log_root
            .new_staged_commit_path(self.version)
            .map(|p| p.location)
    }

    /// The version to which the transaction is being committed.
    pub fn version(&self) -> Version {
        self.version
    }

    /// The in-commit timestamp for the commit. Note that this may differ from the actual commit
    /// file modification time.
    pub fn in_commit_timestamp(&self) -> i64 {
        self.in_commit_timestamp
    }

    /// The maximum published version of the table.
    pub fn max_published_version(&self) -> Option<Version> {
        self.max_published_version
    }

    pub fn table_root(&self) -> &Url {
        self.log_root.table_root()
    }
}

/// `CommitResponse` is the result of committing a transaction via a catalog. The committer uses
/// this type to indicate whether or not the commit was successful or conflicted. The kernel then
/// transforms the associated [`Transaction`] into the appropriate state.
///
/// If the commit was successful, the committer returns `CommitResponse::Committed` with the commit
/// version set. If the commit conflicted (e.g. another writer committed to the same version), the
/// Committer returns `CommitResponse::Conflict` with the version that was attempted.
///
/// [`Transaction`]: crate::transaction::Transaction
#[derive(Debug)]
pub enum CommitResponse {
    Committed { file_meta: crate::FileMeta },
    Conflict { version: Version },
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::path::LogRoot;
    use url::Url;

    #[test]
    fn test_commit_metadata() {
        let table_root = Url::parse("s3://my-bucket/path/to/table/").unwrap();
        let log_root = LogRoot::new(table_root).unwrap();
        let version = 42;
        let ts = 1234;
        let max_published_version = Some(42);

        let commit_metadata = CommitMetadata::new(log_root, version, ts, max_published_version);

        // version
        assert_eq!(commit_metadata.version(), 42);
        // in_commit_timestamp
        assert_eq!(commit_metadata.in_commit_timestamp(), 1234);
        // max_published_version
        assert_eq!(commit_metadata.max_published_version(), Some(42));

        // published commit path
        let published_path = commit_metadata.published_commit_path().unwrap();
        assert_eq!(
            published_path.as_str(),
            "s3://my-bucket/path/to/table/_delta_log/00000000000000000042.json"
        );

        // staged commit path
        let staged_path = commit_metadata.staged_commit_path().unwrap();
        let staged_path_str = staged_path.as_str();

        assert!(
            staged_path_str.starts_with(
                "s3://my-bucket/path/to/table/_delta_log/_staged_commits/00000000000000000042."
            ),
            "Staged path should start with the correct prefix, got: {}",
            staged_path_str
        );
        assert!(
            staged_path_str.ends_with(".json"),
            "Staged path should end with .json, got: {}",
            staged_path_str
        );
        let uuid_str = staged_path_str
            .strip_prefix(
                "s3://my-bucket/path/to/table/_delta_log/_staged_commits/00000000000000000042.",
            )
            .and_then(|s| s.strip_suffix(".json"))
            .expect("Staged path should have expected format");
        uuid::Uuid::parse_str(uuid_str).expect("Staged path should contain a valid UUID");
    }
}
