//! Types for publishing catalog commits to the Delta log.

use url::Url;

use crate::path::{LogPathFileType, LogRoot, ParsedLogPath};
use crate::utils::require;
use crate::{DeltaResult, Error, FileMeta, Version};

/// A catalog commit that has been ratified by the catalog but not yet published to the Delta log.
///
/// Catalog commits are staged commits stored in `_delta_log/_staged_commits/` that have been
/// ratified (accepted) by the catalog but not yet copied to the main delta log as published
/// commits. This struct provides the information needed to publish a catalog commit.
///
/// See [`Committer::publish`] for details on the publish operation.
///
/// [`Committer::publish`]: super::Committer::publish
#[derive(Debug, Clone)]
pub struct CatalogCommit {
    version: Version,
    location: Url,
    published_location: Url,
}

#[allow(dead_code)] // pub(crate) constructor will be used in future PRs
impl CatalogCommit {
    pub(crate) fn new(
        log_root: &LogRoot,
        catalog_commit: &ParsedLogPath<FileMeta>,
    ) -> DeltaResult<Self> {
        require!(
            catalog_commit.file_type == LogPathFileType::StagedCommit,
            Error::Generic(format!(
                "Cannot construct CatalogCommit. Expected a StagedCommit, got {:?}",
                catalog_commit.file_type
            ))
        );
        Ok(Self {
            version: catalog_commit.version,
            location: catalog_commit.location.location.clone(),
            published_location: log_root.new_commit_path(catalog_commit.version)?.location,
        })
    }

    /// The version of this catalog commit.
    pub fn version(&self) -> Version {
        self.version
    }

    /// The location of the staged catalog commit file
    /// (e.g., `s3://bucket/table/_delta_log/_staged_commits/00000000000000000001.uuid.json`).
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// The target location where this commit should be published
    /// (e.g., `s3://bucket/table/_delta_log/00000000000000000001.json`).
    pub fn published_location(&self) -> &Url {
        &self.published_location
    }
}

/// Metadata required for publishing catalog commits to the Delta log.
///
/// `PublishMetadata` bundles all the information needed to publish catalog commits: the snapshot
/// version up to which commits should be published, and the list of catalog commits themselves.
///
/// # Invariants
///
/// The following invariants are enforced at construction time:
/// - `ascending_catalog_commits` must be non-empty
/// - `ascending_catalog_commits` must be contiguous (no version gaps)
/// - The last catalog commit version must equal `snapshot_version`
///
/// See [`Committer::publish`] for details on the publish operation.
///
/// [`Committer::publish`]: super::Committer::publish
pub struct PublishMetadata {
    snapshot_version: Version,
    ascending_catalog_commits: Vec<CatalogCommit>,
}

#[allow(dead_code)] // pub(crate) constructor will be used in future PRs
impl PublishMetadata {
    pub(crate) fn new(
        snapshot_version: Version,
        ascending_catalog_commits: Vec<CatalogCommit>,
    ) -> DeltaResult<Self> {
        Self::validate_catalog_commits_contiguous(&ascending_catalog_commits)?;
        Self::validate_catalog_commits_end_with_snapshot_version(
            &ascending_catalog_commits,
            snapshot_version,
        )?;
        Ok(Self {
            snapshot_version,
            ascending_catalog_commits,
        })
    }

    /// The snapshot version up to which all catalog commits must be published.
    pub fn snapshot_version(&self) -> Version {
        self.snapshot_version
    }

    /// The list of contiguous catalog commits to be published, in ascending order of version.
    pub fn ascending_catalog_commits(&self) -> &[CatalogCommit] {
        &self.ascending_catalog_commits
    }

    fn validate_catalog_commits_contiguous(
        ascending_catalog_commits: &[CatalogCommit],
    ) -> DeltaResult<()> {
        ascending_catalog_commits
            .windows(2)
            .all(|c| c[0].version() + 1 == c[1].version())
            .then_some(())
            .ok_or_else(|| {
                Error::Generic(format!(
                    "Catalog commits must be contiguous: got versions {:?}",
                    ascending_catalog_commits
                        .iter()
                        .map(|c| c.version())
                        .collect::<Vec<_>>()
                ))
            })
    }

    fn validate_catalog_commits_end_with_snapshot_version(
        ascending_catalog_commits: &[CatalogCommit],
        snapshot_version: Version,
    ) -> DeltaResult<()> {
        match ascending_catalog_commits.last().map(|c| c.version()) {
            Some(v) if v == snapshot_version => Ok(()),
            Some(v) => Err(Error::Generic(format!(
                "Catalog commits must end with snapshot version {snapshot_version}, but got {v}"
            ))),
            None => Err(Error::Generic(format!(
                "Catalog commits are empty, expected snapshot version {snapshot_version}"
            ))),
        }
    }
}

/// The result of a [`Committer::publish`] operation.
///
/// This enum distinguishes between committers that support publishing (catalog committers) and
/// those that do not (e.g., [`FileSystemCommitter`]).
///
/// [`Committer::publish`]: super::Committer::publish
/// [`FileSystemCommitter`]: super::FileSystemCommitter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishResult {
    /// Publishing is not applicable for this committer type.
    ///
    /// This is returned by committers that don't support publishing, such as
    /// [`FileSystemCommitter`] for non-catalog-managed tables. Callers should check
    /// [`Committer::is_catalog_committer`] before calling [`Committer::publish`] to avoid
    /// this case.
    ///
    /// [`FileSystemCommitter`]: super::FileSystemCommitter
    /// [`Committer::is_catalog_committer`]: super::Committer::is_catalog_committer
    /// [`Committer::publish`]: super::Committer::publish
    NotApplicable,
    /// Successfully published all catalog commits to the Delta log.
    Success,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::assert_result_error_with_message;

    fn log_root() -> LogRoot {
        let table_root = Url::parse("memory:///").unwrap();
        LogRoot::new(table_root).unwrap()
    }

    #[test]
    fn test_catalog_commit_construction_with_valid_staged_commit() {
        let log_root = log_root();
        let parsed_staged_commit =
            ParsedLogPath::create_parsed_staged_commit(log_root.table_root(), 10);
        let catalog_commit = CatalogCommit::new(&log_root, &parsed_staged_commit).unwrap();
        assert_eq!(catalog_commit.version(), 10);
        assert!(catalog_commit
            .location()
            .as_str()
            .starts_with("memory:///_delta_log/_staged_commits/00000000000000000010"));
        assert_eq!(
            catalog_commit.published_location().as_str(),
            "memory:///_delta_log/00000000000000000010.json"
        );
    }

    #[test]
    fn test_catalog_commit_construction_rejects_non_staged_commit() {
        let log_root = log_root();
        let parsed_commit =
            ParsedLogPath::create_parsed_published_commit(log_root.table_root(), 10);

        assert_result_error_with_message(
            CatalogCommit::new(&log_root, &parsed_commit),
            "Cannot construct CatalogCommit. Expected a StagedCommit, got Commit",
        )
    }

    fn create_catalog_commits(versions: &[Version]) -> Vec<CatalogCommit> {
        let log_root = log_root();
        versions
            .iter()
            .map(|v| {
                let parsed_staged_commit =
                    ParsedLogPath::create_parsed_staged_commit(log_root.table_root(), *v);
                CatalogCommit::new(&log_root, &parsed_staged_commit).unwrap()
            })
            .collect()
    }

    #[test]
    fn test_publish_metadata_construction_with_valid_commits() {
        let catalog_commits = create_catalog_commits(&[10, 11, 12]);
        let publish_metadata = PublishMetadata::new(12, catalog_commits).unwrap();
        assert_eq!(publish_metadata.snapshot_version(), 12);
        assert_eq!(publish_metadata.ascending_catalog_commits().len(), 3);
    }

    #[test]
    fn test_publish_metadata_construction_rejects_empty_commits() {
        assert_result_error_with_message(
            PublishMetadata::new(12, vec![]),
            "Catalog commits are empty, expected snapshot version 12",
        )
    }

    #[test]
    fn test_publish_metadata_construction_rejects_non_contiguous_commits() {
        let catalog_commits = create_catalog_commits(&[10, 12]);
        assert_result_error_with_message(
            PublishMetadata::new(12, catalog_commits),
            "Catalog commits must be contiguous: got versions [10, 12]",
        )
    }

    #[test]
    fn test_publish_metadata_construction_rejects_commits_not_ending_with_snapshot_version() {
        let catalog_commits = create_catalog_commits(&[10, 11]);
        assert_result_error_with_message(
            PublishMetadata::new(12, catalog_commits),
            "Catalog commits must end with snapshot version 12, but got 11",
        )
    }
}
