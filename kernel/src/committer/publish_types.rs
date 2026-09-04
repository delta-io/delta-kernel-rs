//! Types for publishing catalog commits to the Delta log.

use url::Url;

use crate::path::{LogPathFileType, ParsedLogPath};
use crate::utils::require;
use crate::{DeltaResult, FileMeta, KernelError, Version};

/// Invalid versions in a request to publish catalog commits.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    /// Adjacent catalog commits are not in contiguous ascending order.
    #[error("Catalog commits must be contiguous: got versions {versions:?}")]
    NonContiguous {
        /// Commit versions in the order supplied by the caller.
        versions: Vec<Version>,
    },
    /// The last catalog commit does not match the requested publication version.
    #[error("Catalog commits must end with snapshot version {expected}, but got {actual}")]
    EndVersionMismatch {
        /// Requested publication version.
        expected: Version,
        /// Last supplied commit version.
        actual: Version,
    },
    /// No catalog commits were supplied for publication.
    #[error("Catalog commits are empty, expected snapshot version {version}")]
    Empty {
        /// Requested publication version.
        version: Version,
    },
}

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

impl CatalogCommit {
    #[allow(dead_code)] // pub(crate) constructor will be used in future PRs
    pub(crate) fn try_new(
        log_root: &Url,
        catalog_commit: &ParsedLogPath<FileMeta>,
    ) -> DeltaResult<Self> {
        require!(
            catalog_commit.file_type == LogPathFileType::StagedCommit,
            KernelError::InvalidLogPath(format!(
                "Cannot construct CatalogCommit. Expected a StagedCommit, got {:?}",
                catalog_commit.file_type
            ))
        );
        Ok(Self {
            version: catalog_commit.version,
            location: catalog_commit.location.location.clone(),
            published_location: log_root
                .join(&format!("{:020}.json", catalog_commit.version))
                .map_err(crate::KernelError::from)?,
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

#[cfg(any(test, feature = "test-utils"))]
impl CatalogCommit {
    /// Creates a new `CatalogCommit` with explicit locations. Test-only.
    pub fn new_unchecked(version: Version, location: Url, published_location: Url) -> Self {
        Self {
            version,
            location,
            published_location,
        }
    }
}

/// Metadata required for publishing catalog commits to the Delta log.
///
/// `PublishMetadata` bundles all the information needed to publish catalog commits: the version up
/// to which commits should be published, and the list of catalog commits themselves.
///
/// # Invariants
///
/// The following invariants are enforced at construction time:
/// - `commits_to_publish` must be non-empty
/// - `commits_to_publish` must be contiguous (no version gaps) in ascending order of version
/// - The last catalog commit version must equal `publish_to_version`
///
/// See [`Committer::publish`] for details on the publish operation.
///
/// [`Committer::publish`]: super::Committer::publish
pub struct PublishMetadata {
    publish_to_version: Version,
    commits_to_publish: Vec<CatalogCommit>,
}

impl PublishMetadata {
    /// Creates a new `PublishMetadata` with the given publish to version and catalog commits.
    #[allow(dead_code)] // constructor will be used in future PRs
    pub fn try_new(
        publish_to_version: Version,
        commits_to_publish: Vec<CatalogCommit>,
    ) -> DeltaResult<Self> {
        Self::validate_contiguous(&commits_to_publish)?;
        Self::validate_end_version(&commits_to_publish, publish_to_version)?;
        Ok(Self {
            publish_to_version,
            commits_to_publish,
        })
    }

    /// The snapshot version up to which all catalog commits must be published.
    pub fn publish_version(&self) -> Version {
        self.publish_to_version
    }

    /// The list of contiguous catalog commits to be published, in ascending order of version.
    pub fn commits_to_publish(&self) -> &[CatalogCommit] {
        &self.commits_to_publish
    }

    fn validate_contiguous(commits_to_publish: &[CatalogCommit]) -> DeltaResult<()> {
        commits_to_publish
            .windows(2)
            .all(|commits| commits[0].version().checked_add(1) == Some(commits[1].version()))
            .then_some(())
            .ok_or_else(|| {
                KernelError::Publish(PublishError::NonContiguous {
                    versions: commits_to_publish
                        .iter()
                        .map(|commit| commit.version())
                        .collect(),
                })
            })
            .map_err(crate::Error::from)
    }

    fn validate_end_version(
        commits_to_publish: &[CatalogCommit],
        publish_to_version: Version,
    ) -> DeltaResult<()> {
        match commits_to_publish.last().map(|c| c.version()) {
            Some(v) if v == publish_to_version => Ok(()),
            Some(actual) => Err(KernelError::Publish(PublishError::EndVersionMismatch {
                expected: publish_to_version,
                actual,
            })
            .into()),
            None => Err(KernelError::Publish(PublishError::Empty {
                version: publish_to_version,
            })
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unit_test_utils::assert_result_error_with_message;

    fn table_root() -> Url {
        Url::parse("memory:///").unwrap()
    }

    fn log_root() -> Url {
        table_root().join("_delta_log/").unwrap()
    }

    #[test]
    fn test_catalog_commit_try_new_with_valid_staged_commit() {
        let parsed_staged_commit = ParsedLogPath::create_parsed_staged_commit(&table_root(), 10);
        let catalog_commit = CatalogCommit::try_new(&log_root(), &parsed_staged_commit).unwrap();
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
    fn test_catalog_commit_try_new_rejects_non_staged_commit() {
        let parsed_commit = ParsedLogPath::create_parsed_published_commit(&table_root(), 10);

        assert_result_error_with_message(
            CatalogCommit::try_new(&log_root(), &parsed_commit),
            "Cannot construct CatalogCommit. Expected a StagedCommit, got Commit",
        )
    }

    fn create_catalog_commits(versions: &[Version]) -> Vec<CatalogCommit> {
        let table_root = table_root();
        let log_root = log_root();
        versions
            .iter()
            .map(|v| {
                let parsed_staged_commit =
                    ParsedLogPath::create_parsed_staged_commit(&table_root, *v);
                CatalogCommit::try_new(&log_root, &parsed_staged_commit).unwrap()
            })
            .collect()
    }

    #[test]
    fn test_publish_metadata_construction_with_valid_commits() {
        let catalog_commits = create_catalog_commits(&[10, 11, 12]);
        let publish_metadata = PublishMetadata::try_new(12, catalog_commits).unwrap();
        assert_eq!(publish_metadata.publish_version(), 12);
        assert_eq!(publish_metadata.commits_to_publish().len(), 3);
    }

    #[rstest::rstest]
    #[case::empty(vec![], PublishError::Empty { version: 12 })]
    #[case::gap(vec![10, 12], PublishError::NonContiguous { versions: vec![10, 12] })]
    #[case::duplicate(vec![12, 12], PublishError::NonContiguous { versions: vec![12, 12] })]
    #[case::overflow(vec![u64::MAX, 0], PublishError::NonContiguous { versions: vec![u64::MAX, 0] })]
    #[case::wrong_end(vec![10, 11], PublishError::EndVersionMismatch { expected: 12, actual: 11 })]
    fn test_publish_metadata_rejects_invalid_versions(
        #[case] versions: Vec<Version>,
        #[case] expected: PublishError,
    ) {
        let result = PublishMetadata::try_new(12, create_catalog_commits(&versions));
        assert!(
            matches!(result, Err(crate::Error::Kernel(KernelError::Publish(actual)))
            if actual == expected)
        );
    }
}
