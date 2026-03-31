//! Commit metadata types for the committer module.

#[cfg(any(test, feature = "test-utils"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;

use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::path::LogRoot;
use crate::{DeltaResult, Version};

/// The type of commit operation being performed. This communicates to the committer whether this
/// is a table creation or a write to an existing table, and whether the table is catalog-managed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitType {
    /// Creating a new table via filesystem (no catalog involvement).
    PathBasedCreate,
    /// Creating a new catalog-managed table.
    CatalogManagedCreate,
    /// Writing to an existing path-based table.
    PathBasedWrite,
    /// Writing to an existing catalog-managed table.
    CatalogManagedWrite,
    /// Upgrading an existing path-based table to catalog-managed. Not currently supported.
    UpgradeToCatalogManaged,
    /// Downgrading an existing catalog-managed table to path-based. Not currently supported.
    DowngradeToPathBased,
}

impl CommitType {
    /// Returns `true` if this is a create-table commit (version 0).
    pub fn is_create(&self) -> bool {
        matches!(self, Self::PathBasedCreate | Self::CatalogManagedCreate)
    }

    /// Returns `true` if this is a catalog-managed commit.
    pub fn is_catalog_managed(&self) -> bool {
        matches!(
            self,
            Self::CatalogManagedCreate | Self::CatalogManagedWrite | Self::UpgradeToCatalogManaged
        )
    }
}

/// The protocol and metadata state for this commit. Groups the read snapshot state (if any)
/// and the new state being committed (if any).
#[derive(Debug)]
#[allow(dead_code)] // Fields read by delta-kernel-unity-catalog via effective_protocol/metadata
pub struct CommitProtocolMetadata {
    /// Existing table protocol from read snapshot. `None` for create-table.
    pub(crate) read_protocol: Option<Protocol>,
    /// Existing table metadata from read snapshot. `None` for create-table.
    pub(crate) read_metadata: Option<Metadata>,
    /// New protocol being committed. `Some` for create-table and future ALTER TABLE.
    pub(crate) new_protocol: Option<Protocol>,
    /// New metadata being committed. `Some` for create-table and future ALTER TABLE.
    pub(crate) new_metadata: Option<Metadata>,
}

/// `CommitMetadata` bundles the metadata about a commit operation. This includes the commit path,
/// version, and protocol/metadata state of the table being committed to. Catalog committers can
/// use the protocol and metadata getters to validate or inspect the commit.
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
    pub(crate) commit_type: CommitType,
    pub(crate) in_commit_timestamp: i64,
    pub(crate) max_published_version: Option<Version>,
    /// Protocol and metadata state for this commit.
    #[allow(dead_code)] // Read by delta-kernel-unity-catalog for commit validation
    pub(crate) protocol_metadata: CommitProtocolMetadata,
}

impl CommitMetadata {
    pub(crate) fn new(
        log_root: LogRoot,
        version: Version,
        commit_type: CommitType,
        in_commit_timestamp: i64,
        max_published_version: Option<Version>,
        protocol_metadata: CommitProtocolMetadata,
    ) -> Self {
        Self {
            log_root,
            version,
            commit_type,
            in_commit_timestamp,
            max_published_version,
            protocol_metadata,
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

    /// The type of commit operation being performed.
    pub fn commit_type(&self) -> CommitType {
        self.commit_type
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

    /// The root URL of the table being committed to.
    pub fn table_root(&self) -> &Url {
        self.log_root.table_root()
    }

    /// Returns the effective protocol for this commit. Prefers new_protocol (create-table / ALTER
    /// TABLE), falling back to the read snapshot's protocol.
    #[allow(dead_code)] // Used by delta-kernel-unity-catalog for commit validation
    pub(crate) fn effective_protocol(&self) -> Option<&Protocol> {
        let pm = &self.protocol_metadata;
        pm.new_protocol.as_ref().or(pm.read_protocol.as_ref())
    }

    /// Returns the effective metadata for this commit. Prefers new_metadata (create-table / ALTER
    /// TABLE), falling back to the read snapshot's metadata.
    #[allow(dead_code)] // Used by delta-kernel-unity-catalog for commit validation
    pub(crate) fn effective_metadata(&self) -> Option<&Metadata> {
        let pm = &self.protocol_metadata;
        pm.new_metadata.as_ref().or(pm.read_metadata.as_ref())
    }

    /// Creates a new `CommitMetadata` for the given `table_root` and `version`. Test-only.
    ///
    /// Uses a default modern protocol (empty features) and empty metadata.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_unchecked(table_root: Url, version: Version) -> DeltaResult<Self> {
        let log_root = crate::path::LogRoot::new(table_root)?;
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new())?;
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new())?;
        Ok(Self::new(
            log_root,
            version,
            CommitType::PathBasedWrite,
            0,
            None,
            CommitProtocolMetadata {
                read_protocol: Some(protocol),
                read_metadata: Some(metadata),
                new_protocol: None,
                new_metadata: None,
            },
        ))
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

    use std::sync::Arc;

    use crate::path::LogRoot;
    use url::Url;

    #[test]
    fn test_commit_metadata() {
        let table_root = Url::parse("s3://my-bucket/path/to/table/").unwrap();
        let log_root = LogRoot::new(table_root).unwrap();
        let version = 42;
        let ts = 1234;
        let max_published_version = Some(42);
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new()).unwrap();
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();

        let commit_metadata = CommitMetadata::new(
            log_root,
            version,
            CommitType::PathBasedWrite,
            ts,
            max_published_version,
            CommitProtocolMetadata {
                read_protocol: Some(protocol),
                read_metadata: Some(metadata),
                new_protocol: None,
                new_metadata: None,
            },
        );

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
            "Staged path should start with the correct prefix, got: {staged_path_str}"
        );
        assert!(
            staged_path_str.ends_with(".json"),
            "Staged path should end with .json, got: {staged_path_str}"
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
