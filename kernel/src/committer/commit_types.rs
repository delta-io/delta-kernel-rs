//! Commit metadata types for the committer module.

use std::collections::HashMap;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;

use url::Url;

use crate::actions::{DomainMetadata, Metadata, Protocol};
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
    // TODO: Wire these up when ALTER TABLE SET TBLPROPERTIES is supported.
    /// Upgrading an existing path-based table to catalog-managed. Not currently supported.
    #[allow(dead_code)]
    UpgradeToCatalogManaged,
    /// Downgrading an existing catalog-managed table to path-based. Not currently supported.
    #[allow(dead_code)]
    DowngradeToPathBased,
}

impl CommitType {
    /// Returns `true` if this is a create-table commit (version 0).
    pub fn is_create(&self) -> bool {
        matches!(self, Self::PathBasedCreate | Self::CatalogManagedCreate)
    }

    /// Returns `true` if this commit includes a catalog-managed operation,
    /// including upgrade/downgrade.
    pub fn requires_catalog_committer(&self) -> bool {
        matches!(
            self,
            Self::CatalogManagedCreate
                | Self::CatalogManagedWrite
                | Self::UpgradeToCatalogManaged
                | Self::DowngradeToPathBased
        )
    }
}

/// The protocol and metadata state for this commit. Groups the read snapshot state (if any)
/// and the new state being committed (if any).
#[derive(Debug)]
#[allow(dead_code)] // Fields read by delta-kernel-unity-catalog via effective_protocol/metadata
pub(crate) struct CommitProtocolMetadata {
    /// Existing table protocol from read snapshot. `None` for create-table.
    read_protocol: Option<Protocol>,
    /// Existing table metadata from read snapshot. `None` for create-table.
    read_metadata: Option<Metadata>,
    /// New protocol being committed. `Some` for create-table and future ALTER TABLE.
    new_protocol: Option<Protocol>,
    /// New metadata being committed. `Some` for create-table and future ALTER TABLE.
    new_metadata: Option<Metadata>,
}

impl CommitProtocolMetadata {
    pub(crate) fn try_new(
        read_protocol: Option<Protocol>,
        read_metadata: Option<Metadata>,
        new_protocol: Option<Protocol>,
        new_metadata: Option<Metadata>,
    ) -> DeltaResult<Self> {
        if read_protocol.is_some() != read_metadata.is_some() {
            return Err(crate::Error::generic(
                "read_protocol and read_metadata must both be present or both be absent",
            ));
        }
        if read_protocol.is_none() && new_protocol.is_none() {
            return Err(crate::Error::generic(
                "CommitProtocolMetadata requires at least one protocol (read or new)",
            ));
        }
        if read_metadata.is_none() && new_metadata.is_none() {
            return Err(crate::Error::generic(
                "CommitProtocolMetadata requires at least one metadata (read or new)",
            ));
        }
        Ok(Self {
            read_protocol,
            read_metadata,
            new_protocol,
            new_metadata,
        })
    }
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
    pub(crate) protocol_metadata: CommitProtocolMetadata,
    /// Domain metadata actions in this commit (additions and removals).
    pub(crate) domain_metadata_changes: Vec<DomainMetadata>,
}

impl CommitMetadata {
    pub(crate) fn new(
        log_root: LogRoot,
        version: Version,
        commit_type: CommitType,
        in_commit_timestamp: i64,
        max_published_version: Option<Version>,
        protocol_metadata: CommitProtocolMetadata,
        domain_metadata_changes: Vec<DomainMetadata>,
    ) -> Self {
        Self {
            log_root,
            version,
            commit_type,
            in_commit_timestamp,
            max_published_version,
            protocol_metadata,
            domain_metadata_changes,
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
    pub(crate) fn effective_protocol(&self) -> DeltaResult<&Protocol> {
        pm.new_protocol
            .as_ref()
            .or(pm.read_protocol.as_ref())
            .ok_or_else(|| {
                crate::Error::internal_error(
                    "CommitProtocolMetadata should have at least one protocol",
                )
            })
    }

    /// Returns the effective metadata for this commit. Prefers new_metadata (create-table / ALTER
    /// TABLE), falling back to the read snapshot's metadata.
    pub(crate) fn effective_metadata(&self) -> DeltaResult<&Metadata> {
        pm.new_metadata
            .as_ref()
            .or(pm.read_metadata.as_ref())
            .ok_or_else(|| {
                crate::Error::internal_error(
                    "CommitProtocolMetadata should have at least one metadata",
                )
            })
    }

    /// The minimum reader version required by the effective protocol.
    pub fn min_reader_version(&self) -> i32 {
        self.effective_protocol()
            .map(|p| p.min_reader_version())
            .unwrap_or(0)
    }

    /// The minimum writer version required by the effective protocol.
    pub fn min_writer_version(&self) -> i32 {
        self.effective_protocol()
            .map(|p| p.min_writer_version())
            .unwrap_or(0)
    }

    /// Check if the effective protocol has a specific writer feature by name.
    pub fn has_writer_feature(&self, feature_name: &str) -> bool {
        self.effective_protocol()
            .and_then(|p| p.writer_features())
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == feature_name))
    }

    /// Check if the effective protocol has a specific reader feature by name.
    pub fn has_reader_feature(&self, feature_name: &str) -> bool {
        self.effective_protocol()
            .and_then(|p| p.reader_features())
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == feature_name))
    }

    /// Get the raw metadata configuration for the effective metadata.
    pub fn metadata_configuration(&self) -> HashMap<String, String> {
        self.effective_metadata()
            .map(|m| m.configuration().clone())
            .unwrap_or_default()
    }

    /// Returns `true` if this commit changes the table's protocol.
    pub fn has_protocol_change(&self) -> bool {
        self.protocol_metadata.new_protocol.is_some()
    }

    /// Returns `true` if this commit changes the table's metadata.
    pub fn has_metadata_change(&self) -> bool {
        self.protocol_metadata.new_metadata.is_some()
    }

    /// Returns `true` if this commit includes a domain metadata change for the given domain name.
    pub fn has_domain_metadata_change(&self, domain: &str) -> bool {
        self.domain_metadata_changes
            .iter()
            .any(|dm| dm.domain() == domain)
    }

    /// Creates a new `CommitMetadata` for the given `table_root` and `version`. Test-only.
    ///
    /// Uses a default modern protocol (empty features) and empty metadata.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_unchecked(table_root: Url, version: Version) -> DeltaResult<Self> {
        Self::new_unchecked_with(table_root, version, vec![], vec![], HashMap::new())
    }

    /// Creates a new `CommitMetadata` with specific features and configuration. Test-only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_unchecked_with(
        table_root: Url,
        version: Version,
        reader_features: Vec<&str>,
        writer_features: Vec<&str>,
        configuration: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let log_root = crate::path::LogRoot::new(table_root)?;
        let protocol = Protocol::try_new_modern(reader_features, writer_features)?;
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, configuration)?;
        Ok(Self::new(
            log_root,
            version,
            CommitType::PathBasedWrite,
            0,
            None,
            CommitProtocolMetadata::try_new(Some(protocol), Some(metadata), None, None)?,
            vec![],
        ))
    }

    /// Marks this `CommitMetadata` as having a protocol change. Test-only.
    ///
    /// that changes the protocol.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_protocol_change(mut self) -> Self {
        let protocol = self.effective_protocol().ok().cloned();
        self.protocol_metadata.new_protocol = protocol;
        self
    }

    /// Marks this `CommitMetadata` as having a metadata change. Test-only.
    ///
    /// Copies the existing metadata into the `new_metadata` field to simulate an ALTER TABLE
    /// that changes the metadata.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_metadata_change(mut self) -> Self {
        let metadata = self.effective_metadata().ok().cloned();
        self.protocol_metadata.new_metadata = metadata;
        self
    }

    /// Adds a domain metadata change for the given domain name. Test-only.
    ///
    /// Creates a synthetic domain metadata entry to simulate a domain metadata change
    /// (e.g. clustering column change via ALTER TABLE CLUSTER BY).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_domain_change(mut self, domain: &str) -> Self {
        self.domain_metadata_changes
            .push(DomainMetadata::new(domain.to_string(), String::new()));
        self
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
            CommitProtocolMetadata::try_new(Some(protocol), Some(metadata), None, None).unwrap(),
            vec![],
        );

        // version
        assert_eq!(commit_metadata.version(), 42);
        // in_commit_timestamp
        assert_eq!(commit_metadata.in_commit_timestamp(), 1234);
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
