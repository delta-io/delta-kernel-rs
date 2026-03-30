//! Commit metadata types for the committer module.

use std::collections::HashMap;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use url::Url;

use crate::actions::{DomainMetadata, Metadata, Protocol};
use crate::path::LogRoot;
use crate::{DeltaResult, Version};

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
    pub(crate) in_commit_timestamp: i64,
    pub(crate) max_published_version: Option<Version>,
    /// The read snapshot's protocol (table state when the transaction started).
    pub(crate) protocol: Protocol,
    /// The read snapshot's metadata (table state when the transaction started).
    pub(crate) metadata: Metadata,
    /// New protocol being committed, if the transaction changes it (e.g. create-table or future
    /// ALTER TABLE). `None` for normal commits that don't modify the protocol.
    pub(crate) new_protocol: Option<Protocol>,
    /// New metadata being committed, if the transaction changes it (e.g. create-table or future
    /// ALTER TABLE). `None` for normal commits that don't modify metadata.
    pub(crate) new_metadata: Option<Metadata>,
    /// Domain metadata actions in this commit (additions and removals).
    pub(crate) domain_metadata_changes: Vec<DomainMetadata>,
}

impl CommitMetadata {
    pub(crate) fn new(
        log_root: LogRoot,
        version: Version,
        in_commit_timestamp: i64,
        max_published_version: Option<Version>,
        protocol: Protocol,
        metadata: Metadata,
    ) -> Self {
        Self {
            log_root,
            version,
            in_commit_timestamp,
            max_published_version,
            protocol,
            metadata,
            new_protocol: None,
            new_metadata: None,
            domain_metadata_changes: vec![],
        }
    }

    /// Set the new protocol and metadata being committed (for create-table or ALTER TABLE).
    #[internal_api]
    pub(crate) fn with_new_protocol_and_metadata(
        mut self,
        new_protocol: Option<Protocol>,
        new_metadata: Option<Metadata>,
    ) -> Self {
        self.new_protocol = new_protocol;
        self.new_metadata = new_metadata;
        self
    }

    /// Set the domain metadata changes for this commit.
    pub(crate) fn with_domain_metadata_changes(
        mut self,
        domain_metadata_changes: Vec<DomainMetadata>,
    ) -> Self {
        self.domain_metadata_changes = domain_metadata_changes;
        self
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

    /// The root URL of the table being committed to.
    pub fn table_root(&self) -> &Url {
        self.log_root.table_root()
    }

    /// The minimum reader version required by the table's protocol.
    pub fn min_reader_version(&self) -> i32 {
        self.protocol.min_reader_version()
    }

    /// The minimum writer version required by the table's protocol.
    pub fn min_writer_version(&self) -> i32 {
        self.protocol.min_writer_version()
    }

    /// Check if the table's protocol has a specific writer feature by name.
    ///
    /// Feature names use the Delta protocol wire format (e.g., `"catalogManaged"`,
    /// `"inCommitTimestamp"`). Returns `false` for legacy protocols without table features.
    pub fn has_writer_feature(&self, feature_name: &str) -> bool {
        self.protocol
            .writer_features()
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == feature_name))
    }

    /// Check if the table's protocol has a specific reader feature by name.
    ///
    /// Feature names use the Delta protocol wire format (e.g., `"catalogManaged"`,
    /// `"deletionVectors"`). Returns `false` for legacy protocols without table features.
    pub fn has_reader_feature(&self, feature_name: &str) -> bool {
        self.protocol
            .reader_features()
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == feature_name))
    }

    /// Get the raw metadata configuration for the table being committed to.
    ///
    /// This returns the `Metadata.configuration` map as stored in the Delta log, containing
    /// user-defined properties, delta table properties, and application-specific properties.
    pub fn metadata_configuration(&self) -> &HashMap<String, String> {
        self.metadata.configuration()
    }

    /// Returns `true` if this commit changes the table's protocol (e.g. create-table or future
    /// ALTER TABLE). Returns `false` for normal commits that don't modify the protocol.
    pub fn has_protocol_change(&self) -> bool {
        self.new_protocol.is_some()
    }

    /// Returns `true` if this commit changes the table's metadata (e.g. create-table or future
    /// ALTER TABLE). Returns `false` for normal commits that don't modify metadata.
    pub fn has_metadata_change(&self) -> bool {
        self.new_metadata.is_some()
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
        let log_root = crate::path::LogRoot::new(table_root)?;
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new())?;
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new())?;
        Ok(Self::new(log_root, version, 0, None, protocol, metadata))
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
            ts,
            max_published_version,
            protocol,
            metadata,
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
