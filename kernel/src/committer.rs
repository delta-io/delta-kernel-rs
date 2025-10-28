//! The `committer` module provides a [`Committer`] trait which allows different implementations to
//! define how to commit transactions to a catalog or filesystem. For catalog-managed tables, a
//! [`Committer`] specific to the managing catalog should be provided. For non-catalog-managed
//! tables, the [`FileSystemCommitter`] should be used to commit directly to the object store (via
//! put-if-absent call to storage to atomically write new commit files).
//!
//! By implementing the [`Committer`] trait, different catalogs can define what happens when the
//! kernel needs to commit a transaction to a table. The goal terminal state of every
//! [`Transaction`] is to be committed to the table. This means writing the changes (we call these
//! actions) in the transaction as a new version of the table. The [`Committer`] trait exposes a
//! single method, [`commit`] which takes an engine, an iterator of actions (as [`EngineData`]
//! batches), and [`CommitMetadata`] (which includes critical commit metadata like the version to
//! commit) to allow different catalogs to define what it means to 'commit' the actions to a table.
//! For some, this may mean writing staged commits to object storage and retaining an in-memory list
//! (server side) of commits. For others, this may mean writing new (version, actions) tuples to a
//! database.
//!
//! The implementation of [`commit`] must ensure that the actions are committed atomically to the
//! table at the given version and either (1) persisted directly to object storage as published
//! deltas as in non-catalog-managed tables or (2) persisted within the catalog and made available
//! to readers during snapshot contstruction via the [`log_tail`] API.
//!
//! [`Transaction`]: crate::transaction::Transaction
//! [`commit`]: crate::committer::Committer::commit
//! [`log_tail`]: crate::snapshot::SnapshotBuilder::with_log_tail
//! [`EngineData`]: crate::EngineData

use crate::path::LogRoot;
use crate::{AsAny, DeltaResult, Engine, Error, FilteredEngineData, Version};

use url::Url;

/// `CommitMetadata` bundles the metadata about a commit operation. This currently includes the
/// commit path and version but will expand to things like `Protocol`, `Metadata`, etc. to allow
/// for catalogs to understand/cache/persist more information about the table at commit time.
///
/// Note that this struct cannot be constructed. It is handed to the [`Committer`] (in the
/// [`commit`] method) by the kernel when a transaction is being committed.
///
/// See the [module-level documentation] for more details.
///
/// [`commit`]: Committer::commit
/// [module-level documentation]: crate::committer
#[derive(Debug)]
pub struct CommitMetadata {
    pub(crate) log_root: LogRoot,
    pub(crate) version: Version,
    // in the future this will include Protocol, Metadata, CommitInfo, Domain Metadata, etc.
}

impl CommitMetadata {
    pub(crate) fn new(log_root: LogRoot, version: Version) -> Self {
        Self { log_root, version }
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
    Committed { version: Version },
    Conflict { version: Version },
}

/// A Committer is the system by which transactions are committed to a table. Transactions are
/// effectively a collection of actions performed on the table at a specific version. The kernel
/// exposes this trait so different catalogs can build their own commit implementations. For
/// example, different catalogs may: commit directly to a database, commit to an object store, or
/// use another system entirely.
///
/// Critically, a Committer must implement [`commit`] which takes an engine and an iterator of
/// actions (as [`EngineData`] batches) to commit to the table at the given version
/// ([`CommitMetadata::version`]).
///
/// [`commit`]: Committer::commit
/// [`EngineData`]: crate::EngineData
//
// Note: While we could omit the Send bound, we keep it here for simplicity - so usage can be
// Arc<dyn Committer> (instead of Arc<dyn Committer + Send>). If there is a strong case for a !Send
// Committer then we can remove this bound and possibly just do an alias like CommitterRef =
// Arc<dyn Committer + Send>.
pub trait Committer: Send + AsAny {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse>;
}

/// The `FileSystemCommitter` is an internal implementation of the `Committer` trait which
/// commits to a file system directly via `Engine::json_handler().write_json_file` for
/// non-catalog-managed tables.
///
/// SAFETY: it is _incorrect_ to use this committer for catalog-managed tables.
#[derive(Debug, Default)]
pub struct FileSystemCommitter;

impl FileSystemCommitter {
    pub fn new() -> Self {
        Self {}
    }
}

impl Committer for FileSystemCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        match engine.json_handler().write_json_file(
            &commit_metadata.published_commit_path()?,
            Box::new(actions),
            false,
        ) {
            Ok(()) => Ok(CommitResponse::Committed {
                version: commit_metadata.version,
            }),
            Err(Error::FileAlreadyExists(_)) => Ok(CommitResponse::Conflict {
                version: commit_metadata.version,
            }),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::DefaultEngine;
    use crate::path::LogRoot;

    use object_store::memory::InMemory;
    use object_store::ObjectStore as _;
    use url::Url;

    #[test]
    fn test_commit_metadata() {
        let table_root = Url::parse("s3://my-bucket/path/to/table/").unwrap();
        let log_root = LogRoot::new(table_root).unwrap();
        let version = 42;

        let commit_metadata = CommitMetadata::new(log_root, version);

        // version
        assert_eq!(commit_metadata.version(), 42);

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
            staged_path_str
                .starts_with("s3://my-bucket/path/to/table/_delta_log/00000000000000000042."),
            "Staged path should start with the correct prefix, got: {}",
            staged_path_str
        );
        assert!(
            staged_path_str.ends_with(".json"),
            "Staged path should end with .json, got: {}",
            staged_path_str
        );
        let uuid_str = staged_path_str
            .strip_prefix("s3://my-bucket/path/to/table/_delta_log/00000000000000000042.")
            .and_then(|s| s.strip_suffix(".json"))
            .expect("Staged path should have expected format");
        uuid::Uuid::parse_str(uuid_str).expect("Staged path should contain a valid UUID");
    }

    #[cfg(feature = "catalog-managed")]
    #[tokio::test]
    async fn catalog_managed_tables_block_transactions() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));

        let actions = [
            r#"{"commitInfo":{"timestamp":12345678900,"inCommitTimestamp":12345678900}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["catalogManaged"],"writerFeatures":["catalogManaged","inCommitTimestamp"]}}"#,
            r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1234567890}}"#,
        ].join("\n");

        let commit_path = object_store::path::Path::from("_delta_log/00000000000000000000.json");
        storage.put(&commit_path, actions.into()).await.unwrap();

        let snapshot = crate::snapshot::SnapshotBuilder::new_for(table_root)
            .build(&engine)
            .unwrap();
        // Try to create a transaction with FileSystemCommitter
        let committer = Box::new(FileSystemCommitter::new());
        let err = snapshot.transaction(committer).unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Unsupported(e) if e.contains("Writes are not yet supported for catalog-managed tables")
        ));
        // after allowing writes, we will check that this disallows default committer for
        // catalog-managed tables.
        // assert!(matches!(
        //     err,
        //     crate::Error::Generic(e) if e.contains("Cannot use the default committer for a catalog-managed table")
        // ));
    }
}
