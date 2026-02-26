//! File system committer for non-catalog-managed tables.

use crate::{DeltaResult, Engine, Error, FileMeta, FilteredEngineData};
use tracing::{info, instrument};

use super::commit_types::{CommitMetadata, CommitResponse};
use super::publish_types::PublishMetadata;
use super::Committer;

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
    #[instrument(
        name = "fs_committer.commit",
        skip_all,
        fields(version = commit_metadata.version()),
        err
    )]
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let version = commit_metadata.version();
        let published_commit_path = commit_metadata.published_commit_path()?;

        match engine.json_handler().write_json_file(
            &published_commit_path,
            Box::new(actions),
            false,
        ) {
            Ok(()) => {
                info!(
                    committed_version = version,
                    "Committed delta file via filesystem committer"
                );
                // For now, we don't need the real size of the written file, so we can use 0.
                // If we need this in the future, we can get it from StorageHandler::head.
                let file_meta = FileMeta::new(
                    published_commit_path,
                    commit_metadata.in_commit_timestamp(),
                    0,
                );
                Ok(CommitResponse::Committed { file_meta })
            }
            Err(Error::FileAlreadyExists(_)) => {
                info!(
                    conflicting_version = version,
                    "Filesystem commit conflict: target version already exists"
                );
                Ok(CommitResponse::Conflict { version })
            }
            Err(e) => Err(e),
        }
    }

    fn is_catalog_committer(&self) -> bool {
        false
    }

    /// The FileSystemCommitter should never be invoked to publish catalog commits. If it is,
    /// something has gone wrong upstream.
    fn publish(&self, _engine: &dyn Engine, publish_metadata: PublishMetadata) -> DeltaResult<()> {
        if !publish_metadata.commits_to_publish().is_empty() {
            return Err(Error::generic(
                "The FilesystemCommitter does not support publishing catalog commits.",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::engine::default::DefaultEngineBuilder;
    use crate::path::LogRoot;

    use object_store::memory::InMemory;
    use object_store::ObjectStore as _;
    use url::Url;

    #[cfg(feature = "catalog-managed")]
    #[tokio::test]
    async fn disallow_filesystem_committer_for_catalog_managed_tables() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = DefaultEngineBuilder::new(storage.clone()).build();

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
        // Try to commit a transaction with FileSystemCommitter
        let committer = Box::new(FileSystemCommitter::new());
        let err = snapshot
            .transaction(committer, &engine)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Generic(e) if e.contains("A catalog committer must be used to commit to catalog-managed tables. Please provide a catalog committer via Snapshot::transaction().")
        ));
    }

    #[tokio::test]
    async fn test_filesystem_committer_returns_valid_commit_response() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = DefaultEngineBuilder::new(storage).build();

        let committer = FileSystemCommitter::new();
        let log_root = LogRoot::new(table_root).unwrap();
        let commit_metadata = CommitMetadata::new(log_root, 1, 12345, Some(0));
        let actions = Box::new(std::iter::empty());

        let result = committer.commit(&engine, actions, commit_metadata).unwrap();

        match result {
            CommitResponse::Committed { file_meta } => {
                assert_eq!(file_meta.last_modified, 12345);
                assert_eq!(file_meta.size, 0);
                assert!(file_meta
                    .location
                    .as_str()
                    .ends_with("00000000000000000001.json"));
            }
            CommitResponse::Conflict { .. } => panic!("Expected Committed, got Conflict"),
        }
    }

    #[tokio::test]
    async fn test_filesystem_committer_returns_conflict_for_existing_version() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = DefaultEngineBuilder::new(storage).build();

        let committer = FileSystemCommitter::new();
        let first_metadata =
            CommitMetadata::new(LogRoot::new(table_root.clone()).unwrap(), 1, 12345, Some(0));
        let second_metadata =
            CommitMetadata::new(LogRoot::new(table_root).unwrap(), 1, 12346, Some(0));

        let first = committer
            .commit(&engine, Box::new(std::iter::empty()), first_metadata)
            .unwrap();
        assert!(matches!(first, CommitResponse::Committed { .. }));

        let second = committer
            .commit(&engine, Box::new(std::iter::empty()), second_metadata)
            .unwrap();
        assert!(matches!(second, CommitResponse::Conflict { version: 1 }));
    }
}
