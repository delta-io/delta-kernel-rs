//! File system committer for non-catalog-managed tables.

use crate::{DeltaResult, Engine, Error, FileMeta, FilteredEngineData};

use super::commit_types::{CommitMetadata, CommitResponse};
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
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let published_commit_path = commit_metadata.published_commit_path()?;

        match engine.json_handler().write_json_file(
            &published_commit_path,
            Box::new(actions),
            false,
        ) {
            Ok(()) => {
                // For now, we don't need the real size of the written file, so we can use 0.
                // If we need this in the future, we can get it from StorageHandler::head.
                let file_meta = FileMeta::new(
                    published_commit_path,
                    commit_metadata.in_commit_timestamp(),
                    0,
                );
                Ok(CommitResponse::Committed { file_meta })
            }
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
            .transaction(committer)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Generic(e) if e.contains("The FileSystemCommitter cannot be used to commit to catalog-managed tables. Please provide a committer for your catalog via Transaction::with_committer().")
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
}
