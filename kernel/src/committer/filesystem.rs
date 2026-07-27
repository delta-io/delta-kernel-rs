//! File system committer for non-catalog-managed tables.

use tracing::{info, instrument, warn};

use super::commit_types::{CommitMetadata, CommitResponse};
use super::publish_types::PublishMetadata;
use super::Committer;
use crate::{DeltaResult, DeltaResultIterator, Engine, Error, FileMeta, FilteredEngineData};

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
        actions: DeltaResultIterator<'_, FilteredEngineData>,
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
                // The atomic write has already committed. Treat the size lookup as best-effort so
                // a metadata error cannot turn a durable commit into a reported failure.
                let size = match engine.storage_handler().head(&published_commit_path) {
                    Ok(file_meta) => file_meta.size,
                    Err(error) => {
                        warn!(
                            path = %published_commit_path,
                            error = %error,
                            "Failed to retrieve metadata for committed delta file; falling back to size 0"
                        );
                        0
                    }
                };
                let file_meta = FileMeta::new(
                    published_commit_path,
                    commit_metadata.in_commit_timestamp(),
                    size,
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
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use bytes::Bytes;
    use url::Url;

    use super::*;
    use crate::actions::{Metadata, Protocol, LOG_METADATA_SCHEMA};
    use crate::committer::{CommitProtocolMetadata, CommitType};
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::path::LogRoot;
    use crate::{
        EvaluationHandler, FileSlice, IntoEngineData, JsonHandler, ParquetHandler, StorageHandler,
    };

    struct HeadTrackingStorageHandler {
        inner: Arc<dyn StorageHandler>,
        fail_head: bool,
        head_calls: AtomicUsize,
    }

    impl HeadTrackingStorageHandler {
        fn new(inner: Arc<dyn StorageHandler>, fail_head: bool) -> Self {
            Self {
                inner,
                fail_head,
                head_calls: AtomicUsize::new(0),
            }
        }

        fn head_calls(&self) -> usize {
            self.head_calls.load(Ordering::SeqCst)
        }
    }

    impl StorageHandler for HeadTrackingStorageHandler {
        fn list_from(
            &self,
            path: &Url,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
            self.inner.list_from(path)
        }

        fn read_files(
            &self,
            files: Vec<FileSlice>,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
            self.inner.read_files(files)
        }

        fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
            self.inner.copy_atomic(src, dest)
        }

        fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
            self.inner.put(path, data, overwrite)
        }

        fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
            self.head_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_head {
                Err(Error::generic("injected HEAD failure"))
            } else {
                self.inner.head(path)
            }
        }

        fn delete(&self, path: &Url) -> DeltaResult<()> {
            self.inner.delete(path)
        }
    }

    struct HeadTrackingEngine {
        inner: SyncEngine,
        storage_handler: Arc<HeadTrackingStorageHandler>,
    }

    impl HeadTrackingEngine {
        fn new(store: Arc<InMemory>, fail_head: bool) -> Self {
            let inner = SyncEngine::new_with_store(store);
            let storage_handler = Arc::new(HeadTrackingStorageHandler::new(
                inner.storage_handler(),
                fail_head,
            ));
            Self {
                inner,
                storage_handler,
            }
        }

        fn head_calls(&self) -> usize {
            self.storage_handler.head_calls()
        }
    }

    impl Engine for HeadTrackingEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.inner.evaluation_handler()
        }

        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            self.storage_handler.clone()
        }

        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            self.inner.json_handler()
        }

        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            self.inner.parquet_handler()
        }

        #[cfg(feature = "declarative-plans")]
        fn plan_executor(&self) -> Arc<dyn crate::plans::PlanExecutor> {
            self.inner.plan_executor()
        }
    }

    fn commit_input(
        engine: &dyn Engine,
        table_root: Url,
        version: u64,
        in_commit_timestamp: i64,
    ) -> (FilteredEngineData, CommitMetadata) {
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new()).unwrap();
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let action = metadata
            .clone()
            .into_engine_data(LOG_METADATA_SCHEMA.clone(), engine)
            .unwrap();
        let commit_metadata = CommitMetadata::new(
            LogRoot::new(table_root).unwrap(),
            version,
            CommitType::PathBasedWrite,
            in_commit_timestamp,
            version.checked_sub(1),
            CommitProtocolMetadata::try_new(Some(protocol), Some(metadata), None, None).unwrap(),
            vec![],
        );
        (
            FilteredEngineData::with_all_rows_selected(action),
            commit_metadata,
        )
    }

    #[tokio::test]
    async fn disallow_filesystem_committer_for_catalog_managed_tables() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = SyncEngine::new_with_store(storage.clone());

        let actions = [
            r#"{"commitInfo":{"timestamp":12345678900,"inCommitTimestamp":12345678900}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["catalogManaged"],"writerFeatures":["catalogManaged","inCommitTimestamp"]}}"#,
            r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true"},"createdTime":1234567890}}"#,
        ].join("\n");

        let commit_path = Path::from("_delta_log/00000000000000000000.json");
        storage.put(&commit_path, actions.into()).await.unwrap();

        let snapshot = crate::snapshot::SnapshotBuilder::new_for(table_root)
            .with_max_catalog_version(0)
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
            crate::Error::Generic(e) if e.contains("This table is catalog-managed and requires a catalog committer.")
        ));
    }

    #[tokio::test]
    async fn test_filesystem_committer_returns_valid_commit_response() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = HeadTrackingEngine::new(storage.clone(), false);

        let committer = FileSystemCommitter::new();
        let (action, commit_metadata) = commit_input(&engine, table_root, 1, 12345);
        let actions = Box::new(std::iter::once(Ok(action)));

        let result = committer.commit(&engine, actions, commit_metadata).unwrap();
        let stored_size = storage
            .head(&Path::from("_delta_log/00000000000000000001.json"))
            .await
            .unwrap()
            .size;

        match result {
            CommitResponse::Committed { file_meta } => {
                assert_eq!(file_meta.last_modified, 12345);
                assert!(file_meta.size > 0);
                assert_eq!(file_meta.size, stored_size);
                assert!(file_meta
                    .location
                    .as_str()
                    .ends_with("00000000000000000001.json"));
            }
            CommitResponse::Conflict { .. } => panic!("Expected Committed, got Conflict"),
        }
        assert_eq!(engine.head_calls(), 1);
    }

    #[tokio::test]
    async fn test_filesystem_committer_head_failure_preserves_successful_commit() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = HeadTrackingEngine::new(storage.clone(), true);
        let committer = FileSystemCommitter::new();
        let (action, commit_metadata) = commit_input(&engine, table_root, 1, 12345);

        let result = committer
            .commit(
                &engine,
                Box::new(std::iter::once(Ok(action))),
                commit_metadata,
            )
            .unwrap();

        let stored_size = storage
            .head(&Path::from("_delta_log/00000000000000000001.json"))
            .await
            .unwrap()
            .size;
        assert!(stored_size > 0, "the commit file must be durable");
        assert_eq!(engine.head_calls(), 1);
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
    async fn test_filesystem_committer_write_failure_skips_head() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = HeadTrackingEngine::new(storage.clone(), false);
        let committer = FileSystemCommitter::new();
        let (_action, commit_metadata) = commit_input(&engine, table_root, 1, 12345);
        let actions = Box::new(std::iter::once(Err(Error::generic(
            "injected write failure",
        ))));

        let error = committer
            .commit(&engine, actions, commit_metadata)
            .unwrap_err();

        assert!(error.to_string().contains("injected write failure"));
        assert_eq!(engine.head_calls(), 0);
        assert!(storage
            .head(&Path::from("_delta_log/00000000000000000001.json"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_filesystem_committer_returns_conflict_for_existing_version() {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = HeadTrackingEngine::new(storage, false);

        let committer = FileSystemCommitter::new();
        let (first_action, first_metadata) = commit_input(&engine, table_root.clone(), 1, 12345);
        let (second_action, second_metadata) = commit_input(&engine, table_root, 1, 12346);

        let first = committer
            .commit(
                &engine,
                Box::new(std::iter::once(Ok(first_action))),
                first_metadata,
            )
            .unwrap();
        assert!(matches!(first, CommitResponse::Committed { .. }));
        assert_eq!(engine.head_calls(), 1);

        let second = committer
            .commit(
                &engine,
                Box::new(std::iter::once(Ok(second_action))),
                second_metadata,
            )
            .unwrap();
        assert!(matches!(second, CommitResponse::Conflict { version: 1 }));
        assert_eq!(engine.head_calls(), 1);
    }
}
