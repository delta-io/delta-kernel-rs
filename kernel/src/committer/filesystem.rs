//! File system committer for non-catalog-managed tables.

use tracing::{info, instrument};

use super::Committer;
use super::commit_types::{CommitMetadata, CommitResponse};
use super::publish_types::PublishMetadata;
#[cfg(feature = "declarative-plans")]
use crate::committer::write_commit_json_declarative;
#[cfg(feature = "declarative-plans")]
use crate::plans::{RelationRef, ScopedPlanExecutor};
use crate::{
    Engine, FileMeta, FilteredEngineData, KernelError, KernelResult, KernelResultIterator,
};

/// The `FileSystemCommitter` is an internal implementation of the `Committer` trait which
/// commits to a file system directly via `Engine::json_handler().write_json_file` for
/// non-catalog-managed tables.
///
/// SAFETY: it is _incorrect_ to use this committer for catalog-managed tables.
#[derive(Debug, Default)]
pub struct FileSystemCommitter;

impl FileSystemCommitter {
    /// Creates a filesystem committer for a path-based table.
    pub fn new() -> Self {
        Self {}
    }

    fn commit_response(
        result: KernelResult<FileMeta>,
        version: crate::Version,
    ) -> KernelResult<CommitResponse> {
        match result {
            Ok(file_meta) => {
                info!(
                    committed_version = version,
                    "Committed delta file via filesystem committer"
                );
                Ok(CommitResponse::Committed { file_meta })
            }
            Err(KernelError::FileAlreadyExists(_)) => {
                info!(
                    conflicting_version = version,
                    "Filesystem commit conflict: target version already exists"
                );
                Ok(CommitResponse::Conflict { version })
            }
            Err(e) => Err(e),
        }
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
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        let version = commit_metadata.version();
        let published_commit_path = commit_metadata.published_commit_path()?;

        let result = engine
            .json_handler()
            .write_json_file(&published_commit_path, Box::new(actions), false)
            .map(|written_size| {
                FileMeta::new(
                    published_commit_path,
                    commit_metadata.in_commit_timestamp(),
                    written_size,
                )
            });
        Self::commit_response(result, version)
    }

    #[cfg(feature = "declarative-plans")]
    #[instrument(
        name = "fs_committer.commit_declarative",
        skip_all,
        fields(version = commit_metadata.version()),
        err
    )]
    fn commit_declarative(
        &self,
        executor: &dyn ScopedPlanExecutor,
        actions: Box<dyn Iterator<Item = RelationRef> + '_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        let version = commit_metadata.version();
        let path = commit_metadata.published_commit_path()?;
        let result = write_commit_json_declarative(executor, actions, path);
        Self::commit_response(result, version)
    }

    fn is_catalog_committer(&self) -> bool {
        false
    }

    /// The FileSystemCommitter should never be invoked to publish catalog commits. If it is,
    /// something has gone wrong upstream.
    fn publish(&self, _engine: &dyn Engine, publish_metadata: PublishMetadata) -> KernelResult<()> {
        if !publish_metadata.commits_to_publish().is_empty() {
            return Err(KernelError::generic(
                "The FilesystemCommitter does not support publishing catalog commits.",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::{LOG_METADATA_SCHEMA, Metadata, Protocol};
    use crate::committer::{CommitProtocolMetadata, CommitType};
    #[cfg(feature = "declarative-plans")]
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::object_store::ObjectStoreExt as _;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::path::LogRoot;
    use crate::schema::schema_ref;
    use crate::transaction::UpdateTableOperation;
    use crate::{EngineData, create_row};

    #[derive(Clone, Copy)]
    enum CommitStyle {
        Imperative,
        #[cfg(feature = "declarative-plans")]
        Declarative,
    }

    fn commit_with_style(
        committer: &FileSystemCommitter,
        engine: &SyncEngine,
        storage: Arc<InMemory>,
        style: CommitStyle,
        action: Box<dyn EngineData>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        #[cfg(not(feature = "declarative-plans"))]
        let _ = storage;
        match style {
            CommitStyle::Imperative => {
                let actions = Box::new(std::iter::once(Ok(
                    FilteredEngineData::with_all_rows_selected(action),
                )));
                committer.commit(engine, actions, commit_metadata)
            }
            #[cfg(feature = "declarative-plans")]
            CommitStyle::Declarative => {
                let batch = ArrowEngineData::try_from_engine_data(action)?.into();
                let scoped = SyncPlanExecutor::new(Some(storage)).get_scoped();
                let actions =
                    scoped.create_ref("commit-actions", LOG_METADATA_SCHEMA.clone(), batch)?;
                committer.commit_declarative(
                    &scoped,
                    Box::new(std::iter::once(actions)),
                    commit_metadata,
                )
            }
        }
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
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, committer)
            .unwrap()
            .commit(&engine)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::KernelError::Generic(e) if e.contains("This table is catalog-managed and requires a catalog committer.")
        ));
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    #[tokio::test]
    async fn test_filesystem_committer_returns_valid_commit_response(#[case] style: CommitStyle) {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = SyncEngine::new_with_store(storage.clone());

        let committer = FileSystemCommitter::new();
        let log_root = LogRoot::new(table_root).unwrap();
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new()).unwrap();
        let schema = schema_ref! {};
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let action = create_row(&engine, LOG_METADATA_SCHEMA.clone(), metadata.clone()).unwrap();
        let commit_metadata = CommitMetadata::new(
            log_root,
            1,
            CommitType::PathBasedWrite,
            12345,
            Some(0),
            CommitProtocolMetadata::try_new(Some(protocol), Some(metadata), None, None).unwrap(),
            vec![],
        );
        let result = commit_with_style(
            &committer,
            &engine,
            storage.clone(),
            style,
            action,
            commit_metadata,
        )
        .unwrap();
        let stored = storage
            .get(&Path::from("_delta_log/00000000000000000001.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let stored_action: serde_json::Value = serde_json::from_slice(&stored).unwrap();
        assert!(stored_action.get("metaData").is_some());

        match result {
            CommitResponse::Committed { file_meta } => {
                match style {
                    CommitStyle::Imperative => assert_eq!(file_meta.last_modified, 12345),
                    #[cfg(feature = "declarative-plans")]
                    CommitStyle::Declarative => {
                        let stored_meta = storage
                            .head(&Path::from("_delta_log/00000000000000000001.json"))
                            .await
                            .unwrap();
                        assert_eq!(
                            file_meta.last_modified,
                            stored_meta.last_modified.timestamp_millis()
                        );
                    }
                }
                assert!(file_meta.size > 0);
                assert_eq!(file_meta.size, stored.len() as u64);
                assert!(
                    file_meta
                        .location
                        .as_str()
                        .ends_with("00000000000000000001.json")
                );
            }
            CommitResponse::Conflict { .. } => panic!("Expected Committed, got Conflict"),
        }
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    #[tokio::test]
    async fn test_filesystem_committer_returns_conflict_for_existing_version(
        #[case] style: CommitStyle,
    ) {
        let storage = Arc::new(InMemory::new());
        let table_root = Url::parse("memory:///").unwrap();
        let engine = SyncEngine::new_with_store(storage.clone());

        let committer = FileSystemCommitter::new();
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new()).unwrap();
        let schema = schema_ref! {};
        let metadata1 =
            Metadata::try_new(None, None, schema.clone(), vec![], 0, HashMap::new()).unwrap();
        let metadata2 = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        let first_action =
            create_row(&engine, LOG_METADATA_SCHEMA.clone(), metadata1.clone()).unwrap();
        let second_action =
            create_row(&engine, LOG_METADATA_SCHEMA.clone(), metadata2.clone()).unwrap();
        let first_metadata = CommitMetadata::new(
            LogRoot::new(table_root.clone()).unwrap(),
            1,
            CommitType::PathBasedWrite,
            12345,
            Some(0),
            CommitProtocolMetadata::try_new(Some(protocol.clone()), Some(metadata1), None, None)
                .unwrap(),
            vec![],
        );
        let second_metadata = CommitMetadata::new(
            LogRoot::new(table_root).unwrap(),
            1,
            CommitType::PathBasedWrite,
            12346,
            Some(0),
            CommitProtocolMetadata::try_new(Some(protocol), Some(metadata2), None, None).unwrap(),
            vec![],
        );

        let first = commit_with_style(
            &committer,
            &engine,
            storage.clone(),
            style,
            first_action,
            first_metadata,
        )
        .unwrap();
        assert!(matches!(first, CommitResponse::Committed { .. }));

        let second = commit_with_style(
            &committer,
            &engine,
            storage,
            style,
            second_action,
            second_metadata,
        )
        .unwrap();
        assert!(matches!(second, CommitResponse::Conflict { version: 1 }));
    }
}
