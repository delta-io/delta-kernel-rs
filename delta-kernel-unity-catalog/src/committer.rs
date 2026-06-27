use std::sync::Arc;

use delta_kernel::committer::{
    CommitMetadata, CommitResponse, CommitType, Committer, PublishMetadata,
};
use delta_kernel::{
    DeltaResult, DeltaResultIterator, Engine, Error as DeltaError, FilteredEngineData,
};
use tracing::{debug, info};
use unity_catalog_delta_client_api::{
    Commit, Requirement, Update, UpdateTableClient, UpdateTableRequest,
};

use crate::constants::{
    CATALOG_MANAGED_FEATURE, ENABLE_IN_COMMIT_TIMESTAMPS, IN_COMMIT_TIMESTAMP_FEATURE,
    UC_TABLE_ID_KEY, VACUUM_PROTOCOL_CHECK_FEATURE,
};
use crate::errors;
use crate::intents::derive_updates;

/// Convenience macro: returns an error if a condition is not met.
macro_rules! require {
    ($cond:expr, $err:expr) => {
        if !($cond) {
            return Err($err);
        }
    };
}

/// A `Committer` for catalog-managed Delta tables under the UC API.
///
/// For version 0 (table creation), writes `00000000000000000000.json` directly
/// to the published commit path. The connector is responsible for finalizing
/// the table in UC via the `tables` (CREATE) endpoint.
///
/// For version >= 1, writes a staged commit file and dispatches a typed
/// `requirements + updates` payload through the wrapped [`UpdateTableClient`]. ALTER TABLE-style
/// changes (protocol, properties, schema, domain metadata) are translated into typed UC
/// `Update` intents alongside the `AddCommit` registration. The catalog, schema, and table name
/// are carried on each `UpdateTableRequest` so the REST client can build the URL without
/// holding per-table state.
///
/// Thread-safety: this `Committer` blocks on async I/O via
/// `tokio::task::block_in_place` and therefore requires a multi-threaded
/// tokio runtime context (the default engine satisfies this).
#[derive(Debug, Clone)]
pub struct UCCommitter<C: UpdateTableClient> {
    update_table_client: Arc<C>,
    table_id: String,
    catalog: String,
    schema: String,
    table_name: String,
}

impl<C: UpdateTableClient> UCCommitter<C> {
    /// Build a committer that issues commits for the UC-managed table
    /// `catalog.schema.table_name` (identified by `table_id`) via
    /// `update_table_client`.
    pub fn new(
        update_table_client: Arc<C>,
        table_id: impl Into<String>,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        UCCommitter {
            update_table_client,
            table_id: table_id.into(),
            catalog: catalog.into(),
            schema: schema.into(),
            table_name: table_name.into(),
        }
    }

    /// True iff `commit_metadata` carries the `catalogManaged` feature on
    /// both the reader- and writer-feature lists.
    fn has_catalog_managed_feature(commit_metadata: &CommitMetadata) -> bool {
        commit_metadata.has_writer_feature(CATALOG_MANAGED_FEATURE)
            && commit_metadata.has_reader_feature(CATALOG_MANAGED_FEATURE)
    }

    /// Validate that protocol features and metadata properties are correct
    /// for a UC catalog-managed table. Same checks regardless of API version.
    fn validate_catalog_managed_state(&self, commit_metadata: &CommitMetadata) -> DeltaResult<()> {
        require!(
            commit_metadata.commit_type() != CommitType::UpgradeToCatalogManaged,
            errors::upgrade_downgrade_unsupported("upgrade")
        );
        require!(
            commit_metadata.commit_type() != CommitType::DowngradeToPathBased,
            errors::upgrade_downgrade_unsupported("downgrade")
        );
        require!(
            Self::has_catalog_managed_feature(commit_metadata),
            errors::missing_feature(CATALOG_MANAGED_FEATURE)
        );
        require!(
            commit_metadata.has_writer_feature(VACUUM_PROTOCOL_CHECK_FEATURE)
                && commit_metadata.has_reader_feature(VACUUM_PROTOCOL_CHECK_FEATURE),
            errors::missing_feature(VACUUM_PROTOCOL_CHECK_FEATURE)
        );
        require!(
            commit_metadata.has_writer_feature(IN_COMMIT_TIMESTAMP_FEATURE),
            errors::missing_feature(IN_COMMIT_TIMESTAMP_FEATURE)
        );

        let config = commit_metadata
            .metadata_configuration()
            .ok_or_else(errors::missing_metadata_configuration)?;
        let table_id = config
            .get(UC_TABLE_ID_KEY)
            .ok_or_else(|| errors::missing_property(UC_TABLE_ID_KEY))?;
        require!(
            table_id == &self.table_id,
            errors::table_id_mismatch(&self.table_id, table_id)
        );
        require!(
            config.get(ENABLE_IN_COMMIT_TIMESTAMPS).map(String::as_str) == Some("true"),
            errors::ict_not_enabled()
        );
        Ok(())
    }

    /// Commit version 0 (table creation): write `000.json` directly to the
    /// published commit path. v0 never calls UC; UC doesn't know about the
    /// table yet.
    fn commit_version_0(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        commit_metadata: &CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        debug_assert!(
            commit_metadata.version() == 0,
            "commit_version_0 called with version {}",
            commit_metadata.version()
        );
        self.validate_catalog_managed_state(commit_metadata)?;
        let published_commit_path = commit_metadata.published_commit_path()?;
        match engine.json_handler().write_json_file(
            &published_commit_path,
            Box::new(actions),
            false,
        ) {
            Ok(()) => {
                info!("wrote version 0 commit file for UC table creation");
                let file_meta = engine.storage_handler().head(&published_commit_path)?;
                Ok(CommitResponse::Committed { file_meta })
            }
            Err(delta_kernel::Error::FileAlreadyExists(_)) => {
                info!("version 0 commit conflict: commit file already exists");
                Ok(CommitResponse::Conflict { version: 0 })
            }
            Err(e) => Err(e),
        }
    }

    /// Commit version >= 1: write a staged commit file, then dispatch a typed
    /// `requirements + updates` payload through the catalog client.
    fn commit_version_non_zero(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse>
    where
        C: 'static,
    {
        debug_assert!(
            commit_metadata.version() != 0,
            "commit_version_non_zero called with version 0"
        );
        self.validate_catalog_managed_state(&commit_metadata)?;
        let staged_commit_path = commit_metadata.staged_commit_path()?;
        engine
            .json_handler()
            .write_json_file(&staged_commit_path, Box::new(actions), false)?;

        let committed = engine.storage_handler().head(&staged_commit_path)?;
        debug!("wrote staged commit file: {:?}", committed);

        let file_name = staged_commit_path
            .path_segments()
            .ok_or_else(|| DeltaError::generic("staged commit contained no path segments"))?
            .next_back()
            .ok_or_else(|| DeltaError::generic("staged commit segments next_back was empty"))?
            .to_string();
        let version_i64: i64 = commit_metadata.version().try_into().map_err(|_| {
            DeltaError::generic("commit version does not fit into i64 for UC commit")
        })?;
        let file_size_i64: i64 = committed
            .size
            .try_into()
            .map_err(|_| DeltaError::generic("committed size does not fit into i64"))?;

        // Order: AddCommit registers the staged file; derived updates describe what the file
        // contains; SetLatestBackfilledVersion is an independent watermark.
        let mut updates = vec![Update::AddCommit {
            commit: Commit {
                version: version_i64,
                timestamp: commit_metadata.in_commit_timestamp(),
                file_name,
                file_size: file_size_i64,
                file_modification_timestamp: committed.last_modified,
            },
        }];
        updates.extend(derive_updates(&commit_metadata)?);
        if let Some(max_pub) = commit_metadata.max_published_version() {
            let v: i64 = max_pub.try_into().map_err(|_| {
                DeltaError::Generic(format!(
                    "Max published version {max_pub} does not fit into i64 for UC commit"
                ))
            })?;
            updates.push(Update::SetLatestBackfilledVersion {
                latest_published_version: v,
            });
        }
        let update_req = UpdateTableRequest::new(
            self.catalog.clone(),
            self.schema.clone(),
            self.table_name.clone(),
            vec![Requirement::AssertTableUuid {
                uuid: self.table_id.clone(),
            }],
            updates,
        );

        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            DeltaError::generic("UCCommitter may only be used within a tokio runtime")
        })?;
        let result = tokio::task::block_in_place(|| {
            handle.block_on(async move { self.update_table_client.update_table(update_req).await })
        });
        match result {
            Ok(_) => Ok(CommitResponse::Committed {
                file_meta: committed,
            }),
            // A conflict means another writer won this version; surface it so the caller can retry.
            Err(unity_catalog_delta_client_api::Error::CommitConflict) => {
                Ok(CommitResponse::Conflict {
                    version: commit_metadata.version(),
                })
            }
            Err(e) => Err(DeltaError::Generic(format!("UC update_table error: {e}"))),
        }
    }
}

impl<C: UpdateTableClient + 'static> Committer for UCCommitter<C> {
    /// Commit `actions` to the UC-managed table.
    ///
    /// - v0: write `000.json` directly to the published commit path. The connector finalizes the
    ///   table via the UC `tables` (CREATE) endpoint.
    /// - v >= 1: write a staged commit, then call the UC `update_table` endpoint through the
    ///   wrapped `UpdateTableClient` to ratify it.
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        if commit_metadata.version() == 0 {
            return self.commit_version_0(engine, actions, &commit_metadata);
        }
        self.commit_version_non_zero(engine, actions, commit_metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        true
    }

    fn publish(&self, engine: &dyn Engine, publish_metadata: PublishMetadata) -> DeltaResult<()> {
        if publish_metadata.commits_to_publish().is_empty() {
            return Ok(());
        }

        for catalog_commit in publish_metadata.commits_to_publish() {
            let src = catalog_commit.location();
            let dest = catalog_commit.published_location();
            match engine.storage_handler().copy_atomic(src, dest) {
                Ok(_) => (),
                Err(DeltaError::FileAlreadyExists(_)) => (),
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use delta_kernel::committer::{CatalogCommit, CommitMetadata};
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::Version;
    use delta_kernel_default_engine::DefaultEngine;
    use unity_catalog_delta_client_api::error::Result;
    use unity_catalog_delta_client_api::UpdateTableResponse as ApiUpdateTableResponse;

    use super::*;

    struct MockUpdateTableClient;

    impl UpdateTableClient for MockUpdateTableClient {
        async fn update_table(&self, _: UpdateTableRequest) -> Result<ApiUpdateTableResponse> {
            unimplemented!()
        }
    }

    fn test_committer() -> UCCommitter<MockUpdateTableClient> {
        UCCommitter::new(
            Arc::new(MockUpdateTableClient),
            "test-table-id",
            "test_catalog",
            "test_schema",
            "test_table",
        )
    }

    /// Build a valid catalog-managed `CommitMetadata` carrying all required
    /// UC features and properties.
    fn catalog_managed_commit_metadata(table_root: url::Url, version: Version) -> CommitMetadata {
        CommitMetadata::new_unchecked_with(
            table_root,
            version,
            vec!["catalogManaged", "vacuumProtocolCheck"],
            vec!["catalogManaged", "inCommitTimestamp", "vacuumProtocolCheck"],
            HashMap::from([
                (
                    "io.unitycatalog.tableId".to_string(),
                    "test-table-id".to_string(),
                ),
                (
                    "delta.enableInCommitTimestamps".to_string(),
                    "true".to_string(),
                ),
            ]),
        )
        .unwrap()
    }

    #[test]
    fn commit_version_0_writes_published_commit() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root.clone(), 0);
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let result = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap();
        match result {
            CommitResponse::Committed { file_meta } => {
                assert!(
                    file_meta
                        .location
                        .as_str()
                        .ends_with("00000000000000000000.json"),
                    "expected published path for version 0, got: {}",
                    file_meta.location
                );
                let commit_path = tmp_dir.path().join("_delta_log/00000000000000000000.json");
                assert!(commit_path.exists(), "000.json should exist on disk");
            }
            CommitResponse::Conflict { .. } => {
                panic!("expected Committed for version 0, got Conflict")
            }
        }
    }

    #[test]
    fn commit_version_0_conflict_when_file_exists() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let delta_log = tmp_dir.path().join("_delta_log");
        fs::create_dir_all(&delta_log).unwrap();
        fs::write(delta_log.join("00000000000000000000.json"), "existing").unwrap();

        let commit_metadata = catalog_managed_commit_metadata(table_root, 0);
        let result = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap();
        assert!(
            matches!(result, CommitResponse::Conflict { version: 0 }),
            "expected Conflict for version 0 when file exists, got: {result:?}"
        );
    }

    #[test]
    fn commit_version_0_rejects_missing_catalog_managed_feature() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = CommitMetadata::new_unchecked(table_root, 0).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let err = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("catalogManaged"),
            "expected catalogManaged error, got: {err}"
        );
    }

    #[test]
    fn commit_version_non_zero_rejects_non_catalog_managed_table() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = CommitMetadata::new_unchecked(table_root, 1).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("catalogManaged"),
            "expected catalogManaged error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_version_non_zero_emits_set_properties_for_metadata_change() {
        use std::sync::Mutex;

        use delta_kernel::committer::ProtocolMetadataIntent;

        struct CapturingClient {
            captured: Mutex<Option<UpdateTableRequest>>,
        }
        impl UpdateTableClient for CapturingClient {
            async fn update_table(
                &self,
                req: UpdateTableRequest,
            ) -> Result<ApiUpdateTableResponse> {
                *self.captured.lock().unwrap() = Some(req);
                Ok(ApiUpdateTableResponse::default())
            }
        }

        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        fs::create_dir_all(tmp_dir.path().join("_delta_log/_staged_commits")).unwrap();

        // Build a v >= 1 CommitMetadata carrying a SetProperty intent. The committer emits
        // SetProperties straight from the intent rather than diffing read vs new metadata
        // configuration.
        let commit_metadata = catalog_managed_commit_metadata(table_root.clone(), 1)
            .with_protocol_metadata_intents_for_test(vec![ProtocolMetadataIntent::SetProperty {
                key: "user.added".to_string(),
                value: "yes".to_string(),
            }]);

        let client = Arc::new(CapturingClient {
            captured: Mutex::new(None),
        });
        let committer = UCCommitter::new(
            client.clone(),
            "test-table-id",
            "test_catalog",
            "test_schema",
            "test_table",
        );
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let _ = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap();

        let req = client
            .captured
            .lock()
            .unwrap()
            .take()
            .expect("client should capture");
        assert!(matches!(
            req.updates.first(),
            Some(Update::AddCommit { .. })
        ));
        let set_props = req
            .updates
            .iter()
            .find_map(|u| match u {
                Update::SetProperties { updates } => Some(updates),
                _ => None,
            })
            .expect("expected SetProperties update");
        assert_eq!(set_props.get("user.added"), Some(&"yes".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_version_non_zero_maps_conflict_to_conflict_response() {
        struct ConflictingClient;
        impl UpdateTableClient for ConflictingClient {
            async fn update_table(&self, _: UpdateTableRequest) -> Result<ApiUpdateTableResponse> {
                Err(unity_catalog_delta_client_api::Error::CommitConflict)
            }
        }

        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        fs::create_dir_all(tmp_dir.path().join("_delta_log/_staged_commits")).unwrap();

        let commit_metadata = catalog_managed_commit_metadata(table_root, 7);
        let committer = UCCommitter::new(
            Arc::new(ConflictingClient),
            "test-table-id",
            "test_catalog",
            "test_schema",
            "test_table",
        );
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let result = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap();
        assert!(
            matches!(result, CommitResponse::Conflict { version: 7 }),
            "expected Conflict at version 7, got: {result:?}"
        );
    }

    fn staged_commit_url(table_root: &url::Url, version: Version) -> url::Url {
        table_root
            .join(&format!(
                "_delta_log/_staged_commits/{version:020}.uuid.json"
            ))
            .unwrap()
    }

    fn published_commit_url(table_root: &url::Url, version: Version) -> url::Url {
        table_root
            .join(&format!("_delta_log/{version:020}.json"))
            .unwrap()
    }

    #[tokio::test]
    async fn test_publish() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let staged_dir = tmp_dir.path().join("_delta_log/_staged_commits");
        let versions = [10u64, 11, 12];

        let catalog_commits: Vec<CatalogCommit> = versions
            .into_iter()
            .map(|v| {
                CatalogCommit::new_unchecked(
                    v,
                    staged_commit_url(&table_root, v),
                    published_commit_url(&table_root, v),
                )
            })
            .collect();

        fs::create_dir_all(&staged_dir).unwrap();
        for commit in &catalog_commits {
            let path = commit.location().to_file_path().unwrap();
            fs::write(&path, format!("version: {}", commit.version())).unwrap();
        }

        // Pre-existing 10.json should be skipped, not error.
        let existing_published = published_commit_url(&table_root, 10)
            .to_file_path()
            .unwrap();
        fs::create_dir_all(existing_published.parent().unwrap()).unwrap();
        fs::write(&existing_published, "version: 10").unwrap();

        let publish_metadata = PublishMetadata::try_new(12, catalog_commits).unwrap();
        let committer = UCCommitter::new(
            Arc::new(MockUpdateTableClient),
            "testUcTableId",
            "test_catalog",
            "test_schema",
            "test_table",
        );
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        committer.publish(&engine, publish_metadata).unwrap();

        for v in versions {
            let path = published_commit_url(&table_root, v).to_file_path().unwrap();
            assert!(path.exists());
            assert_eq!(fs::read_to_string(&path).unwrap(), format!("version: {v}"));
        }
    }
}
