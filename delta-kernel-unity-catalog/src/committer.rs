use std::sync::Arc;

use delta_kernel::committer::{
    CommitMetadata, CommitResponse, CommitType, Committer, PublishMetadata,
};
use delta_kernel::{
    DeltaResult, DeltaResultIterator, Engine, Error as DeltaError, FileMeta, FilteredEngineData,
};
use tracing::{debug, info};
use unity_catalog_delta_client_api::{
    Commit, DeltaTableRequirement, DeltaTableUpdate, TableName, UpdateTableClient,
    UpdateTableRequest,
};

use crate::constants::{
    CATALOG_MANAGED_FEATURE, CLUSTERING_DOMAIN_NAME, ENABLE_IN_COMMIT_TIMESTAMPS,
    IN_COMMIT_TIMESTAMP_FEATURE, UC_TABLE_ID_KEY, VACUUM_PROTOCOL_CHECK_FEATURE,
};
use crate::errors;

/// Convenience macro: returns an error if a condition is not met.
macro_rules! require {
    ($cond:expr, $err:expr) => {
        if !($cond) {
            return Err($err);
        }
    };
}

/// A [UCCommitter] is a Unity Catalog [`Committer`] implementation for committing to a specific
/// delta table in UC.
///
/// Version 0 (table creation) writes the published commit directly to the delta log.
///
/// For version >= 1, the committer writes a staged commit and calls the UC `update_table` API
/// to ratify it.
///
/// NOTE: this [`Committer`] requires a multi-threaded tokio runtime. That is, whatever
/// implementation consumes the Committer to commit to the table, must call `commit` from within a
/// multi-threaded tokio runtime context. Since the default engine uses tokio, this is compatible,
/// but must ensure that the multi-threaded runtime is used.
#[derive(Debug, Clone)]
pub struct UCCommitter<C: UpdateTableClient> {
    update_table_client: Arc<C>,
    table_id: String,
    table: TableName,
}

impl<C: UpdateTableClient> UCCommitter<C> {
    /// Build a committer that issues commits for the UC-managed table `table` via
    /// `update_table_client`.
    pub fn new(update_table_client: Arc<C>, table_id: impl Into<String>, table: TableName) -> Self {
        UCCommitter {
            update_table_client,
            table_id: table_id.into(),
            table,
        }
    }

    /// Returns true if the commit metadata has the `catalogManaged` feature in both reader and
    /// writer features.
    fn has_catalog_managed_feature(commit_metadata: &CommitMetadata) -> bool {
        commit_metadata.has_writer_feature(CATALOG_MANAGED_FEATURE)
            && commit_metadata.has_reader_feature(CATALOG_MANAGED_FEATURE)
    }

    /// Validates that protocol features and metadata properties are correct for a UC
    /// catalog-managed table.
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
        // UC mandates vacuumProtocolCheck for its catalog-managed tables.
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

    /// Validates that this commit does not include ALTER TABLE changes (protocol, metadata,
    /// or clustering column changes).
    fn validate_no_alter_table_changes(commit_metadata: &CommitMetadata) -> DeltaResult<()> {
        require!(
            !commit_metadata.has_protocol_change(),
            errors::alter_table_unsupported("protocol")
        );
        require!(
            !commit_metadata.has_metadata_change(),
            errors::alter_table_unsupported("metadata")
        );
        require!(
            !commit_metadata.has_domain_metadata_change(CLUSTERING_DOMAIN_NAME),
            errors::alter_table_unsupported("clustering columns")
        );
        Ok(())
    }

    /// Commit version 0 (table creation). Validates that all required UC properties are present,
    /// then writes the version 0 commit file directly to the published commit path.
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
                let file_meta = FileMeta::new(
                    published_commit_path,
                    commit_metadata.in_commit_timestamp(),
                    0,
                );
                Ok(CommitResponse::Committed { file_meta })
            }
            Err(DeltaError::FileAlreadyExists(_)) => {
                info!("version 0 commit conflict: commit file already exists");
                Ok(CommitResponse::Conflict { version: 0 })
            }
            Err(e) => Err(e),
        }
    }

    /// Commit version >= 1. Validates catalog-managed status hasn't changed, writes a staged
    /// commit file, and calls the UC commit API to ratify it.
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
        Self::validate_no_alter_table_changes(&commit_metadata)?;
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

        let mut updates = vec![DeltaTableUpdate::AddCommit {
            commit: Commit {
                version: version_i64,
                timestamp: commit_metadata.in_commit_timestamp(),
                file_name,
                file_size: file_size_i64,
                file_modification_timestamp: committed.last_modified,
            },
        }];
        if let Some(max_pub) = commit_metadata.max_published_version() {
            let v: i64 = max_pub.try_into().map_err(|_| {
                DeltaError::Generic(format!(
                    "Max published version {max_pub} does not fit into i64 for UC commit"
                ))
            })?;
            updates.push(DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version: v,
            });
        }
        let update_req = UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertTableUuid {
                uuid: self.table_id.clone(),
            }],
            updates,
        )
        .map_err(|e| DeltaError::generic(format!("invalid UC update_table request: {e}")))?;
        let target = self.table.clone();

        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            DeltaError::generic("UCCommitter may only be used within a tokio runtime")
        })?;
        // `block_in_place` panics if the current runtime isn't multi-threaded. We can't check for
        // that up front: `runtime_flavor()` can't tell a real single-threaded runtime (where this
        // panics) apart from the FFI case (single-threaded on top of multi-threaded, where it's
        // fine). So we let it run and catch the panic.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                handle.block_on(async move {
                    self.update_table_client
                        .update_table(&target, update_req)
                        .await
                })
            })
        }))
        .map_err(|_| DeltaError::generic("UCCommitter requires a multi-threaded tokio runtime"))?;
        match result {
            Ok(_) => Ok(CommitResponse::Committed {
                file_meta: committed,
            }),
            // TODO(#2970): classify version conflicts as CommitResponse::Conflict so the
            // transaction layer can rebase/retry, instead of collapsing every error to Generic.
            Err(e) => Err(DeltaError::Generic(format!("UC update_table error: {e}"))),
        }
    }
}

impl<C: UpdateTableClient + 'static> Committer for UCCommitter<C> {
    /// Commit the given `actions` to the delta table in UC.
    ///
    /// For version 0, the caller finalizes the table in UC separately. For version >= 1, connectors
    /// should publish staged commits to the delta log immediately after writing; UC expects to be
    /// informed of the last known published version during commit.
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

    use super::*;

    struct MockUpdateTableClient;

    impl UpdateTableClient for MockUpdateTableClient {
        async fn update_table(&self, _: &TableName, _: UpdateTableRequest) -> Result<()> {
            unimplemented!()
        }
    }

    fn test_committer() -> UCCommitter<MockUpdateTableClient> {
        UCCommitter::new(
            Arc::new(MockUpdateTableClient),
            "test-table-id",
            TableName::new("test_catalog", "test_schema", "test_table"),
        )
    }

    /// Creates a valid catalog-managed CommitMetadata with all required UC features and properties.
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

        // Create the _delta_log directory
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
                assert_eq!(file_meta.size, 0);
                // Verify the file was written to disk
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

        // Pre-create the commit file to trigger a conflict
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
    fn commit_version_0_rejects_missing_table_id() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        // Has features but missing io.unitycatalog.tableId in config
        let commit_metadata = CommitMetadata::new_unchecked_with(
            table_root,
            0,
            vec!["catalogManaged", "vacuumProtocolCheck"],
            vec!["catalogManaged", "inCommitTimestamp", "vacuumProtocolCheck"],
            HashMap::from([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            )]),
        )
        .unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let err = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("io.unitycatalog.tableId"),
            "expected tableId error, got: {err}"
        );
    }

    #[test]
    fn commit_version_0_rejects_missing_ict_enablement() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        // Has features and tableId but missing delta.enableInCommitTimestamps=true
        let commit_metadata = CommitMetadata::new_unchecked_with(
            table_root,
            0,
            vec!["catalogManaged", "vacuumProtocolCheck"],
            vec!["catalogManaged", "inCommitTimestamp", "vacuumProtocolCheck"],
            HashMap::from([(
                "io.unitycatalog.tableId".to_string(),
                "test-table-id".to_string(),
            )]),
        )
        .unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let err = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("enableInCommitTimestamps"),
            "expected ICT enablement error, got: {err}"
        );
    }

    #[test]
    fn commit_version_non_zero_rejects_non_catalog_managed_table() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        // Version >= 1 but without catalogManaged feature (simulates downgrade attempt)
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

    // A default `#[tokio::test]` is a current-thread runtime. `block_in_place` would panic there,
    // so the committer must reject the flavor with an error instead.
    #[tokio::test]
    async fn commit_version_non_zero_rejects_current_thread_runtime() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1);
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let err = test_committer()
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("multi-threaded"),
            "expected a multi-threaded-runtime error, got: {err}"
        );
    }

    #[test]
    fn commit_version_non_zero_rejects_protocol_change() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1).with_protocol_change();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = test_committer()
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("table protocol"),
            "expected protocol change error, got: {err}"
        );
    }

    #[test]
    fn commit_version_non_zero_rejects_metadata_change() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1).with_metadata_change();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = test_committer()
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("table metadata"),
            "expected metadata change error, got: {err}"
        );
    }

    #[test]
    fn commit_version_non_zero_rejects_clustering_change() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata =
            catalog_managed_commit_metadata(table_root, 1).with_domain_change("delta.clustering");
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = test_committer()
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("clustering columns"),
            "expected clustering change error, got: {err}"
        );
    }

    #[test]
    fn commit_version_non_zero_rejects_mismatched_table_id() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1);
        // Committer initialized with a different table ID than what's in the metadata.
        let committer = UCCommitter::new(
            Arc::new(MockUpdateTableClient),
            "different-table-id",
            TableName::new("test_catalog", "test_schema", "test_table"),
        );
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = committer
            .commit(&engine, Box::new(std::iter::empty()), commit_metadata)
            .unwrap_err();
        assert!(
            err.to_string().contains("table ID mismatch"),
            "expected table ID mismatch error, got: {err}"
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

        // ===== GIVEN =====
        // Create catalog commits
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

        // Write staged commit files to disk
        fs::create_dir_all(&staged_dir).unwrap();
        for commit in &catalog_commits {
            let path = commit.location().to_file_path().unwrap();
            fs::write(&path, format!("version: {}", commit.version())).unwrap();
        }

        // Write 10.json file to disk (should be skipped, not error)
        let existing_published = published_commit_url(&table_root, 10)
            .to_file_path()
            .unwrap();
        fs::create_dir_all(existing_published.parent().unwrap()).unwrap();
        fs::write(&existing_published, "version: 10").unwrap();

        // ===== WHEN =====
        let publish_metadata = PublishMetadata::try_new(12, catalog_commits).unwrap();
        let committer = UCCommitter::new(
            Arc::new(MockUpdateTableClient),
            "testUcTableId",
            TableName::new("test_catalog", "test_schema", "test_table"),
        );
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        committer.publish(&engine, publish_metadata).unwrap();

        // ===== THEN =====
        for v in versions {
            let path = published_commit_url(&table_root, v).to_file_path().unwrap();
            assert!(path.exists());
            assert_eq!(fs::read_to_string(&path).unwrap(), format!("version: {v}"));
        }
    }
}
