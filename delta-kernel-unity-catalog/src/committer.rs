use std::sync::Arc;

use delta_kernel::committer::{
    CommitMetadata, CommitResponse, CommitType, Committer, PublishMetadata,
};
use delta_kernel::{
    DeltaResult, DeltaResultIterator, Engine, Error as DeltaError, FilteredEngineData,
};
use tracing::debug;
use unity_catalog_delta_client_api::{
    Commit, Requirement, Update, UpdateTableClient, UpdateTableRequest,
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
/// For version 0 (table creation), the committer writes `000.json` directly to the published
/// commit path. The caller (connector) is responsible for finalizing the table in UC via the
/// create table API.
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

        // Order: AddCommit registers the staged file; SetLatestBackfilledVersion is an
        // independent watermark.
        let mut updates = vec![Update::AddCommit {
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
            Err(e) => Err(DeltaError::Generic(format!("UC update_table error: {e}"))),
        }
    }
}

impl<C: UpdateTableClient + 'static> Committer for UCCommitter<C> {
    /// Commit the given `actions` to the delta table in UC.
    ///
    /// For version 0 (table creation), writes `000.json` directly to the published commit path.
    /// The connector is responsible for finalizing the table in UC via the create table API.
    ///
    /// For version >= 1, writes a staged commit then calls the UC update_table API to ratify it.
    /// Connectors should publish staged commits to the delta log immediately after writing.
    /// UC expects to be informed of the last known published version during commit.
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        // TODO: version 0 (CREATE) is not wired end-to-end yet (no connector-side table
        // registration); reject v0 commits until the create flow lands, then restore
        // `commit_version_0` and its tests.
        if commit_metadata.version() == 0 {
            return Err(DeltaError::generic(
                "CREATE (version 0) is not yet supported by UCCommitter",
            ));
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

    // TODO: restore the version-0 (CREATE) committer tests when v0 CREATE support lands:
    // commit_version_0_writes_published_commit / _conflict_when_file_exists /
    // _rejects_missing_catalog_managed_feature.

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
            "test_catalog",
            "test_schema",
            "test_table",
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
