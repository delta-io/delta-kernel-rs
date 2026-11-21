use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

#[cfg(feature = "declarative-plans")]
use delta_kernel::committer::write_commit_json_declarative;
use delta_kernel::committer::{
    CommitMetadata, CommitResponse, CommitType, Committer, PublishMetadata, WinningCommits,
    list_published_commits_and_merge_ratified_tail,
};
#[cfg(feature = "declarative-plans")]
use delta_kernel::plans::{RelationRef, ScopedPlanExecutor};
use delta_kernel::{
    Engine, FileMeta, FilteredEngineData, KernelError as DeltaError, KernelResult,
    KernelResultIterator,
};
use tracing::{debug, info};
use unity_catalog_delta_client_api::{
    Commit, DeltaTableRequirement, DeltaTableUpdate, Error as ClientError, TableIdentifier,
    UpdateTableClient, UpdateTableRequest,
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
/// Catalog requests require a multi-threaded Tokio runtime. Call `commit`, `commit_declarative`,
/// and [`Committer::get_winning_commits`] from within that runtime.
#[derive(Debug, Clone)]
pub struct UCCommitter<C: UpdateTableClient> {
    update_table_client: Arc<C>,
    table_id: String,
    table: TableIdentifier,
}

impl<C: UpdateTableClient> UCCommitter<C> {
    /// Build a committer that issues commits for the UC-managed table `table` via
    /// `update_table_client`.
    pub fn new(
        update_table_client: Arc<C>,
        table_id: impl Into<String>,
        table: TableIdentifier,
    ) -> Self {
        UCCommitter {
            update_table_client,
            table_id: table_id.into(),
            table,
        }
    }

    fn fetch_ratified_commits(
        &self,
        table_root: &url::Url,
    ) -> KernelResult<(Vec<delta_kernel::LogPath>, delta_kernel::Version)> {
        let state = block_on_catalog(
            self.update_table_client
                .get_commits(&self.table, &self.table_id),
        )?
        .map_err(|e| DeltaError::generic(format!("UC commit-history read failed: {e}")))?;
        require!(
            state.table_uuid == self.table_id,
            DeltaError::generic("Catalog commit-history response identifies a different table")
        );
        let latest_version = u64::try_from(state.latest_version)
            .map_err(|_| DeltaError::generic("Catalog returned a negative ratified version"))?;
        let log_tail = crate::log_tail_from_commits(&state.commits, table_root.clone())?;
        Ok((log_tail, latest_version))
    }

    /// Returns true if the commit metadata has the `catalogManaged` feature in both reader and
    /// writer features.
    fn has_catalog_managed_feature(commit_metadata: &CommitMetadata) -> bool {
        commit_metadata.has_writer_feature(CATALOG_MANAGED_FEATURE)
            && commit_metadata.has_reader_feature(CATALOG_MANAGED_FEATURE)
    }

    /// Validates that protocol features and metadata properties are correct for a UC
    /// catalog-managed table.
    fn validate_catalog_managed_state(&self, commit_metadata: &CommitMetadata) -> KernelResult<()> {
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
    fn validate_no_alter_table_changes(commit_metadata: &CommitMetadata) -> KernelResult<()> {
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
        commit_metadata: &CommitMetadata,
        write: impl FnOnce(&url::Url) -> KernelResult<FileMeta>,
    ) -> KernelResult<CommitResponse> {
        debug_assert!(
            commit_metadata.version() == 0,
            "commit_version_0 called with version {}",
            commit_metadata.version()
        );
        self.validate_catalog_managed_state(commit_metadata)?;
        let published_commit_path = commit_metadata.published_commit_path()?;
        match write(&published_commit_path) {
            Ok(written_file) => {
                info!("wrote version 0 commit file for UC table creation");
                let file_meta = FileMeta::new(
                    published_commit_path,
                    commit_metadata.in_commit_timestamp(),
                    written_file.size,
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
        commit_metadata: CommitMetadata,
        write: impl FnOnce(&url::Url) -> KernelResult<FileMeta>,
    ) -> KernelResult<CommitResponse>
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
        let committed = write(&staged_commit_path)?;
        debug!("wrote staged commit file: {:?}", committed);

        let mut updates = vec![DeltaTableUpdate::AddCommit {
            commit: Commit {
                version: u64_to_wire_i64(commit_metadata.version(), "commit version")?,
                timestamp: commit_metadata.in_commit_timestamp(),
                file_name: staged_commit_file_name(&staged_commit_path)?,
                file_size: u64_to_wire_i64(committed.size, "committed size")?,
                file_modification_timestamp: committed.last_modified,
            },
        }];
        if let Some(max_pub) = commit_metadata.max_published_version() {
            updates.push(DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version: u64_to_wire_i64(max_pub, "max published version")?,
            });
        }
        let update_req = UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertTableUuid {
                uuid: self.table_id.clone(),
            }],
            updates,
        )
        .map_err(|e| DeltaError::generic(format!("invalid UC update_table request: {e}")))?;
        let result = block_on_catalog(
            self.update_table_client
                .update_table(&self.table, update_req),
        )?;
        match result {
            Ok(_) => Ok(CommitResponse::Committed {
                file_meta: committed,
            }),
            Err(ClientError::CommitVersionAlreadyExists { version })
                if u64::try_from(version).ok() == Some(commit_metadata.version()) =>
            {
                Ok(CommitResponse::Conflict {
                    version: commit_metadata.version(),
                })
            }
            Err(
                e @ (ClientError::Retryable(_) | ClientError::MaxUnpublishedCommitsExceeded(_)),
            ) => Ok(CommitResponse::Retryable {
                error: DeltaError::generic(format!("UC update_table error: {e}")),
            }),
            Err(e) => Err(DeltaError::Generic(format!("UC update_table error: {e}"))),
        }
    }
}

impl<C: UpdateTableClient + 'static> Committer for UCCommitter<C> {
    /// Commit the given `actions` to the delta table in UC.
    ///
    /// For version 0 (table creation), the committer writes `000.json` directly to the published
    /// commit path; the caller finalizes the table in UC via the create-table API. For version >=
    /// 1, connectors should publish staged commits to the delta log immediately after writing;
    /// UC expects to be informed of the last known published version during commit.
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        if commit_metadata.version() == 0 {
            return self.commit_version_0(&commit_metadata, |path| {
                let file_size =
                    engine
                        .json_handler()
                        .write_json_file(path, Box::new(actions), false)?;
                Ok(FileMeta::new(
                    path.clone(),
                    commit_metadata.in_commit_timestamp(),
                    file_size,
                ))
            });
        }
        self.commit_version_non_zero(commit_metadata, |path| {
            engine
                .json_handler()
                .write_json_file(path, Box::new(actions), false)?;
            engine.storage_handler().head(path)
        })
    }

    #[cfg(feature = "declarative-plans")]
    fn commit_declarative(
        &self,
        executor: &dyn ScopedPlanExecutor,
        actions: Box<dyn Iterator<Item = RelationRef> + '_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        if commit_metadata.version() == 0 {
            return self.commit_version_0(&commit_metadata, |path| {
                write_commit_json_declarative(executor, actions, path.clone())
            });
        }
        self.commit_version_non_zero(commit_metadata, |path| {
            write_commit_json_declarative(executor, actions, path.clone())
        })
    }

    /// Loads winning history from published files and catalog-ratified staged commits.
    ///
    /// Discovery for a version 0 conflict is unsupported because it would load another table's
    /// history after a failed create.
    fn get_winning_commits(
        &self,
        engine: &dyn Engine,
        table_root: &url::Url,
        conflict_version: delta_kernel::Version,
    ) -> KernelResult<WinningCommits> {
        require!(
            conflict_version != 0,
            DeltaError::unsupported("A create-table conflict cannot be rebased onto another table")
        );
        let (log_tail, latest_version) = self.fetch_ratified_commits(table_root)?;
        list_published_commits_and_merge_ratified_tail(
            engine,
            table_root,
            conflict_version,
            log_tail,
            Some(latest_version),
        )
    }

    fn is_catalog_committer(&self) -> bool {
        true
    }

    fn publish(&self, engine: &dyn Engine, publish_metadata: PublishMetadata) -> KernelResult<()> {
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

fn block_on_catalog<T>(
    future: impl Future<Output = unity_catalog_delta_client_api::Result<T>>,
) -> KernelResult<unity_catalog_delta_client_api::Result<T>> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|_| DeltaError::generic("UCCommitter may only be used within a tokio runtime"))?;
    // The FFI runtime can appear single-threaded even when block_in_place is supported, so
    // runtime_flavor cannot reliably replace catching this runtime precondition failure.
    catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| handle.block_on(future))
    }))
    .map_err(|panic| {
        let msg = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
        DeltaError::generic(format!(
            "UCCommitter catalog request panicked (requires a multi-threaded tokio runtime): {msg}"
        ))
    })
}

/// Convert a `u64` to the `i64` the UC wire types use, erroring if it does not fit.
fn u64_to_wire_i64(value: u64, field: &str) -> KernelResult<i64> {
    value
        .try_into()
        .map_err(|_| DeltaError::generic(format!("{field} does not fit into i64 for UC commit")))
}

fn staged_commit_file_name(path: &url::Url) -> KernelResult<String> {
    path.path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
        .ok_or_else(|| DeltaError::generic("staged commit path has no file name"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use delta_kernel::arrow::array::{Array, RecordBatch, StringArray};
    use delta_kernel::committer::{CatalogCommit, CommitMetadata};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    #[cfg(feature = "declarative-plans")]
    use delta_kernel::engine::sync::plan::SyncPlanExecutor;
    use delta_kernel::object_store::local::LocalFileSystem;
    #[cfg(feature = "declarative-plans")]
    use delta_kernel::plans::{PlanBuilder, ScopedPlanExecutor};
    #[cfg(feature = "declarative-plans")]
    use delta_kernel::schema::schema_ref;
    use delta_kernel::{
        EngineData, EvaluationHandler, FilteredEngineData, JsonHandler, ParquetHandler,
        StorageHandler, Version,
    };
    use delta_kernel_default_engine::DefaultEngine;
    use rstest::rstest;
    use unity_catalog_delta_client_api::error::Result;
    use unity_catalog_delta_client_api::{CommitState, InMemoryUpdateTableClient};

    use super::*;

    #[derive(Default)]
    struct MockUpdateTableClient {
        requests: Mutex<Vec<UpdateTableRequest>>,
        error: Mutex<Option<ClientError>>,
        history: Mutex<Option<CommitState>>,
        history_calls: AtomicUsize,
    }

    impl UpdateTableClient for MockUpdateTableClient {
        async fn get_commits(&self, _: &TableIdentifier, _: &str) -> Result<CommitState> {
            self.history_calls.fetch_add(1, Ordering::SeqCst);
            self.history.lock().unwrap().clone().ok_or_else(|| {
                ClientError::UnsupportedOperation("Commit history unavailable".into())
            })
        }

        async fn update_table(
            &self,
            _: &TableIdentifier,
            request: UpdateTableRequest,
        ) -> Result<()> {
            self.requests.lock().unwrap().push(request);
            self.error.lock().unwrap().take().map_or(Ok(()), Err)
        }
    }

    #[derive(Default)]
    struct CountingCatalogClient {
        inner: InMemoryUpdateTableClient,
        history_calls: AtomicUsize,
    }

    impl UpdateTableClient for CountingCatalogClient {
        async fn update_table(
            &self,
            target: &TableIdentifier,
            request: UpdateTableRequest,
        ) -> Result<()> {
            self.inner.update_table(target, request).await
        }

        async fn get_commits(
            &self,
            target: &TableIdentifier,
            table_uuid: &str,
        ) -> Result<CommitState> {
            self.history_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_commits(target, table_uuid).await
        }
    }

    struct CountingEngine {
        inner: Arc<dyn Engine>,
        storage_calls: AtomicUsize,
    }

    impl Engine for CountingEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.inner.evaluation_handler()
        }

        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            self.storage_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.storage_handler()
        }

        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            self.inner.json_handler()
        }

        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            self.inner.parquet_handler()
        }

        #[cfg(feature = "declarative-plans")]
        fn plan_executor(&self) -> Option<Arc<dyn delta_kernel::plans::PlanExecutor>> {
            self.inner.plan_executor()
        }
    }

    fn test_committer() -> UCCommitter<MockUpdateTableClient> {
        UCCommitter::new(
            Arc::new(MockUpdateTableClient::default()),
            "test-table-id",
            TableIdentifier::new("test_catalog", "test_schema", "test_table"),
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

    #[derive(Clone, Copy)]
    enum CommitStyle {
        Imperative,
        #[cfg(feature = "declarative-plans")]
        Declarative,
    }

    fn commit_with_style<C: UpdateTableClient + 'static>(
        committer: &UCCommitter<C>,
        engine: &dyn Engine,
        style: CommitStyle,
        commit_metadata: CommitMetadata,
    ) -> delta_kernel::KernelResult<CommitResponse> {
        commit_with_values(committer, engine, style, commit_metadata, &["value"])
    }

    fn commit_with_values<C: UpdateTableClient + 'static>(
        committer: &UCCommitter<C>,
        engine: &dyn Engine,
        style: CommitStyle,
        commit_metadata: CommitMetadata,
        values: &[&str],
    ) -> delta_kernel::KernelResult<CommitResponse> {
        match style {
            CommitStyle::Imperative => {
                let actions = values.iter().map(|value| {
                    let action: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
                        RecordBatch::try_from_iter(vec![(
                            "test",
                            Arc::new(StringArray::from(vec![*value])) as Arc<dyn Array>,
                        )])
                        .unwrap(),
                    ));
                    Ok(FilteredEngineData::with_all_rows_selected(action))
                });
                let actions = Box::new(actions);
                committer.commit(engine, actions, commit_metadata)
            }
            #[cfg(feature = "declarative-plans")]
            CommitStyle::Declarative => {
                let executor = SyncPlanExecutor::default().get_scoped();
                let actions = values
                    .iter()
                    .enumerate()
                    .map(|(i, value)| {
                        let source = PlanBuilder::values(
                            schema_ref! { nullable "test": STRING },
                            vec![vec![(*value).into()]],
                        )?
                        .build()?;
                        executor.execute_and_retain(&format!("actions-{i}"), source)
                    })
                    .collect::<delta_kernel::KernelResult<Vec<_>>>()?;
                committer.commit_declarative(
                    &executor,
                    Box::new(actions.into_iter()),
                    commit_metadata,
                )
            }
        }
    }

    #[rstest]
    #[case::connection(|| ClientError::Retryable("Connection refused".into()), true, false)]
    #[case::unpublished_limit(|| ClientError::MaxUnpublishedCommitsExceeded(20), true, false)]
    #[case::authentication(|| ClientError::AuthenticationFailed, false, false)]
    #[case::uncertain(|| ClientError::Generic("Response lost".into()), false, false)]
    #[case::conflict_history_unavailable(|| ClientError::CommitVersionAlreadyExists { version: 1 }, false, true)]
    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_failures_are_retryable_only_for_confirmed_temporary_rejections(
        #[case] error: fn() -> ClientError,
        #[case] retryable: bool,
        #[case] conflicted: bool,
    ) {
        for style in [
            CommitStyle::Imperative,
            #[cfg(feature = "declarative-plans")]
            CommitStyle::Declarative,
        ] {
            let tmp_dir = tempfile::tempdir().unwrap();
            let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
            fs::create_dir_all(tmp_dir.path().join("_delta_log/_staged_commits")).unwrap();
            let client = Arc::new(MockUpdateTableClient {
                error: Mutex::new(Some(error())),
                ..Default::default()
            });
            let committer = UCCommitter::new(
                client.clone(),
                "test-table-id",
                TableIdentifier::new("test_catalog", "test_schema", "test_table"),
            );
            let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
            let result = commit_with_style(
                &committer,
                &engine,
                style,
                catalog_managed_commit_metadata(table_root.clone(), 1),
            );
            if conflicted {
                assert!(matches!(
                    result,
                    Ok(CommitResponse::Conflict { version: 1 })
                ));
                assert_eq!(client.requests.lock().unwrap().len(), 1);
                assert_eq!(client.history_calls.load(Ordering::SeqCst), 0);
                let error = committer
                    .get_winning_commits(&engine, &table_root, 1)
                    .unwrap_err();
                assert!(error.to_string().contains("UC commit-history read failed"));
                assert_eq!(client.history_calls.load(Ordering::SeqCst), 1);
            } else if retryable {
                assert!(matches!(result, Ok(CommitResponse::Retryable { .. })));
                let retried = commit_with_style(
                    &committer,
                    &engine,
                    style,
                    catalog_managed_commit_metadata(table_root, 1),
                )
                .unwrap();
                assert!(matches!(retried, CommitResponse::Committed { .. }));
                assert_eq!(client.requests.lock().unwrap().len(), 2);
            } else {
                assert!(result.is_err());
                assert_eq!(client.requests.lock().unwrap().len(), 1);
            }
        }
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_conflict_defers_discovery_and_merges_published_and_staged_winners(
        #[case] style: CommitStyle,
        #[values(0, 2, 3)] published_until: u64,
    ) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        fs::create_dir_all(tmp_dir.path().join("_delta_log/_staged_commits")).unwrap();
        let client = Arc::new(CountingCatalogClient::default());
        client.inner.create_table("test-table-id").unwrap();
        let committer = UCCommitter::new(
            client.clone(),
            "test-table-id",
            TableIdentifier::new("test_catalog", "test_schema", "test_table"),
        );
        let engine = CountingEngine {
            inner: Arc::new(DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build()),
            storage_calls: AtomicUsize::new(0),
        };
        let mut ratified_paths = Vec::new();
        for version in 1..=3 {
            let result = commit_with_style(
                &committer,
                &engine,
                style,
                catalog_managed_commit_metadata(table_root.clone(), version),
            )
            .unwrap();
            let CommitResponse::Committed { file_meta } = result else {
                panic!("expected a ratified commit");
            };
            ratified_paths.push(file_meta);
        }
        let storage_calls_before_conflict = engine.storage_calls.load(Ordering::SeqCst);
        let result = commit_with_style(
            &committer,
            &engine,
            style,
            catalog_managed_commit_metadata(table_root.clone(), 1),
        )
        .unwrap();
        let CommitResponse::Conflict { version } = result else {
            panic!("expected a confirmed version conflict");
        };
        assert_eq!(version, 1);
        assert_eq!(client.history_calls.load(Ordering::SeqCst), 0);
        // Imperative submission requests storage only to HEAD the staged proposal. Declarative
        // submission uses its scoped executor and never requests an engine storage handler.
        let submission_storage_calls = match style {
            CommitStyle::Imperative => 1,
            #[cfg(feature = "declarative-plans")]
            CommitStyle::Declarative => 0,
        };
        assert_eq!(
            engine.storage_calls.load(Ordering::SeqCst),
            storage_calls_before_conflict + submission_storage_calls,
        );
        // The losing proposal also exists on disk, but only the catalog identifies winners.
        let staged_count = fs::read_dir(tmp_dir.path().join("_delta_log/_staged_commits"))
            .unwrap()
            .count();
        assert_eq!(staged_count, 4);
        let state = committer
            .update_table_client
            .inner
            .get_commits(&committer.table, &committer.table_id)
            .await
            .unwrap();
        if published_until > 0 {
            let commits = state
                .commits
                .iter()
                .filter(|commit| commit.version <= published_until as i64)
                .map(|commit| {
                    let src = table_root
                        .join(&format!("_delta_log/_staged_commits/{}", commit.file_name))
                        .unwrap();
                    let dest = table_root
                        .join(&format!("_delta_log/{:020}.json", commit.version))
                        .unwrap();
                    CatalogCommit::new_unchecked(commit.version as u64, src, dest)
                })
                .collect();
            committer
                .publish(
                    &engine,
                    PublishMetadata::try_new(published_until, commits).unwrap(),
                )
                .unwrap();
            client.inner.insert_table(
                "test-table-id",
                unity_catalog_delta_client_api::TableData {
                    max_ratified_version: state.latest_version,
                    catalog_commits: state
                        .commits
                        .into_iter()
                        .filter(|commit| commit.version > published_until as i64)
                        .collect(),
                },
            );
        }
        // Versions newer than the catalog read are outside the pinned history.
        fs::write(
            tmp_dir.path().join("_delta_log/00000000000000000004.json"),
            "not ratified",
        )
        .unwrap();
        let storage_calls_before_discovery = engine.storage_calls.load(Ordering::SeqCst);
        let winners = committer
            .get_winning_commits(&engine, &table_root, version)
            .unwrap();
        assert_eq!(client.history_calls.load(Ordering::SeqCst), 1);
        assert!(engine.storage_calls.load(Ordering::SeqCst) > storage_calls_before_discovery);
        assert_eq!(winners.len(), 3);
        for (winner, ratified) in winners.iter().zip(ratified_paths) {
            let expected_path = if winner.version <= published_until {
                published_commit_url(&table_root, winner.version)
            } else {
                ratified.location
            };
            assert_eq!(
                winner.log_path,
                delta_kernel::LogPath::try_new(
                    engine.storage_handler().head(&expected_path).unwrap()
                )
                .unwrap(),
            );
            assert_eq!(
                fs::read_to_string(expected_path.to_file_path().unwrap()).unwrap(),
                "{\"test\":\"value\"}\n",
            );
        }
        assert_eq!(
            winners
                .iter()
                .map(|winner| winner.version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn winning_history_rejects_wrong_table_before_filesystem_io() {
        let committer = test_committer();
        *committer.update_table_client.history.lock().unwrap() = Some(CommitState {
            table_uuid: "different-table-id".into(),
            latest_version: 1,
            commits: vec![],
        });
        let table_root = url::Url::parse("file:///table/").unwrap();
        let error = committer
            .get_winning_commits(&delta_kernel::engine::PanicEngine, &table_root, 1)
            .unwrap_err();
        assert!(error.to_string().contains("different table"));
    }

    #[rstest]
    #[case::absolute_location("file:///other-table/_delta_log/00000000000000000001.json")]
    #[case::parent_location("../00000000000000000001.json")]
    #[tokio::test(flavor = "multi_thread")]
    async fn winning_history_rejects_invalid_locations_before_filesystem_io(
        #[case] file_name: &str,
    ) {
        let committer = test_committer();
        *committer.update_table_client.history.lock().unwrap() = Some(CommitState {
            table_uuid: "test-table-id".into(),
            latest_version: 1,
            commits: vec![Commit::new(1, 0, file_name, 1, 0)],
        });
        let table_root = url::Url::parse("file:///table/").unwrap();
        assert!(matches!(
            committer.get_winning_commits(&delta_kernel::engine::PanicEngine, &table_root, 1),
            Err(DeltaError::InvalidLogPath(_)),
        ));
    }

    #[rstest]
    #[case::empty(&[], &[], |error: &DeltaError| matches!(error, DeltaError::EmptyLog))]
    #[case::missing_start(&[2, 3], &[], |error: &DeltaError| matches!(error, DeltaError::StartVersionNotFound { .. }))]
    #[case::missing_published_prefix(&[], &[3], |error: &DeltaError| matches!(error, DeltaError::StartVersionNotFound { .. }))]
    #[case::gapped_published(&[1, 3], &[], |error: &DeltaError| matches!(error, DeltaError::MissingVersion(2)))]
    #[case::gapped_catalog(&[], &[1, 3], |error: &DeltaError| matches!(error, DeltaError::LogTailVersionsNotContiguous { .. }))]
    #[case::missing_catalog_end(&[1], &[2], |error: &DeltaError| matches!(error, DeltaError::MaxCatalogVersion(_)))]
    #[tokio::test(flavor = "multi_thread")]
    async fn winning_history_rejects_missing_or_gapped_commits(
        #[case] published_versions: &[Version],
        #[case] staged_versions: &[Version],
        #[case] expected_error: fn(&DeltaError) -> bool,
    ) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();
        for version in published_versions {
            fs::write(
                published_commit_url(&table_root, *version)
                    .to_file_path()
                    .unwrap(),
                "published",
            )
            .unwrap();
        }
        let committer = test_committer();
        *committer.update_table_client.history.lock().unwrap() = Some(CommitState {
            table_uuid: "test-table-id".into(),
            latest_version: 3,
            commits: staged_versions
                .iter()
                .map(|version| {
                    Commit::new(
                        *version as i64,
                        0,
                        format!("{version:020}.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
                        1,
                        0,
                    )
                })
                .collect(),
        });
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        let error = committer
            .get_winning_commits(&engine, &table_root, 1)
            .unwrap_err();
        assert!(expected_error(&error), "unexpected history error: {error}");
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_0_writes_published_commit(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root.clone(), 0);
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        // Create the _delta_log directory
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let result = commit_with_style(&committer, &engine, style, commit_metadata).unwrap();
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
                let stored_size = fs::metadata(&commit_path).unwrap().len();
                assert!(file_meta.size > 0);
                assert_eq!(file_meta.size, stored_size);
                assert_eq!(
                    fs::read_to_string(commit_path).unwrap(),
                    "{\"test\":\"value\"}\n"
                );
            }
            response => panic!("expected Committed for version 0, got {response:?}"),
        }
        assert!(
            committer
                .update_table_client
                .requests
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_version_non_zero_writes_staged_commit_and_ratifies(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        fs::create_dir_all(tmp_dir.path().join("_delta_log/_staged_commits")).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        let response = commit_with_values(
            &committer,
            &engine,
            style,
            catalog_managed_commit_metadata(table_root, 1),
            &["first", "second"],
        )
        .unwrap();
        let CommitResponse::Committed { file_meta } = response else {
            panic!("expected a committed response");
        };
        assert!(file_meta.location.as_str().contains("/_staged_commits/"));
        assert_eq!(
            fs::read_to_string(file_meta.location.to_file_path().unwrap()).unwrap(),
            "{\"test\":\"first\"}\n{\"test\":\"second\"}\n"
        );
        assert_eq!(
            file_meta.size,
            fs::metadata(file_meta.location.to_file_path().unwrap())
                .unwrap()
                .len()
        );

        let requests = committer.update_table_client.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.table_uuid(), Some("test-table-id"));
        let staged = request.staged_commit().unwrap();
        assert_eq!(staged.version, 1);
        assert_eq!(staged.timestamp, 0);
        assert_eq!(staged.file_size, file_meta.size as i64);
        assert_eq!(staged.file_modification_timestamp, file_meta.last_modified);
        assert_eq!(
            staged.file_name,
            staged_commit_file_name(&file_meta.location).unwrap()
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_0_conflict_when_file_exists(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        // Pre-create the commit file to trigger a conflict
        let delta_log = tmp_dir.path().join("_delta_log");
        fs::create_dir_all(&delta_log).unwrap();
        fs::write(delta_log.join("00000000000000000000.json"), "existing").unwrap();

        let commit_metadata = catalog_managed_commit_metadata(table_root.clone(), 0);
        let result = commit_with_style(&committer, &engine, style, commit_metadata).unwrap();
        assert!(
            matches!(result, CommitResponse::Conflict { version: 0 }),
            "expected Conflict for version 0 when file exists, got: {result:?}"
        );
        assert!(matches!(
            committer.get_winning_commits(&delta_kernel::engine::PanicEngine, &table_root, 0),
            Err(DeltaError::Unsupported(_)),
        ));
        assert_eq!(
            committer
                .update_table_client
                .history_calls
                .load(Ordering::SeqCst),
            0,
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_0_rejects_missing_catalog_managed_feature(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = CommitMetadata::new_unchecked(table_root, 0).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let err = commit_with_style(&committer, &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("catalogManaged"),
            "expected catalogManaged error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_0_rejects_missing_table_id(#[case] style: CommitStyle) {
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

        let err = commit_with_style(&committer, &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("io.unitycatalog.tableId"),
            "expected tableId error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_0_rejects_missing_ict_enablement(#[case] style: CommitStyle) {
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

        let err = commit_with_style(&committer, &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("enableInCommitTimestamps"),
            "expected ICT enablement error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_non_zero_rejects_non_catalog_managed_table(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        // Version >= 1 but without catalogManaged feature (simulates downgrade attempt)
        let commit_metadata = CommitMetadata::new_unchecked(table_root, 1).unwrap();
        let committer = test_committer();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = commit_with_style(&committer, &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("catalogManaged"),
            "expected catalogManaged error, got: {err}"
        );
    }

    // A default `#[tokio::test]` is a current-thread runtime. `block_in_place` would panic there,
    // so the committer must reject the flavor with an error instead.
    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    #[tokio::test]
    async fn commit_version_non_zero_rejects_current_thread_runtime(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1);
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        fs::create_dir_all(tmp_dir.path().join("_delta_log")).unwrap();

        let err =
            commit_with_style(&test_committer(), &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("multi-threaded"),
            "expected a multi-threaded-runtime error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_non_zero_rejects_protocol_change(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1).with_protocol_change();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err =
            commit_with_style(&test_committer(), &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("table protocol"),
            "expected protocol change error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_non_zero_rejects_metadata_change(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1).with_metadata_change();
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err =
            commit_with_style(&test_committer(), &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("table metadata"),
            "expected metadata change error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_non_zero_rejects_clustering_change(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata =
            catalog_managed_commit_metadata(table_root, 1).with_domain_change("delta.clustering");
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err =
            commit_with_style(&test_committer(), &engine, style, commit_metadata).unwrap_err();
        assert!(
            err.to_string().contains("clustering columns"),
            "expected clustering change error, got: {err}"
        );
    }

    #[rstest]
    #[case::imperative(CommitStyle::Imperative)]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(CommitStyle::Declarative)
    )]
    fn commit_version_non_zero_rejects_mismatched_table_id(#[case] style: CommitStyle) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_root = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let commit_metadata = catalog_managed_commit_metadata(table_root, 1);
        // Committer initialized with a different table ID than what's in the metadata.
        let committer = UCCommitter::new(
            Arc::new(MockUpdateTableClient::default()),
            "different-table-id",
            TableIdentifier::new("test_catalog", "test_schema", "test_table"),
        );
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let err = commit_with_style(&committer, &engine, style, commit_metadata).unwrap_err();
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
            Arc::new(MockUpdateTableClient::default()),
            "testUcTableId",
            TableIdentifier::new("test_catalog", "test_schema", "test_table"),
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
