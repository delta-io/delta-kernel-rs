use std::sync::Arc;

use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, PublishMetadata};
use delta_kernel::{DeltaResult, Engine, Error as DeltaError, FilteredEngineData};
use uc_client::models::commits::{Commit, CommitRequest};
use uc_client::UCCommitClient;

/// A [UCCommitter] is a Unity Catalog [`Committer`] implementation for committing to a specific
/// delta table in UC.
///
/// NOTE: this [`Committer`] requires a multi-threaded tokio runtime. That is, whatever
/// implementation consumes the Committer to commit to the table, must call `commit` from within a
/// muti-threaded tokio runtime context. Since the default engine uses tokio, this is compatible,
/// but must ensure that the multi-threaded runtime is used.
#[derive(Debug, Clone)]
pub struct UCCommitter<C: UCCommitClient> {
    commits_client: Arc<C>,
    table_id: String,
}

impl<C: UCCommitClient> UCCommitter<C> {
    /// Create a new [UCCommitter] to commit via the `commits_client` to the specific table with the given
    /// `table_id`.
    pub fn new(commits_client: Arc<C>, table_id: impl Into<String>) -> Self {
        UCCommitter {
            commits_client,
            table_id: table_id.into(),
        }
    }
}

impl<C: UCCommitClient + 'static> Committer for UCCommitter<C> {
    /// Commit the given `actions` to the delta table in UC. UC's committer elects to write out a
    /// staged commit for the actions then call the UC commit API to 'finalize' (ratify) the staged
    /// commit. Note that this will accumulate staged commits, and separately clients are expected
    /// to periodically publish the staged commits to the delta log. In it's current form, UC
    /// expects to be informed of the last known published version during this commit.
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let staged_commit_path = commit_metadata.staged_commit_path()?;
        engine
            .json_handler()
            .write_json_file(&staged_commit_path, Box::new(actions), false)?;

        let committed = engine.storage_handler().head(&staged_commit_path)?;
        tracing::debug!("wrote staged commit file: {:?}", committed);

        let commit_req = CommitRequest::new(
            self.table_id.clone(),
            commit_metadata.table_root().as_str(),
            Commit::new(
                commit_metadata.version().try_into().map_err(|_| {
                    DeltaError::generic("commit version does not fit into i64 for UC commit")
                })?,
                commit_metadata.in_commit_timestamp(),
                staged_commit_path
                    .path_segments()
                    .ok_or_else(|| DeltaError::generic("staged commit contained no path segments"))?
                    .next_back()
                    .ok_or_else(|| {
                        DeltaError::generic("staged commit segments next_back was empty")
                    })?,
                committed
                    .size
                    .try_into()
                    .map_err(|_| DeltaError::generic("committed size does not fit into i64"))?,
                committed.last_modified,
            ),
            commit_metadata
                .max_published_version()
                .map(|v| {
                    v.try_into().map_err(|_| {
                        DeltaError::Generic(format!(
                            "Max published version {v} does not fit into i64 for UC commit"
                        ))
                    })
                })
                .transpose()?,
        );
        let commit_version = commit_metadata.version();
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            DeltaError::generic("UCCommitter may only be used within a tokio runtime")
        })?;
        let result = tokio::task::block_in_place(|| {
            handle.block_on(async move { self.commits_client.commit(commit_req).await })
        });
        match result {
            Ok(()) => Ok(CommitResponse::Committed {
                file_meta: committed,
            }),
            Err(uc_client::error::Error::ApiError { status: 409, .. }) => {
                Ok(CommitResponse::Conflict {
                    version: commit_version,
                })
            }
            Err(e) => Err(DeltaError::Generic(format!("UC commit error: {e}"))),
        }
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
    use super::*;
    use delta_kernel::committer::CatalogCommit;
    use delta_kernel::engine::default::DefaultEngine;
    use delta_kernel::Version;
    use object_store::local::LocalFileSystem;
    use std::fs;
    use uc_client::error::Result;

    struct MockCommitsClient {
        commit_result: std::sync::Mutex<Option<Result<()>>>,
    }

    impl MockCommitsClient {
        fn always_unimplemented() -> Self {
            Self {
                commit_result: std::sync::Mutex::new(None),
            }
        }

        fn with_commit_result(result: Result<()>) -> Self {
            Self {
                commit_result: std::sync::Mutex::new(Some(result)),
            }
        }
    }

    impl UCCommitClient for MockCommitsClient {
        async fn commit(&self, _: CommitRequest) -> Result<()> {
            self.commit_result
                .lock()
                .unwrap()
                .take()
                .expect("MockCommitsClient::commit called but no result was configured")
        }
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_conflict_on_409() {
        use delta_kernel::schema::{DataType, StructField, StructType};
        use delta_kernel::transaction::create_table::create_table;
        use delta_kernel::transaction::CommitResult;

        let tmp_dir = tempfile::tempdir().unwrap();
        let table_path = tmp_dir.path().to_str().unwrap();

        let mock_client = MockCommitsClient::with_commit_result(Err(
            uc_client::error::Error::ApiError {
                status: 409,
                message: "Commit version already accepted. Current table version is 0".into(),
            },
        ));
        let committer = UCCommitter::new(Arc::new(mock_client), "test_table_id");
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap(),
        );
        let result = create_table(table_path, schema, "test_engine")
            .build(&engine, Box::new(committer))
            .unwrap()
            .commit(&engine)
            .unwrap();

        assert!(
            matches!(result, CommitResult::ConflictedTransaction(_)),
            "Expected ConflictedTransaction, got: {:?}",
            result
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_non_conflict_error_propagates() {
        use delta_kernel::schema::{DataType, StructField, StructType};
        use delta_kernel::transaction::create_table::create_table;

        let tmp_dir = tempfile::tempdir().unwrap();
        let table_path = tmp_dir.path().to_str().unwrap();

        let mock_client = MockCommitsClient::with_commit_result(Err(
            uc_client::error::Error::ApiError {
                status: 500,
                message: "Internal server error".into(),
            },
        ));
        let committer = UCCommitter::new(Arc::new(mock_client), "test_table_id");
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();

        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap(),
        );
        let result = create_table(table_path, schema, "test_engine")
            .build(&engine, Box::new(committer))
            .unwrap()
            .commit(&engine);

        assert!(result.is_err(), "Expected error for non-409 API error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("UC commit error"),
            "Error should contain 'UC commit error', got: {}",
            err_msg
        );
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
        let committer =
            UCCommitter::new(Arc::new(MockCommitsClient::always_unimplemented()), "test_table_id");
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
