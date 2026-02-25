use std::sync::Arc;

use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, PublishMetadata};
use delta_kernel::{DeltaResult, Engine, Error as DeltaError, FilteredEngineData};
use tracing::{info, instrument};
use uc_client::models::commits::{Commit, CommitRequest};
use uc_client::UCCommitsClient;

/// A [UCCommitter] is a Unity Catalog [`Committer`] implementation for committing to a specific
/// delta table in UC.
///
/// NOTE: this [`Committer`] requires a multi-threaded tokio runtime. That is, whatever
/// implementation consumes the Committer to commit to the table, must call `commit` from within a
/// muti-threaded tokio runtime context. Since the default engine uses tokio, this is compatible,
/// but must ensure that the multi-threaded runtime is used.
#[derive(Debug, Clone)]
pub struct UCCommitter<C: UCCommitsClient> {
    commits_client: Arc<C>,
    table_id: String,
}

impl<C: UCCommitsClient> UCCommitter<C> {
    /// Create a new [UCCommitter] to commit via the `commits_client` to the specific table with the given
    /// `table_id`.
    pub fn new(commits_client: Arc<C>, table_id: impl Into<String>) -> Self {
        UCCommitter {
            commits_client,
            table_id: table_id.into(),
        }
    }
}

impl<C: UCCommitsClient + 'static> Committer for UCCommitter<C> {
    /// Commit the given `actions` to the delta table in UC. UC's committer elects to write out a
    /// staged commit for the actions then call the UC commit API to 'finalize' (ratify) the staged
    /// commit. Note that this will accumulate staged commits, and separately clients are expected
    /// to periodically publish the staged commits to the delta log. In it's current form, UC
    /// expects to be informed of the last known published version during this commit.
    #[instrument(
        name = "uc_committer.commit",
        skip_all,
        fields(version = commit_metadata.version(), table_id = %self.table_id),
        err
    )]
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let version = commit_metadata.version();
        let staged_commit_path = commit_metadata.staged_commit_path()?;
        engine
            .json_handler()
            .write_json_file(&staged_commit_path, Box::new(actions), false)?;

        let committed = engine.storage_handler().head(&staged_commit_path)?;
        info!(
            staged_file = ?committed,
            "Wrote staged commit file"
        );
        let max_published_version =
            max_published_version_for_uc(commit_metadata.max_published_version())?;

        let commit_req = CommitRequest::new(
            self.table_id.clone(),
            commit_metadata.table_root().as_str(),
            Commit::new(
                version.try_into().map_err(|_| {
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
            max_published_version,
        );
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            DeltaError::generic("UCCommitter may only be used within a tokio runtime")
        })?;
        tokio::task::block_in_place(|| {
            handle.block_on(async move {
                self.commits_client
                    .commit(commit_req)
                    .await
                    .map_err(|e| DeltaError::Generic(format!("UC commit error: {e}")))
            })
        })?;
        info!(committed_version = version, "UC commit API call succeeded");
        Ok(CommitResponse::Committed {
            file_meta: committed,
        })
    }

    fn is_catalog_committer(&self) -> bool {
        true
    }

    #[instrument(
        name = "uc_committer.publish",
        skip_all,
        fields(
            num_commits = publish_metadata.commits_to_publish().len(),
            publish_to_version = publish_metadata.publish_version()
        ),
        err
    )]
    fn publish(&self, engine: &dyn Engine, publish_metadata: PublishMetadata) -> DeltaResult<()> {
        if publish_metadata.commits_to_publish().is_empty() {
            return Ok(());
        }

        for catalog_commit in publish_metadata.commits_to_publish() {
            let src = catalog_commit.location();
            let dest = catalog_commit.published_location();
            info!(
                version = catalog_commit.version(),
                source = %src,
                destination = %dest,
                "Publishing catalog commit via atomic copy"
            );
            match engine.storage_handler().copy_atomic(src, dest) {
                Ok(_) => (),
                Err(DeltaError::FileAlreadyExists(_)) => {
                    info!(
                        version = catalog_commit.version(),
                        destination = %dest,
                        "Skipping publish; destination commit already exists"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

fn max_published_version_for_uc(max_published_version: Option<u64>) -> DeltaResult<Option<i64>> {
    max_published_version
        .map(|v| {
            v.try_into().map_err(|_| {
                DeltaError::Generic(format!(
                    "Max published version {v} does not fit into i64 for UC commit"
                ))
            })
        })
        .transpose()
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
    use uc_client::models::commits::{CommitsRequest, CommitsResponse};

    struct MockCommitsClient;

    impl UCCommitsClient for MockCommitsClient {
        async fn get_commits(&self, _: CommitsRequest) -> Result<CommitsResponse> {
            unimplemented!()
        }
        async fn commit(&self, _: CommitRequest) -> Result<()> {
            unimplemented!()
        }
    }

    fn staged_commit_url(table_root: &url::Url, version: Version) -> url::Url {
        table_root
            .join(&format!(
                "_delta_log/_staged_commits/{:020}.uuid.json",
                version
            ))
            .unwrap()
    }

    fn published_commit_url(table_root: &url::Url, version: Version) -> url::Url {
        table_root
            .join(&format!("_delta_log/{:020}.json", version))
            .unwrap()
    }

    #[test]
    fn test_max_published_version_for_uc_none() {
        assert_eq!(max_published_version_for_uc(None).unwrap(), None);
    }

    #[test]
    fn test_max_published_version_for_uc_with_value() {
        assert_eq!(max_published_version_for_uc(Some(42)).unwrap(), Some(42));
    }

    #[test]
    fn test_max_published_version_for_uc_overflow() {
        let err = max_published_version_for_uc(Some(u64::MAX)).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not fit into i64 for UC commit"),
            "Unexpected error: {err}"
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
        let committer = UCCommitter::new(Arc::new(MockCommitsClient), "testUcTableId");
        let engine = DefaultEngine::builder(Arc::new(LocalFileSystem::new())).build();
        committer.publish(&engine, publish_metadata).unwrap();

        // ===== THEN =====
        for v in versions {
            let path = published_commit_url(&table_root, v).to_file_path().unwrap();
            assert!(path.exists());
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                format!("version: {}", v)
            );
        }
    }
}
