use std::sync::Arc;

use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer};
use delta_kernel::{DeltaResult, Engine, Error as DeltaError, FilteredEngineData};
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
        Ok(CommitResponse::Committed {
            file_meta: committed,
        })
    }
}
