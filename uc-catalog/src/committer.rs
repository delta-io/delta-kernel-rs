use std::sync::Arc;

use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer};
use delta_kernel::{DeltaResult, Engine, Error as DeltaError, FilteredEngineData};
use uc_client::models::commits::{Commit, CommitRequest};
use uc_client::prelude::UCClient;

use url::Url;

// A [UCCommitter] is a Unity Catalog [Committer] implementation for committing to a specific
/// delta table in UC.
#[derive(Debug, Clone)]
pub struct UCCommitter {
    client: Arc<UCClient>,
    table_id: String,
    table_uri: String,
}

impl UCCommitter {
    pub fn new(
        client: Arc<UCClient>,
        table_id: impl Into<String>,
        table_uri: impl Into<String>,
    ) -> Self {
        UCCommitter {
            client,
            table_id: table_id.into(),
            table_uri: table_uri.into(),
        }
    }
}

impl Committer for UCCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let uuid = uuid::Uuid::new_v4();
        let filename = format!(
            "{version:020}.{uuid}.json",
            version = commit_metadata.version()
        );
        // FIXME use table path from commit_metadata?
        let mut commit_path = Url::parse(&self.table_uri)?;
        commit_path.path_segments_mut().unwrap().extend(&[
            "_delta_log",
            "_staged_commits",
            &filename,
        ]);

        engine
            .json_handler()
            .write_json_file(&commit_path, Box::new(actions), false)?;

        let mut other = Url::parse(&self.table_uri)?;
        other.path_segments_mut().unwrap().extend(&[
            "_delta_log",
            "_staged_commits",
            &format!("{:020}", commit_metadata.version()),
        ]);
        let committed = engine
            .storage_handler()
            .list_from(&other)?
            .next()
            .unwrap()
            .unwrap();
        println!("wrote commit file: {:?}", committed);

        // FIXME: ?
        let timestamp: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .map_err(|e| DeltaError::Generic(format!("Unable to convert timestamp to i64: {e}")))?;

        // let last_backfilled_version =
        //     commit_metadata.latest_published_version.map(|v| v.try_into().unwrap()); // FIXME
        let last_backfilled_version = None;
        let commit_req = CommitRequest::new(
            self.table_id.clone(),
            self.table_uri.clone(),
            Commit::new(
                commit_metadata.version().try_into().unwrap(),
                timestamp,
                filename,
                committed.size as i64,
                committed.last_modified,
            ),
            last_backfilled_version,
        );
        // FIXME
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| {
            handle.block_on(async move {
                self.client
                    .commit(commit_req)
                    .await
                    .map_err(|e| DeltaError::Generic(format!("UC commit error: {e}")))
            })
        })?;
        Ok(CommitResponse::Committed {
            version: commit_metadata.version(),
        })
    }
}
