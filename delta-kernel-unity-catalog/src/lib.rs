//! Delta Kernel Unity Catalog integration.
//!
//! This crate provides Unity Catalog support for delta-kernel-rs, including the
//! [`UCCommitter`] for committing transactions and the [`UCKernelClient`] for loading snapshots
//! via the UC commits API.

pub mod commits_client;
mod committer;
pub mod config;
pub mod error;
pub mod http;
pub mod models;

pub use commits_client::{UCCommitClient, UCCommitsRestClient, UCGetCommitsClient};
pub use committer::UCCommitter;
pub use config::{ClientConfig, ClientConfigBuilder};
pub use models::{Commit, CommitRequest, CommitsRequest, CommitsResponse};

#[cfg(any(test, feature = "test-utils"))]
pub use commits_client::{InMemoryCommitsClient, TableData};

use std::sync::Arc;

use delta_kernel::{Engine, LogPath, Snapshot, Version};

use itertools::Itertools;
use tracing::debug;
use url::Url;

/// [UCKernelClient] is the bridge between UC and Kernel. It calls UC to get commits for
/// catalog-managed tables, translates the response into kernel [`LogPath`]s, and injects them
/// into Kernel's [`Snapshot`] builder to construct a snapshot.
pub struct UCKernelClient<'a, C: UCGetCommitsClient> {
    client: &'a C,
}

impl<'a, C: UCGetCommitsClient> UCKernelClient<'a, C> {
    /// Create a new [UCKernelClient] instance with the provided client.
    pub fn new(client: &'a C) -> Self {
        UCKernelClient { client }
    }

    /// Load the latest snapshot of a Delta Table identified by `table_id` and `table_uri` in Unity
    /// Catalog. Generally, a separate `get_table` call can be used to resolve the table id/uri from
    /// the table name.
    pub async fn load_snapshot(
        &self,
        table_id: &str,
        table_uri: &str,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error + Send + Sync>> {
        self.load_snapshot_inner(table_id, table_uri, None, engine)
            .await
    }

    /// Load a snapshot of a Delta Table identified by `table_id` and `table_uri` for a specific
    /// version. Generally, a separate `get_table` call can be used to resolve the table id/uri from
    /// the table name.
    pub async fn load_snapshot_at(
        &self,
        table_id: &str,
        table_uri: &str,
        version: Version,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error + Send + Sync>> {
        self.load_snapshot_inner(table_id, table_uri, Some(version), engine)
            .await
    }

    pub(crate) async fn load_snapshot_inner(
        &self,
        table_id: &str,
        table_uri: &str,
        version: Option<Version>,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error + Send + Sync>> {
        let table_uri = table_uri.to_string();
        let req = CommitsRequest {
            table_id: table_id.to_string(),
            table_uri: table_uri.clone(),
            start_version: Some(0),
            end_version: version.and_then(|v| v.try_into().ok()),
        };
        let mut commits = self.client.get_commits(req).await?;
        if let Some(commits) = commits.commits.as_mut() {
            commits.sort_by_key(|c| c.version)
        }

        // if commits are present, we ensure they are sorted+contiguous
        if let Some(commits) = &commits.commits {
            if !commits.windows(2).all(|w| w[1].version == w[0].version + 1) {
                return Err("Received non-contiguous commit versions".into());
            }
        }

        // we always get back the latest version from commits response, and pass that in to
        // kernel's Snapshot builder. basically, load_table for the latest version always looks
        // like a time travel query since we know the latest version ahead of time.
        //
        // note there is a weird edge case: if the table was just created it will return
        // latest_table_version = -1, but the 0.json will exist in the _delta_log.
        let version: Version = match version {
            Some(v) => v,
            None => match commits.latest_table_version {
                -1 => 0,
                i => i.try_into()?,
            },
        };

        // consume UC Commit and hand back a delta_kernel LogPath
        let mut table_url = Url::parse(&table_uri)?;
        // add trailing slash
        if !table_url.path().ends_with('/') {
            // NB: we push an empty segment which effectively adds a trailing slash
            table_url
                .path_segments_mut()
                .map_err(|_| "Cannot modify URL path segments")?
                .push("");
        }
        let commits: Vec<_> = commits
            .commits
            .unwrap_or_default()
            .into_iter()
            .map(
                |c| -> Result<LogPath, Box<dyn std::error::Error + Send + Sync>> {
                    LogPath::staged_commit(
                        table_url.clone(),
                        &c.file_name,
                        c.file_modification_timestamp,
                        c.file_size.try_into()?,
                    )
                    .map_err(|e| e.into())
                },
            )
            .try_collect()?;

        debug!("commits for kernel: {:?}\n", commits);

        Snapshot::builder_for(table_url)
            .at_version(version)
            .with_log_tail(commits)
            .build(engine)
            .map_err(|e| e.into())
    }
}
