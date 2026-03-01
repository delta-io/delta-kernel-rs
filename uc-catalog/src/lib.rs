//! UCCatalog implements a high-level interface for interacting with Delta Tables in Unity Catalog.

mod committer;
pub use committer::UCCommitter;

use std::sync::Arc;

use delta_kernel::{Engine, LogPath, Snapshot, Version};

use uc_client::prelude::*;

use itertools::Itertools;
use tracing::debug;
use url::Url;

/// The [UCCatalog] provides a high-level interface to interact with Delta Tables stored in Unity
/// Catalog. For now this is a lightweight wrapper around a [UCGetCommitsClient].
pub struct UCCatalog<'a, C: UCGetCommitsClient> {
    client: &'a C,
}

impl<'a, C: UCGetCommitsClient> UCCatalog<'a, C> {
    /// Create a new [UCCatalog] instance with the provided client.
    pub fn new(client: &'a C) -> Self {
        UCCatalog { client }
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

        // consume uc-client's Commit and hand back a delta_kernel LogPath
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::transaction::CommitResult;

    use object_store::ObjectStore;
    use tracing::info;

    use super::*;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    /// Configuration for connecting to a UC table
    struct UCTableConfig {
        table_id: String,
        table_uri: String,
        engine: DefaultEngine<TokioBackgroundExecutor>,
        catalog: UCCatalog<'static, UCCommitsRestClient>,
        _commits_client: Arc<UCCommitsRestClient>,
    }

    /// Load UC table configuration from environment variables and set up clients
    async fn load_uc_table_config() -> Result<UCTableConfig, BoxError> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        let config = uc_client::ClientConfig::build(&endpoint, &token).build()?;

        let uc_client = UCClient::new(config.clone())?;
        let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

        let (table_id, table_uri) = get_table(&uc_client, &table_name).await?;
        let creds = uc_client
            .get_credentials(&table_id, Operation::Read)
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

        let creds = creds
            .aws_temp_credentials
            .ok_or("No AWS temporary credentials found")?;

        let options = [
            ("region", "us-west-2"),
            ("access_key_id", &creds.access_key_id),
            ("secret_access_key", &creds.secret_access_key),
            ("session_token", &creds.session_token),
        ];

        let table_url = Url::parse(&table_uri)?;
        let (store, path) = object_store::parse_url_opts(&table_url, options)?;
        info!("created object store: {:?}\npath: {:?}\n", store, path);

        let store: Arc<dyn ObjectStore> = store.into();
        let engine = DefaultEngineBuilder::new(store).build();

        let commits_client_ref: &'static UCCommitsRestClient = Box::leak(Box::new(
            UCCommitsRestClient::new(uc_client::ClientConfig::build(&endpoint, &token).build()?)?,
        ));
        let catalog = UCCatalog::new(commits_client_ref);

        Ok(UCTableConfig {
            table_id,
            table_uri,
            engine,
            catalog,
            _commits_client: commits_client,
        })
    }

    async fn get_table(client: &UCClient, table_name: &str) -> Result<(String, String), BoxError> {
        let res = client.get_table(table_name).await?;
        let table_id = res.table_id;
        let table_uri = res.storage_location;
        info!(
            "[GET TABLE] got table_id: {}, table_uri: {}\n",
            table_id, table_uri
        );
        Ok((table_id, table_uri))
    }

    // ignored test which you can run manually to play around with reading a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo nextest run -p uc-catalog read_uc_table --nocapture --run-ignored all`
    #[ignore]
    #[tokio::test]
    async fn read_uc_table() -> Result<(), BoxError> {
        let config = load_uc_table_config().await?;
        let snapshot = config
            .catalog
            .load_snapshot(&config.table_id, &config.table_uri, &config.engine)
            .await?;
        println!("loaded snapshot: {snapshot:?}");
        Ok(())
    }

    // Run a scan to exercise the full scan setup path (partition detection, checkpoint info, etc).
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo nextest run -p uc-catalog read_uc_table_scan --nocapture --run-ignored all`
    #[ignore]
    #[tokio::test]
    async fn read_uc_table_scan() -> Result<(), BoxError> {
        let config = load_uc_table_config().await?;

        let snapshot = config
            .catalog
            .load_snapshot(&config.table_id, &config.table_uri, &config.engine)
            .await?;

        println!("=== Snapshot version: {} ===", snapshot.version());
        println!("=== Schema ===");
        for field in snapshot.schema().fields() {
            println!("  {}: {:?}", field.name(), field.data_type());
        }

        // Build scan with a partition predicate to activate partitionValues_parsed
        // 2025-01-01 = 20089 days since epoch
        use delta_kernel::expressions::{column_expr, Expression, Scalar};
        let predicate =
            Arc::new(column_expr!("part_date").eq(Expression::Literal(Scalar::Date(20089))));
        let scan = snapshot.scan_builder().with_predicate(predicate).build()?;

        println!("=== Running scan_metadata (first batch only) ===");
        if let Some(result) = scan.scan_metadata(&config.engine)?.next() {
            let scan_metadata = result?;
            let selected = scan_metadata
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&&s| s)
                .count();
            let total = scan_metadata.scan_files.selection_vector().len();
            println!("  first batch: {selected}/{total} files selected");
        } else {
            println!("  no batches returned");
        }
        println!("=== Done ===");

        Ok(())
    }

    // ignored test which you can run manually to play around with writing to a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t write_uc_table --nocapture -- --ignored`
    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn write_uc_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        // build shared config
        let config = uc_client::ClientConfig::build(&endpoint, &token).build()?;

        // build clients
        let client = UCClient::new(config.clone())?;
        let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

        let (table_id, table_uri) = get_table(&client, &table_name).await?;
        let creds = client
            .get_credentials(&table_id, Operation::ReadWrite)
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

        let catalog = UCCatalog::new(commits_client.as_ref());

        // TODO: support non-AWS
        let creds = creds
            .aws_temp_credentials
            .ok_or("No AWS temporary credentials found")?;

        let options = [
            ("region", "us-west-2"),
            ("access_key_id", &creds.access_key_id),
            ("secret_access_key", &creds.secret_access_key),
            ("session_token", &creds.session_token),
        ];

        let table_url = Url::parse(&table_uri)?;
        let (store, _path) = object_store::parse_url_opts(&table_url, options)?;
        let store: Arc<dyn object_store::ObjectStore> = store.into();

        let engine = DefaultEngineBuilder::new(store.clone()).build();
        let committer = Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()));
        let snapshot = catalog
            .load_snapshot(&table_id, &table_uri, &engine)
            .await?;
        println!("latest snapshot version: {:?}", snapshot.version());
        let txn = snapshot.clone().transaction(committer, &engine)?;
        let _write_context = txn.get_write_context();

        match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(t) => {
                println!("🎉 committed version {}", t.commit_version());
                // TODO: should use post-commit snapshot here (plumb through log tail)
                let _snapshot = catalog
                    .load_snapshot_at(&table_id, &table_uri, t.commit_version(), &engine)
                    .await?;
                // then do publish
            }
            CommitResult::ConflictedTransaction(t) => {
                println!("💥 commit conflicted at version {}", t.conflict_version());
            }
            CommitResult::RetryableTransaction(_) => {
                println!("we should retry...");
            }
        }
        Ok(())
    }
}
