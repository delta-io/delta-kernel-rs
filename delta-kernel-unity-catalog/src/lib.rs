//! UCKernelClient implements a high-level interface for interacting with Delta Tables in Unity
//! Catalog.

mod committer;
mod constants;
mod errors;
mod utils;
use std::sync::Arc;

pub use committer::UCCommitter;
use delta_kernel::{DeltaResult, Engine, Error, LogPath, Snapshot, Version};
use tracing::debug;
use unity_catalog_delta_client_api::{Commit, CommitsRequest, GetCommitsClient};
use url::Url;
pub use utils::{get_final_required_properties_for_uc, get_required_properties_for_disk};

/// Convert catalog-provided [`Commit`]s into kernel [`LogPath`]s for
/// `Snapshot::builder_for(..).with_log_tail(..)`.
///
/// Sorts commits by version because they may arrive out of order. Normalizes `table_root`
/// with a trailing `/` for staged-commit path resolution.
///
/// # Errors
///
/// Returns an error if a commit's `file_size` is negative (does not fit in `FileSize`) or if
/// [`LogPath::staged_commit`] rejects the resolved path.
pub(crate) fn log_tail_from_commits(
    commits: &[Commit],
    mut table_root: Url,
) -> DeltaResult<Vec<LogPath>> {
    if !table_root.path().ends_with('/') {
        table_root.set_path(&format!("{}/", table_root.path()));
    }
    let mut sorted: Vec<&Commit> = commits.iter().collect();
    sorted.sort_by_key(|c| c.version);
    sorted
        .into_iter()
        .map(|c| {
            let file_size = c.file_size.try_into().map_err(|_| {
                Error::generic(format!(
                    "commit file_size {} does not fit in FileSize",
                    c.file_size
                ))
            })?;
            LogPath::staged_commit(
                table_root.clone(),
                &c.file_name,
                c.file_modification_timestamp,
                file_size,
            )
        })
        .collect()
}

/// The [UCKernelClient] provides a high-level interface to interact with Delta Tables stored in
/// Unity Catalog. It is a lightweight wrapper around a [GetCommitsClient].
pub struct UCKernelClient<'a, C: GetCommitsClient> {
    get_commits_client: &'a C,
}

impl<'a, C: GetCommitsClient> UCKernelClient<'a, C> {
    /// Create a new [UCKernelClient] instance with the provided client.
    pub fn new(get_commits_client: &'a C) -> Self {
        UCKernelClient { get_commits_client }
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
        let commits = self.get_commits_client.get_commits(req).await?;

        // The catalog always returns the latest ratified version. Use it as the
        // max_catalog_version for snapshot building, and as the effective version when no
        // explicit time-travel version is requested.
        let max_catalog_version: Version = commits.latest_table_version.try_into()?;

        let table_url = Url::parse(&table_uri)?;
        let log_tail = log_tail_from_commits(&commits.commits.unwrap_or_default(), table_url)?;

        debug!("commits for kernel: {:?}\n", log_tail);

        let mut builder = Snapshot::builder_for(&table_uri)
            .with_max_catalog_version(max_catalog_version)
            .with_log_tail(log_tail);

        if let Some(v) = version {
            builder = builder.at_version(v);
        }

        builder.build(engine).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use delta_kernel::committer::{Committer, FileSystemCommitter};
    use delta_kernel::object_store;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::table_features::{
        SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
    };
    use delta_kernel::transaction::CommitResult;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use tracing::info;
    use unity_catalog_delta_client_api::{Commit, InMemoryCommitsClient, Operation, TableData};
    use unity_catalog_delta_rest_client::models::TablesResponse;
    use unity_catalog_delta_rest_client::{UCClient, UCCommitsRestClient};

    use super::*;

    /// Returns `true` when the UC `get_table` response indicates the table uses the
    /// `catalogManaged` protocol feature.
    ///
    /// We key off the UC `properties` map because the snapshot has not been loaded yet and we
    /// need the answer to decide *how* to load it (catalog round-trip vs. direct object-store
    /// read). Once a snapshot is in hand, protocol inspection is the authoritative source.
    fn is_catalog_managed(table: &TablesResponse) -> bool {
        let key = format!("{SET_TABLE_FEATURE_SUPPORTED_PREFIX}catalogManaged");
        table.properties.get(&key).map(String::as_str) == Some(SET_TABLE_FEATURE_SUPPORTED_VALUE)
    }

    /// Load a snapshot of the given UC Delta table, picking the correct strategy based on whether
    /// the table uses the `catalogManaged` protocol feature. Catalog-managed tables are loaded via
    /// [`UCKernelClient`] (which queries UC for the authoritative log tail); other UC Delta tables
    /// are loaded directly from object storage via [`Snapshot::builder_for`].
    ///
    /// When `version` is `None`, loads the latest snapshot; otherwise loads the snapshot at the
    /// given version.
    async fn load_uc_table_snapshot(
        table: &TablesResponse,
        commits_client: &UCCommitsRestClient,
        version: Option<Version>,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error + Send + Sync>> {
        if is_catalog_managed(table) {
            UCKernelClient::new(commits_client)
                .load_snapshot_inner(&table.table_id, &table.storage_location, version, engine)
                .await
        } else {
            let mut builder = Snapshot::builder_for(&table.storage_location);
            if let Some(v) = version {
                builder = builder.at_version(v);
            }
            Ok(builder.build(engine)?)
        }
    }

    // We could just re-export UCClient's get_table to not require consumers to directly import
    // unity_catalog_delta_rest_client themselves.
    async fn get_table(
        client: &UCClient,
        table_name: &str,
    ) -> Result<TablesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let res = client.get_table(table_name).await?;
        info!(
            "[GET TABLE] got table_id: {}, table_uri: {}, catalog_managed: {}\n",
            res.table_id,
            res.storage_location,
            is_catalog_managed(&res),
        );
        Ok(res)
    }

    // ignored test which you can run manually to play around with reading a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t read_uc_table --nocapture -- --ignored`
    //
    // Supports both catalog-managed Delta tables (via UCKernelClient, which queries UC for the
    // log tail) and non-catalog-managed UC tables (via Snapshot::builder_for).
    #[ignore]
    #[tokio::test]
    async fn read_uc_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        // build shared config
        let config =
            unity_catalog_delta_rest_client::ClientConfig::build(&endpoint, &token).build()?;

        // build clients
        let uc_client = UCClient::new(config.clone())?;
        let uc_commits_client = UCCommitsRestClient::new(config)?;

        let table = get_table(&uc_client, &table_name).await?;
        let creds = uc_client
            .get_credentials(&table.table_id, Operation::Read)
            .await
            .map_err(|e| format!("Failed to get credentials: {e}"))?;

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

        let table_url = Url::parse(&table.storage_location)?;
        let (store, path) = object_store::parse_url_opts(&table_url, options)?;

        info!("created object store: {:?}\npath: {:?}\n", store, path);

        let engine = DefaultEngineBuilder::new(store.into()).build();

        let snapshot = load_uc_table_snapshot(&table, &uc_commits_client, None, &engine).await?;
        // or time travel, e.g.
        // load_uc_table_snapshot(&table, &uc_commits_client, Some(2), &engine).await?;

        println!("loaded snapshot: {snapshot:?}");

        Ok(())
    }

    // ignored test which you can run manually to play around with writing to a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t write_uc_table --nocapture -- --ignored`
    //
    // Supports both catalog-managed Delta tables (via UCKernelClient + UCCommitter) and
    // non-catalog-managed UC tables (via Snapshot::builder_for + FileSystemCommitter).
    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn write_uc_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        // build shared config
        let config =
            unity_catalog_delta_rest_client::ClientConfig::build(&endpoint, &token).build()?;

        // build clients
        let client = UCClient::new(config.clone())?;
        let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

        let table = get_table(&client, &table_name).await?;
        let creds = client
            .get_credentials(&table.table_id, Operation::ReadWrite)
            .await
            .map_err(|e| format!("Failed to get credentials: {e}"))?;

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

        let table_url = Url::parse(&table.storage_location)?;
        let (store, _path) = object_store::parse_url_opts(&table_url, options)?;
        let store = Arc::new(store);

        let engine = DefaultEngineBuilder::new(store.clone()).build();

        let snapshot = load_uc_table_snapshot(&table, &commits_client, None, &engine).await?;
        println!("latest snapshot version: {:?}", snapshot.version());

        // UC catalogManaged tables must commit through UC; UC non-catalogManaged commit through
        // the filesystem.
        let committer: Box<dyn Committer> = if is_catalog_managed(&table) {
            Box::new(UCCommitter::new(
                commits_client.clone(),
                table.table_id.clone(),
            ))
        } else {
            Box::new(FileSystemCommitter::new())
        };
        let txn = snapshot.clone().transaction(committer, &engine)?;
        let _write_context = txn.unpartitioned_write_context()?;

        match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(t) => {
                println!("committed version {}", t.commit_version());
                let _snapshot = t
                    .post_commit_snapshot()
                    .ok_or("no post commit snapshot")?
                    .clone();
                // then do publish (catalog-managed only)
            }
            CommitResult::ConflictedTransaction(t) => {
                println!("commit conflicted at version {}", t.conflict_version());
            }
            CommitResult::RetryableTransaction(_) => {
                println!("we should retry...");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn load_snapshot_at_errors_when_version_exceeds_catalog() {
        let client = InMemoryCommitsClient::new();
        client.insert_table(
            "test_table",
            TableData {
                max_ratified_version: 3,
                catalog_commits: vec![],
            },
        );
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let catalog = UCKernelClient::new(&client);

        // Request version 5 but catalog only reports version 3
        let result = catalog
            .load_snapshot_at("test_table", "memory:///", 5, &engine)
            .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Time-travel version 5 exceeds max_catalog_version 3"));
    }

    #[tokio::test]
    async fn load_snapshot_errors_on_non_contiguous_commits() {
        let client = InMemoryCommitsClient::new();
        client.insert_table(
            "test_table",
            TableData {
                max_ratified_version: 3,
                catalog_commits: vec![
                    Commit::new(
                        1,
                        0,
                        "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
                        100,
                        0,
                    ),
                    Commit::new(
                        3,
                        0,
                        "00000000000000000003.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
                        100,
                        0,
                    ), // gap: version 2 missing
                ],
            },
        );
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let catalog = UCKernelClient::new(&client);

        let result = catalog
            .load_snapshot("test_table", "memory:///", &engine)
            .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("log_tail must be sorted and contiguous"));
    }

    #[rstest]
    #[case::missing_trailing_slash("memory:///my_table")]
    #[case::has_trailing_slash("memory:///my_table/")]
    fn log_tail_from_commits_sorts_and_normalizes(#[case] table_root: &str) {
        let names = [
            "00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            "00000000000000000002.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        ];
        // Insert out of order: 2, 0, 1.
        let commits = vec![
            Commit::new(2, 0, names[2], 100, 0),
            Commit::new(0, 0, names[0], 100, 0),
            Commit::new(1, 0, names[1], 100, 0),
        ];
        let normalized = Url::parse("memory:///my_table/").unwrap();

        let log_tail = log_tail_from_commits(&commits, Url::parse(table_root).unwrap()).unwrap();

        let expected: Vec<LogPath> = names
            .iter()
            .map(|name| LogPath::staged_commit(normalized.clone(), name, 0, 100).unwrap())
            .collect();
        assert_eq!(log_tail, expected);
    }

    #[test]
    fn log_tail_from_commits_empty_commits_is_empty() {
        let table_root = Url::parse("memory:///my_table/").unwrap();
        assert!(log_tail_from_commits(&[], table_root).unwrap().is_empty());
    }

    #[test]
    fn log_tail_from_commits_rejects_negative_file_size() {
        let commits = vec![Commit::new(
            0,
            0,
            "00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            -1,
            0,
        )];
        let table_root = Url::parse("memory:///my_table/").unwrap();

        let err = log_tail_from_commits(&commits, table_root).unwrap_err();

        assert!(err.to_string().contains("does not fit in FileSize"));
    }
}
