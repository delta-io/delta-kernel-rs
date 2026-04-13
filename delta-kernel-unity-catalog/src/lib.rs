//! UCKernelClient implements a high-level interface for interacting with Delta Tables in Unity Catalog.

mod committer;
mod constants;
mod errors;
mod utils;
pub use committer::UCCommitter;
pub use utils::{get_final_required_properties_for_uc, get_required_properties_for_disk};

use std::sync::Arc;

use delta_kernel::{Engine, LogPath, Snapshot, Version};

use unity_catalog_delta_client_api::{CommitsRequest, GetCommitsClient};

use itertools::Itertools;
use tracing::debug;
use url::Url;

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
        let mut commits = self.get_commits_client.get_commits(req).await?;
        if let Some(commits) = commits.commits.as_mut() {
            commits.sort_by_key(|c| c.version)
        }

        // The catalog always returns the latest ratified version. Use it as the
        // max_catalog_version for snapshot building, and as the effective version when no
        // explicit time-travel version is requested.
        let max_catalog_version: Version = commits.latest_table_version.try_into()?;

        // consume the UC Commit and hand back a delta_kernel LogPath
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

        let mut builder = Snapshot::builder_for(table_url)
            .with_max_catalog_version(max_catalog_version)
            .with_log_tail(commits);

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

    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::object_store;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::transaction::CommitResult;

    use tracing::info;
    use unity_catalog_delta_client_api::{Commit, InMemoryCommitsClient, Operation, TableData};
    use unity_catalog_delta_rest_client::{UCClient, UCCommitsRestClient};

    use super::*;

    // We could just re-export UCClient's get_table to not require consumers to directly import
    // unity_catalog_delta_rest_client themselves.
    async fn get_table(
        client: &UCClient,
        table_name: &str,
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
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
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t read_uc_table --nocapture -- --ignored`
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

        let (table_id, table_uri) = get_table(&uc_client, &table_name).await?;
        let creds = uc_client
            .get_credentials(&table_id, Operation::Read)
            .await
            .map_err(|e| format!("Failed to get credentials: {e}"))?;

        let catalog = UCKernelClient::new(&uc_commits_client);

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
        let (store, path) = object_store::parse_url_opts(&table_url, options)?;

        info!("created object store: {:?}\npath: {:?}\n", store, path);

        let engine = DefaultEngineBuilder::new(store.into()).build();

        // read table
        let snapshot = catalog
            .load_snapshot(&table_id, &table_uri, &engine)
            .await?;
        // or time travel
        // let snapshot = catalog.load_snapshot_at(&table, 2).await?;

        println!("loaded snapshot: {snapshot:?}");

        Ok(())
    }

    // ignored test which you can run manually to play around with writing to a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t write_uc_table --nocapture -- --ignored`
    //
    // Table schema (created via Databricks SQL):
    //   p_date DATE, value STRING, p_bool BOOLEAN, p_string STRING, p_int INT
    //   PARTITIONED BY (p_string, p_int, p_date, p_bool)
    //
    // Note: schema order != partition order. This exercises col_indices resolution.
    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn write_uc_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use delta_kernel::arrow::array::{
            BooleanArray, Date32Array, Int32Array, RecordBatch, StringArray,
        };
        use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use delta_kernel::engine::arrow_data::ArrowEngineData;

        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        println!("endpoint: {endpoint}");
        println!("table_name: {table_name}");

        // build shared config
        let config =
            unity_catalog_delta_rest_client::ClientConfig::build(&endpoint, &token).build()?;
        println!("workspace_url: {}", config.workspace_url);

        // build clients
        let client = UCClient::new(config.clone())?;
        let commits_client = Arc::new(UCCommitsRestClient::new(config.clone())?);

        // Debug: raw HTTP call to see what we're actually getting back
        {
            let debug_url = format!("{}tables/{}", config.workspace_url, table_name);
            println!("debug GET: {debug_url}");
            let http = unity_catalog_delta_rest_client::http::build_http_client(&config)?;
            let resp = http.get(&debug_url).send().await
                .map_err(|e| format!("send failed: {e}"))?;
            println!("debug status: {}", resp.status());
            println!("debug url after redirects: {}", resp.url());
            let body = resp.text().await.map_err(|e| format!("body read failed: {e}"))?;
            println!(
                "debug body (first 500 chars): {}",
                &body[..body.len().min(500)]
            );
        }

        println!("calling get_table...");
        let table_res = client.get_table(&table_name).await?;
        let table_id = table_res.table_id.clone();
        let table_uri = table_res.storage_location.clone();
        println!("table_id: {table_id}, table_uri: {table_uri}");
        println!("properties: {:?}", table_res.properties);

        let is_catalog_managed = table_res
            .properties
            .get("delta.feature.catalogManaged")
            .map_or(false, |v| v == "supported");
        println!("is_catalog_managed: {is_catalog_managed}");

        // Get cloud storage credentials
        println!("calling get_credentials for table_id={table_id}...");
        let creds = client
            .get_credentials(&table_id, Operation::ReadWrite)
            .await
            .map_err(|e| format!("Failed to get credentials: {e}"))?;
        println!("got credentials, aws={}", creds.aws_temp_credentials.is_some());

        let creds = creds
            .aws_temp_credentials
            .ok_or("No AWS temporary credentials found")?;
        println!(
            "aws creds: access_key_id={}..., session_token_len={}",
            &creds.access_key_id[..12.min(creds.access_key_id.len())],
            creds.session_token.len()
        );

        let options = [
            ("region", "us-west-2"),
            ("access_key_id", &creds.access_key_id),
            ("secret_access_key", &creds.secret_access_key),
            ("session_token", &creds.session_token),
        ];

        let table_url = Url::parse(&table_uri)?;
        println!("table_url: {table_url}");
        let (store, _path) = object_store::parse_url_opts(&table_url, options)?;
        let store = Arc::new(store);

        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Load snapshot and build committer based on catalog-managed status.
        let snapshot = if is_catalog_managed {
            println!("catalog-managed table: using UC commits client for snapshot + commit");
            let catalog = UCKernelClient::new(commits_client.as_ref());
            catalog
                .load_snapshot(&table_id, &table_uri, &engine)
                .await?
        } else {
            println!("non-catalog-managed table: using filesystem snapshot");
            Snapshot::builder_for(table_url.clone()).build(&engine)?
        };
        println!("loaded snapshot version: {:?}", snapshot.version());
        println!("schema: {:?}", snapshot.schema());
        println!(
            "partition cols: {:?}",
            snapshot.table_configuration().partition_columns()
        );

        // Build a batch matching the table schema: p_date, value, p_bool, p_string, p_int
        // 12 rows with non-adjacent partitions to exercise grouping/merging.
        //
        // Rows are intentionally NOT sorted by partition values:
        //   rows 0,3,6  -> ("US",    2024, 2025-03-31, true)   partition A
        //   rows 1,4    -> ("EU",    2025, 2025-01-15, false)  partition B
        //   rows 2,5    -> ("AP",    2024, 2025-06-01, true)   partition C
        //   rows 7,8    -> (NULL,    NULL, NULL,       NULL)   partition D (all nulls)
        //   row  9      -> ("US/East", 2024, 2025-01-01, true) partition E (special chars)
        //   row  10     -> ("100%",  2024, 2025-01-01, false)  partition F (special chars)
        //   row  11     -> ("a=b",   2024, 2025-01-01, true)   partition G (special chars)
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("p_date", ArrowDataType::Date32, true),
            Field::new("value", ArrowDataType::Utf8, true),
            Field::new("p_bool", ArrowDataType::Boolean, true),
            Field::new("p_string", ArrowDataType::Utf8, true),
            Field::new("p_int", ArrowDataType::Int32, true),
        ]));

        // Days since epoch for test dates
        let d_2025_03_31 = 20178; // 2025-03-31
        let d_2025_01_15 = 20103; // 2025-01-15
        let d_2025_06_01 = 20240; // 2025-06-01
        let d_2025_01_01 = 20089; // 2025-01-01

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                // p_date (schema col 0, partition col 3 in partition order)
                Arc::new(Date32Array::from(vec![
                    Some(d_2025_03_31), // row0: partition A
                    Some(d_2025_01_15), // row1: partition B
                    Some(d_2025_06_01), // row2: partition C
                    Some(d_2025_03_31), // row3: partition A (non-adjacent)
                    Some(d_2025_01_15), // row4: partition B (non-adjacent)
                    Some(d_2025_06_01), // row5: partition C (non-adjacent)
                    Some(d_2025_03_31), // row6: partition A (third occurrence)
                    None,               // row7: partition D (all null)
                    None,               // row8: partition D (all null)
                    Some(d_2025_01_01), // row9: partition E (special: US/East)
                    Some(d_2025_01_01), // row10: partition F (special: 100%)
                    Some(d_2025_01_01), // row11: partition G (special: a=b)
                ])),
                // value (schema col 1, NOT a partition column)
                Arc::new(StringArray::from(vec![
                    Some("row0"),
                    Some("row1"),
                    Some("row2"),
                    Some("row3"),
                    Some("row4"),
                    Some("row5"),
                    Some("row6"),
                    Some("row7"),
                    Some("row8"),
                    Some("row9"),
                    Some("row10"),
                    Some("row11"),
                ])),
                // p_bool (schema col 2, partition col 4 in partition order)
                Arc::new(BooleanArray::from(vec![
                    Some(true),  // row0
                    Some(false), // row1
                    Some(true),  // row2
                    Some(true),  // row3
                    Some(false), // row4
                    Some(true),  // row5
                    Some(true),  // row6
                    None,        // row7
                    None,        // row8
                    Some(true),  // row9
                    Some(false), // row10
                    Some(true),  // row11
                ])),
                // p_string (schema col 3, partition col 1 in partition order)
                Arc::new(StringArray::from(vec![
                    Some("US"),      // row0
                    Some("EU"),      // row1
                    Some("AP"),      // row2
                    Some("US"),      // row3
                    Some("EU"),      // row4
                    Some("AP"),      // row5
                    Some("US"),      // row6
                    None,            // row7
                    None,            // row8
                    Some("US/East"), // row9
                    Some("100%"),    // row10
                    Some("a=b"),     // row11
                ])),
                // p_int (schema col 4, partition col 2 in partition order)
                Arc::new(Int32Array::from(vec![
                    Some(2024), // row0
                    Some(2025), // row1
                    Some(2024), // row2
                    Some(2024), // row3
                    Some(2025), // row4
                    Some(2024), // row5
                    Some(2024), // row6
                    None,       // row7
                    None,       // row8
                    Some(2024), // row9
                    Some(2024), // row10
                    Some(2024), // row11
                ])),
            ],
        )?;

        println!("built batch: {} rows, {} cols", batch.num_rows(), batch.num_columns());

        // Write using write_partitioned_parquet
        let committer: Box<dyn delta_kernel::committer::Committer> = if is_catalog_managed {
            Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()))
        } else {
            Box::new(delta_kernel::committer::FileSystemCommitter::new())
        };
        println!("committer: catalog_managed={is_catalog_managed}");

        let mut txn = snapshot
            .clone()
            .transaction(committer, &engine)?
            .with_engine_info("kernel-partition-test")
            .with_data_change(true);

        let metadata = engine
            .write_partitioned_parquet(&ArrowEngineData::new(batch), &txn)
            .await?;
        txn.add_files(metadata);

        match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(t) => {
                println!("committed version {}", t.commit_version());
            }
            CommitResult::ConflictedTransaction(t) => {
                println!("commit conflicted at version {}", t.conflict_version());
            }
            CommitResult::RetryableTransaction(_) => {
                println!("retryable, we should retry...");
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
}
