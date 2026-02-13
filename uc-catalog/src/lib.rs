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
/// Catalog. For now this is a lightweight wrapper around a [UCCommitsClient].
pub struct UCCatalog<'a, C: UCCommitsClient> {
    client: &'a C,
}

impl<'a, C: UCCommitsClient> UCCatalog<'a, C> {
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

    use delta_kernel::arrow::array::RecordBatch;
    use delta_kernel::arrow::util::pretty::print_batches;
    use delta_kernel::engine::arrow_data::EngineDataArrowExt;
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::transaction::CommitResult;

    use itertools::Itertools;
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
        // Keep the commits client alive since catalog borrows from it
        _commits_client: Arc<UCCommitsRestClient>,
    }

    /// Load UC table configuration from environment variables and set up clients
    async fn load_uc_table_config() -> Result<UCTableConfig, BoxError> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        // build shared config
        let config = uc_client::ClientConfig::build(&endpoint, &token).build()?;

        // build clients
        let uc_client = UCClient::new(config.clone())?;
        let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

        let (table_id, table_uri) = get_table(&uc_client, &table_name).await?;
        let creds = uc_client
            .get_credentials(&table_id, Operation::Read)
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

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

        let store: Arc<dyn ObjectStore> = store.into();
        let engine = DefaultEngineBuilder::new(store).build();

        // Use a leaked reference for the catalog to satisfy lifetime requirements in tests
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

    /// Execute a scan and collect results as RecordBatches
    fn execute_scan_to_batches(
        scan: &delta_kernel::scan::Scan,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    ) -> Result<Vec<RecordBatch>, BoxError> {
        let batches: Vec<RecordBatch> = scan
            .execute(engine)?
            .map(EngineDataArrowExt::try_into_record_batch)
            .try_collect()?;
        Ok(batches)
    }

    // We could just re-export UCClient's get_table to not require consumers to directly import
    // uc_client themselves.
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
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t read_uc_table --nocapture -- --ignored`
    #[ignore]
    #[tokio::test]
    async fn read_uc_table() -> Result<(), BoxError> {
        let config = load_uc_table_config().await?;

        // read table
        let snapshot = config
            .catalog
            .load_snapshot(&config.table_id, &config.table_uri, &config.engine)
            .await?;
        // or time travel
        // let snapshot = catalog.load_snapshot_at(&table, 2).await?;

        println!("🎉 loaded snapshot: {snapshot:?}");

        Ok(())
    }

    // ignored test which you can run manually to read all data from a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo nextest run read_uc_table_data --nocapture -- --ignored`
    //
    // You can experiment with the scan builder by adding:
    //   - `.with_predicate(predicate)` - filter rows (enables data skipping)
    //   - `.with_schema(schema)` - select specific columns
    //   - `.include_stats_columns()` - include parsed file statistics in scan metadata
    //
    // Example predicate:
    //   use delta_kernel::expressions::{column_expr, Expression};
    //   let predicate = Arc::new(column_expr!("id").gt(Expression::literal(100i64)));
    //   let scan = snapshot.scan_builder().with_predicate(predicate).build()?;
    //
    // Example schema selection (read only "id" and "name" columns):
    //   use delta_kernel::schema::{StructType, StructField, DataType};
    //   let schema = Arc::new(StructType::try_new(vec![
    //       StructField::nullable("id", DataType::LONG),
    //       StructField::nullable("name", DataType::STRING),
    //   ])?);
    //   let scan = snapshot.scan_builder().with_schema(schema).build()?;
    #[ignore]
    #[tokio::test]
    async fn read_uc_table_data() -> Result<(), BoxError> {
        let config = load_uc_table_config().await?;

        // Load snapshot
        let snapshot = config
            .catalog
            .load_snapshot(&config.table_id, &config.table_uri, &config.engine)
            .await?;
        let version = snapshot.version();
        println!("📋 Table schema (version {version}):");
        for field in snapshot.schema().fields() {
            println!("  - {}: {:?}", field.name(), field.data_type());
        }

        // Build scan to read all data
        let scan = snapshot.scan_builder().build()?;

        println!("\n📊 Scanning table...");

        // Execute scan and collect results
        let batches = execute_scan_to_batches(&scan, Arc::new(config.engine))?;

        if batches.is_empty() {
            println!("⚠️  No data found");
        } else {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!(
                "✅ Found {} rows across {} batches:",
                total_rows,
                batches.len()
            );
            print_batches(&batches)?;

            // ───────────────────────────────────────────────────────────────
            // Examples of extracting data from RecordBatches:
            // ───────────────────────────────────────────────────────────────
            //
            // use delta_kernel::arrow::array::AsArray;
            //
            // 1. Access a column by name:
            //    let batch = &batches[0];
            //    let col_idx = batch.schema().index_of("column_name").unwrap();
            //    let column = batch.column(col_idx);
            //
            // 2. Downcast to specific Arrow array types using AsArray trait:
            //
            //    // For string columns (StringArray):
            //    let string_col = column.as_string::<i32>();
            //    for i in 0..string_col.len() {
            //        if let Some(val) = string_col.value(i) {
            //            println!("row {i}: {val}");
            //        }
            //    }
            //
            //    // For integer columns (Int64Array, Int32Array, etc.):
            //    use delta_kernel::arrow::datatypes::Int64Type;
            //    let int_col = column.as_primitive::<Int64Type>();
            //    for i in 0..int_col.len() {
            //        println!("row {i}: {}", int_col.value(i));
            //    }
            //
            //    // For boolean columns:
            //    let bool_col = column.as_boolean();
            //
            //    // For struct columns (nested data):
            //    let struct_col = column.as_struct();
            //    let nested_field = struct_col.column_by_name("field_name");
            //
            // 3. Get raw values as a slice (for primitive types):
            //    let values: &[i64] = int_col.values();
            //
            // 4. Check for nulls:
            //    if column.is_null(row_idx) { /* handle null */ }
            //
            // 5. Get batch dimensions:
            //    batch.num_rows()    // number of rows
            //    batch.num_columns() // number of columns
            //
            // 6. Iterate over all rows with column access:
            //    for row in 0..batch.num_rows() {
            //        let id = int_col.value(row);
            //        let name = string_col.value(row);
            //        println!("row {row}: id={id}, name={name}");
            //    }
        }

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
