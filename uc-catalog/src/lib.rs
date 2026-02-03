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

    use delta_kernel::arrow::array::{AsArray, RecordBatch};
    use delta_kernel::arrow::compute::filter_record_batch;
    use delta_kernel::arrow::util::pretty::print_batches;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::engine::arrow_expression::apply_schema::apply_schema_to;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::schema::DataType as KernelDataType;
    use delta_kernel::transaction::CommitResult;

    use tracing::info;

    use super::*;

    // We could just re-export UCClient's get_table to not require consumers to directly import
    // uc_client themselves.
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
        let config = uc_client::ClientConfig::build(&endpoint, &token).build()?;

        // build clients
        let uc_client = UCClient::new(config.clone())?;
        let uc_commits_client = UCCommitsRestClient::new(config)?;

        let (table_id, table_uri) = get_table(&uc_client, &table_name).await?;
        let creds = uc_client
            .get_credentials(&table_id, Operation::Read)
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

        let catalog = UCCatalog::new(&uc_commits_client);

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

        // println!("🎉 loaded snapshot: {snapshot:?}");

        // Build scan with stats columns to get parsed stats
        let scan = snapshot.scan_builder().include_stats_columns().build()?;

        // Get the logical stats schema (user-facing column names)
        let logical_stats_schema = scan.logical_stats_schema();

        println!("\n📋 Logical Stats Schema (user-facing column names):");
        if let Some(schema) = &logical_stats_schema {
            for field in schema.fields() {
                println!("  - {}: {:?}", field.name(), field.data_type());
            }
        } else {
            println!("  No logical stats schema available");
        }

        println!("\n📊 Inspecting parsed stats from scan_metadata:");

        // Get scan metadata which contains the parsed stats
        for (i, scan_metadata_result) in scan.scan_metadata(&engine)?.enumerate() {
            let scan_metadata = scan_metadata_result?;
            let (underlying_data, selection_vector) = scan_metadata.scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)?.into();

            // Apply selection vector filter
            let bool_array = delta_kernel::arrow::array::BooleanArray::from(selection_vector);
            let filtered_batch = filter_record_batch(&batch, &bool_array)?;

            println!("\n--- Batch {} ({} rows) ---", i, filtered_batch.num_rows());

            // Print physical stats_parsed schema
            let schema = filtered_batch.schema();
            if let Ok(stats_idx) = schema.index_of("stats_parsed") {
                let stats_field = schema.field(stats_idx);
                if let delta_kernel::arrow::datatypes::DataType::Struct(fields) =
                    stats_field.data_type()
                {
                    println!("\n🔧 Physical Parsed Stats Schema (from stats_parsed column):");
                    for f in fields {
                        println!("  - {}: {:?}", f.name(), f.data_type());
                    }

                    // Get the stats_parsed column and convert to logical names using apply_schema_to
                    if let Some(logical_schema) = &logical_stats_schema {
                        let stats_column = filtered_batch.column(stats_idx).clone();

                        // Use apply_schema_to for ordinal-based renaming (physical -> logical)
                        let logical_stats = apply_schema_to(
                            &stats_column,
                            &KernelDataType::Struct(Box::new(logical_schema.as_ref().clone())),
                        )?;

                        println!("\n✨ Parsed Stats with Logical Names (using apply_schema_to):");
                        let stats_struct = logical_stats.as_struct();
                        let (logical_fields, logical_cols, _) = stats_struct.clone().into_parts();
                        let logical_batch = RecordBatch::try_new(
                            Arc::new(delta_kernel::arrow::datatypes::Schema::new(
                                logical_fields.to_vec(),
                            )),
                            logical_cols,
                        )?;
                        print_batches(&[logical_batch])?;
                    }
                }
            }
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
