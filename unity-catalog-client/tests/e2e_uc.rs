//! Manual e2e tests for reading/writing UC-managed Delta tables.
//! These require a running UC instance and environment variables:
//!   ENDPOINT=".." TABLENAME=".." TOKEN=".."

use std::env;
use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::transaction::CommitResult;
use delta_kernel_unity_catalog::{ClientConfig, UCCommitsRestClient, UCCommitter, UCKernelClient};
use tracing::info;
use unity_catalog_client::models::credentials::Operation;
use unity_catalog_client::UCClient;
use url::Url;

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

// Run with: ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t -p unity-catalog-client read_uc_table -- --ignored
#[ignore]
#[tokio::test]
async fn read_uc_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
    let token = env::var("TOKEN").expect("TOKEN environment variable not set");
    let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

    // build shared config
    let config = ClientConfig::build(&endpoint, &token).build()?;

    // build clients
    let uc_client = UCClient::new(config.clone())?;
    let uc_commits_client = UCCommitsRestClient::new(config)?;

    let (table_id, table_uri) = get_table(&uc_client, &table_name).await?;
    let creds = uc_client
        .get_credentials(&table_id, Operation::Read)
        .await
        .map_err(|e| format!("Failed to get credentials: {}", e))?;

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

    println!("loaded snapshot: {snapshot:?}");

    Ok(())
}

// Run with: ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t -p unity-catalog-client write_uc_table -- --ignored
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn write_uc_table() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
    let token = env::var("TOKEN").expect("TOKEN environment variable not set");
    let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

    // build shared config
    let config = ClientConfig::build(&endpoint, &token).build()?;

    // build clients
    let client = UCClient::new(config.clone())?;
    let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

    let (table_id, table_uri) = get_table(&client, &table_name).await?;
    let creds = client
        .get_credentials(&table_id, Operation::ReadWrite)
        .await
        .map_err(|e| format!("Failed to get credentials: {}", e))?;

    let catalog = UCKernelClient::new(commits_client.as_ref());

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
            println!("committed version {}", t.commit_version());
            // TODO: should use post-commit snapshot here (plumb through log tail)
            let _snapshot = catalog
                .load_snapshot_at(&table_id, &table_uri, t.commit_version(), &engine)
                .await?;
            // then do publish
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
