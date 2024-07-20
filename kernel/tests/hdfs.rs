// Hdfs integration tests
//
// In order to set up the MiniDFS cluster you need to have Java, Maven, Hadoop binaries and Kerberos
// tools available and on your path. Any Java version between 8 and 17 should work.
//
// Run these integration tests with:
//   cargo test --features integration-test,cloud --test hdfs
#![cfg(all(
    feature = "integration-test",
    feature = "cloud",
    not(target_os = "windows")
))]

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::Table;
use hdfs_native::{Client, WriteOptions};
use hdfs_native_object_store::minidfs::MiniDfs;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
extern crate walkdir;
use walkdir::WalkDir;

async fn write_local_path_to_hdfs(
    local_path: &Path,
    remote_path: &Path,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in WalkDir::new(local_path) {
        let entry = entry?;
        let path = entry.path();

        let relative_path = path.strip_prefix(local_path)?;
        let destination = remote_path.join(relative_path);

        if path.is_file() {
            let bytes = fs::read(path)?;
            let mut writer = client
                .create(
                    destination.as_path().to_str().unwrap(),
                    WriteOptions::default(),
                )
                .await?;
            writer.write(bytes.into()).await?;
            writer.close().await?;
        } else {
            client
                .mkdirs(destination.as_path().to_str().unwrap(), 0o755, true)
                .await?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn read_table_version_hdfs() -> Result<(), Box<dyn std::error::Error>> {
    let minidfs = MiniDfs::with_features(&HashSet::new());
    let hdfs_client = Client::default();

    // Copy table to MiniDFS
    write_local_path_to_hdfs(
        "./tests/data/app-txn-checkpoint".as_ref(),
        "/my-delta-table".as_ref(),
        &hdfs_client,
    )
    .await?;

    let url_str = format!("{}/my-delta-table", minidfs.url);
    let url = url::Url::parse(&url_str).unwrap();

    let engine = DefaultEngine::try_new(
        &url,
        std::iter::empty::<(&str, &str)>(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None)?;
    assert_eq!(snapshot.version(), 1);

    Ok(())
}
