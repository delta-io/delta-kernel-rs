//! Hdfs integration tests
//!
//! In order to set up the MiniDFS cluster you need to have Java, Maven, Hadoop binaries and Kerberos
//! tools available and on your path. Any Java version between 8 and 17 should work.
use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::object_store::path::Path as ObjectStorePath;
use delta_kernel::object_store::ObjectStore;

use hdfs_native_object_store::HdfsObjectStore;
use url::Url;

#[cfg(all(feature = "integration-test", not(target_os = "windows")))]
async fn write_local_path_to_hdfs(
    local_path: &std::path::Path,
    remote_path: &std::path::Path,
    client: &hdfs_native::Client,
) -> Result<(), Box<dyn std::error::Error>> {
    use hdfs_native::WriteOptions;
    use std::fs;
    use walkdir::WalkDir;

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

#[cfg(all(feature = "integration-test", not(target_os = "windows")))]
#[tokio::test]
async fn read_table_version_hdfs() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    use delta_kernel::Table;
    use hdfs_native::Client;
    use hdfs_native_object_store::minidfs::MiniDfs;
    use std::collections::HashSet;

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
    let url = Url::parse(&url_str).unwrap();

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

/// Example funciton of doing testing of a custom [HdfsObjectStore] construction
fn parse_url_opts_hdfs_native<I, K, V>(
    url: &Url,
    options: I,
) -> Result<(Box<dyn ObjectStore>, ObjectStorePath), delta_kernel::object_store::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let options_map = options
        .into_iter()
        .map(|(k, v)| (k.as_ref().to_string(), v.into()))
        .collect();
    let store = HdfsObjectStore::with_config(url.as_str(), options_map)?;
    let path = ObjectStorePath::parse(url.path())?;
    Ok((Box::new(store), path))
}

#[test]
fn test_add_hdfs_scheme() {
    let scheme = "hdfs";
    // if let Ok(handlers) = URL_REGISTRY.read() {
    //     assert!(handlers.get(scheme).is_none());
    // } else {
    //     panic!("Failed to read the RwLock for the registry");
    // }
    delta_kernel::engine::default::storage::insert_url_handler(
        scheme,
        Arc::new(parse_url_opts_hdfs_native),
    )
    .expect("Failed to add new URL scheme handler");

    // if let Ok(handlers) = URL_REGISTRY.read() {
    //     assert!(handlers.get(scheme).is_some());
    // } else {
    //     panic!("Failed to read the RwLock for the registry");
    // }

    let url: Url = Url::parse("hdfs://example").expect("Failed to parse URL");
    let options: HashMap<String, String> = HashMap::default();
    // Currently constructing an [HdfsObjectStore] won't work if there isn't an actual HDFS
    // to connect to, so the only way to really verify that we got the object store we
    // jxpected is to inspect the `store` on the error v_v
    if let Err(store_error) = delta_kernel::engine::default::storage::parse_url_opts(&url, options)
    {
        match store_error {
            delta_kernel::object_store::Error::Generic { store, source: _ } => {
                assert_eq!(store, "HdfsObjectStore");
            }
            unexpected => panic!("Unexpected error happened: {unexpected:?}"),
        }
    } else {
        panic!("Expected to get an error when constructing an HdfsObjectStore, but something didn't work as expected! Either the parse_url_opts_hdfs_native function didn't get called, or the hdfs-native-object-store no longer errors when it cannot connect to HDFS");
    }
}
