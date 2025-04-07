/// This module contains all of the miscellaneous acceptance tests for delta-kernel-rs
///
/// Since each new `.rs` file in this directory results in increased build and link time, it is
/// important to only add new files if absolutely necessary for code readability or test
/// performance.
use delta_kernel::snapshot::LastCheckpointHint;

#[test]
fn test_checkpoint_serde() {
    let file = std::fs::File::open(
        "./tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/_last_checkpoint",
    )
    .unwrap();
    let cp: LastCheckpointHint = serde_json::from_reader(file).unwrap();
    assert_eq!(cp.version, 2)
}

/*
#[tokio::test]
async fn test_read_last_checkpoint() {
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let prefix = Path::from(url.path());
    let storage = ObjectStoreStorageHandler::new(store, prefix);
    let cp = read_last_checkpoint(&storage, &url).await.unwrap().unwrap();
    assert_eq!(cp.version, 2);
}

#[tokio::test]
async fn test_read_table_with_checkpoint() {
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/dat/out/reader_tests/generated/with_checkpoint/delta/",
    ))
    .unwrap();
    let location = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(
        DefaultEngine::try_new(&location, HashMap::<String, String>::new()).unwrap(),
    );
    let snapshot = Snapshot::try_new(location, engine, None)
        .await
        .unwrap();

    assert_eq!(snapshot.log_segment.checkpoint_files.len(), 1);
    assert_eq!(
        LogPath(&snapshot.log_segment.checkpoint_files[0].location).commit_version(),
        Some(2)
    );
    assert_eq!(snapshot.log_segment.commit_files.len(), 1);
    assert_eq!(
        LogPath(&snapshot.log_segment.commit_files[0].location).commit_version(),
        Some(3)
    );
}
*/
