//! Reproduction for deadlock in TokioMultiThreadExecutor when mixing sync_channel with handle.spawn()
//!
//! When read_files() is called from a tokio worker thread:
//! 1. Creates sync_channel(0) and spawns async task via task_executor.spawn()
//! 2. Tokio schedules task to current worker's LOCAL queue (optimization)
//! 3. Thread immediately blocks on sync_channel.recv()
//! 4. Task is stuck in local queue - worker is OS-blocked, other workers don't steal it
//!
//! Root cause: tokio's schedule_task() uses with_current() to detect worker context
//! and optimizes by scheduling locally, but sync blocking prevents task execution.

use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::filesystem::ObjectStoreStorageHandler;
use delta_kernel::StorageHandler;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

fn create_test_file() -> (tempfile::TempDir, Url) {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, b"test data").unwrap();

    let mut url = Url::from_directory_path(temp_dir.path()).unwrap();
    url.set_path(&format!("{}/test.txt", url.path()));

    (temp_dir, url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // Expected to timeout
async fn test_tokio_multi_thread_executor_deadlock() {
    let (_temp_dir, url) = create_test_file();

    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));

    let storage = ObjectStoreStorageHandler::new(store, executor, None);

    let task = tokio::spawn(async move {
        let mut files = storage.read_files(vec![(url, None)]).unwrap();
        files.next()
    });

    match tokio::time::timeout(Duration::from_secs(5), task).await {
        Err(_) => {
            panic!("DEADLOCK: read_files() hung when called from tokio worker with TokioMultiThreadExecutor");
        }
        Ok(result) => {
            println!("Unexpected success: {:?}", result);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tokio_background_executor_works() {
    let (_temp_dir, url) = create_test_file();

    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioBackgroundExecutor::new());

    let storage = ObjectStoreStorageHandler::new(store, executor, None);

    let task = tokio::spawn(async move {
        let mut files = storage.read_files(vec![(url, None)]).unwrap();
        files.next()
    });

    match tokio::time::timeout(Duration::from_secs(5), task).await {
        Err(_) => {
            panic!("Unexpected timeout with TokioBackgroundExecutor");
        }
        Ok(result) => {
            let data: bytes::Bytes = result.unwrap().unwrap().unwrap();
            assert_eq!(data.as_ref(), b"test data");
        }
    }
}

#[test]
fn test_deadlock_minimal_repro() {
    use std::sync::mpsc::sync_channel;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let handle = rt.handle().clone();

    rt.block_on(async move {
        let task = tokio::spawn(async move {
            let (sender, receiver) = sync_channel::<String>(0);

            handle.spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                sender.send("done".to_string()).ok();
            });

            receiver.recv()
        });

        match tokio::time::timeout(Duration::from_secs(2), task).await {
            Err(_) => {
                panic!("Deadlock: task scheduled to local queue but worker blocked on sync_channel");
            }
            Ok(Ok(Ok(_))) => {}
            Ok(Err(e)) => panic!("Task panicked: {:?}", e),
            Ok(Ok(Err(e))) => panic!("Receive error: {:?}", e),
        }
    });
}
