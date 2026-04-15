use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectStore, ObjectStoreExt as _};
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::transaction::CommitResult;
use delta_kernel::Snapshot;
use delta_kernel_unity_catalog::{UCCommitter, UCKernelClient};
use test_utils::{
    actions_to_string_with_metadata, add_commit, collect_file_action_paths, TestAction,
};
use unity_catalog_delta_client_api::{Commit, InMemoryCommitsClient, TableData};

// ============================================================================
// Test Setup
// ============================================================================

type TestError = Box<dyn std::error::Error + Send + Sync>;

const TABLE_ID: &str = "64dcd182-b3b4-4ee0-88e0-63c159a4121c";

/// Test fixtures: commits client, engine, snapshot at v2, and temp directory.
struct TestSetup {
    commits_client: Arc<InMemoryCommitsClient>,
    engine: DefaultEngine<TokioMultiThreadExecutor>,
    snapshot: Arc<Snapshot>,
    table_uri: url::Url,
    /// Tests must bind this field (not ignore with `..` or `_`) to prevent the temp directory
    /// from being dropped and cleaned up before the test completes.
    _tmp_dir: tempfile::TempDir,
}

/// Copies test data to temp dir and loads snapshot at v2 with in-memory commits client.
async fn setup() -> Result<TestSetup, TestError> {
    let src = PathBuf::from("./tests/data/catalog_managed_0/");
    let tmp_dir = tempfile::tempdir()?;
    copy_dir_recursive(&src, tmp_dir.path())?;

    // v0 published, v1/v2 ratified but unpublished
    let commits_client = Arc::new(InMemoryCommitsClient::new());
    commits_client.insert_table(
        TABLE_ID,
        TableData {
            max_ratified_version: 2,
            catalog_commits: vec![
                Commit::new(
                    1,
                    1749830871085,
                    "00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json",
                    889,
                    1749830870833,
                ),
                Commit::new(
                    2,
                    1749830881799,
                    "00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json",
                    891,
                    1749830881779,
                ),
            ],
        },
    );

    let store = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = delta_kernel::engine::default::DefaultEngineBuilder::new(store)
        .with_task_executor(executor)
        .build();
    let table_uri = url::Url::from_directory_path(tmp_dir.path()).map_err(|_| "invalid path")?;
    let snapshot = UCKernelClient::new(commits_client.as_ref())
        .load_snapshot_at(TABLE_ID, table_uri.as_str(), 2, &engine)
        .await?;

    Ok(TestSetup {
        commits_client,
        engine,
        snapshot,
        table_uri,
        _tmp_dir: tmp_dir,
    })
}

/// Recursively copies a directory tree.
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dst_path = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir_recursive(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

/// Commits an empty transaction and returns the post-commit snapshot.
fn commit(
    snapshot: &Arc<Snapshot>,
    commits_client: &Arc<InMemoryCommitsClient>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
) -> Result<Arc<Snapshot>, TestError> {
    let committer = Box::new(UCCommitter::new(commits_client.clone(), TABLE_ID));
    match snapshot
        .clone()
        .transaction(committer, engine)?
        .commit(engine)?
    {
        CommitResult::CommittedTransaction(t) => Ok(t
            .post_commit_snapshot()
            .ok_or("no post commit snapshot")?
            .clone()),
        _ => Err("Expected committed transaction".into()),
    }
}

// ============================================================================
// Tests
// ============================================================================

// multi_thread required: UCCommitter uses block_on which panics on single-threaded runtime
#[tokio::test(flavor = "multi_thread")]
async fn test_insert_and_publish() -> Result<(), TestError> {
    let TestSetup {
        commits_client,
        engine,
        mut snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;
    assert_eq!(snapshot.version(), 2);

    let beyond_max = TableData::MAX_UNPUBLISHED_COMMITS as u64 + 5;

    for _ in 3..=beyond_max {
        snapshot = commit(&snapshot, &commits_client, &engine)?;

        let committer = UCCommitter::new(commits_client.clone(), TABLE_ID);

        snapshot = snapshot.publish(&engine, &committer)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_without_publish_hits_limit() -> Result<(), TestError> {
    let TestSetup {
        commits_client,
        engine,
        mut snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;

    // Start with 2 unpublished (v1, v2). Insert up to MAX, then the next should fail.
    let max = TableData::MAX_UNPUBLISHED_COMMITS as u64;
    for _ in 3..=max {
        snapshot = commit(&snapshot, &commits_client, &engine)?;
    }
    assert_eq!(snapshot.version(), max);

    // Next insert should fail with MaxUnpublishedCommitsExceeded
    let committer = Box::new(UCCommitter::new(commits_client.clone(), TABLE_ID));
    let err = snapshot
        .clone()
        .transaction(committer, &engine)?
        .commit(&engine)
        .unwrap_err();
    assert!(
        matches!(err, delta_kernel::Error::Generic(msg) if msg.contains("Max unpublished commits"))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_checkpoint_after_publish() -> Result<(), TestError> {
    let TestSetup {
        commits_client,
        engine,
        snapshot,
        table_uri,
        _tmp_dir,
    } = setup().await?;

    let committer = UCCommitter::new(commits_client.clone(), TABLE_ID);

    commit(&snapshot, &commits_client, &engine)?
        .publish(&engine, &committer)?
        .checkpoint(&engine)?;

    // Load a fresh snapshot and verify checkpoint was written
    let snapshot = Snapshot::builder_for(table_uri)
        .with_max_catalog_version(3)
        .build(&engine)?;
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(3));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cannot_checkpoint_unpublished_snapshot() -> Result<(), TestError> {
    let TestSetup {
        commits_client,
        engine,
        snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;

    let snapshot = commit(&snapshot, &commits_client, &engine)?;

    let err = snapshot.checkpoint(&engine).unwrap_err();
    assert!(matches!(err, delta_kernel::Error::Generic(msg) if msg.contains("not published")));
    Ok(())
}

/// uc-managed V2 table: write commits to InMemory store, load snapshot,
/// write a sidecar checkpoint, and verify checkpoint + sidecars exist.
#[tokio::test(flavor = "multi_thread")]
async fn test_sidecar_checkpoint_uc_managed_v2_table() -> Result<(), TestError> {
    // Create v0 metadata with catalogManaged + v2Checkpoint features
    let v0_metadata = test_utils::CATALOG_MANAGED_METADATA.replace(
        r#""readerFeatures":["catalogManaged"],"writerFeatures":["catalogManaged","inCommitTimestamp"]"#,
        r#""readerFeatures":["v2Checkpoint","catalogManaged"],"writerFeatures":["v2Checkpoint","catalogManaged","inCommitTimestamp"]"#,
    );

    let store = Arc::new(InMemory::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = DefaultEngineBuilder::new(store.clone())
        .with_task_executor(executor)
        .build();

    let table_root = "memory:///";

    let v0_actions = actions_to_string_with_metadata(
        vec![
            TestAction::Metadata,
            TestAction::Add("file1.parquet".into()),
            TestAction::Add("file2.parquet".into()),
        ],
        &v0_metadata,
    );
    add_commit(table_root, store.as_ref(), 0, v0_actions)
        .await
        .map_err(|e| -> TestError { e.to_string().into() })?;

    let table_id = "v2-ckpt-catalog-table";
    let commits_client = Arc::new(InMemoryCommitsClient::new());
    commits_client.insert_table(
        table_id,
        TableData {
            max_ratified_version: 0,
            catalog_commits: vec![],
        },
    );

    let catalog = UCKernelClient::new(commits_client.as_ref());
    let snapshot = catalog
        .load_snapshot_at(table_id, table_root, 0, &engine)
        .await?;
    assert_eq!(snapshot.version(), 0);
    assert!(snapshot.table_configuration().is_catalog_managed());

    let spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(10),
    });
    snapshot.snapshot_checkpoint_placeholder(&engine, Some(&spec))?;

    let ckpt_path = Path::from("_delta_log/00000000000000000000.checkpoint.parquet");
    assert!(
        store.head(&ckpt_path).await.is_ok(),
        "checkpoint file should exist"
    );

    let last_ckpt_path = Path::from("_delta_log/_last_checkpoint");
    assert!(
        store.head(&last_ckpt_path).await.is_ok(),
        "_last_checkpoint file should exist"
    );

    // Read sidecar parquet files and verify add action paths
    let sidecar_prefix = Path::from("_delta_log/_sidecars");
    let sidecar_list = store.list_with_delimiter(Some(&sidecar_prefix)).await?;
    assert!(
        !sidecar_list.objects.is_empty(),
        "at least one sidecar file should exist"
    );

    let mut batches = Vec::new();
    for obj in &sidecar_list.objects {
        let bytes = store.get(&obj.location).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        batches.extend(reader.map(|rb| rb.unwrap()));
    }
    let (add_paths, _) = collect_file_action_paths(&batches);
    assert_eq!(add_paths, vec!["file1.parquet", "file2.parquet"]);

    Ok(())
}
