use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::transaction::CommitResult;
use delta_kernel::Snapshot;
use object_store::local::LocalFileSystem;
use uc_catalog::{UCCatalog, UCCommitter};
use uc_client::commits_client::{InMemoryCommitsClient, TableData};
use uc_client::models::commits::Commit;

// ============================================================================
// Test Setup
// ============================================================================

type TestError = Box<dyn std::error::Error + Send + Sync>;

const TABLE_ID: &str = "64dcd182-b3b4-4ee0-88e0-63c159a4121c";

/// Test fixtures: commits client, engine, snapshot at v2, and temp directory.
struct TestSetup {
    commits_client: Arc<InMemoryCommitsClient>,
    engine: DefaultEngine<TokioBackgroundExecutor>,
    snapshot: Arc<Snapshot>,
    table_uri: url::Url,
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

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = delta_kernel::engine::default::DefaultEngineBuilder::new(store).build();
    let table_uri = url::Url::from_directory_path(tmp_dir.path()).map_err(|_| "invalid path")?;
    let snapshot = UCCatalog::new(commits_client.as_ref())
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
        table_uri,
        _tmp_dir,
    } = setup().await?;
    assert_eq!(snapshot.version(), 2);

    let catalog = UCCatalog::new(commits_client.as_ref());
    let beyond_max = TableData::MAX_UNPUBLISHED_COMMITS as i64 + 5;

    for i in 3..=beyond_max {
        let expected_version = i as u64;

        // Commit
        let committer = Box::new(UCCommitter::new(commits_client.clone(), TABLE_ID));
        let committed = match snapshot.clone().transaction(committer)?.commit(&engine)? {
            CommitResult::CommittedTransaction(t) => t,
            _ => return Err("Expected committed transaction".into()),
        };
        assert_eq!(committed.commit_version(), expected_version);
        snapshot = committed
            .post_commit_snapshot()
            .ok_or("no post commit snapshot")?
            .clone();

        // Publish
        let committer = UCCommitter::new(commits_client.clone(), TABLE_ID);
        snapshot.publish(&engine, &committer)?;

        // TODO(#1688): Have Snapshot::publish return a new Snapshot with the published state.
        //       For now, we reload the snapshot to get updated max_published_version
        snapshot = catalog
            .load_snapshot(TABLE_ID, table_uri.as_str(), &engine)
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_without_publish_hits_limit() -> Result<(), TestError> {
    let TestSetup {
        commits_client,
        engine,
        mut snapshot,
        _tmp_dir,
        ..
    } = setup().await?;

    // Start with 2 unpublished (v1, v2). Insert up to MAX, then the next should fail.
    let max = TableData::MAX_UNPUBLISHED_COMMITS as i64;
    for i in 3..=max {
        let committer = Box::new(UCCommitter::new(commits_client.clone(), TABLE_ID));
        let committed = match snapshot.clone().transaction(committer)?.commit(&engine)? {
            CommitResult::CommittedTransaction(t) => t,
            _ => return Err("Expected committed transaction".into()),
        };
        assert_eq!(committed.commit_version(), i as u64);
        snapshot = committed
            .post_commit_snapshot()
            .ok_or("no post commit snapshot")?
            .clone();
    }
    assert_eq!(snapshot.version(), max as u64);

    // Next insert should fail with MaxUnpublishedCommitsExceeded
    let committer = Box::new(UCCommitter::new(commits_client.clone(), TABLE_ID));
    let err = snapshot
        .clone()
        .transaction(committer)?
        .commit(&engine)
        .unwrap_err();
    assert!(
        matches!(err, delta_kernel::Error::Generic(msg) if msg.contains("Max unpublished commits"))
    );
    Ok(())
}
