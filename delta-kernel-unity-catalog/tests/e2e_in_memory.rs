use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_unity_catalog::{log_tail_from_commits, UCCommitter};
use test_utils::read_scan;
use unity_catalog_delta_client_api::{Commit, InMemoryUpdateTableClient, TableData};

const TEST_CATALOG: &str = "test_catalog";
const TEST_SCHEMA: &str = "test_schema";
const TEST_TABLE: &str = "test_table";
const TABLE_ID: &str = "64dcd182-b3b4-4ee0-88e0-63c159a4121c";

type TestError = Box<dyn std::error::Error + Send + Sync>;

struct TestSetup {
    update_table_client: Arc<InMemoryUpdateTableClient>,
    engine: DefaultEngine<TokioMultiThreadExecutor>,
    snapshot: Arc<Snapshot>,
    table_uri: url::Url,
    /// Tests must bind this field (not ignore with `..` or `_`) to keep the temp directory alive
    /// for the duration of the test.
    _tmp_dir: tempfile::TempDir,
}

/// Copies the fixture into a temp dir and builds a snapshot at v2 backed by an in-memory catalog
/// that knows v1/v2 as ratified-but-unpublished commits.
async fn setup() -> Result<TestSetup, TestError> {
    let src = PathBuf::from("./tests/data/catalog_managed_0/");
    let tmp_dir = tempfile::tempdir()?;
    copy_dir_recursive(&src, tmp_dir.path())?;

    let update_table_client = Arc::new(InMemoryUpdateTableClient::new());
    update_table_client.insert_table(
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
    let engine = delta_kernel_default_engine::DefaultEngineBuilder::new(store)
        .with_task_executor(executor)
        .build();
    let table_uri = url::Url::from_directory_path(tmp_dir.path()).map_err(|_| "invalid path")?;

    let snapshot = build_snapshot(&update_table_client, &engine, &table_uri)?;

    Ok(TestSetup {
        update_table_client,
        engine,
        snapshot,
        table_uri,
        _tmp_dir: tmp_dir,
    })
}

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

/// Mirrors the connector read path: `load_table_response` -> `log_tail_from_commits` -> snapshot
/// builder at the catalog's current ratified version.
fn build_snapshot(
    client: &Arc<InMemoryUpdateTableClient>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
    table_uri: &url::Url,
) -> Result<Arc<Snapshot>, TestError> {
    let resp = client.load_table_response(TABLE_ID, table_uri.as_str())?;
    let log_tail = log_tail_from_commits(&resp.commits, table_uri)?;
    let max: u64 = resp
        .latest_table_version
        .ok_or("missing version")?
        .try_into()?;
    Ok(Snapshot::builder_for(table_uri.clone())
        .with_log_tail(log_tail)
        .with_max_catalog_version(max)
        .build(engine)?)
}

fn uc_committer(client: &Arc<InMemoryUpdateTableClient>) -> UCCommitter<InMemoryUpdateTableClient> {
    UCCommitter::new(
        client.clone(),
        TABLE_ID,
        TEST_CATALOG,
        TEST_SCHEMA,
        TEST_TABLE,
    )
}

/// Commits an empty (metadata-only) transaction through the UC committer and returns the
/// post-commit snapshot.
fn commit_empty(
    snapshot: &Arc<Snapshot>,
    client: &Arc<InMemoryUpdateTableClient>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
) -> Result<Arc<Snapshot>, TestError> {
    Ok(snapshot
        .clone()
        .transaction(Box::new(uc_committer(client)), engine)?
        .with_operation("WRITE".to_string())
        .commit(engine)?
        .unwrap_post_commit_snapshot())
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_returns_fixture_rows() -> Result<(), TestError> {
    let TestSetup {
        engine,
        snapshot,
        _tmp_dir,
        ..
    } = setup().await?;
    assert_eq!(snapshot.version(), 2);

    let scan = snapshot.scan_builder().build()?;
    let engine_dyn: Arc<dyn Engine> = Arc::new(engine);
    let batches = read_scan(&scan, engine_dyn)?;

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 200);

    let schema = batches
        .first()
        .ok_or("expected at least one batch")?
        .schema();
    assert!(schema.column_with_name("part1").is_some());
    assert!(schema.column_with_name("col1").is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn append_commits_advance_version_and_preserve_rows() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        mut snapshot,
        table_uri,
        _tmp_dir,
    } = setup().await?;
    assert_eq!(snapshot.version(), 2);

    // Append three metadata-only commits through the committer, publishing each so the catalog's
    // unpublished-commit watermark advances (AddCommit + SetLatestBackfilledVersion).
    for expected_version in 3..=5 {
        snapshot = commit_empty(&snapshot, &update_table_client, &engine)?;
        assert_eq!(snapshot.version(), expected_version);
        snapshot = snapshot.publish(&engine, &uc_committer(&update_table_client))?;
    }

    // Re-read from the catalog and confirm the fixture's rows are still all present.
    let reloaded = build_snapshot(&update_table_client, &engine, &table_uri)?;
    assert_eq!(reloaded.version(), 5);
    let scan = reloaded.scan_builder().build()?;
    let total: usize = read_scan(&scan, Arc::new(engine))?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(total, 200);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unpublished_commits_hit_backpressure_limit() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        mut snapshot,
        _tmp_dir,
        ..
    } = setup().await?;

    // Start with 2 unpublished (v1, v2). Append without publishing up to the limit.
    let max = TableData::MAX_UNPUBLISHED_COMMITS as u64;
    for _ in 3..=max {
        snapshot = commit_empty(&snapshot, &update_table_client, &engine)?;
    }
    assert_eq!(snapshot.version(), max);

    // The next unpublished append must fail with the max-unpublished-commits backpressure error.
    let err = snapshot
        .clone()
        .transaction(Box::new(uc_committer(&update_table_client)), &engine)?
        .with_operation("WRITE".to_string())
        .commit(&engine)
        .unwrap_err();
    assert!(
        matches!(err, delta_kernel::Error::Generic(msg) if msg.contains("Max unpublished commits"))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cannot_checkpoint_unpublished_snapshot() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        snapshot,
        _tmp_dir,
        ..
    } = setup().await?;

    let snapshot = commit_empty(&snapshot, &update_table_client, &engine)?;
    let err = snapshot.checkpoint(&engine, None).unwrap_err();
    assert!(matches!(err, delta_kernel::Error::Generic(msg) if msg.contains("not published")));
    Ok(())
}
