use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_unity_catalog::{
    get_required_properties_for_disk, log_tail_from_commits, UCCommitter,
};
use test_utils::{insert_data_with, read_scan};
use unity_catalog_delta_client_api::{Commit, InMemoryUpdateTableClient, TableData, Update};

const TEST_CATALOG: &str = "test_catalog";
const TEST_SCHEMA: &str = "test_schema";
const TEST_TABLE: &str = "test_table";

// ============================================================================
// Test Setup
// ============================================================================

type TestError = Box<dyn std::error::Error + Send + Sync>;

const TABLE_ID: &str = "64dcd182-b3b4-4ee0-88e0-63c159a4121c";

/// Test fixtures: commits client, engine, snapshot at v2, and temp directory.
struct TestSetup {
    update_table_client: Arc<InMemoryUpdateTableClient>,
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

    // Read path: connector fetches load_table from UC, builds the
    // log tail via kernel-uc, then drives the snapshot builder directly.
    let snapshot = build_snapshot(&update_table_client, &engine, &table_uri)?;

    Ok(TestSetup {
        update_table_client,
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
    update_table_client: &Arc<InMemoryUpdateTableClient>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
) -> Result<Arc<Snapshot>, TestError> {
    let committer = Box::new(uc_committer(update_table_client));
    Ok(snapshot
        .clone()
        .transaction(committer, engine)?
        .commit(engine)?
        .unwrap_post_commit_snapshot())
}

// ============================================================================
// Tests
// ============================================================================

// multi_thread required: UCCommitter uses block_on which panics on single-threaded runtime
#[tokio::test(flavor = "multi_thread")]
async fn test_insert_and_publish() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        mut snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;
    assert_eq!(snapshot.version(), 2);

    let beyond_max = TableData::MAX_UNPUBLISHED_COMMITS as u64 + 5;

    for _ in 3..=beyond_max {
        snapshot = commit(&snapshot, &update_table_client, &engine)?;

        let committer = uc_committer(&update_table_client);

        snapshot = snapshot.publish(&engine, &committer)?;
    }
    assert_eq!(snapshot.version(), beyond_max);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_without_publish_hits_limit() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        mut snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;

    // Start with 2 unpublished (v1, v2). Insert up to MAX, then the next should fail.
    let max = TableData::MAX_UNPUBLISHED_COMMITS as u64;
    for _ in 3..=max {
        snapshot = commit(&snapshot, &update_table_client, &engine)?;
    }
    assert_eq!(snapshot.version(), max);

    // Next insert should fail with MaxUnpublishedCommitsExceeded
    let committer = Box::new(uc_committer(&update_table_client));
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
        update_table_client,
        engine,
        snapshot,
        table_uri,
        _tmp_dir,
    } = setup().await?;

    let committer = uc_committer(&update_table_client);

    commit(&snapshot, &update_table_client, &engine)?
        .publish(&engine, &committer)?
        .checkpoint(&engine, None)?;

    // Load a fresh snapshot and verify checkpoint was written
    let snapshot = Snapshot::builder_for(table_uri)
        .with_max_catalog_version(3)
        .build(&engine)?;
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(3));

    Ok(())
}

/// ALTER setting a user property forwards a `SetProperties` update through the committer.
#[tokio::test(flavor = "multi_thread")]
async fn test_alter_set_table_property_forwards_set_properties() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;

    let committer = Box::new(uc_committer(&update_table_client));
    snapshot
        .alter_table()
        .set_table_property("user.team", "delta")
        .build(&engine, committer)?
        .commit(&engine)?
        .unwrap_committed();

    let recorded = update_table_client.recorded_updates();
    let last = recorded.last().ok_or("expected a recorded update")?;
    let set_props = last
        .updates
        .iter()
        .find_map(|u| match u {
            Update::SetProperties { updates } => Some(updates),
            _ => None,
        })
        .ok_or("expected a SetProperties update")?;
    assert_eq!(set_props.get("user.team"), Some(&"delta".to_string()));
    Ok(())
}

/// ALTER setting `delta.enableChangeDataFeed=true` auto-enables the `changeDataFeed` feature and
/// forwards a `SetProtocol` update carrying the evolved feature lists.
#[tokio::test(flavor = "multi_thread")]
async fn test_alter_enabling_feature_forwards_set_protocol() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;

    let committer = Box::new(uc_committer(&update_table_client));
    snapshot
        .alter_table()
        .set_table_property("delta.enableChangeDataFeed", "true")
        .build(&engine, committer)?
        .commit(&engine)?
        .unwrap_committed();

    let recorded = update_table_client.recorded_updates();
    let last = recorded.last().ok_or("expected a recorded update")?;
    let protocol = last
        .updates
        .iter()
        .find_map(|u| match u {
            Update::SetProtocol { protocol } => Some(protocol),
            _ => None,
        })
        .ok_or("expected a SetProtocol update")?;
    assert!(
        protocol
            .writer_features
            .contains(&"changeDataFeed".to_string()),
        "SetProtocol must carry changeDataFeed, got {:?}",
        protocol.writer_features
    );
    Ok(())
}

/// Builds a snapshot at the catalog's current ratified version, mirroring the connector read path:
/// `load_table_response` -> `log_tail_from_commits` -> snapshot builder.
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

#[tokio::test(flavor = "multi_thread")]
async fn test_scan_returns_fixture_rows() -> Result<(), TestError> {
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
async fn test_append_scan_back_and_incremental_read() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_uri = url::Url::from_directory_path(tmp.path()).map_err(|_| "invalid path")?;

    let store = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = Arc::new(
        delta_kernel_default_engine::DefaultEngineBuilder::new(store)
            .with_task_executor(executor)
            .build(),
    );

    let client = Arc::new(InMemoryUpdateTableClient::new());

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("val", DataType::STRING),
    ])?);

    // v0 create: writes 000.json directly to storage, does not call the catalog.
    create_table(table_uri.as_str(), schema, "delta-kernel-uc-test")
        .with_table_properties(get_required_properties_for_disk(TABLE_ID))
        .build(engine.as_ref(), Box::new(uc_committer(&client)))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    client.create_table(TABLE_ID)?;

    let snap_v0 = build_snapshot(&client, &engine, &table_uri)?;
    assert_eq!(snap_v0.version(), 0);
    let scan = snap_v0.clone().scan_builder().build()?;
    let v0_rows: usize = read_scan(&scan, engine.clone() as Arc<dyn Engine>)?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(v0_rows, 0);

    // v1 append: 3 rows.
    let id_col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let val_col: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    insert_data_with(
        snap_v0,
        &engine,
        vec![id_col, val_col],
        Box::new(uc_committer(&client)),
        "WRITE",
        true,
        false,
    )
    .await?
    .unwrap_committed();

    let snap_v1 = build_snapshot(&client, &engine, &table_uri)?;
    assert_eq!(snap_v1.version(), 1);
    let scan = snap_v1.clone().scan_builder().build()?;
    let v1_batches = read_scan(&scan, engine.clone() as Arc<dyn Engine>)?;
    let v1_rows: usize = v1_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(v1_rows, 3);
    let mut ids: Vec<i32> = v1_batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is int32");
            col.values().to_vec()
        })
        .collect();
    // Scan row order is not guaranteed, so sort before comparing.
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);

    // v2 append: 2 more rows -> incremental read sees v1 + v2.
    let id_col: ArrayRef = Arc::new(Int32Array::from(vec![4, 5]));
    let val_col: ArrayRef = Arc::new(StringArray::from(vec!["d", "e"]));
    insert_data_with(
        snap_v1,
        &engine,
        vec![id_col, val_col],
        Box::new(uc_committer(&client)),
        "WRITE",
        true,
        false,
    )
    .await?
    .unwrap_committed();

    let snap_v2 = build_snapshot(&client, &engine, &table_uri)?;
    assert_eq!(snap_v2.version(), 2);
    let scan = snap_v2.clone().scan_builder().build()?;
    let v2_rows: usize = read_scan(&scan, engine.clone() as Arc<dyn Engine>)?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(v2_rows, 5);

    // Publish, then confirm reads still return all rows.
    let published = snap_v2.publish(engine.as_ref(), &uc_committer(&client))?;
    let scan = published.scan_builder().build()?;
    let pub_rows: usize = read_scan(&scan, engine.clone() as Arc<dyn Engine>)?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(pub_rows, 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cannot_checkpoint_unpublished_snapshot() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        snapshot,
        table_uri: _,
        _tmp_dir,
    } = setup().await?;

    let snapshot = commit(&snapshot, &update_table_client, &engine)?;

    let err = snapshot.checkpoint(&engine, None).unwrap_err();
    assert!(matches!(err, delta_kernel::Error::Generic(msg) if msg.contains("not published")));
    Ok(())
}
