use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray};
use delta_kernel::coroutine::kernel;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::schema_ref;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::{AsyncEngineConnector, DefaultEngine};
use delta_kernel_unity_catalog::{
    coroutine as uc, get_required_properties_for_disk, snapshot_builder_from_load_table,
    UCCommitter,
};
use test_utils::{insert_data_with, read_scan};
use unity_catalog_delta_client_api::{
    Commit, InMemoryUpdateTableClient, TableData, TableIdentifier,
};

// ============================================================================
// Test Setup
// ============================================================================

type TestError = Box<dyn std::error::Error + Send + Sync>;

const TEST_CATALOG: &str = "test_catalog";
const TEST_SCHEMA: &str = "test_schema";
const TEST_TABLE: &str = "test_table";
const TABLE_ID: &str = "64dcd182-b3b4-4ee0-88e0-63c159a4121c";

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

    let resp = update_table_client.load_table_response(TABLE_ID, table_uri.as_str())?;
    let snapshot = snapshot_builder_from_load_table(&resp)?.build(&engine)?;

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

fn uc_committer(
    update_table_client: &Arc<InMemoryUpdateTableClient>,
) -> UCCommitter<InMemoryUpdateTableClient> {
    UCCommitter::new(
        update_table_client.clone(),
        TABLE_ID,
        TableIdentifier::new(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE),
    )
}

/// Connector coroutine for catalog commit.
///
/// UC `start_commit` takes an opaque action cursor so it cannot observe kernel requests from the
/// generator. This workflow pages that cursor locally and suspends for those kernel requests and
/// for UC requests other than `ContinueActions`.
mod commit {
    use delta_kernel::committer::{CommitMetadata, CommitResponse};
    use delta_kernel::coroutine::{core, Cursor, Page, Resume, YieldResume};
    use delta_kernel::FilteredEngineData;
    use delta_kernel_unity_catalog::coroutine::CommitActions;

    use super::*;

    type ActionGenerator = kernel::Generator<FilteredEngineData>;

    #[derive(derive_more::From)]
    enum Workflow {
        Done(CommitResponse),
        Request(Request),
    }

    enum Request {
        Kernel(
            kernel::Request<ActionGenerator>,
            Resume<Workflow, ActionGenerator>,
        ),
        Catalog(
            uc::Request<uc::Workflow<CommitResponse>>,
            Resume<Workflow, uc::Workflow<CommitResponse>>,
        ),
    }

    enum PendingRequest {
        Kernel(core::Pending<kernel::Request<ActionGenerator>, ActionGenerator>),
        Catalog(
            core::Pending<uc::Request<uc::Workflow<CommitResponse>>, uc::Workflow<CommitResponse>>,
        ),
    }

    impl Default for PendingRequest {
        fn default() -> Self {
            Self::Kernel(core::Pending::default())
        }
    }

    impl core::OutboxEntry for PendingRequest {
        fn is_live(&self) -> bool {
            match self {
                Self::Kernel(request) => request.is_live(),
                Self::Catalog(request) => request.is_live(),
            }
        }
    }

    impl core::IntoRequest<Workflow> for PendingRequest {
        type Request = Request;

        fn into_request(self, step: impl core::Step<Workflow>) -> DeltaResult<Self::Request> {
            match self {
                Self::Kernel(request) => request.into_request(step, Request::Kernel),
                Self::Catalog(request) => request.into_request(step, Request::Catalog),
            }
        }
    }

    struct Channel(core::Channel<PendingRequest>);

    impl Channel {
        async fn dispatch_kernel(
            &self,
            request: kernel::Request<ActionGenerator>,
        ) -> DeltaResult<ActionGenerator> {
            self.0.exchange(request, PendingRequest::Kernel).await
        }

        async fn dispatch_catalog(
            &self,
            request: uc::Request<uc::Workflow<CommitResponse>>,
        ) -> DeltaResult<uc::Workflow<CommitResponse>> {
            self.0.exchange(request, PendingRequest::Catalog).await
        }
    }

    enum CursorState {
        Prepared(ActionGenerator),
        Active(YieldResume<ActionGenerator>),
    }

    impl Workflow {
        fn start(workflow: uc::Workflow<CommitResponse>) -> DeltaResult<Self> {
            core::WorkflowTask::new(|channel| drive_catalog(Channel(channel), workflow)).step()
        }
    }

    pub(super) async fn drive(
        connector: &AsyncEngineConnector,
        committer: &UCCommitter<InMemoryUpdateTableClient>,
        metadata: CommitMetadata,
        actions: ActionGenerator,
    ) -> DeltaResult<CommitResponse> {
        let actions = Cursor::new(CursorState::Prepared(actions));
        let mut workflow = Workflow::start(committer.start_commit(metadata, actions)?);
        loop {
            workflow = match workflow? {
                Workflow::Done(response) => return Ok(response),
                Workflow::Request(Request::Kernel(request, resume)) => {
                    resume(connector.resume(request).await)
                }
                Workflow::Request(Request::Catalog(request, resume)) => {
                    resume(serve_catalog(connector, committer, request).await)
                }
            };
        }
    }

    async fn serve_catalog(
        connector: &AsyncEngineConnector,
        committer: &UCCommitter<InMemoryUpdateTableClient>,
        request: uc::Request<uc::Workflow<CommitResponse>>,
    ) -> DeltaResult<uc::Workflow<CommitResponse>> {
        match request {
            uc::Request::ContinueActions(..) => Err(Error::internal_error(
                "UC commit driver leaked a ContinueActions request",
            )),
            uc::Request::WriteJson(request) => {
                connector.resume(kernel::Request::WriteJson(request)).await
            }
            uc::Request::CopyAtomic(operation, resume) => {
                connector
                    .resume(kernel::Request::CopyAtomic(operation, resume))
                    .await
            }
            uc::Request::UpdateTable(operation, resume) => {
                resume(committer.execute_update_table(operation).await)
            }
        }
    }

    async fn drive_catalog(
        channel: Channel,
        mut workflow: uc::Workflow<CommitResponse>,
    ) -> DeltaResult<CommitResponse> {
        loop {
            workflow = match workflow {
                uc::Workflow::Done(response) => return Ok(response),
                uc::Workflow::Request(uc::Request::ContinueActions(cursor, resume)) => {
                    resume(continue_actions(&channel, cursor).await)?
                }
                uc::Workflow::Request(request) => channel.dispatch_catalog(request).await?,
            };
        }
    }

    async fn continue_actions(
        channel: &Channel,
        cursor: Cursor<CommitActions>,
    ) -> DeltaResult<Page<CommitActions>> {
        let mut generator = match cursor.into_inner()? {
            CursorState::Prepared(generator) => Ok(generator),
            CursorState::Active(resume) => resume(Ok(())),
        };
        loop {
            generator = match generator? {
                kernel::Generator::Done(()) => return Ok(Page::new(Vec::new(), None)),
                kernel::Generator::Yield(action, resume) => {
                    let cursor = Cursor::new(CursorState::Active(resume));
                    return Ok(Page::new(vec![action], Some(cursor)));
                }
                kernel::Generator::Request(request) => channel.dispatch_kernel(request).await,
            };
        }
    }
}

async fn drive_kernel<O: Send + 'static>(
    connector: &AsyncEngineConnector,
    committer: &UCCommitter<InMemoryUpdateTableClient>,
    mut workflow: DeltaResult<kernel::Workflow<O>>,
) -> DeltaResult<O> {
    loop {
        workflow = match workflow? {
            kernel::Workflow::Done(output) => return Ok(output),
            kernel::Workflow::Request(kernel::Request::Commit(commit, resume)) => {
                let commit = *commit;
                resume(commit::drive(connector, committer, commit.metadata, commit.actions).await)
            }
            kernel::Workflow::Request(kernel::Request::Publish(metadata, resume)) => {
                let result = match committer.start_publish(metadata) {
                    Ok(workflow) => drive_uc(connector, committer, workflow).await,
                    Err(err) => Err(err),
                };
                resume(result)
            }
            kernel::Workflow::Request(request) => connector.resume(request).await,
        };
    }
}

async fn drive_uc<O: Send + 'static>(
    connector: &AsyncEngineConnector,
    committer: &UCCommitter<InMemoryUpdateTableClient>,
    mut workflow: uc::Workflow<O>,
) -> DeltaResult<O> {
    loop {
        workflow = match workflow {
            uc::Workflow::Done(output) => return Ok(output),
            uc::Workflow::Request(uc::Request::ContinueActions(..)) => {
                return Err(Error::internal_error(
                    "UC publish driver leaked a ContinueActions request",
                ));
            }
            uc::Workflow::Request(uc::Request::WriteJson(request)) => {
                let request = kernel::Request::WriteJson(request);
                connector.resume(request).await?
            }
            uc::Workflow::Request(uc::Request::CopyAtomic(operation, resume)) => {
                let request = kernel::Request::CopyAtomic(operation, resume);
                connector.resume(request).await?
            }
            uc::Workflow::Request(uc::Request::UpdateTable(operation, resume)) => {
                resume(committer.execute_update_table(operation).await)?
            }
        };
    }
}

/// Commits an empty transaction and returns the post-commit snapshot.
fn commit_empty_transaction(
    snapshot: &Arc<Snapshot>,
    update_table_client: &Arc<InMemoryUpdateTableClient>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
) -> Result<Arc<Snapshot>, TestError> {
    Ok(snapshot
        .clone()
        .transaction(engine)?
        .with_operation("WRITE".to_string())
        .legacy_commit(Box::new(uc_committer(update_table_client)), engine)?
        .unwrap_post_commit_snapshot())
}

/// Loads a snapshot via the connector read path: `load_table` response -> snapshot builder.
fn build_snapshot(
    client: &Arc<InMemoryUpdateTableClient>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
    table_uri: &url::Url,
) -> Result<Arc<Snapshot>, TestError> {
    let resp = client.load_table_response(TABLE_ID, table_uri.as_str())?;
    Ok(snapshot_builder_from_load_table(&resp)?.build(engine)?)
}

// ============================================================================
// Tests
// ============================================================================

// multi_thread required: UCCommitter uses block_on which panics on single-threaded runtime
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
async fn test_native_coroutine_commit_and_publish() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        snapshot,
        table_uri,
        _tmp_dir,
    } = setup().await?;
    let connector = engine.async_connector();
    let committer = uc_committer(&update_table_client);
    let txn = drive_kernel(&connector, &committer, snapshot.start_transaction()).await?;
    let result = drive_kernel(&connector, &committer, txn.start_commit()).await?;

    assert!(result.is_committed());
    assert_eq!(
        update_table_client
            .load_table_response(TABLE_ID, "")?
            .latest_table_version,
        Some(3)
    );
    let snapshot = result.unwrap_post_commit_snapshot();
    drive_kernel(&connector, &committer, snapshot.start_publish(true)).await?;
    let published = connector
        .drive(
            Snapshot::builder_for(table_uri)
                .with_max_catalog_version(3)
                .start(),
        )
        .await?;
    assert_eq!(published.version(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_without_publish_hits_limit() -> Result<(), TestError> {
    let TestSetup {
        update_table_client,
        engine,
        mut snapshot,
        _tmp_dir,
        ..
    } = setup().await?;

    // Start with 2 unpublished (v1, v2). Insert up to MAX, then the next should fail.
    let max = TableData::MAX_UNPUBLISHED_COMMITS as u64;
    for _ in 3..=max {
        snapshot = commit_empty_transaction(&snapshot, &update_table_client, &engine)?;
    }
    assert_eq!(snapshot.version(), max);

    // Next insert should fail with MaxUnpublishedCommitsExceeded
    let committer = Box::new(uc_committer(&update_table_client));
    let err = snapshot
        .clone()
        .transaction(&engine)?
        .legacy_commit(committer, &engine)
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

    commit_empty_transaction(&snapshot, &update_table_client, &engine)?
        .publish(&engine, &uc_committer(&update_table_client))?
        .checkpoint(&engine, None)?;

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
        update_table_client,
        engine,
        snapshot,
        _tmp_dir,
        ..
    } = setup().await?;

    let snapshot = commit_empty_transaction(&snapshot, &update_table_client, &engine)?;
    let err = snapshot.checkpoint(&engine, None).unwrap_err();
    assert!(matches!(err, delta_kernel::Error::UnpublishedVersion(1)));
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

    let schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "val": STRING,
    };

    // v0 create: writes 000.json directly to storage, does not call the catalog.
    create_table(table_uri.as_str(), schema, "delta-kernel-uc-test")
        .with_table_properties(get_required_properties_for_disk(TABLE_ID))
        .build(engine.as_ref())?
        .legacy_commit(Box::new(uc_committer(&client)), engine.as_ref())?
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

    // Append 2 rows. A scan at v2 should see 5 rows.
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
