//! Integration coverage for `ScanBuilder::with_cancellation_token`: a cancelled scan must
//! surface `KernelError::Cancelled` through the real Default Engine and can never be mistaken for a
//! complete listing.

use std::sync::Arc;

use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::scan::StatsOptions;
use delta_kernel::{
    CancellationToken as _, CancellationTokenRef, DeltaResult, Engine, FileMeta, FileSlice,
    JsonHandler, KernelError, ParquetHandler, Snapshot, StorageHandler,
};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    actions_to_string, add_commit, generate_simple_batch, load_test_data, record_batch_to_bytes,
    TestAction, TestCancellationToken, TokenCapturingEngine,
};

const PARQUET_FILE1: &str = "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet";

/// Builds a two-commit JSON-log table (no checkpoint) in memory and returns `(storage, root)`.
async fn json_only_table() -> Result<(Arc<InMemory>, &'static str), Box<dyn std::error::Error>> {
    let batch = generate_simple_batch()?;
    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";
    let file_size = record_batch_to_bytes(&batch).len() as u64;
    add_commit(
        table_root,
        storage.as_ref(),
        0,
        actions_to_string(vec![
            TestAction::Metadata,
            TestAction::AddWithSize(PARQUET_FILE1.to_string(), file_size),
        ]),
    )
    .await?;
    storage
        .put(
            &Path::from(PARQUET_FILE1),
            record_batch_to_bytes(&batch).into(),
        )
        .await?;
    Ok((storage, table_root))
}

// A scan whose builder was given an already-cancelled token yields exactly one
// `KernelError::Cancelled` and then ends -- it never produces a (partial) complete-looking listing.
// Parametrized over stats mode because a predicate/stats scan takes a different replay path
// (checkpoint parquet reads + stats parsing) than the JSON-only default, and both must honor the
// token: `None` is the plain default (no `with_stats` call), `Some` opts into struct stats.
#[rstest]
#[case::json_default(None)]
#[case::with_stats(Some(StatsOptions::all_struct()))]
#[tokio::test]
async fn precancelled_scan_yields_cancelled(
    #[case] stats: Option<StatsOptions>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let mut builder = snapshot.scan_builder().with_cancellation_token(token);
    if let Some(stats) = stats {
        builder = builder.with_stats(stats);
    }
    let scan = builder.build()?;

    // Cancellation surfaces as `KernelError::Cancelled`, never as a complete listing. It may arrive
    // either from the eager setup reads that `scan_metadata` performs (returning `Err` directly)
    // or as the iterator's terminal item -- assert whichever, and that no successful batch and no
    // silent `None`-only stream is ever produced.
    assert_cancelled(scan.scan_metadata(&engine));
    Ok(())
}

/// Asserts that a scan_metadata result represents cancellation: either the call itself returned
/// `Err(Cancelled)`, or the iterator yields `Err(Cancelled)` before any `Ok` and then fuses.
fn assert_cancelled<
    I: Iterator<Item = delta_kernel::DeltaResult<delta_kernel::scan::ScanMetadata>>,
>(
    result: delta_kernel::DeltaResult<I>,
) {
    match result {
        Err(delta_kernel::Error::Kernel(KernelError::Cancelled)) => {}
        Err(other) => panic!("expected Cancelled, got {other:?}"),
        Ok(mut iter) => {
            assert!(
                matches!(
                    iter.next(),
                    Some(Err(delta_kernel::Error::Kernel(KernelError::Cancelled)))
                ),
                "cancelled scan must yield Err(Cancelled), never an Ok batch or bare None"
            );
            assert!(
                iter.next().is_none(),
                "iterator must fuse after cancellation"
            );
        }
    }
}

// Control: the same scan with no cancellation token (the default) completes normally, proving
// the cancellation path is opt-in and does not otherwise change behavior.
#[tokio::test]
async fn uncancelled_json_scan_completes() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    let scan = snapshot.clone().scan_builder().build()?;
    let count = scan.scan_metadata(&engine)?.filter(|r| r.is_ok()).count();
    assert!(count > 0, "uncancelled scan should yield scan metadata");

    // An uncancelled token behaves identically.
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::default());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token)
        .build()?;
    for res in scan.scan_metadata(&engine)? {
        assert!(res.is_ok(), "uncancelled token must not inject errors");
    }
    Ok(())
}

// Cancelling a LIVE token after iteration has started surfaces exactly ONE terminal
// `Err(Cancelled)` and then fuses -- exercising both cancellation layers (kernel batch-boundary
// poll + the engine yielding its own cancelled error), the composition the pre-cancelled tests
// can't reach. Guards against the double-emit the two layers would otherwise produce.
#[tokio::test(flavor = "multi_thread")]
async fn mid_stream_cancellation_yields_exactly_one_error() -> Result<(), Box<dyn std::error::Error>>
{
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    let token = Arc::new(TestCancellationToken::default());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token.clone() as CancellationTokenRef)
        .build()?;

    let mut iter = scan.scan_metadata(&engine)?;
    assert!(
        matches!(iter.next(), Some(Ok(_))),
        "expected an Ok batch before cancellation"
    );

    token.cancel();

    assert!(matches!(
        iter.next(),
        Some(Err(delta_kernel::Error::Kernel(KernelError::Cancelled)))
    ));
    assert!(
        iter.next().is_none(),
        "iterator must fuse after the single error"
    );
    Ok(())
}

// A pre-cancelled scan over a table WITH a checkpoint surfaces `Err(Cancelled)` -- reaching the
// checkpoint/sidecar/footer `check_cancelled` guards that the JSON-only fixtures never enter.
// `packed` distinguishes a `.tar.zst` fixture (unpacked to a temp dir) from a plain directory.
#[rstest]
#[case::v1_checkpoint("with_checkpoint_no_last_checkpoint", false)]
#[case::v2_parquet_sidecars("v2-checkpoints-parquet-with-sidecars", true)]
#[tokio::test]
async fn precancelled_scan_over_checkpoint_yields_cancelled(
    #[case] test_name: &str,
    #[case] packed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // `_tempdir` holds the unpacked fixture (packed case) for the test's lifetime.
    let (table_path, _tempdir) = if packed {
        let dir = load_test_data("./tests/data", test_name)?;
        let path = dir.path().join(test_name);
        (path, Some(dir))
    } else {
        (
            std::path::PathBuf::from(format!("./tests/data/{test_name}")),
            None,
        )
    };
    let url = url::Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token)
        .build()?;

    assert_cancelled(scan.scan_metadata(engine.as_ref()));
    Ok(())
}

/// Asserts the captured token is the exact `Arc` the caller supplied (identity), and therefore
/// downcasts back to the caller's concrete type and observes cancellation through it.
fn assert_token_recovered_by_identity(
    seen: Option<CancellationTokenRef>,
    token: Arc<TestCancellationToken>,
) {
    let seen = seen.expect("kernel should have passed the cancellation token to the handler");
    // Same allocation, not an equivalent wrapper.
    assert!(
        Arc::ptr_eq(&(token.clone() as CancellationTokenRef), &seen),
        "kernel must pass the caller's token through by identity"
    );
    let recovered = seen
        .as_ref()
        .any_ref()
        .downcast_ref::<TestCancellationToken>()
        .expect("token must downcast to the type the caller supplied");
    assert!(!recovered.is_cancelled());
    token.cancel();
    assert!(recovered.is_cancelled());
}

// Pins the pass-through-identity guarantee on the JSON read path: the engine receives the very
// `Arc` the caller supplied, not a wrapper, so it can downcast back to its own token type.
#[tokio::test]
async fn engine_receives_the_callers_token_by_identity_json(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = TokenCapturingEngine::new(Arc::new(DefaultEngineBuilder::new(storage).build()));
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    let token = Arc::new(TestCancellationToken::default());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token.clone() as CancellationTokenRef)
        .build()?;
    scan.scan_metadata(&engine)?.for_each(drop);

    assert_token_recovered_by_identity(engine.json_token(), token);
    Ok(())
}

// Same guarantee on the PARQUET read path, which the JSON-only fixture cannot reach. Checkpoint
// replay threads the token through separate `.cloned()` call sites; a live (uncancelled) token lets
// the read actually execute so the parquet handler observes it.
#[tokio::test]
async fn engine_receives_the_callers_token_by_identity_parquet(
) -> Result<(), Box<dyn std::error::Error>> {
    let table_name = "with_checkpoint_no_last_checkpoint";
    let url =
        url::Url::from_directory_path(std::fs::canonicalize(format!("./tests/data/{table_name}"))?)
            .unwrap();

    let engine = TokenCapturingEngine::new(test_utils::create_default_engine(&url)?);
    let snapshot = Snapshot::builder_for(url).build(&engine)?;

    let token = Arc::new(TestCancellationToken::default());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token.clone() as CancellationTokenRef)
        .build()?;
    scan.scan_metadata(&engine)?.for_each(drop);

    assert_token_recovered_by_identity(engine.parquet_token(), token);
    Ok(())
}

// `parallel_scan_metadata` does not support cancellation; setting a token makes it error rather
// than silently run to completion.
#[tokio::test]
async fn parallel_scan_metadata_errors_when_token_set() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine: Arc<dyn delta_kernel::Engine> =
        Arc::new(DefaultEngineBuilder::new(storage).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::default());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token)
        .build()?;

    let result = scan.parallel_scan_metadata(engine);
    assert!(
        matches!(
            result,
            Err(delta_kernel::Error::Kernel(KernelError::Unsupported(_)))
        ),
        "parallel_scan_metadata must reject a cancellation token"
    );
    Ok(())
}

// Building a snapshot with an already-cancelled token fails rather than returning a snapshot built
// from a partial log listing.
#[tokio::test]
async fn precancelled_snapshot_build_yields_cancelled() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let result = Snapshot::builder_for(table_root)
        .with_cancellation_token(token)
        .build(&engine);

    assert!(
        matches!(
            result,
            Err(delta_kernel::Error::Kernel(KernelError::Cancelled))
        ),
        "a cancelled snapshot build must surface KernelError::Cancelled"
    );
    Ok(())
}

// An uncancelled token leaves snapshot building unchanged, so the feature is opt-in and the token's
// mere presence costs nothing.
#[tokio::test]
async fn snapshot_build_with_uncancelled_token_succeeds() -> Result<(), Box<dyn std::error::Error>>
{
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::default());
    let with_token = Snapshot::builder_for(table_root)
        .with_cancellation_token(token)
        .build(&engine)?;
    let without_token = Snapshot::builder_for(table_root).build(&engine)?;

    assert_eq!(with_token.version(), without_token.version());
    Ok(())
}

/// A storage decorator that cancels `token` when a listing begins, delegating everything else to
/// the real handler. `_last_checkpoint` is read (via `read_files`) *before* the log listing, so
/// this drives cancellation into the listing specifically -- the read has already succeeded by
/// then.
struct CancelOnListHandler {
    inner: Arc<dyn StorageHandler>,
    token: Arc<TestCancellationToken>,
}

impl StorageHandler for CancelOnListHandler {
    fn list_from(
        &self,
        path: &url::Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        self.inner.list_from(path)
    }

    fn list_from_with_cancellation(
        &self,
        path: &url::Url,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        self.token.cancel();
        self.inner
            .list_from_with_cancellation(path, cancellation_token)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<bytes::Bytes>>>> {
        self.inner.read_files(files)
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        cancellation_token: Option<CancellationTokenRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<bytes::Bytes>>>> {
        self.inner
            .read_files_with_cancellation(files, cancellation_token)
    }

    fn put(&self, path: &url::Url, data: bytes::Bytes, overwrite: bool) -> DeltaResult<()> {
        self.inner.put(path, data, overwrite)
    }

    fn copy_atomic(&self, src: &url::Url, dest: &url::Url) -> DeltaResult<()> {
        self.inner.copy_atomic(src, dest)
    }

    fn head(&self, path: &url::Url) -> DeltaResult<FileMeta> {
        self.inner.head(path)
    }

    fn delete(&self, path: &url::Url) -> DeltaResult<()> {
        self.inner.delete(path)
    }
}

/// Installs a [`CancelOnListHandler`] over the real engine's storage, delegating other handlers.
struct CancelOnListEngine {
    inner: Arc<dyn Engine>,
    storage: Arc<CancelOnListHandler>,
}

impl Engine for CancelOnListEngine {
    fn evaluation_handler(&self) -> Arc<dyn delta_kernel::EvaluationHandler> {
        self.inner.evaluation_handler()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.inner.json_handler()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.inner.parquet_handler()
    }
}

// Cancellation reaches the log listing during a real `build()`, not only the pre-listing
// `_last_checkpoint` read: the token is live when `try_read` runs (so that read succeeds) and flips
// only once listing begins, so the `Err(Cancelled)` `build()` surfaces must come from the listing.
// Guards the SnapshotBuilder -> listing token wiring against silent removal.
#[tokio::test]
async fn snapshot_build_cancelled_during_listing() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let token = Arc::new(TestCancellationToken::default());
    let engine = CancelOnListEngine {
        inner: Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
        storage: Arc::new(CancelOnListHandler {
            inner: DefaultEngineBuilder::new(storage).build().storage_handler(),
            token: token.clone(),
        }),
    };

    let result = Snapshot::builder_for(table_root)
        .with_cancellation_token(token.clone() as CancellationTokenRef)
        .build(&engine);
    assert!(
        matches!(
            result,
            Err(delta_kernel::Error::Kernel(KernelError::Cancelled))
        ),
        "cancellation during listing must surface from build()"
    );
    Ok(())
}

// An incremental build (`Snapshot::builder_from`) re-lists the log to find new commits, so an
// already-cancelled token stops it the same way it stops a from-scratch build -- rather than
// returning a snapshot advanced from a partial listing.
#[tokio::test]
async fn precancelled_incremental_snapshot_build_yields_cancelled(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();

    let base = Snapshot::builder_for(table_root).build(&engine)?;

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let result = Snapshot::builder_from(base)
        .with_cancellation_token(token)
        .build(&engine);

    assert!(
        matches!(
            result,
            Err(delta_kernel::Error::Kernel(KernelError::Cancelled))
        ),
        "a cancelled incremental snapshot build must surface KernelError::Cancelled"
    );
    Ok(())
}
