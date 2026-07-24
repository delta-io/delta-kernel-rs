//! Integration coverage for `ScanBuilder::with_cancellation_token`: a cancelled scan must
//! surface `Error::Cancelled` through the real Default Engine and can never be mistaken for a
//! complete listing.

use std::sync::Arc;

use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::scan::StatsOptions;
use delta_kernel::{CancellationTokenRef, Error, Snapshot};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    actions_to_string, add_commit, generate_simple_batch, load_test_data, record_batch_to_bytes,
    TestAction, TestCancellationToken,
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
// `Error::Cancelled` and then ends -- it never produces a (partial) complete-looking listing.
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

    // Cancellation surfaces as `Error::Cancelled`, never as a complete listing. It may arrive
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
        Err(Error::Cancelled) => {}
        Err(other) => panic!("expected Cancelled, got {other:?}"),
        Ok(mut iter) => {
            assert!(
                matches!(iter.next(), Some(Err(Error::Cancelled))),
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

    assert!(matches!(iter.next(), Some(Err(Error::Cancelled))));
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
        matches!(result, Err(Error::Unsupported(_))),
        "parallel_scan_metadata must reject a cancellation token"
    );
    Ok(())
}
