//! Integration coverage for `ScanBuilder::with_cancellation_token`: a cancelled scan must
//! surface `Error::Cancelled` through the real Default Engine and can never be mistaken for a
//! complete listing.

use std::sync::Arc;

use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::{CancellationTokenRef, Error, Snapshot};
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    actions_to_string, add_commit, generate_simple_batch, record_batch_to_bytes, TestAction,
    TestCancellationToken,
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
#[tokio::test]
async fn precancelled_json_scan_yields_cancelled() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let scan = snapshot
        .scan_builder()
        .with_cancellation_token(token)
        .build()?;

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

// A predicate scan (which enables checkpoint parquet reads / stats parsing) is also cancellable:
// a pre-cancelled token surfaces `Error::Cancelled` rather than a complete listing.
#[tokio::test]
async fn precancelled_scan_with_stats_yields_cancelled() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, table_root) = json_only_table().await?;
    let engine = DefaultEngineBuilder::new(storage).build();
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let scan = snapshot
        .scan_builder()
        .with_stats(delta_kernel::scan::StatsOptions::all_struct())
        .with_cancellation_token(token)
        .build()?;

    assert_cancelled(scan.scan_metadata(&engine));
    Ok(())
}
